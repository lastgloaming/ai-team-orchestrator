import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_bootstrap import MAIN, REPOSITORY, initialize, main
from orchestrator.auto_state import REFS, PairedState
from orchestrator.state import STATE_REF, StateConflict


class AutoBootstrapTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.remote = self.root / "fake.git"
        self.git("init", "--bare", "--quiet", str(self.remote))
        self.commit = self.git(
            "-C", str(self.remote), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
            "commit-tree", "4b825dc642cb6eb9a060e54bf8d69288fbee4904", "-m", "Fixture main",
        ).stdout.decode().strip()
        for ref in (MAIN, STATE_REF):
            self.git("-C", str(self.remote), "update-ref", ref, self.commit)

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], capture_output=True, check=True)

    def refs(self):
        rows = self.git("-C", str(self.remote), "for-each-ref", "--format=%(refname) %(objectname)").stdout
        return dict(line.split() for line in rows.decode().splitlines())

    def test_single_atomic_push_and_genesis_verification(self):
        calls = []
        original = PairedState._git

        def record(state, root, *args, **kwargs):
            if "push" in args:
                calls.append(args)
            return original(state, root, *args, **kwargs)

        with patch.object(PairedState, "_git", record):
            anchors = initialize(str(self.remote), self.commit, approval="1", auto_enabled="0")
        self.assertEqual(len(calls), 1)
        self.assertIn("--atomic", calls[0])
        self.assertEqual(self.refs(), {MAIN: self.commit, STATE_REF: self.commit, **dict(zip(REFS, anchors))})
        with self.assertRaisesRegex(StateConflict, "already exist"):
            initialize(str(self.remote), self.commit, approval="1", auto_enabled="")

    def test_approval_auto_events_and_main_guard(self):
        baseline = self.refs()
        for approval, auto in (("", ""), ("0", "0"), ("1", "1"), ("1", "unexpected")):
            with self.subTest(approval=approval, auto=auto), self.assertRaises(StateConflict):
                initialize(str(self.remote), self.commit, approval=approval, auto_enabled=auto)
        with self.assertRaisesRegex(StateConflict, "preflight"):
            initialize(str(self.remote), "a" * 40, approval="1", auto_enabled="")
        self.assertEqual(self.refs(), baseline)

    def test_partial_pair_stops_before_push(self):
        self.git("-C", str(self.remote), "update-ref", REFS[0], self.commit)
        before = self.refs()
        with patch.object(PairedState, "bootstrap", side_effect=AssertionError("attempted bootstrap")):
            with self.assertRaisesRegex(StateConflict, "already exist"):
                initialize(str(self.remote), self.commit, approval="1", auto_enabled="")
        self.assertEqual(self.refs(), before)

    def test_server_without_atomic_support_does_not_create_refs(self):
        self.git("-C", str(self.remote), "config", "receive.advertiseAtomic", "false")
        before = self.refs()
        with self.assertRaisesRegex(StateConflict, "bootstrap unconfirmed"):
            initialize(str(self.remote), self.commit, approval="1", auto_enabled="")
        self.assertEqual(self.refs(), before)

    def test_missing_credential_stops_before_remote_access(self):
        env = {
            "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_REF": MAIN, "GITHUB_SHA": self.commit, "ORCHESTRATOR_AUTO_BOOTSTRAP_CONFIRMED": "true",
            "ORCHESTRATOR_AUTO_EXPECTED_MAIN": self.commit,
        }
        with patch.dict(os.environ, env, clear=True), patch("orchestrator.auto_bootstrap.initialize") as boot:
            with self.assertRaisesRegex(StateConflict, "credential"):
                main()
            boot.assert_not_called()

    def test_post_push_verification_failure_stops_without_retry(self):
        original = PairedState.bootstrap
        attempts = []

        def verify_failure(state):
            attempts.append(original(state))
            return self.commit, attempts[0][1]

        with patch.object(PairedState, "bootstrap", verify_failure):
            with self.assertRaisesRegex(StateConflict, "independent remote verification"):
                initialize(str(self.remote), self.commit, approval="1", auto_enabled="")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(tuple(self.refs()[ref] for ref in REFS), attempts[0])
