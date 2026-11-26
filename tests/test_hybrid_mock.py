import base64
import concurrent.futures
import hashlib
import io
import json
import os
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing, redirect_stdout
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_events import AUTHOR_ACCOUNT, REPOSITORY
from orchestrator.hybrid_mock import REF, LocalWitness, WitnessBinding, schedule_hybrid_mock
from orchestrator.hybrid_source_mock import MARKER, probe
from orchestrator.live_approval import canonical
from orchestrator.live_scheduling import REFS, PrivateLedger
from orchestrator.state import StateConflict
from test_live_scheduling import BILLING, CODE, authorization, event, response


class HybridTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="p5-hybrid-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.public = self.root / "public.git"
        self.private = self.root / "private.git"
        for repo in (self.public, self.private):
            self.git("init", "--bare", "--quiet", str(repo))
        self.binding = WitnessBinding(91, self.witness().bootstrap(), secrets.token_hex(32), 500)
        self.anchors = PrivateLedger(str(self.private)).bootstrap()
        self.approval = authorization(self.anchors)
        self.key = self.root / "key"
        self.pub = self.root / "public-key"
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(self.key)], check=True)
        subprocess.run(["openssl", "pkey", "-in", str(self.key), "-pubout", "-out", str(self.pub)], check=True)
        self.key_pin = hashlib.sha256(self.pub.read_bytes()).hexdigest()
        self.envelope = self.seal(self.approval, self.binding)

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root)

    def witness(self, cls=LocalWitness):
        return cls(str(self.public), 91)

    def state(self, cls=PrivateLedger):
        return cls(str(self.private), self.anchors, ledger_repo=BILLING[0], ledger_id=BILLING[1])

    def seal(self, approved, binding):
        payload = canonical({"approval": asdict(approved), "witness": asdict(binding)})
        file = self.root / "payload"
        file.write_bytes(payload)
        signature = subprocess.check_output(["openssl", "pkeyutl", "-sign", "-inkey", str(self.key),
                                             "-rawin", "-in", str(file)])
        return json.dumps({key: base64.b64encode(value).decode() for key, value in
                           {"payload": payload, "signature": signature, "public_key": self.pub.read_bytes()}.items()})

    def run_mock(self, mock, *, witness=None, state=None, envelope=None, run_id=500, run_attempt=1):
        return schedule_hybrid_mock(witness or self.witness(), state or self.state(), envelope or self.envelope,
                                    self.key_pin, event(self.approval), str(AUTHOR_ACCOUNT), CODE,
                                    run_id, run_attempt, mock)

    def fail_if_called(self, *_):
        self.fail("Mock transport was invoked")

    def test_public_content_is_only_commitment_and_private_precedes_model(self):
        def model(*_):
            self.assertEqual(self.git("-C", str(self.public), "show", f"{REF}:commitment"),
                             self.binding.commitment.encode())
            self.assertEqual(self.git("-C", str(self.public), "ls-tree", "--name-only", REF), b"commitment\n")

            def check(path):
                with closing(sqlite3.connect(path)) as db:
                    return db.execute("SELECT status FROM auto_model_attempt").fetchone()[0]

            self.assertEqual(self.state().update(check), "RESERVED")
            return response()

        self.assertEqual(self.run_mock(model)["model_call"], "SUCCEEDED")
        with self.assertRaises(StateConflict):
            self.run_mock(self.fail_if_called)
        for ref, anchor in zip(REFS, self.anchors, strict=True):
            self.git("-C", str(self.private), "update-ref", ref, anchor)
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called)
        replacement = self.seal(replace(self.approval, approval="c" * 32),
                                replace(self.binding, commitment=secrets.token_hex(32)))
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called, envelope=replacement)

    def test_concurrent_runners_have_only_one_witness_grant(self):
        barrier = threading.Barrier(2)
        calls = []

        class RacingWitness(LocalWitness):
            def push(self, *args):
                barrier.wait(10)
                return super().push(*args)

        def mock(*_):
            calls.append(1)
            return response()

        def runner(_):
            try:
                return self.run_mock(mock, witness=self.witness(RacingWitness))["model_call"]
            except StateConflict:
                return "BLOCKED"

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            self.assertCountEqual(pool.map(runner, range(2)), ["SUCCEEDED", "BLOCKED"])
        self.assertEqual(calls, [1])

    def test_public_rejection_blocks_private_write_and_model(self):
        hook = self.public / "hooks" / "update"
        hook.write_text("#!/bin/sh\nexit 1\n")
        hook.chmod(0o700)
        with self.assertRaisesRegex(StateConflict, "not independently confirmed"):
            self.run_mock(self.fail_if_called)
        self.assertEqual(self.witness().tip(self.public), self.binding.anchor)
        for ref, anchor in zip(REFS, self.anchors, strict=True):
            self.assertEqual(self.git("-C", str(self.private), "rev-parse", ref).decode().strip(), anchor)

    def test_private_write_rejection_consumes_witness_without_model_grant(self):
        hook = self.private / "hooks" / "update"
        hook.write_text("#!/bin/sh\nexit 1\n")
        hook.chmod(0o700)
        self.assertEqual(self.run_mock(self.fail_if_called)["model_call"], "NOT_SENT")
        self.assertNotEqual(self.witness().tip(self.public), self.binding.anchor)
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called)

    def test_lost_public_push_ack_keeps_slot_burned_without_model(self):
        class LostWitness(LocalWitness):
            def push(self, root, commit, old):
                super().push(root, commit, old)
                raise StateConflict("injected loss after witness commit")

        with self.assertRaisesRegex(StateConflict, "injected loss"):
            self.run_mock(self.fail_if_called, witness=self.witness(LostWitness))
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called)

    def test_process_exit_after_public_commit_prevents_future_send(self):
        program = """
import json, os, sys
from orchestrator.hybrid_mock import LocalWitness, WitnessBinding
w=LocalWitness(sys.argv[1],91)
w.consume(WitnessBinding(**json.loads(sys.argv[2])))
os._exit(27)
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.public),
                                json.dumps(asdict(self.binding))],
                               env={**os.environ, "TMPDIR": str(self.root)}, timeout=20)
        self.assertEqual(child.returncode, 27)
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called)

    def test_run_attempt_and_signed_snapshot_replacement_refused(self):
        for args in ({"run_id": 501}, {"run_attempt": 2}):
            with self.assertRaisesRegex(ValueError, "first run"):
                self.run_mock(self.fail_if_called, **args)
        encoded = json.loads(self.envelope)
        payload = json.loads(base64.b64decode(encoded["payload"]))
        payload["witness"]["commitment"] = "0" * 64
        encoded["payload"] = base64.b64encode(canonical(payload)).decode()
        with self.assertRaisesRegex(ValueError, "signature"):
            self.run_mock(self.fail_if_called, envelope=json.dumps(encoded))
        self.assertEqual(self.witness().tip(self.public), self.binding.anchor)

    def test_native_mock_source_uses_only_two_readonly_gets(self):
        payload = event(self.approval, body=MARKER)
        pr = {"state": "open", "number": 2, "base": {"repo": {"id": 50, "full_name": REPOSITORY}}}
        comment = {"id": 101, "body": MARKER, "user": {"id": AUTHOR_ACCOUNT},
                   "issue_url": f"https://api.github.com/repos/{REPOSITORY}/issues/2"}
        with patch("orchestrator.live_scheduling.urllib.request.build_opener") as opener, redirect_stdout(io.StringIO()) as out:
            http = opener.return_value.open
            http.side_effect = [io.BytesIO(json.dumps(row).encode()) for row in (pr, comment)]
            probe(payload, str(AUTHOR_ACCOUNT), 2, 500, CODE, "fixture-token")
            self.assertEqual([call.args[0].get_method() for call in http.call_args_list], ["GET", "GET"])
            self.assertEqual(json.loads(out.getvalue())["source_get"], "PASS")

    def test_private_rollback_between_reservation_and_transport_refuses_mock(self):
        test = self

        class RollbackAfterReserve(PrivateLedger):
            def update(self, mutate):
                result = super().update(mutate)
                if isinstance(result, tuple) and getattr(result[-1], "status", None) == "RESERVED":
                    for ref, anchor in zip(REFS, self.anchors, strict=True):
                        test.git("-C", str(test.private), "update-ref", ref, anchor)
                return result

        result = self.run_mock(self.fail_if_called, state=self.state(RollbackAfterReserve))
        self.assertEqual(result["model_call"], "UNCONFIRMED")
        with self.assertRaisesRegex(StateConflict, "witness consumed"):
            self.run_mock(self.fail_if_called)

    def test_canonical_envelope_transfers_across_process_boundary(self):
        envelope_file = self.root / "envelope.json"
        envelope_file.write_text(self.envelope)
        program = """
import sys
from pathlib import Path
from orchestrator.hybrid_mock import LocalWitness, schedule_hybrid_mock
from orchestrator.live_scheduling import PrivateLedger
from test_live_scheduling import BILLING, CODE, authorization, event, response, AUTHOR_ACCOUNT
public, private, envelope, trust, *anchors = sys.argv[1:]
state = PrivateLedger(private, tuple(anchors), ledger_repo=BILLING[0], ledger_id=BILLING[1])
receipt = schedule_hybrid_mock(LocalWitness(public, 91), state, Path(envelope).read_text(),
                               trust, event(authorization(tuple(anchors))), str(AUTHOR_ACCOUNT),
                               CODE, 500, 1, lambda *_: response())
assert receipt["model_call"] == "SUCCEEDED"
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.public), str(self.private),
                                str(envelope_file), self.key_pin, *self.anchors],
                               env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)}, timeout=20)
        self.assertEqual(child.returncode, 0)
        with self.assertRaises(StateConflict):
            self.run_mock(self.fail_if_called)
