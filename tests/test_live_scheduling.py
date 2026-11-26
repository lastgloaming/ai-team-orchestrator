import concurrent.futures
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from orchestrator.auto_events import AUTHOR_ACCOUNT, REPOSITORY
from orchestrator.auto_state import PairedState
from orchestrator.deepseek import Limits
from orchestrator.live_scheduling import (
    COMMAND, REFS, Authorization, PrivateLedger, checked_remote, check_source, schedule_event,
)

APPROVAL = Authorization("a" * 32, 101, 50, 2)
LIMITS = Limits("deepseek-flash", 2000, 8000, 10, 64, 2_200_000_000, 2_200_000_000,
                1, 1, retries=0, currency="CNY")


def event(approval=APPROVAL, body=COMMAND):
    return {
        "action": "created", "repository": {"id": approval.repository_id, "full_name": REPOSITORY},
        "issue": {"number": approval.pr, "state": "open", "pull_request": {}},
        "sender": {"id": AUTHOR_ACCOUNT},
        "comment": {"id": approval.event_id, "user": {"id": AUTHOR_ACCOUNT}, "body": body},
    }


def response(advice="inspect_status"):
    return {"usage": {"prompt_tokens": 3, "completion_tokens": 2},
            "choices": [{"finish_reason": "stop", "message": {"content": json.dumps({"suggestion": advice})}}]}


class LiveSchedulingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.remote = str(Path(temp.name) / "billing.git")
        subprocess.run(["git", "init", "--bare", "--quiet", self.remote], check=True)
        self.anchors = PrivateLedger(self.remote).bootstrap()

    def state(self, cls=PrivateLedger):
        return cls(self.remote, self.anchors)

    def run_event(self, transport, *, state=None, authorization=APPROVAL, payload=None, actor=str(AUTHOR_ACCOUNT),
                  limits=LIMITS, scheduling=True, live=True, token="fixture"):
        return schedule_event(state or self.state(), authorization, payload or event(authorization), actor,
                              limits, transport, scheduling=lambda: scheduling, live=lambda: live, token=token)

    def fail_if_called(self, *_):
        self.fail("model transport was invoked")

    def rows(self, query):
        def read(path):
            with closing(sqlite3.connect(path)) as db:
                return db.execute(query).fetchall()
        return self.state().update(read)

    def test_default_off_identity_wrong_pr_historical_markers_and_missing_credential(self):
        for scheduling, live in ((False, False), (False, True), (True, False)):
            result = self.run_event(self.fail_if_called, scheduling=scheduling, live=live)
            self.assertEqual(result["scheduling"], "DISABLED")
        incorrect_pr = event()
        incorrect_pr["issue"]["number"] = 3
        for result in (
            self.run_event(self.fail_if_called, actor="99"),
            self.run_event(self.fail_if_called, payload=incorrect_pr),
            self.run_event(self.fail_if_called, payload=event(body="P4-SYNTHETIC-COMMAND")),
            self.run_event(self.fail_if_called, payload=event(body="P5-SYNTHETIC-ADVICE")),
        ):
            self.assertEqual(result["scheduling"], "NOT_ELIGIBLE")
        with self.assertRaisesRegex(ValueError, "credential"):
            self.run_event(self.fail_if_called, token="")
        self.assertEqual(self.rows("SELECT * FROM live_authorization"), [])

    def test_success_requires_committed_authorization_and_cannot_replay_or_reconfigure(self):
        def transport(request, timeout):
            self.assertEqual(request["max_tokens"], 64)
            self.assertEqual(self.rows("SELECT approval,event_id FROM live_authorization"), [(APPROVAL.approval, 101)])
            self.assertEqual(self.rows("SELECT amount,state,currency FROM usage"), [(532_000, "active", "CNY")])
            self.assertEqual(self.rows("SELECT status FROM auto_model_attempt"), [("RESERVED",)])
            return response()

        result = self.run_event(transport)
        self.assertEqual((result["communication"], result["model_call"], result["scheduling"]),
                         ("ACCEPTED", "SUCCEEDED", "ADVISED"))
        self.assertEqual(self.run_event(self.fail_if_called)["model_call"], "RECORDED_SUCCEEDED")
        new = replace(APPROVAL, approval="b" * 32, event_id=102, pr=3)
        self.assertEqual(self.run_event(self.fail_if_called, authorization=new)["scheduling"], "BLOCKED")
        self.assertEqual(self.rows("SELECT count(*) FROM usage"), [(1,)])

    def test_budget_denial_is_persisted_without_usage_and_is_not_reevaluated(self):
        low = replace(LIMITS, daily_budget=531_999)
        result = self.run_event(self.fail_if_called, limits=low)
        self.assertEqual((result["model_call"], result["scheduling"]), ("NOT_SENT", "BLOCKED"))
        self.assertEqual(self.rows("SELECT status,reason FROM auto_model_attempt"), [("BLOCKED", "budget_exhausted")])
        self.assertEqual(self.rows("SELECT count(*) FROM usage"), [(0,)])
        self.assertEqual(self.run_event(self.fail_if_called)["scheduling"], "BLOCKED")

    def test_output_is_not_actionable(self):
        result = self.run_event(lambda *_: response("execute_anything"))
        self.assertEqual((result["model_call"], result["scheduling"]), ("INVALID_RESPONSE", "REJECTED"))
        self.assertNotIn("execute_anything", json.dumps(result))
        self.assertEqual(self.rows("SELECT suggestion FROM auto_model_attempt"), [(None,)])

    def test_crash_after_durable_mark_leaves_no_retry(self):
        code = """
import os,sys
from orchestrator.live_scheduling import PrivateLedger
from test_live_scheduling import APPROVAL,LIMITS,event
from orchestrator.live_scheduling import schedule_event,AUTHOR_ACCOUNT
schedule_event(PrivateLedger(sys.argv[1],tuple(sys.argv[2:])),APPROVAL,event(),str(AUTHOR_ACCOUNT),
               LIMITS,lambda *_: os._exit(23),scheduling=lambda:True,live=lambda:True,token='fixture')
"""
        child = subprocess.run([sys.executable, "-B", "-c", code, self.remote, *self.anchors],
                               env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)}, timeout=20)
        self.assertEqual(child.returncode, 23)
        self.assertEqual(self.run_event(self.fail_if_called)["model_call"], "UNCONFIRMED")
        self.assertEqual(self.rows("SELECT status FROM auto_model_attempt"), [("RESERVED",)])

    def test_rejected_atomic_commit_and_legacy_pair_never_call_model(self):
        hook = Path(self.remote) / "hooks" / "update"
        hook.write_text(f'#!/bin/sh\ntest "$1" != {REFS[1]}\n')
        hook.chmod(0o700)
        result = self.run_event(self.fail_if_called)
        self.assertEqual((result["model_call"], result["scheduling"]), ("NOT_SENT", "BLOCKED"))
        self.assertEqual(self.rows("SELECT * FROM live_authorization"), [])
        with self.assertRaisesRegex(ValueError, "private ledger"):
            self.run_event(self.fail_if_called, state=PairedState(self.remote, "other", self.anchors))

    def test_privacy_check_refuses_source_and_public_repositories(self):
        with self.assertRaises(ValueError):
            checked_remote(REPOSITORY, 50, "fixture")
        base = {"id": 90, "full_name": "lastgloaming/billing-test", "visibility": "private",
                "private": True, "fork": False}
        for metadata in (base, {**base, "visibility": "public"}, {**base, "id": 91}):
            with patch("orchestrator.live_scheduling.urllib.request.build_opener") as open_mock:
                open_mock.return_value.open.return_value = io.BytesIO(json.dumps(metadata).encode())
                if metadata == base:
                    self.assertEqual(checked_remote(base["full_name"], 90, "fixture"),
                                     "https://github.com/lastgloaming/billing-test.git")
                else:
                    with self.assertRaisesRegex(ValueError, "privacy"):
                        checked_remote(base["full_name"], 90, "fixture")

    def test_source_recheck_rejects_edited_comment(self):
        target = {"number": 2, "state": "open", "base": {"repo": {"id": 50, "full_name": REPOSITORY}}}
        comment = {"id": 101, "body": "changed", "user": {"id": AUTHOR_ACCOUNT},
                   "issue_url": f"https://api.github.com/repos/{REPOSITORY}/issues/2"}
        with patch("orchestrator.live_scheduling.urllib.request.build_opener") as open_mock:
            open_mock.return_value.open.side_effect = [io.BytesIO(json.dumps(row).encode()) for row in (target, comment)]
            with self.assertRaisesRegex(ValueError, "no longer matches"):
                check_source(APPROVAL, "fixture")

    def test_concurrent_writers_never_consume_same_approval_twice(self):
        calls = []
        barrier = threading.Barrier(2)

        class Race(PrivateLedger):
            first = True

            def _advance(self, *args):
                if self.first:
                    self.first = False
                    barrier.wait(10)
                return super()._advance(*args)

        def mock(*_):
            calls.append(1)
            return response()

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: self.run_event(mock, state=self.state(Race)), range(2)))
        self.assertEqual(calls, [1])
        self.assertEqual(sum(result["model_call"] == "SUCCEEDED" for result in results), 1)
        self.assertEqual(self.rows("SELECT count(*) FROM live_authorization"), [(1,)])

    def test_timeout_preserves_reservation_and_does_not_retry(self):
        def mock(*_):
            raise TimeoutError()

        self.assertEqual(self.run_event(mock)["model_call"], "UNCONFIRMED")
        self.assertEqual(self.run_event(self.fail_if_called)["model_call"], "UNCONFIRMED")
        self.assertEqual(self.rows("SELECT amount,state FROM usage"), [(532_000, "uncertain")])
