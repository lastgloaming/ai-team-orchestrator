import concurrent.futures
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

from orchestrator.core import parse
from orchestrator.integration_adapter import (
    CONTROL_PR, OWNER_ID, OWNER_LOGIN, REPOSITORY, Comment, IntegrationAdapter, Resources,
)


class IntegrationAdapterTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "control.sqlite"
        self.adapter = self.restart()
        self.adapter.provision_mock_ids(f"CMD-20990101-{i:03d}" for i in range(701, 721))
        self.forbidden = Mock()
        self.addCleanup(self.forbidden.assert_not_called)

    def restart(self):
        return IntegrationAdapter(self.path)

    def enqueue(self, source, action, *, ref=None, adapter=None):
        return (adapter or self.adapter).enqueue(source, action, ref=ref, author_delegated=True)

    def send(self, task, transport, resources=Resources(10, 2)):
        return self.adapter.dispatch_mock(task, transport, sender_id=OWNER_ID,
                                          sender_login=OWNER_LOGIN, resources=resources, after_comment_id=19)

    def comment(self, body, *, identifier=20, actor=OWNER_ID, login=OWNER_LOGIN):
        return Comment(REPOSITORY, CONTROL_PR, identifier, actor, login, body)

    def receipt(self, body, identifier=21):
        return self.adapter.receive_mock(self.comment(body, identifier=identifier, actor=101,
                                                      login="fixture-integration"), integration_id=101)

    def test_v1_roundtrip_memory_via_integration_and_review_dedupe(self):
        task = self.enqueue("fixture-1", "check_memory_index")
        self.assertEqual(task, "CMD-20990101-701")
        self.assertEqual(self.enqueue("fixture-1", "check_memory_index", adapter=self.restart()), task)

        def publish(body):
            self.assertEqual(self.restart().status(task), "SENDING")
            self.assertEqual(parse(body).fields["target"], "memory")
            return self.comment(body)

        self.assertEqual(self.send(task, publish), "SENT")
        ack = (f"AI-INTEGRATION-ACK\nprotocol: 1.0\nfrom: Integration\nto: ChatGPT"
               f"\ncommand_id: {task}\nstatus: ACCEPTED\nreason: synthetic fixture")
        self.assertEqual(self.receipt(ack), "ACKED")
        self.assertEqual(self.receipt(ack), "DUPLICATE")
        result = f"AI-INTEGRATION-RESULT\nid: {task}\nstatus: DONE\nevidence: fixture\nblockers: none"
        self.assertEqual(self.receipt(result, 22), "DONE")
        review = self.enqueue("review-1", "record_review", ref=task)
        self.assertEqual(self.enqueue("review-1", "record_review", ref=task), review)
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.enqueue("another-review", "record_review", ref=task)

        def publish_review(body):
            message = parse(body)
            self.assertEqual((message.kind, message.fields["ref"]), ("REVIEW", task))
            return self.comment(body, identifier=23)

        self.assertEqual(self.send(review, publish_review), "SENT")
        with self.assertRaisesRegex(ValueError, "different intent"):
            self.enqueue("fixture-1", "report_status")

    def test_resource_unknown_reserve_and_author_gate(self):
        task = self.enqueue("one", "report_status")
        for resources in (None, Resources(3, 2), Resources(5, -1)):
            self.assertEqual(self.send(task, self.forbidden, resources), "BLOCKED")
        for action in ("editor", "merge_pr", "increase_budget", "policy_change"):
            reserved = self.enqueue(action, action)
            self.assertEqual(self.send(reserved, self.forbidden), "WAITING_AUTHOR")
        self.assertEqual(self.adapter.dispatch_real(task), "BLOCKED")
        self.assertEqual(self.adapter.dispatch_real(task, enabled=True), "BLOCKED")
        self.assertEqual(self.adapter.dispatch_mock(task, self.forbidden, sender_id=99,
                                                   sender_login=OWNER_LOGIN), "BLOCKED")

    def test_unclear_delivery_no_retry_and_exact_reconciliation(self):
        task = self.enqueue("event", "report_status")
        posted = []

        def publish(body):
            posted.append(self.comment(body))
            raise TimeoutError()

        self.assertEqual(self.send(task, publish), "UNCONFIRMED")
        self.adapter = self.restart()
        self.assertEqual(self.send(task, self.forbidden), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, []), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, [replace(posted[0], id=19)]), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, [posted[0], replace(posted[0], id=21)]), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, [replace(posted[0], pr=4)]), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, posted), "SENT")

    def test_receipts_cannot_schedule_and_shared_actor_cannot_confirm(self):
        task = self.enqueue("event", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        ack = f"AI-INTEGRATION-ACK\nid: {task}\nstatus: ACCEPTED\nreason: fixture"
        self.assertEqual(self.adapter.receive_mock(self.comment(ack), integration_id=OWNER_ID), "UNAUTHENTICATED")
        self.assertEqual(self.adapter.receive_mock(self.comment(ack), integration_id=101), "UNAUTHENTICATED")
        for body in ("ordinary comment", "AI-INTEGRATION-COMMAND\ninvalid"):
            self.assertEqual(self.receipt(body), "IGNORED")
        self.assertEqual(self.receipt("AI-INTEGRATION-REVIEW\ninvalid"), "REJECTED")
        self.assertEqual(self.receipt(ack, 19), "IGNORED")
        self.assertEqual(self.adapter.status(task), "SENT")

    def test_concurrent_send_claim_persists_before_effect(self):
        task = self.enqueue("event", "report_status")
        entered, release = threading.Event(), threading.Event()

        def publish(body):
            entered.set()
            release.wait(10)
            return self.comment(body)

        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            first = pool.submit(self.send, task, publish)
            try:
                self.assertTrue(entered.wait(10))
                self.assertEqual(self.send(task, self.forbidden), "SENDING")
            finally:
                release.set()
            self.assertEqual(first.result(timeout=10), "SENT")
        second = self.enqueue("event-2", "check_memory_index")
        self.assertEqual(self.send(second, self.forbidden, Resources(4, 2)), "BLOCKED")

    def test_legacy_result_can_complete_even_if_ack_arrives_late(self):
        task = self.enqueue("event", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        body = (f"AI-INTEGRATION-RESULT\nprotocol: 1.0\nfrom: Integration\nto: ChatGPT"
                f"\ncommand_id: {task}\nstatus: DONE\nevidence: synthetic")
        self.assertEqual(self.receipt(body), "DONE")
        self.assertEqual(self.receipt(f"AI-INTEGRATION-ACK\nid: {task}\nstatus: ACCEPTED\nreason: fixture", 22),
                         "IGNORED")

    def test_process_exit_during_send_keeps_durable_claim(self):
        task = self.enqueue("event", "report_status")
        program = """
import os, sys
from orchestrator.integration_adapter import IntegrationAdapter, OWNER_ID, OWNER_LOGIN, Resources
IntegrationAdapter(sys.argv[1]).dispatch_mock(sys.argv[2], lambda _: os._exit(23),
    sender_id=OWNER_ID, sender_login=OWNER_LOGIN, resources=Resources(10, 2), after_comment_id=19)
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.path), task], timeout=10)
        self.assertEqual(child.returncode, 23)
        self.adapter = self.restart()
        self.assertEqual(self.send(task, self.forbidden), "SENDING")
        self.assertEqual(self.adapter.reconcile_mock(task, []), "SENDING")

    def test_reserved_ids_required_and_competing_allocators_do_not_reuse(self):
        with self.assertRaises(PermissionError):
            self.adapter.enqueue("no-authority", "report_status")
        other = IntegrationAdapter(self.path.with_name("empty.sqlite"))
        with self.assertRaisesRegex(RuntimeError, "no verified"):
            self.enqueue("unallocated", "report_status", adapter=other)
        self.assertEqual(self.enqueue("occupied", "report_status"), "CMD-20990101-701")
        self.restart().provision_mock_ids(["CMD-20990101-701", "CMD-20990101-702"])

        def allocate(number):
            return self.enqueue(f"competitor-{number}", "report_status", adapter=self.restart())

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            identifiers = list(pool.map(allocate, range(8)))
        self.assertEqual(len(set(identifiers)), 8)
        self.assertNotIn("CMD-20990101-701", identifiers)
        self.assertEqual(allocate(0), identifiers[0])

    def test_duplicate_ack_is_not_acceptance_and_cannot_unlock_result(self):
        task = self.enqueue("unrelated-intent", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        ack = f"AI-INTEGRATION-ACK\nid: {task}\nstatus: DUPLICATE\nreason: fixture"
        self.assertEqual(self.receipt(ack), "DUPLICATE_HOLD")
        self.assertEqual(self.receipt(ack), "DUPLICATE")
        result = f"AI-INTEGRATION-RESULT\nid: {task}\nstatus: DONE\nevidence: fixture\nblockers: none"
        self.assertEqual(self.receipt(result, 22), "IGNORED")
        self.assertEqual(self.restart().status(task), "DUPLICATE_HOLD")

    def test_progress_and_out_of_order_receipts(self):
        task = self.enqueue("event", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        def progress(status, kind="RESULT"):
            return f"AI-INTEGRATION-{kind}\nid: {task}\nstatus: {status}\nevidence: fixture\nblockers: none"

        self.assertEqual(self.receipt(progress("REVIEW", "REVIEW"), 23), "REVIEW")
        self.assertEqual(self.receipt(progress("REVIEW", "REVIEW"), 23), "DUPLICATE")
        self.assertEqual(self.receipt(progress("WAITING_AUTHOR"), 24), "WAITING_AUTHOR")
        self.assertEqual(self.receipt(progress("PARTIAL"), 26), "PARTIAL")
        self.assertEqual(self.receipt(progress("DONE"), 25), "IGNORED")
        self.assertEqual(self.receipt(progress("DONE") + "\npart: 1 of 2", 27), "REJECTED")
        self.assertEqual(self.receipt(progress("DONE").replace("\nblockers: none", ""), 28), "REJECTED")
        self.assertEqual(self.adapter.status(task), "PARTIAL")
        self.assertEqual(self.receipt(progress("DONE"), 29), "DONE")
        self.assertEqual(self.receipt(progress("WAITING_AUTHOR"), 30), "IGNORED")

    def test_old_local_task_cannot_claim_a_new_grant_retroactively(self):
        legacy = "CMD-20990101-701"
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO integration_tasks VALUES (?,?,?,?,?,?,NULL)",
                       (legacy, "legacy-event", "report_status", "integration", None, "QUEUED"))
        self.restart().provision_mock_ids([legacy])
        self.assertEqual(self.send(legacy, self.forbidden), "BLOCKED")
        self.assertEqual(self.enqueue("new-event", "report_status"), "CMD-20990101-702")
