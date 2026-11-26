import concurrent.futures
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import UTC, datetime
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
        self.forbidden = Mock()
        self.addCleanup(self.forbidden.assert_not_called)

    def restart(self):
        return IntegrationAdapter(self.path, now=lambda: datetime(2026, 11, 26, tzinfo=UTC))

    def send(self, task, transport, resources=Resources(10, 2)):
        return self.adapter.dispatch_mock(task, transport, sender_id=OWNER_ID,
                                          sender_login=OWNER_LOGIN, resources=resources)

    def comment(self, body, *, identifier=20, actor=OWNER_ID, login=OWNER_LOGIN):
        return Comment(REPOSITORY, CONTROL_PR, identifier, actor, login, body)

    def receipt(self, body, identifier=21):
        return self.adapter.receive_mock(self.comment(body, identifier=identifier, actor=101,
                                                      login="fixture-integration"), integration_id=101)

    def test_v1_roundtrip_memory_via_integration_and_review_dedupe(self):
        task = self.adapter.enqueue("fixture-1", "check_memory_index")
        self.assertEqual(task, "CMD-20261126-001")
        self.assertEqual(self.restart().enqueue("fixture-1", "check_memory_index"), task)

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
        review = self.adapter.enqueue("review-1", "record_review", ref=task)
        self.assertEqual(self.adapter.enqueue("review-1", "record_review", ref=task), review)
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.adapter.enqueue("another-review", "record_review", ref=task)

        def publish_review(body):
            message = parse(body)
            self.assertEqual((message.kind, message.fields["ref"]), ("REVIEW", task))
            return self.comment(body, identifier=23)

        self.assertEqual(self.send(review, publish_review), "SENT")
        with self.assertRaisesRegex(ValueError, "different intent"):
            self.adapter.enqueue("fixture-1", "report_status")

    def test_resource_unknown_reserve_and_author_gate(self):
        task = self.adapter.enqueue("one", "report_status")
        for resources in (None, Resources(3, 2), Resources(5, -1)):
            self.assertEqual(self.send(task, self.forbidden, resources), "BLOCKED")
        for action in ("editor", "merge_pr", "increase_budget", "policy_change"):
            reserved = self.adapter.enqueue(action, action)
            self.assertEqual(self.send(reserved, self.forbidden), "WAITING_AUTHOR")
        self.assertEqual(self.adapter.dispatch_real(task), "BLOCKED")
        self.assertEqual(self.adapter.dispatch_real(task, enabled=True), "BLOCKED")
        self.assertEqual(self.adapter.dispatch_mock(task, self.forbidden, sender_id=99,
                                                   sender_login=OWNER_LOGIN), "BLOCKED")

    def test_unclear_delivery_no_retry_and_exact_reconciliation(self):
        task = self.adapter.enqueue("event", "report_status")
        posted = []

        def publish(body):
            posted.append(self.comment(body))
            raise TimeoutError()

        self.assertEqual(self.send(task, publish), "UNCONFIRMED")
        self.adapter = self.restart()
        self.assertEqual(self.send(task, self.forbidden), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, []), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, [posted[0], replace(posted[0], id=21)]), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, [replace(posted[0], pr=4)]), "UNCONFIRMED")
        self.assertEqual(self.adapter.reconcile_mock(task, posted), "SENT")

    def test_receipts_cannot_schedule_and_shared_actor_cannot_confirm(self):
        task = self.adapter.enqueue("event", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        ack = f"AI-INTEGRATION-ACK\nid: {task}\nstatus: ACCEPTED\nreason: fixture"
        self.assertEqual(self.adapter.receive_mock(self.comment(ack), integration_id=OWNER_ID), "UNAUTHENTICATED")
        self.assertEqual(self.adapter.receive_mock(self.comment(ack), integration_id=101), "UNAUTHENTICATED")
        for body in ("ordinary comment", "AI-INTEGRATION-REVIEW\ninvalid", "AI-INTEGRATION-COMMAND\ninvalid"):
            self.assertEqual(self.receipt(body), "IGNORED")
        self.assertEqual(self.receipt(ack, 19), "IGNORED")
        self.assertEqual(self.adapter.status(task), "SENT")

    def test_concurrent_send_claim_persists_before_effect(self):
        task = self.adapter.enqueue("event", "report_status")
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
        second = self.adapter.enqueue("event-2", "check_memory_index")
        self.assertEqual(self.send(second, self.forbidden, Resources(4, 2)), "BLOCKED")

    def test_legacy_result_can_complete_even_if_ack_arrives_late(self):
        task = self.adapter.enqueue("event", "report_status")
        self.assertEqual(self.send(task, self.comment), "SENT")
        body = (f"AI-INTEGRATION-RESULT\nprotocol: 1.0\nfrom: Integration\nto: ChatGPT"
                f"\ncommand_id: {task}\nstatus: DONE\nevidence: synthetic")
        self.assertEqual(self.receipt(body), "DONE")
        self.assertEqual(self.receipt(f"AI-INTEGRATION-ACK\nid: {task}\nstatus: ACCEPTED\nreason: fixture", 22),
                         "IGNORED")

    def test_process_exit_during_send_keeps_durable_claim(self):
        task = self.adapter.enqueue("event", "report_status")
        program = """
import os, sys
from orchestrator.integration_adapter import IntegrationAdapter, OWNER_ID, OWNER_LOGIN, Resources
IntegrationAdapter(sys.argv[1]).dispatch_mock(sys.argv[2], lambda _: os._exit(23),
    sender_id=OWNER_ID, sender_login=OWNER_LOGIN, resources=Resources(10, 2))
"""
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.path), task], timeout=10)
        self.assertEqual(child.returncode, 23)
        self.adapter = self.restart()
        self.assertEqual(self.send(task, self.forbidden), "SENDING")
        self.assertEqual(self.adapter.reconcile_mock(task, []), "SENDING")
