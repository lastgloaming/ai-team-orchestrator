"""Offline tests through the exact-proof observation and staging entry points."""
import hashlib
import itertools
import json
import tempfile
import unittest
from pathlib import Path

from orchestrator import control_rc1
from orchestrator.integration_adapter import parse_receipt
from orchestrator.v1 import Queue, Task


class ControlFixture(unittest.TestCase):
    def setUp(self):
        self.new_fixture()

    def new_fixture(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "queue"
        self.config = dict(control_rc1_repository_id=21, verified_rc1_observations={},
                           stop=False, authorizations={"m": "auth"})
        self.queue().add([Task("m", "example/alpha", "memory", "auth", 30, 1)])
        staged = self.stage()
        self.assertEqual(staged, self.stage())
        self.command = staged["command_id"]
        self.pub = dict(task_id="m", repository_id=21, pr=3, authorization="auth",
                        actor_id=338499555, comment=30, kind="publication", created_at=100, updated_at=100,
                        app_id=1144995, app_slug="chatgpt-codex-connector",
                        body=(f"AI-INTEGRATION-COMMAND\nid: {self.command}\nfrom: ChatGPT\ntarget: memory\n"
                              "scope: coordination\ntask: Report synthetic status for the authorized task."))

    def queue(self):
        return Queue(self.path, now=lambda: 100)

    def stage(self):
        return self.queue().stage_rc1(self.config, "m")

    def observe(self, packet):
        self.config["verified_rc1_observations"][str(packet["comment"])] = hashlib.sha256(
            json.dumps(packet, sort_keys=True).encode()).hexdigest()
        return self.queue().observe_rc1(self.config, packet)["status"]

    def reply(self, number=2, status="DONE", group=1, part="1/1", time=102):
        return dict(self.pub, comment=number, created_at=time, updated_at=time, kind="receipt", app_id=1210556,
                    app_slug="cursor", body=(f"AI-INTEGRATION-RESULT\nid: {self.command}\n"
                    f"receipt_id: RESULT-{self.command}-{group}\nin_reply_to: 30\nstatus: {status}\n"
                    f"part: {part}\nevidence:\n- [I] fixture: opaque\n- [S] fixture\n- [C] fixture"))

    def test_parser_and_namespace(self):
        body = self.reply()["body"]
        self.assertEqual(control_rc1.parse(body)[1], self.command)
        for invalid in (body.replace("RESULT-CMD", "RESULT:CMD"), body + "\nstatus: DONE",
                        body + "\n status: DONE", body + "\n- [X] bad", body + "\nunknown: field",
                        body.replace("evidence:", "reason:")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                control_rc1.parse(invalid)
        with self.assertRaises(ValueError):
            parse_receipt(f"AI-INTEGRATION-ACK\nid: {self.command}\nstatus: ACCEPTED\nreason: test")

    def test_provenance_and_authority(self):
        with self.assertRaises(ValueError):
            self.queue().observe_rc1(self.config, self.pub)
        for change in (dict(app_id=1144995.0), dict(app_slug="cursor"), dict(actor_id=1), dict(pr=4)):
            with self.assertRaises(ValueError):
                self.observe(dict(self.pub, **change))
        self.assertEqual(self.observe(self.pub), "SENT")
        p = self.reply()
        for change in (dict(app_id=1144995), dict(app_slug="other"),
                       dict(body=p["body"].replace("in_reply_to: 30", "in_reply_to: 31"))):
            with self.assertRaises(ValueError):
                self.observe(dict(p, **change))
        self.config["authorizations"].clear()
        with self.assertRaises(ValueError):
            self.observe(p)

    def test_multipart_reopen_and_older_progress(self):
        self.observe(self.pub)
        second = self.reply(part="2/2", time=110)
        self.assertEqual(self.observe(second), "INCOMPLETE")
        self.assertEqual(self.observe(second), "DUPLICATE_PART")
        self.assertEqual(self.observe(self.reply(1, part="1/2", time=109)), "RECORDED")
        self.assertEqual(self.observe(self.reply(99, "REVIEW", 2)), "RECORDED")
        self.assertEqual(self.stage()["delivery"], "DONE")
        self.assertEqual(self.queue().status()["m"], "READY")

    def test_conflicting_terminals(self):
        self.observe(self.pub)
        self.observe(self.reply())
        self.assertEqual(self.observe(self.reply(3, "BLOCKED", 2)), "AMBIGUOUS")
        self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")

    def test_edited_publication(self):
        self.observe(self.pub)
        self.assertEqual(self.observe(dict(self.pub, body="edited")), "AMBIGUOUS")

    def test_reused_comment_even_when_edit_malformed(self):
        self.observe(self.pub)
        self.observe(self.reply())
        self.assertEqual(self.observe(dict(self.reply(), body="edited")), "AMBIGUOUS")

    def test_part_mismatch(self):
        self.observe(self.pub)
        self.observe(self.reply(part="1/2"))
        self.assertEqual(self.observe(self.reply(3, part="2/3")), "AMBIGUOUS")

    def test_uncertain_chronology(self):
        self.observe(self.pub)
        self.assertEqual(self.observe(self.reply(time=100)), "AMBIGUOUS")
        self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")

    def test_equal_times_between_distinct_statuses(self):
        self.observe(self.pub)
        self.observe(self.reply(status="PARTIAL"))
        self.assertEqual(self.observe(self.reply(3, "DONE", 2)), "AMBIGUOUS")

    def test_reposted_part_is_idempotent_and_its_id_is_bound(self):
        self.observe(self.pub)
        self.observe(self.reply())
        self.assertEqual(self.observe(self.reply(4)), "DUPLICATE_PART")
        self.assertEqual(self.observe(self.reply(4, "REVIEW", 3)), "AMBIGUOUS")

    def test_first_seen_edited_or_unknown_timestamp(self):
        for kind, metadata in itertools.product(("publication", "receipt"), (
                {"updated_at": 120}, {"created_at": None}, {"updated_at": None}, {"updated_at": True})):
            with self.subTest(kind=kind, metadata=metadata):
                self.new_fixture()
                packet = self.pub
                if kind == "receipt":
                    self.observe(self.pub)
                    packet = self.reply()
                self.assertEqual(self.observe(dict(packet, **metadata)), "AMBIGUOUS")
                self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")

    def test_duplicate_chronology_all_page_orders(self):
        for order in itertools.permutations(range(3)):
            with self.subTest(order=order):
                self.new_fixture()
                self.observe(self.pub)
                packets = [self.reply(2, time=102), self.reply(3, time=110),
                           self.reply(4, status="PARTIAL", group=2, time=105)]
                for index in order:
                    self.observe(packets[index])
                self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")

    def test_timestamps_participate_in_observer_commitment(self):
        self.observe(self.pub)
        for field in ("created_at", "updated_at"):
            with self.assertRaisesRegex(ValueError, "verified GitHub metadata"):
                self.queue().observe_rc1(self.config, dict(self.pub, **{field: 101}))
        self.assertEqual(self.stage()["delivery"], "SENT")

    def test_late_second_publication_after_receipt(self):
        self.observe(self.pub)
        self.observe(self.reply())
        second = dict(self.pub, comment=99, created_at=101, updated_at=101)
        self.assertEqual(self.observe(second), "AMBIGUOUS")
        self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")

    def test_migration_requires_exact_original_receipt_comment(self):
        self.observe(self.pub)
        original = self.reply(31, time=102)
        self.observe(original)
        repost = self.reply(32, time=110)
        self.observe(repost)
        # Recreate R1 storage after a repost attestation, with the pinned writer's
        # original part row but no authenticated timestamps for that original.
        with self.queue()._db() as db:
            db.execute("DELETE FROM rc1_unedited WHERE comment=31")
            db.execute("DELETE FROM rc1_seen WHERE comment=31")
        self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")
        partial = self.reply(33, status="PARTIAL", group=2, time=105)
        for packet in (self.pub, repost, partial, original):
            self.assertEqual(self.observe(packet), "AMBIGUOUS")
            self.assertEqual(self.stage()["delivery"], "AMBIGUOUS")
