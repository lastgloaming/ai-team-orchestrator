import hashlib
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from orchestrator.live_approval import binding_bytes, verify_snapshot
from orchestrator.live_scheduling import parse_approval
from test_live_scheduling import authorization


class ApprovalSignatureTests(unittest.TestCase):
    def test_signature_covers_complete_snapshot_and_rejects_key_or_configuration_replacement(self):
        approved = authorization(("1" * 40, "2" * 40))
        original = binding_bytes(approved)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            secret = root / "ephemeral-test-key"
            public = root / "test-key.pub"
            message = root / "message"
            message.write_bytes(original)
            subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(secret)], check=True)
            subprocess.run(["openssl", "pkey", "-in", str(secret), "-pubout", "-out", str(public)], check=True)
            sig = subprocess.check_output(["openssl", "pkeyutl", "-sign", "-inkey", str(secret),
                                           "-rawin", "-in", str(message)])
            key = public.read_bytes()
        pin = hashlib.sha256(key).hexdigest()
        decoded = verify_snapshot(original, sig, key, pin)
        self.assertEqual(parse_approval(decoded), approved)
        edits = (
            {"code_sha": "3" * 40}, {"ledger_id": 91}, {"anchors": ("3" * 40, "2" * 40)},
            {"request_sha256": "0" * 64},
            {"limits": replace(approved.execution.limits, daily_budget=1_000_000)},
            {"limits": replace(approved.execution.limits, input_ceiling=9)},
        )
        for edit in edits:
            changed = replace(approved, execution=replace(approved.execution, **edit))
            with self.subTest(edit=edit), self.assertRaisesRegex(ValueError, "signature"):
                verify_snapshot(binding_bytes(changed), sig, key, pin)
        with self.assertRaisesRegex(ValueError, "trust root"):
            verify_snapshot(original, sig, key, "0" * 64)
