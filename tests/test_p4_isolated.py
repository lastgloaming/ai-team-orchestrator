import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from p4_isolated import advisor
from orchestrator.core import transaction


class IsolatedP4Tests(unittest.TestCase):
    def test_snapshot_cross_process_mock_loop(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "snapshot.sqlite"
            for phase in ("prepare", "restore"):
                subprocess.run([sys.executable, "-B", "-m", "tests.p4_isolated", phase, str(path)],
                               check=True, timeout=15,
                               env={**os.environ, "ORCHESTRATOR_LIVE_APPROVED": "0"})

    def test_p4_timeout_is_not_retried(self):
        attempts = []

        def timeout(*_):
            attempts.append(1)
            raise TimeoutError()

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "snapshot.sqlite"
            with self.assertRaisesRegex(RuntimeError, "model request failed"):
                advisor(path, timeout).suggest("DONE")
            self.assertEqual(attempts, [1])
            with transaction(path) as db:
                self.assertEqual(db.execute("SELECT state,amount FROM usage").fetchall(), [("uncertain", 384000)])
