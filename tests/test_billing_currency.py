import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from orchestrator.deepseek import Advisor, BudgetExceeded, Limits
from orchestrator.p4 import CNY_TEST_BUDGET, configured_model_limits


class BillingCurrencyTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "ledger.sqlite"
        self.cny = Limits("deepseek-flash", 2000, 8000, 1_099_744, 64,
                          CNY_TEST_BUDGET, CNY_TEST_BUDGET, 1, 1, retries=0, currency="CNY")

    def legacy_ledger(self, row=True):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""CREATE TABLE usage (
                id INTEGER PRIMARY KEY, day TEXT NOT NULL, model TEXT NOT NULL,
                amount INTEGER NOT NULL, state TEXT NOT NULL,
                input_tokens INTEGER, output_tokens INTEGER, attempts INTEGER NOT NULL DEFAULT 0
            )""")
            if row:
                db.execute("INSERT INTO usage VALUES (1,'2020-01-01','mock',900,'counted',1,1,1)")

    def rows(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT currency,amount,state,typeof(amount) FROM usage ORDER BY id").fetchall()

    def no_transport(self, *_):
        self.fail("model transport was invoked")

    def test_exact_cny_reservation_and_cache_independent_settlement(self):
        def mock(request, timeout):
            self.assertEqual(request["max_tokens"], 64)
            self.assertEqual(self.rows(), [("CNY", 2_200_000_000, "active", "integer")])
            return {
                "usage": {"prompt_tokens": 32, "completion_tokens": 8, "prompt_cache_hit_tokens": 32},
                "choices": [{"finish_reason": "stop", "message": {"content": '{"suggestion":"inspect_status"}'}}],
            }

        self.assertEqual(Advisor(self.path, self.cny, mock).suggest("DONE"), "inspect_status")
        self.assertEqual(self.rows(), [("CNY", 128_000, "counted", "integer")])

    def test_one_more_input_token_exceeds_cny_budget(self):
        limits = replace(self.cny, input_ceiling=self.cny.input_ceiling + 1)
        with self.assertRaisesRegex(BudgetExceeded, "period budget"):
            Advisor(self.path, limits, self.no_transport).suggest("DONE")
        self.assertEqual(self.rows(), [])

    def test_old_rows_are_usd_without_repricing_or_cross_currency_spend(self):
        self.legacy_ledger()
        with self.assertRaisesRegex(BudgetExceeded, "billing currency"):
            Advisor(self.path, self.cny, self.no_transport).suggest("DONE")
        usd = Limits("deepseek-flash", 300, 1200, 10, 1, 10_000, 10_000, 1, 1)
        reservation = Advisor(self.path, usd, self.no_transport)._reserve()
        self.assertEqual(reservation, 2)
        self.assertEqual(self.rows(), [("USD", 900, "counted", "integer"),
                                      ("USD", 4200, "active", "integer")])

    def test_empty_old_ledger_allows_cny_and_then_rejects_usd(self):
        self.legacy_ledger(row=False)
        reservation = Advisor(self.path, self.cny, self.no_transport)._reserve()
        usd = Advisor(self.path, replace(self.cny, currency="USD"), self.no_transport)
        with self.assertRaisesRegex(BudgetExceeded, "billing currency"):
            usd.reconcile_terminated_attempt(reservation)
        with self.assertRaisesRegex(BudgetExceeded, "billing currency"):
            usd.suggest("DONE")
        self.assertEqual(self.rows(), [("CNY", CNY_TEST_BUDGET, "active", "integer")])

    def test_p4_currency_switch_selects_distinct_budget_variable_and_validates_cap(self):
        env = {
            "ORCHESTRATOR_P4_INPUT_TOKEN_BOUND": "1024",
            "ORCHESTRATOR_P4_BUDGET_NANODOLLARS": "400000",
            "ORCHESTRATOR_P4_BUDGET_NANOYUAN": "2200000000",
        }
        with patch.dict(os.environ, env, clear=True):
            usd = configured_model_limits()
            self.assertEqual((usd.currency, usd.daily_budget), ("USD", 400_000))
            os.environ["ORCHESTRATOR_P4_BILLING_CURRENCY"] = "CNY"
            limits = configured_model_limits()
            self.assertEqual((limits.currency, limits.input_rate, limits.output_rate, limits.daily_budget),
                             ("CNY", 2000, 8000, CNY_TEST_BUDGET))
            self.assertEqual((limits.max_output, limits.retries, limits.concurrent, limits.daily_calls,
                              limits.monthly_calls), (64, 0, 1, 1, 1))
            os.environ["ORCHESTRATOR_P4_BUDGET_NANOYUAN"] = "2200000001"
            with self.assertRaisesRegex(ValueError, "exceeds"):
                configured_model_limits()
            os.environ["ORCHESTRATOR_P4_BILLING_CURRENCY"] = "EUR"
            with self.assertRaisesRegex(ValueError, "billing currency"):
                configured_model_limits()

    def test_limits_reject_float_money(self):
        with self.assertRaises(ValueError):
            replace(self.cny, daily_budget=2_200_000_000.0)
