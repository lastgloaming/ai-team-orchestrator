"""Persist bounded model advice for eligible synthetic events via a Mock transport."""

import os
from functools import partial

from .core import transaction
from .deepseek import Advisor, BudgetExceeded, InvalidResponse
from .scheduling_ledger import SCHEMA, ModelOutcome, ModelStatus
from .state import StateConflict

model_transaction = partial(transaction, schema=SCHEMA)
DENIAL_REASONS = {
    "period budget or call limit": "budget_exhausted",
    "concurrency limit": "concurrency_limit",
    "usage requires reconciliation": "reconciliation_required",
    "emergency stop": "model_stopped",
}


def enabled():
    return os.environ.get("ORCHESTRATOR_MODEL_SCHEDULING_ENABLED") == "1"


class MockScheduler:
    def __init__(self, state, limits, mock_transport):
        if (limits.currency != "CNY" or limits.retries != 0 or limits.concurrent != 1
                or max(limits.daily_budget, limits.monthly_budget) > 2_200_000_000
                or limits.daily_budget > limits.monthly_budget or limits.max_output > 64
                or (limits.input_rate, limits.output_rate) != (2000, 8000)):
            raise ValueError("Mock scheduling requires CNY peak rates, bounded budget, concurrency 1 and no retries")
        self.state = state
        self.limits = limits
        self.mock_transport = mock_transport

    @staticmethod
    def stopped():
        return (os.environ.get("ORCHESTRATOR_MODEL_STOP") == "1"
                or os.environ.get("ORCHESTRATOR_STOP") == "1")

    def _advisor(self, path):
        return Advisor(path, self.limits, self.mock_transport,
                       stopped=self.stopped, ledger_transaction=model_transaction)

    def __call__(self, event_id):
        if not enabled():
            return ModelOutcome(ModelStatus.DISABLED)

        def reserve(path):
            with transaction(path, schema="") as db:
                has_eligibility = db.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name='auto_model_eligibility' AND type='table'"
                ).fetchone()
                if not has_eligibility or not db.execute(
                    """SELECT 1 FROM auto_model_eligibility JOIN auto_inbox ON id=event_id
                       WHERE event_id=? AND consumed=1""", (event_id,)
                ).fetchone():
                    return None, ModelOutcome(ModelStatus.NOT_ELIGIBLE)
                previous = db.execute(
                    "SELECT status,reason,suggestion FROM auto_model_attempt WHERE event_id=?", (event_id,)
                ).fetchone()
                if previous:
                    return None, ModelOutcome(ModelStatus(previous[0]), *previous[1:])
            try:
                reservation = self._advisor(path)._reserve()
            except BudgetExceeded as error:
                reason = DENIAL_REASONS.get(str(error))
                if reason is None:
                    raise
                status = ModelStatus.STOPPED if reason == "model_stopped" else ModelStatus.BLOCKED
                with model_transaction(path) as db:
                    db.execute("INSERT INTO auto_model_attempt VALUES (?, NULL, ?, ?, NULL)",
                               (event_id, status, reason))
                return None, ModelOutcome(status, reason)
            with model_transaction(path) as db:
                db.execute("INSERT INTO auto_model_attempt VALUES (?, ?, 'RESERVED', NULL, NULL)",
                           (event_id, reservation))
            return reservation, ModelOutcome(ModelStatus.RESERVED)

        reservation, outcome = self.state.update(reserve)
        if reservation is None:
            return outcome

        def settle(path, response):
            advisor = self._advisor(path)
            suggestion = None
            if response is None:
                advisor.reconcile_terminated_attempt(reservation)
            else:
                try:
                    suggestion = advisor._account_and_validate(reservation, response)
                except (InvalidResponse, TypeError, ValueError, KeyError, IndexError):
                    advisor.reconcile_terminated_attempt(reservation)
            with model_transaction(path) as db:
                usage = db.execute("SELECT state FROM usage WHERE id=?", (reservation,)).fetchone()[0]
                final = ModelStatus.REJECTED
                if suggestion:
                    final = ModelStatus.ADVISED
                elif usage == "uncertain":
                    final = ModelStatus.UNCERTAIN
                updated = db.execute(
                    """UPDATE auto_model_attempt SET status=?,suggestion=?
                       WHERE event_id=? AND reservation_id=? AND status='RESERVED'""",
                    (final, suggestion, event_id, reservation),
                ).rowcount
                if not updated:
                    raise StateConflict("model attempt could not be settled")
            return ModelOutcome(final, suggestion=suggestion)

        # Git conflict recovery replays only local SQLite mutations.
        response = None
        if enabled() and not self.stopped():
            try:
                response = self.mock_transport(self._advisor(None).request("ACCEPTED"), 10)
            except Exception:
                pass
        return self.state.update(lambda path: settle(path, response))
