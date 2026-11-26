"""Persist bounded model advice for synthetic events using an injected Mock transport.

The hosted comment workflow does not instantiate this adapter.
"""

import os
from functools import partial

from .core import USAGE_SCHEMA, transaction
from .deepseek import Advisor, BudgetExceeded, InvalidResponse
from .state import StateConflict

SCHEDULING_SCHEMA = USAGE_SCHEMA + """
CREATE TABLE IF NOT EXISTS auto_model_attempt (
    event_id INTEGER PRIMARY KEY REFERENCES auto_inbox(id),
    reservation_id INTEGER NOT NULL UNIQUE REFERENCES usage(id),
    status TEXT NOT NULL CHECK(status IN ('RESERVED', 'UNCERTAIN', 'REJECTED', 'ADVISED')),
    suggestion TEXT CHECK(suggestion IN ('inspect_status', 'check_blockers', 'escalate')),
    CHECK((status = 'ADVISED') = (suggestion IS NOT NULL))
);
"""

model_transaction = partial(transaction, schema=SCHEDULING_SCHEMA)


class MockScheduler:
    def __init__(self, state, limits, mock_transport):
        if limits.retries != 0:
            raise ValueError("model advice has no retries")
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
        if self.stopped():
            return "STOPPED"

        def reserve(path):
            advisor = self._advisor(path)
            with model_transaction(path) as db:
                if not db.execute("SELECT 1 FROM auto_inbox WHERE id=? AND consumed=1", (event_id,)).fetchone():
                    raise StateConflict("model advice requires a committed, consumed synthetic event")
                advisor._require_currency(db)
                previous = db.execute("SELECT status FROM auto_model_attempt WHERE event_id=?", (event_id,)).fetchone()
                if previous:
                    return None, previous[0]
            # These local transactions are published together in one paired Git commit.
            reservation = advisor._reserve()
            with model_transaction(path) as db:
                db.execute("INSERT INTO auto_model_attempt VALUES (?, ?, 'RESERVED', NULL)",
                           (event_id, reservation))
            return reservation, "RESERVED"

        try:
            reservation, status = self.state.update(reserve)
        except BudgetExceeded:
            return "BLOCKED"
        if reservation is None:
            return status

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
                final = "ADVISED" if suggestion else ("UNCERTAIN" if usage == "uncertain" else "REJECTED")
                updated = db.execute(
                    """UPDATE auto_model_attempt SET status=?,suggestion=?
                       WHERE event_id=? AND reservation_id=? AND status='RESERVED'""",
                    (final, suggestion, event_id, reservation),
                ).rowcount
                if not updated:
                    raise StateConflict("model attempt could not be settled")
            return final

        # Never call the transport from a mutation: Git conflicts replay only the
        # reservation or settlement. The attempt stays reserved after process loss.
        response = None
        if not self.stopped():
            try:
                response = self.mock_transport(self._advisor(None).request("ACCEPTED"), 10)
            except Exception:
                pass
        return self.state.update(lambda path: settle(path, response))
