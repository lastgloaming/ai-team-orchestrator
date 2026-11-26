"""Budgeted advice for synthetic or explicitly approved structured metadata."""

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime

from .core import transaction

STATES = {"ACCEPTED", "DONE", "BLOCKED", "WAITING_AUTHOR", "REVIEWED"}
SUGGESTIONS = {"inspect_status", "check_blockers", "escalate"}
ENDPOINT = "https://api.deepseek.com/chat/completions"


class BudgetExceeded(RuntimeError):
    pass


class InvalidResponse(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def live_transport(request, timeout):
    if os.environ.get("ORCHESTRATOR_STOP") == "1":
        raise BudgetExceeded("emergency stop")
    if os.environ.get("ORCHESTRATOR_LIVE_APPROVED") != "1":
        raise RuntimeError("live transport requires explicit approval")
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("missing DEEPSEEK_API_KEY")
    req = urllib.request.Request(
        ENDPOINT, data=json.dumps(request).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=timeout) as response:
        data = response.read(65537)
        if len(data) > 65536:
            raise InvalidResponse("response too large")
        return json.loads(data)


@dataclass(frozen=True)
class Limits:
    model: str
    input_rate: int  # integer nano-units of currency per token, rounded up
    output_rate: int
    input_ceiling: int
    max_output: int
    daily_budget: int
    monthly_budget: int
    daily_calls: int
    monthly_calls: int
    concurrent: int = 1
    retries: int = 1
    currency: str = "USD"

    def __post_init__(self):
        for name, value in vars(self).items():
            if name == "model":
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(name)
            elif name == "currency":
                if value not in ("USD", "CNY"):
                    raise ValueError(name)
            elif type(value) is not int or not (0 if name == "retries" else 1) <= value <= 10**12:
                raise ValueError(name)
        if self.retries > 3:
            raise ValueError("retries")
        if self.input_ceiling * self.input_rate + self.max_output * self.output_rate >= 2**63:
            raise ValueError("reservation exceeds SQLite integer range")


class Advisor:
    def __init__(self, path, limits, transport, *, stopped=lambda: False, pause=time.sleep,
                 now=lambda: datetime.now(UTC), ledger_transaction=transaction):
        self.path = path
        self.limits = limits
        self.transport = transport
        self.stopped = stopped
        self.pause = pause
        self.now = now
        self.ledger_transaction = ledger_transaction

    def _require_currency(self, db):
        # Schema v1 model rows were exclusively denominated in USD.
        if "currency" not in {column[1] for column in db.execute("PRAGMA table_info(usage)")}:
            db.execute("ALTER TABLE usage ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD' "
                       "CHECK(currency IN ('USD', 'CNY'))")
        if db.execute("SELECT 1 FROM usage WHERE currency IS NOT ? LIMIT 1",
                      (self.limits.currency,)).fetchone():
            raise BudgetExceeded("billing currency does not match existing usage ledger")

    def _reserve(self):
        limits = self.limits
        amount = limits.input_ceiling * limits.input_rate + limits.max_output * limits.output_rate
        with self.ledger_transaction(self.path) as db:
            clock = self.now()
            if clock.utcoffset() is None:
                raise ValueError("budget clock must have a timezone")
            day = clock.astimezone(UTC).date().isoformat()
            if self.stopped() or os.environ.get("ORCHESTRATOR_STOP") == "1":
                raise BudgetExceeded("emergency stop")
            self._require_currency(db)
            if db.execute("SELECT 1 FROM usage WHERE state='overrun' LIMIT 1").fetchone():
                raise BudgetExceeded("usage requires reconciliation")
            active = db.execute("SELECT count(*) FROM usage WHERE state='active'").fetchone()[0]
            if active >= limits.concurrent:
                raise BudgetExceeded("concurrency limit")
            for period, budget, calls in (
                (day, limits.daily_budget, limits.daily_calls),
                (day[:7], limits.monthly_budget, limits.monthly_calls),
            ):
                used, count = db.execute(
                    "SELECT COALESCE(sum(amount),0), count(*) FROM usage WHERE day LIKE ?",
                    (period + "%",),
                ).fetchone()
                if used + amount > budget or count >= calls:
                    raise BudgetExceeded("period budget or call limit")
            return db.execute(
                "INSERT INTO usage (day,model,amount,state,attempts,currency) VALUES (?,?,?,'active',1,?)",
                (day, limits.model, amount, limits.currency),
            ).lastrowid

    def reconcile_terminated_attempt(self, reservation):
        """Release the slot after an operator has verified the worker terminated."""
        with self.ledger_transaction(self.path) as db:
            self._require_currency(db)
            return bool(db.execute(
                "UPDATE usage SET state='uncertain' WHERE id=? AND state='active'",
                (reservation,),
            ).rowcount)

    def request(self, status):
        if type(status) is not str or status not in STATES:
            raise ValueError("use an approved status code")
        return {
            "model": self.limits.model, "max_tokens": self.limits.max_output,
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "messages": [
                {"role": "system", "content": "Return a JSON object with only suggestion. "
                 'Choose inspect_status, check_blockers, or escalate. Example: {"suggestion":"inspect_status"}.'},
                {"role": "user", "content": status},
            ],
        }

    def suggest(self, status):
        request = self.request(status)
        for attempt in range(self.limits.retries + 1):
            reservation = self._reserve()
            try:
                data = self.transport(request, 10)
                return self._account_and_validate(reservation, data)
            except (TimeoutError, urllib.error.URLError) as error:
                retryable = not isinstance(error, urllib.error.HTTPError) or error.code in {
                    429, 500, 502, 503, 504
                }
                if not retryable or attempt == self.limits.retries:
                    raise RuntimeError("model request failed") from None
            except (TypeError, ValueError, KeyError, IndexError):
                raise InvalidResponse("malformed model response") from None
            finally:
                # An ambiguous attempt keeps its full reserved amount.
                self.reconcile_terminated_attempt(reservation)
            self.pause(min(2**attempt, 8))

    def _account_and_validate(self, reservation, data):
        usage = data["usage"]
        incoming, outgoing = usage["prompt_tokens"], usage["completion_tokens"]
        if (type(incoming) is not int or type(outgoing) is not int or
                not 0 <= incoming <= self.limits.input_ceiling or
                not 0 <= outgoing <= self.limits.max_output):
            # Stop subsequent calls pending operator reconciliation.
            with self.ledger_transaction(self.path) as db:
                self._require_currency(db)
                db.execute("UPDATE usage SET state='overrun' WHERE id=? AND state='active'", (reservation,))
            raise InvalidResponse("usage outside reserved ceiling")
        amount = incoming * self.limits.input_rate + outgoing * self.limits.output_rate
        with self.ledger_transaction(self.path) as db:
            self._require_currency(db)
            settled = db.execute("""
                UPDATE usage SET amount=?,state='counted',input_tokens=?,output_tokens=?
                WHERE id=? AND state='active'
                """, (amount, incoming, outgoing, reservation)).rowcount
            if not settled:
                raise InvalidResponse("attempt already reconciled")
        choice = data["choices"][0]
        if choice["finish_reason"] != "stop":
            raise InvalidResponse("incomplete model response")
        advice = json.loads(choice["message"]["content"], object_pairs_hook=_unique_object)
        if (not isinstance(advice, dict) or advice.keys() != {"suggestion"} or
                type(advice["suggestion"]) is not str or advice["suggestion"] not in SUGGESTIONS):
            raise InvalidResponse("unrecognized advice")
        return advice["suggestion"]


def _unique_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise InvalidResponse("duplicate output keys")
    return result
