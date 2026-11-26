"""Stage comments for one explicitly selected repository and pull request per ledger directory.

The caller supplies a read-only transport. Exact-comment ingestion needs no pagination.
Recovery checks only page one against a trusted checkpoint and leaves it in place.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from dataclasses import dataclass

API = "https://api.github.com"
REPOSITORY = re.compile(r"[A-Za-z0-9-]+/[A-Za-z0-9_.-]+")
TIER_RE = re.compile(r"^tier-(\d{6})\.jsonl$")
NO_CHECKPOINT = ("UNCONFIRMED: no trusted checkpoint. Initialization needs an author-ruled record naming per_page, "
                 "a page number P and the exact ordered comment-id list (sha256) of that full page; this module never "
                 "scans full history to create one.")


class ScopeError(Exception):
    pass


@dataclass
class Response:
    status: int
    headers: dict
    json: object


class GuardedTransport:
    """GET only; URL must be a target PR comment listing page or a single issue comment."""

    def __init__(self, inner, repo, pr):
        if (not isinstance(repo, str) or not REPOSITORY.fullmatch(repo)
                or repo.split("/")[1] in {".", ".."} or type(pr) is not int or pr <= 0):
            raise ScopeError("explicit owner/repository and positive pull request number required")
        self._inner, self.repo, self.pr = inner, repo, pr
        base = re.escape(f"{API}/repos/{repo}/issues/")
        self._list = re.compile("^" + base + str(pr) + r"/comments\?per_page=([1-9][0-9]?|100)&page=([1-9][0-9]*)$")
        self._one = re.compile("^" + base + r"comments/([1-9][0-9]*)$")

    def check(self, url):
        m = self._list.fullmatch(url)
        if m:
            return "list", int(m.group(1)), int(m.group(2))
        m = self._one.fullmatch(url)
        if m:
            return "one", int(m.group(1)), None
        raise ScopeError("URL outside Control PR read scope: " + url)

    def get(self, url):
        self.check(url)
        return self._inner.get(url)

    def list_url(self, per_page, page):
        return f"{API}/repos/{self.repo}/issues/{self.pr}/comments?per_page={per_page}&page={page}"


_LINK_RE = re.compile(r'<([^>]*)>\s*;\s*rel="([^"]+)"')


def parse_link(header):
    out = {}
    if not isinstance(header, str):
        return out
    for entry in header.split(","):
        match = _LINK_RE.fullmatch(entry.strip())
        if not match:
            return {}
        url, rels = match.groups()
        tokens = rels.split()
        if not tokens:
            return {}
        for rel in tokens:
            if rel in out:
                raise ScopeError("duplicate Link rel " + rel)
            out[rel] = url
    return out


class TierError(Exception):
    pass


class Crash(Exception):
    """Test hook: simulated process death at a named point."""


def _h(rec):
    body = {k: rec[k] for k in ("seq", "type", "key", "data")}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _line(rec):
    rec = dict(rec)
    rec["h"] = _h(rec)
    return (json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n").encode()


class TieredLog:
    def __init__(self, directory, capacities=(1000,), crash_at=None):
        self.dir, self.capacities, self.crash_at = directory, tuple(capacities), crash_at
        os.makedirs(directory, exist_ok=True)

    # --- locking / layout
    def _lock(self):
        fd = os.open(os.path.join(self.dir, ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    def _unlock(self, fd):
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def _cap(self, n):
        return self.capacities[min(n - 1, len(self.capacities) - 1)]

    def _tiers(self):
        nums = sorted(int(m.group(1)) for f in os.listdir(self.dir) if (m := TIER_RE.match(f)))
        if nums and nums != list(range(1, len(nums) + 1)):
            raise TierError(f"tier numbering gap: {nums}")
        return nums

    def _path(self, n):
        return os.path.join(self.dir, f"tier-{n:06d}.jsonl")

    def _crash(self, point):
        if self.crash_at == point:
            raise Crash(point)

    # --- recovery + replay (caller holds lock)
    def _recover(self):
        for f in os.listdir(self.dir):
            if f.endswith(".tmp"):
                os.remove(os.path.join(self.dir, f))          # rotation never committed
        nums = self._tiers()
        if not nums:
            fd = os.open(self._path(1), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            os.fsync(fd)
            os.close(fd)
            dfd = os.open(self.dir, os.O_RDONLY)          # make the new tier's directory entry durable
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            nums = [1]
        records, last_seq, prev_bytes = [], 0, None
        tiers = []
        for n in nums:
            active = n == nums[-1]
            if not active and os.stat(self._path(n)).st_mode & 0o222:
                os.chmod(self._path(n), 0o444)                # finish interrupted seal
            with open(self._path(n), "rb") as fh:
                raw = fh.read()
            lines = raw.split(b"\n")
            tail = lines.pop()                                # b"" if newline-terminated
            if tail:
                if not active:
                    raise TierError(f"sealed tier {n} has torn line")
                with open(self._path(n), "r+b") as fh:        # unacknowledged torn write
                    fh.truncate(len(raw) - len(tail))
                    os.fsync(fh.fileno())
                raw = raw[: len(raw) - len(tail)]
            entries = 0
            for i, ln in enumerate(lines):
                try:
                    rec = json.loads(ln)
                    ok = rec.get("h") == _h(rec)
                except (ValueError, KeyError, TypeError):
                    ok = False
                if not ok:
                    raise TierError(f"tier {n} line {i + 1} corrupt")
                if rec["seq"] != last_seq + 1:
                    raise TierError(f"tier {n} seq {rec['seq']} after {last_seq}")
                last_seq = rec["seq"]
                if rec["type"] == "TIER":
                    if i != 0 or n == 1 or rec["data"]["prev"] != n - 1 or \
                            rec["data"]["prev_sha256"] != hashlib.sha256(prev_bytes).hexdigest():
                        raise TierError(f"tier {n} header does not seal tier {n - 1}")
                else:
                    if i == 0 and n > 1:
                        raise TierError(f"tier {n} missing header")
                    entries += 1
                    records.append(rec)
            tiers.append((n, entries))
            prev_bytes = raw
        return records, last_seq, tiers

    @staticmethod
    def _state(records):
        st = {"keys": {}, "checkpoint": None, "locks": [], "events": {}}
        for r in records:
            if r["key"] is not None:
                st["keys"][r["key"]] = r["data"]
            t = r["type"]
            if t == "EVENT":
                st["events"][r["data"]["id"]] = r["data"]
            elif t == "CHECKPOINT":
                st["checkpoint"] = r["data"]
            elif t == "LOCK":
                st["locks"].append(r["data"])
        return st

    def state(self):
        fd = self._lock()
        try:
            records, _, tiers = self._recover()
            st = self._state(records)
            st["tiers"] = tiers
            return st
        finally:
            self._unlock(fd)

    def _write_all(self, fd, data):
        """Write every byte (loop on short writes), verify the file grew by len(data), then fsync.
        On any failure the file is truncated back to its prior size and fsync'd, then the error is
        raised, so a short write never reports success and never leaves a partial record."""
        start = os.fstat(fd).st_size
        view, off = memoryview(data), 0
        try:
            while off < len(data):
                n = os.write(fd, view[off:])
                if not n or n < 0:
                    raise OSError("write returned no progress")
                off += n
            if os.fstat(fd).st_size != start + len(data):
                raise OSError("written size mismatch")
            os.fsync(fd)
        except BaseException:
            try:
                os.ftruncate(fd, start)
                os.fsync(fd)
            except OSError:
                pass                                       # recovery truncates the torn tail on next open
            raise

    def append(self, rtype, key, data):
        """-> NEW | DUPLICATE | CONFLICT. NEW is returned only after fsync."""
        fd = self._lock()
        try:
            records, last_seq, tiers = self._recover()
            st = self._state(records)
            if key is not None and key in st["keys"]:
                return "DUPLICATE" if st["keys"][key] == data else "CONFLICT"
            n, entries = tiers[-1]
            if entries >= self._cap(n):
                last_seq = self._rotate(n, last_seq, entries)
                n += 1
            line = _line({"seq": last_seq + 1, "type": rtype, "key": key, "data": data})
            wfd = os.open(self._path(n), os.O_WRONLY | os.O_APPEND)
            try:
                if self.crash_at == "torn_write":
                    os.write(wfd, line[: len(line) // 2])
                    os.fsync(wfd)
                    raise Crash("torn_write")
                self._write_all(wfd, line)
            finally:
                os.close(wfd)
            return "NEW"
        finally:
            self._unlock(fd)

    def _rotate(self, n, last_seq, entries):
        with open(self._path(n), "rb") as fh:
            sealed = fh.read()
        hdr = _line({"seq": last_seq + 1, "type": "TIER", "key": None,
                     "data": {"prev": n, "prev_sha256": hashlib.sha256(sealed).hexdigest(),
                              "prev_entries": entries}})
        tmp = self._path(n + 1) + ".tmp"
        tfd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            self._write_all(tfd, hdr)
        finally:
            os.close(tfd)
        self._crash("rotate_before_rename")
        os.rename(tmp, self._path(n + 1))
        dfd = os.open(self.dir, os.O_RDONLY)
        os.fsync(dfd)
        os.close(dfd)
        self._crash("rotate_before_seal")
        os.chmod(self._path(n), 0o444)
        return last_seq + 1



# --------------------------------------------------------------------------- receiving
def _event_data(meta):
    body = meta.get("body") or ""
    return {"id": meta["id"], "digest": hashlib.sha256(body.encode()).hexdigest(),
            "created_at": meta.get("created_at"), "updated_at": meta.get("updated_at")}


def _record(log, meta):
    d = _event_data(meta)
    res = log.append("EVENT", f"event:{d['id']}", d)
    if res == "CONFLICT":
        log.append("LOCK", f"lock:event:{d['id']}:{d['digest']}",
                   {"why": f"comment {d['id']} content/time differs from recorded"})
    return res


def on_event(t, log, comment_id):
    """-> NEW | DUPLICATE | UNCONFIRMED:<why>. A changed comment is a conflict -> UNCONFIRMED (LOCK)."""
    if type(comment_id) is not int or comment_id <= 0:
        return "UNCONFIRMED:event without a real comment id"
    url = f"{API}/repos/{t.repo}/issues/comments/{comment_id}"
    t.check(url)
    try:
        resp = t.get(url)
    except Exception as e:
        return f"UNCONFIRMED:read failure: {e}"
    meta = resp.json if resp.status == 200 and isinstance(resp.json, dict) else None
    if meta is None or type(meta.get("id")) is not int or meta["id"] != comment_id or meta.get("issue_url") != f"{API}/repos/{t.repo}/issues/{t.pr}":
        return "UNCONFIRMED:comment not readable or not on Control PR"
    res = _record(log, meta)
    return "UNCONFIRMED:conflict (LOCK)" if res == "CONFLICT" else res


def _ids_sha(ids):
    return hashlib.sha256(",".join(map(str, ids)).encode()).hexdigest()


def seed_checkpoint(log, per_page, page, ids, origin):
    """Record a trusted checkpoint supplied by the caller (e.g. an author-ruled record). Not derived here."""
    return log.append("CHECKPOINT", f"ckpt:{per_page}:{page}:{_ids_sha(ids)}",
                      {"per_page": per_page, "page": page, "ids_sha256": _ids_sha(ids),
                       "first_id": ids[0], "last_id": ids[-1], "origin": origin})


def catch_up(t, log, per_page=100):
    """Check a short first page with explicit last-page evidence and a trusted prefix."""
    st = log.state()
    new = []
    if st["locks"]:
        return "UNCONFIRMED", "conflict lock present: " + st["locks"][0]["why"], new
    ck = st["checkpoint"]
    if ck is None:
        return "UNCONFIRMED", NO_CHECKPOINT, new
    if ck.get("per_page") != per_page:
        return "UNCONFIRMED", "checkpoint per_page differs", new
    if ck.get("page") != 1:
        return "UNCONFIRMED", "checkpoint is beyond page 1; automatic path reads page 1 only", new
    issue_url = f"{API}/repos/{t.repo}/issues/{t.pr}"
    try:
        resp = t.get(t.list_url(per_page, 1))
    except Exception as e:
        return "UNCONFIRMED", f"page 1 read failure: {e}", new
    items = resp.json if isinstance(resp.json, list) else None
    if resp.status != 200 or items is None:
        return "UNCONFIRMED", f"page 1 missing (HTTP {resp.status})", new
    if any(not isinstance(item, dict) for item in items):
        return "UNCONFIRMED", "page 1 contains a non-object item", new
    ids = [c.get("id") for c in items]
    if any(c.get("issue_url") != issue_url for c in items) or any(type(i) is not int for i in ids) \
            or ids != sorted(set(ids)):
        return "UNCONFIRMED", f"page 1 not a clean ascending PR #{t.pr} slice", new
    if not items:
        return "UNCONFIRMED", "page 1 empty: not proof of anything", new
    if len(items) >= per_page:
        return "UNCONFIRMED", "page 1 full: more may exist; page 2 is not read automatically", new
    if not isinstance(resp.headers, dict):                 # connector does not expose headers
        return "UNCONFIRMED", "page 1 response headers not visible: completeness undeterminable", new
    links = [value for key, value in resp.headers.items() if isinstance(key, str) and key.lower() == "link"]
    if len(links) > 1:
        return "UNCONFIRMED", "ambiguous Link headers", new
    raw_link = links[0] if links else None
    if not raw_link:
        return "UNCONFIRMED", "page 1 has no Link header: completeness undeterminable", new
    try:
        link = parse_link(raw_link)
    except ScopeError as e:
        return "UNCONFIRMED", f"page 1 Link undeterminable: {e}", new
    if not link:
        return "UNCONFIRMED", "page 1 Link header unparseable", new
    if ("last" not in link or set(link) - {"first", "last"}
            or any(url != t.list_url(per_page, 1) for url in link.values())):
        return "UNCONFIRMED", "page 1 short but Link names another page", new
    if ck["last_id"] not in ids or ids[0] != ck["first_id"] or \
            _ids_sha(ids[:ids.index(ck["last_id"]) + 1]) != ck["ids_sha256"]:
        log.append("LOCK", f"lock:overlap:1:{_ids_sha(ids)}", {"why": "page 1 does not extend the checkpoint"})
        return "UNCONFIRMED", "page 1 does not extend the trusted checkpoint (LOCK)", new
    for c in items:
        res = _record(log, c)
        if res == "NEW":
            new.append(c["id"])
        elif res == "CONFLICT":
            return "UNCONFIRMED", f"comment {c['id']} differs from recorded (LOCK)", new
    return "COMPLETE", f"page 1 read ({len(items)} < per_page {per_page}); checkpoint unchanged", new
