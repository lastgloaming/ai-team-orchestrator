"""Synthetic ingestion and ledger recovery tests using a local transport."""
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from orchestrator import control_ingest as M  # noqa: E402

REPO, PR = "fixture-owner/example-ledger", 47
PP = 3


def transport(inner):
    return M.GuardedTransport(inner, REPO, PR)


def comment(cid, body="synthetic", sec=0):
    api = f"{M.API}/repos/{REPO}/issues"
    return {"id": cid, "issue_url": f"{api}/{PR}", "body": body,
            "created_at": f"2099-01-01T00:00:{sec:02d}Z", "updated_at": f"2099-01-01T00:00:{sec:02d}Z"}


class Fake:
    """Synthetic read-only pull request: paged listing with GitHub-style Link headers; knobs for gaps."""

    def __init__(self, comments, missing=(), nolink=(), per_page=None, boom=False, headers_visible=True,
                 link_override=None):
        self.comments, self.missing, self.nolink = sorted(comments, key=lambda c: c["id"]), set(missing), set(nolink)
        self.urls, self.boom = [], boom
        self.headers_visible, self.link_override = headers_visible, link_override

    def get(self, url):
        self.urls.append(url)
        t = transport(self)
        kind, a, page = t.check(url)
        if kind == "one":
            hit = [c for c in self.comments if c["id"] == a]
            hdr = {} if self.headers_visible else None
            return M.Response(200, hdr, dict(hit[0])) if hit else M.Response(404, hdr, {})
        if self.boom:
            raise OSError("synthetic read failure")
        if page in self.missing:
            return M.Response(404, {}, {})
        chunk = [dict(c) for c in self.comments[(page - 1) * a: page * a]]
        last = max(1, -(-len(self.comments) // a))
        rels = ([f'<{t.list_url(a, page + 1)}>; rel="next"'] if page < last else []) + \
               ([f'<{t.list_url(a, page - 1)}>; rel="prev"'] if page > 1 else [])
        if not rels:
            rels = [f'<{t.list_url(a, 1)}>; rel="last"']           # synthetic: single page still carries Link
        hdr = {} if page in self.nolink else {"Link": ", ".join(rels)}
        if self.link_override is not None:
            hdr = {"Link": self.link_override}
        return M.Response(200, hdr if self.headers_visible else None, chunk)


def world(n):
    return [comment(1000 + i, sec=i % 60) for i in range(n)]


def seeded(d, comments, cap=(100,), pp=PP, k=2):
    """Synthetic trusted checkpoint: the first k ids of page 1 (real initialization is out of scope)."""
    log = M.TieredLog(d, capacities=cap)
    M.seed_checkpoint(log, pp, 1, [c["id"] for c in comments][:k], "synthetic-test-fixture")
    return log


def page_fetches(f, page):
    return [u for u in f.urls if u.endswith(f"&page={page}")]


class Minimal(unittest.TestCase):
    def test_explicit_scope(self):
        fake = Fake(world(1))
        with self.assertRaises(TypeError):
            M.GuardedTransport(fake)
        for repo, pr in ((REPO, True), (REPO, 0), ("fixture-owner/..", PR), ("owner/repo/extra", PR)):
            with self.assertRaises(M.ScopeError):
                M.GuardedTransport(fake, repo, pr)
        t = transport(fake)
        exact = f"{M.API}/repos/{REPO}/issues/comments/1000"
        for url in (exact + "\n", exact + "?page=1", exact.replace(REPO, "fixture-owner/other"),
                    t.list_url(10, 1).replace(f"/{PR}/", "/48/")):
            with self.assertRaises(M.ScopeError):
                t.get(url)
        self.assertEqual(fake.urls, [])
        with tempfile.TemporaryDirectory() as directory:
            log = M.TieredLog(directory)
            fake.comments[0]["issue_url"] = f"{M.API}/repos/{REPO}/issues/48"
            self.assertTrue(M.on_event(t, log, 1000).startswith("UNCONFIRMED"))
            self.assertEqual(log.state()["events"], {})

    def test_1_duplicate_event(self):
        d = tempfile.mkdtemp(prefix="min1_dup_")
        f = Fake(world(4))
        t = transport(f)
        log = M.TieredLog(d)
        self.assertEqual(M.on_event(t, log, 1002), "NEW")
        self.assertEqual(M.on_event(t, log, 1002), "DUPLICATE")
        self.assertEqual(M.on_event(t, M.TieredLog(d), 1002), "DUPLICATE")          # after restart
        self.assertEqual(sorted(M.TieredLog(d).state()["events"]), [1002])
        f.comments[2]["body"] = "edited"                                            # same id, new content
        self.assertEqual(M.on_event(t, log, 1002), "UNCONFIRMED:conflict (LOCK)")
        self.assertEqual(M.on_event(t, log, 9999), "UNCONFIRMED:comment not readable or not on Control PR")
        self.assertTrue(all(u.endswith(("/comments/1002", "/comments/9999")) for u in f.urls))  # by id only

    def test_2_missed_event_recovered_and_incomplete_stays_unconfirmed(self):
        d = tempfile.mkdtemp(prefix="min2_miss_")
        pp = 10
        comments = world(6)                                                         # page 1 short
        log = seeded(d, comments, pp=pp)
        self.assertEqual(M.on_event(transport(Fake(comments)), log, 1005), "NEW")
        st, why, new = M.catch_up(transport(Fake(comments, per_page=pp)), log, pp)
        self.assertEqual(st, "COMPLETE", why)
        self.assertEqual(sorted(new), [c["id"] for c in comments if c["id"] != 1005])  # missed ones recovered
        later = comments + [comment(1006), comment(1007)]                          # events missed
        st, why, new = M.catch_up(transport(Fake(later, per_page=pp)), log, pp)
        self.assertEqual((st, new), ("COMPLETE", [1006, 1007]), why)
        for knobs, expect in (({"missing": {1}}, "page 1 missing (HTTP 404)"),
                              ({"boom": True}, "page 1 read failure: synthetic read failure")):
            with self.subTest(**{k: str(v) for k, v in knobs.items()}):
                st, why, _ = M.catch_up(transport(Fake(later, per_page=pp, **knobs)), M.TieredLog(d), pp)
                self.assertEqual((st, why), ("UNCONFIRMED", expect))
        bad = [c for c in later if c["id"] != 1001]                                 # evidence mismatch
        st, why, _ = M.catch_up(transport(Fake(bad, per_page=pp)), M.TieredLog(tempfile.mkdtemp()), pp)
        self.assertEqual((st, why), ("UNCONFIRMED", M.NO_CHECKPOINT))              # no trusted checkpoint
        d2 = tempfile.mkdtemp(prefix="min2_mis2_")
        st, why, _ = M.catch_up(transport(Fake(bad, per_page=pp)), seeded(d2, later, pp=pp), pp)
        self.assertEqual((st, why), ("UNCONFIRMED", "page 1 does not extend the trusted checkpoint (LOCK)"))

    def test_3_tier_rotation_and_restart(self):
        d = tempfile.mkdtemp(prefix="min2_tier_")
        pp = 20
        comments = world(11)                                                        # page 1 short (11 < 20)
        t = transport(Fake(comments, per_page=pp))
        log = seeded(d, comments, cap=(4,), pp=pp)
        with self.assertRaises(M.Crash):                                            # crash mid-rotation
            for c in comments[:6]:
                M.on_event(t, M.TieredLog(d, (4,), crash_at="rotate_before_seal"), c["id"])
        st, why, new = M.catch_up(t, M.TieredLog(d, (4,)), pp)                    # restart + recover
        self.assertEqual(st, "COMPLETE", why)
        state = M.TieredLog(d, (4,)).state()
        self.assertEqual(sorted(state["events"]), [c["id"] for c in comments])     # nothing lost
        self.assertGreaterEqual(len(state["tiers"]), 3)
        for n, _ in state["tiers"][:-1]:                                            # sealed tiers read-only
            self.assertFalse(os.stat(os.path.join(d, f"tier-{n:06d}.jsonl")).st_mode & 0o222)
        for c in comments:                                                          # cross-tier dedup
            self.assertEqual(M.on_event(t, M.TieredLog(d, (4,)), c["id"]), "DUPLICATE")
        self.assertEqual(M.catch_up(t, M.TieredLog(d, (4,)), pp)[2], [])            # restart: nothing new
        self.assertEqual(M.TieredLog(d, (4,)).state()["events"], state["events"])

    def test_4_full_first_page_unconfirmed_no_page2_no_checkpoint_advance(self):
        d = tempfile.mkdtemp(prefix="min2_full_")
        comments = world(8)                                                         # page 1 of 3 is full
        log = seeded(d, comments)
        before = M.TieredLog(d).state()["checkpoint"]
        for links in (True, False):                                                 # with and without rel=next
            with self.subTest(link=links):
                f = Fake(comments, nolink=set() if links else {1})
                st, why, new = M.catch_up(transport(f), M.TieredLog(d), PP)
                self.assertEqual((st, why, new),
                                 ("UNCONFIRMED", "page 1 full: more may exist; page 2 is not read automatically", []))
                self.assertEqual(len(page_fetches(f, 1)), 1)
                self.assertEqual(page_fetches(f, 2), [])                            # zero page-2 fetches
                self.assertEqual([u for u in f.urls if "&page=" in u and not u.endswith("&page=1")], [])
        after = M.TieredLog(d).state()
        self.assertEqual(after["checkpoint"], before)                               # checkpoint not advanced
        self.assertEqual(after["events"], {})                                       # nothing recorded

    def test_5_non_full_complete_first_page_works(self):
        d = tempfile.mkdtemp(prefix="min2_short_")
        comments = world(2)                                                         # 2 < PP, single page
        log = seeded(d, comments)
        before = log.state()["checkpoint"]
        f = Fake(comments)
        st, why, new = M.catch_up(transport(f), log, PP)
        self.assertEqual((st, sorted(new)), ("COMPLETE", [1000, 1001]), why)
        self.assertEqual(page_fetches(f, 2), [])
        self.assertEqual(M.TieredLog(d).state()["checkpoint"], before)              # unchanged


class AcceptanceFix(unittest.TestCase):
    """Header visibility and durable write checks."""

    def test_whitespace_relation_invalidates_entire_link(self):
        comments = world(2)
        f = Fake(comments)
        t = transport(f)
        f.link_override = f'<{t.list_url(PP, 1)}>; rel="last", <{t.list_url(PP, 2)}>; rel="   "'
        with tempfile.TemporaryDirectory() as d:
            log = seeded(d, comments)
            before = log.state()
            self.assertEqual(M.catch_up(t, log, PP), ("UNCONFIRMED", "page 1 Link header unparseable", []))
            self.assertEqual(f.urls, [t.list_url(PP, 1)])
            self.assertEqual(log.state(), before)

    def test_null_page_item_rejects_entire_page(self):
        comments = world(1)
        reader = mock.Mock()
        t = transport(reader)
        url = t.list_url(PP, 1)
        reader.get.return_value = M.Response(200, {"Link": f'<{url}>; rel="last"'}, [comments[0], None])
        with tempfile.TemporaryDirectory() as d:
            log = seeded(d, comments)
            before = log.state()
            self.assertEqual(M.catch_up(t, log, PP), ("UNCONFIRMED", "page 1 contains a non-object item", []))
            reader.get.assert_called_once_with(url)
            self.assertEqual(log.state(), before)

    def test_6_invisible_or_missing_headers_short_first_page_unconfirmed(self):
        comments = world(2)                                                         # short page 1
        cases = ((dict(headers_visible=False), "page 1 response headers not visible: completeness undeterminable"),
                 (dict(nolink={1}), "page 1 has no Link header: completeness undeterminable"),
                 (dict(link_override="garbage"), "page 1 Link header unparseable"))
        for knobs, expect in cases:
            with self.subTest(**{k: str(v) for k, v in knobs.items()}):
                d = tempfile.mkdtemp(prefix="min3_hdr_")
                log = seeded(d, comments)
                f = Fake(comments, **knobs)
                self.assertEqual(M.catch_up(transport(f), log, PP), ("UNCONFIRMED", expect, []))
                self.assertEqual(page_fetches(f, 2), [])
                self.assertEqual(M.TieredLog(d).state()["events"], {})              # nothing recorded
        d = tempfile.mkdtemp(prefix="min3_one_")                                    # single event still works
        t = transport(Fake(comments, headers_visible=False))
        self.assertEqual(M.on_event(t, M.TieredLog(d), 1001), "NEW")
        self.assertEqual(M.on_event(t, M.TieredLog(d), 1001), "DUPLICATE")

    def test_7_short_write_never_reports_success_and_recovers_cleanly(self):
        real_write, real_trunc = os.write, os.ftruncate
        ev = lambda i: ("EVENT", f"event:{i}", {"id": i, "digest": "d", "created_at": None, "updated_at": None})  # noqa: E731
        d = tempfile.mkdtemp(prefix="min3_short_")
        log = M.TieredLog(d)
        self.assertEqual(log.append(*ev(1)), "NEW")
        tier = os.path.join(d, "tier-000001.jsonl")
        size0 = os.path.getsize(tier)

        def short_then_fail(fd, data):                                              # partial write, then ENOSPC
            if getattr(short_then_fail, "done", False):
                raise OSError(28, "No space left on device (synthetic)")
            short_then_fail.done = True
            return real_write(fd, bytes(data)[: len(data) // 2])
        with mock.patch.object(M.os, "write", side_effect=short_then_fail):
            with self.assertRaises(OSError):
                log.append(*ev(2))                                                  # no success reported
        self.assertEqual(os.path.getsize(tier), size0)                              # rolled back, no partial
        short_then_fail.done = False
        with mock.patch.object(M.os, "ftruncate", side_effect=OSError("synthetic truncate failure")):
            with mock.patch.object(M.os, "write", side_effect=short_then_fail):
                with self.assertRaises(OSError):
                    log.append(*ev(2))                                              # rollback also fails
        self.assertGreater(os.path.getsize(tier), size0)                            # torn tail left on disk
        self.assertEqual(sorted(M.TieredLog(d).state()["events"]), [1])             # restart: torn tail dropped
        self.assertEqual(os.path.getsize(tier), size0)
        self.assertEqual(M.TieredLog(d).append(*ev(2)), "NEW")                      # retry once
        self.assertEqual(M.TieredLog(d).append(*ev(2)), "DUPLICATE")                # no duplicate
        calls = []

        def short_only(fd, data):                                                   # short writes that progress
            calls.append(len(data))
            return real_write(fd, bytes(data)[: max(1, len(data) // 2)])
        with mock.patch.object(M.os, "write", side_effect=short_only):
            self.assertEqual(M.TieredLog(d).append(*ev(3)), "NEW")                  # loop completes the record
        self.assertGreater(len(calls), 1)
        self.assertEqual(sorted(M.TieredLog(d).state()["events"]), [1, 2, 3])
        self.assertIs(os.ftruncate, real_trunc)

    def test_8_first_tier_creation_fsyncs_directory(self):
        d = os.path.join(os.path.realpath(tempfile.mkdtemp(prefix="min3_dir_")), "ledger")
        real_fsync, synced = os.fsync, []

        def spy(fd):
            synced.append(os.readlink(f"/proc/self/fd/{fd}"))
            return real_fsync(fd)
        with mock.patch.object(M.os, "fsync", side_effect=spy):
            M.TieredLog(d).state()                                                  # creates tier-000001
        self.assertIn(os.path.join(d, "tier-000001.jsonl"), synced)
        self.assertIn(os.path.realpath(d), synced)                                  # directory fsync'd
        self.assertLess(synced.index(os.path.join(d, "tier-000001.jsonl")), synced.index(os.path.realpath(d)))


if __name__ == "__main__":
    unittest.main()
