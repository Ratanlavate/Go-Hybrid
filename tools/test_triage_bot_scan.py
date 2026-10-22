"""Offline tests for the triage bot's batched, cached PR scan (no network).

Guards the regression that killed shepherd-board: one REST scan of ~320 open
PRs cost >1000 core-bucket calls and every run died on RateGuard, so the board
stopped updating. The scan now lists PRs cheaply (REST), fetches diffs only for
PRs whose (head, main) pair changed (GraphQL aliases), reuses everything else
from a per-head cache, and degrades to "behind-base unknown" instead of
aborting when quota runs out.
"""
import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ENTRY = Path(__file__).with_name("triage_bot.py")
SPEC = importlib.util.spec_from_file_location("triage_bot", ENTRY)
BOT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BOT)


def rest_pr(number, head, paths, mergeable=True, behind=0, title="t"):
    return {
        "number": number,
        "title": title,
        "user": {"login": "author%d" % number},
        "created_at": "2026-10-01T00:00:00Z",
        "updated_at": "2026-10-02T00:00:00Z",
        "draft": False,
        "head": {"sha": head},
        "_paths": paths,
        "_mergeable": mergeable,
        "_behind": behind,
    }


class FakeGh:
    """Serves the REST list + main SHA, aliased diff batches and compares."""

    def __init__(self, prs, fail_compares=False, main="m" * 40):
        self.repo = "owner/name"
        self.prs = prs
        self.main = main
        self.fail_compares = fail_compares
        self.calls = 0
        self.gql_calls = 0
        self.remaining = 5000
        self.quota_exhausted = False
        self.compares = []
        self.graphql_numbers = []
        self.by_number = {p["number"]: p for p in prs}
        self.by_head = {p["head"]["sha"]: p for p in prs}

    def paginate(self, path, max_pages=20, key=None):
        assert "/pulls?" in path, path
        self.calls += 1
        return [dict(p) for p in self.prs]

    def get(self, path):
        self.calls += 1
        if path.endswith("/commits/main"):
            return {"sha": self.main}
        if "/compare/base_" in path:
            return {"files": []}  # merge_base...main overlap probe
        m = re.search(r"/compare/[^.]+\.\.\.([0-9a-f]+)", path)
        assert m, path
        if self.fail_compares:
            raise BOT.RateGuard("rate limit low: 0 remaining")
        pr = self.by_head[m.group(1)]
        self.compares.append(pr["number"])
        return {"behind_by": pr["_behind"], "merge_base_commit": {"sha": "base_" + pr["head"]["sha"]}}

    def graphql(self, query, variables=None):
        self.calls += 1
        self.gql_calls += 1
        nums = [int(n) for n in re.findall(r"pullRequest\(number:(\d+)\)", query)]
        assert nums, "query carries no numbered aliases: %s" % query[:200]
        self.graphql_numbers.append(nums)
        repo = {}
        for i, n in enumerate(nums):
            pr = self.by_number[n]
            repo["p%d" % i] = {
                "number": n,
                "mergeable": "MERGEABLE" if pr["_mergeable"] else "CONFLICTING",
                "mergeStateStatus": "BEHIND" if pr["_behind"] else "CLEAN",
                "additions": len(pr["_paths"]),
                "deletions": 0,
                "files": {
                    "pageInfo": {"hasNextPage": False},
                    "nodes": [{"path": p, "additions": 1, "deletions": 0} for p in pr["_paths"]],
                },
            }
        return {"repository": repo}


def collect(gh, cache_path, jobs=2):
    args = SimpleNamespace(jobs=jobs, cache=str(cache_path))
    return BOT.collect({}, gh, args)


class DiffMappingTests(unittest.TestCase):
    def node(self, mergeable, state, files, more=False):
        return {
            "mergeable": mergeable,
            "mergeStateStatus": state,
            "files": {"pageInfo": {"hasNextPage": more},
                      "nodes": [{"path": p, "additions": 2, "deletions": 1} for p in files]},
        }

    def test_mergeable_enums(self):
        for raw, want in (("MERGEABLE", True), ("CONFLICTING", False), ("UNKNOWN", None)):
            self.assertIs(BOT.diff_from_graphql(self.node(raw, "CLEAN", ["a"]))["mergeable"], want)

    def test_merge_states(self):
        for raw, want in (("BEHIND", "behind"), ("CLEAN", "clean"), ("DIRTY", "dirty"),
                          ("BLOCKED", "blocked"), ("DRAFT", "draft"), ("UNSTABLE", "unstable"),
                          ("HAS_HOOKS", "unknown"), ("UNKNOWN", "unknown")):
            self.assertEqual(BOT.diff_from_graphql(self.node("MERGEABLE", raw, ["a"]))["mergeable_state"], want)

    def test_file_flattening_and_truncation(self):
        d = BOT.diff_from_graphql(self.node("MERGEABLE", "CLEAN", ["a", "b"], more=True))
        self.assertEqual(d["file_paths"], ["a", "b"])
        self.assertEqual(d["file_additions"], [2, 2])
        self.assertTrue(d["files_truncated"])
        self.assertFalse(BOT.diff_from_graphql(self.node("MERGEABLE", "CLEAN", []))["files_truncated"])

    def test_diff_query_aliases_every_number(self):
        q = BOT.diff_query([7, 9, 12])
        self.assertEqual(q.count("pullRequest(number:"), 3)
        for n in (7, 9, 12):
            self.assertIn("pullRequest(number:%d)" % n, q)
        self.assertIn("p0:", q)


class CacheTests(unittest.TestCase):
    def test_entry_requires_same_head_and_main(self):
        cache = {"5": {"head": "h", "main": "m", "behind_by": 3}}
        self.assertIsNotNone(BOT.cache_entry(cache, 5, "h", "m"))
        self.assertIsNone(BOT.cache_entry(cache, 5, "h2", "m"))
        self.assertIsNone(BOT.cache_entry(cache, 5, "h", "m2"))
        self.assertIsNone(BOT.cache_entry(cache, 6, "h", "m"))

    def test_load_rejects_foreign_version_and_garbage(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "c.json"
            self.assertEqual(BOT.load_scan_cache(str(p)), {})
            p.write_text("{not json")
            self.assertEqual(BOT.load_scan_cache(str(p)), {})
            p.write_text(json.dumps({"version": BOT.CACHE_VERSION + 99, "prs": {"1": {}}}))
            self.assertEqual(BOT.load_scan_cache(str(p)), {})
            p.write_text(json.dumps({"version": BOT.CACHE_VERSION, "prs": {"1": {"head": "h"}}}))
            self.assertEqual(BOT.load_scan_cache(str(p)), {"1": {"head": "h"}})

    def test_save_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "sub" / "c.json"
            BOT.save_scan_cache(str(p), {"3": {"head": "h", "main": "m"}}, "m")
            self.assertEqual(BOT.load_scan_cache(str(p)), {"3": {"head": "h", "main": "m"}})


class SelectiveRefreshTests(unittest.TestCase):
    def setUp(self):
        self.prs = [
            rest_pr(1, "a" * 40, ["x.py"], behind=0),
            rest_pr(2, "b" * 40, ["y.py"], behind=7),
            rest_pr(3, "c" * 40, ["tests/z.py"], behind=20),
        ]

    def test_cold_scan_batches_diffs_and_compares_only_behind_prs(self):
        with tempfile.TemporaryDirectory() as td:
            cache = Path(td) / "c.json"
            gh = FakeGh(self.prs)
            items, main, c = collect(gh, cache)
            self.assertEqual(len(items), 3)
            self.assertEqual(main, "m" * 40)
            self.assertEqual(gh.graphql_numbers, [[1, 2, 3]])
            self.assertEqual(sorted(gh.compares), [1, 2, 3])
            by_n = {i["number"]: i for i in items}
            self.assertEqual(by_n[2]["behind_by"], 7)
            self.assertEqual(by_n[3]["paths_sample"], ["tests/z.py"])
            self.assertTrue(by_n[3]["tests_included"])
            self.assertTrue(all(i["behind_known"] for i in items))
            BOT.save_scan_cache(str(cache), c, main)

            # warm scan: same heads + same main => no GraphQL, no compares
            warm = FakeGh(self.prs)
            items2, _, _ = collect(warm, cache)
            self.assertEqual(warm.gql_calls, 0)
            self.assertEqual(warm.compares, [])
            self.assertEqual(warm.calls, 2)  # REST list + main sha
            self.assertEqual(
                [(i["number"], i["behind_by"], i["mergeable"], i["paths_sample"]) for i in items2],
                [(i["number"], i["behind_by"], i["mergeable"], i["paths_sample"]) for i in items])

    def test_one_pushed_head_refreshes_only_that_pr(self):
        with tempfile.TemporaryDirectory() as td:
            cache = Path(td) / "c.json"
            gh = FakeGh(self.prs)
            _, main, c = collect(gh, cache)
            BOT.save_scan_cache(str(cache), c, main)

            pushed = [dict(p) for p in self.prs]
            pushed[1] = dict(pushed[1], head={"sha": "d" * 40})  # PR 2 force-pushed
            gh2 = FakeGh(pushed)
            items, _, _ = collect(gh2, cache)
            self.assertEqual(gh2.graphql_numbers, [[2]])
            self.assertEqual(gh2.compares, [2])
            self.assertEqual({i["number"] for i in items}, {1, 2, 3})

    def test_main_move_refreshes_every_pr(self):
        with tempfile.TemporaryDirectory() as td:
            cache = Path(td) / "c.json"
            gh = FakeGh(self.prs)
            _, main, c = collect(gh, cache)
            BOT.save_scan_cache(str(cache), c, main)

            moved = FakeGh(self.prs, main="n" * 40)
            collect(moved, cache)
            self.assertEqual(moved.graphql_numbers, [[1, 2, 3]])
            self.assertEqual(sorted(moved.compares), [1, 2, 3])


class DegradedScanTests(unittest.TestCase):
    def test_quota_exhaustion_degrades_instead_of_raising(self):
        prs = [rest_pr(1, "a" * 40, ["x.py"], behind=30)]
        with tempfile.TemporaryDirectory() as td:
            gh = FakeGh(prs, fail_compares=True)
            items, _, _ = collect(gh, Path(td) / "c.json")
            self.assertEqual(len(items), 1)
            self.assertIsNone(items[0]["behind_by"])
            self.assertFalse(items[0]["behind_known"])
            self.assertTrue(gh.quota_exhausted)

    def test_classify_and_board_render_unknown_behind(self):
        cfg = BOT.load_cfg()
        item = {
            "number": 1, "title": "t", "author": "a", "created_at": "2026-10-01T00:00:00Z",
            "updated_at": "2026-10-02T00:00:00Z", "age_days": 1.0, "idle_days": 1.0,
            "draft": False, "mergeable": True, "mergeable_state": "unknown",
            "head_sha": "a" * 12, "files": 1, "paths_sample": ["x.py"], "additions": 1,
            "deletions": 0, "behind_by": None, "behind_known": False, "overlap_risk": [],
            "tests_included": False,
        }
        items, clusters = BOT.classify(cfg, [dict(item)])
        self.assertIn(items[0]["bucket"], ("RECON", "LAND-CANDIDATE"))
        self.assertEqual(items[0]["action"],
                         "Base distance unknown (rate-limited scan) — re-check before landing")
        md, _ = BOT.render_board(cfg, items, clusters, {}, "NOW")
        self.assertIn("Degraded scan", md)
        self.assertIn("| ? |", md)


if __name__ == "__main__":
    unittest.main()
