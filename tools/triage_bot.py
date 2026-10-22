#!/usr/bin/env python3
"""triage_bot.py — Go-Hybrid PR triage bot.

Encodes the PR verification protocol used across sessions 9-22 so the backlog
manages itself. Read-only by default; anything that writes to GitHub needs an
explicit flag. The bot NEVER merges and NEVER closes — those stay deliberate
acts (credit-close protocol, wildcard verdict window).

Subcommands:
  scan     snapshot every open PR, classify (wildcard hold / draft / conflicts /
           stale-base / duplicate cluster / tests-included / new arrival /
           overlap-risk), score, write TRIAGE.md + TRIAGE.json.
           --verify K additionally runs the worktree merge gate on the top K
           land-candidates.
  approve  approve action_required CI runs for open PR heads (the chore we used
           to do ~190x per session by hand), with a quota guard (--max).
  labels   ensure the triage:* label palette exists and apply per-PR labels.
  board    print or post the triage board to a GitHub issue (idempotent upsert
           via a body marker).

Stdlib only. Token: --token-file, $GITHUB_TOKEN, or /home/z/my-project/.gh_token.
"""

import argparse
import http.client
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

CONFIG_PATH = Path(__file__).resolve().parent / "triage_config.json"
DEFAULT_TOKEN_FILE = "/home/z/my-project/.gh_token"
BOARD_MARKER = "triage-bot:board:v1"
API = "https://api.github.com"
GRAPHQL_API = "https://api.github.com/graphql"

# The board scans every open PR on every event. With ~320 open PRs the old
# REST-per-PR path cost 3-4 core-bucket calls each (>1000/hr against the
# GITHUB_TOKEN core quota) and the workflow died on RateGuard, leaving the
# board stale. The cheap REST list gives every PR's head SHA; only PRs whose
# (head_sha, main_sha) pair changed need their diff + mergeability re-read,
# and those come in aliased GraphQL batches (never one query per PR).
DIFF_BATCH = 20
DIFF_FIELDS = ("number mergeable mergeStateStatus additions deletions "
               "files(first:100){pageInfo{hasNextPage} nodes{path additions deletions}}")

MERGEABLE_FROM_GQL = {"MERGEABLE": True, "CONFLICTING": False, "UNKNOWN": None}
MERGE_STATE_FROM_GQL = {
    "BEHIND": "behind", "BLOCKED": "blocked", "CLEAN": "clean", "DIRTY": "dirty",
    "DRAFT": "draft", "UNSTABLE": "unstable", "UNKNOWN": "unknown",
    "HAS_HOOKS": "unknown",
}

CACHE_VERSION = 2


def log(msg):
    print(msg, flush=True)


class RateGuard(Exception):
    pass


class Gh:
    """Minimal GitHub REST client with retries and a rate-limit guard."""

    def __init__(self, repo, token, min_remaining=120):
        self.repo = repo
        self.token = token
        self.min_remaining = min_remaining
        self.calls = 0
        self.remaining = None
        self.gql_calls = 0
        self.gql_remaining = None
        # Set when the core bucket runs dry: the scan degrades to "behind-base
        # unknown" instead of aborting, so the board still gets written.
        self.quota_exhausted = False

    def req(self, method, path, body=None):
        """Returns (status, parsed_json_or_None). Raises RateGuard on low quota."""
        url = path if path.startswith("http") else API + path
        data = json.dumps(body).encode() if body is not None else None
        last_err = None
        for attempt in range(5):
            r = Request(url, data=data, method=method)
            r.add_header("Authorization", "Bearer " + self.token)
            r.add_header("Accept", "application/vnd.github+json")
            r.add_header("X-GitHub-Api-Version", "2022-11-28")
            r.add_header("User-Agent", "go-hybrid-triage-bot")
            if data is not None:
                r.add_header("Content-Type", "application/json")
            try:
                with urlopen(r, timeout=60) as resp:
                    self.calls += 1
                    rem = resp.headers.get("x-ratelimit-remaining")
                    resource = resp.headers.get("x-ratelimit-resource", "core")
                    if rem is not None and resource == "core":
                        # search/graphql have their own small buckets (30/min);
                        # only the core bucket is worth guarding.
                        self.remaining = int(rem)
                        if self.remaining < self.min_remaining:
                            raise RateGuard(
                                "rate limit low: %s remaining" % self.remaining)
                    elif rem is not None and resource == "graphql":
                        self.gql_remaining = int(rem)
                    raw = resp.read().decode()
                    return resp.status, (json.loads(raw) if raw.strip() else None)
            except HTTPError as e:
                body_txt = ""
                try:
                    body_txt = e.read().decode()[:400]
                except Exception:
                    pass
                if e.code == 404:
                    return 404, None
                # GitHub secondary rate limits surface as 403 with a "rate
                # limit" body, not 429 — retry those with a longer backoff
                # (the window is typically 30-60s). Other 403s are permission
                # errors: fail fast, retrying cannot help.
                is_rate_403 = e.code == 403 and "rate limit" in body_txt.lower()
                if (e.code in (429, 502, 503, 504) or is_rate_403) and attempt < 4:
                    time.sleep([2, 5, 10, 20][attempt] if e.code != 403
                               else [5, 20, 45, 60][attempt])
                    last_err = e
                    continue
                raise RuntimeError("%s %s -> %s %s" % (method, path, e.code, body_txt))
            except RateGuard:
                raise
            except (URLError, ConnectionError, TimeoutError, http.client.HTTPException) as e:
                # Transient connection churn (RemoteDisconnected under parallel
                # load) used to kill a whole 320-PR scan on the first hiccup.
                if attempt < 4:
                    time.sleep([2, 5, 10, 20][attempt])
                    last_err = e
                    continue
                raise RuntimeError("%s %s failed after retries: %s" % (method, path, last_err))
        raise RuntimeError("%s %s failed after retries: %s" % (method, path, last_err))

    def get(self, path):
        return self.req("GET", path)[1]

    def post(self, path, body):
        return self.req("POST", path, body)[1]

    def patch(self, path, body):
        return self.req("PATCH", path, body)[1]

    def graphql(self, query, variables=None):
        """POST a GraphQL query and return its `data` object."""
        if self.gql_remaining is not None and self.gql_remaining < 100:
            raise RateGuard("graphql rate limit low: %s remaining" % self.gql_remaining)
        status, data = self.req("POST", GRAPHQL_API,
                                {"query": query, "variables": variables or {}})
        self.gql_calls += 1
        if not isinstance(data, dict) or data.get("errors"):
            errs = (data or {}).get("errors") if isinstance(data, dict) else data
            raise RuntimeError("graphql error: %s" % (str(errs)[:300],))
        return data.get("data") or {}

    def paginate(self, path, max_pages=20, key=None):
        """Follow page links; unwrap dict envelopes like {"workflow_runs": [...]}."""
        out, page = [], 1
        while page <= max_pages:
            sep = "&" if "?" in path else "?"
            _, data = self.req("GET", "%s%spage=%d&per_page=100" % (path, sep, page))
            if isinstance(data, dict):
                if key and key in data:
                    data = data[key]
                elif "items" in data:
                    data = data["items"]
                else:
                    data = [data]
            if not data:
                break
            out.extend(data)
            if len(data) < 100:
                break
            page += 1
        return out


# ---------------------------------------------------------------- helpers

def load_cfg():
    cfg = json.loads(CONFIG_PATH.read_text())
    assert cfg["wildcard_holds"] == sorted(cfg["wildcard_holds"])
    return cfg


def load_token(args):
    if getattr(args, "token_env", None) and os.environ.get(args.token_env):
        return os.environ[args.token_env].strip()
    if getattr(args, "token_file", None):
        p = Path(args.token_file)
    elif os.environ.get("GITHUB_TOKEN"):
        return os.environ["GITHUB_TOKEN"].strip()
    else:
        p = Path(DEFAULT_TOKEN_FILE)
    if p.exists():
        return p.read_text().strip()
    sys.exit("no token: set GITHUB_TOKEN or pass --token-file")


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def age_days(iso, now):
    return round((now - datetime.fromisoformat(iso.replace("Z", "+00:00"))).total_seconds() / 86400, 1)


# ---------------------------------------------------------------- graph batch

def diff_from_graphql(node):
    """Diff/mergeability node -> the flat dict the scanner caches per PR."""
    conn = node.get("files") or {}
    fnodes = conn.get("nodes") or []
    mergeable = (node.get("mergeable") or "UNKNOWN").upper()
    state = (node.get("mergeStateStatus") or "UNKNOWN").upper()
    return {
        "mergeable": MERGEABLE_FROM_GQL.get(mergeable),
        "mergeable_state": MERGE_STATE_FROM_GQL.get(state, "unknown"),
        "file_paths": [f.get("path") for f in fnodes if f.get("path")],
        "file_additions": [f.get("additions") or 0 for f in fnodes],
        "file_deletions": [f.get("deletions") or 0 for f in fnodes],
        # >100 files: GraphQL truncates, REST pagination still has the rest.
        "files_truncated": bool((conn.get("pageInfo") or {}).get("hasNextPage")),
    }


def diff_query(numbers):
    """Aliased one-query diff for exactly these PRs.

    GraphQL has no "open PRs by number" filter; aliases keep the fetch
    proportional to the PRs that actually changed (usually 0-5 per event).
    """
    parts = ["p%d: pullRequest(number:%d){%s}" % (i, n, DIFF_FIELDS)
             for i, n in enumerate(numbers)]
    return ("query($owner:String!,$name:String!){repository(owner:$owner,name:$name){%s}}"
            % " ".join(parts))


# ---------------------------------------------------------------- scan cache

def load_scan_cache(path):
    """Compare results from an earlier scan: {pr_number: {head,main,behind_by,...}}."""
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        d = json.loads(p.read_text())
    except (ValueError, OSError):
        return {}
    if d.get("version") != CACHE_VERSION:
        return {}
    return d.get("prs") or {}


def cache_entry(cache, number, head_sha, main_sha):
    """The cached compare result for this exact (head, main) pair, if any."""
    e = cache.get(str(number)) or {}
    if e.get("head") == head_sha and e.get("main") == main_sha:
        return e
    return None


def save_scan_cache(path, cache, main_sha):
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": CACHE_VERSION, "main": main_sha, "prs": cache},
                            indent=1, sort_keys=True))


# ---------------------------------------------------------------- scan

def collect(cfg, gh, args):
    now = datetime.now(timezone.utc)
    prs = gh.paginate("/repos/%s/pulls?state=open&sort=created&direction=desc" % gh.repo)
    log("open PRs: %d" % len(prs))
    main_sha = gh.get("/repos/%s/commits/main" % gh.repo)["sha"]
    log("main: %s" % main_sha[:10])

    # overlap detection via the main-side compare is only worth its cost inside
    # the actionable rebase window; a PR 600 commits behind is a stale-base
    # close-candidate regardless of overlap.
    overlap_window = 100

    cache = load_scan_cache(getattr(args, "cache", None))
    lock = threading.Lock()
    stats = {"refreshed": 0, "cache_hits": 0, "compares": 0, "degraded": 0}

    # Only PRs whose (head, main) pair is not cached need their diff and
    # mergeability re-read. An event that changes no PR head (an issue being
    # opened/labelled, a title edit) then costs the REST list alone.
    missing = [pr["number"] for pr in prs
               if cache_entry(cache, pr["number"], pr["head"]["sha"], main_sha) is None]
    log("  diff refresh: %d, cached: %d" % (len(missing), len(prs) - len(missing)))
    owner, name = gh.repo.split("/", 1)
    diffs = {}
    for i in range(0, len(missing), DIFF_BATCH):
        chunk = missing[i:i + DIFF_BATCH]
        try:
            data = gh.graphql(diff_query(chunk), {"owner": owner, "name": name})
        except (RuntimeError, RateGuard) as e:
            log("  ! graphql batch (%d PRs) failed: %s" % (len(chunk), e))
            continue
        repo = data.get("repository") or {}
        for j, n in enumerate(chunk):
            node = repo.get("p%d" % j)
            if node:
                diffs[n] = diff_from_graphql(node)

    def fetch_meta(pr, d):
        """Diff/mergeability/behind for one changed PR."""
        n, head_sha = pr["number"], pr["head"]["sha"]
        paths = list(d["file_paths"])
        adds, dels = sum(d["file_additions"]), sum(d["file_deletions"])
        if d["files_truncated"]:
            files = gh.paginate("/repos/%s/pulls/%d/files?per_page=100" % (gh.repo, n))
            paths = [f["filename"] for f in files]
            adds = sum(f.get("additions", 0) or 0 for f in files)
            dels = sum(f.get("deletions", 0) or 0 for f in files)
        behind_by, merge_base, overlap = 0, None, []
        # mergeStateStatus cannot stand in for behind_by: a CLEAN PR can still
        # be behind (seen on #1040 while profiling this) and DIRTY PRs are
        # routinely hundreds of commits behind. Compare every refreshed PR.
        if gh.quota_exhausted:
            behind_by = None
        else:
            try:
                cmp1 = gh.get("/repos/%s/compare/%s...%s" % (gh.repo, main_sha, head_sha))
            except RateGuard as e:
                # Degrade instead of aborting: the board is more valuable
                # with an honest "behind-base unknown" than not at all.
                gh.quota_exhausted = True
                behind_by = None
                log("  ! quota: %s (behind-base unknown from here on)" % e)
            else:
                with lock:
                    stats["compares"] += 1
                if cmp1:
                    behind_by = cmp1.get("behind_by", 0) or 0
                    merge_base = (cmp1.get("merge_base_commit") or {}).get("sha")
                    if 0 < behind_by <= overlap_window and merge_base:
                        try:
                            cmp2 = gh.get("/repos/%s/compare/%s...%s?per_page=250"
                                          % (gh.repo, merge_base, main_sha))
                        except RateGuard as e:
                            gh.quota_exhausted = True
                            log("  ! quota on overlap compare: %s" % e)
                        else:
                            main_paths = {f["filename"] for f in (cmp2 or {}).get("files", [])}
                            overlap = sorted(set(paths) & main_paths)[:8]
        return {
            "head": head_sha, "main": main_sha,
            "mergeable": d["mergeable"], "mergeable_state": d["mergeable_state"],
            "additions": adds, "deletions": dels, "files": len(paths),
            "paths_sample": sorted(paths)[:12],
            "tests_included": any(
                "tests/" in p or "/corpus" in p or p.endswith("_reject.hy") or p.endswith("_accept.hy")
                for p in paths),
            "behind_by": behind_by, "merge_base": merge_base, "overlap": overlap,
        }

    def one(pr):
        n = pr["number"]
        meta = cache_entry(cache, n, pr["head"]["sha"], main_sha)
        if meta is not None:
            with lock:
                stats["cache_hits"] += 1
        elif n in diffs:
            meta = fetch_meta(pr, diffs[n])
            with lock:
                stats["refreshed"] += 1
                # UNKNOWN mergeability is async on GitHub's side; do not pin it.
                if meta["behind_by"] is not None and meta["mergeable"] is not None:
                    cache[str(n)] = meta
        else:
            # GraphQL batch failed for this PR: keep it on the board with an
            # explicit unknown instead of dropping it.
            with lock:
                stats["degraded"] += 1
            meta = {"head": pr["head"]["sha"], "main": main_sha, "mergeable": None,
                    "mergeable_state": "unknown", "additions": 0, "deletions": 0,
                    "files": 0, "paths_sample": [], "tests_included": False,
                    "behind_by": None, "merge_base": None, "overlap": []}
        return {
            "number": n,
            "title": (pr.get("title") or "")[:90],
            "author": ((pr.get("user") or {}).get("login") or "unknown"),
            "created_at": pr["created_at"],
            "updated_at": pr["updated_at"],
            "age_days": age_days(pr["created_at"], now),
            "idle_days": age_days(pr["updated_at"], now),
            "draft": bool(pr.get("draft")),
            "mergeable": meta["mergeable"],
            "mergeable_state": meta["mergeable_state"],
            "head_sha": pr["head"]["sha"][:12],
            "files": meta["files"],
            "paths_sample": meta["paths_sample"],
            "additions": meta["additions"],
            "deletions": meta["deletions"],
            "behind_by": meta["behind_by"],
            "behind_known": meta["behind_by"] is not None,
            "overlap_risk": meta["overlap"],
            "tests_included": meta["tests_included"],
        }

    items = []
    workers = getattr(args, "jobs", 6)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(one, pr): pr["number"] for pr in prs}
        for fut in as_completed(futs):
            it = fut.result()
            items.append(it)
            if len(items) % 20 == 0 or len(items) == len(prs):
                log("  ... %d/%d scanned" % (len(items), len(prs)))
    items.sort(key=lambda x: -x["number"])
    log("  refreshed %d, cached %d, compares %d, degraded %d" % (
        stats["refreshed"], stats["cache_hits"], stats["compares"], stats["degraded"]))
    live = {str(it["number"]) for it in items}
    cache = {k: v for k, v in cache.items() if k in live}
    return items, main_sha, cache




def classify(cfg, items):
    """Sets bucket/score/dup_of/labels on each item; returns (items, clusters)."""
    W = set(cfg["wildcard_holds"])
    D = set(cfg["drafts_parked"])

    # duplicate clusters: union-find over file-set Jaccard
    parent = list(range(len(items)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    sets = [set(i["paths_sample"]) for i in items]  # sample keeps it cheap; sample=12 paths
    for a in range(len(items)):
        for b in range(a + 1, len(items)):
            if not sets[a] or not sets[b]:
                continue
            inter = len(sets[a] & sets[b])
            union = len(sets[a] | sets[b])
            if union and inter / union >= cfg["dup_jaccard"]:
                parent[find(a)] = find(b)
    clusters = {}
    for i, it in enumerate(items):
        clusters.setdefault(find(i), []).append(it)
    for members in clusters.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda x: (-x["additions"], x["number"]))
        primary = members[0]
        for m in members[1:]:
            m["dup_of"] = primary["number"]
            primary.setdefault("dup_members", []).append(m["number"])

    for it in items:
        n, lab = it["number"], []
        it["wildcard_hold"] = n in W
        it["draft_parked"] = it["draft"] or n in D
        s = 0
        if it["tests_included"]:
            s += 3
            lab.append("triage:tests")
        if it["mergeable"] is True:
            s += 2
        if it["mergeable"] is False:
            s -= 2
        if not it["wildcard_hold"] and not it["draft_parked"]:
            s += 2
        if it["additions"] < 400:
            s += 1
        if it["additions"] > cfg["mega_additions"]:
            s -= 1
        if it["behind_by"] == 0:
            s += 1
        elif it["behind_by"] is None:
            # Degraded scan: base distance unknown — score it like a stale
            # base rather than rewarding it as up-to-date.
            s -= 1
        elif it["behind_by"] >= cfg["behind_threshold"]:
            s -= 1
        if it.get("dup_of"):
            s -= 1
        it["score"] = s

        if it["wildcard_hold"]:
            it["bucket"] = "PARKED-WILDCARD"
            lab.append("triage:wildcard-hold")
        elif it["draft_parked"]:
            it["bucket"] = "PARKED-DRAFT"
        elif it["mergeable"] is False:
            it["bucket"] = "CONFLICTS"
            lab.append("triage:conflicts")
        elif it["behind_by"] is not None and it["behind_by"] >= cfg["behind_threshold"]:
            it["bucket"] = "STALE-BASE"
            lab.append("triage:behind")
        elif it.get("dup_of"):
            it["bucket"] = "DUP"
            lab.append("triage:duplicate")
        elif s >= 6:
            it["bucket"] = "LAND-CANDIDATE"
            lab.append("triage:pareto")
        else:
            it["bucket"] = "RECON"
        if it["age_days"] <= cfg["new_arrival_days"]:
            lab.append("triage:new")
            it["bucket_new"] = True
        if it.get("overlap_risk"):
            lab.append("triage:behind")
        it["labels"] = sorted(set(lab))

        # recommended action
        if it["wildcard_hold"]:
            it["action"] = "PARK (M1 wildcard hold — verdict 2026-11-01, do not merge)"
        elif it["draft_parked"]:
            it["action"] = "PARK (draft — leave with author)"
        elif it["mergeable"] is False:
            it["action"] = "Rebase or close-with-thanks (conflicts vs main)"
        elif it["behind_by"] is not None and it["behind_by"] >= cfg["behind_threshold"]:
            it["action"] = "Ask author to update branch (behind main by %d)" % it["behind_by"]
        elif it["behind_by"] is None:
            it["action"] = "Base distance unknown (rate-limited scan) — re-check before landing"
        elif it.get("dup_of"):
            it["action"] = "DUP of #%d — verify both takes, land strongest, credit both" % it["dup_of"]
        elif it["bucket"] == "LAND-CANDIDATE":
            it["action"] = "Verify (merge gate -> repro -> corpus -> differential) then land"
        else:
            it["action"] = "Recon: read diff, check for subsumption/real content"
    return items, [sorted(m["number"] for m in v) for v in clusters.values() if len(v) > 1]


def merge_gate(items, main_sha, top_k):
    """Worktree merge gate on the top-K land-candidates. Returns per-PR verdicts."""
    cands = sorted([i for i in items if i["bucket"] == "LAND-CANDIDATE"],
                   key=lambda x: (-x["score"], x["additions"]))[:top_k]
    repo = Path(__file__).resolve().parent.parent
    verdicts = {}
    for it in cands:
        n, wt = it["number"], "/tmp/tb-wt-%d" % it["number"]
        try:
            subprocess.run(["git", "-C", str(repo), "fetch", "-q", "origin",
                            "+refs/pull/%d/head" % n], check=True, timeout=120)
            subprocess.run(["git", "-C", str(repo), "worktree", "add", "--detach", wt,
                            "FETCH_HEAD"], check=True, capture_output=True, timeout=60)
            r = subprocess.run(["git", "-C", wt, "merge", "--no-edit", "--no-ff", main_sha],
                               capture_output=True, text=True, timeout=120)
            out = r.stdout + r.stderr
            if r.returncode == 0:
                verdicts[n] = "MERGE-CLEAN"
            elif "CONFLICT" in out or "Automatic merging failed" in out:
                verdicts[n] = "MERGE-CONFLICT"
            else:
                verdicts[n] = "MERGE-ERROR: " + out.strip().splitlines()[-1][:120]
        except subprocess.CalledProcessError as e:
            verdicts[n] = "SETUP-FAIL: " + (e.stderr or "")[-160:]
        finally:
            subprocess.run(["git", "-C", str(repo), "worktree", "remove", "--force", wt],
                           capture_output=True, timeout=60)
    subprocess.run(["git", "-C", str(repo), "worktree", "prune"], capture_output=True)
    for it in items:
        if it["number"] in verdicts:
            it["merge_gate"] = verdicts[it["number"]]
    return verdicts


def render_board(cfg, items, clusters, verdicts, now_s):
    buckets = {}
    for it in items:
        buckets.setdefault(it["bucket"], []).append(it)
    cand = sorted(buckets.get("LAND-CANDIDATE", []), key=lambda x: (-x["score"], x["additions"]))
    pareto = cand[:cfg["pareto_n"]]
    L = ["# Triage Board", "", "Generated %s by `tools/triage_bot.py scan` — review-first automation." % now_s,
         "The bot never merges and never closes. Buckets: %s." % ", ".join(
             "`%s` x%d" % (k, len(v)) for k, v in sorted(buckets.items())),
         ""]
    degraded = [i for i in items if not i.get("behind_known", True)]
    if degraded:
        L += ["> **Degraded scan**: %d PR(s) have an unknown base distance (API rate limit). "
              "Their `behind` cell reads `?` and their score is discounted — re-check the base "
              "before landing anything from this snapshot." % len(degraded), ""]

    if verdicts:
        L += ["## Merge gate (top land-candidates, worktree merge vs main)", ""]
        L += ["| PR | verdict |", "|---|---|"]
        L += ["| #%d | %s |" % (n, v) for n, v in sorted(verdicts.items())]
        L.append("")

    def table(rows, note=None):
        out = []
        if not rows:
            if note:
                out += [note, ""]
            return out
        out += ["| PR | title | author | age | +lines | files | tests | behind | score | action |",
                "|---|---|---|---|---|---|---|---|---|---|"]
        for it in rows:
            behind = it["behind_by"]
            out.append("| #%d | %s | %s | %sd | +%d | %d | %s | %s | %d | %s |" % (
                it["number"], it["title"].replace("|", "\\|"), it["author"], it["age_days"],
                it["additions"], it["files"], "Y" if it["tests_included"] else "-",
                "?" if behind is None else behind, it["score"], it["action"]))
        out.append("")
        return out

    L += ["## Pareto NOW (top %d)" % cfg["pareto_n"], ""]
    L += table(pareto, "_none this pass — run recon on the RECON bucket_")
    L += ["## Land-candidates (rest)", ""]
    L += table(cand[cfg["pareto_n"]:], "_none_")
    L += ["## Conflicts vs main", ""]
    L += table(buckets.get("CONFLICTS", []), "_none_")
    L += ["## Stale base (behind main >= %d)" % cfg["behind_threshold"], ""]
    L += table(buckets.get("STALE-BASE", []), "_none_")
    L += ["## Duplicate clusters (verify both takes, land strongest, credit both)", ""]
    if clusters:
        L += ["- " + ", ".join("#%d" % n for n in c) for c in clusters]
        L.append("")
    else:
        L += ["_none_", ""]
    new = [i for i in items if i.get("bucket_new")]
    L += ["## New arrivals (<= %dd)" % cfg["new_arrival_days"], ""]
    L += table(new, "_none_")
    wl = [i for i in items if i["wildcard_hold"]]
    L += ["## Wildcard holds (M1 — do NOT merge before 2026-11-01 verdicts)", ""]
    L += ["- " + ", ".join("#%d" % i["number"] for i in sorted(wl, key=lambda x: x["number"]))
          + ("  (%d PRs)" % len(wl) if wl else ""), ""]
    L += ["## Recon queue (everything else)", ""]
    L += table(buckets.get("RECON", []), "_none_")
    L += ["<!-- %s -->" % BOARD_MARKER]
    return "\n".join(L), pareto


def cmd_scan(args):
    cfg = load_cfg()
    gh = Gh(cfg["repo"], load_token(args), min_remaining=150)
    items, main_sha, cache = collect(cfg, gh, args)
    items, clusters = classify(cfg, items)
    verdicts = {}
    if args.verify:
        verdicts = merge_gate(items, main_sha, args.verify)
        log("merge gate: %s" % verdicts)
    now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    md, pareto = render_board(cfg, items, clusters, verdicts, now_s)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "TRIAGE.md").write_text(md)
    (out / "TRIAGE.json").write_text(json.dumps(
        {"generated": now_s, "main": main_sha, "clusters": clusters, "items": items},
        indent=1, ensure_ascii=False))
    if getattr(args, "cache", None):
        save_scan_cache(args.cache, cache, main_sha)
    log("\nwrote %s/TRIAGE.md and TRIAGE.json (%d API calls + %d GraphQL queries, %s remaining)" %
        (out, gh.calls, gh.gql_calls, gh.remaining))
    for it in pareto:
        log("  PARETO: #%-4d score %d  %s" % (it["number"], it["score"], it["title"][:70]))


# ---------------------------------------------------------------- approve

def cmd_approve(args):
    cfg = load_cfg()
    gh = Gh(cfg["repo"], load_token(args))
    prs = gh.paginate("/repos/%s/pulls?state=open&per_page=100" % gh.repo)
    heads = {pr["head"]["sha"]: pr["number"] for pr in prs}
    runs = gh.paginate("/repos/%s/actions/runs?event=pull_request&status=action_required&per_page=100" % gh.repo,
                       key="workflow_runs")
    todo, seen = [], set()
    for r in runs:
        sha = r.get("head_sha", "")
        if sha in heads and sha not in seen:
            seen.add(sha)
            todo.append((r["id"], heads[sha], r["name"]))
    log("action_required runs for open PR heads: %d (cap %s)" % (len(todo), args.max))
    ok = 0
    for run_id, n, name in todo[:args.max]:
        if args.dry:
            log("  DRY would approve: #%d %s (%d)" % (n, name, run_id))
            continue
        try:
            gh.req("POST", "/repos/%s/actions/runs/%d/approve" % (gh.repo, run_id))
            ok += 1
            log("  approved: #%d %s" % (n, name))
        except (RuntimeError, RateGuard) as e:
            log("  FAIL #%d: %s" % (n, e))
            if isinstance(e, RateGuard):
                break
    log("approved %d/%d (%d calls used, %s remaining)" % (ok, len(todo), gh.calls, gh.remaining))


# ---------------------------------------------------------------- labels

def cmd_labels(args):
    cfg = load_cfg()
    gh = Gh(cfg["repo"], load_token(args))
    for name, meta in cfg["labels"].items():
        if args.dry:
            continue
        try:
            gh.post("/repos/%s/labels" % gh.repo,
                    {"name": name, "color": meta["color"], "description": meta["description"]})
        except RuntimeError as e:
            if "422" not in str(e):
                log("  label %s: %s" % (name, e))
    log("palette ensured (%d calls)" % gh.calls)
    items = json.loads(Path(args.from_scan).read_text())["items"] if args.from_scan else None
    if items is None:
        items, _ = classify(cfg, collect(cfg, gh, args)[0])
    for it in items:
        want = it["labels"]
        if not want:
            continue
        if args.dry:
            log("  #%d -> %s" % (it["number"], ",".join(want)))
            continue
        try:
            gh.post("/repos/%s/issues/%d/labels" % (gh.repo, it["number"]), {"labels": want})
        except RuntimeError as e:
            log("  FAIL #%d: %s" % (it["number"], e))
    log("labels applied (%d calls total, %s remaining)" % (gh.calls, gh.remaining))


# ---------------------------------------------------------------- board

def cmd_board(args):
    cfg = load_cfg()
    pareto_n = args.pareto or cfg["pareto_n"]
    cfg = dict(cfg, pareto_n=pareto_n)
    gh = Gh(cfg["repo"], load_token(args), min_remaining=150)
    if args.from_scan:
        d = json.loads(Path(args.from_scan).read_text())
        items, clusters, now_s = d["items"], d["clusters"], d["generated"]
    else:
        items, main_sha, cache = collect(cfg, gh, args)
        items, clusters = classify(cfg, items)
        now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if getattr(args, "cache", None):
            save_scan_cache(args.cache, cache, main_sha)
    verdicts = {}
    for it in items:
        if it.get("merge_gate"):
            verdicts[it["number"]] = it["merge_gate"]
    md, pareto = render_board(cfg, items, clusters, verdicts, now_s)
    if not args.post:
        print(md)
        return
    _, srch = gh.req("GET", "/search/issues?q=%s" % quote(
        'repo:%s is:issue is:open "%s" in:body' % (cfg["repo"], BOARD_MARKER)))
    hits = (srch or {}).get("items", [])
    if hits:
        n = hits[0]["number"]
        gh.patch("/repos/%s/issues/%d" % (cfg["repo"], n), {"body": md})
        log("board updated on #%d (%d calls, %s remaining)" % (n, gh.calls, gh.remaining))
    else:
        iss = gh.post("/repos/%s/issues" % cfg["repo"],
                      {"title": "Triage Board (auto-updated daily — pin me)",
                       "body": md})
        log("board created: %s (%d calls)" % (iss.get("html_url"), gh.calls))


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--token-file", default=None)
    ap.add_argument("--token-env", default=None, help="read token from this env var")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("scan", help="snapshot + classify + report")
    p.add_argument("--out", default="/tmp/triage")
    p.add_argument("--verify", type=int, default=0, metavar="K",
                   help="run worktree merge gate on top-K land-candidates")
    p.add_argument("--jobs", type=int, default=6, help="parallel fetch workers")
    p.add_argument("--cache", default=None,
                   help="JSON file caching per-PR compare results across runs")
    p.set_defaults(fn=cmd_scan)

    p = sub.add_parser("approve", help="approve action_required CI runs")
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--dry", action="store_true")
    p.set_defaults(fn=cmd_approve)

    p = sub.add_parser("labels", help="ensure + apply triage labels")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--from-scan", default=None, help="reuse TRIAGE.json instead of re-fetching")
    p.add_argument("--jobs", type=int, default=6, help="parallel fetch workers")
    p.add_argument("--cache", default=None,
                   help="JSON file caching per-PR compare results across runs")
    p.set_defaults(fn=cmd_labels)

    p = sub.add_parser("board", help="print or post the triage board")
    p.add_argument("--post", action="store_true")
    p.add_argument("--pareto", type=int, default=None)
    p.add_argument("--jobs", type=int, default=6, help="parallel fetch workers")
    p.add_argument("--from-scan", default=None, help="reuse TRIAGE.json instead of re-fetching")
    p.add_argument("--cache", default=None,
                   help="JSON file caching per-PR compare results across runs")
    p.set_defaults(fn=cmd_board)

    args = ap.parse_args()
    if args.cmd == "approve" and args.max is None:
        args.max = load_cfg()["approve_max_per_run"]
    if args.cmd == "labels":
        args.dry = not args.apply
    args.fn(args)


if __name__ == "__main__":
    main()
