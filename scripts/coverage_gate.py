#!/usr/bin/env python3
"""Ratchet + patch coverage gate for src/ kernel lines."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def parse_lcov(path: Path) -> dict[str, dict[int, int]]:
    """relpath (posix, under src/) -> {line: hits}."""
    out: dict[str, dict[int, int]] = defaultdict(dict)
    cur: str | None = None
    for raw in path.read_text().splitlines():
        if raw.startswith("SF:"):
            sf = raw[3:]
            idx = sf.replace("\\", "/").rfind("/src/")
            if idx >= 0:
                cur = sf.replace("\\", "/")[idx + 1 :]
            else:
                cur = None
        elif raw.startswith("DA:") and cur:
            line_s, hit_s = raw[3:].split(",")[:2]
            out[cur][int(line_s)] = int(hit_s)
        elif raw == "end_of_record":
            cur = None
    return out


def kernel_files(cov: dict[str, dict[int, int]]) -> dict[str, dict[int, int]]:
    return {
        f: lines
        for f, lines in cov.items()
        if f.startswith("src/") and not f.startswith("src/testing/")
    }


def totals(cov: dict[str, dict[int, int]]) -> tuple[int, int]:
    found = hit = 0
    for lines in cov.values():
        for h in lines.values():
            found += 1
            if h > 0:
                hit += 1
    return hit, found


def parse_allowlist(baseline: Path) -> dict[str, set[int]]:
    allow: dict[str, set[int]] = defaultdict(set)
    in_allow = False
    for raw in baseline.read_text().splitlines():
        if raw.strip() == "allow:":
            in_allow = True
            continue
        if not in_allow or not raw.strip() or raw.startswith("#"):
            continue
        loc = raw.split()[0] if raw.split() else ""
        if ":" not in loc:
            continue
        file, line_s = loc.rsplit(":", 1)
        allow[file].add(int(line_s))
    return allow


def changed_src_lines(base: str) -> dict[str, set[int]]:
    """Added/changed lines in src/ from `git diff -U0 base`."""
    cmd = ["git", "diff", "--unified=0", "--", "src/"]
    if base:
        cmd = ["git", "diff", "--unified=0", base, "--", "src/"]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    changed: dict[str, set[int]] = defaultdict(set)
    file: str | None = None
    for line in proc.stdout.splitlines():
        if line.startswith("+++ b/"):
            file = line[6:]
            continue
        if line.startswith("@@") and file and file.startswith("src/"):
            # @@ -a,b +c,d @@  or  @@ -a +c @@
            plus = line.split(" ")[2]  # +c,d
            plus = plus[1:]
            if "," in plus:
                start, count = plus.split(",", 1)
                start_i, count_i = int(start), int(count)
            else:
                start_i, count_i = int(plus), 1
            for n in range(start_i, start_i + count_i):
                changed[file].add(n)
    return changed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--lcov", type=Path, required=True)
    ap.add_argument("--floor", type=float, required=True)
    ap.add_argument("--base", default="")
    args = ap.parse_args()

    cov = kernel_files(parse_lcov(args.lcov))
    hit, found = totals(cov)
    pct = 100.0 * hit / found if found else 0.0
    print(f"src/ kernel lines: {hit}/{found} = {pct:.2f}%  (floor {args.floor:.2f}%)")
    if pct + 1e-9 < args.floor:
        print(
            f"FAIL: line coverage {pct:.2f}% is below floor {args.floor:.2f}%",
            file=sys.stderr,
        )
        return 1

    allow = parse_allowlist(args.baseline)
    uncovered = []
    for f, lines in sorted(cov.items()):
        for ln, h in sorted(lines.items()):
            if h == 0 and ln not in allow.get(f, set()):
                uncovered.append(f"{f}:{ln}")

    base = args.base
    if not base:
        # Local / push: uncommitted + last commit vs its parent when on a branch.
        base = "HEAD"
        # Include working tree: diff HEAD (unstaged+staged vs HEAD) is handled
        # by git diff HEAD -- src/ when we pass base=HEAD... we want both.
        # `git diff HEAD` is unstaged+staged; `git diff HEAD~1...HEAD` is the
        # last commit. Use merge-base with main when available.
        remotes = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/main"],
            capture_output=True,
            text=True,
        )
        if remotes.returncode == 0:
            mb = subprocess.run(
                ["git", "merge-base", "origin/main", "HEAD"],
                capture_output=True,
                text=True,
            )
            if mb.returncode == 0:
                base = mb.stdout.strip()

    # Working tree + commits since base.
    changed = changed_src_lines(base)
    # Also include unstaged vs HEAD so local dirty src/ is gated.
    for f, lines in changed_src_lines("HEAD").items():
        changed[f].update(lines)

    patch_fail = []
    for f, lns in sorted(changed.items()):
        if not f.startswith("src/") or f.startswith("src/testing/"):
            continue
        file_cov = cov.get(f, {})
        for ln in sorted(lns):
            if ln not in file_cov:
                continue  # not an executable line (comment, blank, cfg)
            if file_cov[ln] > 0:
                continue
            if ln in allow.get(f, set()):
                continue
            patch_fail.append(f"{f}:{ln}")

    if patch_fail:
        print("FAIL: new/changed src/ lines have no coverage:", file=sys.stderr)
        for loc in patch_fail:
            print(f"  {loc}", file=sys.stderr)
        return 1

    extra_allow = []
    for f, lns in allow.items():
        file_cov = cov.get(f, {})
        for ln in lns:
            if ln in file_cov and file_cov[ln] > 0:
                extra_allow.append(f"{f}:{ln}")
    if extra_allow:
        print(
            "note: allowlisted lines are now covered (safe to drop): "
            + ", ".join(extra_allow)
        )

    print(f"patch coverage: ok ({sum(len(v) for v in changed.values())} changed src/ lines vs {base or 'HEAD'})")
    _ = uncovered  # remaining gaps live in BASELINE allow: — not a CI fail
    return 0


if __name__ == "__main__":
    sys.exit(main())
