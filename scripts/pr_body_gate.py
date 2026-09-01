#!/usr/bin/env python3
"""Fail when a PR description lacks mermaid + behavior, or ships off main.

Recurring AGENTS.md bullets were the symptom. This script is the gate.

Requires:
  - a mermaid fence (```mermaid)
  - a `When a caller` behavior line

Ship PRs target main. chaos pack missed main because #3 targeted a feature branch
(`phase-2/resume` after #2 hit main). Unless title/body contains `[stack]`,
fail when base != main.

Env (CI): PR_BODY, PR_TITLE, PR_BASE
Or flags: --body-file / --title / --base
"""

from __future__ import annotations

import argparse
import os
import sys


def evaluate(body: str, title: str = "", base: str = "") -> list[str]:
    errors: list[str] = []
    if "```mermaid" not in body:
        errors.append("PR body must contain a mermaid fence (```mermaid)")
    if "When a caller" not in body:
        errors.append("PR body must contain a 'When a caller' behavior line")
    stacked = "[stack]" in title or "[stack]" in body
    if base and base != "main" and not stacked:
        errors.append(
            f"PR base is {base!r}, not main. Chaos pack missed main because "
            "#3 targeted a feature branch. Put [stack] in the title or body "
            "only for an intentional stacked PR."
        )
    return errors


def _self_test() -> int:
    good = (
        "## Architecture\n```mermaid\nflowchart TB\n```\n"
        "When a caller runs start, it used to Y. Now it Z.\n"
    )
    cases = [
        (good, "Phase 3", "main", []),
        ("When a caller runs start, it used to Y. Now it Z.\n", "t", "main", ["mermaid"]),
        ("```mermaid\nflowchart TB\n```\n", "t", "main", ["When a caller"]),
        (good, "chaos", "phase-2/resume", ["not main"]),
        (good, "[stack] chaos onto resume", "phase-2/resume", []),
        (good + "\n[stack]\n", "chaos", "phase-2/resume", []),
        ("", "t", "main", ["mermaid", "When a caller"]),
    ]
    failed = 0
    for body, title, base, want_sub in cases:
        got = evaluate(body, title, base)
        ok = True
        if not want_sub and got:
            ok = False
        for needle in want_sub:
            if not any(needle in e for e in got):
                ok = False
        if not ok:
            print(f"FAIL self-test title={title!r} base={base!r} got={got}", file=sys.stderr)
            failed += 1
    if failed:
        return 1
    print("pr_body_gate self-test: ok")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--body-file")
    ap.add_argument("--title", default="")
    ap.add_argument("--base", default="")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return _self_test()

    if args.body_file:
        body = open(args.body_file, encoding="utf-8").read()
    else:
        body = os.environ.get("PR_BODY", "")
    title = args.title or os.environ.get("PR_TITLE", "")
    base = args.base or os.environ.get("PR_BASE", "")
    errors = evaluate(body, title, base)
    if errors:
        print("FAIL: PR description gate", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    print("pr_body_gate: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
