#!/usr/bin/env python3
"""C4 SSOT lint for docs/ tree.

Exit 0 = all checks pass. Exit 1 = one or more violations.
Depends only on the Python 3.8+ standard library.

Checks:
  1. root-markdown-whitelist    — only CLAUDE.md, README.md, AGENTS.md at repo root
  2. forbidden-diagram-dialects — C4Context / C4Container / plantuml / @startuml = 0
  3. mermaid-blocks-parseable   — every ```mermaid fence has a known diagram type
  4. index-trigger-targets      — all [..](path) links in _index.md resolve
  5. relative-links-resolvable  — no broken relative links in any docs/*.md
  6. mermaid-provenance-headers — every ```mermaid block preceded by last-verified + code-ref
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
ROOT_MD_ALLOWED = {"CLAUDE.md", "README.md", "AGENTS.md"}
VALID_DIAGRAM_TYPES = {
    "flowchart", "graph",
    "sequenceDiagram",
    "stateDiagram-v2", "stateDiagram",
    "erDiagram",
    "classDiagram",
    "gantt",
}

# ─────────────────────────────────────────────────────────────────────────────
# Check 1: root markdown whitelist
# ─────────────────────────────────────────────────────────────────────────────

def check_root_markdown_whitelist() -> list[str]:
    """Returns list of forbidden root .md filenames."""
    found = {p.name for p in ROOT.glob("*.md")}
    return sorted(found - ROOT_MD_ALLOWED)


# ─────────────────────────────────────────────────────────────────────────────
# Check 2: forbidden diagram dialects
# ─────────────────────────────────────────────────────────────────────────────

_FORBIDDEN_RE = re.compile(
    r"\b(C4Context|C4Container|plantuml|@startuml)\b", re.IGNORECASE
)


def check_forbidden_diagram_dialects() -> list[tuple[Path, int, str]]:
    """Returns (file, line_no, matched_text) for each forbidden token."""
    violations: list[tuple[Path, int, str]] = []
    for md in DOCS.rglob("*.md"):
        for i, line in enumerate(md.read_text(encoding="utf-8").splitlines(), 1):
            m = _FORBIDDEN_RE.search(line)
            if m:
                violations.append((md, i, m.group()))
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Check 3: mermaid blocks parseable (static — no mmdc dependency)
# ─────────────────────────────────────────────────────────────────────────────

_FENCE_OPEN = re.compile(r"^```mermaid\s*$")
_FENCE_CLOSE = re.compile(r"^```\s*$")


def check_mermaid_blocks_parseable() -> list[tuple[Path, int, str]]:
    """Returns (file, fence_line_no, reason) for malformed mermaid blocks."""
    violations: list[tuple[Path, int, str]] = []
    for md in DOCS.rglob("*.md"):
        lines = md.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            if _FENCE_OPEN.match(lines[i]):
                start = i
                body: list[str] = []
                i += 1
                while i < len(lines) and not _FENCE_CLOSE.match(lines[i]):
                    body.append(lines[i])
                    i += 1
                if i >= len(lines):
                    violations.append((md, start + 1, "unclosed mermaid fence"))
                    break
                first_non_empty = next(
                    (b.strip() for b in body if b.strip()), ""
                )
                head = first_non_empty.split()[0] if first_non_empty else ""
                if head not in VALID_DIAGRAM_TYPES:
                    violations.append(
                        (md, start + 1, f"unknown diagram type: {head!r}")
                    )
            i += 1
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Check 4: _index.md trigger targets exist
# ─────────────────────────────────────────────────────────────────────────────

_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def check_index_trigger_targets_exist() -> list[tuple[Path, str]]:
    """Verify every [..](path) link in docs/_index.md resolves to an existing file."""
    idx = DOCS / "_index.md"
    if not idx.exists():
        return [(DOCS / "_index.md", "_index.md does not exist yet")]
    missing: list[tuple[Path, str]] = []
    for m in _LINK_RE.finditer(idx.read_text(encoding="utf-8")):
        target = m.group(2).split("#", 1)[0]
        if not target or target.startswith(("http://", "https://", "mailto:")):
            continue
        resolved = (idx.parent / target).resolve()
        if not resolved.exists():
            missing.append((idx, target))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Check 5: relative links resolvable
# ─────────────────────────────────────────────────────────────────────────────

def check_relative_links_resolvable() -> list[tuple[Path, str]]:
    """Returns (file, target) for each broken relative markdown link in docs/."""
    broken: list[tuple[Path, str]] = []
    for md in DOCS.rglob("*.md"):
        for m in _LINK_RE.finditer(md.read_text(encoding="utf-8")):
            target = m.group(2).split("#", 1)[0]
            if (
                not target
                or target.startswith(("http://", "https://", "mailto:", "#"))
            ):
                continue
            resolved = (md.parent / target).resolve()
            if not resolved.exists():
                broken.append((md, target))
    return broken


# ─────────────────────────────────────────────────────────────────────────────
# Check 6: mermaid provenance headers
# ─────────────────────────────────────────────────────────────────────────────

_LV_RE = re.compile(r"<!--\s*last-verified:\s*\d{4}-\d{2}-\d{2}\s*-->")
_CR_RE = re.compile(r"<!--\s*code-ref:\s*\S")


def check_mermaid_provenance_headers() -> list[tuple[Path, int]]:
    """Returns (file, fence_line_no) where a mermaid block lacks provenance headers.

    Each ```mermaid block must have both:
      <!-- last-verified: YYYY-MM-DD -->
      <!-- code-ref: <path> -->
    within the 5 lines immediately preceding the fence opener.
    """
    missing: list[tuple[Path, int]] = []
    for md in DOCS.rglob("*.md"):
        lines = md.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            if _FENCE_OPEN.match(line):
                preamble = "\n".join(lines[max(0, i - 5): i])
                if not (_LV_RE.search(preamble) and _CR_RE.search(preamble)):
                    missing.append((md, i + 1))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

CHECKS = [
    ("root-markdown-whitelist",   check_root_markdown_whitelist),
    ("forbidden-diagram-dialects", check_forbidden_diagram_dialects),
    ("mermaid-blocks-parseable",  check_mermaid_blocks_parseable),
    ("index-trigger-targets",     check_index_trigger_targets_exist),
    ("relative-links-resolvable", check_relative_links_resolvable),
    ("mermaid-provenance-headers", check_mermaid_provenance_headers),
]

_MAX_SHOWN = 20


def _fmt(v: object) -> str:
    if isinstance(v, tuple):
        parts = []
        for x in v:
            parts.append(str(x.relative_to(ROOT)) if isinstance(x, Path) else str(x))
        return "  " + "  ".join(parts)
    return "  " + str(v)


def main() -> int:
    fail = 0
    for name, fn in CHECKS:
        violations = fn()
        if violations:
            fail += 1
            print(f"FAIL [{name}]  ({len(violations)} violation(s))")
            for v in violations[:_MAX_SHOWN]:
                print(_fmt(v))
            if len(violations) > _MAX_SHOWN:
                print(f"  ... and {len(violations) - _MAX_SHOWN} more")
        else:
            print(f"PASS [{name}]")
    if fail:
        print(f"\n{fail}/{len(CHECKS)} check(s) failed.")
    else:
        print(f"\nAll {len(CHECKS)} checks passed.")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
