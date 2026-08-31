#!/usr/bin/env python3
"""C4 SSOT lint for docs/ tree.

Exit 0 = all checks pass. Exit 1 = one or more violations.
Depends only on the Python 3.8+ standard library.

Checks:
  1. root-markdown-whitelist     — only CLAUDE.md, README.md, AGENTS.md at repo root
  2. forbidden-diagram-dialects  — C4Context / C4Container / plantuml / @startuml = 0
  3. mermaid-blocks-parseable    — every mermaid fence has a known diagram type
  4. index-markdown-links        — all [..](path) links in _index.md resolve
  5. index-backtick-paths        — all `docs/**.md` backtick paths in _index.md resolve
  6. relative-links-resolvable   — no broken relative links in any docs/*.md
  7. mermaid-provenance-headers  — every mermaid block preceded by last-verified + code-ref
  8. code-ref-targets-exist      — every code-ref path actually exists in the repo
  9. banned-refs                 — docs must not instruct use of deleted symbols
 10. code-paths-exist            — trigger-map globs and ADR related_code resolve

Exemptions:
  - File-level:  <!-- lint-docs: allow-missing-code-refs -->  anywhere in the file
  - Frontmatter: allow_missing_refs: true
  - Block-level: <!-- lint-docs: allow-missing-start --> ... <!-- lint-docs: allow-missing-end -->
  - External repos: paths prefixed with a repo name, e.g. `ASM:app/worker/pipeline.py`
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
INDEX = DOCS / "_index.md"

ROOT_MD_ALLOWED = {"CLAUDE.md", "README.md", "AGENTS.md"}

VALID_DIAGRAM_TYPES = {
    "flowchart", "graph",
    "sequenceDiagram",
    "stateDiagram-v2", "stateDiagram",
    "erDiagram",
    "classDiagram",
    "gantt",
}

# Extensions that mark a backtick token as a code path worth verifying.
CODE_EXTS = (
    ".java", ".kt", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs",
    ".py", ".sh", ".sql", ".yml", ".yaml", ".json", ".toml", ".properties",
    ".gradle", ".gradle.kts", ".conf", ".env", ".css", ".html",
)

# Per-project: symbols that were deleted and must not be recommended any more.
# Each entry: (compiled regex, human reason). Start empty; fill in per project.
BANNED_DOC_REFS: list[tuple[re.Pattern, str]] = [
    # Example (Again-Spring):
    # (re.compile(r"->\s*`KeywordGuard`\s*컴포넌트", re.I),
    #  "KeywordGuard 컴포넌트 — lib/utils/keywordGuard.ts 는 삭제됨"),
]


_MAX_SHOWN = 20


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

_FENCE_OPEN = re.compile(r"^\s*```mermaid\s*$")
_FENCE_CLOSE = re.compile(r"^\s*```\s*$")
_LINK_RE = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")
_LV_RE = re.compile(r"<!--\s*last-verified:\s*\d{4}-\d{2}-\d{2}\s*-->")
_CR_RE = re.compile(r"<!--\s*code-ref:\s*(.+?)\s*-->")
_BACKTICK_RE = re.compile(r"`([^`\n]+)`")
_FILE_OPTOUT_RE = re.compile(r"<!--\s*lint-docs:\s*allow-missing-code-refs\s*-->")
_BLOCK_START_RE = re.compile(r"<!--\s*lint-docs:\s*allow-missing-start\s*-->")
_BLOCK_END_RE = re.compile(r"<!--\s*lint-docs:\s*allow-missing-end\s*-->")
_FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---", re.S)
_ALLOW_FM_RE = re.compile(r"^allow_missing_refs:\s*true\s*$", re.M)
# `Word:` prefix that is NOT a line-number suffix — marks an external repository.
_EXTERNAL_PREFIX_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*:(?!\d)")


def _docs_files() -> list[Path]:
    return sorted(DOCS.rglob("*.md")) if DOCS.is_dir() else []


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def _file_is_exempt(text: str) -> bool:
    """True when the whole file opted out of code-path verification."""
    if _FILE_OPTOUT_RE.search(text):
        return True
    fm = _FRONTMATTER_RE.match(text)
    return bool(fm and _ALLOW_FM_RE.search(fm.group(1)))


def _exempt_line_numbers(lines: list[str]) -> set[int]:
    """0-based line numbers inside allow-missing-start/end blocks."""
    exempt: set[int] = set()
    active = False
    for i, line in enumerate(lines):
        if _BLOCK_START_RE.search(line):
            active = True
        if active:
            exempt.add(i)
        if _BLOCK_END_RE.search(line):
            active = False
    return exempt


def _strip_anchor(target: str) -> str:
    return target.split("#", 1)[0].strip()


def _is_skippable_link(target: str) -> bool:
    return (not target) or target.startswith(
        ("http://", "https://", "mailto:", "#", "<", "{")
    )


def _normalise_code_path(token: str) -> str | None:
    """Strip :line / :start-end suffix. Return None when the token is not a path."""
    token = token.strip().strip("`").rstrip(".,;")
    if not token or _EXTERNAL_PREFIX_RE.match(token):
        return None                      # external repo reference — not ours to verify
    token = re.sub(r":\d+(?:-\d+)?$", "", token)
    token = token.lstrip("/")            # '/path' is repo-root-anchored, same as 'path'
    return token or None


def _glob_prefix(path: str) -> str:
    """For a glob, return the longest leading path with no wildcard."""
    parts = path.split("/")
    keep: list[str] = []
    for part in parts:
        if any(ch in part for ch in "*?["):
            break
        keep.append(part)
    return "/".join(keep)


def _path_exists(path: str) -> bool:
    """Existence check that understands globs: the fixed prefix must exist."""
    if any(ch in path for ch in "*?["):
        prefix = _glob_prefix(path)
        return bool(prefix) and (ROOT / prefix).exists()
    return (ROOT / path).exists()


# ─────────────────────────────────────────────────────────────────────────────
# Check 1: root markdown whitelist
# ─────────────────────────────────────────────────────────────────────────────

def check_root_markdown_whitelist() -> list[str]:
    """Forbidden .md at repo root. iterdir() so symlinks are caught too."""
    found = {p.name for p in ROOT.iterdir() if p.name.endswith(".md")}
    return sorted(found - ROOT_MD_ALLOWED)


# ─────────────────────────────────────────────────────────────────────────────
# Check 2: forbidden diagram dialects
# ─────────────────────────────────────────────────────────────────────────────

_FORBIDDEN_RE = re.compile(
    r"\b(C4Context|C4Container|plantuml|@startuml)\b", re.IGNORECASE
)


def check_forbidden_diagram_dialects() -> list[tuple[Path, int, str]]:
    violations: list[tuple[Path, int, str]] = []
    for md in _docs_files():
        for i, line in enumerate(_read(md).splitlines(), 1):
            m = _FORBIDDEN_RE.search(line)
            if m:
                violations.append((md, i, m.group()))
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Check 3: mermaid blocks parseable (static — no mmdc dependency)
# ─────────────────────────────────────────────────────────────────────────────

def check_mermaid_blocks_parseable() -> list[tuple[Path, int, str]]:
    violations: list[tuple[Path, int, str]] = []
    for md in _docs_files():
        lines = _read(md).splitlines()
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
                first = next((b.strip() for b in body if b.strip()), "")
                head = first.split()[0] if first else ""
                if head not in VALID_DIAGRAM_TYPES:
                    violations.append(
                        (md, start + 1, "unknown diagram type: %r" % head)
                    )
            i += 1
    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Check 4: _index.md markdown links resolve
# ─────────────────────────────────────────────────────────────────────────────

def check_index_markdown_links() -> list[tuple[Path, str]]:
    if not INDEX.exists():
        return [(INDEX, "docs/_index.md does not exist")]
    missing: list[tuple[Path, str]] = []
    for m in _LINK_RE.finditer(_read(INDEX)):
        target = _strip_anchor(m.group(2))
        if _is_skippable_link(target):
            continue
        if not (INDEX.parent / target).exists():
            missing.append((INDEX, target))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Check 5: _index.md backtick doc paths resolve   (from Again-Spring check-docs.js)
# ─────────────────────────────────────────────────────────────────────────────

def check_index_backtick_paths() -> list[tuple[Path, str]]:
    """Verify `docs/**.md` written as inline code in _index.md.

    v1 only parsed markdown links, so the trigger-map tables — which use
    backticks — were never verified. This closes that hole.
    """
    if not INDEX.exists():
        return []                        # check 4 already reports the absence
    missing: list[tuple[Path, str]] = []
    for m in _BACKTICK_RE.finditer(_read(INDEX)):
        token = m.group(1).strip()
        if not token.startswith("docs/") or not token.endswith(".md"):
            continue
        if not (ROOT / token).exists():
            missing.append((INDEX, token))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Check 6: relative links resolvable
# ─────────────────────────────────────────────────────────────────────────────

def check_relative_links_resolvable() -> list[tuple[Path, str]]:
    broken: list[tuple[Path, str]] = []
    for md in _docs_files():
        for m in _LINK_RE.finditer(_read(md)):
            target = _strip_anchor(m.group(2))
            if _is_skippable_link(target):
                continue
            if not (md.parent / target).exists():
                broken.append((md, target))
    return broken


# ─────────────────────────────────────────────────────────────────────────────
# Check 7: mermaid provenance headers
# ─────────────────────────────────────────────────────────────────────────────

def check_mermaid_provenance_headers() -> list[tuple[Path, int]]:
    """Each mermaid fence needs both comments within the 5 preceding lines:
         <!-- last-verified: YYYY-MM-DD -->
         <!-- code-ref: <path> -->
    """
    missing: list[tuple[Path, int]] = []
    for md in _docs_files():
        lines = _read(md).splitlines()
        for i, line in enumerate(lines):
            if _FENCE_OPEN.match(line):
                preamble = "\n".join(lines[max(0, i - 5):i])
                if not (_LV_RE.search(preamble) and _CR_RE.search(preamble)):
                    missing.append((md, i + 1))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Check 8: code-ref targets exist                                       [NEW]
# ─────────────────────────────────────────────────────────────────────────────

def check_code_ref_targets_exist() -> list[tuple[Path, int, str]]:
    """v1 only checked that a code-ref comment was present, never that the path
    was real. A diagram whose source file was deleted stayed green forever.
    """
    missing: list[tuple[Path, int, str]] = []
    for md in _docs_files():
        text = _read(md)
        if _file_is_exempt(text):
            continue
        lines = text.splitlines()
        exempt = _exempt_line_numbers(lines)
        for i, line in enumerate(lines):
            if i in exempt:
                continue
            m = _CR_RE.search(line)
            if not m:
                continue
            for raw in m.group(1).split(","):
                path = _normalise_code_path(raw)
                if path and not _path_exists(path):
                    missing.append((md, i + 1, path))
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Check 9: banned refs                          (from Again-Spring check-docs.js)
# ─────────────────────────────────────────────────────────────────────────────

def check_banned_refs() -> list[tuple[Path, str]]:
    """Docs must not tell readers to use a symbol that no longer exists.

    Mentioning a deletion in a '부재하는 것' section is fine — the patterns are
    written to match usage instructions, not bare mentions.
    """
    hits: list[tuple[Path, str]] = []
    if not BANNED_DOC_REFS:
        return hits
    for md in _docs_files():
        text = _read(md)
        if _file_is_exempt(text):
            continue
        for pattern, reason in BANNED_DOC_REFS:
            if pattern.search(text):
                hits.append((md, reason))
    return hits


# ─────────────────────────────────────────────────────────────────────────────
# Check 10: trigger-map globs and ADR related_code resolve              [NEW]
# ─────────────────────────────────────────────────────────────────────────────

def _related_code_entries(text: str) -> list[str]:
    fm = _FRONTMATTER_RE.match(text)
    if not fm:
        return []
    out: list[str] = []
    inside = False
    for line in fm.group(1).splitlines():
        if re.match(r"^related_code:\s*$", line):
            inside = True
            continue
        if inside:
            m = re.match(r"^\s+-\s+(.+?)\s*$", line)
            if m:
                out.append(m.group(1))
            else:
                inside = False
    return out


def check_code_paths_exist() -> list[tuple[Path, str]]:
    """_index.md trigger-map code globs + every doc's related_code frontmatter.

    Bit-Mania's own _index.md confessed this hole: 'the trigger map's code paths
    are not verified — the linter passes even after the code is deleted'.
    Measured on Bit-Mania: 16 of 39 related_code entries pointed at nothing.
    """
    missing: list[tuple[Path, str]] = []

    if INDEX.exists():
        text = _read(INDEX)
        if not _file_is_exempt(text):
            lines = text.splitlines()
            exempt = _exempt_line_numbers(lines)
            for i, line in enumerate(lines):
                if i in exempt:
                    continue
                for m in _BACKTICK_RE.finditer(line):
                    token = m.group(1).strip()
                    if token.startswith("docs/"):
                        continue                 # check 5 owns those
                    if "/" not in token:
                        continue                 # prose, not a path
                    looks_like_code = token.endswith(CODE_EXTS) or "*" in token
                    if not looks_like_code:
                        continue
                    path = _normalise_code_path(token)
                    if path and not _path_exists(path):
                        missing.append((INDEX, path))

    for md in _docs_files():
        text = _read(md)
        if _file_is_exempt(text):
            continue
        for raw in _related_code_entries(text):
            path = _normalise_code_path(raw)
            if path and not _path_exists(path):
                missing.append((md, path))

    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

CHECKS = [
    ("root-markdown-whitelist",    check_root_markdown_whitelist),
    ("forbidden-diagram-dialects", check_forbidden_diagram_dialects),
    ("mermaid-blocks-parseable",   check_mermaid_blocks_parseable),
    ("index-markdown-links",       check_index_markdown_links),
    ("index-backtick-paths",       check_index_backtick_paths),
    ("relative-links-resolvable",  check_relative_links_resolvable),
    ("mermaid-provenance-headers", check_mermaid_provenance_headers),
    ("code-ref-targets-exist",     check_code_ref_targets_exist),
    ("banned-refs",                check_banned_refs),
    ("code-paths-exist",           check_code_paths_exist),
]


def _fmt(v: object) -> str:
    if isinstance(v, tuple):
        parts = [_rel(x) if isinstance(x, Path) else str(x) for x in v]
        return "  " + "  ".join(parts)
    return "  " + str(v)


def main() -> int:
    if not DOCS.is_dir():
        print("FAIL  docs/ directory not found at %s" % _rel(DOCS))
        return 1
    fail = 0
    for name, fn in CHECKS:
        violations = fn()
        if violations:
            fail += 1
            print("FAIL [%s]  (%d violation(s))" % (name, len(violations)))
            for v in violations[:_MAX_SHOWN]:
                print(_fmt(v))
            if len(violations) > _MAX_SHOWN:
                print("  ... and %d more" % (len(violations) - _MAX_SHOWN))
        else:
            print("PASS [%s]" % name)
    if fail:
        print("\n%d/%d check(s) failed." % (fail, len(CHECKS)))
    else:
        print("\nAll %d checks passed." % len(CHECKS))
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
