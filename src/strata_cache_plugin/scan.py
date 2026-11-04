"""Read-only scanner for a Strata cache directory.

P1 contract, enforced by construction and by tests:

* no file is ever opened - the only filesystem calls are ``os.scandir`` and
  ``stat`` (``lstat``, i.e. symlinks are not followed);
* nothing is written, renamed, deleted or chmod-ed;
* artifact contents are opaque - only names, sizes and mtimes are reported.

Temporary names are recognised but never touched: they belong to a writer that
may still be running, and P1 is a shadow.
"""

from __future__ import annotations

import os
import stat as stat_mod
from dataclasses import dataclass, field
from typing import Any

#: Directories this tool refuses to scan unless the caller opts in explicitly,
#: and even then with ``stat`` only. A root matches itself and everything below
#: it. Matched lexically, never by resolving.
PROTECTED_ROOTS: tuple[str, ...] = ("/models",)

#: Prefixes that are protected as a whole family: every Strata checkout,
#: worktree and build under the operator's home, not just one directory. A
#: prefix matches by plain string comparison, so ``strata`` covers
#: ``strata``, ``strata-v100``, ``strata-patches`` and the plugin's own symlink.
PROTECTED_PREFIXES: tuple[str, ...] = ("/home/g0/strata",)

#: Artifact suffixes the Strata cache writes. Longest first: ``.tokens.json``
#: ends with ``.json``, so a suffix is only ever claimed by one entry here.
ARTIFACT_SUFFIXES: tuple[str, ...] = (
    ".tokens.json",
    ".sess",
    ".meta",
    ".dlog",
    ".dchain",
)

#: The three temporary-name families, in the order they are tested. A hidden
#: ``.<name>.meta.tmp`` matches both the first and the second family; the first
#: match wins, and either way the file is reported as a temporary. Neither
#: family is ever deleted, renamed or opened.
TEMP_DOT_TMP = ".<dot>.tmp"
TEMP_META_TMP = ".meta.tmp"
TEMP_DCHAIN_TMP = ".dchain.tmp"
TEMPORARY_FAMILIES: tuple[str, ...] = (TEMP_DOT_TMP, TEMP_META_TMP, TEMP_DCHAIN_TMP)

KIND_ARTIFACT = "artifact"
KIND_TEMPORARY = "temporary"
KIND_UNKNOWN = "unknown"
KIND_DIRECTORY = "directory"
KIND_SYMLINK = "symlink"
KIND_OTHER = "other"


class ScanError(RuntimeError):
    """The directory could not be scanned."""


def lexical_path(path: str | os.PathLike[str]) -> str:
    """Absolute, normalised path computed without touching the filesystem.

    ``os.path.abspath`` would call ``getcwd`` only; ``realpath`` would ``lstat``
    every component, which is exactly what a protected-root check must not do.
    """
    text = os.fspath(path)
    return os.path.normpath(os.path.join(os.getcwd(), text))


def is_protected_path(path: str | os.PathLike[str]) -> bool:
    """True when *path* is a protected root or lies under one (lexically)."""
    candidate = lexical_path(path)
    for root in PROTECTED_ROOTS:
        if candidate == root or candidate.startswith(root + os.sep):
            return True
    for prefix in PROTECTED_PREFIXES:
        if candidate.startswith(prefix):
            return True
    return False


@dataclass(frozen=True)
class Classification:
    """What a bare file name is, judged from the name alone."""

    kind: str
    label: str  # artifact suffix, temporary family, or "" when neither

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "label": self.label}


def classify_name(name: str) -> Classification:
    """Classify a file name. Never looks at the file itself."""
    if name.startswith(".") and name.endswith(".tmp"):
        return Classification(KIND_TEMPORARY, TEMP_DOT_TMP)
    if name.endswith(TEMP_META_TMP):
        return Classification(KIND_TEMPORARY, TEMP_META_TMP)
    if name.endswith(TEMP_DCHAIN_TMP):
        return Classification(KIND_TEMPORARY, TEMP_DCHAIN_TMP)
    for suffix in ARTIFACT_SUFFIXES:
        if name.endswith(suffix):
            return Classification(KIND_ARTIFACT, suffix)
    return Classification(KIND_UNKNOWN, "")


@dataclass(frozen=True)
class ScanEntry:
    """One directory entry, as reported. Sizes are raw ``lstat`` sizes."""

    name: str
    rel_path: str
    kind: str
    label: str
    size_bytes: int
    mtime_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "rel_path": self.rel_path,
            "kind": self.kind,
            "label": self.label,
            "size_bytes": self.size_bytes,
            "mtime_ns": self.mtime_ns,
        }


@dataclass(frozen=True)
class ScanReport:
    """Aggregates plus the entry list for one scan."""

    root: str
    max_depth: int
    entries: tuple[ScanEntry, ...]
    bytes_by_suffix: dict[str, int]
    files_by_suffix: dict[str, int]
    temporary_bytes_by_family: dict[str, int]
    temporaries_by_family: dict[str, int]
    total_bytes: int
    unknown_bytes: int
    entry_count: int
    file_count: int
    directory_count: int
    symlink_count: int
    temporary_count: int
    unknown_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "root": self.root,
            "max_depth": self.max_depth,
            "entry_count": self.entry_count,
            "file_count": self.file_count,
            "directory_count": self.directory_count,
            "symlink_count": self.symlink_count,
            "temporary_count": self.temporary_count,
            "unknown_count": self.unknown_count,
            "total_bytes": self.total_bytes,
            "unknown_bytes": self.unknown_bytes,
            "bytes_by_suffix": dict(self.bytes_by_suffix),
            "files_by_suffix": dict(self.files_by_suffix),
            "temporary_bytes_by_family": dict(self.temporary_bytes_by_family),
            "temporaries_by_family": dict(self.temporaries_by_family),
            "entries": [entry.to_dict() for entry in self.entries],
        }


@dataclass
class _Totals:
    """Mutable accumulator used while walking; folded into ScanReport."""

    bytes_by_suffix: dict[str, int] = field(
        default_factory=lambda: {suffix: 0 for suffix in ARTIFACT_SUFFIXES}
    )
    files_by_suffix: dict[str, int] = field(
        default_factory=lambda: {suffix: 0 for suffix in ARTIFACT_SUFFIXES}
    )
    temporary_bytes_by_family: dict[str, int] = field(
        default_factory=lambda: {family: 0 for family in TEMPORARY_FAMILIES}
    )
    temporaries_by_family: dict[str, int] = field(
        default_factory=lambda: {family: 0 for family in TEMPORARY_FAMILIES}
    )
    total_bytes: int = 0
    unknown_bytes: int = 0
    file_count: int = 0
    directory_count: int = 0
    symlink_count: int = 0
    temporary_count: int = 0
    unknown_count: int = 0

    def add(self, entry: ScanEntry) -> None:
        if entry.kind == KIND_DIRECTORY:
            self.directory_count += 1
            return
        self.file_count += 1
        self.total_bytes += entry.size_bytes
        if entry.kind == KIND_ARTIFACT:
            self.bytes_by_suffix[entry.label] += entry.size_bytes
            self.files_by_suffix[entry.label] += 1
        elif entry.kind == KIND_TEMPORARY:
            self.temporary_bytes_by_family[entry.label] += entry.size_bytes
            self.temporaries_by_family[entry.label] += 1
            self.temporary_count += 1
        elif entry.kind == KIND_UNKNOWN:
            self.unknown_bytes += entry.size_bytes
            self.unknown_count += 1
        elif entry.kind == KIND_SYMLINK:
            self.symlink_count += 1


def scan_directory(
    path: str | os.PathLike[str],
    *,
    max_depth: int = 1,
) -> ScanReport:
    """Scan *path* and return a report. Depth 1 means "root's entries only".

    Raises :class:`ScanError` when the path is missing, is not a directory, or
    an entry cannot be ``lstat``-ed. P1 prefers a loud failure to partial data.
    """
    if max_depth < 1:
        raise ScanError("max_depth must be >= 1")

    root = lexical_path(path)
    try:
        root_stat = os.stat(root)
    except OSError as exc:
        raise ScanError(f"cannot stat {root}: {exc.strerror or exc}") from exc
    if not stat_mod.S_ISDIR(root_stat.st_mode):
        raise ScanError(f"not a directory: {root}")

    entries: list[ScanEntry] = []
    totals = _Totals()
    _walk(root, "", 1, max_depth, entries, totals)

    return ScanReport(
        root=root,
        max_depth=max_depth,
        entries=tuple(entries),
        bytes_by_suffix=dict(totals.bytes_by_suffix),
        files_by_suffix=dict(totals.files_by_suffix),
        temporary_bytes_by_family=dict(totals.temporary_bytes_by_family),
        temporaries_by_family=dict(totals.temporaries_by_family),
        total_bytes=totals.total_bytes,
        unknown_bytes=totals.unknown_bytes,
        entry_count=len(entries),
        file_count=totals.file_count,
        directory_count=totals.directory_count,
        symlink_count=totals.symlink_count,
        temporary_count=totals.temporary_count,
        unknown_count=totals.unknown_count,
    )


def _walk(
    root: str,
    rel_prefix: str,
    depth: int,
    max_depth: int,
    entries: list[ScanEntry],
    totals: _Totals,
) -> None:
    directory = os.path.join(root, rel_prefix) if rel_prefix else root
    try:
        with os.scandir(directory) as iterator:
            children = sorted(iterator, key=lambda item: item.name)
    except OSError as exc:
        raise ScanError(f"cannot list {directory}: {exc.strerror or exc}") from exc

    subdirectories: list[str] = []
    for child in children:
        rel_path = f"{rel_prefix}/{child.name}" if rel_prefix else child.name
        try:
            child_stat = child.stat(follow_symlinks=False)
        except OSError as exc:
            raise ScanError(
                f"cannot stat {os.path.join(root, rel_path)}: {exc.strerror or exc}"
            ) from exc

        if stat_mod.S_ISDIR(child_stat.st_mode):
            entry = ScanEntry(
                name=child.name,
                rel_path=rel_path,
                kind=KIND_DIRECTORY,
                label="",
                size_bytes=child_stat.st_size,
                mtime_ns=child_stat.st_mtime_ns,
            )
            subdirectories.append(rel_path)
        else:
            if stat_mod.S_ISLNK(child_stat.st_mode):
                classification = Classification(KIND_SYMLINK, "")
            elif stat_mod.S_ISREG(child_stat.st_mode):
                classification = classify_name(child.name)
            else:
                classification = Classification(KIND_OTHER, "")
            entry = ScanEntry(
                name=child.name,
                rel_path=rel_path,
                kind=classification.kind,
                label=classification.label,
                size_bytes=child_stat.st_size,
                mtime_ns=child_stat.st_mtime_ns,
            )
        entries.append(entry)
        totals.add(entry)

    if depth < max_depth:
        for rel_path in subdirectories:
            _walk(root, rel_path, depth + 1, max_depth, entries, totals)


def format_report(report: ScanReport) -> str:
    """Human-readable aggregate table for ``scan`` without ``--json``."""
    lines: list[str] = [
        f"root:  {report.root}",
        f"depth: {report.max_depth}",
        "",
        f"{'suffix':<16}{'files':>8}{'bytes':>16}",
        f"{'-' * 16}{'-' * 8}{'-' * 16}",
    ]
    for suffix, size in report.bytes_by_suffix.items():
        lines.append(f"{suffix:<16}{report.files_by_suffix[suffix]:>8}{size:>16}")

    lines.extend(
        [
            "",
            f"{'temporary':<16}{'count':>8}{'bytes':>16}",
            f"{'-' * 16}{'-' * 8}{'-' * 16}",
        ]
    )
    for family, size in report.temporary_bytes_by_family.items():
        lines.append(f"{family:<16}{report.temporaries_by_family[family]:>8}{size:>16}")

    lines.extend(
        [
            "",
            f"entries:     {report.entry_count}"
            f" ({report.file_count} files, {report.directory_count} dirs,"
            f" {report.symlink_count} symlinks)",
            f"temporaries: {report.temporary_count}",
            f"unknown:     {report.unknown_count} ({report.unknown_bytes} bytes)",
            f"total bytes: {report.total_bytes}  (directory sizes excluded)",
        ]
    )
    return "\n".join(lines)
