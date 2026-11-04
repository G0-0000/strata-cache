"""Scanner tests: classification, aggregates, and proof that nothing changed."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from strata_cache_plugin import scan

ARTIFACTS = {
    "alpha.sess": b"a" * 10,
    "alpha.meta": b"b" * 20,
    "alpha.dlog": b"c" * 30,
    "alpha.dchain": b"d" * 40,
    "conv-1.tokens.json": b"e" * 50,
}

TEMPORARIES = {
    ".leaf.0123abcd.tmp": (scan.TEMP_DOT_TMP, b"f" * 5),
    "beta.meta.tmp": (scan.TEMP_META_TMP, b"g" * 6),
    "beta.dchain.tmp": (scan.TEMP_DCHAIN_TMP, b"h" * 7),
}

UNKNOWN = {"notes.txt": b"i" * 8}


def build_fixture(root: Path) -> None:
    for name, payload in ARTIFACTS.items():
        (root / name).write_bytes(payload)
    for name, (_, payload) in TEMPORARIES.items():
        (root / name).write_bytes(payload)
    for name, payload in UNKNOWN.items():
        (root / name).write_bytes(payload)
    (root / "sub").mkdir()
    (root / "sub" / "nested.sess").write_bytes(b"n" * 3)


def manifest(root: Path) -> list[tuple[str, int, int, int]]:
    """Every entry below *root*: relative path, size, mtime_ns, mode."""
    rows: list[tuple[str, int, int, int]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        for name in sorted(dirnames) + sorted(filenames):
            path = Path(dirpath) / name
            info = path.lstat()
            rows.append(
                (str(path.relative_to(root)), info.st_size, info.st_mtime_ns, info.st_mode)
            )
    return sorted(rows)


@pytest.mark.parametrize(
    ("name", "kind", "label"),
    [
        ("alpha.sess", scan.KIND_ARTIFACT, ".sess"),
        ("alpha.meta", scan.KIND_ARTIFACT, ".meta"),
        ("alpha.dlog", scan.KIND_ARTIFACT, ".dlog"),
        ("alpha.dchain", scan.KIND_ARTIFACT, ".dchain"),
        ("conv-1.tokens.json", scan.KIND_ARTIFACT, ".tokens.json"),
        (".leaf.0123abcd.tmp", scan.KIND_TEMPORARY, scan.TEMP_DOT_TMP),
        ("beta.meta.tmp", scan.KIND_TEMPORARY, scan.TEMP_META_TMP),
        ("beta.dchain.tmp", scan.KIND_TEMPORARY, scan.TEMP_DCHAIN_TMP),
        ("notes.txt", scan.KIND_UNKNOWN, ""),
        ("conv-1.json", scan.KIND_UNKNOWN, ""),
        ("session.sess.bak", scan.KIND_UNKNOWN, ""),
    ],
)
def test_classify_name(name: str, kind: str, label: str) -> None:
    assert scan.classify_name(name) == scan.Classification(kind, label)


def test_hidden_temporary_matches_the_first_family() -> None:
    """.x.meta.tmp matches two patterns; the dotted family is tested first."""
    assert scan.classify_name(".x.meta.tmp").label == scan.TEMP_DOT_TMP


def test_aggregates(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    report = scan.scan_directory(tmp_path)

    assert report.root == str(tmp_path)
    assert report.max_depth == 1

    assert report.files_by_suffix == {
        ".tokens.json": 1,
        ".sess": 1,
        ".meta": 1,
        ".dlog": 1,
        ".dchain": 1,
    }
    assert report.bytes_by_suffix == {
        ".tokens.json": 50,
        ".sess": 10,
        ".meta": 20,
        ".dlog": 30,
        ".dchain": 40,
    }
    assert report.temporaries_by_family == {
        scan.TEMP_DOT_TMP: 1,
        scan.TEMP_META_TMP: 1,
        scan.TEMP_DCHAIN_TMP: 1,
    }
    assert report.temporary_bytes_by_family == {
        scan.TEMP_DOT_TMP: 5,
        scan.TEMP_META_TMP: 6,
        scan.TEMP_DCHAIN_TMP: 7,
    }
    assert report.temporary_count == 3
    assert report.unknown_count == 1
    assert report.unknown_bytes == 8
    assert report.symlink_count == 0
    assert report.directory_count == 1
    # 5 artifacts + 3 temporaries + 1 unknown + 1 directory
    assert report.entry_count == 10
    assert report.file_count == 9
    assert report.total_bytes == 10 + 20 + 30 + 40 + 50 + 5 + 6 + 7 + 8
    assert sorted(entry.name for entry in report.entries) == sorted(
        [*ARTIFACTS, *TEMPORARIES, *UNKNOWN, "sub"]
    )


def test_depth_one_does_not_descend(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    shallow = scan.scan_directory(tmp_path)
    assert "nested.sess" not in {entry.name for entry in shallow.entries}

    deep = scan.scan_directory(tmp_path, max_depth=2)
    assert "nested.sess" in {entry.name for entry in deep.entries}
    assert deep.bytes_by_suffix[".sess"] == 10 + 3


def test_scan_changes_nothing(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    before = manifest(tmp_path)
    scan.scan_directory(tmp_path, max_depth=2)
    assert manifest(tmp_path) == before


def test_scan_never_opens_a_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build_fixture(tmp_path)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("the scanner must not open files")

    monkeypatch.setattr("builtins.open", forbidden)
    monkeypatch.setattr("io.open", forbidden, raising=False)
    monkeypatch.setattr("os.open", forbidden)
    report = scan.scan_directory(tmp_path, max_depth=2)
    assert report.entry_count == 11


def test_symlink_is_reported_not_followed(tmp_path: Path) -> None:
    (tmp_path / "real.sess").write_bytes(b"x" * 4)
    os.symlink(str(tmp_path / "real.sess"), str(tmp_path / "link.sess"))
    report = scan.scan_directory(tmp_path)
    kinds = {entry.name: entry.kind for entry in report.entries}
    assert kinds == {"real.sess": scan.KIND_ARTIFACT, "link.sess": scan.KIND_SYMLINK}
    assert report.symlink_count == 1
    assert report.files_by_suffix[".sess"] == 1


def test_missing_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(scan.ScanError):
        scan.scan_directory(tmp_path / "nope")


def test_file_is_not_a_directory(tmp_path: Path) -> None:
    target = tmp_path / "file.sess"
    target.write_bytes(b"x")
    with pytest.raises(scan.ScanError):
        scan.scan_directory(target)


def test_bad_depth_raises(tmp_path: Path) -> None:
    with pytest.raises(scan.ScanError):
        scan.scan_directory(tmp_path, max_depth=0)


@pytest.mark.parametrize(
    ("path", "protected"),
    [
        ("/models", True),
        ("/models/", True),
        ("/models/strata-data", True),
        ("/modelsX", False),
        ("/var/lib/models", False),
        ("/home/g0/models", False),
    ],
)
def test_protected_paths(path: str, protected: bool) -> None:
    assert scan.is_protected_path(path) is protected


def test_report_serialises(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    payload = scan.scan_directory(tmp_path).to_dict()
    assert payload["entry_count"] == 10
    assert isinstance(payload["entries"], list)
    assert set(payload["bytes_by_suffix"]) == set(scan.ARTIFACT_SUFFIXES)
