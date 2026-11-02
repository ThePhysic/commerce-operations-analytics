from __future__ import annotations

import json

import pytest

from commerce.generate import generate
from commerce.ingest import digest, iter_rows, land_raw, read_manifest


def manifest_fixture(tmp_path):
    path = generate(tmp_path / "source", days=1, orders_per_day=3)
    return path, json.loads(path.read_text())


def save_manifest(path, manifest):
    path.write_text(json.dumps(manifest), encoding="utf-8")


def test_manifest_verifies_checksums_and_explicit_empty_partitions(tmp_path):
    path, spec = manifest_fixture(tmp_path)
    manifest, checksum, verified = read_manifest(path)
    assert checksum == digest(path.read_bytes())
    assert manifest == spec
    assert len(verified) == len(spec["files"])
    assert any(entry["source"] == "refunds" and entry["row_count"] == 0 for entry, _ in verified)


def test_manifest_checksum_tampering_fails_before_landing(tmp_path):
    path, spec = manifest_fixture(tmp_path)
    source = path.parent / spec["files"][0]["path"]
    source.write_bytes(source.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="Checksum mismatch"):
        read_manifest(path)


@pytest.mark.parametrize("unsafe", ["../escape.jsonl", "/absolute.jsonl", "C:/absolute.jsonl", "C:drive-relative.jsonl", "..\\escape.jsonl", "", "."])
def test_manifest_rejects_nonportable_or_escaping_paths(tmp_path, unsafe):
    path, spec = manifest_fixture(tmp_path)
    spec["files"][0]["path"] = unsafe
    save_manifest(path, spec)
    with pytest.raises(ValueError, match="unsafe manifest path"):
        read_manifest(path)


@pytest.mark.parametrize("schema", [True, 0, "1", None])
def test_manifest_rejects_invalid_schema_version_identifiers(tmp_path, schema):
    path, spec = manifest_fixture(tmp_path)
    spec["files"][0]["schema_version"] = schema
    save_manifest(path, spec)
    with pytest.raises(ValueError, match="schema version"):
        read_manifest(path)


def test_unknown_schema_version_survives_verification_for_raw_retention(tmp_path):
    path, spec = manifest_fixture(tmp_path)
    spec["files"][0]["schema_version"] = 2
    save_manifest(path, spec)
    _, _, verified = read_manifest(path)
    assert verified[0][0]["schema_version"] == 2


@pytest.mark.parametrize("replacement", [[], None, {"synthetic": False, "files": []}, {"synthetic": True, "files": [{}]}])
def test_manifest_rejects_invalid_structure(tmp_path, replacement):
    path, _ = manifest_fixture(tmp_path)
    save_manifest(path, replacement)
    with pytest.raises(ValueError):
        read_manifest(path)


def test_manifest_repeated_file_rejected_even_under_different_source(tmp_path):
    path, spec = manifest_fixture(tmp_path)
    duplicate = dict(spec["files"][0])
    duplicate["source"] = "refunds" if duplicate["source"] != "refunds" else "inventory"
    spec["files"].append(duplicate)
    save_manifest(path, spec)
    with pytest.raises(ValueError, match="repeats a file"):
        read_manifest(path)


@pytest.mark.parametrize("body", [b"id,id\n1,2\n", b"", b"a,b\n1,2,3\n"])
def test_csv_duplicate_or_missing_headers_and_unheaded_values_fail(body):
    with pytest.raises(ValueError):
        list(iter_rows("order_lines", body))


def test_malformed_json_is_preserved_for_row_quarantine():
    rows = list(iter_rows("refunds", b'{"refund_id":"R1"}\nnot-json\n\n'))
    assert rows == [{"refund_id": "R1"}, {"_invalid_json": "not-json"}]


def test_empty_csv_still_must_declare_required_schema_headers():
    with pytest.raises(ValueError, match="required CSV headers"):
        list(iter_rows("order_lines", b"line_id,order_id\n"))


def test_raw_landing_is_immutable_and_replay_reuses_bytes(tmp_path):
    body = b'{"synthetic":true}\n'
    entry = {"source": "refunds", "partition": "2024-01-01", "sha256": digest(body)}
    relative = land_raw(tmp_path, entry, body)
    raw = tmp_path / relative
    before = raw.stat().st_mtime_ns
    assert land_raw(tmp_path, entry, body) == relative
    assert raw.read_bytes() == body
    assert raw.stat().st_mtime_ns == before
    assert not list(raw.parent.glob(".landing-*"))
    raw.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="Immutable raw object corruption"):
        land_raw(tmp_path, entry, body)


def test_raw_landing_rejects_wrong_bytes_without_final_object(tmp_path):
    entry = {"source": "refunds", "partition": "2024-01-01", "sha256": digest(b"expected")}
    with pytest.raises(ValueError, match="checksum"):
        land_raw(tmp_path, entry, b"wrong")
    assert list(tmp_path.iterdir()) == []


def test_interrupted_landing_never_exposes_a_partial_final_file(tmp_path, monkeypatch):
    body = b"complete source bytes\n"
    entry = {"source": "refunds", "partition": "2024-01-01", "sha256": digest(body)}
    def fail_link(*args):
        raise OSError("injected publication failure")
    monkeypatch.setattr("commerce.ingest.os.link", fail_link)
    with pytest.raises(OSError, match="injected"):
        land_raw(tmp_path, entry, body)
    assert not any(path.is_file() for path in tmp_path.rglob("*"))
