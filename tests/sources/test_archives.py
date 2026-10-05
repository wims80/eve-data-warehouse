import bz2
import io
import json
import tarfile
from pathlib import Path

from evedw.sources.archives import decompress_bz2, tar_json_to_ndjson
from tests.helpers import KILLMAIL_FIXTURE


def test_decompress_bz2(tmp_path: Path) -> None:
    src = tmp_path / "x.bz2"
    src.write_bytes(bz2.compress(b"hello\nworld\n"))
    assert decompress_bz2(src, tmp_path / "out" / "x.txt") == 12
    assert (tmp_path / "out" / "x.txt").read_bytes() == b"hello\nworld\n"


def test_fixture_members_become_one_line_each(tmp_path: Path) -> None:
    out = tmp_path / "day.ndjson"
    result = tar_json_to_ndjson(KILLMAIL_FIXTURE, out)
    assert result.documents == 50 and result.compacted == 0
    lines = out.read_bytes().split(b"\n")
    assert lines[-1] == b"" and len(lines) == 51
    ids = [json.loads(line)["killmail_id"] for line in lines[:-1]]
    assert len(set(ids)) == 50 and 900000001 in ids


def test_pretty_printed_and_foreign_members_are_handled(tmp_path: Path) -> None:
    archive = tmp_path / "a.tar.bz2"
    with tarfile.open(archive, "w:bz2") as tar:
        for name, body in (
            ("killmails/1.json", b'{"killmail_id": 1}'),
            ("killmails/2.json", b'{\n  "killmail_id": 2,\n  "victim": {"items": []}\n}\n'),
            ("killmails/README.txt", b"not json"),
            ("killmails/empty.json", b"   \n"),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    out = tmp_path / "out.ndjson"
    result = tar_json_to_ndjson(archive, out)
    assert result.documents == 2 and result.compacted == 1
    assert out.read_bytes() == b'{"killmail_id": 1}\n{"killmail_id":2,"victim":{"items":[]}}\n'


def test_extract_members_streams_named_members(tmp_path: Path) -> None:
    from evedw.sources.archives import extract_members

    archive = tmp_path / "a.tar.bz2"
    with tarfile.open(archive, "w:bz2") as tar:
        for name, body in (
            ("snapshot/characters.json", b"[1]"),
            ("snapshot/notes.txt", b"skip"),
            ("snapshot/alliances.json", b"[2,3]"),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    found = extract_members(archive, tmp_path / "out", {"characters.json", "alliances.json"})
    assert sorted(found) == ["alliances.json", "characters.json"]
    assert found["characters.json"].read_bytes() == b"[1]"
    assert found["alliances.json"] == tmp_path / "out" / "alliances.json"
    assert not (tmp_path / "out" / "notes.txt").exists()
