"""Archive readers. They turn retained raw files into scratch files DuckDB can read."""

import bz2
import json
import logging
import tarfile
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


def decompress_bz2(src: Path, dest: Path, *, chunk_size: int = 1 << 20) -> int:
    """Stream-decompress ``src`` into ``dest``; returns bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with bz2.open(src, "rb") as reader, dest.open("wb") as writer:
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            writer.write(chunk)
            written += len(chunk)
    return written


def extract_members(
    src: Path, dest_dir: Path, names: Collection[str], *, chunk_size: int = 1 << 20
) -> dict[str, Path]:
    """Stream the members whose base name is in ``names`` out of a tar archive.

    Returns base name -> extracted path. Directory prefixes inside the archive are
    dropped. A base name appearing twice is an error: the archive is not what we expect.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    found: dict[str, Path] = {}
    with tarfile.open(src, "r|*") as tar:
        for member in tar:
            base = member.name.rsplit("/", 1)[-1]
            if not member.isfile() or base not in names:
                continue
            if base in found:
                raise ValueError(f"{src.name}: member {base!r} appears more than once")
            fh = tar.extractfile(member)
            if fh is None:
                continue
            target = dest_dir / base
            with target.open("wb") as out:
                while True:
                    chunk = fh.read(chunk_size)
                    if not chunk:
                        break
                    out.write(chunk)
            found[base] = target
    return found


@dataclass(frozen=True, slots=True)
class NdjsonResult:
    documents: int
    compacted: int
    """Members that were not single-line JSON and had to be re-serialised."""


def tar_json_to_ndjson(src: Path, dest: Path, *, member_suffix: str = ".json") -> NdjsonResult:
    """Concatenate every ``*.json`` member of a tar archive into one NDJSON file.

    EVE Ref killmail members are compact single-line documents, which are copied byte for
    byte. A member containing a newline is parsed and re-serialised compactly so the
    output stays one document per line. Non-JSON members are ignored.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    documents = compacted = 0
    with tarfile.open(src, "r|*") as tar, dest.open("wb") as out:
        for member in tar:
            if not member.isfile() or not member.name.endswith(member_suffix):
                continue
            fh = tar.extractfile(member)
            if fh is None:
                continue
            body = fh.read().strip()
            if not body:
                continue
            if b"\n" in body or b"\r" in body:
                body = json.dumps(json.loads(body), separators=(",", ":")).encode("utf-8")
                compacted += 1
            out.write(body)
            out.write(b"\n")
            documents += 1
    if compacted:
        log.warning(
            "%d of %d members were not single-line JSON and were compacted", compacted, documents
        )
    return NdjsonResult(documents=documents, compacted=compacted)
