"""Archive readers. They turn retained raw files into scratch files DuckDB can read."""

import bz2
from pathlib import Path


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
