"""Raw objects of zKillboard's live feed (design §4, §7.1): one NDJSON line per object as
served, appended to ``raw/zkillboard/live/<day>/<hour>.ndjson`` by the hour it was fetched
(UTC), gzip-compressed to ``<hour>.ndjson.gz`` once the hour has closed and never modified
after. The follower appends; the flush reads what came after its checkpoint.
"""

import gzip
import json
import logging
import os
import shutil
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

log = logging.getLogger(__name__)

OPEN_SUFFIX = ".ndjson"
CLOSED_SUFFIX = ".ndjson.gz"


def sequence_of(obj: dict[str, Any]) -> int | None:
    value = obj.get("sequence_id")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class LiveRaw:
    """Appends and reads the hourly files. One lock covers both, so a reader never sees a
    half-written line or a file while it is being compressed."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = threading.Lock()
        self._open: tuple[Path, TextIO] | None = None

    def _hour_file(self, at: datetime) -> Path:
        at = at.astimezone(UTC)
        return self.root / at.date().isoformat() / f"{at.hour:02d}{OPEN_SUFFIX}"

    def append(self, obj: dict[str, Any], *, now: datetime) -> None:
        """Write one object to the file of ``now``'s hour, closing earlier hours first."""
        path = self._hour_file(now)
        with self._lock:
            if self._open is None or self._open[0] != path:
                self._close_open()
                self._compress_closed(keep=path)
                path.parent.mkdir(parents=True, exist_ok=True)
                self._open = (path, path.open("a", encoding="utf-8"))
            fh = self._open[1]
            fh.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")
            fh.flush()

    def close(self, *, now: datetime) -> None:
        """Stop appending: flush the open hour to disk and compress every closed hour."""
        with self._lock:
            self._close_open()
            self._compress_closed(keep=self._hour_file(now))

    def compress_closed(self, *, now: datetime) -> list[Path]:
        """Compress every hour file but the current one; returns the files written."""
        with self._lock:
            return self._compress_closed(keep=self._hour_file(now))

    def _close_open(self) -> None:
        if self._open is not None:
            _, fh = self._open
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
            self._open = None

    def _compress_closed(self, *, keep: Path) -> list[Path]:
        written: list[Path] = []
        if not self.root.is_dir():
            return written
        for path in sorted(self.root.glob(f"*/*{OPEN_SUFFIX}")):
            if path == keep or (self._open is not None and path == self._open[0]):
                continue
            final = path.with_name(path.name.removesuffix(OPEN_SUFFIX) + CLOSED_SUFFIX)
            if final.exists():
                # A crash between writing the archive and removing the file it came from.
                path.unlink()
                continue
            tmp = final.with_name(final.name + ".tmp")
            with path.open("rb") as src, gzip.open(tmp, "wb", compresslevel=6) as out:
                shutil.copyfileobj(src, out)
            with tmp.open("rb") as fh:
                os.fsync(fh.fileno())
            tmp.replace(final)
            path.unlink()
            written.append(final)
            log.info("closed live hour %s", final.relative_to(self.root))
        return written

    def files(self) -> list[Path]:
        """Every hour file, oldest first."""
        if not self.root.is_dir():
            return []
        found = [
            p
            for p in self.root.glob("*/*")
            if p.name.endswith(OPEN_SUFFIX) or p.name.endswith(CLOSED_SUFFIX)
        ]
        return sorted(found, key=lambda p: (p.parent.name, p.name.split(".")[0]))

    def read_since(self, sequence: int | None) -> list[dict[str, Any]]:
        """Objects with a sequence above ``sequence`` (all of them for ``None``), in the
        order they were fetched. Files whose first object is not past the checkpoint are
        read from the newest such file on; a line cut short by a crash is skipped."""
        with self._lock:
            if self._open is not None:
                self._open[1].flush()
            files = self.files()
            start = 0
            if sequence is not None:
                for index in range(len(files) - 1, -1, -1):
                    first = _first_sequence(files[index])
                    if first is not None and first <= sequence:
                        start = index
                        break
            out: list[dict[str, Any]] = []
            for path in files[start:]:
                for obj in _objects(path):
                    seq = sequence_of(obj)
                    if seq is not None and (sequence is None or seq > sequence):
                        out.append(obj)
            return out


def _open_text(path: Path) -> TextIO:
    if path.name.endswith(CLOSED_SUFFIX):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def _objects(path: Path) -> Iterator[dict[str, Any]]:
    with _open_text(path) as fh:
        for line in fh:
            try:
                obj: Any = json.loads(line)
            except json.JSONDecodeError:
                log.warning("skipping a damaged line in %s", path)
                continue
            if isinstance(obj, dict):
                yield dict(obj)  # pyright: ignore[reportUnknownArgumentType]


def _first_sequence(path: Path) -> int | None:
    for obj in _objects(path):
        return sequence_of(obj)
    return None
