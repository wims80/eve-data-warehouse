"""``GET /query/{name}``: named read queries as JSON or as an Arrow IPC stream."""

import io
import json
from collections.abc import Buffer, Iterator
from typing import Any

import pyarrow as pa
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

from evedw.service.models import QueriesOut, QueryOut
from evedw.service.state import state_of

router = APIRouter(prefix="/query", tags=["query"])

ARROW_STREAM = "application/vnd.apache.arrow.stream"
BATCH_ROWS = 65_536


class _ChunkSink(io.RawIOBase):
    """A file-like object ``pa.ipc.new_stream`` writes into; ``drain`` hands the bytes
    written since the last call to the HTTP response."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[bytes] = []

    def writable(self) -> bool:
        return True

    def write(self, b: Buffer, /) -> int:
        data = bytes(b)
        self._parts.append(data)
        return len(data)

    def drain(self) -> bytes:
        out = b"".join(self._parts)
        self._parts.clear()
        return out


def arrow_stream(table: pa.Table) -> Iterator[bytes]:
    sink = _ChunkSink()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        yield sink.drain()
        for batch in table.to_batches(max_chunksize=BATCH_ROWS):
            writer.write_batch(batch)
            yield sink.drain()
    yield sink.drain()


def wants_arrow(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return ARROW_STREAM in accept


@router.get("", response_model=QueriesOut)
def list_queries(request: Request) -> QueriesOut:
    state = state_of(request)
    return QueriesOut(
        queries=[QueryOut.from_domain(state.queries.describe(n)) for n in state.queries.names()],
        arrow_media_type=ARROW_STREAM,
    )


@router.get("/{name}")
def run_query(request: Request, name: str) -> Response:
    """Parameters come from the query string; ``GET /query`` lists them. Send
    ``Accept: application/vnd.apache.arrow.stream`` for Arrow, anything else gets JSON
    ``{"name", "row_count", "columns", "rows"}`` with rows as objects."""
    state = state_of(request)
    params = {k: v for k, v in request.query_params.items()}
    table = state.queries.run(name, params)
    if wants_arrow(request):
        return StreamingResponse(
            arrow_stream(table),
            media_type=ARROW_STREAM,
            headers={"X-Row-Count": str(table.num_rows)},
        )
    body: dict[str, Any] = {
        "name": name,
        "row_count": table.num_rows,
        "columns": table.column_names,
        "rows": table.to_pylist(),
    }
    return Response(content=json.dumps(body, default=str), media_type="application/json")
