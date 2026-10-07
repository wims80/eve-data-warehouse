"""HTTP client for a running service, used by the CLI (design §8: a CLI invoked while
the service runs sends a trigger instead of taking the writer lock)."""

import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from evedw.config import Settings
from evedw.domain.ids import RunId
from evedw.domain.registry import DatasetSummary, ImportRun
from evedw.jobs.runner import WriterLock
from evedw.service.models import DatasetsOut, HealthOut, RunOut, RunsOut, TriggerOut


class ServiceError(RuntimeError):
    pass


class ServiceClient:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=httpx.Timeout(30.0, connect=5.0))
        self._sleep = sleep

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self._client.request(method, f"{self.base_url}{path}", **kwargs)
        except httpx.HTTPError as exc:
            raise ServiceError(f"service at {self.base_url} is not answering: {exc}") from None
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise ServiceError(f"service returned HTTP {response.status_code}: {detail}")
        return response.json()

    def health(self) -> HealthOut:
        return HealthOut.model_validate(self._request("GET", "/health"))

    def datasets(self) -> tuple[list[DatasetSummary], dict[str, int]]:
        out = DatasetsOut.model_validate(self._request("GET", "/datasets"))
        return [d.to_domain() for d in out.datasets], out.entities

    def entity_refresh(self) -> tuple[dict[str, dict[str, int]], dict[str, Any]]:
        out = DatasetsOut.model_validate(self._request("GET", "/datasets"))
        return out.refresh_queue, out.refresh_state

    def runs(self, *, limit: int = 20, job: str | None = None) -> list[ImportRun]:
        params: dict[str, Any] = {"limit": limit}
        if job is not None:
            params["job"] = job
        out = RunsOut.model_validate(self._request("GET", "/runs", params=params))
        return [r.to_domain() for r in out.runs]

    def run(self, run_id: RunId) -> ImportRun:
        return RunOut.model_validate(self._request("GET", f"/runs/{run_id}")).to_domain()

    def trigger(self, job: str, params: Mapping[str, Any]) -> RunId:
        body = {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in params.items()}
        out = TriggerOut.model_validate(self._request("POST", f"/jobs/{job}", json=body))
        return RunId(out.run_id)

    def wait(self, run_id: RunId, *, poll_seconds: float = 2.0) -> ImportRun:
        """Poll until the run has finished."""
        while True:
            run = self.run(run_id)
            if run.status.finished:
                return run
            self._sleep(poll_seconds)


def find_service(settings: Settings) -> ServiceClient | None:
    """The service holding the writer lock, if any. A lock held by a plain CLI process
    is not a service; the caller will fail on the lock with the holder in the message."""
    holder = WriterLock(settings.lock_path).holder()
    if holder is None or holder.service_url is None:
        return None
    return ServiceClient(holder.service_url)
