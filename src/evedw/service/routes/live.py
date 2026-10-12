"""``GET /live``: the live feed follower's state (design §7.1, §9)."""

from datetime import UTC, datetime

from fastapi import APIRouter, Request

from evedw.service.models import LiveOut
from evedw.service.state import state_of

router = APIRouter(tags=["live"])


@router.get("/live", response_model=LiveOut)
def live(request: Request) -> LiveOut:
    follower = state_of(request).live
    status = follower.status if follower is not None else None
    return LiveOut.from_status(status, now=datetime.now(UTC))
