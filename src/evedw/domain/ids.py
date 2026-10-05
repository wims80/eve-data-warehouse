"""Small identifier types."""

import uuid
from typing import NewType

RunId = NewType("RunId", str)


def new_run_id() -> RunId:
    return RunId(str(uuid.uuid4()))
