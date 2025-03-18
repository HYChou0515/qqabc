import sys
from enum import Enum
from typing import Annotated

import typer
from rich.console import Console

from qqabc.application.domain.model.job import (
    SerializedResult,
    StatusEnum,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewSerializedJobStatusRequest,
)
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.di.out import di_job_queue_service

console = Console()

app = typer.Typer()


class Status(str, Enum):
    process = "process"
    success = "success"
    fail = "fail"


def _map_status(status: Status) -> StatusEnum:
    if status == Status.process:
        return StatusEnum.RUNNING
    if status == Status.success:
        return StatusEnum.COMPLETED
    if status == Status.fail:
        return StatusEnum.FAILED
    raise NotImplementedError


@app.command()
def post(job_id: str, status: Annotated[Status, typer.Option("-s")]) -> None:
    svc = di_job_queue_service()
    result = sys.stdin.buffer.read()
    if not result:
        result = None
    else:
        result = SerializedResult(result)
    req = NewSerializedJobStatusRequest(
        job_id=job_id,
        status=_map_status(status),
        detail="",
        result_serialized=result,
    )
    try:
        svc.add_job_status(req)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
