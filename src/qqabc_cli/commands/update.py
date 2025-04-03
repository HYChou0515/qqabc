import sys
from enum import Enum
from typing import Annotated, Optional

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
    running = "running"
    success = "success"
    fail = "fail"


def _map_status(status: Status) -> StatusEnum:
    if status == Status.running:
        return StatusEnum.RUNNING
    if status == Status.success:
        return StatusEnum.COMPLETED
    if status == Status.fail:
        return StatusEnum.FAILED
    raise NotImplementedError


@app.command()
def update(
    *,
    job_id: str,
    status: Annotated[Status, typer.Option("-s")],
    detail: Annotated[Optional[str], typer.Option("-d")] = None,
    is_read_stdin: Annotated[bool, typer.Option("--stdin")] = False,
) -> None:
    svc = di_job_queue_service()
    if is_read_stdin:
        result = SerializedResult(sys.stdin.buffer.read())
    else:
        result = None
    req = NewSerializedJobStatusRequest(
        job_id=job_id,
        status=_map_status(status),
        detail="" if detail is None else detail,
        result_serialized=result,
    )
    try:
        svc.add_job_status(req)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
