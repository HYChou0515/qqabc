from enum import Enum
from typing import Annotated, Optional

import typer
from rich.console import Console

from qqabc.application.domain.model.job import (
    StatusEnum,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.di.out import di_status_service

console = Console()

app = typer.Typer(name="update")


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


@app.command(name="status")
def update(
    *,
    status: Annotated[Status, typer.Argument()],
    job_id: Annotated[str, typer.Option("--job-id")],
    detail: Annotated[Optional[str], typer.Option("--detail")] = None,
) -> None:
    svc = di_status_service()
    req = NewJobStatusRequest(
        job_id=job_id,
        status=_map_status(status),
        detail="" if detail is None else detail,
    )
    try:
        svc.add_job_status(req)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
