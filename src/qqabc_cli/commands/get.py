import datetime as dt
import sys
from enum import Enum
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from qqabc.application.domain.model.job import (
    SerializedJobStatus,
    SerializedResult,
)
from qqabc.application.domain.service.job_queue_service import JobQueueService
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.exception import NotFoundError

console = Console()

app = typer.Typer()


class Resource(str, Enum):
    result = "result"
    status = "status"


def _get_status(job_id: str) -> Optional[SerializedJobStatus]:
    controller = JobQueueService()
    try:
        s_status = controller.get_latest_status(job_id, deserialize=False)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
    return s_status


def _render_datetime(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def _render_single_status(s_status: Optional[SerializedJobStatus]) -> None:
    table = Table("Status", "Time", "Detail")
    if s_status is not None:
        table.add_row(
            s_status.status, _render_datetime(s_status.issue_time), s_status.detail
        )
    console.print(table)


def _write_result_to_stdout(s_result: SerializedResult) -> None:
    sys.stdout.buffer.write(s_result)


@app.command()
def get(resource: Resource, job_id: str) -> None:
    if resource == Resource.result:
        s_status = _get_status(job_id)
        if s_status is None:
            raise NotFoundError
        if s_status.result_serialized is None:
            raise NotFoundError
        _write_result_to_stdout(s_status.result_serialized)
        return
    if resource == Resource.status:
        s_status = _get_status(job_id)
        _render_single_status(s_status)
        if s_status is None:
            raise NotFoundError
        return

    raise NotImplementedError
