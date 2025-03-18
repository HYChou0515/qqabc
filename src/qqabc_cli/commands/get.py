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
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.di.out import di_job_queue_service
from qqabc_cli.exception import (
    JobIdNotFoundError,
    ResultNotFoundError,
    StatusNotFoundError,
)

console = Console()

app = typer.Typer(name="get")

def _get_status(job_id: str) -> Optional[SerializedJobStatus]:
    svc = di_job_queue_service()
    try:
        s_status = svc.get_latest_status(job_id, deserialize=False)
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

@app.command(name="jobs")
def get_job(job_id: Optional[str]=None) -> None:
    svc = di_job_queue_service()
    table = Table("ID", "Type", "Time")
    jobs = svc.list_jobs()
    for job in jobs:
        if job_id is not None and job.job_id != job_id:
            continue
        table.add_row(job.job_id, job.job_type)
    console.print(table)

@app.command(name="result")
def get_result(job_id: str) -> None:
    s_status = _get_status(job_id)
    if s_status is None:
        raise JobIdNotFoundError(job_id)
    if s_status.result_serialized is None:
        raise ResultNotFoundError(job_id)
    _write_result_to_stdout(s_status.result_serialized)

@app.command(name="status")
def get_status(job_id: str) -> None:
    s_status = _get_status(job_id)
    _render_single_status(s_status)
    if s_status is None:
        raise StatusNotFoundError(job_id)
