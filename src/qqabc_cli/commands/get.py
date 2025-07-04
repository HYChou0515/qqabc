import datetime as dt
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from qqabc.application.domain.model.job import (
    JobStatus,
)
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.di.out import di_job_queue_service, di_status_service
from qqabc_cli.exception import (
    StatusNotFoundError,
)

console = Console()

app = typer.Typer(name="get")


def _get_status(job_id: str) -> Optional[JobStatus]:
    svc = di_status_service()
    try:
        s_status = svc.get_latest_status(job_id, deserialize=False)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
    return s_status


def _render_datetime(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def _render_single_status(s_status: Optional[JobStatus]) -> None:
    table = Table("Status", "Time", "Detail")
    if s_status is not None:
        table.add_row(
            s_status.status, _render_datetime(s_status.issue_time), s_status.detail
        )
    console.print(table)


def _render_many_status(s_status: list[JobStatus]) -> None:
    table = Table("Status", "Time", "Detail")
    for s in s_status:
        table.add_row(s.status, _render_datetime(s.issue_time), s.detail)
    console.print(table)


@app.command(name="jobs")
def get_job() -> None:
    svc = di_job_queue_service()
    table = Table("ID", "Type", "Time")
    jobs = svc.list_jobs()
    for job in jobs:
        table.add_row(job.job_id, job.job_type)
    console.print(table)


@app.command(name="status")
def get_status(
    *,
    job_id: str,
    all_status: Annotated[bool, typer.Option("--all", "-a")] = False,
) -> None:
    if all_status:
        _list_status(job_id)
    else:
        _get_and_render_single_status(job_id)


def _get_and_render_single_status(job_id: str) -> None:
    s_status = _get_status(job_id)
    _render_single_status(s_status)
    if s_status is None:
        raise StatusNotFoundError(job_id)


def _list_status(job_id: str) -> None:
    svc = di_status_service()
    s_status = svc.list_job_status(job_id)
    _render_many_status(s_status)
