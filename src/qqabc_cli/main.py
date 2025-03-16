import datetime as dt
import sys
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from qqabc import JobQueueController
from qqabc.application.domain.model.job import (
    SerializedJob,
    SerializedJobBody,
    SerializedJobStatus,
    SerializedResult,
    StatusEnum,
)
from qqabc.exceptions import EmptyQueueError, JobNotFoundError
from qqabc.types import (
    NewSerializedJobRequest,
    NewSerializedJobStatusRequest,
)

console = Console()

app = typer.Typer()


class NotFoundError(typer.Exit):
    def __init__(self) -> None:
        self.exit_code: int = 10


def _submit_to_queue(job_type: str, job_body: bytes) -> None:
    controller = JobQueueController()
    request = NewSerializedJobRequest(
        job_type=job_type,
        job_body_serialized=SerializedJobBody(job_body),
    )
    job = controller.add_job(request)
    console.print("job submitted")
    console.print(f"job id = {job.job_id}")


def _submit_with_file(job_type: str, file_path: str) -> None:
    if not Path(file_path).exists():
        raise typer.BadParameter(f"Error: {file_path} does not exist")
    return _submit_to_queue(job_type, Path(file_path).read_bytes())


def _submit_with_stdin(job_type: str) -> None:
    return _submit_to_queue(job_type, sys.stdin.buffer.read())


@app.command()
def submit(
    job_type: str, file_path: Annotated[Optional[str], typer.Option("-f")] = None
) -> None:
    if file_path is not None:
        return _submit_with_file(job_type, file_path)
    return _submit_with_stdin(job_type)


def _check_dir(d: str) -> None:
    if not Path(d).exists():
        raise typer.BadParameter(f"Error: directory {d} does not exist")
    if not Path(d).is_dir():
        raise typer.BadParameter(f"Error: {d} is not a directory")


def _pop_from_queue(job_type: str) -> SerializedJob:
    controller = JobQueueController()
    try:
        sjob = controller.get_next_job(job_type, deserialize=False)
    except EmptyQueueError as e:
        raise typer.BadParameter(f"Error: No job with job type: {job_type}") from e
    return sjob


def _pop_to_dir(job_type: str, outdir: str) -> None:
    _check_dir(outdir)
    sjob = _pop_from_queue(job_type)
    output = Path(outdir) / f"{sjob.job_id}"
    with open(output, "wb") as f:
        f.write(sjob.job_body_serialized)


def _pop_to_stdout(job_type: str) -> None:
    sjob = _pop_from_queue(job_type)
    sys.stdout.buffer.write(sjob.job_body_serialized)


@app.command()
def pop(
    job_type: str, outdir: Annotated[Optional[str], typer.Option("-d")] = None
) -> None:
    if outdir:
        return _pop_to_dir(job_type, outdir)
    return _pop_to_stdout(job_type)


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
    controller = JobQueueController()
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
        controller.add_job_status(req)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e


class Resource(str, Enum):
    result = "result"
    status = "status"


def _get_status(job_id: str) -> Optional[SerializedJobStatus]:
    controller = JobQueueController()
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
