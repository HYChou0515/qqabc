import os
import os.path as osp
import sys
from enum import Enum
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from qqabc.application.domain.model.job import (
    SerializedJob,
)
from qqabc.common.exceptions import EmptyQueueError
from qqabc_cli.di.out import di_job_queue_service

console = Console()
meta_console = Console(stderr=True)
info_console = Console(stderr=True, style="blue")
warn_console = Console(stderr=True, style="yellow")
err_console = Console(stderr=True, style="red")

app = typer.Typer()


def _pop_from_queue(job_type: Optional[str]) -> SerializedJob:
    svc = di_job_queue_service()
    try:
        sjob = svc.get_next_job(job_type, deserialize=False)
    except EmptyQueueError as e:
        err_console.print(str(e))
        raise typer.Exit from e
    return sjob


def _pipe_job_to_stdout(sjob: SerializedJob) -> None:
    sys.stdout.buffer.write(sjob.job_body_serialized)


def _pipe_job_to_dir(sjob: SerializedJob, dirpath: str) -> None:
    if osp.exists(dirpath) and not osp.isdir(dirpath):
        err_console.print(f"Dir is not valid: {dirpath}")
        err_console.print(f"Error: {dirpath} is not a directory")
        raise typer.Exit(code=2)
    if not osp.exists(dirpath):
        os.makedirs(dirpath)
        warn_console.print(f"Dir created: {dirpath}")
    fpath = f"{dirpath}/{sjob.job_id}.job"
    return _pipe_job_to_file(sjob, fpath)


def _pipe_job_to_file(sjob: SerializedJob, filepath: str) -> None:
    with open(filepath, "wb") as f:
        f.write(sjob.job_body_serialized)
    rpath = osp.relpath(filepath, ".")
    info_console.print(f"Job consumed into {rpath}")


class ConsumeResource(Enum):
    JOB = "job"


def consume_job(
    *,
    job_type: Optional[str],
    to_stdout: bool,
    to_files: list[str],
    to_dirs: list[str],
) -> None:
    sjob = _pop_from_queue(job_type)
    table = Table("Key", "Value", title="Job Meta")
    table.add_row("Job ID", sjob.job_id)
    table.add_row("Job Type", sjob.job_type)
    table.add_row("Created Time", sjob.created_time.isoformat())
    meta_console.print(table)

    if to_stdout:
        _pipe_job_to_stdout(sjob)
    for fname in to_files:
        _pipe_job_to_file(sjob, fname)
    for dname in to_dirs:
        _pipe_job_to_dir(sjob, dname)


def handle_default_value(
    *,
    to_stdout: bool,
    to_files: list[str],
    to_dirs: list[str],
) -> tuple[bool, list[str], list[str]]:
    if not to_files and not to_dirs and not to_stdout:
        to_dirs = ["."]
    return to_stdout, to_files, to_dirs


@app.command()
def consume(
    resource: ConsumeResource,
    job_type: Annotated[Optional[str], typer.Argument()] = None,
    *,
    to_stdout: Annotated[bool, typer.Option("--to-stdout")] = False,
    to_files: Annotated[list[str], typer.Option("--to-file", default_factory=list)],
    to_dirs: Annotated[list[str], typer.Option("--to-dir", default_factory=list)],
) -> None:
    if resource == ConsumeResource.JOB:
        to_stdout, to_files, to_dirs = handle_default_value(
            to_stdout=to_stdout,
            to_files=to_files,
            to_dirs=to_dirs,
        )
        consume_job(
            job_type=job_type,
            to_stdout=to_stdout,
            to_files=to_files,
            to_dirs=to_dirs,
        )
    else:
        raise NotImplementedError(f"Resource {resource} not implemented")
