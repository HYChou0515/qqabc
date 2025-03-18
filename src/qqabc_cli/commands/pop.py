import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console

from qqabc.application.domain.model.job import (
    SerializedJob,
)
from qqabc.common.exceptions import EmptyQueueError
from qqabc_cli.di.out import di_job_queue_service

console = Console()

app = typer.Typer()


def _check_dir(d: str) -> None:
    if not Path(d).exists():
        raise typer.BadParameter(f"Error: directory {d} does not exist")
    if not Path(d).is_dir():
        raise typer.BadParameter(f"Error: {d} is not a directory")


def _pop_from_queue(job_type: str) -> SerializedJob:
    svc = di_job_queue_service()
    try:
        sjob = svc.get_next_job(job_type, deserialize=False)
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
