import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console

from qqabc.application.domain.model.job import (
    SerializedJobBody,
)
from qqabc.application.port.in_.submit_job_use_case import NewSerializedJobRequest
from qqabc_cli.di.out import di_job_queue_service

console = Console()

app = typer.Typer()


def _submit_to_queue(
    job_type: str, 
    job_body: bytes
) -> None:
    svc = di_job_queue_service()
    request = NewSerializedJobRequest(
        job_type=job_type,
        job_body_serialized=SerializedJobBody(job_body),
    )
    job = svc.add_job(request)
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
