import sys
from enum import Enum
from typing import Annotated, Optional

import typer
from rich.console import Console

from qqabc.application.domain.model.job import (
    SerializedResult,
)
from qqabc.application.port.in_.upload_result_use_case import NewJobResultRequest
from qqabc.common.exceptions import JobNotFoundError
from qqabc_cli.di.out import di_result_service

console = Console()

app = typer.Typer(name="upload")


class ResultFrom(str, Enum):
    stdout = "stdout"
    file = "file"


@app.command(name="result")
def upload_result(
    *,
    job_id: Annotated[str, typer.Option("--job-id")],
    from_stdout: Annotated[Optional[bool], typer.Option("--from-stdout")] = False,
    from_file: Annotated[Optional[str], typer.Option("--from-file")] = None,
) -> None:
    svc = di_result_service()
    if from_stdout:
        result = SerializedResult(sys.stdin.buffer.read())
    elif from_file:
        with open(from_file, "rb") as f:
            result = SerializedResult(f.read())
    req = NewJobResultRequest(
        job_id=job_id,
        result=result,
    )
    try:
        svc.add_job_result(req)
    except JobNotFoundError as e:
        raise typer.BadParameter(f"Error: job {job_id} does not exist") from e
