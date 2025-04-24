from typing import Annotated, Optional

import typer
from rich.table import Table

from qqabc.application.domain.model.job import (
    SerializedJob,
)
from qqabc.common.exceptions import EmptyQueueError
from qqabc_cli.common.console import (
    err_console,
    meta_console,
)
from qqabc_cli.common.outputs import Piper
from qqabc_cli.common.utils import handle_default_to
from qqabc_cli.di.out import di_job_queue_service

app = typer.Typer(name="consume")


def _pop_from_queue(job_type: Optional[str]) -> SerializedJob:
    svc = di_job_queue_service()
    try:
        sjob = svc.get_next_job(job_type, deserialize=False)
    except EmptyQueueError as e:
        err_console.print(str(e))
        raise typer.Exit from e
    return sjob


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
    piper = Piper()
    if to_stdout:
        piper.pipe_to_stdout(sjob)
    for fname in to_files:
        piper.pipe_to_file(sjob, fname)
    for dname in to_dirs:
        piper.pipe_to_dir(sjob, dname)


@app.command(name="job")
def consume(
    job_type: Annotated[Optional[str], typer.Argument()] = None,
    *,
    to_stdout: Annotated[bool, typer.Option("--to-stdout")] = False,
    to_files: Annotated[list[str], typer.Option("--to-file", default_factory=list)],
    to_dirs: Annotated[list[str], typer.Option("--to-dir", default_factory=list)],
) -> None:
    to_stdout, to_files, to_dirs = handle_default_to(
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
