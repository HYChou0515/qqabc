from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from qqabc_cli.common.console import (
    meta_console,
)
from qqabc_cli.common.outputs import Piper
from qqabc_cli.common.utils import handle_default_to
from qqabc_cli.di.out import di_result_service
from qqabc_cli.exception import ResultNotFoundError

console = Console()

app = typer.Typer(name="download")


def download_latest_result(
    *,
    job_id: str,
    to_stdout: bool,
    to_files: list[str],
    to_dirs: list[str],
    index: int,
) -> None:
    svc = di_result_service()
    result = svc.get_latest_result(job_id, index=index)
    if result is None:
        raise ResultNotFoundError(job_id)

    table = Table("Key", "Value", title="Job Meta")
    table.add_row("Result ID", result.result_id)
    table.add_row("Job ID", job_id)
    table.add_row("Issue Time", result.issue_time.isoformat())
    meta_console.print(table)

    piper = Piper()
    if to_stdout:
        piper.pipe_to_stdout(result)
    for fname in to_files:
        piper.pipe_to_file(result, fname)
    for dname in to_dirs:
        piper.pipe_to_dir(result, dname)


@app.command(name="result")
def download_result(
    *,
    job_id: Annotated[str, typer.Option("--job-id")],
    to_stdout: Annotated[bool, typer.Option("--to-stdout")] = False,
    to_files: Annotated[list[str], typer.Option("--to-file", default_factory=list)],
    to_dirs: Annotated[list[str], typer.Option("--to-dir", default_factory=list)],
    index: Annotated[int, typer.Option("--index", "-k")] = 1,
) -> None:
    to_stdout, to_files, to_dirs = handle_default_to(
        to_stdout=to_stdout,
        to_files=to_files,
        to_dirs=to_dirs,
    )
    download_latest_result(
        job_id=job_id,
        to_stdout=to_stdout,
        to_files=to_files,
        to_dirs=to_dirs,
        index=index,
    )
