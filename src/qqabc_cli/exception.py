import typer
from rich.console import Console

error_console = Console(stderr=True, style="bold red")


class NotFoundError(typer.Exit):
    def __init__(self) -> None:
        super().__init__(code=10)


class JobIdNotFoundError(NotFoundError):
    def __init__(self, job_id: str) -> None:
        error_console.print(f"Error: job {job_id} does not exist")
        super().__init__()


class ResultNotFoundError(NotFoundError):
    def __init__(self, job_id: str) -> None:
        error_console.print(f"Error: job {job_id} has no result.")
        super().__init__()


class StatusNotFoundError(NotFoundError):
    def __init__(self, job_id: str) -> None:
        error_console.print(f"Error: job {job_id} has no status.")
        super().__init__()
