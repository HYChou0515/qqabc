import typer


class NotFoundError(typer.Exit):
    def __init__(self) -> None:
        super().__init__(code=10)
