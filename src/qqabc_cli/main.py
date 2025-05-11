import typer

import qqabc_cli.di.out
from qqabc_cli.commands import consume, download, get, submit, update, upload


def create_app() -> typer.Typer:
    app = typer.Typer()
    app.add_typer(consume.app)
    app.add_typer(update.app)
    app.add_typer(submit.app)
    app.add_typer(get.app)
    app.add_typer(download.app)
    app.add_typer(upload.app)
    return app


def main() -> None:  # pragma: no cover
    qqabc_cli.di.out.get_container()
    app = create_app()
    app()
