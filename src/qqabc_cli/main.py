import typer

import qqabc_cli.di.out
from qqabc_cli.commands import get, pop, post, submit
from qqabc_cli.di.in_ import Container


def build_container() -> Container:
    container = Container()
    container.wire(modules=[qqabc_cli.di.out.__name__])
    return container


def create_app() -> typer.Typer:
    app = typer.Typer()
    app.add_typer(pop.app)
    app.add_typer(post.app)
    app.add_typer(submit.app)
    app.add_typer(get.app)
    return app


def main() -> None:  # pragma: no cover
    build_container()
    app = create_app()
    app()
