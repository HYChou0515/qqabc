import typer

from qqabc_cli.commands import get, pop, post, submit

app = typer.Typer()


app.add_typer(pop.app)
app.add_typer(post.app)
app.add_typer(submit.app)
app.add_typer(get.app)
