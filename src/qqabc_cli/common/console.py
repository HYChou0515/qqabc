from rich.console import Console

console = Console()
meta_console = Console(stderr=True)
info_console = Console(stderr=True, style="blue")
warn_console = Console(stderr=True, style="yellow")
err_console = Console(stderr=True, style="red")
