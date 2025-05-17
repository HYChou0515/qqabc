def handle_default_to(
    *,
    to_stdout: bool,
    to_files: list[str],
    to_dirs: list[str],
) -> tuple[bool, list[str], list[str]]:
    if not to_files and not to_dirs and not to_stdout:
        to_dirs = ["."]
    return to_stdout, to_files, to_dirs
