from __future__ import annotations
import re
import subprocess as sp

from click.testing import Result as ClickResult


def assert_result_success_subprocess(result: sp.CompletedProcess[bytes]):
    assert result.returncode == 0, result.stderr.decode() + result.stdout.decode()

def assert_result_success_click(result: ClickResult):
    assert result.exit_code == 0, result.stderr + result.stdout

def assert_result_success(result: sp.CompletedProcess[bytes] | ClickResult):
    if isinstance(result, sp.CompletedProcess):
        return assert_result_success_subprocess(result)
    return assert_result_success_click(result)

def assert_status(status: str, stdout: str) -> None:
    if status == "running":
        assert "RUNNING" in stdout
    elif status == "success":
        assert "COMPLETED" in stdout
    elif status == "fail":
        assert "FAILED" in stdout
    else:
        raise NotImplementedError

def get_stdout_subprocess(result: sp.CompletedProcess[bytes]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout.decode()))


def get_sterr_subprocess(result:  sp.CompletedProcess[bytes]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stderr.decode()))


def get_stdout_click(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout))


def get_sterr_click(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stderr))

def get_stdout(result: sp.CompletedProcess[bytes] | ClickResult) -> str:
    if isinstance(result, sp.CompletedProcess):
        return get_stdout_subprocess(result)
    return get_stdout_click(result)

def get_sterr(result: sp.CompletedProcess[bytes] | ClickResult) -> str:
    if isinstance(result, sp.CompletedProcess):
        return get_sterr_subprocess(result)
    return get_sterr_click(result)
