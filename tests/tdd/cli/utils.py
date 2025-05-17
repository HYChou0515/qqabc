from __future__ import annotations

import os
import tempfile
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Literal
from unittest.mock import patch

import click
import pytest

from tests.utils import assert_result_success, assert_status

if TYPE_CHECKING:
    import typer
    from click.testing import Result as ClickResult
    from typer.testing import CliRunner

    from tests.tdd.fixtures.faker import Faker

BAD_ARG_EXIT_CODE = click.UsageError.exit_code
NOT_FOUND_CODE = 10

ALL_STATUS = ("running", "success", "fail")


def job_file_name(job_id: str) -> str:
    return f"{job_id}.job"


def if_none(value: Any | Callable[[], Any], fallback: Any | Callable[[], Any]) -> Any:
    if callable(value):
        x = value()
    else:
        x = value
    if x is None:
        if callable(fallback):
            return fallback()
        return fallback
    return x


@dataclass
class AddJobType:
    job_type: str
    job_body: str
    job_id: str


class BaseCliTest:
    @pytest.fixture(autouse=True)
    def setup_method(
        self, fx_faker: Faker, fx_app: typer.Typer, fx_runner: CliRunner
    ) -> None:
        self.fx_faker = fx_faker
        self.app = fx_app
        self.runner = fx_runner


class PopJobMixin(BaseCliTest):
    def _pop_job(
        self,
        job_type: str | None = None,
        *,
        d: str | None = None,
        pipe: bool = False,
        fpath: str | None = None,
    ) -> ClickResult:
        commands = ["consume", "job"]
        if job_type is not None:
            commands.extend([job_type])
        if d is not None:
            commands.extend(["--to-dir", d])
        if pipe:
            commands.append("--to-stdout")
        if fpath is not None:
            commands.extend(["--to-file", fpath])
        result = self.runner.invoke(self.app, commands)
        return result


class AddJobMixin(BaseCliTest):
    def _add_job(
        self,
        *,
        job_type: str | None = None,
        job_body: str | None = None,
        job_id: str | None = None,
    ) -> tuple[AddJobType, ClickResult]:
        add_job_type = AddJobType(
            job_type=if_none(job_type, self.fx_faker.job_type),
            job_body=if_none(job_body, self.fx_faker.json),
            job_id=if_none(job_id, self.fx_faker.job_id),
        )
        with patch.object(uuid, "uuid4", return_value=uuid.UUID(add_job_type.job_id)):
            result = self.runner.invoke(
                self.app, ["submit", add_job_type.job_type], input=add_job_type.job_body
            )
        assert_result_success(result)
        return add_job_type, result

    def _submit_job(self) -> ClickResult:
        return self.runner.invoke(
            self.app,
            ["submit", self.fx_faker.job_type()],
            input=self.fx_faker.json(),
        )

    def _submit_job_by_file(self, file_name: str) -> ClickResult:
        return self.runner.invoke(
            self.app, ["submit", self.fx_faker.job_type(), "-f", file_name]
        )

    def _assert_job_in_table(self, ajs: list[AddJobType], s: str) -> None:
        table_headers = ["ID", "Type", "Time"]
        assert all(header in s for header in table_headers)
        for aj in ajs:
            assert aj.job_id in s
            assert aj.job_type in s


class UpdateStatusMixin(BaseCliTest):
    def _assert_posted_status(self, job_id: str, status: str) -> None:
        result = self.runner.invoke(self.app, ["get", "status", job_id])
        assert_result_success(result)
        assert_status(status, result.stdout)
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

    def _assert_posted_result_stdout(
        self, job_id: str, result_bytes: bytes | None
    ) -> None:
        result = self.runner.invoke(
            self.app, ["download", "result", "--job-id", job_id, "--to-stdout"]
        )
        if result_bytes is not None:
            assert_result_success(result)
            assert result_bytes == result.stdout_bytes
        else:
            assert result.exit_code == NOT_FOUND_CODE, result.stderr
            assert "has no result" in result.stderr

    def _assert_posted_result_dir(
        self, job_id: str, result_bytes: bytes | None
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self.runner.invoke(
                self.app, ["download", "result", "--job-id", job_id, "--to-dir", tmpdir]
            )
            if result_bytes is not None:
                assert_result_success(result)
                assert result.stdout_bytes == b""
                for fname in os.listdir(tmpdir):
                    assert fname in result.stderr
                    with open(f"{tmpdir}/{fname}", "rb") as f:
                        assert result_bytes == f.read()
            else:
                assert result.exit_code == NOT_FOUND_CODE, result.stderr
                assert "has no result" in result.stderr

    def _assert_posted_result_file(
        self, job_id: str, result_bytes: bytes | None
    ) -> None:
        with tempfile.NamedTemporaryFile() as tfile:
            result = self.runner.invoke(
                self.app,
                ["download", "result", "--job-id", job_id, "--to-file", tfile.name],
            )
            if result_bytes is not None:
                assert_result_success(result)
                assert result.stdout_bytes == b""
                assert tfile.name in result.stderr
                with open(tfile.name, "rb") as f:
                    assert result_bytes == f.read()
            else:
                assert result.exit_code == NOT_FOUND_CODE, result.stderr
                assert "has no result" in result.stderr

    def _post_status(
        self,
        job_id: str,
        status: str,
        *,
        with_result: bool = False,
        detail: str | None = None,
    ) -> str | None:
        command = ["update", "status", status, "--job-id", job_id]
        if with_result:
            s_result = self.fx_faker.json()
            command.append("--stdin")
        else:
            s_result = None
        if detail is not None:
            command.extend(["--detail", detail])
        result = self.runner.invoke(
            self.app,
            command,
            input=s_result,
        )
        assert_result_success(result)
        return s_result

    def _upload_result(
        self, job_id: str, result: bytes, from_: Literal["stdout", "file"]
    ) -> ClickResult:
        command = ["upload", "result", "--job-id", job_id]
        if from_ == "stdout":
            command.append("--from-stdout")
            return self.runner.invoke(self.app, command, input=result.decode())
        if from_ == "file":
            with tempfile.NamedTemporaryFile(mode="w+b") as f:
                f.write(result)
                f.flush()
                job_file = f.name
                command.extend(["--from-file", job_file])
                return self.runner.invoke(self.app, command)
        raise ValueError(f"Unknown source: {from_}")

    def _update_status(
        self, job_id: str, status: str, detail: str | None = None
    ) -> ClickResult:
        command = ["update", "status", status, "--job-id", job_id]
        if detail is not None:
            command.extend(["--detail", detail])
        return self.runner.invoke(self.app, command)
