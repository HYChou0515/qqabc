from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable
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
        self, job_type: str | None = None, *, d: str | None = None, pipe: bool = False
    ) -> ClickResult:
        commands = ["pop"]
        if job_type is not None:
            commands.extend([job_type])
        if d is not None:
            commands.extend(["-d", d])
        if pipe:
            commands.append("--pipe")
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

    def _assert_posted_result(self, job_id: str, s_result: str | None) -> None:
        result = self.runner.invoke(self.app, ["get", "result", job_id])
        if s_result:
            assert_result_success(result)
            assert s_result.encode() == result.stdout_bytes
        else:
            assert result.exit_code == NOT_FOUND_CODE
            assert result.stdout == ""

    def _post_status(
        self,
        job_id: str,
        status: str,
        *,
        with_result: bool = False,
        detail: str | None = None,
    ) -> str | None:
        command = ["update", job_id, "-s", status]
        if with_result:
            s_result = self.fx_faker.json()
            command.append("--stdin")
        else:
            s_result = None
        if detail is not None:
            command.extend(["-d", detail])
        result = self.runner.invoke(
            self.app,
            command,
            input=s_result,
        )
        assert_result_success(result)
        return s_result
