from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import patch

import click
import pytest

if TYPE_CHECKING:
    import typer
    from click.testing import Result as ClickResult
    from typer.testing import CliRunner

    from tdd.fixtures.faker import Faker

BAD_ARG_EXIT_CODE = click.UsageError.exit_code
NOT_FOUND_CODE = 10


def get_stdout(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout))


def get_sterr(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stderr))


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


class AddJobMixin(BaseCliTest):
    def _add_job(
        self,
        *,
        job_type: str | None = None,
        job_body: str | None = None,
        job_id: str | None = None,
    ) -> tuple[AddJobType, ClickResult]:
        add_job_type = AddJobType(
            job_type=job_type or self.fx_faker.job_type(),
            job_body=job_body or self.fx_faker.json(),
            job_id=job_id or self.fx_faker.job_id(),
        )
        with patch.object(uuid, "uuid4", return_value=uuid.UUID(add_job_type.job_id)):
            result = self.runner.invoke(
                self.app, ["submit", add_job_type.job_type], input=add_job_type.job_body
            )
        assert result.exit_code == 0
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
