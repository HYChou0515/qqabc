from __future__ import annotations

import os
import re
import tempfile
import uuid
from typing import TYPE_CHECKING
from unittest.mock import patch

import click
import pytest
from typer.testing import CliRunner

from qqabc import JobQueueController
from qqabc_cli.main import app

if TYPE_CHECKING:
    from collections.abc import Generator

    from click.testing import Result as ClickResult
    from pytest_mock import MockerFixture

    from tests.fixtures.faker import Faker

runner = CliRunner()

BAD_ARG_EXIT_CODE = click.UsageError.exit_code


@pytest.fixture
def fx_job_body_file(fx_faker: Faker) -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(fx_faker.json_bytes())
        f.flush()
        yield f.name


def _get_stdout(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout))


class BaseCliTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker


class TestCliSubmit(BaseCliTest):
    def test_submit_without_args(
        self,
    ) -> None:
        result = runner.invoke(app, ["submit"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_submit(self, mocker: MockerFixture) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueController, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = runner.invoke(
                app,
                ["submit", self.fx_faker.job_type()],
                input=self.fx_faker.json(),
            )
            assert result.exit_code == 0
            stdout = _get_stdout(result)
            assert "job submitted" in stdout
            assert "job id = " in stdout
            assert stub_uid.hex in stdout
            assert spy.call_count == 1
            assert spy2.call_count == 1

    def test_submit_with_file(
        self, fx_job_body_file: str, mocker: MockerFixture
    ) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueController, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = runner.invoke(
                app, ["submit", self.fx_faker.job_type(), "-f", fx_job_body_file]
            )
            assert result.exit_code == 0
            stdout = _get_stdout(result)
            assert "job submitted" in stdout
            assert "job id = " in stdout
            assert stub_uid.hex in stdout
            assert spy.call_count == 1
            assert spy2.call_count == 1

    def test_submit_with_invalid_file(
        self,
    ) -> None:
        fname = self.fx_faker.file_name()
        result = runner.invoke(app, ["submit", self.fx_faker.job_type(), "-f", fname])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert fname in stdout
        assert "does not exist" in stdout


class TestCliConsume(BaseCliTest):
    def test_pop_without_args(
        self,
    ) -> None:
        result = runner.invoke(app, ["pop"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_pop_from_empty_queue(self) -> None:
        job_type = self.fx_faker.job_type()
        with tempfile.TemporaryDirectory() as d:
            result = runner.invoke(app, ["pop", job_type, "-d", d])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert "No job with job type" in stdout
        assert job_type in stdout


class AddJobMixin:
    def _add_job(
        self,
        job_type: str,
        *,
        job_body: str | None = None,
        job_id: str | None = None,
    ) -> ClickResult:
        job_body_ = job_body or self.fx_faker.json()

        def add() -> ClickResult:
            return runner.invoke(app, ["submit", job_type], input=job_body_)

        if job_id is not None:
            with patch.object(uuid, "uuid4", return_value=uuid.UUID(job_id)):
                return add()
        else:
            return add()


class TestCliAddJob(BaseCliTest, AddJobMixin):
    def test_pop_to_stdout(self) -> None:
        job_type = self.fx_faker.job_type()
        self._add_job(
            job_type,
            job_body=(job_body := self.fx_faker.json()),
            job_id=(self.fx_faker.job_id()),
        )

        result = runner.invoke(app, ["pop", job_type])
        assert result.exit_code == 0
        assert job_body.encode() == result.stdout_bytes

    def test_pop_to_dir(self) -> None:
        job_type = self.fx_faker.job_type()
        self._add_job(
            job_type,
            job_body=(job_body := self.fx_faker.json()),
            job_id=(job_id := self.fx_faker.job_id()),
        )

        with tempfile.TemporaryDirectory() as d:
            result = runner.invoke(app, ["pop", job_type, "-d", d])
            assert result.exit_code == 0
            assert len(os.listdir(d)) == 1.0
            job_file = os.path.join(d, job_id)
            assert os.path.exists(job_file)
            with open(job_file, "rb") as f:
                assert job_body.encode() == f.read()

    def test_pop_with_invalid_dir(self) -> None:
        job_type = self.fx_faker.job_type()
        self._add_job(job_type)
        result = runner.invoke(
            app, ["pop", job_type, "-d", dirname := self.fx_faker.file_name()]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert "does not exist" in stdout
        assert "directory" in stdout
        assert dirname in stdout

    def test_pop_with_invalid_dir2(self) -> None:
        job_type = self.fx_faker.job_type()
        self._add_job(job_type)
        with tempfile.NamedTemporaryFile() as f:
            result = runner.invoke(app, ["pop", job_type, "-d", f.name])
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stdout = _get_stdout(result)
            assert "Error" in stdout
            assert "is not a directory" in stdout
            assert f.name in stdout

    def test_post_result_without_args(self) -> None:
        result = runner.invoke(app, ["post"])
        assert result.exit_code == BAD_ARG_EXIT_CODE


class TestCliPostResult(BaseCliTest, AddJobMixin):
    def test_post_result_to_absent_job(self) -> None:
        result = runner.invoke(app, ["post", job_id := self.fx_faker.job_id(), "-s", "success"])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert job_id in stdout
        assert "does not exist" in stdout

    def test_get_result_when_no_result(self) -> None:
        result = runner.invoke(app, ["get", "result", job_id := self.fx_faker.job_id()])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert job_id in stdout
        assert "does not exist" in stdout

    @staticmethod
    def _assert_status(status: str, stdout: str) -> None:
        if status == "process":
            assert "RUNNING" in stdout
        elif status == "success":
            assert "COMPLETED" in stdout
        elif status == "fail":
            assert "FAILED" in stdout
        else:
            raise NotImplementedError

    def _assert_posted_result(self, job_id: str, s_result: str) -> None:
        result = runner.invoke(app, ["get", "result", job_id])
        assert result.exit_code == 0
        assert s_result.encode() == result.stdout_bytes

    def _assert_posted_status(self, job_id: str, status: str) -> None:
        result = runner.invoke(app, ["get", "status", job_id])
        assert result.exit_code == 0
        self._assert_status(status, result.stdout)
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

    def _post_status(self, job_id: str, status: str) -> str:
        result = runner.invoke(
            app,
            ["post", job_id, "-s", status],
            input=(s_result := self.fx_faker.json()),
        )
        assert result.exit_code == 0
        return s_result

    @pytest.mark.parametrize("status", ["process", "success", "fail"])
    def test_get_posted_result(self, status: str) -> None:
        self._add_job(
            self.fx_faker.job_type(),
            job_body=(self.fx_faker.json()),
            job_id=(job_id := self.fx_faker.job_id()),
        )
        for _ in range(3):
            s_result = self._post_status(job_id, status)
            self._assert_posted_result(job_id, s_result)
            self._assert_posted_status(job_id, status)

            status = self.fx_faker.job_status()
