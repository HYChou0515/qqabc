from __future__ import annotations

from dataclasses import dataclass
import os
import re
import tempfile
import uuid
from typing import TYPE_CHECKING
from unittest.mock import patch
import typer
import click
import pytest
from typer.testing import CliRunner

from qqabc.adapter.out.pseristence.job_repo_adapter import InMemoryJobRepo
from qqabc.application.domain.service.job_queue_service import JobQueueService
from qqabc_cli.main import build_container, create_app

if TYPE_CHECKING:
    from collections.abc import Generator

    from click.testing import Result as ClickResult
    from pytest_mock import MockerFixture

    from tdd.fixtures.faker import Faker

runner = CliRunner(mix_stderr=False)

BAD_ARG_EXIT_CODE = click.UsageError.exit_code
NOT_FOUND_CODE = 10


@pytest.fixture
def fx_job_body_file(fx_faker: Faker) -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(fx_faker.json_bytes())
        f.flush()
        yield f.name


def _get_stdout(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout))


def _get_sterr(result: ClickResult) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stderr))

@pytest.fixture
def fx_app() -> Generator[typer.Typer]:
    container = build_container()
    job_dao = InMemoryJobRepo()
    with container.job_dao.override(job_dao):
        app = create_app()
        yield app
        job_dao.teardown()


class BaseCliTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker, fx_app: typer.Typer) -> None:
        self.fx_faker = fx_faker
        self.app = fx_app


class TestCliSubmit(BaseCliTest):
    def test_submit_without_args(
        self,
    ) -> None:
        result = runner.invoke(self.app, ["submit"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_submit(self, mocker: MockerFixture) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueService, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = runner.invoke(
                self.app,
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
        spy = mocker.spy(JobQueueService, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = runner.invoke(
                self.app, ["submit", self.fx_faker.job_type(), "-f", fx_job_body_file]
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
        result = runner.invoke(self.app, ["submit", self.fx_faker.job_type(), "-f", fname])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = _get_sterr(result)
        assert "Error" in stderr
        assert fname in stderr
        assert "does not exist" in stderr


class TestCliConsume(BaseCliTest):
    def test_pop_without_args(
        self,
    ) -> None:
        result = runner.invoke(self.app, ["pop"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_pop_from_empty_queue(self) -> None:
        job_type = self.fx_faker.job_type()
        with tempfile.TemporaryDirectory() as d:
            result = runner.invoke(self.app, ["pop", job_type, "-d", d])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = _get_sterr(result)
        assert "Error" in stderr
        assert "No job with job type" in stderr
        assert job_type in stderr

@dataclass
class AddJobType:
    job_type: str
    job_body: str
    job_id: str

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
            result = runner.invoke(self.app, ["submit", add_job_type.job_type], input=add_job_type.job_body)
        assert result.exit_code == 0
        return add_job_type, result

class TestCliAddJob(AddJobMixin):
    def test_pop_to_stdout(self) -> None:
        aj, _ = self._add_job()

        result = runner.invoke(self.app, ["pop", aj.job_type])
        assert result.exit_code == 0, result.stderr+result.stdout
        assert aj.job_body.encode() == result.stdout_bytes

    def test_pop_to_dir(self) -> None:
        aj, _ = self._add_job()

        with tempfile.TemporaryDirectory() as d:
            result = runner.invoke(self.app, ["pop", aj.job_type, "-d", d])
            assert result.exit_code == 0
            assert len(os.listdir(d)) == 1.0
            job_file = os.path.join(d, aj.job_id)
            assert os.path.exists(job_file)
            with open(job_file, "rb") as f:
                assert aj.job_body.encode() == f.read()

    def test_pop_with_invalid_dir(self) -> None:
        aj, _ = self._add_job()
        self._add_job()
        result = runner.invoke(
            self.app, ["pop", aj.job_type, "-d", dirname := self.fx_faker.file_name()]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = _get_sterr(result)
        assert "Error" in stderr
        assert "does not exist" in stderr
        assert "directory" in stderr
        assert dirname in stderr

    def test_pop_with_invalid_dir2(self) -> None:
        aj, _ = self._add_job()
        with tempfile.NamedTemporaryFile() as f:
            result = runner.invoke(self.app, ["pop", aj.job_type, "-d", f.name])
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stderr = _get_sterr(result)
            assert "Error" in stderr
            assert "is not a directory" in stderr
            assert f.name in stderr

    def test_post_result_without_args(self) -> None:
        result = runner.invoke(self.app, ["post"])
        assert result.exit_code == BAD_ARG_EXIT_CODE


class TestCliPostResult(AddJobMixin):
    def test_post_result_to_absent_job(self) -> None:
        result = runner.invoke(
            self.app, ["post", job_id := self.fx_faker.job_id(), "-s", "success"]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = _get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    def test_get_result_when_no_job(self) -> None:
        result = runner.invoke(self.app, ["get", "result", job_id := self.fx_faker.job_id()])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = _get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

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

    def _assert_posted_result(self, job_id: str, s_result: str | None) -> None:
        result = runner.invoke(self.app, ["get", "result", job_id])
        if s_result:
            assert result.exit_code == 0
            assert s_result.encode() == result.stdout_bytes
        else:
            assert result.exit_code == NOT_FOUND_CODE
            assert result.stdout == ""

    def _assert_posted_status(self, job_id: str, status: str) -> None:
        result = runner.invoke(self.app, ["get", "status", job_id])
        assert result.exit_code == 0
        self._assert_status(status, result.stdout)
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

    def _post_status(
        self, job_id: str, status: str, *, with_result: bool = True
    ) -> str | None:
        if with_result:
            s_result = self.fx_faker.json()
        else:
            s_result = None
        result = runner.invoke(
            self.app,
            ["post", job_id, "-s", status],
            input=s_result,
        )
        assert result.exit_code == 0
        return s_result

    @pytest.mark.parametrize("status", ["process", "success", "fail"])
    def test_get_posted_result(self, status: str) -> None:
        aj, _ = self._add_job()
        result = runner.invoke(self.app, ["get", "status", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

        result = runner.invoke(self.app, ["get", "result", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        assert result.stdout == ""

        for i in range(3):
            s_result = self._post_status(aj.job_id, status, with_result=i % 2 == 0)
            self._assert_posted_result(aj.job_id, s_result)
            self._assert_posted_status(aj.job_id, status)

            status = self.fx_faker.job_status()

    def test_get_jobs_from_nothing(self) -> None:
        result = runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr+result.stdout
        self._assert_job_in_table([], _get_stdout(result))

    def _assert_job_in_table(self, ajs: list[AddJobType], s: str) -> None:
        table_headers = ["ID", "Type", "Time"]
        assert all(header in s for header in table_headers)
        for aj in ajs:
            assert aj.job_id in s
            assert aj.job_type in s

    def test_get_jobs(self) -> None:
        aj1, _ = self._add_job()
        result = runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr+result.stdout
        self._assert_job_in_table([aj1], _get_stdout(result))

        aj2, _ = self._add_job()
        result = runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr+result.stdout
        self._assert_job_in_table([aj1, aj2], _get_stdout(result))
