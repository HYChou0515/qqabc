import os
import re
import tempfile
import uuid
from collections.abc import Generator
from unittest.mock import patch

import click
import pytest
from click.testing import Result as ClickResult
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from qqabc import JobQueueController
from qqabc_cli.main import app
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


class TestCli:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker

    def test_submit_without_args(
        self,
    ) -> None:
        result = runner.invoke(app, ["submit"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_submit(self, fx_job_body_file: str, mocker: MockerFixture) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueController, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = runner.invoke(
                app, ["submit", self.fx_faker.job_type(), fx_job_body_file]
            )
            assert result.exit_code == 0
            stdout = _get_stdout(result)
            assert "job submitted" in stdout
            assert "job id = " in stdout
            assert stub_uid.hex in stdout
            assert spy.call_count == 1
            assert spy2.call_count == 1

    def test_submit_without_file(
        self,
    ) -> None:
        result = runner.invoke(app, ["submit", self.fx_faker.job_type()])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_submit_with_invalid_file(
        self,
    ) -> None:
        fname = self.fx_faker.file_name()
        result = runner.invoke(app, ["submit", self.fx_faker.job_type(), fname])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert fname in stdout
        assert "does not exist" in stdout

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

    def test_pop(self, fx_job_body_file: str) -> None:
        stub_uid = uuid.UUID(self.fx_faker.uuid4())
        job_type = self.fx_faker.job_type()
        with patch.object(uuid, "uuid4", return_value=stub_uid):
            runner.invoke(app, ["submit", job_type, fx_job_body_file])

        with tempfile.TemporaryDirectory() as d:
            result = runner.invoke(app, ["pop", job_type, "-d", d])
            assert result.exit_code == 0
            assert len(os.listdir(d)) == 1.0
            job_file = os.path.join(d, stub_uid.hex)
            assert os.path.exists(job_file)
            with open(job_file, "rb") as f, open(fx_job_body_file, "rb") as f2:
                assert f2.read() == f.read()

    def test_pop_with_invalid_dir(self, fx_job_body_file: str) -> None:
        job_type = self.fx_faker.job_type()
        runner.invoke(app, ["submit", job_type, fx_job_body_file])
        result = runner.invoke(
            app, ["pop", job_type, "-d", dirname := self.fx_faker.file_name()]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stdout = _get_stdout(result)
        assert "Error" in stdout
        assert "does not exist" in stdout
        assert "directory" in stdout
        assert dirname in stdout

    def test_pop_with_invalid_dir2(self, fx_job_body_file: str) -> None:
        job_type = self.fx_faker.job_type()
        runner.invoke(app, ["submit", job_type, fx_job_body_file])
        with tempfile.NamedTemporaryFile() as f:
            result = runner.invoke(app, ["pop", job_type, "-d", f.name])
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stdout = _get_stdout(result)
            assert "Error" in stdout
            assert "is not a directory" in stdout
            assert f.name in stdout
