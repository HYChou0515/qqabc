import tempfile
import uuid
from collections.abc import Generator
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from qqabc import JobQueueController
from qqabc_cli.main import app

runner = CliRunner()

BAD_ARG_EXIT_CODE = 2


def test_submit_without_args() -> None:
    result = runner.invoke(app, ["submit"])
    assert result.exit_code == BAD_ARG_EXIT_CODE


@pytest.fixture
def fx_job_body_file() -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile("wb") as f:
        f.write(b"job_body")
        f.flush()
        yield f.name


def test_submit(fx_job_body_file: str, mocker: MockerFixture) -> None:
    stub_uid = uuid.uuid4()
    spy = mocker.spy(JobQueueController, "add_job")
    with (
        patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
    ):
        result = runner.invoke(app, ["submit", "my_job_type", fx_job_body_file])
        assert result.exit_code == 0
        assert "job submitted" in result.stdout
        assert "job id = " in result.stdout
        assert stub_uid.hex in result.stdout
        assert spy.call_count == 1
        assert spy2.call_count == 1


def test_submit_without_file() -> None:
    result = runner.invoke(app, ["submit", "my_job_type"])
    assert result.exit_code == BAD_ARG_EXIT_CODE


def test_submit_with_invalid_file() -> None:
    result = runner.invoke(app, ["submit", "my_job_type", "my_invalid_file.txt"])
    assert result.exit_code == BAD_ARG_EXIT_CODE
    assert "Error" in result.stdout
    assert "my_invalid_file.txt" in result.stdout
    assert "does not exist" in result.stdout
