from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import patch

from qqabc.application.domain.service.job_queue_service import JobQueueService
from tdd.cli.utils import BAD_ARG_EXIT_CODE, BaseCliTest, get_stdout, get_sterr

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


class TestCliSubmit(BaseCliTest):
    def test_submit_without_args(
        self,
    ) -> None:
        result = self.runner.invoke(self.app, ["submit"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_submit(self, mocker: MockerFixture) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueService, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = self.runner.invoke(
                self.app,
                ["submit", self.fx_faker.job_type()],
                input=self.fx_faker.json(),
            )
            assert result.exit_code == 0
            stdout = get_stdout(result)
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
            result = self.runner.invoke(
                self.app, ["submit", self.fx_faker.job_type(), "-f", fx_job_body_file]
            )
            assert result.exit_code == 0
            stdout = get_stdout(result)
            assert "job submitted" in stdout
            assert "job id = " in stdout
            assert stub_uid.hex in stdout
            assert spy.call_count == 1
            assert spy2.call_count == 1

    def test_submit_with_invalid_file(
        self,
    ) -> None:
        fname = self.fx_faker.file_name()
        result = self.runner.invoke(
            self.app, ["submit", self.fx_faker.job_type(), "-f", fname]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert fname in stderr
        assert "does not exist" in stderr
