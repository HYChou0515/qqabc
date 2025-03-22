from __future__ import annotations

import uuid
from functools import partial
from typing import TYPE_CHECKING
from unittest.mock import patch

from qqabc.application.domain.service.job_queue_service import JobQueueService
from tdd.cli.utils import BAD_ARG_EXIT_CODE, AddJobMixin, get_stdout, get_sterr

if TYPE_CHECKING:
    from collections.abc import Callable

    from click.testing import Result as ClickResult
    from pytest_mock import MockerFixture


class TestCliSubmit(AddJobMixin):
    def test_submit_without_args(
        self,
    ) -> None:
        result = self.runner.invoke(self.app, ["submit"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def _test_submit_job(
        self, mocker: MockerFixture, submit_job: Callable[[], ClickResult]
    ) -> None:
        stub_uid = uuid.uuid4()
        spy = mocker.spy(JobQueueService, "add_job")
        with (
            patch.object(uuid, "uuid4", return_value=stub_uid) as spy2,
        ):
            result = submit_job()
            assert result.exit_code == 0
            stdout = get_stdout(result)
            assert "job submitted" in stdout
            assert "job id = " in stdout
            assert stub_uid.hex in stdout
            assert spy.call_count == 1
            assert spy2.call_count == 1

    def test_submit(self, mocker: MockerFixture) -> None:
        self._test_submit_job(mocker, self._submit_job)

    def test_submit_with_file(
        self, fx_job_body_file: str, mocker: MockerFixture
    ) -> None:
        self._test_submit_job(
            mocker, partial(self._submit_job_by_file, fx_job_body_file)
        )

    def test_submit_with_invalid_file(
        self,
    ) -> None:
        fname = self.fx_faker.file_name()
        result = self._submit_job_by_file(fname)
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert fname in stderr
        assert "does not exist" in stderr
