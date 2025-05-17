from __future__ import annotations

import pytest

from tests.tdd.cli.utils import (
    ALL_STATUS,
    BAD_ARG_EXIT_CODE,
    NOT_FOUND_CODE,
    AddJobMixin,
    UpdateStatusMixin,
)
from tests.utils import assert_result_success, assert_status, get_stderr, get_stdout


class TestCliUpdateStatus(UpdateStatusMixin, AddJobMixin):
    def test_post_result_to_absent_job(self) -> None:
        result = self._update_status(job_id := self.fx_faker.job_id(), "success")
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_stderr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    def test_get_status_if_no_status(self) -> None:
        aj, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "status", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        table_headers = ["Status", "Time", "Detail"]
        stdout = get_stdout(result)
        assert all(header in stdout for header in table_headers)

    @pytest.mark.parametrize("status", ALL_STATUS)
    def test_get_updated_status(self, status: str) -> None:
        aj, _ = self._add_job()
        for _ in range(3):
            self._update_status(aj.job_id, status)
            self._assert_posted_status(aj.job_id, status)
            status = self.fx_faker.job_status_enum()

    @pytest.mark.parametrize("status", ALL_STATUS)
    def test_update_status_with_detail(self, status: str) -> None:
        aj, _ = self._add_job()
        detail = " ".join(self.fx_faker.words())
        self._update_status(aj.job_id, status, detail=detail)
        result = self.runner.invoke(self.app, ["get", "status", aj.job_id])
        assert_result_success(result)
        assert detail in get_stdout(result)

    @pytest.mark.parametrize("status", ALL_STATUS)
    @pytest.mark.parametrize("all_options", ["--all", "-a"])
    def test_get_all_status(self, status: str, all_options: str) -> None:
        aj, _ = self._add_job()
        statuses = []
        for _ in range(3):
            self._update_status(aj.job_id, status)
            statuses.append(status)
            status = self.fx_faker.job_status_enum()
        result = self.runner.invoke(self.app, ["get", "status", aj.job_id, all_options])
        assert_result_success(result)
        table_headers = ["Status", "Time", "Detail"]
        stdout = get_stdout(result)
        assert all(header in stdout for header in table_headers)
        for status in statuses:
            assert_status(status, stdout)
