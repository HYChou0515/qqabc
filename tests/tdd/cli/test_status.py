from __future__ import annotations

import pytest

from tests.tdd.cli.utils import (
    ALL_STATUS,
    BAD_ARG_EXIT_CODE,
    NOT_FOUND_CODE,
    AddJobMixin,
    UpdateStatusMixin,
)
from tests.utils import get_sterr


class TestCliUpdateStatus(UpdateStatusMixin, AddJobMixin):
    def test_post_result_to_absent_job(self) -> None:
        result = self.runner.invoke(
            self.app, ["update", job_id := self.fx_faker.job_id(), "-s", "success"]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    @pytest.mark.parametrize("status", ALL_STATUS)
    def test_get_updated_status(self, status: str) -> None:
        aj, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "status", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

        for _ in range(3):
            self._post_status(aj.job_id, status, with_result=False)
            self._assert_posted_status(aj.job_id, status)
            status = self.fx_faker.job_status_enum()
