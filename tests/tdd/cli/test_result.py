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


class TestCliPostResult(UpdateStatusMixin, AddJobMixin):
    def test_get_result_when_no_job(self) -> None:
        result = self.runner.invoke(
            self.app, ["get", "result", job_id := self.fx_faker.job_id()]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    @pytest.mark.parametrize("status", ALL_STATUS)
    def test_get_posted_result(self, status: str) -> None:
        aj, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "result", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        assert result.stdout == ""

        for i in range(3):
            s_result = self._post_status(aj.job_id, status, with_result=i % 2 == 0)
            self._assert_posted_result(aj.job_id, s_result)
            self._assert_posted_status(aj.job_id, status)

            status = self.fx_faker.job_status_enum()
