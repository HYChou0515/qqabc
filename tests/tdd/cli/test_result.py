from __future__ import annotations

from tests.tdd.cli.utils import (
    BAD_ARG_EXIT_CODE,
    AddJobMixin,
    UpdateStatusMixin,
)
from tests.utils import assert_result_success, get_stderr


class TestCliPostResult(UpdateStatusMixin, AddJobMixin):
    def test_get_status_when_no_job(self) -> None:
        status = self.runner.invoke(
            self.app, ["get", "status", job_id := self.fx_faker.job_id()]
        )
        assert status.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_stderr(status)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    def test_get_posted_result(self) -> None:
        aj, _ = self._add_job()
        self._assert_posted_result_stdout(aj.job_id, None)
        self._assert_posted_result_dir(aj.job_id, None)
        self._assert_posted_result_file(aj.job_id, None)

        for _ in range(3):
            r = self._upload_result(
                aj.job_id, result := self.fx_faker.json_bytes(), from_="file"
            )
            assert_result_success(r)
            self._assert_posted_result_stdout(aj.job_id, result)
            self._assert_posted_result_dir(aj.job_id, result)
            self._assert_posted_result_file(aj.job_id, result)
