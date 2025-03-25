from __future__ import annotations

import tempfile

from tests.tdd.cli.utils import (
    BAD_ARG_EXIT_CODE,
    AddJobMixin,
    PopJobMixin,
    job_file_name,
)
from tests.utils import assert_result_success, get_stdout, get_sterr


class TestCliConsume(AddJobMixin, PopJobMixin):
    def test_pop_without_args(
        self,
    ) -> None:
        result = self.runner.invoke(self.app, ["pop"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_pop_from_empty_queue(self) -> None:
        job_type = self.fx_faker.job_type()
        with tempfile.TemporaryDirectory() as d:
            result = self._pop_job(job_type=job_type, d=d)
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert "No job with job type" in stderr
        assert job_type in stderr

    def test_pop_job(self, fx_workdir: str) -> None:
        job_, _ = self._add_job(job_type="")
        result = self._pop_job()
        assert_result_success(result)
        stdout = get_stdout(result)
        assert "Job consumed" in stdout
        assert f"into {job_file_name(job_.job_id)}" in stdout
