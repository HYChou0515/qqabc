from __future__ import annotations

import tempfile

from tests.tdd.cli.utils import (
    AddJobMixin,
    PopJobMixin,
    job_file_name,
)
from tests.utils import assert_result_success, get_stderr


class TestCliConsume(AddJobMixin, PopJobMixin):
    def test_pop_without_args(
        self,
    ) -> None:
        result = self.runner.invoke(self.app, ["consume", "job"])
        assert_result_success(result)

    def test_pop_from_empty_queue(self) -> None:
        job_type = self.fx_faker.job_type()
        with tempfile.TemporaryDirectory() as d:
            result = self._pop_job(job_type=job_type, d=d)
        assert_result_success(result)
        stderr = get_stderr(result)
        assert "No job with job type" in stderr
        assert job_type in stderr

    def test_pop_job(self) -> None:
        job_, _ = self._add_job()
        result = self._pop_job()
        assert_result_success(result)
        stderr = get_stderr(result)
        assert "Job consumed" in stderr
        assert f"into {job_file_name(job_.job_id)}" in stderr
