from __future__ import annotations

import os
import tempfile

from tests.tdd.cli.utils import (
    BAD_ARG_EXIT_CODE,
    AddJobMixin,
    PopJobMixin,
    job_file_name,
)
from tests.utils import assert_result_success, get_sterr


class TestCliAddJob(AddJobMixin, PopJobMixin):
    def test_pop_to_stdout(self) -> None:
        aj, _ = self._add_job()
        result = self._pop_job(job_type=aj.job_type, pipe=True)
        assert_result_success(result)
        assert aj.job_body.encode() == result.stdout_bytes

    def test_pop_to_dir(self) -> None:
        aj, _ = self._add_job()

        with tempfile.TemporaryDirectory() as d:
            result = self._pop_job(job_type=aj.job_type, d=d)
            assert_result_success(result)
            assert len(os.listdir(d)) == 1.0
            job_file = os.path.join(d, job_file_name(aj.job_id))
            assert os.path.exists(job_file)
            with open(job_file, "rb") as f:
                assert aj.job_body.encode() == f.read()

    def test_pop_with_invalid_dir(self) -> None:
        aj, _ = self._add_job()
        self._add_job()
        result = self._pop_job(
            job_type=aj.job_type,
            d=(dirname := self.fx_faker.file_name()),
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert "does not exist" in stderr
        assert "directory" in stderr
        assert dirname in stderr

    def test_pop_with_invalid_dir2(self) -> None:
        aj, _ = self._add_job()
        with tempfile.NamedTemporaryFile() as f:
            result = self._pop_job(job_type=aj.job_type, d=f.name)
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stderr = get_sterr(result)
            assert "Error" in stderr
            assert "is not a directory" in stderr
            assert f.name in stderr

    def test_post_result_without_args(self) -> None:
        result = self.runner.invoke(self.app, ["post"])
        assert result.exit_code == BAD_ARG_EXIT_CODE
