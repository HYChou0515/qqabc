from __future__ import annotations

import os
import tempfile

from tests.tdd.cli.utils import (
    BAD_ARG_EXIT_CODE,
    AddJobMixin,
    AddJobType,
    PopJobMixin,
    job_file_name,
)
from tests.utils import assert_result_success, get_stderr, get_stdout


class TestCliAddJob(AddJobMixin, PopJobMixin):
    def test_pop_to_stdout(self) -> None:
        aj, _ = self._add_job()
        result = self._pop_job(job_type=aj.job_type, pipe=True)
        assert_result_success(result)
        assert aj.job_body.encode() == result.stdout_bytes

    def _assert_job_in_dir(self, dirname: str, added_job: AddJobType) -> None:
        assert len(os.listdir(dirname)) == 1.0
        job_file = os.path.join(dirname, job_file_name(added_job.job_id))
        self._assert_job_in_file(job_file, added_job)

    def _assert_job_in_file(self, fpath: str, added_job: AddJobType) -> None:
        assert os.path.exists(fpath)
        with open(fpath, "rb") as f:
            assert added_job.job_body.encode() == f.read()

    def test_pop_to_file(self) -> None:
        aj, _ = self._add_job()
        fpath = self.fx_faker.file_name()
        result = self._pop_job(job_type=aj.job_type, fpath=fpath)
        assert_result_success(result)
        stderr = get_stderr(result)
        assert "Job consumed" in stderr
        assert f"into {fpath}" in stderr
        self._assert_job_in_file(fpath, aj)

    def test_pop_to_dir(self) -> None:
        aj, _ = self._add_job()

        with tempfile.TemporaryDirectory() as d:
            result = self._pop_job(job_type=aj.job_type, d=d)
            assert_result_success(result)
            self._assert_job_in_dir(d, aj)

    def test_pop_with_absent_dir(self) -> None:
        aj, _ = self._add_job()
        self._add_job()
        # Create a non-existing directory
        d = self.fx_faker.file_name()
        result = self._pop_job(
            job_type=aj.job_type,
            d=(d),
        )
        assert_result_success(result)
        stderr = get_stderr(result)
        assert "Dir created: " in stderr
        assert d in stderr
        self._assert_job_in_dir(d, aj)

    def test_pop_with_invalid_dir(self) -> None:
        aj, _ = self._add_job()
        with tempfile.NamedTemporaryFile() as f:
            result = self._pop_job(job_type=aj.job_type, d=f.name)
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stderr = get_stderr(result)
            assert "Error" in stderr
            assert "is not a directory" in stderr
            assert f.name in stderr

    def test_post_result_without_args(self) -> None:
        result = self.runner.invoke(self.app, ["post"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_get_jobs_from_nothing(self) -> None:
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert_result_success(result)
        self._assert_job_in_table([], get_stdout(result))

    def test_get_jobs(self) -> None:
        aj1, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert_result_success(result)
        self._assert_job_in_table([aj1], get_stdout(result))

        aj2, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert_result_success(result)
        self._assert_job_in_table([aj1, aj2], get_stdout(result))
