from __future__ import annotations

import pytest

from tests.tdd.cli.utils import (
    BAD_ARG_EXIT_CODE,
    NOT_FOUND_CODE,
    AddJobMixin,
    AddJobType,
)
from tests.utils import get_stdout, get_sterr


class TestCliPostResult(AddJobMixin):
    def test_post_result_to_absent_job(self) -> None:
        result = self.runner.invoke(
            self.app, ["post", job_id := self.fx_faker.job_id(), "-s", "success"]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    def test_get_result_when_no_job(self) -> None:
        result = self.runner.invoke(
            self.app, ["get", "result", job_id := self.fx_faker.job_id()]
        )
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert job_id in stderr
        assert "does not exist" in stderr

    @staticmethod
    def _assert_status(status: str, stdout: str) -> None:
        if status == "process":
            assert "RUNNING" in stdout
        elif status == "success":
            assert "COMPLETED" in stdout
        elif status == "fail":
            assert "FAILED" in stdout
        else:
            raise NotImplementedError

    def _assert_posted_result(self, job_id: str, s_result: str | None) -> None:
        result = self.runner.invoke(self.app, ["get", "result", job_id])
        if s_result:
            assert result.exit_code == 0
            assert s_result.encode() == result.stdout_bytes
        else:
            assert result.exit_code == NOT_FOUND_CODE
            assert result.stdout == ""

    def _assert_posted_status(self, job_id: str, status: str) -> None:
        result = self.runner.invoke(self.app, ["get", "status", job_id])
        assert result.exit_code == 0
        self._assert_status(status, result.stdout)
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

    def _post_status(
        self, job_id: str, status: str, *, with_result: bool = True
    ) -> str | None:
        if with_result:
            s_result = self.fx_faker.json()
        else:
            s_result = None
        result = self.runner.invoke(
            self.app,
            ["post", job_id, "-s", status],
            input=s_result,
        )
        assert result.exit_code == 0
        return s_result

    @pytest.mark.parametrize("status", ["process", "success", "fail"])
    def test_get_posted_result(self, status: str) -> None:
        aj, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "status", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        table_headers = ["Status", "Time", "Detail"]
        assert all(header in result.stdout for header in table_headers)

        result = self.runner.invoke(self.app, ["get", "result", aj.job_id])
        assert result.exit_code == NOT_FOUND_CODE
        assert result.stdout == ""

        for i in range(3):
            s_result = self._post_status(aj.job_id, status, with_result=i % 2 == 0)
            self._assert_posted_result(aj.job_id, s_result)
            self._assert_posted_status(aj.job_id, status)

            status = self.fx_faker.job_status_enum()

    def test_get_jobs_from_nothing(self) -> None:
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr + result.stdout
        self._assert_job_in_table([], get_stdout(result))

    def _assert_job_in_table(self, ajs: list[AddJobType], s: str) -> None:
        table_headers = ["ID", "Type", "Time"]
        assert all(header in s for header in table_headers)
        for aj in ajs:
            assert aj.job_id in s
            assert aj.job_type in s

    def test_get_jobs(self) -> None:
        aj1, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr + result.stdout
        self._assert_job_in_table([aj1], get_stdout(result))

        aj2, _ = self._add_job()
        result = self.runner.invoke(self.app, ["get", "jobs"])
        assert result.exit_code == 0, result.stderr + result.stdout
        self._assert_job_in_table([aj1, aj2], get_stdout(result))
