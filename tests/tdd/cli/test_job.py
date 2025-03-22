from __future__ import annotations

import os
import tempfile

from tdd.cli.utils import BAD_ARG_EXIT_CODE, AddJobMixin, get_sterr


class TestCliAddJob(AddJobMixin):
    def test_pop_to_stdout(self) -> None:
        aj, _ = self._add_job()

        result = self.runner.invoke(self.app, ["pop", aj.job_type])
        assert result.exit_code == 0, result.stderr + result.stdout
        assert aj.job_body.encode() == result.stdout_bytes

    def test_pop_to_dir(self) -> None:
        aj, _ = self._add_job()

        with tempfile.TemporaryDirectory() as d:
            result = self.runner.invoke(self.app, ["pop", aj.job_type, "-d", d])
            assert result.exit_code == 0
            assert len(os.listdir(d)) == 1.0
            job_file = os.path.join(d, aj.job_id)
            assert os.path.exists(job_file)
            with open(job_file, "rb") as f:
                assert aj.job_body.encode() == f.read()

    def test_pop_with_invalid_dir(self) -> None:
        aj, _ = self._add_job()
        self._add_job()
        result = self.runner.invoke(
            self.app, ["pop", aj.job_type, "-d", dirname := self.fx_faker.file_name()]
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
            result = self.runner.invoke(self.app, ["pop", aj.job_type, "-d", f.name])
            assert result.exit_code == BAD_ARG_EXIT_CODE
            stderr = get_sterr(result)
            assert "Error" in stderr
            assert "is not a directory" in stderr
            assert f.name in stderr

    def test_post_result_without_args(self) -> None:
        result = self.runner.invoke(self.app, ["post"])
        assert result.exit_code == BAD_ARG_EXIT_CODE
