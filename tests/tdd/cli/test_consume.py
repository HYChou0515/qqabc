from __future__ import annotations

import tempfile

from tdd.cli.utils import BAD_ARG_EXIT_CODE, BaseCliTest, get_sterr


class TestCliConsume(BaseCliTest):
    def test_pop_without_args(
        self,
    ) -> None:
        result = self.runner.invoke(self.app, ["pop"])
        assert result.exit_code == BAD_ARG_EXIT_CODE

    def test_pop_from_empty_queue(self) -> None:
        job_type = self.fx_faker.job_type()
        with tempfile.TemporaryDirectory() as d:
            result = self.runner.invoke(self.app, ["pop", job_type, "-d", d])
        assert result.exit_code == BAD_ARG_EXIT_CODE
        stderr = get_sterr(result)
        assert "Error" in stderr
        assert "No job with job type" in stderr
        assert job_type in stderr
