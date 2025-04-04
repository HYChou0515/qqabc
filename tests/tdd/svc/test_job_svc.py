from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.tdd.utils import TestUtils

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory


class TestJobConsumer(TestUtils):
    @pytest.mark.parametrize("pop_job", [True, False])
    def test_get_job_result_of_no_status_job(self, *, pop_job: bool) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        if pop_job:
            self.job_controller.get_next_job(job_type=self.job_type)
        assert self.status_svc.get_latest_status(job.job_id) is None

    @pytest.mark.parametrize("with_result", [True, False])
    @pytest.mark.parametrize("multiple_statuses", [1, 2, 100])
    def test_get_job_result_after_job_pop(
        self,
        *,
        with_result: bool,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        self.job_controller.get_next_job(job_type=self.job_type)
        self._assert_job_status_is_same_as_added(
            job,
            with_result=with_result,
            multiple_statuses=multiple_statuses,
            freezer=freezer,
        )
