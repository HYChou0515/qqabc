from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from dependency_injector import providers

from qqabc.adapter.out.pseristence.job_repo_adapter import MemoryJobRepo
from qqabc.adapter.out.pseristence.job_status_dao import MemoryJobStatusRepo
from tests.tdd.utils import TestUtils

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory


class TestJobConsumer(TestUtils):
    @pytest.fixture(autouse=True)
    def setup_method_2(
        self,
    ) -> None:
        self.container.job_dao.override(providers.Factory(MemoryJobRepo))
        self.svc = self.container.result_service()

    def test_add_result_of_absent_job(self) -> None:
        request = self.faker.new_job_result_request()
        with (
            patch.object(
                MemoryJobRepo,
                "get_job",
                return_value=None,
            ) as spy,
            pytest.raises(ValueError, match=request.job_id),
        ):
            self.svc.add_job_result(request)
        spy.assert_called_once_with(request.job_id)

    def test_get_result_of_absent_job(self) -> None:
        job_id = self.faker.job_id()
        with (
            patch.object(
                MemoryJobRepo,
                "get_job",
                return_value=None,
            ) as spy,
        ):
            assert self.svc.get_latest_result(job_id) is None
            spy.assert_called_once_with(job_id)

    def test_list_job_result_of_absent_job(self) -> None:
        job_id = self.faker.job_id()
        with (
            patch.object(
                MemoryJobRepo,
                "get_job",
                return_value=None,
            ) as spy,
        ):
            assert self.svc.list_job_results(job_id) == []
            spy.assert_called_once_with(job_id)

    def test_list_job_results(self) -> None:
        job_id = self.faker.job_id()
        results = [self.faker.job_result(job_id=job_id) for _ in range(3)]
        with (
            patch.object(
                MemoryJobRepo,
                "get_job",
            ) as spy_get_job,
            patch.object(
                MemoryJobStatusRepo,
                "iter_result",
                return_value=results,
            ) as spy_iter_result,
        ):
            assert self.svc.list_job_results(job_id) == results
            spy_get_job.assert_called_once_with(job_id)
            spy_iter_result.assert_called_once_with(job_id)

    @pytest.mark.parametrize("multiple_statuses", [1, 2, 100])
    def test_get_job_result_after_job_pop(
        self,
        *,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        self.job_controller.get_next_job(job_type=self.job_type)
        self._assert_job_status_is_same_as_added(
            job,
            multiple_statuses=multiple_statuses,
            freezer=freezer,
        )
