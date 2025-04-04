from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import pytest

from qqabc.application.domain.model.job import (
    Result,
    SerializedResult,
    StatusEnum,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
    NewSerializedJobStatusRequest,
)
from qqabc.common.exceptions import (
    JobNotFoundError,
)
from tests.tdd.utils import TestUtils

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory


class TestStatusService(TestUtils):
    @pytest.mark.parametrize("with_result", [True, False])
    @pytest.mark.parametrize("multiple_statuses", [1, 2, 100])
    def test_get_job_result(
        self,
        *,
        with_result: bool,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        self._assert_job_status_is_same_as_added(
            job,
            with_result=with_result,
            multiple_statuses=multiple_statuses,
            freezer=freezer,
        )

    def test_return_job_serialized_result(self, freezer: FrozenDateTimeFactory) -> None:
        now = self.faker.date_time(tzinfo=dt.timezone.utc)
        freezer.move_to(now)
        job = self.job_controller.add_job(
            self.faker.new_job_request(
                job_type=self.job_type,
            )
        )
        status_request = NewSerializedJobStatusRequest(
            job_id=job.job_id,
            status=StatusEnum.COMPLETED,
            detail="Job completed successfully",
            result_serialized=SerializedResult(b"my result"),
        )
        status = self.status_svc.add_job_status(status_request)
        assert status.job_id == status_request.job_id
        assert status.issue_time == now
        assert status.status == status_request.status
        assert status.detail == status_request.detail
        assert status.result_serialized == status_request.result_serialized

    def test_return_job_result(self, freezer: FrozenDateTimeFactory) -> None:
        now = self.faker.date_time(tzinfo=dt.timezone.utc)
        freezer.move_to(now)
        job = self.job_controller.add_job(
            self.faker.new_job_request(
                job_type=self.job_type,
            )
        )
        status_request = NewJobStatusRequest(
            job_id=job.job_id,
            status=StatusEnum.COMPLETED,
            detail="Job completed successfully",
            result=Result("my result"),
        )
        status = self.status_svc.add_job_status(status_request)
        assert status.job_id == status_request.job_id
        assert status.issue_time == now
        assert status.status == status_request.status
        assert status.detail == status_request.detail
        assert status.result == status_request.result

    def test_list_job_status(
        self,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        statuses = self.status_svc.list_job_status(job.job_id)
        assert len(statuses) == 0
        added_status = []
        for _ in range(3):
            s = self.status_svc.add_job_status(
                self.faker.new_status_request(job_id=job.job_id)
            )
            added_status.append(s)
        statuses = self.status_svc.list_job_status(job.job_id)
        assert len(statuses) == 3

    def test_get_job_result_of_non_existed_job(self) -> None:
        job_id = self.faker.uuid4()
        with pytest.raises(JobNotFoundError, match=job_id):
            self.status_svc.get_latest_status(job_id)
