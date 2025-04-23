from __future__ import annotations

import datetime as dt
import pickle
from typing import TYPE_CHECKING, overload

import pytest

from qqabc.application.domain.model.job import (
    Job,
    JobBody,
    JobStatus,
    Result,
    SerializedJobBody,
    SerializedResult,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializer,
    JobSerializerRegistry,
)

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory

    from qqabc.application.domain.service.job_queue_service import IJobQueueService
    from qqabc.application.domain.service.status_service import IStatusService
    from tests.tdd.fixtures.faker import Faker


class MyJobSerializer(JobSerializer):
    @overload
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        pass

    @overload
    def serialize(self, job_body: Result) -> SerializedResult:
        pass

    def serialize(
        self, job_body: JobBody | Result
    ) -> SerializedJobBody | SerializedResult:
        return SerializedJobBody(pickle.dumps(job_body))

    @overload
    def deserialize(self, serialized: SerializedJobBody) -> JobBody:
        pass

    @overload
    def deserialize(self, serialized: SerializedResult) -> Result:
        pass

    def deserialize(
        self, serialized: SerializedJobBody | SerializedResult
    ) -> JobBody | Result:
        return pickle.loads(serialized)


class TestUtils:
    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        fx_faker: Faker,
        fx_job_queue_controller: IJobQueueService,
        fx_job_serializer_registry: JobSerializerRegistry,
        fx_status_svc: IStatusService,
    ) -> None:
        self.faker = fx_faker
        self.job_type = fx_faker.job_type()
        self.status_svc = fx_status_svc
        self.job_controller = fx_job_queue_controller
        fx_job_serializer_registry.register_job_serializer(
            job_type=self.job_type,
            job_serializer=MyJobSerializer(),
        )

    def _assert_job_status_is_same_as_added(
        self,
        job: Job,
        *,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        latest_status: tuple[dt.datetime | None, JobStatus | None] = None, None
        for _ in range(multiple_statuses):
            freezer.move_to(t := self.faker.date_time(tzinfo=dt.timezone.utc))
            s = self.status_svc.add_job_status(
                self.faker.new_status_request(job_id=job.job_id)
            )
            if latest_status[0] is None or t > latest_status[0]:
                latest_status = t, s
        status1 = latest_status[1]
        status2 = self.status_svc.get_latest_status(job.job_id)
        assert status2 is not None
        assert status1 == status2
