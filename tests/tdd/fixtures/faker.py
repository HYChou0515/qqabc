from __future__ import annotations

import datetime as dt

import pytest
from faker import Faker as _Faker

from qqabc.application.domain.model.job import (
    JobBody,
    JobStatus,
    SerializedJob,
    SerializedJobBody,
    StatusEnum,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewJobRequest,
    NewSerializedJobRequest,
)
from tests.tdd.cli.utils import ALL_STATUS


class Faker(_Faker):
    def job_body(self) -> object:
        return self.random_element([object() for _ in range(20)])

    def uuid4_hex(self) -> str:
        return self.uuid4(cast_to=lambda x: x.hex)

    def job_id(self) -> str:
        return self.uuid4_hex()

    def status_id(self) -> str:
        return self.uuid4_hex()

    def job_body_serialized(self) -> SerializedJobBody:
        return SerializedJobBody(self.json_bytes())

    def job_type(self) -> str:
        return self.name()

    def job_status_enum(self) -> str:
        return self.random_element(ALL_STATUS)

    def status_enum(self) -> StatusEnum:
        return self.random_element(list(StatusEnum))

    def serialized_job(self) -> SerializedJob:
        return SerializedJob(
            job_type=self.job_type(),
            job_id=self.job_id(),
            job_body_serialized=self.job_body_serialized(),
            created_time=self.date_time(tzinfo=dt.timezone.utc),
            nice=0,
        )

    def job_status(self, *, job_id: str | None = None) -> JobStatus:
        job_id_ = self.job_id() if job_id is None else job_id
        return JobStatus(
            status_id=self.status_id(),
            job_id=job_id_,
            issue_time=self.date_time(tzinfo=dt.timezone.utc),
            status=self.status_enum(),
            detail=self.sentence(),
        )

    def new_job_request(
        self,
        *,
        job_type: str = ...,  # type: ignore[assignment]
        job_body: JobBody = ...,  # type: ignore[assignment]
    ) -> NewJobRequest:
        job_body_ = self.job_body() if job_body is ... else job_body
        job_type_ = self.job_type() if job_type is ... else job_type
        return NewJobRequest(
            job_type=job_type_,
            job_body=job_body_,
        )

    def new_serialized_job_request(
        self,
        *,
        job_type: str = ...,  # type: ignore[assignment]
        job_body_serialized: SerializedJobBody = ...,  # type: ignore[assignment]
    ) -> NewSerializedJobRequest:
        job_body_serialized_ = (
            self.job_body_serialized()
            if job_body_serialized is ...
            else job_body_serialized
        )
        job_type_ = self.job_type() if job_type is ... else job_type
        return NewSerializedJobRequest(
            job_type=job_type_,
            job_body_serialized=job_body_serialized_,
        )

    def new_status_request(
        self,
        *,
        job_id: str = ...,  # type: ignore[assignment]
        status: StatusEnum = ...,  # type: ignore[assignment]
        detail: str = ...,  # type: ignore[assignment]
    ) -> NewJobStatusRequest:
        job_id_ = self.uuid4() if job_id is ... else job_id
        status_ = self.status_enum() if status is ... else status
        detail_ = self.sentence() if detail is ... else detail
        return NewJobStatusRequest(
            job_id=job_id_,
            status=status_,
            detail=detail_,
        )


@pytest.fixture
def fx_faker() -> Faker:
    return Faker()
