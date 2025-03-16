from __future__ import annotations

from typing import Literal

import pytest
from faker import Faker as _Faker

from qqabc.application.domain.model.job import (
    QQABC,
    JobBody,
    Result,
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


class Faker(_Faker):
    def job_body(self) -> object:
        return self.random_element([object() for _ in range(20)])

    def uuid4_hex(self) -> str:
        return self.uuid4(cast_to=lambda x: x.hex)

    def job_id(self) -> str:
        return self.uuid4_hex()

    def job_body_serialized(self) -> bytes:
        return self.json_bytes()

    def job_result_serialized(self) -> bytes:
        return self.json_bytes()

    def job_type(self) -> str:
        return self.name()

    def job_status(self) -> str:
        return self.random_element(["success", "fail", "process"])

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

    def status_enum(self) -> StatusEnum:
        return self.random_element(StatusEnum)

    def new_status_request(
        self,
        *,
        job_id: str = ...,  # type: ignore[assignment]
        status: StatusEnum = ...,  # type: ignore[assignment]
        detail: str = ...,  # type: ignore[assignment]
        result: Result | Literal[QQABC.NO_RESULT] = ...,  # type: ignore[assignment]
    ) -> NewJobStatusRequest:
        job_id_ = self.uuid4() if job_id is ... else job_id
        status_ = self.status_enum() if status is ... else status
        detail_ = self.sentence() if detail is ... else detail
        result_ = self.job_result() if result is ... else result
        return NewJobStatusRequest(
            job_id=job_id_,
            status=status_,
            detail=detail_,
            result=result_,
        )


@pytest.fixture
def fx_faker() -> Faker:
    return Faker()
