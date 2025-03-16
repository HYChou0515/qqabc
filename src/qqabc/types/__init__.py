from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from qqabc.application.domain.model.job import (
    NO_RESULT,
    QQABC,
    JobBody,
    Result,
    SerializedJobBody,
    SerializedResult,
    StatusEnum,
)

if TYPE_CHECKING:
    import datetime as dt


class BaseNewJobStatusRequest:
    def __init__(
        self,
        *,
        job_id: str,
        issue_time: dt.datetime | None = None,
        status: StatusEnum,
        detail: str,
    ) -> None:
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail


class NewJobStatusRequest(BaseNewJobStatusRequest):
    def __init__(
        self,
        *,
        job_id: str,
        status: StatusEnum,
        issue_time: dt.datetime | None = None,
        detail: str,
        result: Result | Literal[QQABC.NO_RESULT] = NO_RESULT,
    ) -> None:
        super().__init__(
            job_id=job_id, issue_time=issue_time, status=status, detail=detail
        )
        self.result = result


class NewSerializedJobStatusRequest(BaseNewJobStatusRequest):
    def __init__(
        self,
        *,
        job_id: str,
        status: StatusEnum,
        issue_time: dt.datetime | None = None,
        detail: str,
        result_serialized: SerializedResult | None = None,
    ) -> None:
        super().__init__(
            job_id=job_id, issue_time=issue_time, status=status, detail=detail
        )
        self.result_serialized = result_serialized


class BaseNewJobRequest:
    def __init__(self, *, job_type: str, nice: int = 0) -> None:
        self.job_type = job_type
        self.nice = nice


class NewJobRequest(BaseNewJobRequest):
    def __init__(self, *, job_type: str, job_body: JobBody, nice: int = 0) -> None:
        super().__init__(job_type=job_type, nice=nice)
        self.job_body = job_body


class NewSerializedJobRequest(BaseNewJobRequest):
    def __init__(
        self, *, job_type: str, job_body_serialized: SerializedJobBody, nice: int = 0
    ) -> None:
        super().__init__(job_type=job_type, nice=nice)
        self.job_body_serialized = job_body_serialized
