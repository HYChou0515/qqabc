from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from qqabc.application.domain.model.job import (
    NO_RESULT,
    QQABC,
    Result,
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
