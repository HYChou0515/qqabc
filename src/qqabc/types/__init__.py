from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Literal, NewType, TypeVar

if TYPE_CHECKING:
    import datetime as dt


class StrEnum(str, Enum):
    pass


class StatusEnum(StrEnum):
    INITIAL = "INITIAL"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


JobBody = NewType("JobBody", object)
SerializedJobBody = NewType("SerializedJobBody", bytes)
Result = NewType("Result", object)
SerializedResult = NewType("SerializedResult", bytes)

GJobBody = TypeVar("GJobBody", bound=Any)
GSerializedJobBody = TypeVar("GSerializedJobBody", bound=SerializedJobBody)
GResult = TypeVar("GResult", bound=Any)
GSerializedResult = TypeVar("GSerializedResult", bound=SerializedResult)


class SupportEq:  # noqa: PLW1641: hash should not be implemented, there's no need to put this object into a set
    def __eq__(self, others: object) -> bool:
        if not isinstance(others, self.__class__):
            return NotImplemented
        return self.__dict__ == others.__dict__


class SupportRepr:
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({', '.join([f'{k}={v!r}' for k, v in self.__dict__.items()])})"
        )


class Job(SupportEq, SupportRepr, Generic[GJobBody]):
    def __init__(
        self, *, job_type: str, job_id: str, job_body: GJobBody, nice: int = 0
    ) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body = job_body
        self.nice = nice


class QQABC(Enum):
    NO_RESULT = 1


NO_RESULT = QQABC.NO_RESULT


class SerializedJob(SupportEq, SupportRepr):
    def __init__(
        self,
        *,
        job_type: str,
        job_id: str,
        job_body_serialized: SerializedJobBody,
        nice: int = 0,
    ) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body_serialized = job_body_serialized
        self.nice = nice


class JobStatus(SupportEq, SupportRepr):
    def __init__(  # noqa: PLR0913
        self,
        *,
        status_id: str,
        job_id: str,
        issue_time: dt.datetime,
        status: StatusEnum,
        detail: str,
        result: Result | Literal[QQABC.NO_RESULT],
    ) -> None:
        self.status_id = status_id
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result = result


class SerializedJobStatus:
    def __init__(  # noqa: PLR0913
        self,
        *,
        status_id: str,
        job_id: str,
        issue_time: dt.datetime,
        status: StatusEnum,
        detail: str,
        result_serialized: SerializedResult | None,
    ) -> None:
        self.status_id = status_id
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result_serialized = result_serialized


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
