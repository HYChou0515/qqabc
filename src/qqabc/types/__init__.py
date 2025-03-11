from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NewType

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


JobBody = NewType("JobBody", Any)  # type: ignore[valid-newtype]
SerializedJobBody = NewType("SerializedJobBody", bytes)
Result = NewType("Result", Any)  # type: ignore[valid-newtype]
SerializedResult = NewType("SerializedResult", bytes)


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


class Job(SupportEq, SupportRepr):
    def __init__(
        self, *, job_type: str, job_id: str, job_body: JobBody, nice: int = 0
    ) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body = job_body
        self.nice = nice


class Singleton(type):
    _instances: ClassVar[dict] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


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


class NewJobStatusRequest:
    def __init__(
        self,
        *,
        job_id: str,
        status: StatusEnum,
        issue_time: dt.datetime | None = None,
        detail: str,
        result: Result | Literal[QQABC.NO_RESULT] = NO_RESULT,
    ) -> None:
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result = result


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


class EmptyQueueError(IndexError):
    pass
