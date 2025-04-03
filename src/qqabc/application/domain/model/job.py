from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Any, Generic, Literal, NewType, TypeVar

from typing_extensions import Self

from qqabc.common.types import StrEnum

GJobBody = TypeVar("GJobBody", bound=Any)
SerializedJobBody = NewType("SerializedJobBody", bytes)
GSerializedJobBody = TypeVar("GSerializedJobBody", bound=SerializedJobBody)
GResult = TypeVar("GResult", bound=Any)
SerializedResult = NewType("SerializedResult", bytes)
GSerializedResult = TypeVar("GSerializedResult", bound=SerializedResult)


class StatusEnum(StrEnum):
    INITIAL = "INITIAL"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class QQABC(Enum):
    NO_RESULT = 1


class SupportRepr:
    def __repr__(self) -> str:
        tokens = []
        for k, v in self.__dict__.items():
            if isinstance(v, dt.datetime):
                tokens.append(f"{k}={v.isoformat()}")
            elif isinstance(v, str):
                tokens.append(f"{k}='{v}'")
            else:
                tokens.append(f"{k}={v}")
        return f"{self.__class__.__name__}({', '.join(tokens)})"


class SupportEq:  # noqa: PLW1641: hash should not be implemented, there's no need to put this object into a set
    def __eq__(self, others: object) -> bool:
        if not isinstance(others, self.__class__):
            return NotImplemented
        return self.__dict__ == others.__dict__


class SupportSerialization:
    def get_serializable(self) -> dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_serializable(cls, serializable: dict[str, Any]) -> Self:
        return cls(**serializable)


Result = NewType("Result", object)


class SerializedJobStatus(SupportEq, SupportRepr, SupportSerialization):
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


class SerializedJob(SupportEq, SupportRepr, SupportSerialization):
    def __init__(
        self,
        *,
        job_type: str,
        job_id: str,
        job_body_serialized: SerializedJobBody,
        created_time: dt.datetime,
        nice: int,
    ) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body_serialized = job_body_serialized
        self.created_time = created_time
        self.nice = nice


NO_RESULT = QQABC.NO_RESULT


class Job(SupportEq, SupportRepr, Generic[GJobBody]):
    def __init__(
        self,
        *,
        job_type: str,
        job_id: str,
        job_body: GJobBody,
        created_time: dt.datetime,
        nice: int,
    ) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body = job_body
        self.created_time = created_time
        self.nice = nice


JobBody = NewType("JobBody", object)
