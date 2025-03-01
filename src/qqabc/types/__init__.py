from __future__ import annotations
from enum import Enum
from typing import Any, NewType
import datetime as dt

class StrEnum(str, Enum):
    pass

class StatusEnum(StrEnum):
    INITIAL = "INITIAL"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

JobBody = NewType("JobBody", Any)
SerializedJobBody = NewType("SerializedJobBody", bytes)
Result = NewType("Result", Any)
SerializedResult = NewType("SerializedResult", bytes)

class Job:
    def __init__(self, *,
                 job_type: str,
                 job_id: str,
                 job_body: JobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body = job_body
        self.nice = nice


class SerializedJob:
    def __init__(self, *,
                 job_type: str,
                 job_id: str,
                 job_body_serialized: SerializedJobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body_serialized = job_body_serialized
        self.nice = nice

class JobStatus:
    def __init__(self, *,
                 status_id: str,
                 job_id: str,
                 issue_time: dt.datetime,
                 status: StatusEnum,
                 detail: str,
                 result: Result) -> None:
        self.status_id = status_id
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result = result


# class SerializedJobStatus:
#     def __init__(self, *,
#                  job_type: str,
#                  job_id: str,
#                  job_body_serialized: SerializedJobBody,
#                  nice: int = 0) -> None:
#         self.job_type = job_type
#         self.job_id = job_id
#         self.job_body_serialized = job_body_serialized
#         self.nice = nice


class NewJobRequest:
    def __init__(self, *,
                 job_type: str,
                 job_body: JobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_body = job_body
        self.nice = nice


class EmptyQueueError(IndexError):
    pass
