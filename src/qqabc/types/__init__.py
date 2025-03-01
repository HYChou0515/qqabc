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

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class NoResult(metaclass=Singleton):
    pass

NO_RESULT = NoResult()

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
                 result: Result|NoResult) -> None:
        self.status_id = status_id
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result = result

class SerializedJobStatus:
    def __init__(self, *,
                 status_id: str,
                 job_id: str,
                 issue_time: dt.datetime,
                 status: StatusEnum,
                 detail: str,
                 result_serialized: SerializedResult|None) -> None:
        self.status_id = status_id
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result_serialized = result_serialized

class NewJobStatusRequest:
    def __init__(self, *,
                 job_id: str,
                 status: StatusEnum,
                 issue_time: dt.datetime|None=None,
                 detail: str,
                 result: Result|NoResult=NO_RESULT) -> None:
        self.job_id = job_id
        self.issue_time = issue_time
        self.status = status
        self.detail = detail
        self.result = result

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
