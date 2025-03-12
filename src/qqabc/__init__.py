from __future__ import annotations

import abc
import datetime as dt
import uuid
from typing import Literal

from typing_extensions import overload

from qqabc.types import (
    NO_RESULT,
    EmptyQueueError,
    Job,
    JobBody,
    JobStatus,
    NewJobRequest,
    NewJobStatusRequest,
    NewSerializedJobRequest,
    NewSerializedJobStatusRequest,
    Result,
    SerializedJob,
    SerializedJobBody,
    SerializedJobStatus,
    SerializedResult,
)


class JobSerializer(abc.ABC):
    @overload
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        raise NotImplementedError

    @overload
    def serialize(self, job_body: Result) -> SerializedResult:
        raise NotImplementedError

    @abc.abstractmethod
    def serialize(
        self, job_body: JobBody | Result
    ) -> SerializedJobBody | SerializedResult:
        raise NotImplementedError

    @overload
    def deserialize(self, serialized: SerializedJobBody) -> JobBody:
        raise NotImplementedError

    @overload
    def deserialize(self, serialized: SerializedResult) -> Result:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(
        self, serialized: SerializedJobBody | SerializedResult
    ) -> JobBody | Result:
        raise NotImplementedError

    def serialize_result(self, result: Result) -> SerializedResult:
        return SerializedResult(self.serialize(result))

    def deserialize_result(self, serialized_result: SerializedResult) -> Result:
        return Result(self.deserialize(serialized_result))


class JobSerializerRegistry:
    def __init__(self) -> None:
        self._job_serializers: dict[str, JobSerializer] = {}

    def register_job_serializer(
        self, job_serializer: JobSerializer, job_type: str
    ) -> None:
        self._job_serializers[job_type] = job_serializer

    def get_job_serializer(self, job_type: str) -> JobSerializer:
        if job_type not in self._job_serializers:
            raise KeyError(f"Job type {job_type} not found in registry")
        return self._job_serializers[job_type]


_queue: dict[str, SerializedJob] = {}  # Singleton
_hist: dict[str, SerializedJob] = {}  # Singleton
_job_status: dict[str, list[SerializedJobStatus]] = {}  # Singleton


class JobDao:
    def __init__(self) -> None:
        self._queue = _queue
        self._hist = _hist
        self._status_hist = _job_status

    def job_exists(self, job_id: str) -> bool:
        return job_id in self._queue

    def add_job(self, s_job: SerializedJob) -> None:
        self._queue[s_job.job_id] = s_job

    def _get_job_from_queue(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._queue:
            return None
        return self._queue[job_id]

    def _get_job_from_hist(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._hist:
            return None
        return self._hist[job_id]

    def get_job(self, job_id: str) -> SerializedJob | None:
        if (job := self._get_job_from_queue(job_id)) is None:
            return self._get_job_from_hist(job_id)
        return job

    def add_status(self, s_status: SerializedJobStatus) -> None:
        if s_status.job_id not in self._status_hist:
            self._status_hist[s_status.job_id] = []
        self._status_hist[s_status.job_id].append(s_status)

    def get_latest_status(self, job_id: str) -> SerializedJobStatus | None:
        if job_id not in self._status_hist:
            return None
        sh = self._status_hist[job_id]
        if len(sh) == 0:
            return None
        return self._status_hist[job_id][-1]

    def pop_largest_priority_job(self, job_type: str) -> SerializedJob | None:
        jobs_with_type = [
            job for job in self._queue.values() if job.job_type == job_type
        ]
        if not jobs_with_type:
            return None
        sjob = min(jobs_with_type, key=lambda job: job.nice)
        self._hist[sjob.job_id] = self._queue[sjob.job_id]
        del self._queue[sjob.job_id]
        return sjob


class JobQueueController:
    def __init__(self) -> None:
        self.job_dao = JobDao()
        self.job_serializer_registry = JobSerializerRegistry()

    def _check_job_exists(self, job_id: str) -> None:
        if not self.job_dao.job_exists(job_id):
            raise KeyError(job_id)

    def _get_serializer(self, job_type: str) -> JobSerializer:
        return self.job_serializer_registry.get_job_serializer(job_type)

    def _deserialize_job(self, sjob: SerializedJob) -> Job:
        serializer = self._get_serializer(sjob.job_type)
        job = Job(
            job_id=sjob.job_id,
            job_type=sjob.job_type,
            job_body=serializer.deserialize(sjob.job_body_serialized),
        )
        return job

    def _serialize_job(self, job: Job) -> SerializedJob:
        serializer = self._get_serializer(job.job_type)
        sjob = SerializedJob(
            job_id=job.job_id,
            job_type=job.job_type,
            job_body_serialized=serializer.serialize(job.job_body),
        )
        return sjob

    @overload
    def get_job(self, job_id: str) -> Job:
        raise NotImplementedError

    @overload
    def get_job(self, job_id: str, *, deserialize: Literal[True] = True) -> Job:
        raise NotImplementedError

    @overload
    def get_job(self, job_id: str, *, deserialize: Literal[False]) -> SerializedJob:
        raise NotImplementedError

    def get_job(self, job_id: str, *, deserialize: bool = True) -> Job | SerializedJob:
        sjob = self.job_dao.get_job(job_id)
        if sjob is None:
            raise KeyError(job_id)
        if deserialize:
            return self._deserialize_job(sjob)
        return sjob

    @overload
    def add_job(self, new_job_request: NewJobRequest) -> Job:
        raise NotImplementedError

    @overload
    def add_job(self, new_job_request: NewSerializedJobRequest) -> SerializedJob:
        raise NotImplementedError

    def add_job(
        self, new_job_request: NewJobRequest | NewSerializedJobRequest
    ) -> Job | SerializedJob:
        if isinstance(new_job_request, NewJobRequest):
            return self._add_job(new_job_request)
        return self._add_serialized_job(new_job_request)

    def _add_serialized_job(self, req: NewSerializedJobRequest) -> SerializedJob:
        sjob = SerializedJob(
            job_id=uuid.uuid4().hex,
            job_type=req.job_type,
            job_body_serialized=req.job_body_serialized,
        )
        self.job_dao.add_job(sjob)
        return sjob

    def _add_job(self, req: NewJobRequest) -> Job:
        job = Job(
            job_id=uuid.uuid4().hex,
            job_type=req.job_type,
            job_body=req.job_body,
        )
        sjob = self._serialize_job(job)
        self.job_dao.add_job(sjob)
        return job

    @overload
    def get_next_job(self, job_type: str) -> Job:
        raise NotImplementedError

    @overload
    def get_next_job(self, job_type: str, *, deserialize: Literal[True]) -> Job:
        raise NotImplementedError

    @overload
    def get_next_job(
        self, job_type: str, *, deserialize: Literal[False]
    ) -> SerializedJob:
        raise NotImplementedError

    def get_next_job(
        self, job_type: str, *, deserialize: bool = True
    ) -> Job | SerializedJob:
        if deserialize:
            return self._get_next_job(job_type)
        return self._get_next_sjob(job_type)

    def _get_next_job(self, job_type: str) -> Job:
        sjob = self._get_next_sjob(job_type)
        return self._deserialize_job(sjob)

    def _get_next_sjob(self, job_type: str) -> SerializedJob:
        sjob = self.job_dao.pop_largest_priority_job(job_type)
        if sjob is None:
            raise EmptyQueueError(f"No job with job type: {job_type}")
        return sjob

    def _get_job_type(self, job_id: str, job: Job | SerializedJob | None) -> str:
        if job is not None:
            return job.job_type
        return self.get_job(job_id, deserialize=False).job_type

    def _deserialize_status(
        self, s_status: SerializedJobStatus, *, job: Job | SerializedJob | None = None
    ) -> JobStatus:
        if s_status.result_serialized is None:
            result = NO_RESULT
        else:
            job_type = self._get_job_type(s_status.job_id, job)
            serializer = self._get_serializer(job_type)
            result = serializer.deserialize_result(s_status.result_serialized)
        return JobStatus(
            status_id=s_status.status_id,
            job_id=s_status.job_id,
            issue_time=s_status.issue_time,
            status=s_status.status,
            detail=s_status.detail,
            result=result,
        )

    def _serialize_status(
        self, status: JobStatus, *, job: Job | None = None
    ) -> SerializedJobStatus:
        if status.result is NO_RESULT:
            serialized_result = None
        else:
            job_type = self._get_job_type(status.job_id, job)
            serializer = self._get_serializer(job_type)
            serialized_result = serializer.serialize_result(status.result)
        return SerializedJobStatus(
            status_id=status.status_id,
            job_id=status.job_id,
            issue_time=status.issue_time,
            status=status.status,
            detail=status.detail,
            result_serialized=serialized_result,
        )

    def _add_serialized_job_status(
        self, request: NewSerializedJobStatusRequest
    ) -> SerializedJobStatus:
        self._check_job_exists(request.job_id)
        issue_time = request.issue_time or dt.datetime.now(tz=dt.timezone.utc)
        s_status = SerializedJobStatus(
            status_id=uuid.uuid4().hex,
            job_id=request.job_id,
            issue_time=issue_time,
            status=request.status,
            detail=request.detail,
            result_serialized=request.result_serialized,
        )
        self.job_dao.add_status(s_status)
        return s_status

    def _add_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        issue_time = request.issue_time or dt.datetime.now(tz=dt.timezone.utc)
        status = JobStatus(
            status_id=uuid.uuid4().hex,
            job_id=request.job_id,
            issue_time=issue_time,
            status=request.status,
            detail=request.detail,
            result=request.result,
        )
        s_status = self._serialize_status(status)
        self.job_dao.add_status(s_status)
        return status

    @overload
    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        raise NotImplementedError

    @overload
    def add_job_status(
        self, request: NewSerializedJobStatusRequest
    ) -> SerializedJobStatus:
        raise NotImplementedError

    def add_job_status(
        self, request: NewJobStatusRequest | NewSerializedJobStatusRequest
    ) -> JobStatus | SerializedJobStatus:
        if isinstance(request, NewJobStatusRequest):
            return self._add_job_status(request)
        return self._add_serialized_job_status(request)

    @overload
    def get_latest_status(self, job_id: str) -> JobStatus | None:
        raise NotImplementedError

    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[True]
    ) -> JobStatus | None:
        raise NotImplementedError

    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[False]
    ) -> SerializedJobStatus | None:
        raise NotImplementedError

    def get_latest_status(
        self, job_id: str, *, deserialize: bool = True
    ) -> JobStatus | SerializedJobStatus | None:
        job = self.get_job(job_id, deserialize=False)
        s_status = self.job_dao.get_latest_status(job_id)
        if s_status is None:
            return None
        if deserialize:
            return self._deserialize_status(s_status, job=job)
        return s_status
