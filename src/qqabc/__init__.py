from __future__ import annotations

import abc
import datetime as dt
import uuid

from typing_extensions import overload

from qqabc.types import (
    NO_RESULT,
    EmptyQueueError,
    Job,
    JobBody,
    JobStatus,
    NewJobRequest,
    NewJobStatusRequest,
    Result,
    SerializedJob,
    SerializedJobBody,
    SerializedJobStatus,
    SerializedResult,
)


class JobSerializer(abc.ABC):
    @overload
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        pass

    @overload
    def serialize(self, job_body: Result) -> SerializedResult:
        pass

    @abc.abstractmethod
    def serialize(
        self, job_body: JobBody | Result
    ) -> SerializedJobBody | SerializedResult:
        pass

    @overload
    def deserialize(self, serialized: SerializedJobBody) -> JobBody:
        pass

    @overload
    def deserialize(self, serialized: SerializedResult) -> Result:
        pass

    @abc.abstractmethod
    def deserialize(
        self, serialized: SerializedJobBody | SerializedResult
    ) -> JobBody | Result:
        pass

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

    def reset(self) -> None:
        self._job_serializers = {}


_queue: dict[str, SerializedJob] = {}  # Singleton
_hist: dict[str, SerializedJob] = {}  # Singleton
_job_status: dict[str, list[SerializedJobStatus]] = {}  # Singleton


class JobDao:
    def __init__(self) -> None:
        self._queue = _queue
        self._hist = _hist
        self._status_hist = _job_status

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
        return self._status_hist[job_id][0]

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

    def drop_all_jobs(self) -> None:
        self._queue.clear()


class JobQueueController:
    def __init__(self) -> None:
        self.job_dao = JobDao()
        self.job_serializer_registry = JobSerializerRegistry()

    def get_job(self, job_id: str) -> Job:
        sjob = self.job_dao.get_job(job_id)
        if sjob is None:
            raise KeyError(job_id)
        serializer = self.job_serializer_registry.get_job_serializer(sjob.job_type)
        job = Job(
            job_id=sjob.job_id,
            job_type=sjob.job_type,
            job_body=serializer.deserialize(sjob.job_body_serialized),
        )
        return job

    def add_job(self, new_job_request: NewJobRequest) -> Job:
        job = Job(
            job_id=uuid.uuid4().hex,
            job_type=new_job_request.job_type,
            job_body=new_job_request.job_body,
        )
        serializer = self.job_serializer_registry.get_job_serializer(
            new_job_request.job_type
        )
        sjob = SerializedJob(
            job_id=job.job_id,
            job_type=job.job_type,
            job_body_serialized=serializer.serialize(job.job_body),
        )
        self.job_dao.add_job(sjob)
        return job

    def get_next_job(self, job_type: str) -> Job:
        sjob = self.job_dao.pop_largest_priority_job(job_type)
        if sjob is None:
            raise EmptyQueueError(f"No job with job type: {job_type}")
        serializer = self.job_serializer_registry.get_job_serializer(sjob.job_type)
        job = Job(
            job_id=sjob.job_id,
            job_type=sjob.job_type,
            job_body=serializer.deserialize(sjob.job_body_serialized),
        )
        return job

    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        issue_time = request.issue_time or dt.datetime.now(tz=dt.timezone.utc)
        status = JobStatus(
            status_id=uuid.uuid4().hex,
            job_id=request.job_id,
            issue_time=issue_time,
            status=request.status,
            detail=request.detail,
            result=request.result,
        )

        job = self.get_job(request.job_id)
        serializer = self.job_serializer_registry.get_job_serializer(job.job_type)
        if request.result is not NO_RESULT:
            serialized_result = serializer.serialize_result(request.result)
        else:
            serialized_result = None
        s_status = SerializedJobStatus(
            status_id=status.status_id,
            job_id=status.job_id,
            issue_time=status.issue_time,
            status=status.status,
            detail=status.detail,
            result_serialized=serialized_result,
        )
        self.job_dao.add_status(s_status)
        return status

    def get_latest_status(self, job_id: str) -> JobStatus | None:
        job = self.get_job(job_id)
        s_status = self.job_dao.get_latest_status(job_id)
        if s_status is None:
            return s_status
        serializer = self.job_serializer_registry.get_job_serializer(job.job_type)
        if s_status.result_serialized is None:
            result = NO_RESULT
        else:
            result = serializer.deserialize_result(s_status.result_serialized)
        return JobStatus(
            status_id=s_status.status_id,
            job_id=s_status.job_id,
            issue_time=s_status.issue_time,
            status=s_status.status,
            detail=s_status.detail,
            result=result,
        )
