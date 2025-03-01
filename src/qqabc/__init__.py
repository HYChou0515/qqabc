from __future__ import annotations

import abc
import uuid

from qqabc.types import (
    EmptyQueueError,
    Job,
    JobBody,
    NewJobRequest,
    SerializedJob,
    SerializedJobBody,
)


class JobSerializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        pass

    @abc.abstractmethod
    def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
        pass

    # def serialize_result(self, result: Result) -> SerializedJobBody:
    #     return self.serialize(result)

    # def deserialize_result(self, serialized_result: SerializedResult) -> Result:
    #     return self.deserialize(serialized_result)

    # def serialize_job_body(self, result: Result) -> SerializedJobBody:
    #     return self.serialize(result)

    # def deserialize_job_body(self, serialized_result: SerializedResult) -> Result:
    #     return self.deserialize(serialized_result)


class JobSerializerRegistry:
    def __init__(self) -> None:
        self._job_serializers: dict[str, JobSerializer] = {}

    def register_job_serializer(
            self,
            job_serializer: JobSerializer, job_type: str) -> None:
        self._job_serializers[job_type] = job_serializer

    def get_job_serializer(self, job_type: str) -> JobSerializer:
        if job_type not in self._job_serializers:
            raise KeyError(f"Job type {job_type} not found in registry")
        return self._job_serializers[job_type]

    def reset(self) -> None:
        self._job_serializers = {}


_queue: dict[str, SerializedJob] = {}  # Singleton


class JobDao:
    def __init__(self) -> None:
        self._queue = _queue

    def add_job(self, sjob: SerializedJob) -> None:
        self._queue[sjob.job_id] = sjob

    def get_job(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._queue:
            return None
        return self._queue[job_id]

    def pop_largest_priority_job(self, job_type: str) -> SerializedJob | None:
        jobs_with_type = [
            job for job in self._queue.values() if job.job_type == job_type]
        if not jobs_with_type:
            return None
        sjob = min(jobs_with_type, key=lambda job: job.nice)
        del self._queue[sjob.job_id]
        return sjob

    def drop_all_jobs(self) -> None:
        self._queue.clear()


class JobQueueController:
    # def create_job_result(self, job_id: str, result: Result) -> None:
    #     serializer = self.job_serializer_registry.get_job_serializer(job_id)
    #     serialized_result = serializer.serialize_result(result)
    #     self.job_dao.add_result(job_id, serialized_result)
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
            new_job_request.job_type)
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
