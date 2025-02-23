from __future__ import annotations

import abc
import uuid

from qqabc.types import Job, JobBody, NewJobRequest, SerializedJob, SerializedJobBody


class JobSerializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        pass

    @abc.abstractmethod
    def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
        pass


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


_queue: list[SerializedJob] = []  # Singleton


class JobDao:
    def __init__(self) -> None:
        self._queue = _queue
        self._id_index: dict[str, int] = {}

    def add_job(self, sjob: SerializedJob) -> None:
        self._queue.append(sjob)
        self._id_index[sjob.job_id] = len(self._queue) - 1

    def get_job(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._id_index:
            return None
        return self._queue[self._id_index[job_id]]


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
            new_job_request.job_type)
        sjob = SerializedJob(
            job_id=job.job_id,
            job_type=job.job_type,
            job_body_serialized=serializer.serialize(job.job_body),
        )
        self.job_dao.add_job(sjob)
        return job
