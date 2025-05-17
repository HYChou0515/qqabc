from __future__ import annotations

import datetime as dt
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from qqabc.application.domain.model.job import (
    Job,
    SerializedJob,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewJobRequest,
    NewSerializedJobRequest,
)
from qqabc.common.exceptions import EmptyQueueError, JobNotFoundError

if TYPE_CHECKING:
    from qqabc.adapter.out.pseristence.job_repo_adapter import (
        IJobRepo,
    )
    from qqabc.application.domain.service.job_serializer_registry import (
        JobSerializer,
        JobSerializerRegistry,
    )


class IJobQueueService(ABC):
    @abstractmethod
    def get_job(self, job_id: str, *, deserialize: bool = False) -> Job | SerializedJob:
        pass

    @abstractmethod
    def list_jobs(
        self, *, deserialize: bool = False
    ) -> list[SerializedJob] | list[Job]:
        pass

    @abstractmethod
    def add_job(
        self, new_job_request: NewJobRequest | NewSerializedJobRequest
    ) -> Job | SerializedJob:
        pass

    @abstractmethod
    def get_next_job(
        self, job_type: str | None, *, deserialize: bool = False
    ) -> Job | SerializedJob:
        pass

    @abstractmethod
    def check_job_exists(self, job_id: str) -> None:
        pass


class JobQueueService(IJobQueueService):
    def __init__(
        self, job_dao: IJobRepo, job_serializer_registry: JobSerializerRegistry
    ) -> None:
        self.job_dao = job_dao
        self.job_serializer_registry = job_serializer_registry

    def check_job_exists(self, job_id: str) -> None:
        if not self.job_dao.job_exists(job_id):
            raise JobNotFoundError(job_id)

    def _get_serializer(self, job_type: str) -> JobSerializer:
        return self.job_serializer_registry.get_job_serializer(job_type)

    def _deserialize_job(self, sjob: SerializedJob) -> Job:
        serializer = self._get_serializer(sjob.job_type)
        job = Job(
            job_id=sjob.job_id,
            job_type=sjob.job_type,
            job_body=serializer.deserialize(sjob.job_body_serialized),
            created_time=sjob.created_time,
            nice=sjob.nice,
        )
        return job

    def _serialize_job(self, job: Job) -> SerializedJob:
        serializer = self._get_serializer(job.job_type)
        sjob = SerializedJob(
            job_id=job.job_id,
            job_type=job.job_type,
            job_body_serialized=serializer.serialize(job.job_body),
            created_time=job.created_time,
            nice=job.nice,
        )
        return sjob

    def get_job(self, job_id: str, *, deserialize: bool = False) -> Job | SerializedJob:
        sjob = self.job_dao.get_job(job_id)
        if sjob is None:
            raise JobNotFoundError(job_id)
        if deserialize:
            return self._deserialize_job(sjob)
        return sjob

    def list_jobs(
        self, *, deserialize: bool = False
    ) -> list[SerializedJob] | list[Job]:
        jobs = self.job_dao.list_jobs()
        if deserialize:
            return [self._deserialize_job(job) for job in jobs]
        return jobs

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
            created_time=dt.datetime.now(tz=dt.timezone.utc),
            nice=0,
        )
        self.job_dao.add_job(sjob)
        return sjob

    def _add_job(self, req: NewJobRequest) -> Job:
        job = Job(
            job_id=uuid.uuid4().hex,
            job_type=req.job_type,
            job_body=req.job_body,
            created_time=dt.datetime.now(tz=dt.timezone.utc),
            nice=0,
        )
        sjob = self._serialize_job(job)
        self.job_dao.add_job(sjob)
        return job

    def get_next_job(
        self, job_type: str | None, *, deserialize: bool = False
    ) -> Job | SerializedJob:
        if deserialize:
            return self._get_next_job(job_type)
        return self._get_next_sjob(job_type)

    def _get_next_job(self, job_type: str | None) -> Job:
        sjob = self._get_next_sjob(job_type)
        return self._deserialize_job(sjob)

    def _get_next_sjob(self, job_type: str | None) -> SerializedJob:
        sjob = self.job_dao.pop_largest_priority_job(job_type)
        if sjob is None:
            if job_type is None:
                raise EmptyQueueError("No jobs in queue")
            raise EmptyQueueError(f"No job with job type: {job_type}")
        return sjob
