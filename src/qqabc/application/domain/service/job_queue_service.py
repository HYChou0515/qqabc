from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import overload

from qqabc.application.domain.model.job import (
    NO_RESULT,
    QQABC,
    Job,
    JobStatus,
    SerializedJob,
    SerializedJobStatus,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewJobRequest,
    NewSerializedJobRequest,
)
from qqabc.common.exceptions import EmptyQueueError, JobNotFoundError

if TYPE_CHECKING:
    from qqabc.adapter.out.pseristence.job_repo_adapter import (
        JobRepoAdapter,
    )
    from qqabc.application.domain.service.job_serializer_registry import (
        JobSerializer,
        JobSerializerRegistry,
    )
    from qqabc.application.port.in_.post_job_status_use_case import (
        NewSerializedJobStatusRequest,
    )


class JobQueueService:
    def __init__(
        self, job_dao: JobRepoAdapter, job_serializer_registry: JobSerializerRegistry
    ) -> None:
        self.job_dao = job_dao
        self.job_serializer_registry = job_serializer_registry

    def _check_job_exists(self, job_id: str) -> None:
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

    @overload
    def get_job(self, job_id: str) -> SerializedJob:
        pass

    @overload
    def get_job(self, job_id: str, *, deserialize: Literal[True]) -> Job:
        pass

    @overload
    def get_job(
        self, job_id: str, *, deserialize: Literal[False] = False
    ) -> SerializedJob:
        pass

    def get_job(self, job_id: str, *, deserialize: bool = False) -> Job | SerializedJob:
        sjob = self.job_dao.get_job(job_id)
        if sjob is None:
            raise JobNotFoundError(job_id)
        if deserialize:
            return self._deserialize_job(sjob)
        return sjob

    @overload
    def list_jobs(self) -> list[SerializedJob]:
        pass

    @overload
    def list_jobs(self, *, deserialize: Literal[True]) -> list[Job]:
        pass

    @overload
    def list_jobs(self, *, deserialize: Literal[False] = False) -> list[SerializedJob]:
        pass

    def list_jobs(
        self, *, deserialize: bool = False
    ) -> list[SerializedJob] | list[Job]:
        jobs = self.job_dao.list_jobs()
        if deserialize:
            return [self._deserialize_job(job) for job in jobs]
        return jobs

    @overload
    def add_job(self, new_job_request: NewJobRequest) -> Job:
        pass

    @overload
    def add_job(self, new_job_request: NewSerializedJobRequest) -> SerializedJob:
        pass

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

    @overload
    def get_next_job(self, job_type: str | None) -> SerializedJob:
        pass

    @overload
    def get_next_job(self, job_type: str | None, *, deserialize: Literal[True]) -> Job:
        pass

    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: Literal[False]
    ) -> SerializedJob:
        pass

    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: bool
    ) -> Job | SerializedJob:
        pass

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

    def _get_job_type(self, job_id: str, job: Job | SerializedJob | None) -> str:
        if job is not None:
            return job.job_type
        return self.get_job(job_id, deserialize=False).job_type

    def _deserialize_status(
        self, s_status: SerializedJobStatus, *, job: Job | SerializedJob | None = None
    ) -> JobStatus:
        result: Any | Literal[QQABC.NO_RESULT]
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
        pass

    @overload
    def add_job_status(
        self, request: NewSerializedJobStatusRequest
    ) -> SerializedJobStatus:
        pass

    def add_job_status(
        self, request: NewJobStatusRequest | NewSerializedJobStatusRequest
    ) -> JobStatus | SerializedJobStatus:
        if isinstance(request, NewJobStatusRequest):
            return self._add_job_status(request)
        return self._add_serialized_job_status(request)

    @overload
    def get_latest_status(self, job_id: str) -> JobStatus | None:
        pass

    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[True]
    ) -> JobStatus | None:
        pass

    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[False]
    ) -> SerializedJobStatus | None:
        pass

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
