from __future__ import annotations

import datetime as dt
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

from qqabc.application.domain.model.job import (
    NO_RESULT,
    QQABC,
    Job,
    JobStatus,
    SerializedJob,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)

if TYPE_CHECKING:
    from qqabc.adapter.out.pseristence.job_repo_adapter import (
        JobRepoAdapter,
    )
    from qqabc.application.domain.service.job_queue_service import IJobQueueService
    from qqabc.application.domain.service.job_serializer_registry import (
        JobSerializer,
        JobSerializerRegistry,
    )


class IStatusService(ABC):
    @abstractmethod
    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        pass

    @abstractmethod
    def get_latest_status(
        self, job_id: str, *, deserialize: bool = True
    ) -> JobStatus | None:
        pass

    @abstractmethod
    def list_job_status(self, job_id: str) -> list[JobStatus]:
        pass


class StatusService(IStatusService):
    def __init__(
        self,
        job_svc: IJobQueueService,
        job_dao: JobRepoAdapter,
        job_serializer_registry: JobSerializerRegistry,
    ) -> None:
        self.job_dao = job_dao
        self.job_serializer_registry = job_serializer_registry
        self.job_svc = job_svc

    def _get_serializer(self, job_type: str) -> JobSerializer:
        return self.job_serializer_registry.get_job_serializer(job_type)

    def _deserialize_status(
        self, s_status: JobStatus, *, job: Job | SerializedJob | None = None
    ) -> JobStatus:
        result: Any | Literal[QQABC.NO_RESULT]
        if s_status.result_serialized is None:
            result = NO_RESULT
        else:
            job_type = self.job_svc.get_job_type(s_status.job_id, job)
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
    ) -> JobStatus:
        if status.result is NO_RESULT:
            serialized_result = None
        else:
            job_type = self.job_svc.get_job_type(status.job_id, job)
            serializer = self._get_serializer(job_type)
            serialized_result = serializer.serialize_result(status.result)
        return JobStatus(
            status_id=status.status_id,
            job_id=status.job_id,
            issue_time=status.issue_time,
            status=status.status,
            detail=status.detail,
            result_serialized=serialized_result,
        )

    def _add_serialized_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        self.job_svc.check_job_exists(request.job_id)
        issue_time = request.issue_time or dt.datetime.now(tz=dt.timezone.utc)
        s_status = JobStatus(
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
        )
        s_status = self._serialize_status(status)
        self.job_dao.add_status(s_status)
        return status

    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus:
        if isinstance(request, NewJobStatusRequest):
            return self._add_job_status(request)
        return self._add_serialized_job_status(request)

    def get_latest_status(
        self, job_id: str, *, deserialize: bool = True
    ) -> JobStatus | None:
        job = self.job_svc.get_job(job_id, deserialize=False)
        s_status = self.job_dao.get_latest_status(job_id)
        if s_status is None:
            return None
        if deserialize:
            return self._deserialize_status(s_status, job=job)
        return s_status

    def list_job_status(self, job_id: str) -> list[JobStatus]:
        s_status_list = list(self.job_dao.iter_status(job_id))
        return s_status_list
