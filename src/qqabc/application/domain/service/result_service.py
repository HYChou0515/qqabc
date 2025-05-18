from __future__ import annotations

import datetime as dt
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from qqabc.application.domain.model.job import JobResult
from qqabc.common.exceptions import JobNotFoundError

if TYPE_CHECKING:
    from qqabc.adapter.out.pseristence.job_repo_adapter import IJobRepo
    from qqabc.adapter.out.pseristence.job_status_dao import IJobStatusRepo
    from qqabc.application.domain.service.job_queue_service import IJobQueueService
    from qqabc.application.port.in_.upload_result_use_case import NewJobResultRequest


class IResultService(ABC):
    @abstractmethod
    def add_job_result(self, request: NewJobResultRequest) -> JobResult:
        pass

    @abstractmethod
    def get_latest_result(self, job_id: str, index: int = 1) -> JobResult | None:
        pass

    @abstractmethod
    def list_job_results(self, job_id: str) -> list[JobResult]:
        pass


class ResultService(IResultService):
    def __init__(
        self,
        job_svc: IJobQueueService,
        job_dao: IJobRepo,
        job_status_dao: IJobStatusRepo,
    ) -> None:
        self.job_dao = job_dao
        self.job_status_dao = job_status_dao
        self.job_svc = job_svc

    def add_job_result(self, request: NewJobResultRequest) -> JobResult:
        issue_time = request.issue_time or dt.datetime.now(tz=dt.timezone.utc)
        job = self.job_dao.get_job(request.job_id)
        if job is None:
            raise JobNotFoundError(request.job_id)
        result = JobResult(
            result_id=uuid.uuid4().hex,
            job_id=request.job_id,
            issue_time=issue_time,
            serialized_result=request.result,
        )
        self.job_status_dao.add_result(result)
        return result

    def get_latest_result(self, job_id: str, index: int = 1) -> JobResult | None:
        job = self.job_dao.get_job(job_id)
        if job is None:
            return None
        result = self.job_status_dao.get_kth_latest_result(job_id, k=index)
        return result

    def list_job_results(self, job_id: str) -> list[JobResult]:
        job = self.job_dao.get_job(job_id)
        if job is None:
            return []
        results = list(self.job_status_dao.iter_result(job_id))
        return results
