from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qqabc.application.domain.model.job import JobResult
    from qqabc.application.port.in_.upload_result_use_case import NewJobResultRequest


class IStatusService(ABC):
    @abstractmethod
    def add_job_result(self, request: NewJobResultRequest) -> JobResult:
        pass

    @abstractmethod
    def get_latest_result(
        self, job_id: str, *, deserialize: bool = True
    ) -> JobResult | None:
        pass

    @abstractmethod
    def list_job_results(self, job_id: str) -> list[JobResult]:
        pass
