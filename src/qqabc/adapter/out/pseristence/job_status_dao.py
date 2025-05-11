from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

    from qqabc.application.domain.model.job import JobResult, JobStatus


class IJobStatusRepo(ABC):
    @abstractmethod
    def add_status(self, s_status: JobStatus) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_latest_status(self, job_id: str) -> JobStatus | None:
        raise NotImplementedError

    @abstractmethod
    def iter_status(self, job_id: str) -> Generator[JobStatus]:
        raise NotImplementedError

    @abstractmethod
    def add_result(self, result: JobResult) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_latest_result(self, job_id: str) -> JobResult | None:
        raise NotImplementedError

    @abstractmethod
    def iter_result(self, job_id: str) -> Generator[JobResult]:
        raise NotImplementedError


class MemoryJobStatusRepo(IJobStatusRepo):
    def __init__(self) -> None:
        self._status_hist: dict[str, list[JobStatus]] = {}
        self._result: dict[str, list[JobResult]] = {}

    def add_result(self, result: JobResult) -> None:
        if result.job_id not in self._result:
            self._result[result.job_id] = []
        self._result[result.job_id].append(result)

    def get_latest_result(self, job_id: str) -> JobResult | None:
        return max(self.iter_result(job_id), key=lambda r: r.issue_time, default=None)  # type: ignore[union-attr]

    def iter_result(self, job_id: str) -> Generator[JobResult]:
        if job_id not in self._result:
            return
        yield from self._result[job_id]

    def add_status(self, s_status: JobStatus) -> None:
        if s_status.job_id not in self._status_hist:
            self._status_hist[s_status.job_id] = []
        self._status_hist[s_status.job_id].append(s_status)

    def get_latest_status(self, job_id: str) -> JobStatus | None:
        return max(self.iter_status(job_id), key=lambda s: s.issue_time, default=None)  # type: ignore[union-attr]

    def iter_status(self, job_id: str) -> Generator[JobStatus]:
        if job_id not in self._status_hist:
            return
        yield from self._status_hist[job_id]
