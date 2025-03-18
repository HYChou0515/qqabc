from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qqabc.application.domain.model.job import SerializedJob, SerializedJobStatus


class JobRepoAdapter(ABC):
    @abstractmethod
    def job_exists(self, job_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_job(self, s_job: SerializedJob) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_job(self, job_id: str) -> SerializedJob | None:
        raise NotImplementedError

    @abstractmethod
    def list_jobs(self) -> list[SerializedJob]:
        raise NotImplementedError

    @abstractmethod
    def add_status(self, s_status: SerializedJobStatus) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_latest_status(self, job_id: str) -> SerializedJobStatus | None:
        raise NotImplementedError

    @abstractmethod
    def pop_largest_priority_job(self, job_type: str) -> SerializedJob | None:
        raise NotImplementedError


_queue: dict[str, SerializedJob] = {}  # Singleton
_hist: dict[str, SerializedJob] = {}  # Singleton
_job_status: dict[str, list[SerializedJobStatus]] = {}  # Singleton


class InMemoryJobRepo(JobRepoAdapter):
    def __init__(self) -> None:
        self._queue = _queue
        self._hist = _hist
        self._status_hist = _job_status

    def teardown(self) -> None:
        self._queue.clear()
        self._hist.clear()
        self._status_hist.clear()

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
    
    def list_jobs(self) -> list[SerializedJob]:
        return list(self._queue.values()) + list(self._hist)

    def add_status(self, s_status: SerializedJobStatus) -> None:
        if s_status.job_id not in self._status_hist:
            self._status_hist[s_status.job_id] = []
        self._status_hist[s_status.job_id].append(s_status)

    def get_latest_status(self, job_id: str) -> SerializedJobStatus | None:
        if job_id not in self._status_hist:
            return None
        return max(self._status_hist[job_id], key=lambda s: s.issue_time)

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
