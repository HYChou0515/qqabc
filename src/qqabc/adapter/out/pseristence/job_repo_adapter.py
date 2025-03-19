from __future__ import annotations

import itertools as it
import os
from abc import ABC, abstractmethod
from typing import ClassVar

import msgpack

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


class InMemoryJobRepo(JobRepoAdapter):
    _queue: ClassVar[dict[str, SerializedJob]] = {}  # Singleton
    _hist: ClassVar[dict[str, SerializedJob]] = {}  # Singleton
    _status_hist: ClassVar[dict[str, list[SerializedJobStatus]]] = {}  # Singleton

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
        return list(it.chain(self._queue.values(), self._hist.values()))

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


class FileJobRepo(JobRepoAdapter):
    def __init__(self, db_root: str) -> None:
        self._db_root = db_root
        self._job_dir = os.path.join(self._db_root, "job")
        self._hist_dir = os.path.join(self._db_root, "history")
        self._status_dir = os.path.join(self._db_root, "status")
        os.makedirs(self._job_dir, exist_ok=True)
        os.makedirs(self._hist_dir, exist_ok=True)
        os.makedirs(self._status_dir, exist_ok=True)

    def _history_path(self, job_id: str) -> str:
        return os.path.join(self._hist_dir, job_id)

    def _job_path(self, job_id: str) -> str:
        return os.path.join(self._job_dir, job_id)

    def _status_path(self, job_id: str) -> str:
        return os.path.join(self._status_dir, job_id)

    def job_exists(self, job_id: str) -> bool:
        return os.path.exists(self._job_path(job_id))

    def add_job(self, s_job: SerializedJob) -> None:
        with open(self._job_path(s_job.job_id), "wb") as f:
            f.write(msgpack.packb(s_job.get_serializable()))

    def _get_job_from_path(self, fpath: str) -> SerializedJob | None:
        if not os.path.exists(fpath):
            return None
        with open(fpath, "rb") as f:
            return SerializedJob.from_serializable(msgpack.unpack(f))

    def _get_job_from_queue(self, job_id: str) -> SerializedJob | None:
        return self._get_job_from_path(self._job_path(job_id))

    def _get_job_from_hist(self, job_id: str) -> SerializedJob | None:
        return self._get_job_from_path(self._history_path(job_id))

    def get_job(self, job_id: str) -> SerializedJob | None:
        if (job := self._get_job_from_queue(job_id)) is None:
            return self._get_job_from_hist(job_id)
        return job

    def list_jobs(self) -> list[SerializedJob]:
        return [
            job
            for f in os.listdir(self._job_dir)
            if (job := self.get_job(f)) is not None
        ]

    def add_status(self, s_status: SerializedJobStatus) -> None:
        sdir = self._status_path(s_status.job_id)
        os.makedirs(sdir, exist_ok=True)
        with open(os.path.join(sdir, s_status.status_id), "wb") as f:
            f.write(msgpack.packb(s_status.get_serializable()))

    def _list_status(self, job_id: str) -> list[SerializedJobStatus]:
        results = []
        for fpath in os.listdir(self._status_path(job_id)):
            with open(os.path.join(self._status_path(job_id), fpath), "rb") as f:
                results.append(SerializedJobStatus.from_serializable(msgpack.unpack(f)))
        return results

    def get_latest_status(self, job_id: str) -> SerializedJobStatus | None:
        sdir = self._status_path(job_id)
        if not os.path.exists(sdir):
            return None
        all_status = self._list_status(job_id)
        if not all_status:
            return None
        return max(all_status, key=lambda s: s.issue_time)

    def pop_largest_priority_job(self, job_type: str) -> SerializedJob | None:
        candidate: list[SerializedJob] = [
            job for job in self.list_jobs() if job.job_type == job_type
        ]
        if not candidate:
            return None
        sjob = min(candidate, key=lambda job: job.nice)
        os.rename(self._job_path(sjob.job_id), self._history_path(sjob.job_id))
        return sjob
