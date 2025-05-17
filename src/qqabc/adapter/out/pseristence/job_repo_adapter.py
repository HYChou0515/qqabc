from __future__ import annotations

import datetime as dt
import itertools as it
import os
from abc import ABC, abstractmethod
from typing import TypedDict

from qqabc.application.domain.model.job import SerializedJob
from qqabc.common.serializer import serializer


class JobRepoAdapterDumps(TypedDict):
    queue: list[dict]
    history: list[dict]


def _get_filo_priority(job: SerializedJob) -> tuple[int, dt.timedelta]:
    return (
        job.nice,
        dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc) - job.created_time,
    )


class IJobRepo(ABC):
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
    def pop_largest_priority_job(self, job_type: str | None) -> SerializedJob | None:
        raise NotImplementedError

    def dump(self) -> bytes:
        return serializer.packb(self.dump_dict())

    @abstractmethod
    def dump_dict(self) -> JobRepoAdapterDumps:
        raise NotImplementedError

    def load(self, raw: bytes) -> None:
        return self.load_dict(serializer.unpackb(raw))

    @abstractmethod
    def load_dict(self, obj: JobRepoAdapterDumps) -> None:
        raise NotImplementedError


class MemoryJobRepo(IJobRepo):
    def __init__(self) -> None:
        self._queue: dict[str, SerializedJob] = {}
        self._hist: dict[str, SerializedJob] = {}

    def job_exists(self, job_id: str) -> bool:
        return job_id in self._queue or job_id in self._hist

    def add_job(self, s_job: SerializedJob) -> None:
        self._queue[s_job.job_id] = s_job

    def get_job(self, job_id: str) -> SerializedJob | None:
        if (job := self._get_job_from_queue(job_id)) is None:
            return self._get_job_from_hist(job_id)
        return job

    def list_jobs(self) -> list[SerializedJob]:
        return list(it.chain(self._queue.values(), self._hist.values()))

    def pop_largest_priority_job(self, job_type: str | None) -> SerializedJob | None:
        jobs_with_type = [
            job
            for job in self._queue.values()
            if job_type is None or job.job_type == job_type
        ]
        if not jobs_with_type:
            return None
        sjob = min(jobs_with_type, key=lambda job: job.nice)
        self._hist[sjob.job_id] = self._queue[sjob.job_id]
        del self._queue[sjob.job_id]
        return sjob

    def dump_dict(self) -> JobRepoAdapterDumps:
        return JobRepoAdapterDumps(
            queue=self._dump_queue(),
            history=self._dump_history(),
        )

    def load_dict(self, dumps: JobRepoAdapterDumps) -> None:
        self._queue.clear()
        self._hist.clear()

        for job in dumps["queue"]:
            self._queue[job["job_id"]] = SerializedJob.from_serializable(job)
        for job in dumps["history"]:
            self._hist[job["job_id"]] = SerializedJob.from_serializable(job)

    def _get_job_from_queue(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._queue:
            return None
        return self._queue[job_id]

    def _get_job_from_hist(self, job_id: str) -> SerializedJob | None:
        if job_id not in self._hist:
            return None
        return self._hist[job_id]

    def _dump_queue(self) -> list[dict]:
        return [
            self._queue[jid].get_serializable() for jid in sorted(self._queue.keys())
        ]

    def _dump_history(self) -> list[dict]:
        return [self._hist[jid].get_serializable() for jid in sorted(self._hist.keys())]


class DiskJobRepo(IJobRepo):
    def __init__(self, db_root: str) -> None:
        self._db_root = db_root
        self._job_dir = os.path.join(self._db_root, "job")
        self._hist_dir = os.path.join(self._db_root, "history")
        os.makedirs(self._db_root, exist_ok=True)
        os.makedirs(self._job_dir, exist_ok=True)
        os.makedirs(self._hist_dir, exist_ok=True)

    def job_exists(self, job_id: str) -> bool:
        return os.path.exists(self._job_path(job_id)) or os.path.exists(
            self._history_path(job_id)
        )

    def add_job(self, s_job: SerializedJob) -> None:
        with open(self._job_path(s_job.job_id), "wb") as f:
            f.write(serializer.packb(s_job.get_serializable()))

    def get_job(self, job_id: str) -> SerializedJob | None:
        if (job := self._get_job_from_queue(job_id)) is None:
            return self._get_job_from_hist(job_id)
        return job

    def list_jobs(self) -> list[SerializedJob]:
        return [
            job
            for job_id in self._list_queue_job_ids()
            if (job := self._get_job_from_queue(job_id)) is not None
        ]

    def pop_largest_priority_job(self, job_type: str | None) -> SerializedJob | None:
        candidate: list[SerializedJob] = [
            job
            for job in self.list_jobs()
            if job_type is None or job.job_type == job_type
        ]
        if not candidate:
            return None
        sjob = min(candidate, key=_get_filo_priority)
        self._move_job_to_history(sjob.job_id)
        return sjob

    def dump_dict(self) -> JobRepoAdapterDumps:
        queue = self._dump_queue()
        history = self._dump_history()
        return JobRepoAdapterDumps(
            queue=queue,
            history=history,
        )

    def load_dict(self, dumps: JobRepoAdapterDumps) -> None:
        for job in dumps["queue"]:
            self.add_job(SerializedJob.from_serializable(job))
        for job in dumps["history"]:
            self.add_job(SerializedJob.from_serializable(job))
            self._move_job_to_history(job["job_id"])

    def _dump_queue(self) -> list[dict]:
        return [
            job.get_serializable()
            for job_id in sorted(self._list_queue_job_ids())
            if (job := self._get_job_from_queue(job_id)) is not None
        ]

    def _dump_history(self) -> list[dict]:
        return [
            job.get_serializable()
            for job_id in sorted(self._list_historty_job_ids())
            if (job := self._get_job_from_hist(job_id)) is not None
        ]

    def _history_path(self, job_id: str) -> str:
        return os.path.join(self._hist_dir, job_id)

    def _job_path(self, job_id: str) -> str:
        return os.path.join(self._job_dir, job_id)

    def _get_job_from_path(self, fpath: str) -> SerializedJob | None:
        if not os.path.exists(fpath):
            return None
        with open(fpath, "rb") as f:
            return SerializedJob.from_serializable(serializer.unpackb(f.read()))

    def _get_job_from_queue(self, job_id: str) -> SerializedJob | None:
        return self._get_job_from_path(self._job_path(job_id))

    def _get_job_from_hist(self, job_id: str) -> SerializedJob | None:
        return self._get_job_from_path(self._history_path(job_id))

    def _list_queue_job_ids(self) -> list[str]:
        return os.listdir(self._job_dir)

    def _list_historty_job_ids(self) -> list[str]:
        return os.listdir(self._hist_dir)

    def _move_job_to_history(self, job_id: str) -> None:
        os.rename(self._job_path(job_id), self._history_path(job_id))
