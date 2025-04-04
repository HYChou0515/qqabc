from __future__ import annotations

import datetime as dt
import itertools as it
import os
import shutil
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, TypedDict

from qqabc.application.domain.model.job import SerializedJob, SerializedJobStatus
from qqabc.common.serializer import serializer

if TYPE_CHECKING:
    from collections.abc import Generator


class JobRepoAdapterDumps(TypedDict):
    queue: list[dict]
    history: list[dict]
    status_history: dict[str, list[dict]]


def _get_filo_priority(job: SerializedJob) -> tuple[int, dt.timedelta]:
    return (
        job.nice,
        dt.datetime(1900, 1, 1, tzinfo=dt.timezone.utc) - job.created_time,
    )


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
    def iter_status(self, job_id: str) -> Generator[SerializedJobStatus]:
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


class InMemoryJobRepo(JobRepoAdapter):
    _queue: ClassVar[dict[str, SerializedJob]] = {}  # Singleton
    _hist: ClassVar[dict[str, SerializedJob]] = {}  # Singleton
    _status_hist: ClassVar[dict[str, list[SerializedJobStatus]]] = {}  # Singleton

    def job_exists(self, job_id: str) -> bool:
        return job_id in self._queue

    def add_job(self, s_job: SerializedJob) -> None:
        self._queue[s_job.job_id] = s_job

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
        return max(self.iter_status(job_id), key=lambda s: s.issue_time, default=None)  # type: ignore[union-attr]

    def iter_status(self, job_id: str) -> Generator[SerializedJobStatus]:
        if job_id not in self._status_hist:
            return
        yield from self._status_hist[job_id]

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
            status_history=self._dump_status(),
        )

    def load_dict(self, dumps: JobRepoAdapterDumps) -> None:
        self._queue.clear()
        self._hist.clear()
        self._status_hist.clear()

        for job in dumps["queue"]:
            self._queue[job["job_id"]] = SerializedJob.from_serializable(job)
        for job in dumps["history"]:
            self._hist[job["job_id"]] = SerializedJob.from_serializable(job)
        for job_id, status_list in dumps["status_history"].items():
            self._status_hist[job_id] = [
                SerializedJobStatus.from_serializable(status) for status in status_list
            ]

    def teardown(self) -> None:
        self._queue.clear()
        self._hist.clear()
        self._status_hist.clear()

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

    def _dump_status(self) -> dict[str, list[dict]]:
        return {
            job_id: [
                s.get_serializable()
                for s in sorted(self._status_hist[job_id], key=lambda s: s.status_id)
            ]
            for job_id in sorted(self._status_hist.keys())
        }


class FileJobRepo(JobRepoAdapter):
    def __init__(self, db_root: str) -> None:
        self._db_root = db_root
        self._job_dir = os.path.join(self._db_root, "job")
        self._hist_dir = os.path.join(self._db_root, "history")
        self._status_dir = os.path.join(self._db_root, "status")
        os.makedirs(self._db_root, exist_ok=True)
        os.makedirs(self._job_dir, exist_ok=True)
        os.makedirs(self._hist_dir, exist_ok=True)
        os.makedirs(self._status_dir, exist_ok=True)

    def job_exists(self, job_id: str) -> bool:
        return os.path.exists(self._job_path(job_id))

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

    def add_status(self, s_status: SerializedJobStatus) -> None:
        sdir = self._status_path(s_status.job_id)
        os.makedirs(sdir, exist_ok=True)
        with open(os.path.join(sdir, s_status.status_id), "wb") as f:
            f.write(serializer.packb(s_status.get_serializable()))

    def get_latest_status(self, job_id: str) -> SerializedJobStatus | None:
        return max(self.iter_status(job_id), key=lambda s: s.issue_time, default=None)  # type: ignore[union-attr]

    def iter_status(self, job_id: str) -> Generator[SerializedJobStatus]:
        for status_id in self._list_status_ids_of_job(job_id):
            yield self._get_status_from_job_and_status_id(job_id, status_id)

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
        status_history = self._dump_status()
        return JobRepoAdapterDumps(
            queue=queue,
            history=history,
            status_history=status_history,
        )

    def load_dict(self, dumps: JobRepoAdapterDumps) -> None:
        for job in dumps["queue"]:
            self.add_job(SerializedJob.from_serializable(job))
        for job in dumps["history"]:
            self.add_job(SerializedJob.from_serializable(job))
            self._move_job_to_history(job["job_id"])
        for status_list in dumps["status_history"].values():
            for status in status_list:
                self.add_status(SerializedJobStatus.from_serializable(status))

    def teardown(self) -> None:
        shutil.rmtree(self._db_root)
        os.makedirs(self._db_root, exist_ok=True)
        os.makedirs(self._job_dir, exist_ok=True)
        os.makedirs(self._hist_dir, exist_ok=True)
        os.makedirs(self._status_dir, exist_ok=True)

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

    def _dump_status(self) -> dict[str, list[dict]]:
        return {
            job_id: [
                self._get_status_from_job_and_status_id(job_id, sid).get_serializable()
                for sid in sorted(self._list_status_ids_of_job(job_id))
            ]
            for job_id in sorted(self._list_status_job_ids())
        }

    def _history_path(self, job_id: str) -> str:
        return os.path.join(self._hist_dir, job_id)

    def _job_path(self, job_id: str) -> str:
        return os.path.join(self._job_dir, job_id)

    def _status_path(self, job_id: str) -> str:
        return os.path.join(self._status_dir, job_id)

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

    def _list_status_job_ids(self) -> list[str]:
        return os.listdir(self._status_dir)

    def _list_status_ids_of_job(self, job_id: str) -> list[str]:
        p = self._status_path(job_id)
        if not os.path.exists(p):
            return []
        return os.listdir(p)

    def _get_status_from_job_and_status_id(
        self, job_id: str, status_id: str
    ) -> SerializedJobStatus:
        sdir = self._status_path(job_id)
        fpath = os.path.join(sdir, status_id)
        with open(fpath, "rb") as f:
            return SerializedJobStatus.from_serializable(serializer.unpackb(f.read()))

    def _move_job_to_history(self, job_id: str) -> None:
        os.rename(self._job_path(job_id), self._history_path(job_id))
