from __future__ import annotations

import os
import shutil
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypedDict

from qqabc.application.domain.model.job import JobResult, JobStatus
from qqabc.common.serializer import serializer

if TYPE_CHECKING:
    from collections.abc import Generator


class JobStatusRepoDumps(TypedDict):
    status_history: dict[str, list[dict]]
    result: dict[str, list[dict]]


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
    def get_kth_latest_result(self, job_id: str, k: int) -> JobResult | None:
        raise NotImplementedError

    @abstractmethod
    def iter_result(self, job_id: str) -> Generator[JobResult]:
        raise NotImplementedError

    def dump(self) -> bytes:
        return serializer.packb(self.dump_dict())

    @abstractmethod
    def dump_dict(self) -> JobStatusRepoDumps:
        raise NotImplementedError

    def load(self, raw: bytes) -> None:
        return self.load_dict(serializer.unpackb(raw))

    @abstractmethod
    def load_dict(self, obj: JobStatusRepoDumps) -> None:
        raise NotImplementedError


def get_kth_latest_result(
    iter_result: Generator[JobResult], k: int
) -> JobResult | None:
    results = sorted(iter_result, key=lambda r: r.issue_time, reverse=k > 0)
    k = abs(k)
    if k > len(results):
        return None
    return results[k - 1] if k > 0 else None


class MemoryJobStatusRepo(IJobStatusRepo):
    def __init__(self) -> None:
        self._status_hist: dict[str, list[JobStatus]] = {}
        self._result: dict[str, list[JobResult]] = {}

    def add_result(self, result: JobResult) -> None:
        if result.job_id not in self._result:
            self._result[result.job_id] = []
        self._result[result.job_id].append(result)

    def get_kth_latest_result(self, job_id: str, k: int) -> JobResult | None:
        return get_kth_latest_result(self.iter_result(job_id), k=k)

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

    def dump_dict(self) -> JobStatusRepoDumps:
        return JobStatusRepoDumps(
            status_history=self._dump_status(),
            result=self._dump_result(),
        )

    def load_dict(self, dumps: JobStatusRepoDumps) -> None:
        self._status_hist.clear()
        for job_id, status_list in dumps["status_history"].items():
            self._status_hist[job_id] = [
                JobStatus.from_serializable(status) for status in status_list
            ]
        for job_id, result_list in dumps["result"].items():
            self._result[job_id] = [
                JobResult.from_serializable(result) for result in result_list
            ]

    def _dump_status(self) -> dict[str, list[dict]]:
        return {
            job_id: [
                s.get_serializable()
                for s in sorted(self._status_hist[job_id], key=lambda s: s.status_id)
            ]
            for job_id in sorted(self._status_hist.keys())
        }

    def _dump_result(self) -> dict[str, list[dict]]:
        return {
            job_id: [
                s.get_serializable()
                for s in sorted(self._result[job_id], key=lambda s: s.result_id)
            ]
            for job_id in sorted(self._result.keys())
        }


class DiskJobStatusRepo(IJobStatusRepo):
    def __init__(self, db_root: str) -> None:
        self._db_root = db_root
        self._status_dir = os.path.join(self._db_root, "status")
        self._result_dir = os.path.join(self._db_root, "result")
        os.makedirs(self._db_root, exist_ok=True)
        os.makedirs(self._status_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)

    def _job_result_path(self, job_id: str) -> str:
        return os.path.join(self._result_dir, job_id)

    def _job_status_path(self, job_id: str) -> str:
        return os.path.join(self._status_dir, job_id)

    def add_result(self, result: JobResult) -> None:
        dirname = self._job_result_path(result.job_id)
        if not os.path.isdir(dirname):
            os.makedirs(dirname, exist_ok=True)
        filename = os.path.join(dirname, f"{result.result_id}.json")
        with open(filename, "wb") as f:
            f.write(serializer.packb(result.get_serializable()))

    def get_kth_latest_result(self, job_id: str, k: int) -> JobResult | None:
        dirname = self._job_result_path(job_id)
        if not os.path.isdir(dirname):
            return None
        return get_kth_latest_result(self.iter_result(job_id), k=k)

    def iter_result(self, job_id: str) -> Generator[JobResult]:
        dirname = self._job_result_path(job_id)
        if not os.path.isdir(dirname):
            return
        for filename in os.listdir(dirname):
            with open(os.path.join(dirname, filename), "rb") as f:
                result = serializer.unpackb(f.read())
            yield JobResult.from_serializable(result)

    def add_status(self, s_status: JobStatus) -> None:
        dirname = self._job_status_path(s_status.job_id)
        if not os.path.isdir(dirname):
            os.makedirs(dirname, exist_ok=True)
        filename = os.path.join(dirname, f"{s_status.status_id}.json")
        with open(filename, "wb") as f:
            f.write(serializer.packb(s_status.get_serializable()))

    def get_latest_status(self, job_id: str) -> JobStatus | None:
        return max(self.iter_status(job_id), key=lambda s: s.issue_time, default=None)  # type: ignore[union-attr]

    def iter_status(self, job_id: str) -> Generator[JobStatus]:
        dirname = self._job_status_path(job_id)
        if not os.path.isdir(dirname):
            return
        for filename in os.listdir(dirname):
            with open(os.path.join(dirname, filename), "rb") as f:
                status = serializer.unpackb(f.read())
            yield JobStatus.from_serializable(status)

    def dump_dict(self) -> JobStatusRepoDumps:
        return JobStatusRepoDumps(
            status_history=self._dump_status(),
            result=self._dump_result(),
        )

    def load_dict(self, dumps: JobStatusRepoDumps) -> None:
        shutil.rmtree(self._status_dir, ignore_errors=True)
        shutil.rmtree(self._result_dir, ignore_errors=True)
        os.makedirs(self._status_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)
        for status_list in dumps["status_history"].values():
            for status in status_list:
                self.add_status(JobStatus.from_serializable(status))
        for result_list in dumps["result"].values():
            for result in result_list:
                self.add_result(JobResult.from_serializable(result))

    def _dump_status(self) -> dict[str, list[dict]]:
        dumped: dict[str, list[dict]] = {}
        for job_id in sorted(os.listdir(self._status_dir)):
            dumped[job_id] = [
                s.get_serializable()
                for s in sorted(self.iter_status(job_id), key=lambda s: s.status_id)
            ]
        return dumped

    def _dump_result(self) -> dict[str, list[dict]]:
        dumped: dict[str, list[dict]] = {}
        for job_id in sorted(os.listdir(self._result_dir)):
            dumped[job_id] = [
                s.get_serializable()
                for s in sorted(self.iter_result(job_id), key=lambda s: s.result_id)
            ]
        return dumped
