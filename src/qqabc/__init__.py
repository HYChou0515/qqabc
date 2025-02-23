import abc
from typing import NoReturn

from qqabc.types import JobBody, SerializedJobBody


class JobSerializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, job_body: JobBody) -> SerializedJobBody:
        pass

    @abc.abstractmethod
    def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
        pass


class JobSerializerRegistry:
    def __init__(self) -> None:
        self._job_serializers: dict[str, JobSerializer] = {}

    def register_job_serializer(
            self,
            job_serializer: JobSerializer, job_type: str) -> None:
        self._job_serializers[job_type] = job_serializer

    def get_job_serializer(self, job_type: str) -> JobSerializer:
        if job_type not in self._job_serializers:
            raise KeyError(f"Job type {job_type} not found in registry")
        return self._job_serializers[job_type]

    def reset(self) -> None:
        self._job_serializers = {}


class JobQueueController:
    def get_job(self, job_id: str) -> NoReturn:
        raise KeyError(job_id)
