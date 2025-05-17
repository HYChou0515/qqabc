from __future__ import annotations

import abc
from typing import Generic

from qqabc.application.domain.model.job import (
    GJobBody,
    GResult,
    GSerializedJobBody,
    GSerializedResult,
)
from qqabc.common.exceptions import SerializerNotFoundError


class JobSerializer(
    abc.ABC, Generic[GJobBody, GSerializedJobBody, GResult, GSerializedResult]
):
    @abc.abstractmethod
    def serialize(
        self, job_body: GJobBody | GResult
    ) -> GSerializedJobBody | GSerializedResult:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(
        self, serialized: GSerializedJobBody | GSerializedResult
    ) -> GJobBody | GResult:
        raise NotImplementedError


class JobSerializerRegistry:
    def __init__(self) -> None:
        self._job_serializers: dict[str, JobSerializer] = {}

    def register_job_serializer(
        self, job_serializer: JobSerializer, job_type: str
    ) -> None:
        self._job_serializers[job_type] = job_serializer

    def get_job_serializer(self, job_type: str) -> JobSerializer:
        if job_type not in self._job_serializers:
            raise SerializerNotFoundError(f"Job type {job_type} not found in registry")
        return self._job_serializers[job_type]
