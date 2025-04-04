import abc
from typing import Generic

from typing_extensions import overload

from qqabc.application.domain.model.job import (
    GJobBody as GJobBody,
)
from qqabc.application.domain.model.job import (
    GResult as GResult,
)
from qqabc.application.domain.model.job import (
    GSerializedJobBody as GSerializedJobBody,
)
from qqabc.application.domain.model.job import (
    GSerializedResult as GSerializedResult,
)
from qqabc.common.exceptions import SerializerNotFoundError as SerializerNotFoundError

class JobSerializer(
    abc.ABC, Generic[GJobBody, GSerializedJobBody, GResult, GSerializedResult]
):
    @overload
    def serialize(self, job_body: GJobBody) -> GSerializedJobBody: ...
    @overload
    def serialize(self, job_body: GResult) -> GSerializedResult: ...
    @overload
    def deserialize(self, serialized: GSerializedJobBody) -> GJobBody: ...
    @overload
    def deserialize(self, serialized: GSerializedResult) -> GResult: ...
    def serialize_result(self, result: GResult) -> GSerializedResult: ...
    def deserialize_result(self, serialized_result: GSerializedResult) -> GResult: ...

class JobSerializerRegistry:
    def __init__(self) -> None: ...
    def register_job_serializer(
        self, job_serializer: JobSerializer, job_type: str
    ) -> None: ...
    def get_job_serializer(self, job_type: str) -> JobSerializer: ...
