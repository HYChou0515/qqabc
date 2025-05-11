from abc import ABC, abstractmethod
from typing import Literal

from typing_extensions import overload

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    IJobRepo as IJobRepo,
)
from qqabc.application.domain.model.job import (
    Job as Job,
)
from qqabc.application.domain.model.job import (
    SerializedJob as SerializedJob,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializer as JobSerializer,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializerRegistry as JobSerializerRegistry,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewJobRequest as NewJobRequest,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewSerializedJobRequest as NewSerializedJobRequest,
)
from qqabc.common.exceptions import (
    EmptyQueueError as EmptyQueueError,
)
from qqabc.common.exceptions import (
    JobNotFoundError as JobNotFoundError,
)

class IJobQueueService(ABC):
    @overload
    def get_job(self, job_id: str) -> SerializedJob: ...
    @overload
    def get_job(self, job_id: str, *, deserialize: Literal[True]) -> Job: ...
    @overload
    def get_job(
        self, job_id: str, *, deserialize: Literal[False] = False
    ) -> SerializedJob: ...
    @overload
    def list_jobs(self) -> list[SerializedJob]: ...
    @overload
    def list_jobs(self, *, deserialize: Literal[True]) -> list[Job]: ...
    @overload
    def list_jobs(
        self, *, deserialize: Literal[False] = False
    ) -> list[SerializedJob]: ...
    @overload
    def add_job(self, new_job_request: NewJobRequest) -> Job: ...
    @overload
    def add_job(self, new_job_request: NewSerializedJobRequest) -> SerializedJob: ...
    @overload
    def get_next_job(self, job_type: str | None) -> SerializedJob: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: Literal[True]
    ) -> Job: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: Literal[False]
    ) -> SerializedJob: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: bool
    ) -> Job | SerializedJob: ...
    @abstractmethod
    def check_job_exists(self, job_id: str) -> None: ...
    @abstractmethod
    def get_job_type(self, job_id: str, job: Job | SerializedJob | None) -> str: ...

class JobQueueService(IJobQueueService):
    job_dao: IJobRepo
    job_serializer_registry: JobSerializerRegistry
    def __init__(
        self, job_dao: IJobRepo, job_serializer_registry: JobSerializerRegistry
    ) -> None: ...
    def check_job_exists(self, job_id: str) -> None: ...
    @overload
    def get_job(self, job_id: str) -> SerializedJob: ...
    @overload
    def get_job(self, job_id: str, *, deserialize: Literal[True]) -> Job: ...
    @overload
    def get_job(
        self, job_id: str, *, deserialize: Literal[False] = False
    ) -> SerializedJob: ...
    @overload
    def list_jobs(self) -> list[SerializedJob]: ...
    @overload
    def list_jobs(self, *, deserialize: Literal[True]) -> list[Job]: ...
    @overload
    def list_jobs(
        self, *, deserialize: Literal[False] = False
    ) -> list[SerializedJob]: ...
    @overload
    def add_job(self, new_job_request: NewJobRequest) -> Job: ...
    @overload
    def add_job(self, new_job_request: NewSerializedJobRequest) -> SerializedJob: ...
    @overload
    def get_next_job(self, job_type: str | None) -> SerializedJob: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: Literal[True]
    ) -> Job: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: Literal[False]
    ) -> SerializedJob: ...
    @overload
    def get_next_job(
        self, job_type: str | None, *, deserialize: bool
    ) -> Job | SerializedJob: ...
    def get_job_type(self, job_id: str, job: Job | SerializedJob | None) -> str: ...
