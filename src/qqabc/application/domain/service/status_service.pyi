from abc import ABC, abstractmethod
from typing import Literal

from typing_extensions import overload

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    JobRepoAdapter as JobRepoAdapter,
)
from qqabc.application.domain.model.job import (
    NO_RESULT as NO_RESULT,
)
from qqabc.application.domain.model.job import (
    QQABC as QQABC,
)
from qqabc.application.domain.model.job import (
    Job as Job,
)
from qqabc.application.domain.model.job import (
    JobStatus as JobStatus,
)
from qqabc.application.domain.model.job import (
    SerializedJob as SerializedJob,
)
from qqabc.application.domain.model.job import (
    SerializedJobStatus as SerializedJobStatus,
)
from qqabc.application.domain.service.job_queue_service import (
    IJobQueueService as IJobQueueService,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializer as JobSerializer,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializerRegistry as JobSerializerRegistry,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest as NewJobStatusRequest,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewSerializedJobStatusRequest as NewSerializedJobStatusRequest,
)

class IStatusService(ABC):
    @overload
    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus: ...
    @overload
    def add_job_status(
        self, request: NewSerializedJobStatusRequest
    ) -> SerializedJobStatus: ...
    @overload
    def get_latest_status(self, job_id: str) -> JobStatus | None: ...
    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[True]
    ) -> JobStatus | None: ...
    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[False]
    ) -> SerializedJobStatus | None: ...
    @abstractmethod
    def list_job_status(self, job_id: str) -> list[SerializedJobStatus]: ...

class StatusService(IStatusService):
    job_svc: IJobQueueService
    job_dao: JobRepoAdapter
    job_serializer_registry: JobSerializerRegistry
    def __init__(
        self,
        job_svc: IJobQueueService,
        job_dao: JobRepoAdapter,
        job_serializer_registry: JobSerializerRegistry,
    ) -> None: ...
    @overload
    def add_job_status(self, request: NewJobStatusRequest) -> JobStatus: ...
    @overload
    def add_job_status(
        self, request: NewSerializedJobStatusRequest
    ) -> SerializedJobStatus: ...
    @overload
    def get_latest_status(self, job_id: str) -> JobStatus | None: ...
    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[True]
    ) -> JobStatus | None: ...
    @overload
    def get_latest_status(
        self, job_id: str, *, deserialize: Literal[False]
    ) -> SerializedJobStatus | None: ...
    def list_job_status(self, job_id: str) -> list[SerializedJobStatus]: ...
