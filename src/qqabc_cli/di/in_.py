from dependency_injector import containers, providers

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    DiskJobRepo,
    MemoryJobRepo,
)
from qqabc.adapter.out.pseristence.job_status_dao import (
    DiskJobStatusRepo,
    MemoryJobStatusRepo,
)
from qqabc.application.domain.service.job_queue_service import (
    IJobQueueService,
    JobQueueService,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializerRegistry,
)
from qqabc.application.domain.service.result_service import (
    IResultService,
    ResultService,
)
from qqabc.application.domain.service.status_service import (
    IStatusService,
    StatusService,
)


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    job_dao: providers.Selector = providers.Selector(
        config.job_dao.type,
        memory=providers.Singleton(MemoryJobRepo),
        disk=providers.Singleton(DiskJobRepo, db_root=config.job_dao.root_dir),
    )
    job_status_dao: providers.Selector = providers.Selector(
        config.job_dao.type,
        memory=providers.Singleton(MemoryJobStatusRepo),
        disk=providers.Singleton(DiskJobStatusRepo, db_root=config.job_dao.root_dir),
    )
    job_serializer_registry = providers.Singleton(
        JobSerializerRegistry,
    )
    job_queue_service: providers.Factory[IJobQueueService] = providers.Factory(
        JobQueueService,
        job_dao=job_dao,
        job_serializer_registry=job_serializer_registry,
    )

    status_service: providers.Factory[IStatusService] = providers.Factory(
        StatusService,
        job_svc=job_queue_service,
        job_dao=job_dao,
        job_status_dao=job_status_dao,
        job_serializer_registry=job_serializer_registry,
    )

    result_service: providers.Factory[IResultService] = providers.Factory(
        ResultService,
        job_svc=job_queue_service,
        job_dao=job_dao,
        job_status_dao=job_status_dao,
    )
