
from dependency_injector import containers, providers

from qqabc.adapter.out.pseristence.job_repo_adapter import FileJobRepo
from qqabc.application.domain.service.job_queue_service import JobQueueService
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializerRegistry,
)


class Container(containers.DeclarativeContainer):
    config = providers.Configuration(
        ini_files=["config.ini"],
        default={
            "debug": False,
            "root_dir": ".qqabc",
            "job_dir": ".qqabc/jobs",
        },
    )

    job_dao = providers.Singleton(
        FileJobRepo,
        db_root=config.job_dir,
    )
    job_serializer_registry = providers.Singleton(
        JobSerializerRegistry,
    )

    job_queue_service = providers.Factory(
        JobQueueService,
        job_dao=job_dao,
        job_serializer_registry=job_serializer_registry,
    )
