
from dependency_injector.wiring import Provide, inject
from qqabc.application.domain.service.job_queue_service import JobQueueService
from qqabc_cli.di.in_ import Container

@inject
def di_job_queue_service(svc: JobQueueService = Provide[Container.job_queue_service]) -> JobQueueService:
    return svc
