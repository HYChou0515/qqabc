from dependency_injector.wiring import Provide, inject

from qqabc.application.domain.service.job_queue_service import IJobQueueService
from qqabc.application.domain.service.result_service import IResultService
from qqabc.application.domain.service.status_service import IStatusService
from qqabc_cli.di.in_ import Container


@inject
def di_job_queue_service(
    svc: IJobQueueService = Provide[Container.job_queue_service],
) -> IJobQueueService:
    return svc


@inject
def di_status_service(
    svc: IStatusService = Provide[Container.status_service],
) -> IStatusService:
    return svc


@inject
def di_result_service(
    svc: IResultService = Provide[Container.result_service],
) -> IResultService:
    return svc
