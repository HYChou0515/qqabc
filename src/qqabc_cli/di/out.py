from dependency_injector.wiring import Provide, inject

from qqabc.application.domain.service.job_queue_service import IJobQueueService
from qqabc.application.domain.service.result_service import IResultService
from qqabc.application.domain.service.status_service import IStatusService
from qqabc_cli.config import get_default_config
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


def __build_container() -> Container:
    container = Container()
    container.wire(modules=[__name__])
    return container


__container: Container = __build_container()


def get_container(*, reset: bool = False) -> Container:
    if reset:  # pragma: no cover
        __container.reset_singletons()
        __container.config.reset_override()
        __container.job_dao.reset_override()
        __container.job_status_dao.reset_override()
        __container.job_serializer_registry.reset()
        __container.job_serializer_registry.reset_override()
        __container.job_queue_service.reset_override()
        __container.status_service.reset_override()
        __container.result_service.reset_override()
    __container.config.from_dict(get_default_config())  # type: ignore[arg-type]
    return __container
