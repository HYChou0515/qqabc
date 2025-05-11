from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qqabc.application.domain.service.job_queue_service import (
    IJobQueueService,
    JobQueueService,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializerRegistry,
)
from qqabc.application.domain.service.status_service import (
    IStatusService,
    StatusService,
)

if TYPE_CHECKING:
    from qqabc.adapter.out.pseristence.job_repo_adapter import (
        IJobRepo,
    )


@pytest.fixture
def fx_job_serializer_registry() -> JobSerializerRegistry:
    return JobSerializerRegistry()


@pytest.fixture
def fx_job_queue_controller(
    fx_job_repo_adapter: IJobRepo,
    fx_job_serializer_registry: JobSerializerRegistry,
) -> IJobQueueService:
    return JobQueueService(fx_job_repo_adapter, fx_job_serializer_registry)


@pytest.fixture
def fx_status_svc(
    fx_job_queue_controller: JobQueueService,
    fx_job_repo_adapter: IJobRepo,
    fx_job_serializer_registry: JobSerializerRegistry,
) -> IStatusService:
    return StatusService(
        fx_job_queue_controller, fx_job_repo_adapter, fx_job_serializer_registry
    )
