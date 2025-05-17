import tempfile
from collections.abc import Generator

import pytest

from qqabc_cli.di.in_ import Container
from qqabc_cli.di.out import get_container


@pytest.fixture(params=["memory", "disk"])
def fx_test_container(request: pytest.FixtureRequest) -> Generator[Container]:
    job_dao_type = request.param
    c = get_container(reset=True)
    c.config.reset_override()
    c.job_dao.reset_override()
    c.job_serializer_registry.reset()
    c.job_serializer_registry.reset_override()
    c.job_queue_service.reset_override()
    c.status_service.reset_override()
    c.result_service.reset_override()
    with tempfile.TemporaryDirectory() as tmpdir:
        c.config.job_dao.type.override(job_dao_type)
        c.config.job_dao.root_dir.override(tmpdir)
        yield c
