from __future__ import annotations

import os
import shutil
import tempfile
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from qqabc.adapter.out.pseristence.job_repo_adapter import InMemoryJobRepo
from qqabc_cli.main import build_container, create_app

if TYPE_CHECKING:
    from collections.abc import Generator

    import typer

    from tests.tdd.fixtures.faker import Faker


@pytest.fixture
def fx_workdir() -> Generator[str, None, None]:
    """建立臨時資料夾, 並切換到該資料夾"""
    d = tempfile.mkdtemp()
    os.chdir(d)
    try:
        yield d
    finally:
        os.chdir(os.path.dirname(__file__))
        shutil.rmtree(d)


@pytest.fixture
def fx_app() -> Generator[typer.Typer]:
    container = build_container()
    job_dao = InMemoryJobRepo()
    with container.job_dao.override(job_dao):
        app = create_app()
        yield app
        job_dao.teardown()


@pytest.fixture
def fx_runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


@pytest.fixture
def fx_job_body_file(fx_faker: Faker) -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(fx_faker.json_bytes())
        f.flush()
        yield f.name
