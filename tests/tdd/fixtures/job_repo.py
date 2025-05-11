from __future__ import annotations

import tempfile
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    DiskJobRepo,
    IJobRepo,
    MemoryJobRepo,
)

if TYPE_CHECKING:
    from collections.abc import Generator


@contextmanager
def _in_mem_job_repo() -> Generator[IJobRepo]:
    job_repo = MemoryJobRepo()
    yield job_repo


@contextmanager
def _file_job_repo() -> Generator[IJobRepo]:
    with tempfile.TemporaryDirectory() as d:
        job_repo = DiskJobRepo(d)
        yield job_repo


@pytest.fixture(params=["InMemoryJobRepo", "FileJobRepo"])
def fx_job_repo_adapter(request: pytest.FixtureRequest) -> Generator[IJobRepo]:
    if request.param == "InMemoryJobRepo":
        with _in_mem_job_repo() as job_repo:
            yield job_repo
    if request.param == "FileJobRepo":
        with _file_job_repo() as job_repo:
            yield job_repo


@pytest.fixture(params=["InMemoryJobRepo", "FileJobRepo"])
def fx_job_repo_adapter2(request: pytest.FixtureRequest) -> Generator[IJobRepo]:
    if request.param == "InMemoryJobRepo":
        with _in_mem_job_repo() as job_repo:
            yield job_repo
    if request.param == "FileJobRepo":
        with _file_job_repo() as job_repo:
            yield job_repo
