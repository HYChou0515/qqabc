import tempfile
from collections.abc import Generator

import pytest

from qqabc.adapter.out.pseristence.job_status_dao import (
    DiskJobStatusRepo,
    IJobStatusRepo,
    MemoryJobStatusRepo,
)
from qqabc.application.domain.model.job import JobResult, JobStatus
from tests.tdd.fixtures.faker import Faker


@pytest.fixture(params=["MemoryJobStatusRepo", "DiskJobStatusRepo"])
def fx_repo_1(request: pytest.FixtureRequest) -> Generator[IJobStatusRepo]:
    if request.param == "MemoryJobStatusRepo":
        yield MemoryJobStatusRepo()
    if request.param == "DiskJobStatusRepo":
        with tempfile.TemporaryDirectory() as tmpdir:
            yield DiskJobStatusRepo(db_root=tmpdir)


@pytest.fixture(params=["MemoryJobStatusRepo", "DiskJobStatusRepo"])
def fx_repo_2(request: pytest.FixtureRequest) -> Generator[IJobStatusRepo]:
    if request.param == "MemoryJobStatusRepo":
        yield MemoryJobStatusRepo()
    if request.param == "DiskJobStatusRepo":
        with tempfile.TemporaryDirectory() as tmpdir:
            yield DiskJobStatusRepo(db_root=tmpdir)


class TestJobRepoAdapter:
    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        fx_faker: Faker,
        fx_repo_1: IJobStatusRepo,
    ) -> None:
        self.faker = fx_faker
        self.job_status_dao = fx_repo_1

    def prepare_data(self) -> tuple[list[JobResult], list[JobStatus]]:
        job_results = []
        job_statuses = []
        for _ in range(10):
            job_id = self.faker.job_id()
            for _ in range(self.faker.random_int(1, 3)):
                job_result = self.faker.job_result(job_id=job_id)
                self.job_status_dao.add_result(job_result)
                job_results.append(job_result)
        for _ in range(10):
            job_id = self.faker.job_id()
            for _ in range(self.faker.random_int(1, 3)):
                job_status = self.faker.job_status(job_id=job_id)
                self.job_status_dao.add_status(job_status)
                job_statuses.append(job_status)
        return job_results, job_statuses

    def test_iter_result_from_absent_job(self) -> None:
        job_id = self.faker.job_id()
        assert list(self.job_status_dao.iter_result(job_id)) == []

    def test_iter_status_from_absent_job(self) -> None:
        job_id = self.faker.job_id()
        assert list(self.job_status_dao.iter_status(job_id)) == []

    def test_add_result(self) -> None:
        job_result = self.faker.job_result()
        self.job_status_dao.add_result(job_result)
        assert job_result == self.job_status_dao.get_latest_result(job_result.job_id)
        assert job_result in self.job_status_dao.iter_result(job_result.job_id)

    def test_add_status(self) -> None:
        job_status = self.faker.job_status()
        self.job_status_dao.add_status(job_status)
        assert job_status == self.job_status_dao.get_latest_status(job_status.job_id)
        assert job_status in self.job_status_dao.iter_status(job_status.job_id)

    def test_dump(self) -> None:
        job_results, job_statuses = self.prepare_data()
        dumps = self.job_status_dao.dump()
        for r in job_results:
            assert r.job_id.encode() in dumps
            assert r.result_id.encode() in dumps
        for s in job_statuses:
            assert s.job_id.encode() in dumps
            assert s.status_id.encode() in dumps

    def test_load(self) -> None:
        self.prepare_data()
        dumps = self.job_status_dao.dump()
        self.job_status_dao.load(dumps)
        dumps2 = self.job_status_dao.dump()
        assert dumps == dumps2

    def test_transfer(self, fx_repo_2: IJobStatusRepo) -> None:
        self.prepare_data()
        dumps = self.job_status_dao.dump()
        fx_repo_2.load(dumps)
        dumps2 = fx_repo_2.dump()
        assert dumps == dumps2
