import pytest

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    IJobRepo,
)
from qqabc.application.domain.model.job import SerializedJob
from tests.tdd.fixtures.faker import Faker


class TestJobRepoAdapter:
    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        fx_faker: Faker,
        fx_job_repo_adapter: IJobRepo,
    ) -> None:
        self.faker = fx_faker
        self.job_repo = fx_job_repo_adapter

    def test_job_add_get(self) -> None:
        job = self.faker.serialized_job()
        self.job_repo.add_job(job)
        sjob = self.job_repo.get_job(job.job_id)
        assert sjob == job
        sjob2 = self.job_repo.get_job(job.job_id)
        assert sjob2 == job
        sjob3 = self.job_repo.pop_largest_priority_job(None)
        assert sjob3 == job
        sjob4 = self.job_repo.pop_largest_priority_job(None)
        assert sjob4 is None
        sjob5 = self.job_repo.get_job(job.job_id)
        assert sjob5 == job

    def prepare_data(self) -> list[SerializedJob]:
        jobs: list[SerializedJob] = []
        for _ in range(10):
            job = self.faker.serialized_job()
            jobs.append(job)
            self.job_repo.add_job(job)
        for _ in range(3):
            self.job_repo.pop_largest_priority_job(None)
        return jobs

    def test_dump(self) -> None:
        jobs = self.prepare_data()
        dumps = self.job_repo.dump()
        for job in jobs:
            assert job.job_id.encode() in dumps

    def test_load(self) -> None:
        self.prepare_data()
        dumps = self.job_repo.dump()
        self.job_repo.load(dumps)
        dumps2 = self.job_repo.dump()
        assert dumps == dumps2

    def test_transfer(self, fx_job_repo_adapter2: IJobRepo) -> None:
        self.prepare_data()
        dumps = self.job_repo.dump()
        fx_job_repo_adapter2.load(dumps)
        dumps2 = fx_job_repo_adapter2.dump()
        assert dumps == dumps2
