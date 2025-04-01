import pytest

from qqabc.adapter.out.pseristence.job_repo_adapter import (
    FileJobRepo,
    InMemoryJobRepo,
    JobRepoAdapter,
)
from qqabc.application.domain.model.job import SerializedJob
from tests.tdd.fixtures.faker import Faker


def teardown_job_repo_adapter(
    job_repo_adapter: JobRepoAdapter,
) -> None:
    if isinstance(job_repo_adapter, (InMemoryJobRepo, FileJobRepo)):
        return job_repo_adapter.teardown()
    raise NotImplementedError


class TestJobRepoAdapter:
    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        fx_faker: Faker,
        fx_job_repo_adapter: JobRepoAdapter,
    ) -> None:
        self.faker = fx_faker
        self.job_repo = fx_job_repo_adapter

    def prepare_data(self) -> list[SerializedJob]:
        jobs: list[SerializedJob] = []
        for _ in range(10):
            job = self.faker.serialized_job()
            jobs.append(job)
            self.job_repo.add_job(job)
        for _ in range(3):
            self.job_repo.pop_largest_priority_job(None)
        for i in range(3):
            job = jobs[i]
            for _ in range(3):
                self.job_repo.add_status(
                    self.faker.serialized_status(job_id=job.job_id)
                )
        return jobs

    def test_dump(self) -> None:
        jobs = self.prepare_data()
        dumps = self.job_repo.dump()
        for job in jobs:
            assert job.job_id.encode() in dumps
        teardown_job_repo_adapter(self.job_repo)
        for job in jobs:
            assert job.job_id.encode() not in self.job_repo.dump()

    def test_load(self) -> None:
        self.prepare_data()
        dumps = self.job_repo.dump()
        teardown_job_repo_adapter(self.job_repo)
        self.job_repo.load(dumps)
        dumps2 = self.job_repo.dump()
        assert dumps == dumps2

    def test_transfer(self, fx_job_repo_adapter2: JobRepoAdapter) -> None:
        self.prepare_data()
        dumps = self.job_repo.dump()
        teardown_job_repo_adapter(self.job_repo)
        fx_job_repo_adapter2.load(dumps)
        dumps2 = fx_job_repo_adapter2.dump()
        assert dumps == dumps2
