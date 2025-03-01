import pytest
from faker import Faker as _Faker

from qqabc.types import JobBody, NewJobRequest


class Faker(_Faker):
    def job_body(self):
        return self.random_element([
            object() for _ in range(20)
        ])

    def job_body_serialized(self):
        return self.random_element([
            bytes(self.random_int(1, 30)) for _ in range(20)
        ])

    def job_type(self):
        return self.name()

    def new_job_request(self, *, job_type: str = ..., job_body: JobBody = ...):
        if job_body is ...:
            job_body = self.job_body()
        if job_type is ...:
            job_type = self.job_type()
        return NewJobRequest(
            job_type=job_type,
            job_body=job_body,
        )


@pytest.fixture
def fx_faker():
    return Faker()
