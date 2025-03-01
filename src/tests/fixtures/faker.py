import pytest
from faker import Faker as _Faker

class Faker(_Faker):
    def job_body(self):
        return self.random_element([
            object() for _ in range(20)
        ])
    def job_body_serialized(self):
        return self.random_element([
            bytes(self.random_int(1, 30)) for _ in range(20)
        ])

@pytest.fixture
def fx_faker():
    return Faker()
