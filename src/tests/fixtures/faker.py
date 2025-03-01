import pytest
from faker import Faker as _Faker

class Faker(_Faker):
    pass

@pytest.fixture
def fx_faker():
    return Faker()
