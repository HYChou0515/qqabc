import json

import pytest

from qqabc import JobQueueController, JobSerializer, JobSerializerRegistry
from qqabc.types import (
    EmptyQueueError,
    Job,
    JobBody,
    NewJobRequest,
    NewJobStatusRequest,
    SerializedJobBody,
    StatusEnum,
)
import datetime as dt
from tests.fixtures.faker import Faker
from freezegun.api import FrozenDateTimeFactory

@pytest.fixture
def job_serializer_registry() -> JobSerializerRegistry:
    return JobSerializerRegistry()

class TestJobSerializer:
    def test_register_job_serializer(self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

            def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
                return object()

        job_serializer = MyJobSerializer()
        returned = job_serializer_registry.register_job_serializer(
            job_serializer, job_type=fx_faker.job_type())  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return fx_faker.job_body_serialized()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> JobBody:
                return fx_faker.job_body()

        job_type = fx_faker.job_type()
        job_serializer = MyJobSerializer()
        job_serializer_registry.register_job_serializer(
            job_serializer, job_type=job_type)
        returned = job_serializer_registry.get_job_serializer(
            job_type=job_type)
        assert returned is job_serializer

    def test_get_unregistered_job_serializer_raises_key_error(self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry) -> None:
        job_type = fx_faker.job_type()
        with pytest.raises(KeyError) as e:
            job_serializer_registry.get_job_serializer(job_type=job_type)
        assert e.match(job_type)


def test_job_queue_controller_can_be_instantiated() -> None:
    controller = JobQueueController()
    assert controller is not None

class TestJobController:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return fx_faker.job_body_serialized()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> JobBody:
                return fx_faker.job_body()
        self.job_controller = JobQueueController()
        self.my_job_type = fx_faker.job_type()
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=self.my_job_type,
            job_serializer=MyJobSerializer(),
        )

    def _add_new_job_request_of_my_job_1(self) -> Job:
        new_job_request = NewJobRequest(
            job_type=self.my_job_type,
            job_body=self.fx_faker.job_body(),
        )
        job = self.job_controller.add_job(new_job_request)
        return job

    def assert_job_type(self, job: Job) -> None:
        assert isinstance(job, Job)
        assert job.job_type == self.my_job_type
        assert isinstance(job.job_id, str)
        assert isinstance(job.nice, int)

    def test_get_non_created_job_raises_key_error(self) -> None:
        job_id = self.fx_faker.uuid4()
        with pytest.raises(KeyError) as e:
            self.job_controller.get_job(job_id=job_id)
        assert e.match(job_id)

    def test_add_new_job(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        assert job is not None

    def test_get_added_job_returns_the_job(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_job(job.job_id)
        self.assert_job_type(job)
        assert returned.nice == 0
        assert returned.job_id == job.job_id

    def test_get_next_job_from_empty_queue_raise_empty_queue_error(self) -> None:
        with pytest.raises(EmptyQueueError) as e:
            self.job_controller.get_next_job(job_type=self.my_job_type)
        assert e.match(self.my_job_type)

    def test_get_next_job_returns_the_job(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_next_job(job_type=self.my_job_type)
        self.assert_job_type(job)
        assert returned.nice == 0
        assert returned.job_id == job.job_id

class MathJobBody:
    def __init__(self, *args, op: str, a: int, b: int, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.op = op
        self.a = a
        self.b = b

class TestJobConsumer:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker
        self.job_controller = JobQueueController()

        class MathJobSerializer(JobSerializer):
            def serialize(self, job_body: MathJobBody) -> SerializedJobBody:
                return json.dumps({
                    "op": job_body.op,
                    "a": job_body.a,
                    "b": job_body.b,
                }).encode()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> MathJobBody:
                data = json.loads(serialized_job_body.decode())
                return MathJobBody(
                    op=data["op"],
                    a=data["a"],
                    b=data["b"],
                )
            def serialize_result(self, result: int) -> SerializedJobBody:
                return json.dumps(result).encode()

            def deserialize_result(self,
                            serialized_result: SerializedJobBody) -> int:
                return json.loads(serialized_result.decode()) 
                
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type="math_job",
            job_serializer=MathJobSerializer(),
        )

    def test_return_job_result(self, freezer: FrozenDateTimeFactory) -> None:
        now = self.fx_faker.date_time()
        freezer.move_to(now)
        new_job_request = NewJobRequest(
            job_type="math_job",
            job_body=MathJobBody(op="add", a=1, b=2),
        )
        self.job_controller.add_job(new_job_request)
        job = self.job_controller.get_next_job(job_type="math_job")
        assert job.job_body.op == "add"
        assert job.job_body.a == 1
        assert job.job_body.b == 2
        status_request = NewJobStatusRequest(
            job_id=job.job_id,
            status=StatusEnum.COMPLETED,
            detail="Job completed successfully",
            result=3,
        )
        status = self.job_controller.add_job_status(status_request)
        assert status.job_id == status_request.job_id
        assert status.issue_time == now
        assert status.status == status_request.status
        assert status.detail == status_request.detail
        assert status.result == status_request.result
