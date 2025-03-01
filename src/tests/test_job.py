import json
import unittest

import pytest

from qqabc import JobQueueController, JobSerializer, JobSerializerRegistry
from qqabc.types import (
    EmptyQueueError,
    Job,
    JobBody,
    JobStatus,
    NewJobRequest,
    SerializedJob,
    SerializedJobBody,
    StatusEnum,
)
from tests.fixtures.faker import Faker

def test_job_serializer_can_be_instantiated() -> None:
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return bytes()

        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return object()

    job_serializer = MyJobSerializer()
    assert job_serializer is not None


def test_job_serializer_should_implement_serialize() -> None:
    class MyJobSerializer(JobSerializer):
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return object()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()  # type: ignore[abstract]
    assert e.match("Can't instantiate abstract class "
                   "MyJobSerializer with abstract method serialize")


def test_job_serializer_should_implement_deserialize() -> None:
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return bytes()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()  # type: ignore[abstract]
    assert e.match("Can't instantiate abstract class "
                   "MyJobSerializer with abstract method deserialize")

def test_status_enum() -> None:
    assert StatusEnum.INITIAL == "INITIAL"
    assert StatusEnum.PENDING == "PENDING"
    assert StatusEnum.RUNNING == "RUNNING"
    assert StatusEnum.COMPLETED == "COMPLETED"
    assert StatusEnum.FAILED == "FAILED"

def test_job_status_instantiated(fx_faker: Faker) -> None:
    job_status = JobStatus(
        status_id=(status_id:=fx_faker.uuid4()),
        job_id=(job_id:=fx_faker.uuid4()),
        issue_time=(issue_time:=fx_faker.date_time()),
        status=(status:=fx_faker.random_element(StatusEnum)),
        detail=(detail:=fx_faker.sentence()),
        result=(result:=fx_faker.pyint()),
    )
    assert job_status.status_id == status_id
    assert job_status.job_id == job_id
    assert job_status.issue_time == issue_time
    assert job_status.status == status
    assert job_status.detail == detail
    assert job_status.result == result

class TestJobSerializer(unittest.TestCase):
    def setUp(self) -> None:
        self.job_serializer_registry = JobSerializerRegistry()

    def tearDown(self) -> None:
        self.job_serializer_registry.reset()

    def test_register_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

            def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
                return object()

        job_serializer = MyJobSerializer()
        returned = self.job_serializer_registry.register_job_serializer(
            job_serializer, job_type="my_job")  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> JobBody:
                return object()

        job_serializer = MyJobSerializer()
        self.job_serializer_registry.register_job_serializer(
            job_serializer, job_type="my_job")
        returned = self.job_serializer_registry.get_job_serializer(
            job_type="my_job")
        assert returned is job_serializer

    def test_get_unregistered_job_serializer_raises_key_error(self) -> None:
        with pytest.raises(KeyError) as e:
            self.job_serializer_registry.get_job_serializer(job_type="my_job")
        assert e.match("my_job")


def test_job_queue_controller_can_be_instantiated() -> None:
    controller = JobQueueController()
    assert controller is not None


def test_new_job_request_can_be_instantiated() -> None:
    new_job_request = NewJobRequest(
        job_type="my_job",
        job_body=object(),
    )
    assert new_job_request is not None


def test_job_can_be_instantiated() -> None:
    job = Job(
        job_type="my_job",
        job_id="my_job_id",
        job_body=object(),
    )
    assert job is not None


def test_serialized_job_can_be_instantiated() -> None:
    job = SerializedJob(
        job_type="my_job",
        job_id="my_job_id",
        job_body_serialized=bytes(),
    )
    assert job is not None


class TestJobController(unittest.TestCase):
    def setUp(self) -> None:
        self.job_controller = JobQueueController()

        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> JobBody:
                return object()
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type="my_job",
            job_serializer=MyJobSerializer(),
        )

    def tearDown(self) -> None:
        self.job_controller.job_dao.drop_all_jobs()

    def _add_new_job_request_of_my_job_1(self) -> Job:
        new_job_request = NewJobRequest(
            job_type="my_job",
            job_body=object(),
        )
        job = self.job_controller.add_job(new_job_request)
        return job

    def assert_job_type(self, job: Job) -> None:
        assert isinstance(job, Job)
        assert job.job_type == "my_job"
        assert isinstance(job.job_id, str)
        assert isinstance(job.nice, int)

    def test_get_non_created_job_raises_key_error(self) -> None:
        with pytest.raises(KeyError) as e:
            self.job_controller.get_job(job_id="my_job")
        assert e.match("my_job")

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
            self.job_controller.get_next_job(job_type="my_job")
        assert e.match("my_job")

    def test_get_next_job_returns_the_job(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_next_job(job_type="my_job")
        self.assert_job_type(job)
        assert returned.nice == 0
        assert returned.job_id == job.job_id

class MathJobBody:
    def __init__(self, *args, op: str, a: int, b: int, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.op = op
        self.a = a
        self.b = b

class TestJobConsumer(unittest.TestCase):
    def setUp(self) -> None:
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

    def tearDown(self) -> None:
        self.job_controller.job_dao.drop_all_jobs()

    # def _test_return_job_result(self) -> None:
    #     new_job_request = NewJobRequest(
    #         job_type="math_job",
    #         job_body=MathJobBody(op="add", a=1, b=2),
    #     )
    #     self.job_controller.add_job(new_job_request)
    #     job = self.job_controller.get_next_job(job_type="math_job")
    #     assert job.job_body.op == "add"
    #     assert job.job_body.a == 1
    #     assert job.job_body.b == 2
    #     self.job_controller.create_job_result(job.job_id, 3)
