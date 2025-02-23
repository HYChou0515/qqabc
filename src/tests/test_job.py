import unittest

import pytest

from qqabc import JobQueueController, JobSerializer, JobSerializerRegistry
from qqabc.types import JobBody, NewJobRequest, SerializedJobBody


def test_job_body_exists() -> None:
    job_body = JobBody()
    assert job_body is not None


def test_job_serialized_body_exists() -> None:
    serialized_job_body = SerializedJobBody()
    assert serialized_job_body is not None


def test_job_serializer_can_be_instantiated() -> None:
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()

        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()

    job_serializer = MyJobSerializer()
    assert job_serializer is not None


def test_job_serializer_should_implement_serialize() -> None:
    class MyJobSerializer(JobSerializer):
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()  # type: ignore[abstract]
    assert e.match("Can't instantiate abstract class "
                   "MyJobSerializer with abstract method serialize")


def test_job_serializer_should_implement_deserialize() -> None:
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()  # type: ignore[abstract]
    assert e.match("Can't instantiate abstract class "
                   "MyJobSerializer with abstract method deserialize")


class TestJobSerializer(unittest.TestCase):
    def setUp(self) -> None:
        self.job_serializer_registry = JobSerializerRegistry()

    def tearDown(self) -> None:
        self.job_serializer_registry.reset()

    def test_register_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return SerializedJobBody()

            def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
                return JobBody()

        job_serializer = MyJobSerializer()
        returned = self.job_serializer_registry.register_job_serializer(
            job_serializer, job_type="my_job")  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return SerializedJobBody()

            def deserialize(self,
                            serialized_job_body: SerializedJobBody) -> JobBody:
                return JobBody()

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
        job_body=JobBody(),
    )
    assert new_job_request is not None


class TestJobController(unittest.TestCase):
    def setUp(self) -> None:
        self.job_controller = JobQueueController()

    def test_get_non_created_job_raises_key_error(self) -> None:
        with pytest.raises(KeyError) as e:
            self.job_controller.get_job(job_id="my_job")
        assert e.match("my_job")
