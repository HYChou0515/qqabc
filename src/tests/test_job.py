from qqabc.types import JobBody, SerializedJobBody
from qqabc import JobSerializer, JobSerializerRegistry
import pytest

def test_job_body_exists():
    job_body = JobBody()
    assert job_body is not None

def test_job_serialized_body_exists():
    serialized_job_body = SerializedJobBody()
    assert serialized_job_body is not None

def test_job_serializer_can_be_instantiated():
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()
        
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()

    job_serializer = MyJobSerializer()
    assert job_serializer is not None

def test_job_serializer_should_implement_serialize():
    class MyJobSerializer(JobSerializer):
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()
    assert e.match("Can't instantiate abstract class MyJobSerializer with abstract method serialize")


def test_job_serializer_should_implement_deserialize():
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()

    with pytest.raises(TypeError) as e:
        MyJobSerializer()
    assert e.match("Can't instantiate abstract class MyJobSerializer with abstract method deserialize")

@pytest.fixture
def job_serializer_registry():
    try:
        registry = JobSerializerRegistry()
        yield registry
    finally:
        registry.reset()
def test_register_job_serializer(job_serializer_registry: JobSerializerRegistry):
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()
        
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()

    job_serializer = MyJobSerializer()
    returned = job_serializer_registry.register_job_serializer(job_serializer, job_type="my_job")
    assert returned is None

def test_get_job_serializer(job_serializer_registry: JobSerializerRegistry):
    class MyJobSerializer(JobSerializer):
        def serialize(self, job_body: JobBody) -> SerializedJobBody:
            return SerializedJobBody()
        
        def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
            return JobBody()
        
    job_serializer = MyJobSerializer()
    job_serializer_registry.register_job_serializer(job_serializer, job_type="my_job")
    returned = job_serializer_registry.get_job_serializer(job_type="my_job")
    assert returned is job_serializer

def test_get_unregistered_job_serializer_raises_key_error(job_serializer_registry: JobSerializerRegistry):
    with pytest.raises(KeyError) as e:
        job_serializer_registry.get_job_serializer(job_type="my_job")
    assert e.match("my_job")
