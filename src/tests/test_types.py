from __future__ import annotations


import pytest

from qqabc import JobSerializer
from qqabc.types import (
    Job,
    JobBody,
    JobStatus,
    NewJobRequest,
    SerializedJob,
    SerializedJobBody,
    StatusEnum,
)
from tests.fixtures.faker import Faker

class TestEntityInstantiation:

    def test_status_enum(self) -> None:
        assert StatusEnum.INITIAL == "INITIAL"
        assert StatusEnum.PENDING == "PENDING"
        assert StatusEnum.RUNNING == "RUNNING"
        assert StatusEnum.COMPLETED == "COMPLETED"
        assert StatusEnum.FAILED == "FAILED"

    @pytest.mark.parametrize("set_default", [None, "nice"])
    def test_job(self, fx_faker: Faker, set_default: str|None) -> None:
        kwargs = {
            "job_type": fx_faker.name(),
            "job_id": fx_faker.uuid4(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
        }
        if set_default == "nice":
            del kwargs["nice"]

        job = Job(**kwargs)
        assert job.job_type == kwargs["job_type"]
        assert job.job_id == kwargs["job_id"]
        assert job.job_body == kwargs["job_body"]
        assert job.nice == kwargs.get("nice", 0)

    @pytest.mark.parametrize("set_default", [None, "nice"])
    def test_serialized_job(self, fx_faker: Faker, set_default: str|None) -> None:
        kwargs = {
            "job_type": fx_faker.name(),
            "job_id": fx_faker.uuid4(),
            "job_body_serialized": fx_faker.job_body_serialized(),
            "nice": fx_faker.pyint(),
        }
        if set_default == "nice":
            del kwargs["nice"]
        
        serialized_job = SerializedJob(**kwargs)
        assert serialized_job.job_type == kwargs["job_type"]
        assert serialized_job.job_id == kwargs["job_id"]
        assert serialized_job.job_body_serialized == kwargs["job_body_serialized"]
        assert serialized_job.nice == kwargs.get("nice", 0)

    def test_job_status(self, fx_faker: Faker) -> None:
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

    @pytest.mark.parametrize("set_default", [None, "nice"])
    def test_new_job_quest(self, fx_faker: Faker, set_default: str|None) -> None:
        kwargs = {
            "job_type": fx_faker.name(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
        }
        if set_default == "nice":
            del kwargs["nice"]
        
        new_job_request = NewJobRequest(**kwargs)
        assert new_job_request.job_type == kwargs["job_type"]
        assert new_job_request.job_body == kwargs["job_body"]
        assert new_job_request.nice == kwargs.get("nice", 0)
    

class TestJobSerializer:
    def test_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

            def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
                return object()

        job_serializer = MyJobSerializer()
        assert job_serializer is not None

    def test_job_serializer_should_implement_serialize(self) -> None:
        class MyJobSerializer(JobSerializer):
            def deserialize(self, serialized_job_body: SerializedJobBody) -> JobBody:
                return object()

        with pytest.raises(TypeError) as e:
            MyJobSerializer()  # type: ignore[abstract]
        assert e.match("Can't instantiate abstract class "
                    "MyJobSerializer with abstract method serialize")

    def test_job_serializer_should_implement_deserialize(self) -> None:
        class MyJobSerializer(JobSerializer):
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                return bytes()

        with pytest.raises(TypeError) as e:
            MyJobSerializer()  # type: ignore[abstract]
        assert e.match("Can't instantiate abstract class "
                    "MyJobSerializer with abstract method deserialize")