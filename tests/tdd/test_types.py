from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, overload

import pytest

from qqabc.application.domain.model.job import (
    Job,
    JobBody,
    JobStatus,
    Result,
    SerializedJob,
    SerializedJobBody,
    SerializedResult,
    StatusEnum,
)
from qqabc.application.domain.service.job_serializer_registry import JobSerializer
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)
from qqabc.application.port.in_.submit_job_use_case import (
    NewJobRequest,
    NewSerializedJobRequest,
)
from qqabc.common.serializer import serializer

if TYPE_CHECKING:
    from tests.tdd.fixtures.faker import Faker


def assert_eq(a: object, b: object) -> None:
    assert a == b
    assert b == a


def assert_ne(a: object, b: object) -> None:
    assert a != b
    assert b != a


def assert_eq_ne(
    cls: type[object],
    kwargs1: dict,
    kwargs2: dict,
    *,
    assert_subclass: bool = True,
    cls2: type[object] | None = None,
) -> None:
    cls2 = cls2 or cls
    assert_eq(cls(**kwargs1), cls2(**kwargs1))
    assert_ne(cls(**kwargs1), cls2(**kwargs2))

    assert_ne(cls(**kwargs1), None)

    for k in kwargs1:
        k1 = kwargs1.copy()
        k1[k] = kwargs2[k]
        assert_ne(cls(**kwargs1), cls2(**k1))
        assert_ne(cls(**k1), cls2(**kwargs1))

    if assert_subclass:

        class SubCls(cls):  # type: ignore[misc,valid-type]
            pass

        assert_eq_ne(SubCls, kwargs1, kwargs2, assert_subclass=False, cls2=SubCls)
        assert_eq_ne(cls, kwargs1, kwargs2, assert_subclass=False, cls2=SubCls)
        assert_eq_ne(SubCls, kwargs1, kwargs2, assert_subclass=False, cls2=cls)


class TestEntityInstantiation:
    def test_status_enum(self) -> None:
        assert StatusEnum.INITIAL == "INITIAL"
        assert StatusEnum.PENDING == "PENDING"
        assert StatusEnum.RUNNING == "RUNNING"
        assert StatusEnum.COMPLETED == "COMPLETED"
        assert StatusEnum.FAILED == "FAILED"

    def test_job(self, fx_faker: Faker) -> None:
        kwargs = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
            "created_time": fx_faker.date_time(),
        }

        job = Job(**kwargs)  # type: ignore[arg-type]
        assert job.job_type == kwargs["job_type"]
        assert job.job_id == kwargs["job_id"]
        assert job.job_body == kwargs["job_body"]
        assert job.nice == kwargs["nice"]
        assert job.created_time == kwargs["created_time"]

    def test_job_eq(self, fx_faker: Faker) -> None:
        kwargs1 = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
            "created_time": fx_faker.date_time(),
        }
        kwargs2 = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
            "created_time": fx_faker.date_time(),
        }
        assert_eq_ne(Job, kwargs1, kwargs2)

    def test_job_repr(self) -> None:
        job = Job(
            job_type="job_type",
            job_id="job_id",
            job_body=JobBody("job_body"),
            created_time=dt.datetime(1999, 1, 23, tzinfo=dt.timezone.utc),
            nice=0,
        )
        assert repr(job) == (
            "Job(job_type='job_type', job_id='job_id', "
            "job_body='job_body', "
            "created_time=1999-01-23T00:00:00+00:00, nice=0)"
        )

        class SubJob(Job):
            pass

        sub_job = SubJob(
            job_type="job_type",
            job_id="job_id",
            job_body=JobBody("job_body"),
            created_time=dt.datetime(1999, 1, 23, tzinfo=dt.timezone.utc),
            nice=0,
        )
        assert repr(sub_job) == (
            "SubJob(job_type='job_type', job_id='job_id', "
            "job_body='job_body', "
            "created_time=1999-01-23T00:00:00+00:00, nice=0)"
        )

    def test_serialized_job(self, fx_faker: Faker) -> None:
        kwargs = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body_serialized": fx_faker.job_body_serialized(),
            "created_time": fx_faker.date_time(),
            "nice": fx_faker.pyint(),
        }

        serialized_job = SerializedJob(**kwargs)  # type: ignore[arg-type]
        assert serialized_job.job_type == kwargs["job_type"]
        assert serialized_job.job_id == kwargs["job_id"]
        assert serialized_job.job_body_serialized == kwargs["job_body_serialized"]
        assert serialized_job.nice == kwargs["nice"]
        assert serialized_job.created_time == kwargs["created_time"]

    def test_serialized_job_eq(self, fx_faker: Faker) -> None:
        kwargs1 = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body_serialized": fx_faker.job_body_serialized(),
            "created_time": fx_faker.date_time(),
            "nice": fx_faker.pyint(),
        }
        kwargs2 = {
            "job_type": fx_faker.job_type(),
            "job_id": fx_faker.uuid4(),
            "job_body_serialized": fx_faker.job_body_serialized(),
            "created_time": fx_faker.date_time(),
            "nice": fx_faker.pyint(),
        }
        assert_eq_ne(SerializedJob, kwargs1, kwargs2)

    def test_job_serialized_repr(self) -> None:
        job = SerializedJob(
            job_type="job_type",
            job_id="job_id",
            job_body_serialized=SerializedJobBody(b"job_body"),
            created_time=dt.datetime(1999, 1, 23, tzinfo=dt.timezone.utc),
            nice=0,
        )
        assert repr(job) == (
            "SerializedJob(job_type='job_type', "
            "job_id='job_id', job_body_serialized=b'job_body', "
            "created_time=1999-01-23T00:00:00+00:00, "
            "nice=0)"
        )

        class Sub(SerializedJob):
            pass

        sub = Sub(
            job_type="job_type",
            job_id="job_id",
            job_body_serialized=SerializedJobBody(b"job_body"),
            created_time=dt.datetime(1999, 1, 23, tzinfo=dt.timezone.utc),
            nice=0,
        )
        assert repr(sub) == (
            "Sub(job_type='job_type', job_id='job_id', "
            "job_body_serialized=b'job_body', "
            "created_time=1999-01-23T00:00:00+00:00, "
            "nice=0)"
        )

    def test_job_status(self, fx_faker: Faker) -> None:
        job_status = JobStatus(
            status_id=(status_id := fx_faker.uuid4()),
            job_id=(job_id := fx_faker.uuid4()),
            issue_time=(issue_time := fx_faker.date_time()),
            status=(status := fx_faker.random_element(StatusEnum)),
            detail=(detail := fx_faker.sentence()),
        )
        assert job_status.status_id == status_id
        assert job_status.job_id == job_id
        assert job_status.issue_time == issue_time
        assert job_status.status == status
        assert job_status.detail == detail

    def test_job_status_eq(self, fx_faker: Faker) -> None:
        status1, status2 = fx_faker.random_elements(list(StatusEnum), 2, unique=True)
        kwargs1 = {
            "status_id": fx_faker.uuid4(),
            "job_id": fx_faker.uuid4(),
            "issue_time": fx_faker.date_time(),
            "status": status1,
            "detail": fx_faker.sentence(),
            "result": fx_faker.pyint(),
        }
        kwargs2 = {
            "status_id": fx_faker.uuid4(),
            "job_id": fx_faker.uuid4(),
            "issue_time": fx_faker.date_time(),
            "status": status2,
            "detail": fx_faker.sentence(),
            "result": fx_faker.pyint(),
        }
        assert_eq_ne(JobStatus, kwargs1, kwargs2)

    def test_job_status_repr(self) -> None:
        job_status = JobStatus(
            status_id="status_id",
            job_id="job_id",
            issue_time=dt.datetime(1999, 1, 1, tzinfo=dt.timezone.utc),
            status=StatusEnum.INITIAL,
            detail="detail",
        )
        assert repr(job_status) == (
            "JobStatus(status_id='status_id', job_id='job_id', "
            "issue_time=1999-01-01T00:00:00+00:00, "
            "status='INITIAL', "
            "detail='detail')"
        )

        class SubJobStatus(JobStatus):
            pass

        sub_job_status = SubJobStatus(
            status_id="status_id",
            job_id="job_id",
            issue_time=dt.datetime(1999, 1, 1, tzinfo=dt.timezone.utc),
            status=StatusEnum.INITIAL,
            detail="detail",
        )
        assert repr(sub_job_status) == (
            "SubJobStatus(status_id='status_id', "
            "job_id='job_id', "
            "issue_time=1999-01-01T00:00:00+00:00, "
            "status='INITIAL', detail='detail')"
        )

    @pytest.mark.parametrize("set_default", [None, "nice"])
    def test_new_job_quest(self, fx_faker: Faker, set_default: str | None) -> None:
        kwargs = {
            "job_type": fx_faker.job_type(),
            "job_body": fx_faker.job_body(),
            "nice": fx_faker.pyint(),
        }
        if set_default == "nice":
            del kwargs["nice"]

        new_job_request = NewJobRequest(**kwargs)  # type: ignore[arg-type]
        assert new_job_request.job_type == kwargs["job_type"]
        assert new_job_request.job_body == kwargs["job_body"]
        assert new_job_request.nice == kwargs.get("nice", 0)

    @pytest.mark.parametrize("set_default", [None, "nice"])
    def test_new_serialized_job_quest(
        self, fx_faker: Faker, set_default: str | None
    ) -> None:
        kwargs = {
            "job_type": fx_faker.job_type(),
            "job_body_serialized": fx_faker.job_body_serialized(),
            "nice": fx_faker.pyint(),
        }
        if set_default == "nice":
            del kwargs["nice"]

        request = NewSerializedJobRequest(**kwargs)  # type: ignore[arg-type]
        assert request.job_type == kwargs["job_type"]
        assert request.job_body_serialized == kwargs["job_body_serialized"]
        assert request.nice == kwargs.get("nice", 0)

    @pytest.mark.parametrize("set_default", [None, "issue_time"])
    def test_new_job_status_request(
        self, fx_faker: Faker, set_default: str | None
    ) -> None:
        kwargs = {
            "job_id": fx_faker.uuid4(),
            "status": fx_faker.random_element(StatusEnum),
            "issue_time": fx_faker.date_time(),
            "detail": fx_faker.sentence(),
        }
        if set_default == "issue_time":
            del kwargs["issue_time"]

        new_job_status_request = NewJobStatusRequest(**kwargs)  # type: ignore[arg-type]
        assert new_job_status_request.job_id == kwargs["job_id"]
        assert new_job_status_request.status == kwargs["status"]
        assert new_job_status_request.detail == kwargs["detail"]
        assert new_job_status_request.issue_time == kwargs.get("issue_time")

    @pytest.mark.parametrize("set_default", [None, "issue_time"])
    def test_new_serialized_job_status_request(
        self, fx_faker: Faker, set_default: str | None
    ) -> None:
        kwargs = {
            "job_id": fx_faker.uuid4(),
            "status": fx_faker.random_element(StatusEnum),
            "issue_time": fx_faker.date_time(),
            "detail": fx_faker.sentence(),
        }
        if set_default == "issue_time":
            del kwargs["issue_time"]

        new_job_status_request = NewJobStatusRequest(**kwargs)  # type: ignore[arg-type]
        assert new_job_status_request.job_id == kwargs["job_id"]
        assert new_job_status_request.status == kwargs["status"]
        assert new_job_status_request.detail == kwargs["detail"]
        assert new_job_status_request.issue_time == kwargs.get("issue_time")


class TestJobSerializer:
    def test_job_serializer(self) -> None:
        class MyJobSerializer(JobSerializer):
            @overload
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                pass

            @overload
            def serialize(self, job_body: Result) -> SerializedResult:
                pass

            def serialize(
                self, job_body: JobBody | Result
            ) -> SerializedJobBody | SerializedResult:
                raise NotImplementedError

            @overload
            def deserialize(self, serialized: SerializedJobBody) -> JobBody:
                pass

            @overload
            def deserialize(self, serialized: SerializedResult) -> Result:
                pass

            def deserialize(
                self, serialized: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                raise NotImplementedError

        job_serializer = MyJobSerializer()
        assert job_serializer is not None

    def test_job_serializer_should_implement_serialize(self) -> None:
        class MyJobSerializer(JobSerializer):
            @overload
            def deserialize(self, serialized: SerializedJobBody) -> JobBody:
                pass

            @overload
            def deserialize(self, serialized: SerializedResult) -> Result:
                pass

            def deserialize(
                self, serialized: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                raise NotImplementedError

        with pytest.raises(TypeError) as e:
            MyJobSerializer()  # type: ignore[abstract]
        assert e.match(
            "Can't instantiate abstract class "
            "MyJobSerializer with abstract method serialize"
        )

    def test_job_serializer_should_implement_deserialize(self) -> None:
        class MyJobSerializer(JobSerializer):
            @overload
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                pass

            @overload
            def serialize(self, job_body: Result) -> SerializedResult:
                pass

            def serialize(
                self, job_body: JobBody | Result
            ) -> SerializedJobBody | SerializedResult:
                raise NotImplementedError

        with pytest.raises(TypeError) as e:
            MyJobSerializer()  # type: ignore[abstract]
        assert e.match(
            "Can't instantiate abstract class "
            "MyJobSerializer with abstract method deserialize"
        )


class TestSerialization:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.faker = fx_faker

    def test_serialized_job_serialization(self) -> None:
        job = self.faker.serialized_job()
        d = serializer.packb(job.get_serializable())
        assert_eq(job, SerializedJob.from_serializable(serializer.unpackb(d)))
