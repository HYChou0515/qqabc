from __future__ import annotations

import datetime as dt
import json
import pickle  # noqa: S403
from typing import TYPE_CHECKING, Any, overload

import pytest

from qqabc import JobQueueController, JobSerializer, JobSerializerRegistry
from qqabc.types import (
    NO_RESULT,
    EmptyQueueError,
    Job,
    JobBody,
    NewJobRequest,
    NewJobStatusRequest,
    Result,
    SerializedJobBody,
    SerializedResult,
    StatusEnum,
)

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory

    from tests.fixtures.faker import Faker


@pytest.fixture
def job_serializer_registry() -> JobSerializerRegistry:
    return JobSerializerRegistry()


class TestJobSerializer:
    def test_register_job_serializer(
        self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry
    ) -> None:
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
                return SerializedJobBody(b"")

            def deserialize(
                self, serialized_job_body: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return JobBody(object())

        job_serializer = MyJobSerializer()
        returned = job_serializer_registry.register_job_serializer(
            job_serializer, job_type=fx_faker.job_type()
        )  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(
        self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry
    ) -> None:
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
                return fx_faker.job_body_serialized()

            def deserialize(
                self, serialized_job_body: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return fx_faker.job_body()

        job_type = fx_faker.job_type()
        job_serializer = MyJobSerializer()
        job_serializer_registry.register_job_serializer(
            job_serializer, job_type=job_type
        )
        returned = job_serializer_registry.get_job_serializer(job_type=job_type)
        assert returned is job_serializer

    def test_get_unregistered_job_serializer_raises_key_error(
        self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry
    ) -> None:
        job_type = fx_faker.job_type()
        with pytest.raises(KeyError) as e:
            job_serializer_registry.get_job_serializer(job_type=job_type)
        assert e.match(job_type)


def test_job_queue_controller_can_be_instantiated() -> None:
    controller = JobQueueController()
    assert controller is not None


class MathJobBody:
    def __init__(self, *args: Any, op: str, a: int, b: int, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.op = op
        self.a = a
        self.b = b


class TestJobController:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:  # noqa: C901
        self.fx_faker = fx_faker

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
                return fx_faker.job_body_serialized()

            def deserialize(
                self, serialized_job_body: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return fx_faker.job_body()

        class MathJobSerializer(JobSerializer):
            @overload
            def serialize(self, job_body: JobBody) -> SerializedJobBody:
                pass

            @overload
            def serialize(self, job_body: Result) -> SerializedResult:
                pass

            def serialize(
                self, job_body: JobBody | Result
            ) -> SerializedJobBody | SerializedResult:
                return SerializedJobBody(
                    json.dumps(
                        {
                            "op": job_body.op,
                            "a": job_body.a,
                            "b": job_body.b,
                        }
                    ).encode()
                )

            def deserialize(
                self, serialized_job_body: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                data = json.loads(serialized_job_body.decode())
                return JobBody(
                    MathJobBody(
                        op=data["op"],
                        a=data["a"],
                        b=data["b"],
                    )
                )

            def serialize_result(self, result: Result) -> SerializedResult:
                return SerializedResult(json.dumps(result).encode())

            def deserialize_result(self, serialized_result: SerializedResult) -> Result:
                return Result(json.loads(serialized_result.decode()))

        self.job_controller = JobQueueController()
        self.my_job_type = fx_faker.job_type()
        self.math_job_type = fx_faker.job_type()
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=self.my_job_type,
            job_serializer=MyJobSerializer(),
        )
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=self.math_job_type,
            job_serializer=MathJobSerializer(),
        )

    def _add_new_job_request_of_my_job_1(self) -> Job:
        req = NewJobRequest(
            job_type=self.my_job_type,
            job_body=self.fx_faker.job_body(),
        )
        job = self.job_controller.add_job(req)
        assert job.job_body == req.job_body
        assert job.job_type == req.job_type
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

    def test_serialize_job(self) -> None:
        job1 = self.job_controller.add_job(
            self.fx_faker.new_job_request(
                job_type=self.math_job_type,
                job_body=JobBody(MathJobBody(op="add", a=1, b=2)),
            )
        )
        job2 = self.job_controller.get_job(job1.job_id)
        assert job1.job_body.op == job2.job_body.op
        assert job1.job_body.a == job2.job_body.a
        assert job1.job_body.b == job2.job_body.b

    def test_serialize_job_invalid_object(self) -> None:
        with pytest.raises(AttributeError) as m:
            self.job_controller.add_job(
                self.fx_faker.new_job_request(
                    job_type=self.math_job_type, job_body=JobBody(object())
                )
            )
        assert m.match("object has no attribute 'op'")


class TestJobConsumer:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker
        self.job_controller = JobQueueController()

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
                return SerializedJobBody(pickle.dumps(job_body))

            def deserialize(
                self, serialized_job_body: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return pickle.loads(serialized_job_body)  # noqa: S301

        self.job_type = fx_faker.job_type()
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=self.job_type,
            job_serializer=MyJobSerializer(),
        )

    def test_return_job_result(self, freezer: FrozenDateTimeFactory) -> None:
        now = self.fx_faker.date_time(tzinfo=dt.timezone.utc)
        freezer.move_to(now)
        job = self.job_controller.add_job(
            self.fx_faker.new_job_request(
                job_type=self.job_type,
            )
        )
        status_request = NewJobStatusRequest(
            job_id=job.job_id,
            status=StatusEnum.COMPLETED,
            detail="Job completed successfully",
            result=Result("my result"),
        )
        status = self.job_controller.add_job_status(status_request)
        assert status.job_id == status_request.job_id
        assert status.issue_time == now
        assert status.status == status_request.status
        assert status.detail == status_request.detail
        assert status.result == status_request.result

    def test_get_job_result_of_non_existed_job(self) -> None:
        with pytest.raises(KeyError) as e:
            self.job_controller.get_latest_status(job_id := self.fx_faker.uuid4())
        assert e.match(job_id)

    def test_get_job_result_of_no_status_job(self) -> None:
        job = self.job_controller.add_job(
            self.fx_faker.new_job_request(job_type=self.job_type)
        )
        assert self.job_controller.get_latest_status(job.job_id) is None

    def test_get_job_result(self) -> None:
        job = self.job_controller.add_job(
            self.fx_faker.new_job_request(job_type=self.job_type)
        )
        status1 = self.job_controller.add_job_status(
            self.fx_faker.new_status_request(job_id=job.job_id, result=Result(123))
        )
        status2 = self.job_controller.get_latest_status(job.job_id)
        assert status2 is not None
        assert status1 == status2

    def test_add_job_status_no_result(self) -> None:
        job = self.job_controller.add_job(
            self.fx_faker.new_job_request(job_type=self.job_type)
        )
        status1 = self.job_controller.add_job_status(
            self.fx_faker.new_status_request(job_id=job.job_id, result=NO_RESULT)
        )
        status2 = self.job_controller.get_latest_status(job.job_id)
        assert status2 is not None
        assert status2.result == NO_RESULT
        assert status1 == status2
