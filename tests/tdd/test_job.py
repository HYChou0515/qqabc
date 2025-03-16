from __future__ import annotations

import datetime as dt
import json
import pickle
from typing import TYPE_CHECKING, Any, Literal, overload

import pytest
from typing_extensions import override

from qqabc import JobQueueController, JobSerializer, JobSerializerRegistry
from qqabc.application.domain.model.job import (
    NO_RESULT,
    QQABC,
    Job,
    JobBody,
    JobStatus,
    Result,
    SerializedJobBody,
    SerializedResult,
    StatusEnum,
)
from qqabc.application.port.in_.post_job_status_use_case import (
    NewJobStatusRequest,
)
from qqabc.application.port.in_.submit_job_use_case import NewJobRequest
from qqabc.exceptions import (
    EmptyQueueError,
    JobNotFoundError,
    SerializerNotFoundError,
)

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory

    from tdd.fixtures.faker import Faker


@pytest.fixture
def job_serializer_registry() -> JobSerializerRegistry:
    return JobSerializerRegistry()


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


class TestJobSerializer:
    def test_register_job_serializer(
        self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry
    ) -> None:
        job_serializer = MyJobSerializer()
        returned = job_serializer_registry.register_job_serializer(
            job_serializer, job_type=fx_faker.job_type()
        )  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(
        self, fx_faker: Faker, job_serializer_registry: JobSerializerRegistry
    ) -> None:
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
        with pytest.raises(SerializerNotFoundError, match=job_type):
            job_serializer_registry.get_job_serializer(job_type=job_type)


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
    def _register_my_job_serializer(self) -> None:
        fx_faker = self.fx_faker

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
                return SerializedJobBody(fx_faker.job_body_serialized())

            @overload
            def deserialize(self, serialized: SerializedJobBody) -> JobBody:
                pass

            @overload
            def deserialize(self, serialized: SerializedResult) -> Result:
                pass

            def deserialize(
                self, serialized: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return JobBody(fx_faker.job_body())

        self.my_job_type = self.fx_faker.job_type()

        self._register_serializer(
            job_type=self.my_job_type,
            serializer=MyJobSerializer(),
        )

    def _register_math_job_serializer(self) -> None:
        class MathJobSerializer(JobSerializer):
            @overload
            def serialize(self, job_body: MathJobBody) -> SerializedJobBody:
                pass

            @overload
            def serialize(self, job_body: Result) -> SerializedResult:
                pass

            @override
            def serialize(
                self, job_body: MathJobBody | Result
            ) -> SerializedJobBody | SerializedResult:
                if isinstance(job_body, MathJobBody):
                    return SerializedJobBody(
                        json.dumps(
                            {
                                "op": job_body.op,
                                "a": job_body.a,
                                "b": job_body.b,
                            }
                        ).encode()
                    )
                raise ValueError("job_body is not MathJobBody")

            @overload
            def deserialize(self, serialized: SerializedJobBody) -> MathJobBody:
                pass

            @overload
            def deserialize(self, serialized: SerializedResult) -> Result:
                pass

            def deserialize(
                self, serialized: SerializedJobBody | SerializedResult
            ) -> MathJobBody | Result:
                data = json.loads(serialized.decode())
                return MathJobBody(
                    op=data["op"],
                    a=data["a"],
                    b=data["b"],
                )

        self.math_job_type = self.fx_faker.job_type()

        self._register_serializer(
            job_type=self.math_job_type,
            serializer=MathJobSerializer(),
        )

    def _register_serializer(self, job_type: str, serializer: JobSerializer) -> None:
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=job_type,
            job_serializer=serializer,
        )

    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.fx_faker = fx_faker

        self.job_controller = JobQueueController()
        self._register_my_job_serializer()
        self._register_math_job_serializer()

    def _add_new_job_request_of_my_job_1(self) -> Job:
        req = NewJobRequest(
            job_type=self.my_job_type,
            job_body=JobBody(self.fx_faker.job_body()),
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
        with pytest.raises(JobNotFoundError, match=job_id):
            self.job_controller.get_job(job_id=job_id)

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
        with pytest.raises(EmptyQueueError, match=self.my_job_type):
            self.job_controller.get_next_job(job_type=self.my_job_type)

    def test_get_next_job_returns_the_job(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_next_job(job_type=self.my_job_type)
        self.assert_job_type(job)
        assert returned.nice == 0
        assert returned.job_id == job.job_id

    def test_add_new_serialized_job(self) -> None:
        job = self.job_controller.add_job(self.fx_faker.new_serialized_job_request())
        assert job is not None

    def test_get_next_sjob_returns_the_sjob(self) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_next_job(
            job_type=self.my_job_type, deserialize=False
        )
        self.assert_job_type(job)
        assert returned.nice == 0
        assert returned.job_id == job.job_id

    def test_serialize_job(self) -> None:
        job1: Job[MathJobBody] = self.job_controller.add_job(
            self.fx_faker.new_job_request(
                job_type=self.math_job_type,
                job_body=JobBody(MathJobBody(op="add", a=1, b=2)),
            )
        )
        job2: Job[MathJobBody] = self.job_controller.get_job(job1.job_id)
        assert job1.job_body.op == job2.job_body.op
        assert job1.job_body.a == job2.job_body.a
        assert job1.job_body.b == job2.job_body.b

    def test_serialize_job_invalid_object(self) -> None:
        with pytest.raises(ValueError, match="job_body is not MathJobBody"):
            self.job_controller.add_job(
                self.fx_faker.new_job_request(
                    job_type=self.math_job_type, job_body=JobBody(object())
                )
            )


class TestJobConsumer:
    @pytest.fixture(autouse=True)
    def setup_method(self, fx_faker: Faker) -> None:
        self.faker = fx_faker
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

            @overload
            def deserialize(self, serialized: SerializedJobBody) -> JobBody:
                pass

            @overload
            def deserialize(self, serialized: SerializedResult) -> Result:
                pass

            def deserialize(
                self, serialized: SerializedJobBody | SerializedResult
            ) -> JobBody | Result:
                return pickle.loads(serialized)

        self.job_type = fx_faker.job_type()
        self.job_controller.job_serializer_registry.register_job_serializer(
            job_type=self.job_type,
            job_serializer=MyJobSerializer(),
        )

    def test_return_job_result(self, freezer: FrozenDateTimeFactory) -> None:
        now = self.faker.date_time(tzinfo=dt.timezone.utc)
        freezer.move_to(now)
        job = self.job_controller.add_job(
            self.faker.new_job_request(
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
        job_id = self.faker.uuid4()
        with pytest.raises(JobNotFoundError, match=job_id):
            self.job_controller.get_latest_status(job_id)

    @pytest.mark.parametrize("pop_job", [True, False])
    def test_get_job_result_of_no_status_job(self, *, pop_job: bool) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        if pop_job:
            self.job_controller.get_next_job(job_type=self.job_type)
        assert self.job_controller.get_latest_status(job.job_id) is None

    def _assert_job_status_is_same_as_added(
        self,
        job: Job,
        *,
        with_result: bool,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        result: Result | Literal[QQABC.NO_RESULT]
        if with_result:
            result = Result(self.faker.json())
        else:
            result = NO_RESULT
        latest_status: tuple[dt.datetime | None, JobStatus | None] = None, None
        for _ in range(multiple_statuses):
            freezer.move_to(t := self.faker.date_time(tzinfo=dt.timezone.utc))
            s = self.job_controller.add_job_status(
                self.faker.new_status_request(job_id=job.job_id, result=result)
            )
            if latest_status[0] is None or t > latest_status[0]:
                latest_status = t, s
        status1 = latest_status[1]
        status2 = self.job_controller.get_latest_status(job.job_id)
        assert status2 is not None
        if with_result:
            assert status2.result == result
        else:
            assert status2.result == NO_RESULT
        assert status1 == status2

    @pytest.mark.parametrize("with_result", [True, False])
    @pytest.mark.parametrize("multiple_statuses", [1, 2, 100])
    def test_get_job_result(
        self,
        *,
        with_result: bool,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        self._assert_job_status_is_same_as_added(
            job,
            with_result=with_result,
            multiple_statuses=multiple_statuses,
            freezer=freezer,
        )

    @pytest.mark.parametrize("with_result", [True, False])
    @pytest.mark.parametrize("multiple_statuses", [1, 2, 100])
    def test_get_job_result_after_job_pop(
        self,
        *,
        with_result: bool,
        multiple_statuses: int,
        freezer: FrozenDateTimeFactory,
    ) -> None:
        job = self.job_controller.add_job(
            self.faker.new_job_request(job_type=self.job_type)
        )
        self.job_controller.get_next_job(job_type=self.job_type)
        self._assert_job_status_is_same_as_added(
            job,
            with_result=with_result,
            multiple_statuses=multiple_statuses,
            freezer=freezer,
        )
