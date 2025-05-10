from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, overload

import pytest
from typing_extensions import override

from qqabc.application.domain.model.job import (
    Job,
    JobBody,
    Result,
    SerializedJobBody,
    SerializedResult,
    SupportEq,
)
from qqabc.application.domain.service.job_serializer_registry import (
    JobSerializer,
    JobSerializerRegistry,
)
from qqabc.application.port.in_.submit_job_use_case import NewJobRequest
from qqabc.common.exceptions import (
    EmptyQueueError,
    JobNotFoundError,
    SerializerNotFoundError,
)
from tests.tdd.utils import MyJobSerializer

if TYPE_CHECKING:
    from qqabc_cli.di.in_ import Container
    from tests.tdd.fixtures.faker import Faker


class MathJobBody(SupportEq):
    def __init__(self, *args: Any, op: str, a: int, b: int, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.op = op
        self.a = a
        self.b = b


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


class TestJobSerializer:
    def test_register_job_serializer(
        self, fx_faker: Faker, fx_job_serializer_registry: JobSerializerRegistry
    ) -> None:
        job_serializer = MyJobSerializer()
        returned = fx_job_serializer_registry.register_job_serializer(
            job_serializer, job_type=fx_faker.job_type()
        )  # type: ignore[func-returns-value]
        assert returned is None

    def test_get_job_serializer(
        self, fx_faker: Faker, fx_job_serializer_registry: JobSerializerRegistry
    ) -> None:
        job_type = fx_faker.job_type()
        job_serializer = MyJobSerializer()
        fx_job_serializer_registry.register_job_serializer(
            job_serializer, job_type=job_type
        )
        returned = fx_job_serializer_registry.get_job_serializer(job_type=job_type)
        assert returned is job_serializer

    def test_get_unregistered_job_serializer_raises_key_error(
        self, fx_faker: Faker, fx_job_serializer_registry: JobSerializerRegistry
    ) -> None:
        job_type = fx_faker.job_type()
        with pytest.raises(SerializerNotFoundError, match=job_type):
            fx_job_serializer_registry.get_job_serializer(job_type=job_type)


class TestJobController:
    def _register_my_job_serializer(self) -> None:
        self.my_job_type = self.fx_faker.job_type()
        self._register_serializer(
            job_type=self.my_job_type,
            serializer=MyJobSerializer(),
        )

    def _register_math_job_serializer(self) -> None:
        self.math_job_type = self.fx_faker.job_type()
        self._register_serializer(
            job_type=self.math_job_type,
            serializer=MathJobSerializer(),
        )

    def _register_serializer(self, job_type: str, serializer: JobSerializer) -> None:
        self.job_serializer_registry.register_job_serializer(
            job_type=job_type,
            job_serializer=serializer,
        )

    @pytest.fixture(autouse=True)
    def setup_method(
        self,
        fx_faker: Faker,
        fx_test_container: Container,
    ) -> None:
        self.fx_faker = fx_faker

        self.job_controller = fx_test_container.job_queue_service()
        self.job_serializer_registry = fx_test_container.job_serializer_registry()
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

    def _add_new_job_request_of_math_job_1(self) -> Job:
        req = NewJobRequest(
            job_type=self.math_job_type,
            job_body=JobBody(
                MathJobBody(
                    op="add",
                    a=self.fx_faker.pyint(-10, 10),
                    b=self.fx_faker.pyint(20, 50),
                )
            ),
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

    @pytest.mark.parametrize("deserialize", [True, False])
    def test_get_next_job_from_empty_queue_raise_empty_queue_error(
        self, *, deserialize: bool
    ) -> None:
        with pytest.raises(EmptyQueueError, match=self.my_job_type):
            self.job_controller.get_next_job(
                job_type=self.my_job_type, deserialize=deserialize
            )

    @pytest.mark.parametrize("deserialize", [True, False])
    def test_get_next_job_returns_the_job(self, *, deserialize: bool) -> None:
        job = self._add_new_job_request_of_my_job_1()
        returned = self.job_controller.get_next_job(
            job_type=self.my_job_type, deserialize=deserialize
        )
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

    def test_list_jobs_from_nothing(self) -> None:
        assert self.job_controller.list_jobs() == []

    def test_list_jobs(self) -> None:
        job1 = self._add_new_job_request_of_math_job_1()
        job2 = self._add_new_job_request_of_math_job_1()
        jobs = self.job_controller.list_jobs(deserialize=True)
        assert len(jobs) == 2
        assert job1 in jobs
        assert job2 in jobs

    def test_serialize_job(self) -> None:
        job1: Job[MathJobBody] = self.job_controller.add_job(
            self.fx_faker.new_job_request(
                job_type=self.math_job_type,
                job_body=JobBody(MathJobBody(op="add", a=1, b=2)),
            )
        )
        job2: Job[MathJobBody] = self.job_controller.get_job(
            job1.job_id, deserialize=True
        )
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
