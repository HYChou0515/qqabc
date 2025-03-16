from qqabc.application.domain.model.job import JobBody, SerializedJobBody


class BaseNewJobRequest:
    def __init__(self, *, job_type: str, nice: int = 0) -> None:
        self.job_type = job_type
        self.nice = nice


class NewJobRequest(BaseNewJobRequest):
    def __init__(self, *, job_type: str, job_body: JobBody, nice: int = 0) -> None:
        super().__init__(job_type=job_type, nice=nice)
        self.job_body = job_body


class NewSerializedJobRequest(BaseNewJobRequest):
    def __init__(
        self, *, job_type: str, job_body_serialized: SerializedJobBody, nice: int = 0
    ) -> None:
        super().__init__(job_type=job_type, nice=nice)
        self.job_body_serialized = job_body_serialized
