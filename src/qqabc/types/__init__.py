import abc


class JobBody(abc.ABC):
    pass


class SerializedJobBody(abc.ABC):
    pass


class NewJobRequest:
    def __init__(self, *,
                 job_type: str,
                 job_body: JobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_body = job_body
        self.nice = nice
