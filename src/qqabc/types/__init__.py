import abc


class JobBody(abc.ABC):
    pass


class SerializedJobBody(abc.ABC):
    pass


class Job:
    def __init__(self, *,
                 job_type: str,
                 job_id: str,
                 job_body: JobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body = job_body
        self.nice = nice


class SerializedJob:
    def __init__(self, *,
                 job_type: str,
                 job_id: str,
                 job_body_serialized: SerializedJobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_id = job_id
        self.job_body_serialized = job_body_serialized
        self.nice = nice


class NewJobRequest:
    def __init__(self, *,
                 job_type: str,
                 job_body: JobBody,
                 nice: int = 0) -> None:
        self.job_type = job_type
        self.job_body = job_body
        self.nice = nice
