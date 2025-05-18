class SerializerNotFoundError(Exception):
    pass


class JobNotFoundError(Exception):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job with ID {job_id} not found.")
        self.job_id = job_id


class EmptyQueueError(Exception):
    pass
