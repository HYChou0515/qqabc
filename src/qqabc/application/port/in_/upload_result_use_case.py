from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt

    from qqabc.application.domain.model.job import (
        SerializedResult,
    )


class NewJobResultRequest:
    def __init__(
        self,
        *,
        job_id: str,
        issue_time: dt.datetime | None = None,
        result: SerializedResult,
    ) -> None:
        self.job_id = job_id
        self.issue_time = issue_time
        self.result = result
