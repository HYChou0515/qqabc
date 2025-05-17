from typing import Literal, TypedDict


class JobDaoConfig(TypedDict):
    type: Literal["memory", "disk"]
    root_dir: str


class Config(TypedDict):
    debug: bool
    job_dao: JobDaoConfig


def get_default_config() -> Config:
    default_config = Config(
        debug=False,
        job_dao=JobDaoConfig(
            type="disk",
            root_dir=".qqabc/jobs",
        ),
    )
    return default_config
