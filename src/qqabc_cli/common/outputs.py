from __future__ import annotations

import contextlib
import contextvars
import os
import os.path as osp
import sys
from abc import ABC
from typing import TYPE_CHECKING

import typer

from qqabc.application.domain.model.job import (
    JobResult,
    SerializedJob,
)
from qqabc_cli.common.console import (
    err_console,
    info_console,
    warn_console,
)

if TYPE_CHECKING:
    from collections.abc import Generator


class IPiper(ABC):
    def pipe_to_stdout(self, obj: JobResult | SerializedJob) -> None:
        raise NotImplementedError

    def pipe_to_dir(self, obj: JobResult | SerializedJob, dirpath: str) -> None:
        raise NotImplementedError

    def pipe_to_file(self, obj: JobResult | SerializedJob, filepath: str) -> None:
        raise NotImplementedError


class Piper(IPiper):
    def __init__(self) -> None:
        super().__init__()
        self._pipe_to_file_msg_context: contextvars.ContextVar[str | None] = (
            contextvars.ContextVar(
                "pipe_to_file_msg_context",
                default=None,
            )
        )

    @contextlib.contextmanager
    def _ctx_pipe_to_file_msg(self, msg: str) -> Generator[None]:
        token = self._pipe_to_file_msg_context.set(msg)
        try:
            yield
        finally:
            self._pipe_to_file_msg_context.reset(token)

    def _get_pipe_to_file_msg(self, *, rpath: str) -> str | None:
        if (msg := self._pipe_to_file_msg_context.get()) is None:
            return None
        return msg.format(rpath=rpath)

    def _get_bytes(self, obj: JobResult | SerializedJob) -> bytes:
        if isinstance(obj, JobResult):
            return obj.serialized_result
        if isinstance(obj, SerializedJob):
            return obj.job_body_serialized
        raise NotImplementedError(f"Unsupported type: {type(obj)}")

    def _get_file_msg(self, obj: JobResult | SerializedJob) -> str:
        if isinstance(obj, JobResult):
            return "Result downloaded into {rpath}"
        if isinstance(obj, SerializedJob):
            return "Job consumed into {rpath}"
        raise NotImplementedError(f"Unsupported type: {type(obj)}")

    def pipe_to_stdout(self, obj: JobResult | SerializedJob) -> None:
        return self._pipe_to_stdout(self._get_bytes(obj))

    def _pipe_to_stdout(self, b: bytes) -> None:
        sys.stdout.buffer.write(b)

    def pipe_to_dir(self, obj: JobResult | SerializedJob, dirpath: str) -> None:
        b = self._get_bytes(obj)
        msg = self._get_file_msg(obj)
        if isinstance(obj, JobResult):
            fpath = f"{obj.result_id}.result"
        elif isinstance(obj, SerializedJob):
            fpath = f"{obj.job_id}.job"
        else:
            raise NotImplementedError(f"Unsupported type: {type(obj)}")
        with self._ctx_pipe_to_file_msg(msg):
            self._pipe_to_dir(b, dirpath, fpath)

    def _pipe_to_dir(self, b: bytes, dirpath: str, fname: str) -> None:
        if osp.exists(dirpath) and not osp.isdir(dirpath):
            err_console.print(f"Dir is not valid: {dirpath}")
            err_console.print(f"Error: {dirpath} is not a directory")
            raise typer.Exit(code=2)
        if not osp.exists(dirpath):
            os.makedirs(dirpath)
            warn_console.print(f"Dir created: {dirpath}")
        fpath = f"{dirpath}/{fname}"
        return self._pipe_to_file(b, fpath)

    def pipe_to_file(self, obj: JobResult | SerializedJob, filepath: str) -> None:
        msg = self._get_file_msg(obj)
        with self._ctx_pipe_to_file_msg(msg):
            self._pipe_to_file(self._get_bytes(obj), filepath)

    def _pipe_to_file(self, b: bytes, filepath: str) -> None:
        with open(filepath, "wb") as f:
            f.write(b)
        rpath = osp.relpath(filepath, ".")
        if (msg := self._get_pipe_to_file_msg(rpath=rpath)) is not None:
            info_console.print(msg)
