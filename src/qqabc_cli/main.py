from enum import Enum
from pathlib import Path
import sys
from typing import Annotated, Literal, Optional
import typer
from qqabc import JobQueueController
from qqabc.types import EmptyQueueError, NewJobStatusRequest, NewSerializedJobRequest, NewSerializedJobStatusRequest, StatusEnum

app = typer.Typer()

def _submit_to_queue(job_type: str, job_body: bytes):
    controller = JobQueueController()
    request = NewSerializedJobRequest(
        job_type=job_type,
        job_body_serialized=job_body,
    )
    job = controller.add_job(request)
    print("job submitted")
    print(f"job id = {job.job_id}")

def _submit_with_file(job_type: str, file_path: str):
    if not Path(file_path).exists():
        raise typer.BadParameter(f"Error: {file_path} does not exist")
    return _submit_to_queue(job_type, Path(file_path).read_bytes())

def _submit_with_stdin(job_type: str):
    return _submit_to_queue(job_type, sys.stdin.buffer.read())

@app.command()
def submit(job_type: str, file_path: Annotated[Optional[str], typer.Option("-f")] = None):
    if file_path is not None:
        return _submit_with_file(job_type, file_path)
    else:
        return _submit_with_stdin(job_type)

def _check_dir(d: str):
    if not Path(d).exists():
        raise typer.BadParameter(f"Error: directory {d} does not exist")
    if not Path(d).is_dir():
        raise typer.BadParameter(f"Error: {d} is not a directory")

def _pop_from_queue(job_type: str):
    controller = JobQueueController()
    try:
        sjob = controller.get_next_job(job_type, deserialize=False)
    except EmptyQueueError:
        raise typer.BadParameter(f"Error: No job with job type: {job_type}")
    return sjob

def _pop_to_dir(job_type: str, outdir: str):
    _check_dir(outdir)
    sjob = _pop_from_queue(job_type)
    output = Path(outdir) / f"{sjob.job_id}"
    with open(output, "wb") as f:
        f.write(sjob.job_body_serialized)

def _pop_to_stdout(job_type: str):
    sjob = _pop_from_queue(job_type)
    sys.stdout.buffer.write(sjob.job_body_serialized)

@app.command()
def pop(job_type: str, outdir: Annotated[Optional[str], typer.Option("-d")]=None):
    if outdir:
        return _pop_to_dir(job_type, outdir)
    else:
        return _pop_to_stdout(job_type)

class Status(str, Enum):
    process = "process"
    success = "success"
    fail = "fail"


def _map_status(status: Status) -> StatusEnum:
    return StatusEnum.RUNNING
    # if status == Status.process:
    #     return StatusEnum.RUNNING
    # if status == Status.success:
    #     return StatusEnum.SUCCESS
    # if status == Status.fail:
    #     return StatusEnum.FAIL
    # raise ValueError(f"Error: invalid status {status}")

@app.command()
def post(job_id: str, status: Annotated[Optional[Status], typer.Option("-s")]=None):
    controller = JobQueueController()
    req = NewSerializedJobStatusRequest(
        job_id=job_id,
        status=_map_status(status),
        detail="",
        result_serialized=sys.stdin.buffer.read(),
    )
    try:
        controller.add_job_status(req)
    except KeyError:
        raise typer.BadParameter(f"Error: job {job_id} does not exist")

class Resource(str, Enum):
    result = "result"

def _get_result(job_id: str):
    controller = JobQueueController()
    try:
        s_status = controller.get_latest_status(job_id, deserialize=False)
    except KeyError:
        raise typer.BadParameter(f"Error: job {job_id} does not exist")
    return s_status

@app.command()
def get(resource: Resource, job_id: str):
    if resource == Resource.result:
        s_status = _get_result(job_id)
        sys.stdout.buffer.write(s_status.result_serialized)
        return
    raise NotImplementedError
