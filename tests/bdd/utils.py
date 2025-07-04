from __future__ import annotations
import os
import subprocess as sp

def qqabc_cli(args: list[str], *, input: bytes = None):
    r = sp.run(["python", "-m", "qqabc_cli", *args], capture_output=True, input=input)
    return r

def create_a_job_file(workdir: str, *, job_content: str|None=None) -> str:
    job_file_path = os.path.join(workdir, "job.txt")

    with open(job_file_path, "w") as f:
        if job_content is not None:
            f.write(job_content)
        else:
            f.write("這是一個測試 job 檔案\n")
    return job_file_path

def _create_a_job_online_with_stdin(job_file_path: str):
    with open(job_file_path, "rb") as f:
        r = qqabc_cli(["submit", "job"], input=f.read())
    return r

def _create_a_job_online_with_file(job_file_path: str):
    r = qqabc_cli(["submit", "job", "--file", job_file_path])
    return r

def create_a_job_online(job_file_path: str, *, stdin=True):
    if stdin:
        r = _create_a_job_online_with_stdin(job_file_path)
    else:
        r = _create_a_job_online_with_file(job_file_path)
    return r

def update_status(job_id: str, status: str, detail: str = None):
    command = ["update", "status", status, "--job-id", job_id]
    if detail is not None:
        command.extend(["--detail", detail])
    r = qqabc_cli(command)
    return r

def upload_result(job_id: str, result: bytes):
    command = ["upload", "result", "--job-id", job_id, "--from-stdout"]
    r = qqabc_cli(command, input=result)
    return r

def upload_result_by_file(job_id: str, file_name: str):
    command = ["upload", "result", "--job-id", job_id, "--from-file", file_name]
    r = qqabc_cli(command)
    return r

def upload_result_by_data(job_id: str, data: str):
    command = ["upload", "result", "--job-id", job_id, "--from-data", data]
    r = qqabc_cli(command)
    return r

def get_job_id_by_submission_return(r: sp.CompletedProcess[bytes]):
    job_id = None
    for line in r.stdout.decode().splitlines():
        if "job id" in line:
            job_id = line.split()[-1]
            break
    else:
        raise RuntimeError(f"No job id found in submission return: {r.stdout.decode()}")
    return job_id
