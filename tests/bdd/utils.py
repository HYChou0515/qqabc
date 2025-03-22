import os
import re
import subprocess as sp

def qqabc_cli(args: list[str], *, input: bytes = None):
    r = sp.run(["python", "-m", "qqabc_cli", *args], capture_output=True, input=input)
    return r

def assert_result_success(result: sp.CompletedProcess[bytes]):
    assert result.returncode == 0, result.stderr.decode() + result.stdout.decode()


def get_stdout(result: sp.CompletedProcess[bytes]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stdout.decode()))


def get_sterr(result:  sp.CompletedProcess[bytes]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\s╭─╮│╰╯]", " ", result.stderr.decode()))


def create_a_job_file(workdir: str):
    job_file_path = os.path.join(workdir, "job.txt")

    with open(job_file_path, "w") as f:
        f.write("這是一個測試 job 檔案\n")

    return job_file_path

def create_a_job_online(job_file_path: str):
    with open(job_file_path, "rb") as f:
        r = qqabc_cli(["submit", "job"], input=f.read())
    return r

def get_job_id_by_submission_return(r: sp.CompletedProcess[bytes]):
    for line in r.stdout.decode().splitlines():
        if "job id" in line:
            job_id = line.split()[-1]
            break
    return job_id
