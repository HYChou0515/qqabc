import os
import tempfile
import pytest
from pytest_bdd import scenario, given, when, then
import subprocess as sp

@pytest.fixture
def fx_workdir():
    """建立臨時資料夾，並切換到該資料夾"""
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        yield d

@scenario('submit_job.feature', '送出一個job')
def test_job_submission_送出一個job():
    pass

@given("我有一個job", target_fixture="job_file_path")
def 我有一個job(fx_workdir):
    """建立 job 檔案"""
    job_file_path = os.path.join(fx_workdir, "job.txt")

    with open(job_file_path, "w") as f:
        f.write("這是一個測試 job 檔案\n")

    return job_file_path

@when("我送出這個job", target_fixture="job_id")
def 我送出這個job(job_file_path):
    """送出 job"""
    with open(job_file_path, "rb") as f:
        r = sp.run(["python", "-m", "qqabc_cli", "submit", "job"], 
                input=f.read(),
                capture_output=True)
        assert r.returncode == 0
    for line in r.stdout.decode().splitlines():
        if "job id" in line:
            job_id = line.split()[-1]
            break
    return job_id

@then("我可以在job list裡面看到這個job")
def 我可以在job_list裡面看到這個job(job_id):
    """檢查 job 是否在 job list 中"""
    r = sp.run(
        ["python", "-m", "qqabc_cli", "get", "jobs"], 
        capture_output=True
    )
    assert r.returncode == 0
    assert job_id in r.stdout.decode()
