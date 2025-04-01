import time
from pytest_bdd import scenario, given, when, then

from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli
import subprocess as sp

from tests.utils import assert_result_success, get_stdout, get_sterr


# @scenario('consume_job.feature', '可以吃job')
# def test_consume_job():
#     pass
# @scenario('consume_job.feature', '當job list裡面沒有job')
# def test_consume_job_no_job():
#     pass
@scenario('consume_job.feature', '當job list裡面有多個job')
def test_consume_job_multiple_jobs():
    pass

@given("線上有一個job", target_fixture="job_id")
def step(fx_workdir: str):
    job_file = create_a_job_file(fx_workdir)
    r = create_a_job_online(job_file)
    job_id = get_job_id_by_submission_return(r)
    return job_id

@given("job list裡面沒有job")
def step():
    return

@given("job list裡面有多個job", target_fixture="job_id")
def step(fx_workdir: str):
    for _ in range(3):
        job_file = create_a_job_file(fx_workdir)
        r = create_a_job_online(job_file)
        last_job_id = get_job_id_by_submission_return(r)
        time.sleep(0.1)
    return last_job_id


@when("我consume job", target_fixture="cusume_job_result")
def step():
    r = qqabc_cli(["pop"])
    return r

@then("我可以取得priority最高的job")
@then("我可以取得這個job")
def step(cusume_job_result: sp.CompletedProcess[bytes], job_id: str):    
    assert_result_success(cusume_job_result)
    stdout = get_stdout(cusume_job_result)
    stderr = get_sterr(cusume_job_result)
    assert job_id in stdout, f"ERR: {stderr}\nOUT: {stdout}"
    assert "Job consumed" in stdout
    assert f"into {job_id}.job" in stdout

@then("我不會取得job")
def step(cusume_job_result: sp.CompletedProcess[bytes]):
    assert_result_success(cusume_job_result)
    stdout = get_stdout(cusume_job_result)
    assert "no job" in stdout

@then("這個job不再存在於job list裡面")
def step(job_id: str):
    r = qqabc_cli(["get", "jobs"])
    assert job_id not in get_stdout(r)
