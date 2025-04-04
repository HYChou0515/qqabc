from subprocess import CompletedProcess
from pytest_bdd import scenario, given, when, then

from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli, update_status

from tests.utils import assert_result_success, assert_status, get_stdout


@scenario('get_status.feature', '看到所有的status')
def test_get_all_status():
    pass

@given("job有很多status", target_fixture="job_id")
def step(fx_workdir: str):
    job_file = create_a_job_file(fx_workdir)
    r = create_a_job_online(job_file)
    job_id = get_job_id_by_submission_return(r)
    update_status(job_id, "running")
    update_status(job_id, "running")
    update_status(job_id, "success")
    update_status(job_id, "fail")
    return job_id

@when("我想看所有的status", target_fixture="get_status_result")
def step(job_id: str):
    r = qqabc_cli(["get", "status", job_id, "--all"])
    return r

@then("我可以看到所有的status")
def step(get_status_result: CompletedProcess[bytes]):
    assert_result_success(get_status_result)
    stdout = get_stdout(get_status_result)
    assert_status("running", stdout)
    assert_status("success", stdout)
    assert_status("fail", stdout)
