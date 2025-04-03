from pytest_bdd import scenario, given, when, then

from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli

from tests.tdd.cli.utils import ALL_STATUS
from tests.utils import assert_result_success, assert_status, get_stdout


@scenario('post_status.feature', '可以新增一個status')
def test_post_one_status():
    pass
@scenario('post_status.feature', '可以新增第多個status')
def test_post_many_status():
    pass

@given("線上有一個job", target_fixture="job_id")
def step(fx_workdir: str):
    job_file = create_a_job_file(fx_workdir)
    r = create_a_job_online(job_file)
    job_id = get_job_id_by_submission_return(r)
    return job_id

@when("我post status", target_fixture="status")
def step(job_id: str):
    r = qqabc_cli(["update", job_id, "-s", "running"])
    assert_result_success(r)
    return "running"

@when("我post status多次", target_fixture="status")
def step(job_id: str):
    r = qqabc_cli(["update", job_id, "-s", "running"])
    assert_result_success(r)
    r = qqabc_cli(["update", job_id, "-s", "fail"])
    assert_result_success(r)
    r = qqabc_cli(["update", job_id, "-s", "success"])
    assert_result_success(r)
    return "success"

@then("我可以看到這個status")
@then("我可以看到最新的status")
def step(job_id: str, status: str):
    r = qqabc_cli(["get", "status", job_id])
    assert_result_success(r)
    stdout = get_stdout(r)
    assert_status(status, stdout)
