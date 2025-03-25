from pytest_bdd import scenario, given, when, then
from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli
from tests.utils import assert_result_success, get_stdout

@scenario('submit_job.feature', '送出一個job')
def test_job_submission_送出一個job():
    pass

@given("我有一個job", target_fixture="job_file_path")
def 我有一個job(fx_workdir):
    return create_a_job_file(fx_workdir)

@when("我送出這個job", target_fixture="job_id")
def 我送出這個job(job_file_path):
    r = create_a_job_online(job_file_path)
    assert_result_success(r)
    return get_job_id_by_submission_return(r)

@then("我可以在job list裡面看到這個job")
def 我可以在job_list裡面看到這個job(job_id):
    r = qqabc_cli(["get", "jobs"])
    assert_result_success(r)
    assert job_id in get_stdout(r)
