import tempfile
from pytest_bdd import scenario, given, when, then

from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli, update_status, upload_result, upload_result_by_data, upload_result_by_file

from tests.utils import assert_result_success, get_stdout


@scenario('upload_result.feature', '可以新增result from stdout')
def test_add_result_from_stdout():
    pass

@scenario('upload_result.feature', '可以新增result from file')
def test_add_result_from_file():
    pass

@scenario('upload_result.feature', '可以新增result from data')
def test_add_result_from_data():
    pass

@given("線上有一個job", target_fixture="job_id")
def step(fx_workdir: str):
    job_file = create_a_job_file(fx_workdir)
    r = create_a_job_online(job_file)
    job_id = get_job_id_by_submission_return(r)
    return job_id

@when("我upload result by stdout", target_fixture="result")
def step(job_id: str):
    r = upload_result(job_id, b"123")
    assert_result_success(r)
    return b"123"

@when("我upload result by file", target_fixture="result")
def step(job_id: str):
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"123")
        f.flush()
        r = upload_result_by_file(job_id, f.name)
        assert_result_success(r)
    return b"123"

@when("我upload result by data", target_fixture="result")
def step(job_id: str):
    r = upload_result_by_data(job_id, "123")
    assert_result_success(r)
    return b"123"

@when("我update status多次", target_fixture="status_and_detail")
def step(job_id: str):
    r = update_status(job_id, "running")
    assert_result_success(r)
    r = update_status(job_id, "fail")
    assert_result_success(r)
    r = update_status(job_id, "success")
    assert_result_success(r)
    return ("success", None)

@when("update status with detail", target_fixture="status_and_detail")
def step(job_id: str):
    r = update_status(job_id, "fail", "some detail")
    assert_result_success(r)
    return ("fail", "some detail")

@then("我可以看到這個result")
def step(job_id: str, result: bytes):
    r = qqabc_cli(["download", "result", "--job-id", job_id, "--to-stdout"])
    assert_result_success(r)
    stdout = get_stdout(r)
    assert result.decode() == stdout
