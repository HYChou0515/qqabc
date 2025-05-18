from glob import glob
import os
import tempfile
from random import randbytes
from pytest_bdd import scenario, given, when, then

from tests.bdd.utils import create_a_job_file, create_a_job_online, get_job_id_by_submission_return, qqabc_cli, update_status, upload_result, upload_result_by_data, upload_result_by_file

from tests.utils import assert_result_success


@scenario('upload_result.feature', '可以新增result from stdout')
def test_add_result_from_stdout():
    pass

@scenario('upload_result.feature', '可以新增result from file')
def test_add_result_from_file():
    pass

@scenario('upload_result.feature', '可以新增result from data')
def test_add_result_from_data():
    pass

@scenario('upload_result.feature', '可以下載result到stdout')
def test_download_result_to_stdout():
    pass

@scenario('upload_result.feature', '可以下載result到file')
def test_download_result_to_file():
    pass

@scenario('upload_result.feature', '預設下載result到cwd')
def test_download_result_to_cwd():
    pass

@scenario('upload_result.feature', '可以下載result到dir')
def test_download_result_to_dir():
    pass

@scenario('upload_result.feature', '有多個result時，預設下載最新的result')
def test_download_latest_result():
    pass

@scenario('upload_result.feature', '有多個result時，下載指定的result')
def test_download_specified_result():
    pass

@given("線上有一個job", target_fixture="job_id")
def step(fx_workdir: str):
    job_file = create_a_job_file(fx_workdir)
    r = create_a_job_online(job_file)
    job_id = get_job_id_by_submission_return(r)
    return job_id

@given("有多個result", target_fixture="results")
def step(job_id: str):
    results = [
        f"{i}==".encode() + randbytes(10) for i in range(3)
    ]
    for result in results:
        r = upload_result(job_id, result)
        assert_result_success(r)
    return results

@given("有一個result", target_fixture="result")
@when("我upload result by stdout", target_fixture="result")
def step(job_id: str):
    result = randbytes(10)
    r = upload_result(job_id, result)
    assert_result_success(r)
    return result

@when("我upload result by file", target_fixture="result")
def step(job_id: str):
    result = randbytes(10)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(result)
        f.flush()
        r = upload_result_by_file(job_id, f.name)
        assert_result_success(r)
    return result

@when("我upload result by data", target_fixture="result")
def step(job_id: str):
    r = upload_result_by_data(job_id, "123")
    assert_result_success(r)
    return b"123"

@when("我下載result to stdout", target_fixture="got_result")
@when("我下載result", target_fixture="got_result")
@when("下載result", target_fixture="got_result")
def step(job_id: str):
    r = qqabc_cli(["download", "result", "--job-id", job_id, "--to-stdout"])
    assert_result_success(r)
    return r.stdout

@when("我下載result指定第二個", target_fixture="got_result")
def step(job_id: str):
    r = qqabc_cli(["download", "result", "--job-id", job_id, "--to-stdout", "-k", "2"])
    assert_result_success(r)
    return r.stdout

@when("我下載result to file", target_fixture="got_result")
def step(job_id: str):
    with tempfile.NamedTemporaryFile() as f:
        r = qqabc_cli(["download", "result", "--job-id", job_id, "--to-file", f.name])
        assert_result_success(r)
        with open(f.name, "rb") as f:
            got_result = f.read()
    return got_result

@when("我下載result to dir", target_fixture="got_result")
def step(job_id: str):
    with tempfile.TemporaryDirectory() as d:
        r = qqabc_cli(["download", "result", "--job-id", job_id, "--to-dir", d])
        assert_result_success(r)
        assert len(files := os.listdir(d)) == 1, f"no files in {d}"
        fname = files[0]
        with open(os.path.join(d, fname), "rb") as f:
            got_result = f.read()
    return got_result

@when("我下載result(不指定dest)", target_fixture="got_result")
def step(job_id: str):
    r = qqabc_cli(["download", "result", "--job-id", job_id])
    assert_result_success(r)
    d = os.getcwd()
    files = glob(f"{d}/*.result")
    assert len(files) == 1, f"no files in {d}"
    fname = files[0]
    with open(os.path.join(d, fname), "rb") as f:
        got_result = f.read()
    return got_result

@then("我可以看到最新的result")
def step(results: tuple[bytes, ...], got_result: bytes):
    result = results[-1]
    assert result == got_result

@then("我可以看到第二個result")
def step(results: tuple[bytes, ...], got_result: bytes):
    result = results[-2]
    assert result == got_result

@then("我可以看到這個result")
def step(result: bytes, got_result: bytes):
    assert result == got_result
