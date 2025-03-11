from pathlib import Path
import typer
from qqabc import JobQueueController
from qqabc.types import NewSerializedJobRequest

app = typer.Typer()


@app.command()
def submit(job_type: str, file_path: str):
    if not Path(file_path).exists():
        raise typer.BadParameter(f"Error: {file_path} does not exist")
    controller = JobQueueController()
    request = NewSerializedJobRequest(
        job_type=job_type,
        job_body_serialized=Path(file_path).read_bytes(),
    )
    job = controller.add_job(request)
    print("job submitted")
    print(f"job id = {job.job_id}")

@app.command()
def _():
    pass
