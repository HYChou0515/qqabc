from pathlib import Path
from typing import Annotated
import typer
from qqabc import JobQueueController
from qqabc.types import EmptyQueueError, NewSerializedJobRequest

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
def pop(job_type: str, outdir: Annotated[str, typer.Option("-d")]):
    if not Path(outdir).exists():
        raise typer.BadParameter(f"Error: directory {outdir} does not exist")
    if not Path(outdir).is_dir():
        raise typer.BadParameter(f"Error: {outdir} is not a directory")

    controller = JobQueueController()
    try:
        sjob = controller.get_next_job(job_type, deserialize=False)
    except EmptyQueueError:
        raise typer.BadParameter(f"Error: No job with job type: {job_type}")
    output = Path(outdir) / f"{sjob.job_id}"
    with open(output, "wb") as f:
        f.write(sjob.job_body_serialized)

@app.command()
def _():
    pass
