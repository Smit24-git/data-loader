from prefect import flow, task
from prefect.artifacts import create_link_artifact
import app


@flow
def main(job_name):
    app.main(job_name=job_name)

