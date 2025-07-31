from prefect import flow, task
from prefect.artifacts import create_link_artifact
import app
from utils.args import Args
from prefect.logging import get_run_logger

@flow
def main(args):
    prefect_logger = get_run_logger()
    a = Args()
    supported_keys = ['job_name', 'list_columns']
    for arg_key in args.keys():
        if arg_key in supported_keys:
            setattr(a, 'job_name', args[arg_key])
        else:
            prefect_logger.warning(f'{arg_key} is not supported.')
    
    app.main(args=a)

