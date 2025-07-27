from typing import List
from utils.full_backup import run_full_backup
from utils.job_profile import JobProfile
import sys
import math
import logging
import argparse
from prefect import task

__version__ = "0.1.3"
out = sys.stdout

# arg parse
parser = argparse.ArgumentParser()
parser.add_argument('-n','--name', metavar='name', dest='job_name', help='job name to run.')

# logger
logger = logging.getLogger(__name__)
def setup_logging():
    FORMAT = '%(asctime)s -  %(name)s -  %(levelname)s - %(message)s'
    
    logging.basicConfig(
        filename='app.log', 
        level=logging.INFO,
        format=FORMAT)
    
    # to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

 
def input_selection(options: List[JobProfile]):
    opt = -1
    max_allowed = len(options)+1
    
    for (i, o) in enumerate(options):
        print((i+1), '|', o.name)
    print('0 | Exit', end='\n\n')

    while (opt < 0 or opt>=max_allowed):
        try: opt = int(input('please choose your option:'))
        except: opt = -1
        
        if opt < 0 or opt >= max_allowed:
            print("Invalid Input, Try again")

    return opt
           
@task
def main(job_name):
    """entry point"""
    setup_logging()
    logger.info(f'Started v{__version__}')
    
    profiles = JobProfile.load_profiles()
    profiles = [j for j in profiles if j.disabled == False]
    
    if len(profiles) == 0:
        logger.info("No jobs to load. Please add at least one job to proceed further.")
        return
    print(len(profiles), "Job(s) Loaded successfully.", end='\n\n')

    job:JobProfile = None

    if job_name is None:
        opt = input_selection(profiles)
        if opt==0:
            logger.info('Ended')
            return
        
        job = profiles[opt-1]
    else:    
        jobs = [p for p in profiles if p.name == job_name]
        if len(jobs) == 0:
            logger.error(f"job '{job_name}' not found.")
            logger.info(f"Ended")
            return
        job = jobs.pop()
    
    (is_success, msg) = job.validate()
    
    logger.info(f'{job.name} job selected.')
    if is_success:
        if(job.type is None or job.type == 'full'):
            for (total_rows_count, completed) in run_full_backup(job):
                print(f'{job.name}: {math.ceil((completed/total_rows_count)*100)}%', "completed.", end='\r', file=out, flush=True)
            print('\n')
            logger.info(f'{job.name} completed successfully.')
        else:
            logger.error("Job Type is not supported!")
    else:
        logger.error(f'Invalid job {job.name}.\n',
               msg if msg is not None else 'Please make sure all jobs are configured correctly.')    
    logger.info('Finished')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args.job_name)