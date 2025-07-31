import json
from typing import List
from utils.full_backup import run_full_backup
from utils.job_profile import JobProfile
import sys
import math
import logging
import argparse

__version__ = "0.1.3"
out = sys.stdout

# arg parse
parser = argparse.ArgumentParser()
parser.add_argument('-n','--name', metavar='name', dest='job_name', help='job name to run.')
parser.add_argument('-cc','--columns', action='store_true', dest='list_columns', help='list all columns from the job.')

# logger
logger = logging.getLogger(__name__)
def setup_logging():
    FORMAT = '%(asctime)s -  %(name)s -  %(levelname)s - %(message)s'
    
    logging.basicConfig(
        filename='app.log', 
        level=logging.INFO,
        format=FORMAT,
        force=True)
    
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
           
def main(args):
    """entry point"""
    setup_logging()
    logger.info(f'Started v{__version__}')
    logger.info(f"provided arguments {args}")
    
    profiles = JobProfile.load_profiles()
    profiles = [j for j in profiles if j.disabled == False]
    
    if len(profiles) == 0:
        logger.info("No jobs to load. Please add at least one job to proceed further.")
        return
    print(len(profiles), "Job(s) Loaded successfully.", end='\n\n')

    job:JobProfile = None
    if args is None:
        opt = input_selection(profiles)
        if opt==0:
            logger.info('Ended')
            return
        
        job = profiles[opt-1]
    else:
        if(args.job_name is not None):
            job_name = args.job_name
            jobs = [p for p in profiles if p.name == job_name]
            if len(jobs) == 0:
                logger.error(f"job '{job_name}' not found.")
                logger.info(f"Ended")
                return
            job = jobs.pop()
            if args.list_columns == True:
                (col_desc, columns) = job.source.list_columns_metadata()
                if col_desc is None:
                    print(columns)
                else:
                    desc = [i[0] for i in col_desc]
                    columns_metadata = []
                    for col in columns:
                        c_metadata = {}
                        for (i, col_property) in enumerate(col):
                            c_metadata[desc[i]] = str(col_property) if type(col_property) == type else col_property
                        columns_metadata.append(c_metadata)
                    print(json.dumps(columns_metadata, indent=4))
                logger.info(f"Ended")
                return
            else:
                """proceed with running a job"""
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
    main(args)