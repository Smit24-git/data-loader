from typing import List
from utils.full_backup import run_full_backup
from utils.job_profile import JobProfile
import sys
import math
import logging

logger = logging.getLogger(__name__)

__version__ = "0.1.1"

out = sys.stdout
 
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



def main():
    """entry point"""
    setup_logging()
    logger.info('Started')
    profiles = JobProfile.load_profiles()
    profiles = [j for j in profiles if j.disabled == False]
    
    if len(profiles) == 0:
        logger.info("No jobs to load. Please add at least one job to proceed further.")
        return

    print(len(profiles), "Job(s) Loaded successfully.", end='\n\n')

    opt = input_selection(profiles)

    if opt==0:
        logger.info('Ended')
        return
    
    job = profiles[opt-1]
    (is_success, msg) = job.validate()
    
    logger.info(f'{job.name} job selected.')
    if is_success:
        if(job.type is None or job.type == 'full'):
            logger.info("Full Backup may take several minutes to finish, Please wait until the job completes. :)")
            for (total_rows_count, completed) in run_full_backup(job):
                print(f'{job.name}: {math.ceil((completed/total_rows_count)*100)}%', "completed.", end='\r', file=out, flush=True)
            print('\n')
            logger.info(f'{job.name} completed successfully.')
        else:
            logger.error("Job Type is not supported!")
    else:
        logger.error(f'Invalid job {job.name}.\n',
               msg if msg is not None else 'Please make sure all jobs are configured correctly.',
               sep='')    
    logger.info('Finished')

if __name__ == '__main__':    main()