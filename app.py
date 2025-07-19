from typing import List
from utils.full_backup import run_full_backup
from utils.job_profile import JobProfile
import sys
import math


__version__ = "0.0.4"

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

           



def main():
    """entry point"""
    profiles = JobProfile.load_profiles()
    profiles = [j for j in profiles if j.disabled == False]
    
    if len(profiles) == 0:
        print("No jobs to load. Please add at least one job to proceed further.")
        return

    print(len(profiles), "Job(s) Loaded successfully.", end='\n\n')

    opt = input_selection(profiles)

    if opt==0:
        print("Goodbye.")
        return
    
    job = profiles[opt-1]

    (is_success, msg) = job.validate()

    if is_success:
        if(job.type is None or job.type == 'full'):
            print("Full Backup may take several minutes to finish, Please wait until the job completes. :)")
            for (total_rows_count, completed) in run_full_backup(job):
                print(f'{job.name}: {math.ceil((completed/total_rows_count)*100)}%', "completed.", end='\r', file=out, flush=True)
            print('\n')
            print("Job Completed Successfully.")
        else:
            print("Job Type is not supported!")
    else:
        print(f'Invalid job {job.name}.\n',
               msg if msg is not None else 'Please make sure all jobs are configured correctly.',
               sep='')    
    print("Goodbye.")

if __name__ == '__main__':
    main()