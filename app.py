from utils.full_backup import run_full_backup
from utils.jobs_accessor import get_jobs
from dotenv import dotenv_values
import sys
import math
import re

__version__ = "0.0.2"

env_values = dotenv_values('.env')
source_connection = env_values['source_conn']
target_database =  env_values['target_sqlite_db']
out = sys.stdout
injection_detection_r = r"(\s*([\0\b\'\"\n\r\t\%\_\\]*\s*(((select\s*.+\s*from\s*.+)|(insert\s*.+\s*into\s*.+)|(update\s*.+\s*set\s*.+)|(delete\s*.+\s*from\s*.+)|(drop\s*.+)|(truncate\s*.+)|(alter\s*.+)|(exec\s*.+)|(\s*(all|any|not|and|between|in|like|or|some|contains|containsall|containskey)\s*.+[\=\>\<=\!\~]+.+)|(let\s+.+[\=]\s*.*)|(begin\s*.*\s*end)|(\s*[\/\*]+\s*.*\s*[\*\/]+)|(\s*(\-\-)\s*.*\s+)|(\s*(contains|containsall|containskey)\s+.*)))(\s*[\;]\s*)*)+)"
 
def input_selection(options):
    opt = -1
    max_allowed = len(options)+1
    
    for (i, o) in enumerate(options):
        print((i+1), '|', o['name'])
    print('0 | Exit', end='\n\n')

    while (opt < 0 or opt>=max_allowed):
        try: opt = int(input('please choose your option:'))
        except: opt = -1
        
        if opt < 0 or opt >= max_allowed:
            print("Invalid Input, Try again")

    return opt

def validate(jobs:list[dict]):
    """validates values"""
    for job in jobs:
        source = job['source']
        destination = job['destination']
        for (k,v) in job.items():
            if k == 'source' or k == 'destination':
                for (_,vv) in v.items():
                    if re.match(injection_detection_r,vv) is not None:
                        print("SQL Injection Detected!")
                        return job
            else:
                if re.match(injection_detection_r,v) is not None:
                    print("SQL Injection Detected!")
                    return job
        
        if re.fullmatch(r'([\w\-]){1,30}',job['name']) is None:
            return job
        if hasKey(job, 'type') and re.fullmatch(r'(full|part){1}', job['type']) is None:
            return job
        if re.fullmatch(r'(\w|\.){1,30}', source['table']) is None:
            return job
        if hasKey(source, 'columns') and re.fullmatch(r'((\w)+(\s)*,?(\s)*)+', source['columns']) is None:
            return job
        if re.fullmatch(r'\w{1,30}', destination['table']) is None:
            return job             
    return None
           

def hasKey(obj, key) -> bool:
    return key in obj.keys()

def main():
    """entry point"""
    jobs = get_jobs()
    jobs = [j for j in jobs if hasKey(j,'disabled') == False or j['disabled'] == False]
    
    if len(jobs) == 0:
        print("No jobs to load. Please add at least one job to proceed further.")
        return

    print(len(jobs), "Job(s) Loaded successfully.", end='\n\n')

    opt = input_selection(jobs)

    if opt==0:
        print("Goodbye.")
        return
    
    job = jobs[opt-1]
    
    bad_job = validate([job])
    if bad_job is not None:
        print(f'Invalid job {bad_job['name']}.',
               'Please make sure all jobs are configured correctly.')
        return

    source_tbl = job['source']['table']
    dest_tbl = job['destination']['table']
    columns = job['source']['columns'] if hasKey(job['source'], 'columns') else None
    if(hasKey(job, 'type') == False or job['type'] == 'full'):
        print("Full Backup may take several minutes to finish, Please wait until the job completes. :)")
        for (total_rows_count, completed) in run_full_backup(
                source_connection_str=source_connection,
                target_database = target_database,
                table_to_backup = source_tbl,
                target_table = dest_tbl,
                table_columns_to_backup= columns):
            print(f'{job['name']}: {math.ceil((completed/total_rows_count)*100)}%', "completed.", end='\r', file=out, flush=True)
        print('\n')
        print("Job Completed Successfully.")
    else:
        print("Job Type is not supported!")
    
    print("Goodbye.")

if __name__ == '__main__':
    main()