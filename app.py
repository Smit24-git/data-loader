from utils.full_backup import run_full_backup
from utils.jobs_accessor import get_jobs
from utils.defaults.batch_size import default_batch_size
from dotenv import dotenv_values
import sys
import math
import re

from utils.source_data_accessor import SourceDataAccessor

__version__ = "0.0.4"

env_values = dotenv_values('.env')
source_connection = env_values['source_conn']
target_database =  env_values['target_sqlite_db']
out = sys.stdout
 
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
        
        # basic type checks
        if hasKey(job, 'batch_size') and isinstance(job['batch_size'], int) == False:
            return job

        # basic value boundary checks
        if hasKey(job, 'batch_size') and int(job['batch_size']) < 2000:
            return job

        # basic regex check
        if re.fullmatch(r'([\w\-]){1,30}',job['name']) is None:
            return job
        if hasKey(job, 'type') and re.fullmatch(r'(full|part){1}', job['type']) is None:
            return job
        if hasKey(source, 'table') and re.fullmatch(r'(\w|\.){1,30}', source['table']) is None:
            return job
        if hasKey(source, 'columns') and re.fullmatch(r'((\w)+(\s)*,?(\s)*)+', source['columns']) is None:
            return job
        if re.fullmatch(r'\w{1,30}', destination['table']) is None:
            return job             

        # validate against general sql syntax 
        if re.fullmatch(r'(create|alter|drop|delete|insert|update|drop)+[-*;]*', destination['table'].strip()) is not None:
            return job        

        # validate source against database
        if hasKey(source,'table'):
            srcAccessor = SourceDataAccessor(source_connection)
            tables:list[str] = srcAccessor.get_table_names()
            if source['table'] not in tables:
                return job

        if hasKey(source, 'columns'):
            columns = srcAccessor.get_columns_of(source['table'])
            user_entered_cols = [i.strip() for i in source['columns'].split(',')]
            if len(set(user_entered_cols).intersection(set(columns))) != len(user_entered_cols):
                return job

    return None
           

def hasKey(obj, key) -> bool:
    return key in obj.keys()

def get_query(type, job_name) -> str:
    with open(f'./commands/{job_name}/{type}.sql') as file:
        return file.read()

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

    source_query = get_query('source', job['name']) if hasKey(job['source'], 'from_file') and job['source']['from_file'] == True else None
    source_count_query = get_query('count', job['name']) if hasKey(job['source'], 'from_file') and job['source']['from_file'] == True else None
    source_tbl = job['source']['table'] if hasKey(job['source'], 'table') else None
    dest_tbl = job['destination']['table']
    columns = job['source']['columns'] if hasKey(job['source'], 'columns') else None
    batch_size = job['batch_size'] if hasKey(job, 'batch_size') else default_batch_size 
    if(hasKey(job, 'type') == False or job['type'] == 'full'):
        print("Full Backup may take several minutes to finish, Please wait until the job completes. :)")
        for (total_rows_count, completed) in run_full_backup(
                source_connection_str=source_connection,
                target_database = target_database,
                table_to_backup = source_tbl,
                target_table = dest_tbl,
                table_columns_to_backup = columns,
                source_query = source_query,
                source_count_query = source_count_query,
                batch_size = batch_size):
            print(f'{job['name']}: {math.ceil((completed/total_rows_count)*100)}%', "completed.", end='\r', file=out, flush=True)
        print('\n')
        print("Job Completed Successfully.")
    else:
        print("Job Type is not supported!")
    
    print("Goodbye.")

if __name__ == '__main__':
    main()