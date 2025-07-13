import json


def get_jobs():
    jobs = []
    with open('job_profiles.json') as file:
        jobs = json.load(file)
    return jobs