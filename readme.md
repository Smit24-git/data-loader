# Data Loader
SQL data loader designed to extract and load from single sql database to lightweight sqlite database.

## Prerequisites
1. Python 3.13.5 (both 32 or 64 should work just fine)
2. Installed SQL Drivers required for the source database

## Setup guide (windows)
1. pull / clone the repo
2. navigate to the project directory and open the command prompt
3. setup the environment on local directory  with `py -m venv venv`
4. activate environment with `.\venv\Scripts\activate`
5. install packages for initial setup with `pip install -r requirements.txt`
6. create `.env` file, copy the content from `.env.example`, and update the configuration. 
7. create `job_profiles.json` file, copy the content from `job_profiles.example.json`, and update the configuration.
8. run script with `py .\app.py`

## Job profiles
Project uses `job_profiles.json` JSON file to configure profiles. the current structure of the file can house multiple profiles in an array with given format.
```json
[
    {
        "disabled": true,
        "name": "job-name",
        "desc": "what does this job do.",
        "source": {
            "table": "source table name",
            "columns":"columns separated by comma"
        },
        "destination": {
            "table": "sqlite destination table name"  
        }
    },
    ...
]
```
$$Properties$$ 
* **disabled:** removes the profile from execution when set `True`. _optional_   
* **name:**  unique profile name. Allows program to identify available jobs.
* **desc:** Internal Notes. Not used within program. (may change in future)
* **source.table:** table name of the source database. data from the provided source table will be retrieved on job execution.
* **source.columns:** column names of the source database (separated by comma). columns provided in this list will be retrieved (and stored in to destination location) on job execution.
* **destination.table:** table name of the destination database. Retrieved data is transmitted to the provided destination table.

## Features
### Batch Processing
Queries from the source is processed in a batch of 5000 records per transaction by default. The configurations as of now is hard coded and will be moved to configuration file.

### Validation
Jobs are validated before initiated. Validation checks for job profile configuration to make sure valid values are entered. 

Validation also looks for basic SQL injection threats. However, it is not advised to solely rely on this. To safeguard from any fatal errors within profiles, it is highly recommended to use the lease privileges principle while setting up the database connections.
 

## Upcoming Features

<!-- ### SQL Scripts -->
<!-- ### Job Type -->
<!-- ### Airflow -->

## Tests
unit tests has not been implemented yet but are under consideration. 

> [!IMPORTANT] \
> It is not advised to use the tool directly in production without running it through the dev environment.
