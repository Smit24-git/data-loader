from dotenv import dotenv_values
env_values = dotenv_values('.env')

def get_query(type, job_name) -> str:
    try:
        with open(f'./commands/{job_name}/{type}.sql') as file:
            return file.read()
    except Exception:
        return None

def has_key(obj, key) -> bool:
    return key in obj.keys()

def get_connection_string(data_source):
    try:
        return env_values[f'{data_source}_source_conn']
    except:
        return None