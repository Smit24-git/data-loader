from utils.env import get_env


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
        env_values = get_env()
        return env_values[f'{data_source}_source_conn']
    except KeyError:
        print(f"{data_source}_source_conn is not configured.")
        return None