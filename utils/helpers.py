def get_query(type, job_name) -> str:
    try:
        with open(f'./commands/{job_name}/{type}.sql') as file:
            return file.read()
    except Exception:
        return None

def has_key(obj, key) -> bool:
    return key in obj.keys()
