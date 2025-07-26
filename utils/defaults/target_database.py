from utils.env import get_env


try:
    env_values = get_env()
    default_target_db = env_values['target_sqlite_db']
except:
    default_target_db = None
    print(f'failed to set default connection string for the source database')

