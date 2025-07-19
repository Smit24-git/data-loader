from dotenv import dotenv_values

env_values = dotenv_values('.env')

try:
    default_target_db = env_values['target_sqlite_db']
except:
    default_target_db = None
    print(f'failed to set default connection string for the source database')

