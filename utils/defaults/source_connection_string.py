from dotenv import dotenv_values

env_values = dotenv_values('.env')

try:
    default_source_connection_string = env_values['source_conn']
except:
    default_source_connection_string = None
