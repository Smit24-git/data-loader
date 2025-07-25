from dotenv import dotenv_values

env_values = dotenv_values('.env')
default_batch_size = 5000
try:
    default_batch_size = int(env_values['default_batch_size']) if 'default_batch_size' in env_values.keys() else default_batch_size
except:
    print(f'failed to set default batch size from configuration! system default ({default_batch_size}) will be used. ')
