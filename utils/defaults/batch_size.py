from utils.env import get_env
import logging
logger = logging.getLogger(__name__)

default_batch_size = 50000
try:
    env_values = get_env()
    default_batch_size = int(env_values['default_batch_size']) if 'default_batch_size' in env_values.keys() else default_batch_size
except:
    logging.warning(f'failed to set default batch size from configuration! system default ({default_batch_size}) will be used. ')
