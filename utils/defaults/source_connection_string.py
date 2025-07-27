from utils.env import get_env
import logging
logger = logging.getLogger(__name__)

try:
    env_values = get_env()
    default_source_connection_string = env_values['source_conn']
except:
    default_source_connection_string = None
    logger.fatal(f'failed to set the default connection string.')
