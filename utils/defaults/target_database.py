from utils.env import get_env
import logging 
logger = logging.getLogger(__name__)

try:
    env_values = get_env()
    default_target_db = env_values['target_sqlite_db']
except:
    default_target_db = None
    logger.fatal(f'failed to set default target database')

