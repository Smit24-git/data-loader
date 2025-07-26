
from dotenv import dotenv_values



def get_env():
    return dotenv_values('.env')