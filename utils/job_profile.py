from utils.defaults.batch_size import default_batch_size
from utils.defaults.source_connection_string import default_source_connection_string
from utils.defaults.target_database import default_target_db
from utils.helpers import has_key, get_query 
import re
import pyodbc
from utils.source_data_accessor import SourceDataAccessor
import json


class Source:
    def __init__(self, json, job_name):
        self.job_name = job_name
        self.connection_str = default_source_connection_string
        self.table = None
        self.columns = None
        self.from_file = False
        self.load_profile(json=json)

    def load_profile(self, json:dict):
        if json is None: return
        self.table = json['table'] if has_key(json, 'table') else None
        self.columns = json['columns']  if has_key(json, 'columns') else None
        self.from_file = json['from_file'] if has_key(json, 'from_file') else False

    def get_query_from_file(self, query_type):
        if self.from_file:
            return get_query(query_type, self.job_name ) 
        else:
            return None
    
    def validate(self):
        if self.connection_str is None:
            return False, 'Unable to identify default connection string of source database'
        if self.table is not None and re.fullmatch(r'(\w|\.){1,30}', self.table) is None:
            return False, 'Invalid source table name'
        if self.columns is not None and re.fullmatch(r'((\w)+(\s)*,?(\s)*)+', self.columns) is None:
            return False, 'Invalid column name'
        
        srcAccessor = None
        try:
            srcAccessor = SourceDataAccessor(self.connection_str)
            if self.table is not None:
                tables:list[str] = srcAccessor.get_table_names()
                if self.table not in tables:
                    return False, 'Invalid Table Name.'

                if self.columns is not None:
                    columns = srcAccessor.get_columns_of(self.table)
                    user_entered_cols = [i.strip() for i in self.columns.split(',')]
                    if len(set(user_entered_cols).intersection(set(columns))) != len(user_entered_cols):
                        return False, 'Invalid Columns detected.'
        except pyodbc.Error as e:
            return False, 'Validation failed to compare the source dataset tables and columns.\n' \
            'Please make sure the source database is configured correctly.\n' \
            f'{e}'
        return True, None
    
class Destination:
    def __init__(self, json):
        self.database_name = default_target_db 
        self.table = None
        self.load_profile(json=json)

    def load_profile(self, json:dict):
        if json is None: return
        
        self.table = json['table']

    def validate(self):
        if self.table is None:
            return (False, "Destination Table is Required")
        if re.fullmatch(r'\w{1,30}', self.table) is None:
            return False, 'Invalid destination table name'
        if re.fullmatch(r'(create|alter|drop|delete|insert|update|drop)+[-*;]*', self.table.strip()) is not None:
            return False, 'Invalid destination table name'        
        
        return True, None        

class JobProfile:

    def __init__(self, json):
        self.load_profile(json=json)

    @staticmethod
    def load_profiles():
        try:
            with open('job_profiles.json') as file:
                profiles_json = json.load(file)
                return [JobProfile(i) for i in profiles_json]
        except:
            pass
        return []


    def load_profile(self, json:dict):
        self.disabled = json['disabled'] if has_key(json,'disabled') else False
        self.name = json['name']
        self.type = json['type'] if has_key(json,'type') else None
        self.desc = json['desc']
        self.batch_size = json['batch_size'] if has_key(json, 'batch_size') else default_batch_size
        self.source = Source(json['source'], self.name)
        self.destination = Destination(json['destination'])

    def validate(self):
        """validates values"""
        min_batch_size = 2000
        
        if self.batch_size is None:
            return False, "Batch Size Must Require."

        # basic type checks
        
        if isinstance(self.batch_size, int) == False:
            return False, 'Batch size must be an integer value.'

        # basic value boundary checks
        if self.batch_size < min_batch_size:
            return False, f'Batch size must be >= {min_batch_size}'

        # basic regex check
        if re.fullmatch(r'([\w\-]){1,30}',self.name) is None:
            return False, 'Invalid profile name'
        if self.type is not None and re.fullmatch(r'(full|part){1}', self.type) is None:
            return False, 'Invalid profile type'

        source_res, source_msg = self.source.validate()
        if source_res == False:
            return source_res, source_msg
        
        dest_res, dest_msg = self.destination.validate()
        if dest_res == False:
            return dest_res, dest_msg
        
        return True, None
