from utils.defaults.batch_size import default_batch_size
from utils.defaults.source_connection_string import default_source_connection_string
from utils.defaults.target_database import default_target_db
from utils.helpers import get_connection_string, has_key, get_query 
import re
import pyodbc
from utils.source_data_accessor import SourceDataAccessor
import json
import logging
from jsonschema import SchemaError, ValidationError, validate

logger = logging.getLogger(__name__)

basic_schema_profile = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Job Profile",
    "description": "Profile",
    "type": "object",
    "properties": {
        "disabled": {
            "description": "optional flag to disable profile",
            "type": "boolean"
        },
        "name": {
            "description": "profile name",
            "type": "string",
            "minLength": 1
        },
        "desc": {
            "description": "profile description",
            "type": "string",
        },
        "type": {
            "description": "describes how the job is loaded.",
            "enum": ["full", "incremental_by_count", None]
        },
        "batch_size": {
            "description": "number of records to be processed per batch",
            "type": "number",
            "minimum": 2000
        },
        "source": {
            "description": "source location configuration for the profile",
            "type": "object",
            "properties": {
                "datasource": {
                    "description": "source data source key to target specific source location",
                    "type": "string"
                },
                "table": {
                    "description": "name of the source table to be transmitted",
                    "type": "string"
                },
                "columns": {
                    "description": "names of the column to be transmitted (separated by comma)",
                    "type": "string"
                },
            },
            "dependentRequired": {
                "columns": ["table"]
            },
        },
        "destination": {
            "description": "target location configuration for the profile",
            "type": "object",
            "properties": {
                "table": {
                    "description":"name of the destination table",
                    "type": "string"
                }
            }
        },
    }
}
basic_schema_list = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Job Profiles",
    "description": "List of profiles.",
    "type": "array",
    "items": basic_schema_profile,
    "minItems": 1,
    "uniqueItems": True,
    "required": ['name'],
}

class Source:
    def __init__(self, json, job_name):
        self.job_name = job_name
        self.table_full_name = None
        self.table = None
        self.schema = None
        self.columns = None
        self.from_file = False
        self.load_profile(json=json)

    def load_profile(self, json:dict):
        if json is None: return
        self.table_full_name = json['table'] if has_key(json, 'table') else None
        self.columns = json['columns']  if has_key(json, 'columns') else None
        self.from_file = json['from_file'] if has_key(json, 'from_file') else False
        
        self.connection_str =  get_connection_string(json['datasource']) if has_key(json, 'datasource') else default_source_connection_string
        self.table = self.table_full_name

    def get_query_from_file(self, query_type):
        if self.from_file:
            return get_query(query_type, self.job_name) 
        else:
            return None
    def list_columns_metadata(self):
        self.validate_against_source()
        srcAccessor = SourceDataAccessor(self.connection_str)

        if self.from_file:
            return srcAccessor.get_columns_by_query(
                self.get_query_from_file('source'),
                metadata=True
            )
        else:
            (desc, columns_meta) =  srcAccessor.get_columns_by_table(
                self.table, self.schema, metadata=True)
            user_entered_cols = [i.strip().lower() for i in self.columns.split(',')]
            return (desc,[cm for cm in columns_meta if cm[3].lower() in user_entered_cols])

    def validate(self):
        if self.connection_str is None:
            return False, 'Unable to identify default connection string of source database'
        if self.table_full_name is not None:
            if re.fullmatch(r'[\d|\.]+', self.table_full_name) is not None:
                return False, 'Invalid source table name'
            if  re.fullmatch(r'(\w|\.){1,30}', self.table_full_name) is None:
                return False, 'Invalid source table name'
            if re.fullmatch(r'(create|alter|drop|delete|insert|update|drop)+[-*;]*', self.table_full_name.strip()) is not None:
                return False, 'Invalid source table name'        
        if self.columns is not None and re.fullmatch(r'((\w)+(\s)*,?(\s)*)+', self.columns) is None:
            return False, 'Invalid column name'
        if self.from_file == False and self.table_full_name is None:
            return False, 'Source table name is required.'
        
        (res, err) = self.validate_against_source()
        if res == False: return res, err
        
        return True, None
    
    def validate_against_source(self):
        srcAccessor = None
        try:
            srcAccessor = SourceDataAccessor(self.connection_str)
            if self.table_full_name is not None:
                tables:list[str] = srcAccessor.get_table_names()
                if len(tables) == 0:
                    return False, "database does not have any tables to validate against."
                
                if self.table_full_name.lower() not in [i[2].lower() for i in tables]:
                    table_split = self.table_full_name.split('.')
                    if len(table_split) > 1:
                        check = 2
                        for i in range(len(table_split)-1, -1, -1):
                            if table_split[i].lower() not in [det[check].lower() for det in tables]:
                                return False, f'Table {table_split[i]} does not exist'
                            else:
                                if check == 2:
                                    self.table = table_split[i]
                                elif check == 1:
                                    self.schema = table_split[i]
                                check -= 1
                    else:    
                        return False, 'Table does not exist.'
                    

                if self.columns is not None:
                    columns = srcAccessor.get_columns_by_table(self.table, self.schema)
                    columns = [c.lower() for c in columns]
                    user_entered_cols = [i.strip().lower() for i in self.columns.split(',')]
                    if len(set(user_entered_cols).intersection(set(columns))) != len(user_entered_cols):
                        return False, 'one or more columns missing.'
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
        
        if re.fullmatch(r'\d{1,30}', self.table) is not None:
            return False, 'Invalid destination table name'
        if re.fullmatch(r'\w{1,30}', self.table) is None:
            return False, 'Invalid destination table name'
        if re.fullmatch(r'(create|alter|drop|delete|insert|update|drop)+[-*;]*', self.table.strip()) is not None:
            return False, 'Invalid destination table name'        
        
        return True, None        

class JobProfile:

    def __init__(self, json):
        validate(json, basic_schema_profile)
        self.load_profile(json=json)

    @staticmethod
    def load_profiles():
        profiles_json = []
        try:
            with open('job_profiles.json') as file:
                profiles_json = json.load(file)
                # validate structure and types
                validate(profiles_json, basic_schema_list)

        except ValueError as e:
            logger.error('Invalid profile.')
            raise e
        except FileNotFoundError as e:
            logger.error('Profiles not found.')
            raise e
        except ValidationError as e:
            logger.error('invalid profile(s).')
            raise e
        except SchemaError as e:
            logger.critical("invalid schema.")
            raise e
        
        profiles = []
        failed_profiles = 0
        for prf_json in profiles_json:
            try:
                profiles.append(JobProfile(prf_json))
            except:
                failed_profiles +=1
        if failed_profiles > 0:
            logger.warning(f'unable to load {failed_profiles} profiles. please make sure all profiles are configured correctly in job_profiles.json')
        return profiles


    def load_profile(self, json:dict):
        self.disabled = json['disabled'] if has_key(json,'disabled') else False
        self.name = json['name']
        self.type = json['type'] if has_key(json,'type') else None
        self.desc = json['desc'] if has_key(json, 'desc') else None
        self.batch_size = json['batch_size'] if has_key(json, 'batch_size') else default_batch_size
        self.source = Source(json['source'], self.name)
        self.destination = Destination(json['destination'])

    def validate(self):
        """validates values"""
        
        
        if self.batch_size is None:
            return False, "Batch Size Must Require."

        # basic type checks
        
        if isinstance(self.batch_size, int) == False:
            return False, 'Batch size must be an integer value.'

        # basic regex check
        if re.fullmatch(r'([\w\-]){1,30}',self.name) is None:
            return False, 'Invalid profile name'

        source_res, source_msg = self.source.validate()
        if source_res == False:
            return source_res, source_msg
        
        dest_res, dest_msg = self.destination.validate()
        if dest_res == False:
            return dest_res, dest_msg
        
        return True, None
