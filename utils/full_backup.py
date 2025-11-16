from decimal import Decimal
from utils.job_profile import JobProfile, Source
from utils.source_data_accessor import SourceDataAccessor
from utils.destination_data_collection import DestinationDataCollection
from collections.abc import Generator
import pyodbc
import logging
logger = logging.getLogger(__name__)

# TODO: abstract away functions 

def transform(batched_rows):
    '''TODO: solve this pragmatically without checking every columns'''
    return [[str(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]

def get_columns_by_table(connection_str, table, schema):
    with SourceDataAccessor(connection=connection_str) as accessor:
        return accessor.get_columns_by_table(table, schema)

def get_columns_by_source_query(connection_str, source_query):
    with SourceDataAccessor(connection=connection_str) as accessor:
        return accessor.get_columns_by_query(source_query)

def get_bracketed_source_columns(prf:JobProfile):    
    wrap_brackets = lambda lst: [] if lst is None else [f'[{i.strip()}]' for i in lst]
    
    # get columns by source if raw query is provided
    source_query = prf.source.get_query_from_file('source')
    if source_query is not None:
        return wrap_brackets(get_columns_by_source_query(prf.source.connection_str,source_query))
    
    # get specified columns if column list is provided
    if prf.source.columns is not None:
        return wrap_brackets(prf.source.columns.split(','))
    else: # get all columns 
        logger.info("Columns are not provided, all columns are to be transmitted.")
        return wrap_brackets(get_columns_by_table(prf.source.connection_str, prf.source.table, prf.source.schema))
    
def get_full_backup_batches(prf:JobProfile):
    column_list = get_bracketed_source_columns(prf)
    columns = ', '.join(column_list)
    custom_source_query = prf.source.get_query_from_file('source')
    
    with SourceDataAccessor(connection=prf.source.connection_str, batch_size=prf.batch_size) as accessor:            
        if custom_source_query is None:
            return accessor.yield_data(
                table_name = prf.source.table_full_name, 
                columns = columns)
        else:
            custom_source_count_query = prf.source.get_query_from_file('count')
            return  accessor.yield_data_by_query(
                query = custom_source_query, 
                count_query = custom_source_count_query)

def get_incremental_backup_batches(prf:JobProfile, skip):
    column_list = get_bracketed_source_columns(prf)
    columns = ', '.join(column_list)
    with SourceDataAccessor(connection=prf.source.connection_str, batch_size=prf.batch_size) as accessor:            
        return accessor.yield_data_batches(table_name=prf.source.table_full_name, columns=columns, skip = skip)

def run_backup(prf:JobProfile) -> Generator[tuple]:
    collector = DestinationDataCollection(prf.destination.database_name)
    try:
        source_query = prf.source.get_query_from_file('source')
        column_list = get_bracketed_source_columns(prf)
        columns = ', '.join(column_list)
        completed = 0
        data_batches = None

        # get data batches
        if prf.type == 'incremental_by_count':            
            if source_query is not None:
                logger.warning("Incremental Update is not supported for Custom Queries from File. full backup will be executed instead.")
                collector.clear_table(prf.destination.table, columns)
                data_batches = get_full_backup_batches(prf)
            elif collector.is_exist(prf.destination.table) == False:
                logger.info(f'destination table {prf.destination.table} does not exist! full backup will be executed for current run.')
                collector.clear_table(prf.destination.table, columns)
                data_batches = get_full_backup_batches(prf)
            else:
                records_exists = collector.get_count(prf.destination.table)
                logger.info(f'destination table {prf.destination.table} will skip {records_exists} number of records.')
                data_batches = get_incremental_backup_batches(prf, records_exists) 
                completed += records_exists
        else:
            collector.clear_table(prf.destination.table, columns)
            data_batches = get_full_backup_batches(prf)
        
        for (total_count, batched_rows) in data_batches:
            transformed_rows = transform(batched_rows)
            collector.append_data(table_name=prf.destination.table, columns=columns, data=transformed_rows)
            completed += len(transformed_rows)
            yield  (total_count, completed)
    except pyodbc.Error as e:
        logger.error("Database Error occur. Batch can not be proceeded.")
        logger.error(e)
