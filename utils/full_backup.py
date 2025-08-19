from decimal import Decimal
from utils.job_profile import JobProfile
from utils.source_data_accessor import SourceDataAccessor
from utils.destination_data_collection import DestinationDataCollection
from collections.abc import Generator
import pyodbc
import logging
logger = logging.getLogger(__name__)

def transform(batched_rows):
    return [[str(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]


def run_backup(prf:JobProfile) -> Generator[tuple]:
    
    collector = DestinationDataCollection(prf.destination.database_name)
    try:
        with SourceDataAccessor(connection=prf.source.connection_str, batch_size=prf.batch_size) as accessor:
            # fetch columns to process
            source_query = prf.source.get_query_from_file('source')
            source_query_count = prf.source.get_query_from_file('count')
            column_list = []
            if(source_query is None):
                if(prf.source.columns is None or prf.source.columns.strip() == ''):
                    logger.info("Columns are not provided, all columns are to be transmitted.")
                    column_list = accessor.get_columns_by_table(prf.source.table, prf.source.schema)
                else:
                    column_list = prf.source.columns.split(',')
            else:
                column_list = accessor.get_columns_by_query(source_query)
            
            column_list = [f'[{i.strip()}]' for i in column_list]
            columns = ', '.join(column_list)
            completed = 0
            data_batches_generator = None
            if prf.type == 'incremental_by_count':
                if collector.is_exist(prf.destination.table):
                    skip = collector.get_count(prf.destination.table)
                    logger.info(f'destination table {prf.destination.table} will skip {skip} number of records.')
                    if(source_query is None):
                        data_batches_generator = accessor.yield_data_batches(table_name=prf.source.table_full_name, columns=columns, skip = skip)
                    else:
                        logger.warning("Incremental Update is not supported for Custom Queries from File. full backup will be executed instead.")
                    completed += skip # marked as skipped.
                else:
                    logger.info(f'destination table {prf.destination.table} does not exist! full backup will be executed for current run.')
            
            # full backup batch generator
            if data_batches_generator is None:
                collector.clear_table(prf.destination.table, columns)
                if(source_query is None):
                    data_batches_generator = accessor.yield_data_batches(table_name=prf.source.table_full_name, columns=columns)
                else:
                    data_batches_generator = accessor.yield_data_batches_by_query(query=source_query, count_query=source_query_count)

            for (total_count, batched_rows) in data_batches_generator:
                transformed_rows = transform(batched_rows)
                collector.append_data(table_name=prf.destination.table, columns=columns, data=transformed_rows)
                completed += len(transformed_rows)
                yield  (total_count, completed)
    except pyodbc.Error as e:
        logger.error("Database Error occur. Batch can not be proceeded.")
        logger.error(e)
