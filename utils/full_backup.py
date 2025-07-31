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


def run_full_backup(prf:JobProfile) -> Generator[tuple]:
    
    collector = DestinationDataCollection(prf.destination.database_name)
    try:
        with SourceDataAccessor(connection=prf.source.connection_str, batch_size=prf.batch_size) as accessor:
            # fetch columns to process
            source_query = prf.source.get_query_from_file('source')
            source_query_count = prf.source.get_query_from_file('count')
            if(source_query is None):
                if(prf.source.columns is None or prf.source.columns.strip() == ''):
                    logger.info("Columns are not provided, all columns are to be transmitted.")
                    columns = ','.join(accessor.get_columns_by_table(prf.source.table, prf.source.schema))
                else:
                    columns = prf.source.columns
            else:
                columns = ','.join(accessor.get_columns_by_query(source_query))
            
            # initialize target collector
            collector.clear_table(prf.destination.table, columns)
            completed = 0

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
        logger.error("Database Error occur. Batch can not be proceeded.", e)
