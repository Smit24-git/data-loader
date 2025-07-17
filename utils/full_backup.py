from decimal import Decimal
from utils.source_data_accessor import SourceDataAccessor
from utils.destination_data_collection import DestinationDataCollection
from collections.abc import Generator


def transform(batched_rows):
    return [[str(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]


def run_full_backup(
        source_connection_str, 
        target_database, 
        table_to_backup,
        target_table,
        table_columns_to_backup,
        source_query,
        source_count_query,
        batch_size) -> Generator[list]:
    
    collector = DestinationDataCollection(target_database)
    with SourceDataAccessor(connection=source_connection_str, batch_size=batch_size) as accessor:
        # fetch columns to process
        if(source_query is None):
            if(table_columns_to_backup is None or table_columns_to_backup.strip() == ''):
                print("Columns are not provided, all columns are to be transmitted.")
                columns = ','.join(accessor.get_columns_of(table_to_backup))
            else:
                columns = table_columns_to_backup
        else:
            columns = ','.join(accessor.get_columns_from(source_query))
        
        # initialize target collector
        collector.clear_table(target_table, columns)
        completed = 0

        if(source_query is None):
            data_batches_generator = accessor.yield_data_batches(table_name=table_to_backup, columns=columns)
        else:
            data_batches_generator = accessor.yield_data_batches_by_query(query=source_query, count_query=source_count_query)
        
        for (total_count, batched_rows) in data_batches_generator:
            transformed_rows = transform(batched_rows)
            collector.append_data(table_name=target_table, columns=columns, data=transformed_rows)
            completed += len(transformed_rows)
            yield  (total_count, completed)