from decimal import Decimal
from utils.source_data_accessor import SourceDataAccessor
from utils.destination_data_collection import DestinationDataCollection
from collections.abc import Generator

def run_full_backup(
        source_connection_str, 
        target_database, 
        table_to_backup,
        target_table,
        table_columns_to_backup,
        source_query,
        source_count_query) -> Generator[list]:
    s_conn_str:str = source_connection_str
    t_database:str = target_database
    source_table_name:str = table_to_backup
    target_table_name:str = target_table
    columns:str = table_columns_to_backup
    
    collector = DestinationDataCollection(t_database)
    
    with SourceDataAccessor(connection=s_conn_str) as accessor:
        if(source_query is not None):
            columns = ','.join(accessor.get_columns_from(source_query))
            collector.clear_table(target_table_name, columns)
            completed = 0
            for (total_count, batched_rows) in accessor.yield_data_batches_by_query(query=source_query, count_query=source_count_query):
                conv_rows = [[str(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]
                collector.append_data(table_name=target_table_name, columns=columns, data=conv_rows)
                completed += len(batched_rows)
                yield  (total_count, completed)
        else:
            if(columns is None or columns.strip() == ''):
                print("Columns are not provided, all columns are to be transmitted.")
                columns = ','.join(accessor.get_columns_of(source_table_name))
            
            collector.clear_table(target_table_name, columns)
            completed = 0
            for (total_count, batched_rows) in accessor.yield_data_batches(table_name=source_table_name, columns=columns):
                conv_rows = [[str(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]
                collector.append_data(table_name=target_table_name, columns=columns, data=conv_rows)
                completed += len(batched_rows)
                yield  (total_count, completed)
