from decimal import Decimal
from utils.source_data_accessor import SourceDataAccessor
from utils.destination_data_collection import DestinationDataCollection
from collections.abc import Generator

def run_full_backup(
        source_connection_str, 
        target_database, 
        table_to_backup,
        target_table,
        table_columns_to_backup) -> Generator[list]:
    s_conn_str = source_connection_str
    t_database = target_database
    table_name = table_to_backup
    target_table_name = target_table
    columns = table_columns_to_backup

    print(t_database)
    collector = DestinationDataCollection(t_database)
    with SourceDataAccessor(connection=s_conn_str) as accessor:
        collector.clear_table(target_table_name, columns)
        completed = 0
        for (total_count, batched_rows) in accessor.yield_data_batches(table_name=table_name, columns=columns):
            conv_rows = [[float(r_item) if type(r_item)==Decimal else r_item  for r_item in row] for row in batched_rows]
            collector.append_data(table_name=target_table_name, columns=columns, data=conv_rows)
            completed += len(batched_rows)
            yield  (total_count, completed)
