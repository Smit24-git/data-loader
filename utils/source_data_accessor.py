import pyodbc
from collections.abc import Generator

# the advantage database server does not have any prevention against "WRITES"
# Never Ever use any insert, update, and delete statements here.

class SourceDataAccessor:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cnxn.rollback() # in case any changes incur

    """Exposes tables from advantage server."""
    def __init__(self, connection, batch_size=5_000, max_batch_count_per_request=1000):
        """Secures the connection, and disables autocommit"""
        self.connection = connection
        self.cnxn = pyodbc.connect(connection, autocommit=True)
        try:
            self.cnxn.autocommit = False
        except:
            print("AutoCommit for the source is set to True")

        self.batch_size = batch_size
        self.max_batch_count_per_request = max_batch_count_per_request 
    
    def get_columns_for(self, table_name) -> list[str]:
        """returns list of columns for table"""
        crsr = self.cnxn.cursor()
        
        cols = [col[3] for col in crsr.columns(table_name).fetchall()]
        return cols


    def __get_cursor_for(self, table_name, columns) -> pyodbc.Cursor:
        """returns cursor embedded with select command"""
        crsr = self.cnxn.cursor()
        crsr.execute(f"select {columns} from {table_name}")
        return crsr
    
    def __get_row_count(self, table_name) -> int:
        """returns cursor embedded with select command"""
        crsr = self.cnxn.cursor()
        crsr.execute(f"select count(*) from {table_name}")
        return crsr.fetchone()[0]
    
    def yield_data_batches(self, table_name, columns) -> Generator[list]:
        """collects data in batches, ram intensive. if """
        total_row_count = self.__get_row_count(table_name)
        crsr = self.__get_cursor_for(table_name, columns)
        cnt = 0
        while True:
            rows = crsr.fetchmany(self.batch_size)
            yield (total_row_count, rows)
            cnt+=1
            if len(rows) == 0: break
            # extreme case trigger to stop long running conditions internally.
            if cnt > self.max_batch_count_per_request: 
                print('\n Max batch threshold reached. please increase the threshold or batch size to increase the read limits')
                break