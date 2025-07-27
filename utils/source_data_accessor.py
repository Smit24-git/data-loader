import pyodbc
from collections.abc import Generator
from utils.defaults.batch_size import default_batch_size
import logging
logger = logging.getLogger(__name__)

# the advantage database server does not have any prevention against "WRITES"
# Never Ever use any insert, update, and delete statements here.

class SourceDataAccessor:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.cnxn is not None:
            self.cnxn.rollback() # in case any changes incur

    """Exposes tables from advantage server."""
    def __init__(self, connection, batch_size=default_batch_size):
        """Secures the connection, and disables autocommit"""
        self.connection = connection
        self.batch_size = batch_size
        self.connect()

    def connect(self):
        self.cnxn = pyodbc.connect(self.connection, autocommit=True)
        try:
            self.cnxn.autocommit = False
        except:
            logger.warning("AutoCommit for the source is set to True")


    def get_columns_from(self, query) -> list[str]:
        """returns columns for query table"""
        crsr = self.cnxn.cursor()
        crsr.execute(query)        
        cols = [col[0] for col in crsr.description]
        return cols
    
    def get_table_names(self) -> list[str]:
        """returns tables"""
        crsr = self.cnxn.cursor()
        tables = [(i[0], i[1], i[2]) for i in crsr.tables().fetchall()]

        return tables
    
    def get_columns_of(self, table_name, schema_name = None) -> list[str]:
        """returns columns for table"""
        crsr = self.cnxn.cursor()
        
        cols_crsr = crsr.columns(table_name) if schema_name is None else crsr.columns(table=table_name, schema=schema_name)
        
        cols = [col[3] for col in cols_crsr.fetchall()]
        return cols

    def __get_cursor_for_query(self, query) -> pyodbc.Cursor:
        """returns cursor embedded with select command"""
        crsr = self.cnxn.cursor()
        crsr.execute(query)
        return crsr
    
    def __get_cursor_for_table(self, table_name, columns) -> pyodbc.Cursor:
        """returns cursor embedded with select command"""
        return self.__get_cursor_for_query(f"select {columns} from {table_name}")
    
    def __get_row_count(self, table_name) -> int:
        """returns cursor embedded with select command"""
        return self.__get_row_count_by_query(f"select count(*) from {table_name}")
    
    def __get_row_count_by_query(self, query) -> int:
        """returns cursor embedded with select command"""
        crsr = self.cnxn.cursor()
        crsr.execute(query)
        return crsr.fetchone()[0]
    
    def yield_data_batches_by_query(self, query:str, count_query:str) -> Generator[list]:
        """collects data in batches, ram intensive. if """
        total_row_count = self.__get_row_count_by_query(count_query)
        crsr = self.__get_cursor_for_query(query)
        cnt = 0
        while True:
            rows = crsr.fetchmany(self.batch_size)
            yield (total_row_count, rows)
            cnt+=1
            if len(rows) == 0: break
    
    def yield_data_batches(self, table_name, columns) -> Generator[list]:
        """collects data in batches, ram intensive. if """
        total_row_count = self.__get_row_count(table_name)
        crsr = self.__get_cursor_for_table(table_name, columns)
        cnt = 0
        while True:
            rows = crsr.fetchmany(self.batch_size)
            yield (total_row_count, rows)
            cnt+=1
            if len(rows) == 0: break