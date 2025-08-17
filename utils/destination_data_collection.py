import sqlite3
import logging
logger = logging.getLogger(__name__)

class DestinationDataCollection:

    def __init__(self, database_location ):
        self.conn = sqlite3.connect(
            database_location, 
            autocommit=False)        

    def is_exist(self, table_name):
        cur = self.conn.cursor()
        cur.execute("select name from sqlite_master where type='table' and name=?", [table_name])
        data = cur.fetchone()
        return data is not None and data[0] == table_name

    def get_count(self, table_name):
        cur = self.conn.cursor()
        cur.execute(f'select count(*) from {table_name}')
        data = cur.fetchone()
        return data[0]

    def clear_table(self, table_name, columns):
        cur = self.conn.cursor()
        cur.execute(f'drop table if exists {table_name}')
        self.conn.commit()
        try:        
            cur.execute('vacuum')
            self.conn.commit()
        except:
            logger.critical("unable to vacuum the destination database")
        
        cur.execute(f'create table {table_name}({columns})')
        self.conn.commit()
        

    def append_data(self, table_name, columns, data):
        cur = self.conn.cursor()
        total_columns = len(columns.split(','))
        columns_q = ', '.join(['?' for i in range(total_columns)])
        cur.executemany(f'insert into {table_name}({columns}) values({columns_q})', data)        
        self.conn.commit()
