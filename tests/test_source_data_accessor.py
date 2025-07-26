from unittest.mock import MagicMock, patch, Mock
import pytest
import pyodbc
from utils.source_data_accessor import SourceDataAccessor

# test auto commit set to false
@patch('pyodbc.connect')
class Test_SourceDataAccessor:
    
    def test_autocommit_set_as_false(self, connect ):
        source = SourceDataAccessor('connection', 200)
        assert source.cnxn.autocommit == False, "Auto Commit is Enabled"

    def test_rolledback_on_exit(self, connect):
        with SourceDataAccessor('connection') as source:
            pass
        connect.return_value.rollback.assert_called_once()

    def test_table_column_fetchall(self, connect):
        cursor = Mock()
        tables = [[('1','1','1', '11'),('2','2','2','22'),('3','3','3','33')]]
            
        expected = [('1','1','1'),('2','2','2'),('3','3','3')]

        cursor.tables.return_value.fetchall.side_effect = tables
        
        connect.return_value.cursor.return_value = cursor
        with SourceDataAccessor('connection') as source:
            res = source.get_table_names()
            assert cursor.tables.return_value.fetchall.call_count == 1, 'table name is not fetched properly'
            assert res == expected, "table values are expected in a list of tuple with first 3 entries" 