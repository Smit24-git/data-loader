from tests.job_examples import *

from utils.job_profile import Source
import pytest
from unittest.mock import MagicMock, patch, mock_open
import pyodbc
source_table_validations = [
    ('table1', 'col1', True),
    ('table1', 'col1, col2', True),
    ('table3', 'col1', False),
    ('table1', 'col4', False),
]

@patch('utils.source_data_accessor.SourceDataAccessor.connect')
@patch('utils.source_data_accessor.SourceDataAccessor.get_columns_of', 
       return_value = ['col1', 'col2'])
@patch('utils.source_data_accessor.SourceDataAccessor.get_table_names', 
       return_value = ['table1', 'table2'])
@pytest.mark.parametrize('table,columns,result',source_table_validations)
def test_source_validation(mock_source, mock_col, mock_table, table, columns, result):
    """"""
    json = valid_full_json.copy()
    source = Source(json['source'], json['name'])
    source.table = table
    source.columns = columns
    (res, err) = source.validate()
    assert res == result, err



@patch('utils.source_data_accessor.SourceDataAccessor.connect',
       side_effect=pyodbc.Error)
@patch('utils.source_data_accessor.SourceDataAccessor.get_columns_of', 
       side_effect=pyodbc.Error)
@patch('utils.source_data_accessor.SourceDataAccessor.get_table_names',
       side_effect=pyodbc.Error)
@pytest.mark.parametrize('table,columns,result',source_table_validations)
def test_source_validation_against_odbc_error(mock_source, mock_col, mock_table, table, columns, result):
    """"""
    json = valid_full_json.copy()
    source = Source(json['source'], json['name'])
    source.table = table
    source.columns = columns
    (res, err) = source.validate()
    assert res == False, err



@patch('utils.job_profile.Source.validate_against_source',
       return_value=(True, None))
@pytest.mark.parametrize('invalid_table_name', [
    'my table',
    'mytable;',
    '123',
    '123.123',
    'delete',
    'drop',
    '--' 
])
def test_invalid_source_table_name(_, invalid_table_name):
    json = valid_full_json.copy() 
    source = Source(json['source'], json['name'])
    source.table = invalid_table_name
    (res, err) = source.validate()
    assert res == False, f'{invalid_table_name} is not a valid source table.'
    

@patch('utils.job_profile.Source.validate_against_source',
       return_value=(True, None))
@pytest.mark.parametrize('valid_table_name', [
    'mytable',
    'myTable123',
    't.table'
])
def test_valid_source_table_name(_, valid_table_name):
    json = valid_full_json.copy() 
    source = Source(json['source'], json['name'])
    source.table = valid_table_name
    (res, err) = source.validate()
    assert res, f'{valid_table_name} is a valid source table.'
    
