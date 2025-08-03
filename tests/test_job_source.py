from tests.job_examples import *

from utils.job_profile import Source
import pytest
from unittest.mock import MagicMock, patch, mock_open
import pyodbc
source_table_validations = [
    ('table1', 'col1', True),
    ('schema.table1', 'col1', True),
    ('db.schema.table1', 'col1', True),
    ('table1', 'col1, col2', True),
    ('table3', 'col1', False),
    ('table1', 'col4', False),
    ('schema2.table1', 'col1', False),
]

@patch('utils.job_profile.default_source_connection_string', return_value = 'mock_default_conn')
@patch('utils.helpers.get_env', return_value={'my_data_source_source_conn': 'mock_connection_string'})
@patch('utils.source_data_accessor.SourceDataAccessor.connect')
@patch('utils.source_data_accessor.SourceDataAccessor.get_columns_by_table', 
       return_value = ['col1', 'col2'])
@patch('utils.source_data_accessor.SourceDataAccessor.get_table_names', 
       return_value = [('db','schema','table1'), ('db','schema','table2')])
@pytest.mark.parametrize('table,columns,result',source_table_validations)
def test_source_validation(mock_tables, mock_cols, mock_connect, mock_helper_env, mock_default_conn, table, columns, result):
    """"""
    json = valid_full_json.copy()
    json['source']['datasource'] = 'my_data_source'
    source = Source(json['source'], json['name'])
    source.table_full_name = table
    source.columns = columns
    (res, err) = source.validate()
    assert res == result, err



@patch('utils.job_profile.default_source_connection_string', return_value = 'mock_default_conn_str')
@patch('utils.source_data_accessor.SourceDataAccessor.connect',
       side_effect=pyodbc.Error)
@patch('utils.source_data_accessor.SourceDataAccessor.get_columns_by_table', 
       side_effect=pyodbc.Error)
@patch('utils.source_data_accessor.SourceDataAccessor.get_table_names',
       side_effect=pyodbc.Error)
@pytest.mark.parametrize('table,columns,result',source_table_validations)
def test_source_validation_against_odbc_error(mock_tables, mock_cols, mock_connect, mock_default_conn, table, columns, result):
    """"""
    json = valid_full_json.copy()
    # This test case does not use the default connection string
    json['source']['datasource'] = 'my_data_source'
    source = Source(json['source'], json['name'])
    source.table_full_name = table
    source.columns = columns
    (res, err) = source.validate()
    assert res == False, err



@patch('utils.job_profile.default_source_connection_string', return_value = 'mock_default_conn')
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
def test_invalid_source_table_name(mock_validate_source, mock_default_conn, invalid_table_name):
    json = valid_full_json.copy() 
    # This test case uses the default connection string
    if 'datasource' in json['source']:
        del json['source']['datasource']
    source = Source(json['source'], json['name'])
    source.table_full_name = invalid_table_name
    (res, err) = source.validate()
    assert res == False, f'{invalid_table_name} is not a valid source table.'
    

@patch('utils.job_profile.default_source_connection_string', return_value = 'mock_default_conn')
@patch('utils.job_profile.Source.validate_against_source',
       return_value=(True, None))
@pytest.mark.parametrize('valid_table_name', [
    'mytable',
    'myTable123',
    't.table'
])
def test_valid_source_table_name(mock_validate_source, mock_default_conn, valid_table_name):
    json = valid_full_json.copy() 
    if 'datasource' in json['source']:
        del json['source']['datasource']
    source = Source(json['source'], json['name'])
    source.table_full_name = valid_table_name
    (res, err) = source.validate()
    assert res, f'{valid_table_name} is a valid source table.'
    
