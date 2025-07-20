from tests.job_examples import *
import pytest

from utils.job_profile import Destination

@pytest.mark.parametrize('invalid_table_name', [
    'my table',
    'mytable;',
    '123',
    '123',
    't.table',
    'delete',
    'drop',
    '--',
    None
])
def test_invalid_destination_table_name(invalid_table_name):
    json = valid_full_json.copy() 
    destination = Destination(json['destination'])
    destination.table = invalid_table_name
    (res, err) = destination.validate()
    assert res == False, f'{invalid_table_name} is not a valid destination.'



@pytest.mark.parametrize('valid_table_name', [
    'mytable',
    'myTable123'
])
def test_valid_source_table_name(valid_table_name):
    json = valid_full_json.copy() 
    source = Destination(json['destination'])
    source.table = valid_table_name
    (res, err) = source.validate()
    assert res, f'{valid_table_name} is a valid destination table.'