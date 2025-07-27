from utils.job_profile import JobProfile
from tests.job_examples import *
import pytest
import json
from unittest.mock import MagicMock, patch, mock_open
@pytest.mark.parametrize('valid_json_profile', [
  valid_full_json,
  valid_from_file_json,
  missing_desc_json,
  invalid_batch_data_type_json,
  invalid_batch_size_json  
])
def test_load_profile(valid_json_profile):
    """tests profile loading with json parameter"""
    try:
        JobProfile(valid_json_profile)
        assert True, None        
    except:
        assert False, "Exception occur."

def test_nameless_profile_raises_exception():
    with pytest.raises(Exception):
        JobProfile(missing_name_json)  

def test_optional_desc_profile():
    try:
        JobProfile(missing_desc_json)
        assert True, None        
    except:
        assert False, "Exception occur."

def test_validate_profile():
    prf = JobProfile(valid_full_json)
    prf.source.validate = MagicMock(return_value=(True, None))
    prf.destination.validate = MagicMock(return_value=(True, None))
    
    (res, err) = prf.validate()
    
    assert res, err

def test_validate_invalid_mocked_source():
    prf = JobProfile(valid_full_json)
    prf.source.validate = MagicMock(return_value=(False, None))
    prf.destination.validate = MagicMock(return_value=(True, None))
    
    (res, err) = prf.validate()
    
    assert res == False, "should raise the validation failure"

def test_validate_invalid_mocked_destination():
    prf = JobProfile(valid_full_json)
    prf.source.validate = MagicMock(return_value=(True, None))
    prf.destination.validate = MagicMock(return_value=(False, None))
    
    (res, err) = prf.validate()
    
    assert res == False, "should raise the validation failure"

def test_validate_missing_batch_size():    
    prf = JobProfile(valid_full_json)
    prf.source.validate = MagicMock(return_value=(False, None))
    prf.destination.validate = MagicMock(return_value=(False, None))
    prf.batch_size = None # assumes batch size is not configured correctly
    (res, err) = prf.validate()
    assert res == False, "should fail to validate missing batch size"


@pytest.mark.parametrize('invalid_batch_profile', [
    invalid_batch_size_json,
    invalid_batch_data_type_json
])
def test_validate_invalid_batch_size(invalid_batch_profile):
    prf = JobProfile(invalid_batch_profile)
    prf.source.validate = MagicMock(return_value=(False, None))
    prf.destination.validate = MagicMock(return_value=(False, None))
    (res, err) = prf.validate()
    assert res == False, "should fail to validate invalid batch size"

@pytest.mark.parametrize('valid_name', [
    'myProfile', 
    'profile123',
    'not-too-long-profile'
])
def test_valid_profile_name(valid_name):
    case = valid_full_json.copy()
    case['name'] =  valid_name
    prf = JobProfile(case)
    prf.source.validate = MagicMock(return_value=(True, None))
    prf.destination.validate = MagicMock(return_value=(True, None))
    (res, err) = prf.validate()
    assert res == True, f"should accept {prf.name} as valid name"

@pytest.mark.parametrize('invalid_name', [
    'my profile', # space is not allowed
    'myprofile;', # special char not allowed
    'asddddddddddddddddddddddssstttwwww' # long names are not allowed 
])
def test_invalid_profile_name(invalid_name):
    case = valid_full_json.copy()
    case['name'] = invalid_name  
    prf = JobProfile(case)
    prf.source.validate = MagicMock(return_value=(True, None))
    prf.destination.validate = MagicMock(return_value=(True, None))
    (res, err) = prf.validate()
    assert res == False and err == 'Invalid profile name', "should fail to validate profile name"



@pytest.mark.parametrize('valid_type', [
    'full',
    'part',
    None,
])
def test_valid_profile_type(valid_type):
    json = valid_full_json.copy()
    json['type'] = valid_type
    prf = JobProfile(json)
    prf.source.validate = MagicMock(return_value = (True, None))
    prf.destination.validate = MagicMock(return_value = (True, None))
    (res, err) = prf.validate()
    assert res == True, err    



@pytest.mark.parametrize('invalid_profile_type', [
    'inv',
])
def test_invalid_profile_type(invalid_profile_type):
    case = valid_full_json.copy()
    case['type'] = invalid_profile_type
    prf = JobProfile(case)
    prf.source.validate = MagicMock(return_value=(True, None))
    prf.destination.validate = MagicMock(return_value=(True, None))
    (res, err) = prf.validate()
    assert res == False, "should fail to validate profile type"


@patch('builtins.open', new_callable=mock_open, read_data = json.dumps([
    valid_full_json,
    valid_from_file_json,
    missing_desc_json,
    invalid_batch_size_json
]))
@patch('utils.job_profile.logger')
def test_load_profiles(mock_file, mock_open):
    """validates accurate profile loading behavior"""
    jobs = JobProfile.load_profiles()
    assert len(jobs) == 4, 'unable to load correct amount of profiles'


@patch('builtins.open', new_callable=mock_open)
@patch('utils.job_profile.logger')
def test_missing_profile_file(mock_logger, mock_file):
    mock_file.side_effect = [FileNotFoundError]
    _ = JobProfile.load_profiles()
    mock_logger.error.assert_called_once()
    

@pytest.mark.parametrize('invalid_results',[
    [missing_name_json],
    [invalid_batch_data_type_json],
    {},
    None,
    [],
])
@patch('utils.job_profile.logger')
@patch('builtins.open', new_callable=mock_open)
def test_schema_validation_for_invalid_results(mock_file, mock_logger, invalid_results):
    mock_file.read_data = invalid_results
    _ = JobProfile.load_profiles()
    mock_logger.error.assert_called_once()


    