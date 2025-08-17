from tests.job_examples import *
from utils.job_profile import JobProfile
from utils.full_backup import run_backup
from unittest.mock import patch, MagicMock
from utils.full_backup import SourceDataAccessor
from collections.abc import Generator

@patch.object(SourceDataAccessor, 'yield_data_batches')
@patch('utils.full_backup.DestinationDataCollection')
@patch('pyodbc.connect')
def test_full_backup_with_table_columns(odbc, mock_collector, mock_yield):
    """should complete full backup with tables and columns specified"""
    fake_batches=[
        (15, [(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)]),
        (15, [(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)]),
        (15, [(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)]),
    ]
    mock_yield.return_value = iter(fake_batches)

    job = JobProfile(valid_full_json)
    job.source.from_file = False
    job.source.table = "sample"
    job.source.columns = "col1, col2, col3"
    total = -1
    processed = 0
    batches_processed = 0
    for (total_count, completed) in run_backup(job):
        batches_processed += 1
        total = total_count
        processed = completed
    
    expected_rows = 15
    assert batches_processed == 3, "expected to be called 3 times"
    assert processed == expected_rows, f"expected {expected_rows} processed rows."
    assert total == expected_rows, f"expected {expected_rows} total rows."
    assert total == processed, f"expected processed row same as total rows."