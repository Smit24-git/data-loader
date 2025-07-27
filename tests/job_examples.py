valid_full_json = {
    'name': 'test',
    'type': 'full',
    'desc': 'should load successfully',
    'batch_size': 5000,
    'source': {
        'from_file': False,
        'table': 'DatasetVersions.csv',
        'columns': 'Id, DatasetId',
    },
    'destination': {
        'table': 'testSet'
    }
}

valid_from_file_json = {
    'name': 'test',
    'type': 'full',
    'desc': 'should load successfully',
    'batch_size': 5000,
    'source': {
        'from_file': True,
    },
    'destination': {
        'table': 'testSet'
    }
}

missing_name_json = {
    'type': 'full',
    'desc': 'should load successfully',
    'batch_size': 5000,
    'source': {
        'from_file': True,
    },
    'destination': {
        'table': 'testSet'
    }

}

missing_source_table_json = {
    'name': 'test',
    'type': 'full',
    # 'desc': 'should load successfully'
    'batch_size': 5000,
    'source': {
        'from_file': False,
        'columns': 'Id, DatesetId',
    },
    'destination': {
        'table': 'testSet'
    }
}

missing_desc_json = {
    'name': 'test',
    'type': 'full',
    # 'desc': 'should load successfully'
    'batch_size': 5000,
    'source': {
        'from_file': False,
        'table': 'Dataset',
        'columns': 'Id, DatesetId',
    },
    'destination': {
        'table': 'testSet'
    }
}

invalid_batch_data_type_json = {
    'name': 'test',
    'type': 'full',
    'desc': 'should render an exception',
    'batch_size': 'five thousand',
    'source': {
        'from_file': True,
    },
    'destination': {
        'table': 'testSet'
    }
}

invalid_batch_size_json = {
    'name': 'test',
    'type': 'full',
    'desc': 'should render an exception',
    'batch_size': 100,
    'source': {
        'from_file': True,
    },
    'destination': {
        'table': 'testSet'
    }
}
