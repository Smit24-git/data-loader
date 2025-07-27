# Data Loader

SQL data loader designed to extract and load from different sql sources to the lightweight sqlite tables.

## Getting Started

### Prerequisites

*   Python 3.10+
*   PIP
*   Git

### Installation

1.  Clone the repository:

    ```
    git clone https://github.com/your-username/your-repository.git
    ```

2.  Navigate to the project directory:

    ```
    cd your-repository
    ```

3.  Create a virtual environment:

    ```
    python -m venv venv
    ```

4.  Activate the virtual environment:

    *   On Windows:

        ```
        venv\Scripts\activate
        ```

    *   On macOS and Linux:

        ```
        source venv/bin/activate
        ```

5.  Install the required packages:

    ```
    pip install -r requirements.txt
    ```

### Configuration

1.  Create a `.env` file in the root directory of the project.
2.  Copy the contents of `.env.example` into `.env`.
3.  Update the values in `.env` to match your environment.

### Usage

Run the application:

```
python app.py
```

## Job Profiles

The `job_profiles.json` file is used to configure data loading jobs. It contains an array of job objects.

### Job Properties
*   `disabled` (boolean, optional): If `true`, the profile will be ignored.
*   `name` (string, required): A unique name for the job.
*   `desc` (string, optional): A description of what the job does.
*   `type` (string, optional): The type of job. Currently supports `full`. Defaults to `full` if omitted.
*   `batch_size` (integer, optional): The number of records to process in each batch. Defaults to the value set in `.env` or 5000.
*   `source` (object, required): Defines the data source.
    *   `datasource` (string, optional): Specifies a connection string key from the `.env` file (e.g., `mssql_source_conn`). If not provided, the `source_conn` from the `.env` file is used.
    *   `table` (string, required): The full name of the source table (e.g., `database.schema.table`).
    *   `columns` (string, optional): A comma-separated list of columns to extract. If omitted, all columns will be extracted.
    *   `from_file` (boolean, optional): If `true`, the job will use query files from the `./commands/<profile_name>/` directory instead of the `table` and `columns` properties.
*   `destination` (object, required): Defines the data destination.
    *   `table` (string, required): The name of the table in the destination SQLite database.

### Example

```json
[
    {
        "disabled": true,
        "name": "x-job-name",
        "desc": "what does this job do.",
        "type": "full",
        "batch_size": 5000,
        "source": {
            "datasource": "specify datasource if table exists in different source than default",
            "table": "source table name",
            "columns":"columns separated by comma"
        },
        "destination": {
            "table": "sqlite destination table name"  
        }
    }
]
```

## Running Tests

To run the tests, run the following command:

```
pytest
```



## Contributing

Contributions are welcome! Please open an issue or submit a pull request.