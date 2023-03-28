# Universal Flat File Loader

Welcome to my first Python project.  My primary reason for building this application was to help me learn Python while also addressing a common use-case I have encountered in my career as a Data Engineer: loading simple and well-formed flat files into a database without needing to build a formal ETL job.

The purpose of this application is to allow users to load any predictable flat file structure into a database using config files and specs.  It incorporates features such as logging, archiving, file expiration and execution of custom post-load SQL scripts.  The current version is focused on loading to an on-premises PostgreSQL database, but will be enhanced to target other platforms in the future.

## Requirements

python = "^3.8"

pandas = "^1.5.3"

sqlalchemy = "^2.0.4"

psycopg2-binary = "2.9.5"

configparser = "^5.3.0"

openpyxl = "^3.1.0"

keyring = "^23.13.1" # Optional

pywin32 = "^305" # Required for Windows machines

## Installation

### Via poetry (Installation instructions [here](https://python-poetry.org/docs/)):

```python
poetry install
```

### Via pip:
```python
pip3 install pandas
pip3 install sqlalchemy
pip3 install psycopg2-binary
pip3 install configparser
pip3 install openpyxl
pip3 install keyring # Optional
pip3 install pywin32 # Required for Windows machines
```

## Usage

Configure the following files:

* ./src/app_config.ini
* ./src/connections_config.ini

Create at least one folder in your "load_file_path" directory.  Configure the following files:

* load_file_path/load_folder/support/file_config.ini
* load_file_path/load_folder/support/mapping_spec_\*.xls\*
* Optional: Any SQL scripts required (see section "SQL Scripts" below for details)

```python
python3 src/main.py
```

## Passwords

The module for retrieving database passwords is located at **'./src/utils/password.py'**.  By default it uses the 'keyring' library, accepts two strings of 'secret_key' and 'user_name' and returns a string of 'password'.  If you wish to use a different method of storing and retrieving database passwords, modify this .py file.

If you require more significant changes to how the password is retrieved (Eg. need to pass a different number of parameters), it is called by the **'./src/utils/build_engine.py'** module.

## App Configuration

The application is controlled by a number of configuration files, read using the ConfigParser library:

**./src/app_config.ini**

Controls the general behavior of the application (example values provided)

``` python
[app_settings]
# Root location of sub-folders to process
load_file_path = C:\Flat Files\Upload
# Root location to place loaded files
archive_file_path = C:\Flat Files\Archive
# Location to place log files
log_file_path = C:\Flat Files\Logs
# 'read_chunk_size' not currently used; placeholder for future enhancement
read_chunk_size = 100_000
# Whether files should be archived after processing.  Accepts True or False
archive_flag = True
# Whether logs should be generated.  Accepts True or False
logging_flag = True
# Number of days before deleting job logs
log_archive_expire_days = 30
```

**./src/connections_config.ini**

Holds connection details for the target database.  Future releases will allow for multiple target databases to be specified.  Builds a SQLAlchemy connection string with the following pattern:

'{connect_type}://{user_name}:{password}@{server_address}:{server_port}/{server_name}'

``` python
[target_connection]
# SQLAlchemy connection type
connect_type = postgresql+psycopg2
# Environment for connection (dev / qa / prod).  Informational only
environment = dev
server_address = localhost
server_port = 5432
server_name = my_server
# Initial landing schema.  Account should have create / drop / insert / delete / update privileges
schema = wrk_schema
user_name = python_load_wrk
# Reference value for retrieving the correct password
secret_key = Database_Dev_WRK
```

## Load File Configuration

Each upload folder located in the 'load_file_path' has a number of components.  Samples are located in the './templates/flat_files_01/support/' folder

**./templates/flat_files_01/support/file_config.ini**

Read using the ConfigParser library:

``` python
[file_settings]
# File extension to process.  Will also process the following archive formats:  '.gz', '.zip', '.bz2', '.xz'
extension = txt
delimiter = \t
encoding = UTF-8
# Optional value to ignore and load as NULL.  Will also load blank string ('') as NULL
null_value = <NULL>
# Takes integer.  Use one of QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE (3)
quoting = 3
# chunk size to process
read_chunk_size = 100_000
# Number of days before deleting archives
archive_expire_days = 30
```

---

**./templates/flat_files_01/support/mapping_spec_flat_files_01.xlsx**

Spec outlining the structure of the flat file.  Looks for a pattern of **'mapping_spec_\*.xls\*'**

*Important:  You can add additional columns / tabs, but do not remove or rename any existing columns.*

Below is an outline of the columns used by the application:

| *Target Table* | *Target Field* | *Type* | *Source System* | *Source Field(s)* |
| :--- | :--- | :--- | :--- | :--- |
|wrk_flat_file_01|record_id|string|Flat File|record_id|
|wrk_flat_file_01|float_field_01|float64|Flat File|float_field_01|
|wrk_flat_file_01|integer_field_01|float64|Flat File|integer_field_01|
|wrk_flat_file_01|date_field_01|date|Flat File|date_field_01|
|wrk_flat_file_01|timestamp_field_01|timestamp|Flat File|timestamp_field_01|

For all rows with a value of 'Flat File' in the 'Source System' column:

* 'Target Table' will be the name of the table created in the landing schema.  The document should only contain a single 'Target Table' value specified with 'Flat File' as its 'Source System'
* The load file will be checked that all fields listed under 'Source Field(s)' are available
* All values will be loaded as string, with the following exceptions based on 'Type':
    * float64 - Apply pandas 'to_numeric' function
    * date - Apply pandas 'to_datetime' function
    * timestamp - Apply pandas 'to_datetime' function
* Provided sample also has entries for stage and target tables.  This is ignored by the program, but can be a good place to maintain this documentation for the overall load.  Just ensure to use something other than 'Flat File' in the 'Source System' column

---
### SQL Scripts

**./templates/flat_files_01/support/wrk to stg load - flat_files_01.sql**

SQL script to load from the wrk table to an appropriate staging table.  Looks for a pattern of **'wrk to stg load\*.sql\*'**

---

**./templates/flat_files_01/support/stage post-load 01 - flat_files_01.sql**

SQL script(s) to run after the stage load.  Will be executed in alphabetical order.  Looks for a pattern of **'stg post-load\*.sql\*'**

---

**./templates/flat_files_01/support/stg to target load - flat_files_01.sql**

SQL script to load from the stage table to an appropriate target table.  Looks for a pattern of **'stg to target load\*.sql\*'**

---

**./templates/flat_files_01/support/target post-load 01 - flat_files_01.sql**

**./templates/flat_files_01/support/target post-load 02 - flat_files_01.sql**

SQL script(s) to run after the target load.  Will be executed in alphabetical order.  Looks for a pattern of **'target post-load\*.sql\*'**

---

**General notes for SQL scripts:**

* Every SQL statement must end with a ';'
* Tables should be prefixed with Stage and Target schema names where appropriate, as the connection will default to the App Setting landing schema
* Comments prefixed with '--' or enclosed in '/\* \*/' will be ignored
* All SQL files are optional.  It might be useful to run an initial load without these scripts so data is available in the work schema for profiling and to write and test appropriate SQL scripts.  Scripts can be placed in the 'Disabled' folder to be ignored by the application
* When running on Windows, the .sql scripts can also be shortcuts ending with '.lnk'.  This is useful if you have multiple flat file formats that populate the same target table.  You can, for example, have the target post-load scripts in a primary folder, with other file format variations having links to the main scripts.  Future support planned for other operating systems.

## Logging

When Logging is enabled via the app_config.ini, two files will be generated:

* YYYY-MM-DD_HHMMSS_flat_file_loader.log
* YYYY-MM-DD_HHMMSS_flat_file_loader_load_summary.json

The first contains basic job logging and will display any errors encountered during the job run.  


The "load_summary.json" file captures basic job metrics and will look similar to the below:

*Note: The calculated durations can be slightly inaccurate when partial seconds are involved*

```json
{
    "jobName": "flat_file_loader",
    "jobScriptPath": "C:/Data Projects/flat_file_loader/main.py",
    "jobFoldersProcessed": "2",
    "jobFilesLoaded": "2",
    "jobRecordsLoaded": "75,826",
    "jobStart": "2023-03-24 08:17:42",
    "jobEnd": "2023-03-24 08:17:51",
    "jobTotalDuration": "9 seconds",
    "folders": [
        {
            "folderPath": "Y:/Python Upload Data Files/Upload/timeday",
            "folderFilesLoaded": "1",
            "folderRecordsLoaded": "73,414",
            "folderStart": "2023-03-24 08:17:43",
            "folderEnd": "2023-03-24 08:17:51",
            "folderTotalDuration": "8 seconds",
            "files": [
                {
                    "fileName": "TimeDay.txt",
                    "fileRecordsLoaded": "73,414",
                    "fileStart": "2023-03-24 08:17:43",
                    "fileEnd": "2023-03-24 08:17:51",
                    "fileTotalDuration": "8 seconds"
                }
            ]
        },
        {
            "folderPath": "Y:/Python Upload Data Files/Upload/timemonth",
            "folderFilesLoaded": "1",
            "folderRecordsLoaded": "2,412",
            "folderStart": "2023-03-24 08:17:51",
            "folderEnd": "2023-03-24 08:17:51",
            "folderTotalDuration": "0 seconds",
            "files": [
                {
                    "fileName": "TimeMonth.txt",
                    "fileRecordsLoaded": "2,412",
                    "fileStart": "2023-03-24 08:17:51",
                    "fileEnd": "2023-03-24 08:17:51",
                    "fileTotalDuration": "0 seconds"
                }
            ]
        }
    ]
}
```

## About the Author

My name is James Larsen, and I have been working professionally as a Business Analyst, Database Architect and Data Engineer since 2007.  While I specialize in Data Modeling and SQL, I am working to improve my knowledge in different data engineering technologies, particularly Python.

[https://www.linkedin.com/in/jameslarsen42/](https://www.linkedin.com/in/jameslarsen42/)