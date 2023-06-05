# Universal Flat File Loader

Welcome to my first Python project.  My primary reason for building this application was to help me learn Python while also addressing a common use-case I have encountered in my career as a Data Engineer and Business Analyst: loading simple and well-formed flat files into a database without needing to build a formal ETL job.

The purpose of this application is to allow users to load any predictable flat file structure into a database using config files and specs.  It incorporates features such as logging, archiving, file expiration and execution of custom post-load SQL scripts.  The current version is focused on loading to an on-premises PostgreSQL database, but will be enhanced to target other platforms in the future.

## Requirements

python = "^3.8"

pandas = "^1.5.3"

sqlalchemy = "^2.0.4"

psycopg2-binary = "2.9.5"

configparser = "^5.3.0"

openpyxl = "^3.1.0"

boto3 = "^1.26.123"

keyring = "^23.13.1" # Optional

nexus-utilities = "^0.2.8" # My custom utilities package

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
pip3 install boto3
pip3 install keyring # Optional
pip3 install nexus-utilities # My custom utilities package
pip3 install pywin32 # Required for Windows machines
```

***As the application uses package-level relative imports, you should add the parent folder containing the "flat_file_loader" folder to your PATH variable.***

## Usage

Configure the following files:

* ./src/config/app_config.ini
* ./src/config/connections_config.ini

Create at least one folder in your "load_file_path" directory.  Configure the following files:

* load_file_path/load_folder/support/file_config.ini
* load_file_path/load_folder/support/mapping_spec_\*.xls\*
* Optional: Any SQL scripts required (see section "SQL Scripts" below for details)

```python
python3 src/main.py
```

## Passwords

The modules for retrieving secured information are located in the nexus-utilities package. The desired method should be specified in the app_config.ini file. All methods accept two required strings of 'password_method' and 'password_key' and a number of optional arguments, and return a string of 'secret_value'.  See the documentation for nexus-utilities at [https://github.com/james-larsen/nexus-utilities](https://github.com/james-larsen/nexus-utilities) for more details

If you do decide to use the keyring library, you will need to add an entry using the "user_name" and "secret_key" from the connections_config.ini file:

```python
keyring.set_password("user_name", "secret_key", "myPassword")
```

## App Configuration

The application is controlled by a number of configuration files, read using the ConfigParser library:

**./src/config/app_config.ini**

Controls the general behavior of the application (example values provided)

``` python
[app_settings]
# Root location of sub-folders to process
load_file_path = C:\Flat Files\Upload
# Root location to place loaded files
archive_file_path = C:\Flat Files\Archive
# Location to place log files
log_file_path = C:\Flat Files\Logs
# Method for retrieving secrets.  Accepts "keyring", "secretsmanager" or "ssm"
password_method = ssm
# Access key for secrets retrieval method
password_access_key = 
# Secret key for secrets retrieval method
password_secret_key = 
# Enpoint URL for secrets retrieval method
password_endpoint_url = 
# Region name for secrets retrieval method
password_region_name = 
# Path for secrets retrieval method
password_password_path = 
# 'read_chunk_size' not currently used; placeholder for future enhancement
read_chunk_size = 100_000
# Whether files should be archived after processing.  Accepts True or False
archive_flag = True
# Whether logs should be generated.  Accepts True or False
logging_flag = True
# Number of days before deleting job logs
log_archive_expire_days = 30
```

**./src/config/connections_config.ini**

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
# Reference value for retrieving the correct password using keyring library
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

## Docker Deployment with S3

A Docker image has been created based on the "tiangolo/uwsgi-nginx" Linux image, with all necessary files and libraries deployed.  It can be found at "jameslarsen42/nexus_flat_file_loader" on DockerHub.  Alternatively, a Dockerfile has been included in this package if you wish to build it yourself.

### **S3 Folder Structure**

The Docker Container uses s3sf fuse to mount an S3 bucket to specific locations used by the application.  An S3 bucket should be created with a certain sub-folder structure.  A template can be found at **./templates/S3 Folder Structure/**.  Note that the "app_config.ini" in this folder has already been optimized to point to the correct locations for Uploads, Archives and Logs, but other settings should be customized before uploading to S3.  Similarly make sure to customize "connections_config.ini" for your target database.  The file "docker.env" is not used within the application, but can be useful when launching the Docker Container, if you prefer to use environment variables rather than storing sensitive information in the .ini files.  

The below environment variables are required to be defined when launching the container in order for the S3 mounts to work:
*  ***AWS_ACCESS_KEY_ID***
*  ***AWS_SECRET_ACCESS_KEY***
*  ***S3_SERVER_PATH***

### **Deploying the Container**

You can specify variables directly if you like, but the simplest method is below, after customizing your "docker.env" file.  Note that the "--cap-add SYS_ADMIN --device /dev/fuse" is necessary for the S3 mounts to work properly.

``` bash
docker run --env-file file/path/to/docker.env --cap-add SYS_ADMIN --device /dev/fuse -it nexus_flat_file_loader
```

### **Triggering the Application**

Once the container is running, you can trigger the application using the below statement:

``` bash
python3 /opt/python_scripts/flat_file_loader/src/main.py
```

## File Analyzer

A special utility script can be found at **./src/spec_builder/spec_builder.py**.  Its purpose is to help you analyze a given flat file, and build a starting point for the spec and SQL scripts used by the application, as well as the stage and target table creation scripts for your target database.  It is designed to be run directly, rather than to be called by the main program.

**Usage:**

First fill out **./src/config/spec_builder_config.ini**:

``` python
[source_file_settings]
# Path to source file being analyzed
source_file_path = C:\Flat Files\Upload\flat_files_01\flat_files_01.csv
# Value to treat as NULL
null_value = <NULL>
# Name of subject area, will be embedded in the names of specs and scripts
subject_area_name = flat_files_01
# Name of initial landing schema for raw data
wrk_schema = my_wrk_schema
# Name of initial landing table in wrk schema.  Leave blank to use "wrk_" + subject_area_name
wrk_table = wrk_flat_files_01
# Name of stage schema
stg_schema = my_stg_schema
# Name of stage table in stage schema.  Leave blank to use "stg_" + subject_area_name
stg_table = stg_flat_files_01
# Name of target schema
tgt_schema = my_tgt_schema
# Name of target table in target schema.  Leave blank to use subject_area_name
tgt_table = my_target_table
```

Once this is filled out, execute **./src/spec_builder/spec_builder.py**.  This will create a "generated_files" subfolder in the spec_builder folder if it does not already exist, and inside there a folder named according to the "subject_area_name" value specified.  The following files will be placed in the folder  
*Note: All .sql files may need to be adjusted slightly if target db is not PostgreSQL*

* mapping_spec_*subject_area_name*.xlsx - The necessary details to tell the application how to read and map your flat file.  It lacks some of the formatting from the "template" version, but is functional as is, or can be pasted into a formatted version of the spec
    * The application will attempt to determine likely data types in your file.  For the stg and tgt table specs, it will attempt to estimate decimal scale and precision, string size, and differentiate dates from timestamps\
    **Important note:** Columns with NULL for every record will be treated as "string" in the wrk table, and will use "varchar(?)" for the stg and tgt fields.  Make sure to manually fix this in the spec as well as the generated SQL files before running
    * For stg and tgt field names, the source file headers will automatically be converted in the following ways: 
        * Values to lowercase
        * Spaces and "(" replaced with "_"
        * ")" removed
        * The following ending keywords updated with a leading "_":\
        'id','cd','code','num','flag','desc','name','amt','qty','price'\
        Eg. "ProductID" to "product_id" and "Amount (USD)" to "amount_usd"
* *subject_area_name*_file_config.txt - Determined extension, delimiter and encoding for your file.  These will be plugged into the **file_config.ini** mentioned above
* *subject_area_name* - table creation scripts.sql - Basic creation scripts for your stg and tgt tables.  Primary key in the tgt table should be updated to be "NOT NULL" and the CONSTRAINT definiton updated
* wrk to stg load - *subject_area_name*.sql - Deletes current contents of the stg table and loads from the wrk table
* stg to tgt load - *subject_area_name*.sql - Deletes from tgt table based on matching records from stg table and loads from the stg table.  Make sure to update the primary key join point between stg and tgt tables

As an additional feature, if you specify a folder for "source_file_path", it will scan the entire folder for all files of type .csv, .txt, .tsv, .dat or .tab and process them automatically.  Note that in this case the "subject_area_name", "wrk_table", "stg_table" and "tgt_table" values from the config file will be ignored.  Instead it will infer the "subject_area_name" from the file name, and use it for all table names.

**Rebuilding Scripts from Spec:**

Once the initial files are built, you can have the .sql scripts regenerated from the spec using the **./src/spec_builder/script_updater.py**.  This is useful if you want to make minor tweaks in the spec and have the table creation and load scripts rebuilt.  Note that this script makes many assumptions about the spec structure, so I recommend you only change stage and target field names and data types (Eg. change a Decimal precision and scale, Varchar length, etc.).

It can be used as follows:

```python
python script_updater.py -fp 'path/to/mapping/spec/mapping_spec_name.xlsx'
```

## About the Author

My name is James Larsen, and I have been working professionally as a Business Analyst, Database Architect and Data Engineer since 2007.  While I specialize in Data Modeling and SQL, I am working to improve my knowledge in different data engineering technologies, particularly Python.

[https://www.linkedin.com/in/jameslarsen42/](https://www.linkedin.com/in/jameslarsen42/)