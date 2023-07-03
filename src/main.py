"""Parse and load flat files"""
#%%
import sys
import os
import platform
import pathlib
from threading import Lock
import shutil
import argparse
import glob
import logging
#import time
import datetime
import warnings
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from flask import Flask, request#, make_response, jsonify
import traceback
import re
import json
from collections import OrderedDict
import copy
import gzip
import zipfile
import bz2
import lzma
# pylint: disable=import-error
from nexus_utils.package_utils import add_package_to_path#, import_relative
package_root_dir, package_root_name = add_package_to_path()
# import_relative('flat_file_loader', 'src.utils.build_engine', 'build_engine', caller_globals=globals())
# import_relative(package_root_name, 'src.utils.build_engine', 'build_engine')
from nexus_utils.database_utils import build_engine
# import_relative(package_root_name, 'src.utils', 'config_reader', alias='cr')
from nexus_utils import config_utils as cr
# import_relative(package_root_name, 'src.utils', 'clean_sql')
from nexus_utils.database_utils import clean_sql_statement
# import_relative(package_root_name, 'src.utils', 'time_difference')
from nexus_utils.datetime_utils import get_current_timestamp, get_duration
# import_relative(package_root_name, 'src.utils.current_timestamp', 'get_current_timestamp')
# from nexus_utils.datetime_utils import get_current_timestamp
from nexus_utils import password_utils as pw
# from nexus_utils import string_utils
# import api_response
# pylint: enable=import-error

if platform.system() == 'Windows':
    import win32com.client

# pylint: disable=line-too-long
# pylint: disable=trailing-whitespace
# pylint: disable=redefined-outer-name

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

flat_file_loader_app = Flask(__name__)
flat_file_loader_app.config['JSON_SORT_KEYS'] = False
lock = Lock()

class NoPrint:
    def write(self, x):
        pass
    
    def flush(self):
        pass

#%%

def run_flask_app(host=None, port=None, verbose_flag=False, _logging_flag=False):
    from waitress import serve
    global logging_flag

    if not host:
        host = 'localhost'

    host = os.getenv('NEXUS_FFL_API_HOST', host)

    if not port:
        port = 5001

    port = os.getenv('NEXUS_FFL_API_PORT', port)

    print(f'Listening on {host}:{port}')
    print('Press Ctrl+C to exit')

    # Disable print functionality
    if not verbose_flag:
        sys.stdout = NoPrint()
    
    logging_flag = _logging_flag
    
    serve(flat_file_loader_app, host=host, port=port)
    # flat_file_loader_app.run(host=host, port=port)

common_function_aliases = {
    'run_all': 'run_all',
    'all': 'run_all',
    'process_folder': 'process_folder',
    'pf': 'process_folder'
}

api_function_aliases = {
    
}

@flat_file_loader_app.route('/request', methods=['POST'])
def trigger_function_from_api():
    
    global job_errors_or_warnings
    
    # Process one request at a time
    with lock:
        try:
            per_run_initializations()
            
            params = request.args.to_dict()
            # print(request)
            # print(params)
        
            # payload = request.json

            function_aliases = common_function_aliases.copy()
            function_aliases.update(api_function_aliases)

            # function = payload['function_name']
            # kwargs = payload.get('kwargs', {})
            function = params.get('function', None)

            function_lookup = function_aliases.get(function, function)

            if function is None:
                function = 'run_all'
            elif function and not function_lookup:
                valid_function_values = set(function_aliases.values())
                valid_functions_string = ''.join([f'\n--{value}' for value in valid_function_values])
                print(f'Invalid function specified.  Valid values are:{valid_functions_string}')
                return  {'error': f'Unknown function: "{function}"'}, 400
            elif function and function_lookup:
                function = function_lookup
            
            folder_to_process = params.get('folder_to_process', None)
            # env_values = params.get('env_values', None)
            # if env_values:
            #     cr.process_env_file(env_values)

            call_function(function, folder_to_process=folder_to_process)
            
            my_api_response.build_api_response(flat_file_loader_app.app_context(), function, job_errors_or_warnings)

            return my_api_response.api_response
        
        except Exception as e:
            print(f'Error: {str(e)}')
            if my_api_response.api_error_flag == None:
                my_api_response.api_error_flag = True
            if not my_api_response.api_message:
                my_api_response.api_message = str(e)
            my_api_response.build_api_response(flat_file_loader_app.app_context(), function, job_errors_or_warnings)
            return my_api_response.api_response

cli_function_aliases = {
    'api_listener': 'api_listener'
}

def parse_command_run_arguments():
    """Interprets the arguments passed into the command line to run the correct function"""
    # global folder_to_process

    parser = argparse.ArgumentParser(description='Process flat files')

    function_aliases = cli_function_aliases.copy()
    function_aliases.update(common_function_aliases)

    parser.add_argument('function', nargs='?', default='run_all', choices=function_aliases.keys(), help='Function to call')
    parser.add_argument('-host', '--api_host', type=str, help='Hostname for Flask API listener')
    parser.add_argument('-p', '--port', type=str, help='Port for Flask API listener')
    parser.add_argument('-v', '--verbose_flag', action='store_true', help='Whether to allow stdout while in API listener mode')
    parser.add_argument('-l', '--logging_flag', action='store_true', help='Whether to enable local logs while in API listener mode')
    parser.add_argument('-f', '--folder_to_process', type=str, help='Folder to process')
    parser.add_argument('-env', '--env_file', type=str, help='Path to environment variables file')

    if not DEBUGGING_MODE:
        args = parser.parse_args()
    else:
        # provide test values when developing / debugging
        args = argparse.Namespace(
            function='run_all'
        )
        # args = argparse.Namespace(
        #     function='process_folder',
        #     folder_to_process='no_folder'
        # )

    # # function = args.function
    function = function_aliases.get(args.function, args.function)
    host = getattr(args, 'api_host', None)
    port = getattr(args, 'port', None)
    verbose_flag = getattr(args, 'verbose_flag', False)
    logging_flag = getattr(args, 'logging_flag', False)
    folder_to_process = getattr(args, 'folder_to_process', None)
    env_values = getattr(args, 'env_values', None)
    if env_values:
        cr.process_env_file(env_values)

    if function:
        call_function(
                function,
                host,
                port,
                verbose_flag,
                logging_flag,
                folder_to_process=folder_to_process)
    else:
        valid_function_values = set(function_aliases.values())
        valid_functions_string = ''.join([f'\n--{value}' for value in valid_function_values])
        print(f'Invalid function specified.  Valid values are:{valid_functions_string}')
        return

def call_function(
        function,
        host=None,
        port=None,
        verbose_flag=None,
        logging_flag=None,
        folder_to_process=None):

    if function == 'api_listener':
        run_flask_app(host=host, port=port, verbose_flag=verbose_flag, _logging_flag=logging_flag)
    elif function == 'run_all':
        main_run()
    elif function == 'process_folder':
        main_run(folder_to_process)

def convert_to_date(value):
    try:
        return pd.to_datetime(value)
    except pd.errors.OutOfBoundsDatetime:
        return pd.NaT

def read_app_config_settings(input_app_config_path):
    """Read app configuration parameters"""
    input_app_config_path = pathlib.Path(input_app_config_path)
    app_config = cr.read_config_file(input_app_config_path)  # type: ignore
    local_config_entry = 'app_settings'
    # local_environment = app_config[config_entry]['environment']
    # local_load_file_path = app_config[local_config_entry]['load_file_path']
    # local_archive_file_path = app_config[local_config_entry]['archive_file_path']
    # local_read_chunk_size = int(app_config[local_config_entry]['read_chunk_size'])

    return (
        app_config[local_config_entry]['load_file_path'],
        app_config[local_config_entry]['archive_file_path'],
        app_config[local_config_entry]['log_file_path'],
        app_config[local_config_entry]['password_method'],
        app_config[local_config_entry]['password_access_key'],
        app_config[local_config_entry]['password_secret_key'],
        app_config[local_config_entry]['password_endpoint_url'],
        app_config[local_config_entry]['password_region_name'],
        app_config[local_config_entry]['password_password_path'],
        int(app_config[local_config_entry]['read_chunk_size']),
        app_config[local_config_entry]['archive_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'archive'],
        app_config[local_config_entry]['logging_flag'].lower() in ['t', 'true', 'y', 'yes', '1', 'on', 'log'],
        int(app_config[local_config_entry]['log_archive_expire_days'])
        )

def read_connection_config_settings(input_connection_config_path, db_target_config):
    """Read database connection configuration"""
    db_config = cr.read_config_file(input_connection_config_path)
    connect_type = db_config[db_target_config]['connect_type']
    #environment = db_config[db_target_config]['environment']
    server_address = db_config[db_target_config]['server_address']
    server_port = db_config[db_target_config]['server_port']
    server_name = db_config[db_target_config]['server_name']
    schema = db_config[db_target_config]['schema']
    user_name = db_config[db_target_config]['user_name']
    secret_key = db_config[db_target_config]['secret_key']

    return (
        connect_type, server_address, server_port, server_name, schema, user_name, secret_key
    )

def read_file_config_settings(input_file_config_path):
    """Read file configuration parameters"""
    file_config = cr.read_config_file(input_file_config_path)  # type: ignore
    config_entry = 'file_settings'
    extension_cleansed = file_config[config_entry]['extension']
    if extension_cleansed[0] == '.':
        extension_cleansed = extension_cleansed[1:]
    #print(extension)
    # local_delimiter = file_config[config_entry]['delimiter']
    # local_encoding = file_config[config_entry]['encoding']
    # local_null_value = file_config[config_entry]['null_value'] # Determine best way to accommodate multiple values
    return (
        extension_cleansed,
        file_config[config_entry]['delimiter'],
        file_config[config_entry]['encoding'],
        file_config[config_entry]['null_value'], # Determine best way to accommodate multiple values
        int(file_config[config_entry]['quoting']),
        int(file_config[config_entry]['read_chunk_size']),
        int(file_config[config_entry]['archive_expire_days'])
        )

def connection_test(engine, schema):
    """Confirm database connection works and wrk_schema can be accessed"""
    try:
        with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            create_table_statement = text(f"CREATE TABLE {schema}.access_test (id INT)")
            conn.execute(create_table_statement)

            insert_statement = text(f"INSERT INTO {schema}.access_test (id) VALUES (1)")
            conn.execute(insert_statement)

            select_statement = text(f"SELECT * FROM {schema}.access_test")
            result = conn.execute(select_statement)
            records = result.fetchall()

            drop_table_statement = text(f"DROP TABLE {schema}.access_test")
            conn.execute(drop_table_statement)
            return 'Success'
    except OperationalError as e:
        full_error_message = traceback.format_exc()
        error_message = extract_from_error(full_error_message, 'sqlalchemy.exc')
        if error_message:
            return error_message
        else:
            return full_error_message

def extract_from_error(full_error_message, error_keyword):
    """Extract a single line from error message based on keyword"""
    
    error_lines = full_error_message.splitlines()
    error_message = next((line for line in error_lines if error_keyword in line), None)
    
    return error_message

def run_sql_statements(input_support_path, folder):
    """Run available SQL statements"""

    global job_errors_or_warnings

    sql_errors = 0
    
    try:
        exception_handled = False
        
        script_patterns = [
            {'script_type': 'WRK to STG Load Scripts', 'pattern': 'wrk to stg load*.sql*'},
            {'script_type': 'STG Post-Load Scripts', 'pattern': 'stg post-load*.sql*'},
            {'script_type': 'STG to Target Load Scripts', 'pattern': 'stg to target load*.sql*'},
            {'script_type': 'Target Post-Load Scripts', 'pattern': 'target post-load*.sql*'}
        ]
        for pattern in script_patterns:
            script_type = pattern['script_type']
            script_pattern = pattern['pattern']
            script_path = glob.glob(f'{input_support_path}/{script_pattern}')
            print(f'Running "{script_type}":')
            logger.info('Running "%s":', script_type)
            script_path_length = len(script_path)
            if script_path_length > 0:
                for i, script in enumerate(script_path):
                    if pathlib.Path(script_path[i]).suffix == '.lnk':
                        if os_platform == 'Windows':
                            shell = win32com.client.Dispatch("WScript.Shell")
                            shortcut = shell.CreateShortCut(script)
                            if pathlib.Path(shortcut.Targetpath).is_file():
                                script = shortcut.Targetpath
                            else: 
                                script_path_length -= 1
                                logger.error('Script "%s" referenced by shortcut "%s" not found', shortcut.Targetpath, script_path[i])
                                job_errors_or_warnings += 1
                                continue
                        else:
                            script_path_length -= 1
                            logger.info('OS is not windows, skipping script: "%s"', script_path[i])
                            continue
                    print(f'    Running script: "{script}"')
                    logger.info('Running script: "%s"', script)
                    file_reader = open(pathlib.Path(script), 'r', encoding='utf-8')
                    full_sql_statement = file_reader.read()
                    file_reader.close()
                    sql_statements = clean_sql_statement(full_sql_statement)
                    for i, sql_statement in enumerate(sql_statements):
                        exception_handled = False
                        try:
                            statement_action = ''
                            if re.search(r'(?i)\bUPDATE\b[\s\n]', sql_statement):
                                statement_action = 'updated'
                            elif re.search(r'(?i)\bDELETE\b[\s\n]', sql_statement):
                                statement_action = 'deleted'
                            elif re.search(r'(?i)\bINSERT\b[\s\n]', sql_statement):
                                statement_action = 'inserted'
                            with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                                result_proxy = conn.execute(text(sql_statement))
                            # print(sql_statement)
                            print_statement = f'        Running SQL statement number {i + 1}'
                            log_statement = f'Running SQL statement: \n"{sql_statement}"'
                            if result_proxy.rowcount and statement_action:
                                affected_rows_string = str(format(result_proxy.rowcount, ','))
                                print_statement += f': {affected_rows_string} rows {statement_action}'
                                log_statement += f'\n\t\t{affected_rows_string} rows {statement_action}'
                            print(print_statement)
                            # logger.debug('Running SQL statement: \n"%s"', sql_statement)
                            logger.info(log_statement)
                            #engine.execute(text(sql_statement), schema=schema)
                            print('            Success')
                        except Exception as e: # pylint: disable=broad-except
                            full_error_message = traceback.format_exc()
                            error_message = extract_from_error(full_error_message, 'sqlalchemy.exc')
                            print(f'            Error running SQL statement, see log for more details: "{error_message}"')
                            logger.error('Error running SQL statement:\n%s', full_error_message)
                            job_errors_or_warnings += 1
                            sql_errors += 1
                            exception_handled = True
            if script_path_length == 0:
                print(f'    No "{script_type}" present')
                logger.info('No "%s" matching pattern "%s" present', script_type, script_pattern)
    except Exception as e: # pylint: disable=broad-except
        if not exception_handled:
            print(e)
            logger.error('Error encountered:\n', exc_info=True)
            job_errors_or_warnings += 1
            sql_errors += 1

    if sql_errors > 0:
        folder['folderSQLErrors'] = f'ERROR: Folder encountered {sql_errors} errors while running SQL statements, see log for details'

def delete_expired_files(target_folder, archive_expire_days):
    """Delete archive files and empty folders based on retention policy"""
    
    # Delete files past their expiration date
    deleted_files = ''
    
    for root, dirs, files in os.walk(target_folder, topdown=False):
        for name in files:
            today = datetime.datetime.today()
            file_path = os.path.join(root, name)
            modified_date = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            duration = today - modified_date
            if duration.days > archive_expire_days:
                os.remove(file_path)
                deleted_files += '\n' + file_path
    
    if len(deleted_files) > 0:
        logger.info('Removing expired file(s): %s', deleted_files)
    
    # Delete empty directories
    deleted_dirs = ''

    for root, dirs, files in os.walk(target_folder, topdown=False):
        for name in dirs:
            directory = os.listdir(os.path.join(root, name))
            if len(directory) == 0:
                dir_to_remove = os.path.join(root, name)
                os.rmdir(dir_to_remove)
                deleted_dirs += '\n' + dir_to_remove
    
    if len(deleted_dirs) > 0:
        logger.info('Removing empty directories(s): %s', deleted_dirs)

def log_metrics():
    """Log performance metrics for loads"""
    global job, job_name, folders, load_summary_file_path, job_records_loaded, logging_flag, my_api_response
    job_end_time, job_end_time_string, job_end_time_log = get_current_timestamp()
    days, hours, minutes, seconds, display_string = get_duration(job_start_time, job_end_time)
    job['jobName'] = job_name
    job['jobScriptPath'] = current_script_path
    job['jobFoldersProcessed'] = str(format(job_folders_processed, ','))
    job['jobFilesLoaded'] = str(format(job_files_loaded, ','))
    job['jobRecordsLoaded'] = str(format(job_records_loaded, ','))
    job['jobStart'] = job_start_time_log
    job['jobEnd'] = job_end_time_log
    job['jobTotalDuration'] = display_string
    if job_bad_files > 0:
        job['jobBadFiles'] = str(format(job_bad_files, ','))
    job['folders'] = folders

    if job_records_loaded > 0 and logging_flag:
        with open(load_summary_file_path, 'w') as f:
            json.dump(job, f, indent=4, default=str)

        with open(load_summary_file_path, 'r') as f:
            data = f.read()
            #data = data.replace("\\\\", "\\")
            data = data.replace("\\\\", "/")
        with open(load_summary_file_path, 'w') as f:
            f.write(data)

    logger.info('Job start:%s, job end: %s, total duration: %s', job_start_time_log, job_end_time_log, display_string)

    my_api_response.api_result = copy.deepcopy(job)

def get_raw_row_count(file_path, encoding='utf-8'):
    file_path = str(file_path)
    if file_path.endswith('.gz'):
        with gzip.open(file_path, 'rt', encoding=encoding) as csvfile:
            original_row_count = sum(1 for line in csvfile)
    elif file_path.endswith('.zip'):
        with zipfile.ZipFile(file_path) as zip_file:
            first_file = zip_file.namelist()[0]
            with zip_file.open(first_file, 'r') as csvfile:
                original_row_count = sum(1 for line in csvfile)
    elif file_path.endswith('.bz2'):
        with bz2.open(file_path, 'rt', encoding=encoding) as csvfile:
            original_row_count = sum(1 for line in csvfile)
    elif file_path.endswith('.xz'):
        with lzma.open(file_path, 'rt', encoding=encoding) as csvfile:
            original_row_count = sum(1 for line in csvfile)
    else:
        with open(file_path, 'r', encoding=encoding) as csvfile:
            original_row_count = sum(1 for line in csvfile)

    if original_row_count > 0:
        original_row_count -= 1

    return original_row_count

def load_file(path, files_to_process, extension, delimiter, encoding, null_value, quoting, file_read_chunk_size, archive_expire_days):
    """Load files to be processed"""

    global engine, schema

    # Read file spec
    load_folder = os.path.basename(os.path.normpath(path))
    spec_path = glob.glob(f'{support_path}/mapping_spec_*.xls*')
    if spec_path is None or len(spec_path) == 0:
        logger.info('No valid spec file at "%s%s" matching pattern "mapping_spec_*.xls*"', support_path, os.sep)
        #continue
        return False

    df_spec = pd.read_excel(spec_path[0], sheet_name='Fields & Mappings')

    df_spec.columns = df_spec.columns.str.replace(' ', '_').str.lower()
    for char in ['(', ')', '[', ']']:
        df_spec.columns = df_spec.columns.str.replace(char, '', regex= False)

    # df_spec.columns = df.columns.map(lambda col: string_utils.cleanse_string(col, title_to_snake_case=True))
    
    df_columns = df_spec.query("source_system == 'Flat File'").copy()

    # df_columns['source_fields'] = df_columns['source_fields'].apply(str.lower)
    df_columns.loc[:, 'source_fields'] = df_columns['source_fields'].str.lower()

    wrk_table = df_columns.loc[1].at["target_table"]
    
    # Drop wrk table if it already exists
    if wrk_table != 'python_test_case':
        try:
            drop_sql_statement = f"drop table if exists {schema}.{wrk_table};"
            with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                conn.execute(text(drop_sql_statement))
            #engine.execute(text(drop_sql_statement), schema=schema)
            logger.info('Running SQL statement: "%s"', drop_sql_statement)
        except Exception as e: # pylint: disable=broad-except
            print(e)
            #logger.error(e, exc_info=True)
            pass

    break_out_flag = False # pylint: disable=invalid-name
    global job_folders_processed, job_files_loaded, job_bad_files, job_errors_or_warnings
    folder_files_loaded = 0 # pylint: disable=invalid-name
    folder_bad_files = 0 # pylint: disable=invalid-name
    folder_records_loaded = 0 # pylint: disable=invalid-name
    files = []
    folder_start_time, folder_start_time_string, folder_start_time_log = get_current_timestamp()
    bad_files_folder = pathlib.Path(path + os.sep + 'bad_files' + os.sep + job_start_time_string)
    
    for file_path in files_to_process:
        try:
            file = {}
            file_start_time, file_start_time_string, file_start_time_log = get_current_timestamp()  # type: ignore
            logger.info('Loading file "%s"', file_path)
            #print(file_path)
            load_file = file_path.name

            file_verified = False # pylint: disable=invalid-name
            
            file_records_loaded = 0
            global job_records_loaded

            # get raw row count
            # with open(file_path, "rb"):
            #     with open(file_path, "r", encoding=encoding) as csvfile:
            #         original_row_count = sum(1 for line in csvfile)
            #         if original_row_count > 0:
            #             original_row_count -= 1
            original_row_count = get_raw_row_count(file_path, encoding)
            
            for n, chunk in (enumerate(
                pd.read_csv(file_path,
                    sep=delimiter,
                    dtype=str,
                    encoding=encoding,
                    na_values={null_value, '', '#N/A', 'NaN', 'NULL', 'null', 'Null', '<NULL>', '[NULL]', '(NULL)', '#VALUE!', '#DIV/0!', '#REF!', '#NUM!', '#NAME?'},
                    keep_default_na=False,
                    chunksize=file_read_chunk_size,
                    quoting=quoting,
                    # quotechar='"',
                    engine='python',
                    compression='infer',
                    # error_bad_lines=True,
                    # warn_bad_lines=False,
                    on_bad_lines='error'
                    #,nrows=100_000
                    #,nrows=10
                    ))):
                df = chunk

                # Remove quotes from all fields
                if quoting != 3:
                    df = df.applymap(lambda x: x.replace('"', '') if isinstance(x, str) else x)
                    df.replace('', None, inplace=True)
                    df.columns = [col.replace('"', '') for col in df.columns]

                df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()
                for char in ['(', ')', '[', ']']:
                    df.columns = df.columns.str.replace(char, '', regex= False)

                # df.columns = df.columns.map(lambda col: string_utils.cleanse_string(col, title_to_snake_case=True))

                spec_column_list = df_columns.source_fields.values.tolist()
                spec_column_list = [column.replace(' ', '_').replace('-', '_').lower() for column in spec_column_list]
                spec_column_list = [column.replace('(', '').replace(')', '').replace('[', '').replace(']', '') for column in spec_column_list]

                df['load_file_name'] = file_path.name
                df['load_timestamp'] = job_start_time
                #df = df.drop(columns=['boxofficerank']) # test for file doesn't match spec
            
                # Check if load file matches spec
                if not file_verified:
                    load_column_list = list(df.columns.values)
                    # spec_column_list = df_columns.source_fields.values.tolist()
                    if len(list(set(spec_column_list).difference(load_column_list))) == 0:
                        file_verified = True # pylint: disable=invalid-name
                    else:
                        print('Load file does not match spec')
                        logger.error('Load file "%s" does not match spec, moved to "%s%s"', load_file, bad_files_folder, os.sep)
                        break_out_flag = True # pylint: disable=invalid-name
                        break
                
                for row in range(len(df_columns)):
                    #print(df_columns.loc[row + 1].at["type"])
                    try:
                        # column_name = df_columns.loc[row + 1].at["source_fields"]
                        column_name = spec_column_list[row]
                        if df_columns.loc[row + 1].at["type"] == 'float64':
                            df[column_name] = pd.to_numeric(df[column_name])#, errors='coerce')
                        elif df_columns.loc[row + 1].at["type"] in ['date', 'timestamp'] and column_name != 'load_timestamp':
                            # try:
                            #     df[column_name] = pd.to_datetime(df[column_name])
                            # except pd.errors.OutOfBoundsDatetime as e:
                            #     # df[column_name] = pd.to_datetime('1900-01-01')
                            #     df[column_name] = pd.NaT
                            df[column_name] = df[column_name].apply(convert_to_date)
                    except Exception as e: # pylint: disable=broad-except
                        print(e)
                        logger.error('Error attempting to convert field "%s" to type "%s":', column_name, df_columns.loc[row + 1].at["type"], exc_info=True)
                        break_out_flag = True # pylint: disable=invalid-name
                        break

                df.to_sql(wrk_table, engine, schema = schema, if_exists = 'append', index= False)
                file_records_loaded += len(df)
                print(f"Table {schema}.{wrk_table} - rows "+ str(format((n * file_read_chunk_size) + 1, ',')) +" - " + str(format((n * file_read_chunk_size) + len(df), ',')) + " (" + str(format(len(df), ',')) + " records) loaded successfully")
        except Exception as e: # pylint: disable=broad-except
            print(e)
            logger.error('Error encountered:', exc_info=True)
            # job_errors_or_warnings += 1
            break_out_flag = True # pylint: disable=invalid-name
            
        if break_out_flag:
            if not os.path.exists(bad_files_folder):
                os.makedirs(bad_files_folder)
            folder_bad_files += 1
            job_bad_files += 1
            job_errors_or_warnings += 1
            file['fileName'] = load_file
            file['fileStart'] = file_start_time_log
            new_path = pathlib.Path(bad_files_folder / load_file)
            pathlib.Path(file_path).rename(new_path)
            file['fileError'] = f'ERROR: File not loaded.  Moved to "{new_path}".  See log for more details'
            continue
        
        # attempt more accurate way of determine records loaded
        # DOESN'T WORK WITH MULTIPLE FILES
        # count_sql_statement = f'select count(*) from {schema}.{wrk_table};'
        # try:
        #     with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
        #         result = conn.execute(text(count_sql_statement))
        #         record_count = result.scalar()
        #         if record_count:
        #             file_records_loaded = record_count
        # except Exception as e: # pylint: disable=broad-except
        #     pass
        
        folder_files_loaded += 1
        folder_records_loaded += file_records_loaded
        # job_files_loaded += 1
        job_records_loaded += file_records_loaded
        file_end_time, file_end_time_string, file_end_time_log = get_current_timestamp()
        days, hours, minutes, seconds, display_string = get_duration(file_start_time, file_end_time)
        original_row_count_string = str(format(original_row_count, ','))
        file_records_loaded_string = str(format(file_records_loaded, ','))
        file['fileName'] = load_file
        file['fileRawRecords'] = original_row_count_string
        file['fileRecordsLoaded'] = file_records_loaded_string
        file['fileStart'] = file_start_time_log
        file['fileEnd'] = file_end_time_log
        file['fileTotalDuration'] = display_string
        if file_records_loaded != original_row_count and file_records_loaded > 0:
            loaded_rows_difference_string = str(format(original_row_count - file_records_loaded, ','))
            file['fileLoadError'] = f'ERROR: {file_records_loaded_string} records loaded of {original_row_count_string} in source file ({loaded_rows_difference_string} records difference)'
            job_errors_or_warnings += 1
            logger.info('%s records loaded successfully.  WARNING: This is less than the source file contained (%s records).', file_records_loaded_string, original_row_count_string)
        else:
            logger.info('%s records loaded successfully', format(file_records_loaded, ','))
        files.append(dict(file))

        # Archive file
        if archive_flag:
            archive_file(file_path, load_folder, load_file)
            # if not os.path.exists(archive_file_path):
            #     os.makedirs(archive_file_path)
            # archive_folder = pathlib.Path(archive_file_path + os.sep + load_folder + os.sep + job_start_time_string)
            # if not os.path.exists(archive_folder):
            #     os.makedirs(archive_folder)
            # new_path = pathlib.Path(archive_folder / load_file)
            # # Update the modification timestamp of the file
            # pathlib.Path(file_path).touch()
            # shutil.move(file_path, new_path)
        
        # # Archive file
        # if archive_flag:
        #     if not os.path.exists(archive_file_path):
        #         os.makedirs(archive_file_path)
        #     archive_folder = pathlib.Path(archive_file_path + os.sep + load_folder + os.sep + job_start_time_string)
        #     if not os.path.exists(archive_folder):
        #         os.makedirs(archive_folder)
        #     new_path = pathlib.Path(archive_folder / load_file)
        #     # Update the modification timestamp of the file
        #     pathlib.Path(file_path).touch()
        #     pathlib.Path(file_path).rename(new_path)
    
    if folder_files_loaded > 0:
        job_folders_processed += 1
        job_files_loaded += folder_files_loaded
        
        global folder, folders
        folder_end_time, folder_end_time_string, folder_end_time_log = get_current_timestamp()  # type: ignore
        days, hours, minutes, seconds, display_string = get_duration(folder_start_time, folder_end_time)  # type: ignore
        folder['folderPath'] = os.path.normpath(path)
        folder['folderFilesLoaded'] = str(format(folder_files_loaded, ','))
        folder['folderRecordsLoaded'] = str(format(folder_records_loaded, ','))
        folder['folderStart'] = folder_start_time_log
        folder['folderEnd'] = folder_end_time_log
        folder['folderTotalDuration'] = display_string
        if folder_bad_files > 0:
            folder['folderBadFiles'] = str(format(folder_bad_files, ','))
        run_sql_statements(support_path, folder)
        folder['files'] = files
        folders.append(dict(folder))

    return True

def process_folders(load_file_path, archive_file_path, log_file_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days, logger, folder_to_process=None):
    global support_path
    if logger is None:
        logger = logging.getLogger()
        logger.disabled = True
    # Build list of folders to process by checking for at least one file or the specified type
    path_list = glob.glob(f'{load_file_path}/*/')
    if folder_to_process:
        path_list = [path for path in path_list if os.path.basename(path.rstrip(os.sep)) == folder_to_process]
    for path in path_list:
        # Skip "[Ignore]" folder
        if os.path.basename(os.path.normpath(path)) == '[Ignore]':
            continue
        #path = r'test_path.txt'
        #print(path)
        # folder_start_time = datetime.datetime.now()
        logger.info('Processing folder: "%s"', path)
        support_path = pathlib.Path(path + 'support')
        file_config_path = pathlib.Path(os.path.join(support_path, 'file_config.ini'))

        # Check for valid config file
        if not os.path.isfile(file_config_path):
            print(f'File not found: "{file_config_path}"')
            logger.info('No valid config file at "%s"', file_config_path)
            continue
        extension, delimiter, encoding, null_value, quoting, file_read_chunk_size, archive_expire_days = read_file_config_settings(file_config_path)

        # Delete expired archived files
        if archive_file_path != '':
            delete_expired_files(pathlib.Path(archive_file_path + os.sep + os.path.basename(os.path.normpath(path))), archive_expire_days)
        
        # Delete expired log files
        if log_file_path != '':
            delete_expired_files(pathlib.Path(log_file_path), log_archive_expire_days)
        
        # Determine folders with files to process
        #files_to_process = list((p.resolve() for p in pathlib.Path(path).glob("**/*") if p.suffix in {"." + extension, ".gz", ".zip", ".bz2", ".xz"}))
        files_to_process = list((p.resolve() for p in pathlib.Path(path).glob("*") if p.is_file() and p.suffix in {"." + extension, ".gz", ".zip", ".bz2", ".xz"}))

        if files_to_process is None or len(files_to_process) == 0:
            continue

        if not load_file(path, files_to_process, extension, delimiter, encoding, null_value, quoting, file_read_chunk_size, archive_expire_days):
            continue

    log_metrics()

def archive_file(file_path, load_folder, load_file):
    """Archive loaded file"""
    global archive_file_path, job_start_time_string

    if not os.path.exists(archive_file_path):
        os.makedirs(archive_file_path)
    archive_folder = pathlib.Path(archive_file_path + os.sep + load_folder + os.sep + job_start_time_string)
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    new_path = pathlib.Path(archive_folder / load_file)
    # Update the modification timestamp of the file
    pathlib.Path(file_path).touch()
    shutil.move(file_path, new_path)

def main_run(folder_to_process=None):
    
    global DEBUGGING_MODE, project_dir, my_api_response, fatal_error, engine, schema, load_file_path, archive_file_path, log_file, log_file_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days, current_script_path, load_summary_file_path, job_start_time_string, job_errors_or_warnings, job_records_loaded, log_file, log_file_name, local_log_file_path, final_log_file_path, logger, job
    
    try:
        # Logger settings
        # if logging_flag:
        #     if not os.path.exists(log_file_path):
        #         os.makedirs(log_file_path)
        #     log_file = pathlib.Path(log_file_path) / f'{job_start_time_string}_flat_file_loader.log'
        #     # logging.getLogger().handlers.clear()
        #     logging.basicConfig(filename=log_file, 
        #         filemode='w', 
        #         level=logging.INFO, 
        #         format='%(asctime)s - %(name)s - %(levelname)s - \n\t%(message)s\n')
        #     logger = logging.getLogger(__name__)
        #     # Update the log file location for subsequent runs
        #     handler = logger.handlers[0]
        #     handler.baseFilename = str(log_file)
        #     handler.close()
        #     handler.stream = None
        # print(f'Logging flag: {logging_flag}')
        if logging_flag:
            # if not os.path.exists(log_file_path):
            #     os.makedirs(log_file_path)
            log_file_name = f'{job_start_time_string}_flat_file_loader.log'
            local_log_file_path = pathlib.Path(project_dir) / 'logging_local' / log_file_name
            final_log_file_path = pathlib.Path(log_file_path) / log_file_name
            # log_file = pathlib.Path(log_file_path) / log_file_name
            log_file = local_log_file_path
            # print(f'Log File: {str(log_file)}')
            
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            logger.propagate = False

            # Create new file handler
            new_file_handler = logging.FileHandler(log_file)
            new_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - \n\t%(message)s\n'))
            
            # If logger has already handlers, remove them before adding new one
            if logger.hasHandlers():
                logger.handlers.clear()
            
            logger.addHandler(new_file_handler)
            # print(f'Logger location: {next((handler for handler in logger.handlers if isinstance(handler, logging.FileHandler)), None).baseFilename}')
        else:
            logger = logging.getLogger()
            logger.disabled = True
        current_script_path = pathlib.Path(os.getcwd() + os.sep + os.path.basename(__file__)).resolve()
        logger.info('Start script: %s', current_script_path)
        logger.info('Build engine using config entry: %s', db_target_config)

        # (job_start_time, job_start_time_string, job_start_time_log, os_platform, support_path, logger, job_name, job, folders, folder, job_folders_processed, job_files_loaded, job_bad_files, job_records_loaded, db_target_config, engine, schema) = setup_globals().values()
        # app_config_path = pathlib.Path(os.getcwd() + '/config/app_config.ini')
        connect_type, server_address, server_port, database_name, schema, user_name, secret_key = read_connection_config_settings(connection_config_path, db_target_config)
        if 'NEXUS_FFL_TARGET_DB_PASSWORD' in os.environ:
            db_password = os.environ['NEXUS_FFL_TARGET_DB_PASSWORD']
        else:
            db_password = pw.get_password(password_method, secret_key, account_name=user_name, access_key=password_access_key, secret_key=password_secret_key, endpoint_url=password_endpoint_url, region_name=password_region_name, password_path=password_password_path)
        
        # engine, schema = build_engine(pathlib.Path(connection_config_path), db_target_config, password_method)  # type: ignore

        engine = build_engine(connect_type, server_address, server_port, database_name, user_name, db_password)#, schema)
        connection_result = connection_test(engine, schema)
        if connection_result != 'Success':
            fatal_error = True
            logger.error(f'Error accessing database: \n{connection_result}')
            print(f'Fatal Error: \n{connection_result}')
            my_api_response.api_error_flag = True
            my_api_response.api_message = f'Connection Error: {connection_result}'
        
        if not fatal_error:
            process_folders(load_file_path, archive_file_path, log_file_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days, logger, folder_to_process)

            if job_errors_or_warnings > 0:
                print(f'ERRORS OR WARNINGS ENCOUNTERED\nSee the below files for details:\n{str(final_log_file_path)}\n{str(load_summary_file_path)}')
                my_api_response.api_message = f'ERRORS OR WARNINGS ENCOUNTERED\nSee the following files for details:\n{str(final_log_file_path)}\n{str(load_summary_file_path)}'
                my_api_response.log_file_path = str(final_log_file_path)
            elif job_records_loaded > 0:
                print_string = 'Job completed successfully'
                if logging_flag:
                    print_string += f'\nSee the below files for details:\n{str(final_log_file_path)}\n{str(load_summary_file_path)}'
                    my_api_response.log_file_path = str(final_log_file_path)
                print(print_string)
                my_api_response.api_message = 'Job completed successfully'
            else:  
                print('No files to process')
                my_api_response.api_message = 'No files to process'

    except Exception as e:
            pass
    finally:
        engine.dispose()

        for handler in logger.handlers:
            handler.close()
        
        move_log_file()

def per_run_initializations():
    import api_response
    global my_api_response, job_start_time, job_start_time_string, job_start_time_log, fatal_error, job, folders, folder, job_folders_processed, job_files_loaded, job_bad_files, job_records_loaded, job_errors_or_warnings, log_file_path, log_file, load_summary_file_path

    job_start_time, job_start_time_string, job_start_time_log = get_current_timestamp()
    log_file = pathlib.Path(log_file_path) / f'{job_start_time_string}_flat_file_loader.log'
    load_summary_file_path = str(pathlib.Path(log_file_path) / f'{job_start_time_string}_flat_file_loader_load_summary.json')

    fatal_error = False

    job = OrderedDict()
    folders = []
    folder = {}
    job_folders_processed = 0
    job_files_loaded = 0
    job_bad_files = 0
    job_records_loaded = 0
    job_errors_or_warnings = 0

    my_api_response = api_response.ApiResponse()

def move_log_file():
    global project_dir, logging_flag, log_file_path, log_file_name, local_log_file_path, final_log_file_path
    
    # local_log_file_path = pathlib.Path(project_dir) / log_file_name
    
    if os.path.isfile(local_log_file_path):
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)
        shutil.move(local_log_file_path, pathlib.Path(log_file_path) / log_file_name)


#%%

# run_from_command_line = os.getenv("PYTHON_ISATTY", "True").lower() == "true" and sys.stdin.isatty()
# if 'KUBERNETES_SERVICE_HOST' in os.environ:
#     run_from_command_line = True

DEBUGGING_MODE = False
if 'VSCODE_PID' in os.environ or sys.gettrace() is not None:
    DEBUGGING_MODE = True
if 'KUBERNETES_SERVICE_HOST' in os.environ:
    DEBUGGING_MODE = False
# if DEBUGGING_MODE:
#     print(f'DEBUGGING_MODE: {DEBUGGING_MODE}')

job_start_time, job_start_time_string, job_start_time_log = get_current_timestamp()
os_platform = platform.system()
# support_path = ''
logger = logging.getLogger()
log_file = ''
log_file_name = ''
local_log_file_path = ''
final_log_file_path = ''
# flat_file_loader_app.logger.handlers = logger.handlers
# flat_file_loader_app.logger.setLevel(logger.level)
# flat_file_loader_app.logger.propagate = logger.propagate
fatal_error = False

# Job performance json
job_name = 'flat_file_loader'
# job = {}
job = OrderedDict()
folders = []
folder = {}
job_folders_processed = 0
job_files_loaded = 0
job_bad_files = 0
job_records_loaded = 0
job_errors_or_warnings = 0

# Determine project folder
current_dir = os.getcwd()
while not os.path.basename(current_dir) == package_root_name:
    current_dir = os.path.dirname(current_dir)
project_dir = current_dir

config_path = pathlib.Path(project_dir + '/src/config')

app_config_path = config_path / "app_config.ini"
load_file_path, archive_file_path, log_file_path, password_method, password_access_key, password_secret_key, password_endpoint_url, password_region_name, password_password_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days = read_app_config_settings(app_config_path)

load_summary_file_path = str(pathlib.Path(log_file_path) / f'{job_start_time_string}_flat_file_loader_load_summary.json')

connection_config_path = config_path / "connections_config.ini"
db_target_config = 'target_connection' # Allow user to provide via parameter - future enhancement

# folder_to_process = ''
engine = None
schema = None
current_script_path = ''
my_api_response = None
per_run_initializations()

#%%
if __name__ == '__main__':
    if not DEBUGGING_MODE:
        # per_run_initializations()
        parse_command_run_arguments()
        # print()
    else:
        # move_log_file()
        # per_run_initializations()

        # parse_command_run_arguments()
        
        call_function('run_all')

        # with flat_file_loader_app.test_request_context('/request', method='POST', query_string={'function': 'run_all'}):
        #     trigger_function_from_api()

        # print('Job complete')
    
    # %%
