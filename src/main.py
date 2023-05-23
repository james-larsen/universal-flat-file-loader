"""Parse and load flat files"""
#%%
import os
import platform
import pathlib
import shutil
#import sys
import glob
import logging
#import time
import datetime
import warnings
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import traceback
import re
import json
# pylint: disable=import-error
# from utils.build_engine import build_engine
# from utils import config_reader as cr
# from utils import clean_sql
# from utils import time_difference
# from utils.current_timestamp import get_current_timestamp
from nexus_utils.package_utils import add_package_to_path, import_relative
package_root_dir, package_root_name = add_package_to_path()
# from flat_file_loader.src.utils.build_engine import build_engine
# import_relative('flat_file_loader', 'src.utils.build_engine', 'build_engine', caller_globals=globals())
# import_relative(package_root_name, 'src.utils.build_engine', 'build_engine')
from nexus_utils.database_utils import build_engine
# from flat_file_loader.src.utils import config_reader as cr
# import_relative(package_root_name, 'src.utils', 'config_reader', alias='cr')
from nexus_utils import config_utils as cr
# from flat_file_loader.src.utils import clean_sql
# import_relative(package_root_name, 'src.utils', 'clean_sql')
from nexus_utils.database_utils import clean_sql_statement
# from flat_file_loader.src.utils import time_difference
# import_relative(package_root_name, 'src.utils', 'time_difference')
from nexus_utils.datetime_utils import get_current_timestamp, get_duration
# from flat_file_loader.src.utils.current_timestamp import get_current_timestamp
# import_relative(package_root_name, 'src.utils.current_timestamp', 'get_current_timestamp')
from nexus_utils.datetime_utils import get_current_timestamp
from nexus_utils import password_utils as pw
from nexus_utils import string_utils
# from flat_file_loader.src.global_variables import setup_globals
# pylint: enable=import-error

if platform.system() == 'Windows':
    import win32com.client

# pylint: disable=line-too-long
# pylint: disable=trailing-whitespace
# pylint: disable=redefined-outer-name

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

#%%

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
                            logger.debug(log_statement)
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
    global job_name, job, folders, load_summary_file_path
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

    with open(load_summary_file_path, 'w') as f:
        json.dump(job, f, indent=4, default=str)

    with open(load_summary_file_path, 'r') as f:
        data = f.read()
        #data = data.replace("\\\\", "\\")
        data = data.replace("\\\\", "/")
    with open(load_summary_file_path, 'w') as f:
        f.write(data)

    logger.info('Job start:%s, job end: %s, total duration: %s', job_start_time_log, job_end_time_log, display_string)

def load_file(path, files_to_process, extension, delimiter, encoding, null_value, quoting, file_read_chunk_size, archive_expire_days):
    """Load files to be processed"""

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
            with open(file_path, "rb"):
                with open(file_path, "r", encoding=encoding) as csvfile:
                    original_row_count = sum(1 for line in csvfile)
                    if original_row_count > 0:
                        original_row_count -= 1
            
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

                df.columns = df.columns.str.replace(' ', '_').str.lower()
                for char in ['(', ')', '[', ']']:
                    df.columns = df.columns.str.replace(char, '', regex= False)

                # df.columns = df.columns.map(lambda col: string_utils.cleanse_string(col, title_to_snake_case=True))
                
                df['load_file_name'] = file_path.name
                df['load_timestamp'] = job_start_time
                #df = df.drop(columns=['boxofficerank']) # test for file doesn't match spec
            
                # Check if load file matches spec
                if not file_verified:
                    load_column_list = list(df.columns.values)
                    spec_column_list = df_columns.source_fields.values.tolist()
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
                        column_name = df_columns.loc[row + 1].at["source_fields"]
                        if df_columns.loc[row + 1].at["type"] == 'float64':
                            df[column_name] = pd.to_numeric(df[column_name])#, errors='coerce')
                        elif df_columns.loc[row + 1].at["type"] in ['date', 'timestamp'] and column_name != 'load_timestamp':
                            df[column_name] = pd.to_datetime(df[column_name])
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
            job_errors_or_warnings += 1
            break_out_flag = True # pylint: disable=invalid-name
            
        if break_out_flag:
            if not os.path.exists(bad_files_folder):
                os.makedirs(bad_files_folder)
            folder_bad_files += 1
            job_bad_files += 1
            file['fileName'] = load_file
            file['fileStart'] = file_start_time_log
            new_path = pathlib.Path(bad_files_folder / load_file)
            pathlib.Path(file_path).rename(new_path)
            file['fileError'] = f'ERROR: File not loaded.  Moved to "{new_path}".  See log for more details'
            continue
        
        # attempt more accurate way of determine records loaded
        count_sql_statement = f'select count(*) from {schema}.{wrk_table};'
        try:
            with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                result = conn.execute(text(count_sql_statement))
                record_count = result.scalar()
                if record_count:
                    file_records_loaded = record_count
        except Exception as e: # pylint: disable=broad-except
            pass
        
        folder_files_loaded += 1
        folder_records_loaded += file_records_loaded
        # job_files_loaded += 1
        job_records_loaded += file_records_loaded
        file_end_time, file_end_time_string, file_end_time_log = get_current_timestamp()
        days, hours, minutes, seconds, display_string = get_duration(file_start_time, file_end_time)
        file['fileName'] = load_file
        file['fileRawRecords'] = str(format(original_row_count, ','))
        file['fileRecordsLoaded'] = str(format(file_records_loaded, ','))
        file['fileStart'] = file_start_time_log
        file['fileEnd'] = file_end_time_log
        file['fileTotalDuration'] = display_string
        if file_records_loaded != original_row_count and file_records_loaded > 0:
            file_records_loaded_string = str(format(file_records_loaded, ','))
            original_row_count_string = str(format(original_row_count, ','))
            loaded_rows_difference_string = str(format(original_row_count - file_records_loaded, ','))
            file['fileLoadError'] = f'ERROR: {file_records_loaded_string} records loaded of {original_row_count_string} in source file ({loaded_rows_difference_string} records difference)'
            job_errors_or_warnings += 1
            logger.info('%s records loaded successfully.  WARNING: This is less than the source file contained (%s records).', format(file_records_loaded, ','), format(original_row_count_string, ','))
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

def process_folders(load_file_path, archive_file_path, log_file_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days, logger):
    global support_path
    if logger is None:
        # logger = globals()['logger']
        logger = logging.getLogger()
        logger.disabled = True
    # Build list of folders to process by checking for at least one file or the specified type
    for path in glob.glob(f'{load_file_path}/*/'):
        # Skip "[Ignore]" folder
        if os.path.basename(os.path.normpath(path)) == '[Ignore]':
            continue
        #path = r'test_path.txt'
        #print(path)
        folder_start_time = datetime.datetime.now()
        logger.info('Processing folder: "%s"', path)
        support_path = pathlib.Path(path + 'support')
        file_config_path = pathlib.Path(os.path.join(support_path, 'file_config.ini'))

        # Check for valid config file
        if not os.path.isfile(file_config_path):
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

    if job_records_loaded > 0:
        if logging_flag:
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


#%%

job_start_time, job_start_time_string, job_start_time_log = get_current_timestamp()
os_platform = platform.system()
# support_path = ''
logger = logging.getLogger()
fatal_error = False

# Job performance json
job_name = 'flat_file_loader'
job = {}
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

#%%
if __name__ == '__main__':
    # globals().update(setup_globals())

    # Logger settings
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    log_file = pathlib.Path(log_file_path) / f'{job_start_time_string}_flat_file_loader.log'
    current_script_path = pathlib.Path(os.getcwd() + os.sep + os.path.basename(__file__)).resolve()
    if logging_flag:
        logging.basicConfig(filename=log_file, 
            filemode='w', 
            level=logging.INFO, 
            format='%(asctime)s - %(name)s - %(levelname)s - \n\t%(message)s\n')
        logger = logging.getLogger(__name__)
    else:
        logger = logging.getLogger()
        logger.disabled = True
    logger.info('Start script: %s', current_script_path)
    logger.info('Build engine using config entry: %s', db_target_config)

    # (job_start_time, job_start_time_string, job_start_time_log, os_platform, support_path, logger, job_name, job, folders, folder, job_folders_processed, job_files_loaded, job_bad_files, job_records_loaded, db_target_config, engine, schema) = setup_globals().values()
    # app_config_path = pathlib.Path(os.getcwd() + '/config/app_config.ini')
    connect_type, server_address, server_port, database_name, schema, user_name, secret_key = read_connection_config_settings(connection_config_path, db_target_config)
    db_password = pw.get_password(password_method, secret_key, account_name=user_name, access_key=password_access_key, secret_key=password_secret_key, endpoint_url=password_endpoint_url, region_name=password_region_name, password_path=password_password_path)
    
    # engine, schema = build_engine(pathlib.Path(connection_config_path), db_target_config, password_method)  # type: ignore

    engine = build_engine(connect_type, server_address, server_port, database_name, user_name, db_password)#, schema)
    connection_result = connection_test(engine, schema)
    if connection_result != 'Success':
        fatal_error = True
        logger.error(f'Error accessing database: \n{connection_result}')
        print(f'Fatal Error: \n{connection_result}')
    
    if not fatal_error:
        process_folders(load_file_path, archive_file_path, log_file_path, read_chunk_size, archive_flag, logging_flag, log_archive_expire_days, logger)

        if job_errors_or_warnings > 0:
            print(f'ERRORS OR WARNINGS ENCOUNTERED\nSee the below files for details:\n{log_file}\n{load_summary_file_path}')
        else:
            print(f'Job completed successfully\nSee the below files for details:\n{log_file}\n{load_summary_file_path}')

        engine.dispose()

# %%
