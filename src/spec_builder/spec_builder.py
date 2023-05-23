"""Scan a flat file and provide information to assist with building the appropriate spec / scripts"""
#%%
import sys
import os
import warnings
from pathlib import Path
# import pathlib
import pandas as pd
from openpyxl import Workbook
from datetime import datetime, time
import csv
import json
from nexus_utils.package_utils import add_package_to_path#, import_relative
package_root_dir, package_root_name = add_package_to_path()
# from flat_file_loader.src.utils import config_reader as cr
# import_relative(package_root_name, 'src.utils', 'config_reader', alias='cr')
from nexus_utils import config_utils as cr
# from flat_file_loader.src.utils import detect_encoding as de
# import_relative(package_root_name, 'src.utils', 'detect_encoding', alias='de')
from nexus_utils import flatfile_utils
# import flatfile_utils
from nexus_utils import string_utils

# pylint: disable=line-too-long
# pylint: disable=trailing-whitespace

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

#%%

# def analyze_dataframe(df, target_path):
#     analysis_dict = {}

#     for col in df.columns:
#         column_dict = {}

#         # Add column header
#         column_dict['Column'] = col

#         # Check column data type
#         col_dtype = df[col].dtype
#         if pd.api.types.is_string_dtype(col_dtype):
#             # For string columns, calculate the maximum length
#             max_size = df[col].str.len().max()
#             max_size_string = str(int(max_size)) if not pd.isna(max_size) else ''
#             column_dict['Max Size'] = f'Max Length: {max_size_string}'
#             column_dict['Type'] = 'String'
#         elif pd.api.types.is_numeric_dtype(col_dtype):
#             if pd.api.types.is_integer_dtype(col_dtype):
#                 # For integer columns, convert the maximum value to int
#                 max_value = df[col].max()
#                 max_value_string = str(int(max_value)) if not pd.isna(max_value) else ''
#                 column_dict['Max Size'] = f'Max Value: {max_value_string}'
#             else:
#                 # For other numeric columns (float), store the maximum value as is
#                 max_value_string = str(df[col].max())
#                 column_dict['Max Size'] = f'Max Value: {max_value_string}'
#             column_dict['Type'] = 'Numeric'

#         # Get distinct values and their counts
#         value_counts = df[col].value_counts()
#         if len(value_counts) > 50:
#             # If there are more than 50 distinct values, store top 50 and "More than 50 distinct values"
#             top_50_values = value_counts.nlargest(50).to_dict()
#             # top_50_values['More than 50 distinct values'] = str(len(value_counts) - 50)
#             top_50_values['More than 50 distinct values'] = ''
#             column_dict['Distinct Values'] = {str(k): str(int(v)) if v != '' else str(v) for k, v in top_50_values.items()}
#         else:
#             column_dict['Distinct Values'] = {str(k): str(int(v)) if v != '' else str(v) for k, v in value_counts.items()}

#         # Add the column dictionary to the analysis dictionary
#         analysis_dict[col] = column_dict

#     # print(json.dumps(analysis_dict, indent=4))

#     # return analysis_dict

#     write_analysis_to_excel(analysis_dict, target_path)

# def write_analysis_to_excel(analysis_dict, file_path):
#     file_path = os.path.join(file_path, 'Distinct Values Analysis.xlsx')

#     # Create a workbook and select the active sheet
#     wb = Workbook()
#     ws = wb.active

#     # Initialize the start column index
#     start_column_index = 1

#     # Iterate over each column in the analysis dictionary
#     for col, col_dict in analysis_dict.items():
#         # Write the "Column" and "Max Size" to the Excel file
#         ws.cell(row=1, column=start_column_index, value=col_dict['Column'])
#         if 'Max Size' in col_dict:
#             ws.cell(row=1, column=start_column_index + 1, value=col_dict['Max Size'])

#         ws.cell(row=2, column=start_column_index, value='Distinct Values')
#         ws.cell(row=2, column=start_column_index + 1, value='Occurrences')

#         # Write the "Distinct Values" to the Excel file
#         distinct_values = col_dict['Distinct Values']
#         for row_num, (key, value) in enumerate(distinct_values.items(), start=3):
#             ws.cell(row=row_num, column=start_column_index, value=key)
#             ws.cell(row=row_num, column=start_column_index + 1, value=value)

#         # Increment the start column index for the next iteration
#         start_column_index += 3

#     ws.freeze_panes = 'A3'
    
#     # Save the workbook
#     wb.save(file_path)

#     df_results = pd.DataFrame(wb.active.values)

#     df_results.to_excel(file_path, sheet_name='Distinct Values', index=False, header=False)

#     print(f"Distinct Values Analysis has been written to {file_path}")

def get_wrk_schema_user(path):
    """Retrieve wrk schema user"""

    config_dir = Path(path).parent
    
    # navigate to the target folder
    while not os.path.basename(config_dir) == package_root_name:
        config_dir = os.path.dirname(config_dir)

    connections_config_path = os.path.join(config_dir, 'src', 'config', 'connections_config.ini')

    if os.path.exists(os.path.join(config_dir, 'src', 'config', 'connections_config_local.ini')):
        connections_config_path = os.path.join(config_dir, 'src', 'config', 'connections_config_local.ini')

    connections_file_config = cr.read_config_file(connections_config_path)  # type: ignore
    connections_config_entry = 'target_connection'
    wrk_schema_user = connections_file_config[connections_config_entry]['user_name']

    return wrk_schema_user

def get_files_to_process(path):
    files_to_process = []
    source_file_path_type = ''

    if os.path.isfile(path):
        files_to_process.append(path)
        source_file_path_type = 'file'
    elif os.path.isdir(path):
        source_file_path_type = 'folder'
        extensions = ['.csv', '.txt', '.tsv', '.dat', '.tab']
        for file in os.listdir(path):
            file_extension = os.path.splitext(file)[1]
            if os.path.isfile(os.path.join(path, file)) and file_extension in extensions:
                file_path = os.path.join(path, file)
                files_to_process.append(file_path)

    return source_file_path_type, files_to_process

def get_dtype(dtype, value):
    if dtype == "datetime64[ns]":
        dt = pd.to_datetime(value, errors='coerce')
        if not pd.isnull(dt) and dt.time() != time(0, 0, 0):
            return "timestamp"
        else:
            return "date"
    elif dtype == "float":
        return "float64"
    else:
        return "string"

def get_db_datatype(target_column_name):
    global df_source, df_spec
    source_dtype = df_spec[(df_spec["Target Field"] == target_column_name) & 
                           (df_spec["Source System"] == "Flat File")]["Type"].iloc[0]
    column_name = df_spec[(df_spec["Target Field"] == target_column_name) & 
                           (df_spec["Source System"] == "Flat File")]["Source Field(s)"].iloc[0]
    if source_dtype == 'timestamp':
        return 'timestamp(0)'
    elif source_dtype == 'date':
        return 'date'
    elif source_dtype == 'float64':
        non_null_values = df_source[column_name][df_source[column_name].notnull()]
        if non_null_values.empty:
            return 'unknown'
        else:
            if (non_null_values % 1 == 0).all():
                return 'integer'
            else:
                # Remove integer values
                # non_null_values = non_null_values[non_null_values % 1 != 0]
                # Determine precision and scale
                max_left = max(non_null_values.apply(lambda x: len(str(int(x)))))
                max_right = max(non_null_values.apply(lambda x: len(str(x).split('.')[1]) if len(str(x).split('.')) > 1 else 0))

                # Use the maximum number of digits to the left and right of the decimal point to determine precision and scale
                scale = max_right + 1  # add buffer of 1 to scale
                precision = max_left + scale + 2  # add buffer of 2 to precision
                return f'decimal({precision},{scale})'
    else:
        # Check if all values in the column are null
        if df_source[column_name].isnull().all():
            return 'varchar(?)'
        else:
            # Calculate the largest value in the column
            non_null_values = df_source.loc[df_source[column_name].notna(), column_name].drop_duplicates()

            max_len = max(non_null_values.astype(str).str.len())
            # max_len = max(df_source.loc[df_source[column_name].notnull(), column_name].apply(lambda x: len(str(x).strip())))

            # Add 30% to the largest value
            max_len_with_buffer = int(max_len * 1.3)

            # Choose the next highest number over the max_len_with_buffer from common string field lengths
            n = next((x for x in [10, 20, 40, 60, 80, 100, 255, 500, 2000, 4000] if x > max_len_with_buffer), 4000)
            if max_len == 1:
                n = 1

            return f'varchar({n})'

# def cleanse_string(string):
#     string = string.lower().replace(' (', '_').replace(' ', '_').replace('(', '_').replace(')', '')
#     return string

def format_field(string):
    ending_keywords = ['id', 'cd', 'code', 'num', 'flag', 'desc', 'name', 'amt', 'qty', 'price']
    # string = string.lower().replace(' (', '_').replace(' ', '_').replace('(', '_').replace(')', '')
    string = string_utils.cleanse_string(string, title_to_snake_case=True)
    # string = string.replace('__', '_')
    for keyword in ending_keywords:
        if string.endswith(keyword) and len(string) > len(keyword):
            if not string.endswith('_' + keyword):
                string = string[:-len(keyword)] + '_' + string[-len(keyword):]
    return string

def build_wrk_spec():
    def clean_field(field_name):
        field_name = field_name.replace(' ', '_').lower()
        for char in ['(', ')', '[', ']']:
            field_name = field_name.replace(char, '')
        return field_name
    
    global df_spec, df_source, headers_list, wrk_schema, wrk_table

    header_record = {
        'Action': 'Create Table',
        'Schema': wrk_schema,
        'Target Table': wrk_table,
        'Target Field': None,
        'Comment': None,
        'Type': None,
        'Char Encoding': None,
        'PK': None,
        'FK': None,
        'Source System': None,
        'Source Table': None,
        'Source Field(s)': None
    }

    df_spec.loc[len(df_spec)] = header_record

    for header in headers_list:
        dtype = df_source[header].dtype
        first_valid_index = df_source[header].first_valid_index()
        if first_valid_index is not None:
            first_value = df_source.loc[first_valid_index, header]
        else:
            first_value = None
        type = get_dtype(dtype, first_value)

        detail_record = {
            'Action': 'Add Field',
            'Schema': wrk_schema,
            'Target Table': wrk_table,
            # 'Target Field': string_utils.cleanse_string(header, title_to_snake_case=True),
            'Target Field': clean_field(header),
            'Comment': None,
            'Type': type,
            'Char Encoding': None,
            'PK': None,
            'FK': None,
            'Source System': 'Flat File',
            'Source Table': 'Flat File',
            'Source Field(s)': header
        }

        df_spec.loc[len(df_spec)] = detail_record

    # Add audit records
    detail_record.update({
        'Target Field': 'load_file_name',
        'Comment': 'Automatically added by the application',
        'Type': 'string',
        'Source Table': 'System Generated',
        'Source Field(s)': 'load_file_name'
    })
    df_spec.loc[len(df_spec)] = detail_record

    detail_record.update({
        'Target Field': 'load_timestamp',
        'Comment': 'Automatically added by the application',
        'Type': 'timestamp',
        'Source Table': 'System Generated',
        'Source Field(s)': 'load_timestamp'
    })
    df_spec.loc[len(df_spec)] = detail_record

def build_stg_spec():
    global df_spec, wrk_df, wrk_schema, wrk_table, stg_schema, stg_table

    header_record = {
        'Action': 'Create Table',
        'Schema': stg_schema,
        'Target Table': stg_table,
        'Target Field': None,
        'Comment': None,
        'Type': None,
        'Char Encoding': None,
        'PK': None,
        'FK': None,
        'Source System': None,
        'Source Table': None,
        'Source Field(s)': None
    }

    df_spec.loc[len(df_spec)] = header_record

    for index, row in wrk_df.iterrows():
        field_name = row['Target Field']
        # source_field_name = row['Source Field(s)']
        source_field_name = string_utils.cleanse_string(row['Source Field(s)'], title_to_snake_case=True)
        if field_name in ['load_file_name', 'load_timestamp']:
            continue
        datatype = get_db_datatype(field_name)
        comment = "All records are NULL value, fix data type" if "varchar(?)" in datatype else None
        detail_record = {
            'Action': 'Add Field',
            'Schema': stg_schema,
            'Target Table': stg_table,
            'Target Field': format_field(source_field_name),
            'Comment': comment,
            'Type': datatype,
            'Char Encoding': None,
            'PK': None,
            'FK': None,
            'Source System': 'DB Internal',
            'Source Table': wrk_table,
            'Source Field(s)': field_name
        }

        df_spec.loc[len(df_spec)] = detail_record

    # Add audit records
    detail_record.update({
        'Target Field': 'load_file_name',
        'Type': 'varchar(255)',
        'Source Table': wrk_table,
        'Source Field(s)': 'load_file_name'
    })
    df_spec.loc[len(df_spec)] = detail_record

    detail_record.update({
        'Target Field': 'load_timestamp',
        'Type': 'timestamp(0)',
        'Source Table': wrk_table,
        'Source Field(s)': 'load_timestamp'
    })
    df_spec.loc[len(df_spec)] = detail_record
    
def build_tgt_spec():
    global df_spec, stg_df, stg_schema, stg_table, tgt_schema, tgt_table

    header_record = {
        'Action': 'Create Table',
        'Schema': tgt_schema,
        'Target Table': tgt_table,
        'Target Field': None,
        'Comment': None,
        'Type': None,
        'Char Encoding': None,
        'PK': None,
        'FK': None,
        'Source System': None,
        'Source Table': None,
        'Source Field(s)': None
    }

    df_spec.loc[len(df_spec)] = header_record

    for index, row in stg_df.iterrows():
        field_name = row['Target Field']
        if field_name in ['load_file_name', 'load_timestamp']:
            continue
        datatype = row['Type']
        detail_record = {
            'Action': 'Add Field',
            'Schema': tgt_schema,
            'Target Table': tgt_table,
            'Target Field': field_name,
            'Comment': None,
            'Type': datatype,
            'Char Encoding': None,
            'PK': None,
            'FK': None,
            'Source System': 'DB Internal',
            'Source Table': stg_table,
            'Source Field(s)': field_name
        }

        df_spec.loc[len(df_spec)] = detail_record

    # Add audit records
    detail_record.update({
        'Target Field': 'load_file_name',
        'Type': 'varchar(255)',
        'Source Table': stg_table,
        'Source Field(s)': 'load_file_name'
    })
    df_spec.loc[len(df_spec)] = detail_record

    detail_record.update({
        'Target Field': 'load_timestamp',
        'Type': 'timestamp(0)',
        'Source Table': stg_table,
        'Source Field(s)': 'load_timestamp'
    })
    df_spec.loc[len(df_spec)] = detail_record

def create_scripts(spec_path):
    """Create scripts based on a spec"""
    wrk_schema_user = get_wrk_schema_user(spec_path)

    df_spec = pd.read_excel(spec_path, na_values='', engine='openpyxl')

    wrk_schema, stg_schema, tgt_schema = df_spec['Schema'].unique()
    wrk_table, stg_table, tgt_table = df_spec['Target Table'].unique()

    # wrk_df = df_spec[(df_spec['Schema'] == wrk_schema) & (~df_spec['Target Field'].isna())].copy()
    stg_df = df_spec[(df_spec['Schema'] == stg_schema) & (~df_spec['Target Field'].isna())].copy()
    tgt_df = df_spec[
        (df_spec['Schema'] == tgt_schema) & 
        (~df_spec['Target Field'].isna()) &
        (~df_spec['Target Field'].isin(['load_file_name', 'load_timestamp']))
    ].copy()

    base_filename = os.path.splitext(os.path.basename(spec_path))[0]
    subject_area_name = base_filename.replace("mapping_spec_", "")
    target_path = os.path.dirname(spec_path)
    
    tgt_pk_values = tgt_df.loc[(tgt_df["Target Table"] == tgt_table) & (~tgt_df["PK"].isna()), "Target Field"].tolist()
    if tgt_pk_values:
        tgt_pk_string = ', '.join(tgt_pk_values)
    else:
        tgt_pk_string = 'field'
    
    stg_pk_values = tgt_df.loc[(tgt_df["Target Table"] == tgt_table) & (~tgt_df["PK"].isna()), "Source Field(s)"].tolist()
    if stg_pk_values:
        stg_pk_string = ', '.join(stg_pk_values)
    else:
        stg_pk_string = 'field'

    # Write table creation scripts
    sql_string = f'-- DROP TABLE {stg_schema}.{stg_table};\n\nCREATE TABLE {stg_schema}.{stg_table} (\n'

    for index, row in stg_df.iterrows():
        target_field = row['Target Field']
        datatype = row['Type']
        sql_string += f'\t{target_field} {datatype} NULL, \n'

    sql_string = sql_string[:-3]

    sql_string += f'\n);\n'

    if stg_pk_values:
        sql_string += f'\nCREATE INDEX idx_{stg_table} ON {stg_schema}.{stg_table} ({stg_pk_string});\n'
    else:
        sql_string += f'\nCREATE INDEX idx_{stg_table} ON {stg_schema}.{stg_table} (field);\n'

    sql_string += f'\n-- DROP TABLE {tgt_schema}.{tgt_table};\n\nCREATE TABLE {tgt_schema}.{tgt_table} (\n'

    for index, row in tgt_df.iterrows():
        target_field = row['Target Field']
        datatype = row['Type']
        sql_string += f'\t{target_field} {datatype} NULL, \n'

    sql_string += f'\tCONSTRAINT {tgt_table}_pkey PRIMARY KEY ({tgt_pk_string})\n);'

    if wrk_schema_user:
        sql_string += f'\n\nGRANT UPDATE, SELECT, INSERT, DELETE ON TABLE {stg_schema}.{stg_table} TO {wrk_schema_user};'
        sql_string += f'\nGRANT UPDATE, SELECT, INSERT, DELETE ON TABLE {tgt_schema}.{tgt_table} TO {wrk_schema_user};'

    with open(f'{target_path}/{subject_area_name} - table creation scripts.sql', 'w') as f:
        f.write(sql_string)

    # Write wrk to stg script
    delete_sql = f'--DELETE FROM {stg_schema}.{stg_table};\n\n'
    sql_string = 'SELECT \n'
    insert_string = ''

    for index, row in stg_df.iterrows():
        source_field = row['Source Field(s)']
        target_field = row['Target Field']
        data_type = row['Type']
        sql_string += f'CAST(WRK.{source_field} AS {data_type}) AS {target_field}, \n'
        insert_string += f'{target_field}, '

    sql_string = sql_string.rstrip(', \n')
    sql_string += f'\nFROM {wrk_schema}.{wrk_table} WRK;'

    # insert_string = insert_string[:-2]

    with open(f'{target_path}/wrk to stg load - {subject_area_name}.sql', 'w') as f:
        f.write(delete_sql)
        f.write(f'INSERT INTO {stg_schema}.{stg_table} ({insert_string[:-2]})\n')
        f.write(sql_string)

    # Write stg to tgt script
    if tgt_pk_values:
        join_string = f"WHERE STG.{tgt_pk_values[0]} = TGT.{tgt_pk_values[0]}"
        if len(tgt_pk_values) > 1:
            for pk_value in tgt_pk_values[1:]:
                join_string += f"\n\t  AND STG.{pk_value} = TGT.{pk_value}"
    else:
        join_string = f'WHERE STG.field = TGT.field'

    delete_sql = f'/*\nDELETE FROM {tgt_schema}.{tgt_table} TGT\nWHERE EXISTS\n(\n\tSELECT 1\n\tFROM {stg_schema}.{stg_table} STG\n\t{join_string}\n);\n*/\n\n'
    sql_string = 'SELECT \n'
    insert_string = ''

    for index, row in tgt_df.iterrows():
        source_field = row['Source Field(s)']
        target_field = row['Target Field']
        data_type = row['Type']
        sql_string += f'CAST(STG.{source_field} AS {data_type}) AS {target_field}, \n'
        insert_string += f'{target_field}, '

    sql_string = sql_string.rstrip(', \n')
    sql_string += f'\nFROM {stg_schema}.{stg_table} STG;'

    truncate_wrk_table = f'\n\n/*\nTRUNCATE {wrk_schema}.{wrk_table};\n\nTRUNCATE {stg_schema}.{stg_table};\n*/'

    with open(f'{target_path}/stg to target load - {subject_area_name}.sql', 'w') as f:
        f.write(delete_sql)
        f.write(f'INSERT INTO {tgt_schema}.{tgt_table} ({insert_string[:-2]})\n')
        f.write(sql_string)
        f.write(truncate_wrk_table)

    # if not os.path.exists("generated_files"):
    #     os.makedirs("generated_files")

#%%

if __name__ == '__main__':
    # source_file_config_path = pathlib.Path(os.getcwd() + '/spec_builder_config.ini')

    config_dir = os.getcwd()
    current_dir = os.getcwd()

    # navigate to the target folder
    while not os.path.basename(config_dir) == package_root_name:
        config_dir = os.path.dirname(config_dir)

    # remove folders to the right of the target folder
    # target_path = os.path.join(config_dir, 'src', 'config')
    # target_path = os.path.join(os.path.dirname(current_dir), 'src', 'config')
    target_path = current_dir

    source_file_config_path = os.path.join(config_dir, 'src', 'config', 'spec_builder_config.ini')

    if os.path.exists(os.path.join(config_dir, 'src', 'config', 'spec_builder_config_local.ini')):
        source_file_config_path = os.path.join(config_dir, 'src', 'config', 'spec_builder_config_local.ini')

    source_file_config = cr.read_config_file(source_file_config_path)  # type: ignore
    local_config_entry = 'source_file_settings'
    source_file_path = source_file_config[local_config_entry]['source_file_path']
    subject_area_name = source_file_config[local_config_entry]['subject_area_name']
    null_value = source_file_config[local_config_entry]['null_value']
    wrk_schema = source_file_config[local_config_entry]['wrk_schema']
    stg_schema = source_file_config[local_config_entry]['stg_schema']
    tgt_schema = source_file_config[local_config_entry]['tgt_schema']

    # loop through files to process
    source_file_path_type, files_to_process = get_files_to_process(source_file_path)

    if not subject_area_name and source_file_path_type == 'file':
        file_name = os.path.splitext(os.path.basename(source_file_path))[0]
        subject_area_name = string_utils.cleanse_string(file_name, title_to_snake_case=True)

    if source_file_path_type == 'file' and any(not var for var in [subject_area_name, wrk_schema, stg_schema, tgt_schema]):
        print("The following values in 'spec_builder_config.ini' are required:\nsubject_area_name\nwrk_schema\nstg_schema\ntgt_schema")
        sys.exit(1)

    if source_file_path_type == 'folder' and any(not var for var in [wrk_schema, stg_schema, tgt_schema]):
        print("The following values in 'spec_builder_config.ini' are required:\nwrk_schema\nstg_schema\ntgt_schema")
        sys.exit(1)

    for files_to_process in files_to_process:

        print(f'"{files_to_process}" Starting\n')

        if source_file_path_type == 'folder':
            subject_area_name = string_utils.cleanse_string(os.path.splitext(os.path.basename(files_to_process))[0], title_to_snake_case=True)
            source_file_path = files_to_process
        wrk_table = source_file_config[local_config_entry]['wrk_table']
        if wrk_table == '' or source_file_path_type == 'folder':
            wrk_table = 'wrk_' + subject_area_name
        stg_table = source_file_config[local_config_entry]['stg_table']
        if stg_table == '' or source_file_path_type == 'folder':
            stg_table = 'stg_' + subject_area_name
        tgt_table = source_file_config[local_config_entry]['tgt_table']
        if tgt_table == '' or source_file_path_type == 'folder':
            tgt_table = subject_area_name

        if not source_file_path.lower().endswith(('.csv', '.txt', '.tsv', '.dat', '.tab')):
            print("Source file must be one of the following: csv, txt, tsv, dat, tab")
            sys.exit(1)

        file_encoding = flatfile_utils.detect_encoding(source_file_path)  # type: ignore

        # Create spec data frame
        columns = [
            'Action', 'Schema', 'Target Table', 'Target Field', 'Comment',
            'Type', 'Char Encoding', 'PK', 'FK', 'Source System', 'Source Table', 'Source Field(s)'
        ]

        df_spec = pd.DataFrame(columns=columns)

        # Read flat file into data frame
        with open(source_file_path, "r", encoding=file_encoding) as file:
            first_row = file.readline().strip()
            
        if "\t" in first_row:
            delimiter = "\t"
        elif "|" in first_row:
            delimiter = "|"
        else:
            delimiter = ","

        df_source = pd.read_csv(
            source_file_path,
            delimiter=delimiter,
            encoding=file_encoding,
            na_values=null_value,
            dtype=str,
            quoting=csv.QUOTE_ALL, 
            on_bad_lines='skip'
            #compression='infer'
        )

        headers_list = df_source.columns.tolist()

        # Attempt to convert each field to an appropriate type
        for col in headers_list:
            if df_source[col].notnull().any():
                try:
                    df_source[col] = df_source[col].astype(float)
                except ValueError:
                    try:
                        df_source[col] = pd.to_datetime(df_source[col], errors='raise')
                    except ValueError:
                        pass
        
        build_wrk_spec()

        wrk_df = df_spec[df_spec['Source System'] == 'Flat File'].copy()

        build_stg_spec()

        stg_df = df_spec[(df_spec['Schema'] == stg_schema) & (~df_spec['Target Field'].isna())].copy()

        build_tgt_spec()

        tgt_df = df_spec[(df_spec['Schema'] == tgt_schema) & (~df_spec['Target Field'].isna())].copy()

        target_path = f'{current_dir}/generated_files/{subject_area_name}'

        # analyze_dataframe(df_source, target_path)
        df_results = flatfile_utils.analyze_dataframe(df_source)

        os.makedirs(f'{current_dir}/generated_files', exist_ok=True)
        os.makedirs(target_path, exist_ok=True)

        analysis_path = f'{target_path}/{subject_area_name} - Distinct Values Analysis.xlsx'
        df_results.to_excel(analysis_path, sheet_name='Distinct Values', index=False, header=False)

        spec_path = f'{target_path}/mapping_spec_{subject_area_name}.xlsx'

        # Output spec file
        df_spec.to_excel(spec_path, sheet_name='Fields & Mappings', index=False)

        # Output relevant file_config fields
        filename, file_extension = os.path.splitext(source_file_path)

        delimiter_output = delimiter.replace("\t", "\\t")

        with open(f'{target_path}/{subject_area_name}_file_config.txt', 'w') as f:
            f.write(f"extension = {file_extension[1:]}\n")
            f.write(f"delimiter = {delimiter_output}\n")
            f.write(f"encoding = {file_encoding}\n")
            f.write(f"null_value = {null_value}\n")

        create_scripts(os.path.normpath(spec_path))

        print(f'"{spec_path}" Processed\n')

        #%%

"""
#%%

from script_updater import call_spec_builder

file_path = r'C:\Data Projects\Development\projects\flat_file_loader\src\spec_builder\generated_files\personal_files'
call_spec_builder(file_path)

print(f'"{file_path}" Rebuilt\n')

#%%
"""
