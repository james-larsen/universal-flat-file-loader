#%%
import sys
import os
import glob
import argparse
from spec_builder import create_scripts, read_source_file
from nexus_utils import flatfile_utils

def call_spec_builder(spec_file_path):
    if os.path.isdir(spec_file_path):
        for file_path in glob.glob(os.path.join(spec_file_path, 'mapping_spec_*.xls*')):
            spec_file_path = file_path
    
    create_scripts(os.path.normpath(spec_file_path))

def check_uniqueness(source_file_path, field_list, print_results=True):

    df_source, file_encoding, delimiter = read_source_file(source_file_path)

    # field_list = field_list[:2]

    is_unique, no_nulls, sample_df = flatfile_utils.check_primary_key_fields(df_source, field_list, print_results=print_results)
    # is_unique, no_nulls, sample_df = check_primary_key_fields(df_source, field_list, print_results=print_results)


DEBUGGING_MODE = False
if 'VSCODE_PID' in os.environ or sys.gettrace() is not None:
    DEBUGGING_MODE = True
if 'KUBERNETES_SERVICE_HOST' in os.environ:
    DEBUGGING_MODE = False

#%%

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Script Updater')
    parser.add_argument('-fp', '--file_path', type=str, help='Path to the file')
    parser.add_argument('-pk', '--pk_check', nargs='+', type=str, help='Fields to check for uniqueness')
    
    if not DEBUGGING_MODE:
        args = parser.parse_args()
    else:
        # provide test values when developing / debugging
        # print(f'DEBUGGING_MODE: {DEBUGGING_MODE}')
        
        args = argparse.Namespace(
            file_path=r'Y:\Misc Data\ML Datasets\iTunes Sales by Genre.txt.gz',
            pk_check=['iTunesProductID', 'PrimaryGenre', 'CustomerPurchaseDate']
            # file_path=r'Y:\Misc Data\Product.txt',
            # pk_check=['product_id', 'parent_product_id', 'ProductName', 'ProductSubtype']
        )

    file_path = getattr(args, 'file_path', None)
    field_list = getattr(args, 'pk_check', None)
    
    if not field_list:
        # spec_builder.create_scripts(os.path.normpath(file_path))
        call_spec_builder(file_path)

        print(f'"{file_path}" Rebuilt')
    elif file_path and field_list:
        check_uniqueness(file_path, field_list, print_results=True)
    else:
        parser.print_help()

    #%%
