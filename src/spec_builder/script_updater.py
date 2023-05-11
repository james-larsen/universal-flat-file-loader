#%%
import os
import argparse
import spec_builder

#%%

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script Updater')
    parser.add_argument('-fp', '--file_path', type=str, help='Path to the file')

    args = parser.parse_args()
    if args.file_path:
        file_path = args.file_path
    else:
        parser.print_help()

    # file_path = r'c:\Data Projects\Development\projects\flat_file_loader\src\spec_builder/generated_files/currency/mapping_spec_currency.xlsx'

    spec_builder.create_scripts(os.path.normpath(file_path))
    #%%
