#%%
import os
import glob
import argparse
import spec_builder

def call_spec_builder(spec_file_path):
    if os.path.isdir(spec_file_path):
        for file_path in glob.glob(os.path.join(spec_file_path, 'mapping_spec_*.xls*')):
            spec_file_path = file_path
    
    spec_builder.create_scripts(os.path.normpath(spec_file_path))

#%%

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script Updater')
    parser.add_argument('-fp', '--file_path', type=str, help='Path to the file')

    args = parser.parse_args()
    if args.file_path:
        file_path = args.file_path
    else:
        parser.print_help()

    # spec_builder.create_scripts(os.path.normpath(file_path))
    call_spec_builder(file_path)

    #%%
