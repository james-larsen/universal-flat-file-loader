"""Config file reader"""
#%%
import os
import glob
import configparser

#%%
def read_config_file(config_filepath):
    """Read config file based on current environment"""
    config = configparser.ConfigParser()
    # Locate local config file
    local_config_path = ''
    for root, dirs, files in os.walk(os.getcwd()):
        files = [file for file in files if file.endswith(('.ini'))]
        for file in files:
            if file == config_filepath.stem + '_local.ini':
                local_config_path = os.path.join(root, file)
    config.read([config_filepath, local_config_path])
    return config

# %%
