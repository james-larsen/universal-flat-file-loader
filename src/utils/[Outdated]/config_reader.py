"""Config file reader"""
#%%
import os
import glob
import configparser
import io

#%%
def read_config_file(config_filepath):
    """Read config file based on current environment"""
    config = configparser.ConfigParser()
    break_out_flag = False
    # Locate local config file
    local_config_path = ''
    for root, dirs, files in os.walk(os.getcwd()):
        if break_out_flag == True:
            break
        files = [file for file in files if file.endswith(('.ini'))]
        for file in files:
            if break_out_flag == True:
                break
            if file == config_filepath.stem + '_local.ini':
                local_config_path = os.path.join(root, file)
                break_out_flag = True
                break
                #fp_local = io.open(local_config_path, 'r', encoding='utf_8_sig')
    #fp = io.open(config_filepath, 'r', encoding='utf_8_sig')
    config.read([config_filepath, local_config_path])#, encoding='utf-8-sig')
    return config

# %%
