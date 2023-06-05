'''Create configuration files'''
#%%
import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

#%%
# ADD SECTION
config_file.add_section('test_section')
# ADD SETTINGS TO SECTION
config_file.set('test_section', 'test', {'<NULL>', ''})

# SAVE CONFIG FILE
with open('connections_config.ini', 'w', encoding='UTF8') as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'connections_config' created")

# PRINT FILE CONTENT
# read_file = open('connections_config', 'r', encoding='UTF8')
# content = read_file.read()
# print('Content of the config file are:\n')
# print(content)
# read_file.flush()
# read_file.close()

#%%
# ADD SECTION
config_file.add_section('app_settings')
# ADD SETTINGS TO SECTION
config_file.set('app_settings', 'load_file_path', r'')
config_file.set('app_settings', 'archive_file_path', r'')

# SAVE CONFIG FILE
with open('app_config.ini', 'w', encoding='UTF8') as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'app_config' created")
