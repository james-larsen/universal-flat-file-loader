"""Build SQL Alchemy Engine based on input parameters"""
#%%
from sqlalchemy import create_engine
# from . import config_reader as cr
# from . import password as pw
from flat_file_loader.src.utils import config_reader as cr
# from flat_file_loader.src.utils import password as pw

# pylint: disable=line-too-long
# pylint: disable=trailing-whitespace

#%%
def build_engine(config_path, config_entry, password_method="keyring"):
    """Build SQL Alchemy Engine based on input parameters"""

    db_config = cr.read_config_file(config_path)
    connect_type = db_config[config_entry]['connect_type']
    #environment = db_config[config_entry]['environment']
    server_address = db_config[config_entry]['server_address']
    server_port = db_config[config_entry]['server_port']
    server_name = db_config[config_entry]['server_name']
    schema = db_config[config_entry]['schema']
    user_name = db_config[config_entry]['user_name']
    secret_key = db_config[config_entry]['secret_key']

    if password_method == 'keyring':
        from flat_file_loader.src.utils import password_keyring as pw
    elif password_method == 'secretsmanager':
        from flat_file_loader.src.utils import password_aws as pw
    elif password_method == 'ssm':
        from flat_file_loader.src.utils import password_ssm as pw
    if password_method == 'custom':
        from flat_file_loader.src.utils import password_custom as pw
    password = pw.get_password(secret_key, user_name)
    conn_string = f'{connect_type}://{user_name}:{password}@{server_address}:{server_port}/{server_name}'
    engine = create_engine(conn_string)

    return engine, schema
