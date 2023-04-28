"""Retrieve password using Systems Manager Parameter Store"""
import boto3
import os
from botocore.exceptions import ClientError
import keyring

def get_password(account_name, password_key, encoding='utf-8'):
    """Return password based on account name and secret key"""
    # TODO:  Come up with a better method of retrieving these values
    access_key = keyring.get_password(password_key, "SMSAccessKey")
    secret_key = keyring.get_password(password_key, "SMSSecretKey")

    sts_client = boto3.client('sts', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    response = sts_client.get_session_token()
    access_key_id = response['Credentials']['AccessKeyId']
    secret_access_key = response['Credentials']['SecretAccessKey']
    session_token = response['Credentials']['SessionToken']

    client = boto3.client(
        'ssm',
        endpoint_url='https://ssm.us-west-1.amazonaws.com',
        region_name='us-west-1',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token
    )

    password_path = '/flat_file_loader/passwords/dev'
    parameter_name = f"{account_name}_{password_key}"
    if password_path is not None and password_path != '':
        parameter_name = f"{password_path}/{account_name}_{password_key}"
    try:
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return None
    else:
        secret_value = response['Parameter']['Value']
        supported_encodings = ['utf-8', 'ascii', 'latin-1', 'utf-16']
        if encoding not in supported_encodings:
            print(f"Unsupported encoding: {encoding}")
            return None
        if isinstance(secret_value, bytes):
            secret_value = secret_value.decode(encoding)
    
    return secret_value

""" Set Password Variables - DO NOT SAVE VALUES AFTER RUNNING
    os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key_id'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_access_key'
"""
