"""Retrieve password using AWS Secret Manager"""
import boto3
import os
from botocore.exceptions import ClientError
import base64

client = boto3.client(
    'secretsmanager',
    endpoint_url='https://my-secret-manager-instance.example.com',
    region_name='your_region_name',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.environ.get('AWS_SESSION_TOKEN')
)

def get_password(password_key, account_name=None, encoding='utf-8'):
    """Return password based on account name and secret key"""
    
    secret_name = f"{account_name}_{password_key}"
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return None
    else:
        if 'SecretString' in response:
            secret_value = response['SecretString']
        else:
            supported_encodings = ['utf-8', 'ascii', 'latin-1', 'utf-16']
            
            if encoding not in supported_encodings:
                print(f"Unsupported encoding: {encoding}")
                return None
            
            secret_value = base64.b64decode(response['SecretBinary']).decode(encoding)
        return secret_value

""" Set Password Variables - DO NOT SAVE VALUES AFTER RUNNING
    os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key_id'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_access_key'
    os.environ['AWS_SESSION_TOKEN'] = 'your_session_token'
"""