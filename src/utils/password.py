"""Retrieve password"""
import keyring

def get_password(secret_key, user_name):
    """Return password based on username and secret key"""
    
    return keyring.get_password(secret_key, user_name)