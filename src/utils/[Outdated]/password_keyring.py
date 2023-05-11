"""Retrieve password using keyring library"""
import keyring

def get_password(password_key, account_name):
    """Return password based on username and secret key"""
    
    try:
        response = keyring.get_password(account_name, password_key)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None
    else:
        secret_value = response
        return secret_value