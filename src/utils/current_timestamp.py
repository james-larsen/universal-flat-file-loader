import datetime

def get_current_timestamp():
    """Get current timestamp
    Variable 1: Timestamp object - Used difference calcs
    Variable 2: 'YYYY-MM-DD_HHMMSS' - Used for filenames
    Variable 3: 'YYYY-MM-DD HH:MM:SS' - Used for logs"""
    return [datetime.datetime.now(), 
    (
        datetime.datetime
        .fromtimestamp(datetime.datetime.now().timestamp())
        .strftime("%Y-%m-%d_%H%M%S")
        ), 
    (
        datetime.datetime
        .fromtimestamp(datetime.datetime.now().timestamp())
        .strftime("%Y-%m-%d %H:%M:%S")
        )]