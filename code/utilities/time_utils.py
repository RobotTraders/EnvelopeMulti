from datetime import datetime

def get_timestamp() -> str:
    """Return formatted timestamp string"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') 