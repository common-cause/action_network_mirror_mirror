from datetime import datetime

def log(msg):
    print(datetime.now().isoformat() + ' - ' + msg)