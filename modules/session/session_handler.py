import dill
import os
from datetime import datetime

def save_session(dir="./sessions/"):
    timestamp = datetime.today().strftime("%Y-%m-%d_%H%M%S")
    file=f"session_backup_{timestamp}.pkl"
    full_path = os.path.join(dir, file)
    
    with open(full_path, "wb") as f:
        dill.dump_session(f)

def load_session(file, dir="./sessions/"):
    full_path = os.path.join(dir, file)
    
    with open(full_path, "rb") as f:
        dill.load_session(f)