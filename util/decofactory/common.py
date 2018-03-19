import datetime as dt
from functools import wraps
import sys
import os


def log(f):
    module_name = os.path.basename(sys.argv[0])
    # mod = sys.modules["__main__"]
    # file = getattr(mod, '__file__', None)
    # module_name = file and os.path.splitext(os.path.basename(file))[0]

    @wraps(f)
    def wrapper(*args, **kwargs):
        print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {module_name}; START;")
        res = f(*args, **kwargs)
        print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {module_name}; Done;")

        return res
    return wrapper
