import datetime as dt
from functools import wraps
import sys
import os
import hashlib
import pickle


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


def inscache(cacheattr, paramhash=False):
    """
        Cache decorator for class instance, with different level of cache strategy.

    Args:
        cacheattr: str
            instance attribute for storing cache;
        paramhash: bool
            cache strategy

    Returns:

    """

    def _cache(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if paramhash:
                hash_key = hashlib.md5(pickle.dumps((func.__name__, args, kwargs))).hexdigest()
            else:
                hash_key = hashlib.md5(pickle.dumps(func.__name__)).hexdigest()

            if hasattr(self, cacheattr):
                if hash_key not in self.__getattribute__(cacheattr):
                    self.__getattribute__(cacheattr)[hash_key] = func(self, *args, **kwargs)
            else:
                self.__setattr__(cacheattr, {})
                self.__getattribute__(cacheattr)[hash_key] = func(self, *args, **kwargs)

            return self.__getattribute__(cacheattr)[hash_key]

        return wrapper

    return _cache
