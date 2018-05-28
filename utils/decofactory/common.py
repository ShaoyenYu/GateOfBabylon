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


def hash_inscache(cacheattr, paramhash=False, selfhash=False):
    """
        Cache decorator for class instance, with different level of cache strategy.

    Args:
        cacheattr: str
            instance attribute for storing cache;
        paramhash: bool, default False
            cache strategy. If True, when `func` is called with different parameters, all result will be cached;
        selfhash: bool, default False
            whether to use self as a parameter to generate cache key. If True, the instance should support __hash__
            method;

    """

    def _cache(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            to_pickle = [func.__name__]
            if paramhash:
                to_pickle.extend([args, kwargs])
            if selfhash:
                to_pickle.append(self.__hash__())
            hash_key = hashlib.md5(pickle.dumps(tuple(to_pickle))).hexdigest()

            if hasattr(self, cacheattr):
                if hash_key not in self.__getattribute__(cacheattr):
                    self.__getattribute__(cacheattr)[hash_key] = func(self, *args, **kwargs)
            else:
                self.__setattr__(cacheattr, {hash_key: func(self, *args, **kwargs)})

            return self.__getattribute__(cacheattr)[hash_key]

        return wrapper

    return _cache


def unhash_inscache(prefix="_", suffix=""):
    def _cache(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            fn = prefix + func.__name__ + suffix
            if not hasattr(self, fn):
                self.__setattr__(fn, func(self, *args, **kwargs))

            return self.__getattribute__(fn)

        return wrapper

    return _cache


inscache = hash_inscache  # compatible to previous ver
