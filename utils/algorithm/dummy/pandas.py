import numpy as np
import pandas as pd
from utils.algorithm.dummy import numpy as npd


def combine_first(left: pd.DataFrame, right: pd.DataFrame, is_type_safe=False):
    left, right = left.align(right, join="outer")
    if not is_type_safe:
        # call np.isnan with "unsafe" type(string, etc.) will raise type error,
        # so we use pandas.DataFrame.notnull() for side-stepping
        return pd.DataFrame(np.where(left.notnull().values, left.values, right.values), left.index, left.columns)
    else:
        return pd.DataFrame(np.where(np.isnan(left.values), right.values, left.values), left.index, left.columns)


def rolling_apply2d(obj: pd.DataFrame, window: int, func: callable):
    """

    Args:
        obj: pandas.DataFrame
        window: int
        func: callable
            func1d

    e.g.1
        import numpy as np
        import pandas as pd

        shape = (50, 100)
        lst = [np.nan, *list(np.random.rand(*shape).flatten())[:-1]]

        arr = np.array(lst).reshape(shape)
        df = pd.DataFrame(arr)

        window = 3

        f = lambda x: x.sum(0) + 1
        rolling_apply(df, window, f)  # 15.2 ms ± 87.4 µs
        df.rolling(window).apply(f)  # 4.16 s ± 198 ms

        f = lambda x: x.mean(0)
        rolling_apply(df, 3, f)  # 13.1 ms ± 46.7 µs
        df.rolling(window).apply(f)  # 6.92 s ± 70.3

    Returns:
        pandas.DataFrame

    """

    val = func(np.swapaxes(npd.rolling2d(obj.values, window).T, 0, 1)).T
    return pd.DataFrame(val, obj.index[-len(val):], obj.columns)
