import numpy as np
import pandas as pd


def combine_first(left: pd.DataFrame, right: pd.DataFrame, is_type_safe=False):
    left, right = left.align(right, join="outer")
    if not is_type_safe:
        # call np.isnan with "unsafe" type(string, etc.) will raise type error,
        # so we use pandas.DataFrame.notnull() for side-stepping
        return pd.DataFrame(np.where(left.notnull().values, left.values, right.values), left.index, left.columns)
    else:
        return pd.DataFrame(np.where(np.isnan(left.values), right.values, left.values), left.index, left.columns)
