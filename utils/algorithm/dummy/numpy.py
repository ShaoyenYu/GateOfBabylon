import numpy as np


def rolling2d(arr: np.array, window: int):
    """
    Generate rolling sub-arrays with given window size of original array.

    e.g.1
        Given a ndarray of shape(2500, 3600), window size = 3,
        then this will generate a new view of shape (2498, 3, 3600)

    Args:
        arr: numpy.ndarray
            two-dimension-ndarray
        window: int

    Returns:
        ndarray

    """

    # refer from: http://www.rigtorp.se/2011/01/01/rolling-statistics-numpy.html
    # def rolling_window(arr: np.ndarray, window: int):
    #     shape = arr.shape[:-1] + (arr.shape[-1] - window + 1, window)
    #     strides = arr.strides + (arr.strides[-1],)
    #     return np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)

    shape = np.array((arr.shape[0] - window + 1, window, arr.shape[-1]))
    strides = arr.itemsize * np.array([shape[-1], shape[-1], 1])
    return np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides, writeable=False)
