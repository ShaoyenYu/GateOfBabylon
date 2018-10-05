import numpy as np


def rolling2d(arr: np.array, window: int):
    """
    Generate rolling sub-arrays with given window size of original array.

    e.g.1
        Given a ndarray of shape(2500, 3600), window size = 3,
        then this will generate a new view of shape (2498, 3, 3600)

    e.g.2
        import numpy as np
        import pandas as pd

        shape = (1000, 2000)
        lst = [np.nan, *list(np.random.rand(*shape).flatten())[:-1]]

        arr = np.array(lst).reshape(shape)
        df = pd.DataFrame(arr)

        window = 3
        f1 = lambda x: x.sum(1) + 1
        f2 = lambda x: x.sum(0) + 1

        f1(dummy.rolling(arr, window))  # 15.2 ms ± 87.4 µs
        df.rolling(window).apply(f2)  # 4.16 s ± 198 ms

        np.mean(dummy.rolling(arr, window), 1)  # 10.1 ms ± 96.3 µs
        df.rolling(window).apply(np.mean)  # 8.44 s ± 87.3 ms

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
