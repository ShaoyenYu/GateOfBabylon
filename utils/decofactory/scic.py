import numpy as np
from functools import wraps
from collections import Iterable


def vectorize(func):
    """
        Vectorially apply a function, depends on the input args form
        1) 1-D -> scalar
        2) or 2-D -> 1-D vector

    Returns:
        decorated function

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        if type(args[0]) is list or args[0].ndim == 1:
            # todo 20180525 if input list is two dimension, this will lead to wrong result
            return func(*args, **kwargs)
        if args[0].ndim == 2:
            return np.apply_along_axis(func, 0, args[0], *args[1:], **kwargs)

        raise NotImplementedError("Unsupported type input/dimension")
    return wrapper


def align(to_which, all_=True):
    """
        Filter and align arrays with NaN value in the input *args.

    Args:
        to_which: int
            Arrays to be aligned will be args[:`to_which`]

        all_: bool, default True
            align and filter method when dealing with NaN element.
            True: Align arrays with NaN value, drop all the i_th element of all arrays if any i_th element is NaN,
            the output arrays will be in the same shape;
            False: Filter NaN element in the arrays by each, the output arrays may be different shape.

    Returns:
        decorated function

    """

    def _align(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            args_to_align = np.array(args[:to_which])
            masks = ~np.isnan(args_to_align)
            if all_:
                aligned = args_to_align[:, masks.all(0)]
            else:
                aligned = [args_to_align[i][masks[i]] for i in range(len(masks))]

            return func(*aligned, *args[to_which:], **kwargs)
        return wrapper
    return _align


def align_first(to_which):
    """
        Filter and align arrays with NaN value in the input *args.

    Args:
        to_which: int
            Arrays to be aligned will be args[:`to_which`]

    Returns:
        decorated function

    """

    def _align(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            args_to_align = np.array(args[:to_which])
            masks = ~np.isnan(args_to_align[0])
            aligned = args_to_align[:, masks]

            return func(*aligned, *args[to_which:], **kwargs)
        return wrapper
    return _align


def sample_check(which, sample_nums):
    """

    Args:
        which: Iterable[int]
            idx of arg to check sample num;
        sample_nums: int, list[int], or tuple[int]
            min sample requirement of each arg. If doesn't meet, raise AssertionError;

    Returns:

    """

    def _sample_check(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if type(sample_nums) is int:
                for i in which:
                    if len(args[i]) <= sample_nums:
                        raise AssertionError("arg%s sample is not enough" % i)

            elif isinstance(sample_nums, Iterable):
                for i in which:
                    if len(args[i]) <= sample_nums[i]:
                        raise AssertionError("arg%s sample is not enough" % i)

            return func(*args, **kwargs)
        return wrapper
    return _sample_check


def main():
    @align(3, "align")
    def t1(*args):
        return args

    @align(3, "each")
    def t2(*args):
        return args

    def t3(*args):
        return args

    import numpy as np
    q1 = np.array([1, 1, None, 1])
    q2 = np.array([1, None, None, 3])
    q3 = np.array([1, None, None, 4])
    t1(q1, q2, q3)
    t2(q1, q2, q3)
    t3(q1, q2, q3)


if __name__ == "__main__":
    main()
