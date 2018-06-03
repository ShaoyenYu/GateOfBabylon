import numpy as np
from functools import wraps
from collections import Iterable


def align(which):
    """
        Filter and align arrays with NaN value in the input *args.

    Args:
        which: list[int]
            Index of args to be aligned;

    Returns:
        decorated function

    """

    def _align(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            args_to_align = np.array(tuple(args[i] for i in which))
            if len(which) == 1:
                mask = ~np.isnan(args_to_align[0])
            else:
                mask = (~np.isnan(args_to_align)).all(0)
            args_new = (args[i][mask] if i in which else args[i] for i in range(len(args)))

            return func(*args_new, **kwargs)
        return wrapper
    return _align


def sample_check(which, sample_nums):
    """
    Apply length check to specified arguments

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
                    if len(args[i]) < sample_nums:
                        raise AssertionError("arg%s sample is not enough" % i)

            elif isinstance(sample_nums, Iterable):
                for i in which:
                    if len(args[i]) < sample_nums[i]:
                        raise AssertionError("arg%s sample is not enough" % i)

            return func(*args, **kwargs)
        return wrapper
    return _sample_check


def vectorize(func):
    """
        Vectorially apply function, depends on the input args form

        1) 1-D -> scalar
        2) or 2-D -> 1-D vector

    Returns:
        decorated function

    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[0].ndim == 1:
            return func(*args, **kwargs)
        if args[0].ndim == 2:
            return np.apply_along_axis(func, 0, args[0], *args[1:], **kwargs)

        raise NotImplementedError("Unsupported type input/dimension")
    return wrapper


def _keep_order(idx):
    def _f(func):
        def wrapper(*args, **kwargs):
            new_args = list(args)
            new_args[0]
            new_args = list(args)[1:]
            new_args.insert(idx, args[0])

            return func(*new_args, **kwargs)
        return wrapper
    return _f


def auto(pos):
    """
        Vectorize, or broadcast function, depends on the input args form

        1) all(args[pos].ndim) == 2 -> broadcasting
        2) any(args[pos].ndim) == 2 -> dot
        3) else: -> original function apply

    Args:
        pos: list[int]

    Returns:
        decorated function

    """

    def _auto(func):
        def wrapper(*args, **kwargs):
            eqdim = tuple(args[x].ndim == 2 for x in pos)
            if all(eqdim):
                n = args[pos[0]].shape[1]
                l = len(args)
                res = []
                for i in range(n):
                    new_args = (args[_][:, i] if _ in pos else args[_] for _ in range(l))
                    res.append(func(*new_args, **kwargs))
                return np.array(res)

            elif any(eqdim):
                for idx in pos:
                    if args[idx].ndim == 2:
                        break

                if idx == 0:
                    return np.apply_along_axis(func, 0, args[idx], *args[1:], **kwargs)
                # todo 20180603 this may cause unexpected performance lost
                args = list(args)
                return np.apply_along_axis(_keep_order(idx)(func), 0, args.pop(idx), *args, **kwargs)

            return func(*args, **kwargs)
        return wrapper
    return _auto
