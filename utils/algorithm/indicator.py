import numpy as np
import pandas as pd


class Derivative:
    # Returns
    @classmethod
    def return_series(cls, frame):
        """"""
        return (frame / frame.shift(1) - 1)[1:]

    @classmethod
    def accumulative_return(cls, frame: pd.DataFrame):
        """"""

        return (frame / frame.shift(len(frame) - 1))[-1:]

    @classmethod
    def annualized_return(cls, frame: pd.DataFrame, period: int, method="a"):
        """

        Args:
            frame: pandas.DataFrame
                DataFrame with datetime-like index, and each column vector contains a whole time-series of a subject;

            period: int
                period_num in a year;

            method: str, or None, optional {"a" or "accumulative", "m" or "mean", None}, default "a"
                annualized method to use.
                If "a", or "accumulative" passed, the annualized return is calculated as:
                    (1 + r_acc) ** (period_num_in_a_year / periods_in_frame - 1) - 1;
                elif "m", or "mean" passed, the annualized return is calculated  as:
                    (1 + r_mean) ** period_num - 1
                elif None is passed, the annualized method is automatically chosen, depends on the interval
                of the frame. If interval is longer than 365 days, "mean" is chosen; otherwise "accumulative" is chosen;

        Returns:

        """

        if method in {"a", "accumulative"}:
            return (1 + cls.accumulative_return(frame)) ** (period / (len(frame) - 1)) - 1

        elif method in {"m", "mean"}:
            res = (1 + cls.return_series(frame).mean()) ** period - 1

            # encapsulate pd.Series into pd.DataFrame
            return pd.DataFrame(res, columns=[frame.index[-1:]]).T

        elif method is None:
            interval = (frame.index[-1] - frame.index[0]).days
            if interval > 365:
                return cls.annualized_return(frame, period, "m")
            else:
                return cls.annualized_return(frame, period, "a")

    @classmethod
    def excess_return_a(cls, frame: pd.DataFrame, frame_bm: pd.DataFrame, period, method="a"):
        """"""

        r_annu = cls.annualized_return(frame, period, method)
        rbm_annu = cls.annualized_return(frame_bm, period, method)
        return (r_annu.T - rbm_annu.T.unstack()).T

    # Risk
    @classmethod
    def standard_deviation(cls, frame):
        """"""

        res = np.std(cls.return_series(frame), ddof=1)
        return pd.DataFrame(res, columns=[frame.index[-1:]]).T

    @classmethod
    def standard_deviation_a(cls, frame, period):
        """"""

        return cls.standard_deviation(frame) * (period ** .5)

    @classmethod
    def drawdown(cls, frame):
        """"""

        return (frame.cummax() - frame) / frame.cummax()

    @classmethod
    def max_drawdown(cls, frame):
        """"""

        return cls.drawdown(frame).cummax()

    # Risk-adjusted Return
    @classmethod
    def sharpe_a(cls, frame, frame_rf, period, method="a"):
        """
        Annualized return earned in excess of the risk-free per unit of volatility.

        Args:
            frame:
            frame_rf:
            period:
            method:

        Returns:

        """

        er = cls.excess_return_a(frame, frame_rf, period, method)
        std_a = cls.standard_deviation_a(frame, period)
        return (er.T / std_a.T).T
