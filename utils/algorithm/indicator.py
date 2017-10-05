import numpy as np
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from utils import config as cfg


class Derivative:
    class Return:
            @classmethod
            def return_series(cls, frame):
                """

                Args:
                    frame: pandas.DataFrame
                    DataFrame with datetime-like index, and each column vector contains whole series of a subject.

                Returns:

                """

                return (frame / frame.shift(1) - 1)[1:]

            @classmethod
            def accumulative_return(cls, frame: pd.DataFrame):
                """"""

                return (frame / frame.shift(len(frame) - 1))[-1:]

            @classmethod
            def annualized_return(cls, frame: pd.DataFrame, period: int, method="a"):
                """

                Args:
                    frame:
                    period:
                    method:

                Returns:

                """

                if method in {"a", "accumulative"}:
                    return (1 + cls.accumulative_return(frame)) ** (period / (len(frame) - 1)) - 1

                elif method in {"m", "mean"}:
                    res = (1 + cls.return_series(frame).mean()) ** period - 1

                    # encapsulate pd.Series into pd.DataFrame
                    return pd.DataFrame(res, columns=[frame.index[-1:]]).T

                elif method in {None, "auto"}:
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

            @classmethod
            def sharpe_a(cls, frame, frame_rf):
                """"""


def main():
    from utils import config as cfg

    def get_testid(conn, num):
        return str(
            tuple(pd.read_sql(f"SELECT stock_id FROM stock_info WHERE date = '20170929' LIMIT 0, {num}", conn)[
                      "stock_id"]))

    engine = cfg.default_engine

    sids = get_testid(engine, 200)
    df = pd.read_sql(f"SELECT stock_id, date, close_fadj FROM stock_kdata_d WHERE stock_id IN {sids}", engine)
    q = df.copy()
    q.index = pd.Index(q.date.tolist(), name="datetime")

    q = q[(q.index >= dt.datetime(2012, 9, 25)) & (q.index <= dt.datetime(2017, 9, 25))]
    g = q.groupby(["date", "stock_id"])["close_fadj"]
    a = g.last().unstack()
    bm = a[["000001", "000002"]]
    bm.columns = pd.Index(["a", "b"], name="bm_id")

    er = Derivative.Return.excess_return_a(a, bm, 250)
    ra1 = Derivative.Return.annualized_return(a, 250, "a")
    rabm = Derivative.Return.annualized_return(bm, 250, "a")

    # (ra1.T.unstack() - rabm.T).unstack()
    # (ra1.T - rabm.T.unstack()).T
    # ra1.sub(rabm.unstack(), axis=0)

    rs = Derivative.Return.return_series(a)
    racc = Derivative.Return.accumulative_return(a)

    ra2 = Derivative.Return.annualized_return(a, 250, method="m")
    ra3 = Derivative.Return.annualized_return(a, 250, "auto")

    racc_bm = Derivative.Return.accumulative_return()
    del q["date"]
    g = q.groupby(["stock_id"])
    g.plot()
