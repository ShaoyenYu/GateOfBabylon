import datetime as dt
import pandas as pd
from backtesting import basetype, loader

from importlib import reload
reload(basetype)
reload(loader)


class Account:
    pass


class Stock:
    pass


class Position(basetype.TsProcessor):
    def __init__(self, positions=None, start=None, end=None, freq="D"):
        basetype.TsProcessor.__init__(self, start, end, freq)
        self.positions = positions


class StockPosition(Position):
    def __init__(self, positions=None, start=None, end=None, freq="W-FRI"):
        self.data_loader = loader.StockDataLoader(positions, start, end)
        Position.__init__(self, positions, start, end, freq)

    @property
    def turnover_series(self):
        df = self.data_loader.load_turnover("b")
        return df.pivot(index="date", columns="stock_id", values="amount")

    @property
    def price_series(self):
        df = self.data_loader.load_price()
        return df.pivot(index="date", columns="stock_id", values="value")

    @property
    def return_series(self):
        p = self.resample(self.price_series, self.freq).last().dropna(axis=0, how="all")
        return (p / p.shift(1) - 1)[1:]

    @property
    def cumret(self):
        return ((1 + self.return_series).prod() - 1).mean()


def test():
    s = ['000001', '000002', '000735', '000837', '000886', '000908', '000955',]
    from util.config import default_engine
    sids = [x[0] for x in default_engine.execute("SELECT DISTINCT stock_id FROM stock_info WHERE name NOT LIKE '*%%'").fetchall()]
    t1, t2 = dt.date(2018, 4, 1), dt.date(2018, 5, 14)
    qq = StockPosition(sids, start=t1, end=t2, freq="D")
    d1, d2 = qq.return_series.dropna(axis=1, how="any"), qq.turnover_series.dropna(axis=1, how="any")

    start, end = dt.date(2018, 5, 15), dt.date(2018, 5, 17)
    a = StockPosition(s, start=start, end=end, freq="D")

    a.cumret
    a.price_series
    a.return_series
    a.turnover_series
