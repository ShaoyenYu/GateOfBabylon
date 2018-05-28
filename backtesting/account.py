import datetime as dt
from utils.decofactory import common
from utils.config import default_engine
from backtesting import basetype, loader

from importlib import reload
reload(basetype)
reload(loader)


class Account:
    pass


class Stock:
    def __init__(self, stock_id):
        pass


class Position(basetype.TsProcessor):
    def __init__(self, positions=None, start=None, end=None, freq="D"):
        basetype.TsProcessor.__init__(self, start, end, freq)
        self.positions = positions


class StockPosition(Position):
    engine = default_engine

    def __init__(self, positions=None, start=None, end=None, freq="W-FRI"):
        if positions is None:
            sql = "SELECT DISTINCT stock_id FROM stock_info WHERE name NOT LIKE '*%%'"
            positions = [x[0] for x in default_engine.execute(sql).fetchall()]
        self.data_loader = loader.StockDataLoader(positions, start, end)
        Position.__init__(self, positions, start, end, freq)

    @property
    @common.inscache("_cached")
    def turnover_series(self):
        df = self.data_loader.load_turnover("b")
        return df.pivot(index="date", columns="stock_id", values="value")

    @property
    @common.inscache("_cached")
    def price_series(self):
        df = self.data_loader.load_price()
        return df.pivot(index="date", columns="stock_id", values="value")

    @property
    @common.inscache("_cached")
    def type(self):
        return self.data_loader.load_type()

    @property
    def return_series(self):
        p = self.resample(self.price_series, self.freq).last().dropna(axis=0, how="all")
        return (p / p.shift(1) - 1)[1:]

    @property
    def cumret(self):
        return ((1 + self.return_series).prod() - 1).mean()


def test():
    from backtesting.strategy import T1

    s1, e1 = dt.date(2018, 4, 1), dt.date(2018, 5, 23)
    t = T1(s1, e1)

    start, end = dt.date(2018, 5, 23), dt.date(2018, 5, 25)
    for wl, p in [(wl, p) for wl in range(2, 5) for p in range(2, 15)]:
        s = t.pool(wl, p)
        a = StockPosition(s, start=start, end=end, freq="D")
        print(wl, p, a.cumret, len(s))
