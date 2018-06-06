import calendar as cld
import pandas as pd
import datetime as dt
from backtesting import basetype, loader
from utils.decofactory import common
from utils.config import default_engine
from utils.algorithm.perf import api



class Account:
    pass


class Stock:
    def __init__(self, stock_id):
        pass


class Position(basetype.TsProcessor):
    def __init__(self, positions=None, start=None, end=None, freq="d"):
        basetype.TsProcessor.__init__(self, start, end, freq)
        self.positions = positions


class StockPosition(Position):
    engine = default_engine

    def __init__(self, positions=None, start=None, end=None, freq="w"):
        if positions is None:
            sql = "SELECT DISTINCT stock_id FROM stock_info WHERE name NOT LIKE '*%%'"
            positions = [x[0] for x in default_engine.execute(sql).fetchall()]
        self.data_loader = loader.StockDataLoader(positions, start, end)
        Position.__init__(self, positions, start, end, freq)

    @property
    @common.unhash_cache()
    def turnover_series(self):
        df = self.data_loader.load_turnover("b")
        return df.pivot(index="date", columns="stock_id", values="value")

    @property
    @common.unhash_cache()
    def price_series(self):
        df = self.data_loader.load_price()
        df = df.pivot(index="date", columns="stock_id", values="value")
        return self.resample(df, self.freq).last()

    @property
    @common.unhash_cache()
    def type_sws(self):
        return self.data_loader.load_type_sws()

    @property
    def return_series(self):
        p = self.resample(self.price_series, self.freq).last().dropna(axis=0, how="all")
        return (p / p.shift(1) - 1)[1:]

    @property
    def cumret(self):
        return api.accumulative_return(self.price_series.values).mean()


def test_startegy(test_start):
    get_end = lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1])
    test_end = get_end(test_start)

    t = T1(test_start, test_end)

    start = test_end + dt.timedelta(1)
    end = get_end(start)
    print("#" * 32, f"test: {test_start}-{test_end}, real: {start}-{end}", "#" * 32)
    for wl, p in [(wl, p) for wl in range(3, 4) for p in [3, 4, 5, 7, 9, 10, 11]]:
        # s = t.pool(wl, p, bi)
        s = t.by_signal(wl, p).intersection(t.by_var()).intersection(t.by_std())
        if s:
            a = StockPosition(s, start=start, end=min(end, dt.date.today() - dt.timedelta(1)), freq="D")
            print(f"Window Length: {wl}; Persis period: {p}; Pool Size: {len(s)}; Return Accumulative: {a.cumret}")
    print("#" * 64, "\n")


def test():
    date_ranges = [dt.date(x.year, x.month, 1) for x in pd.date_range(dt.date(2018, 1, 1), dt.date(2018, 4, 30), freq="m")]
    for test_start in date_ranges:
        test_startegy(test_start)


def test2():
    s1, e1 = dt.date(2018, 1, 1), dt.date(2018, 1, 31)

    t = T1(s1, e1)
    t.by_std()
    bi = (0, 0.15)

    start, end = dt.date(2018, 2, 1), dt.date(2018, 2, 28)
    for wl, p in [(wl, p) for wl in range(4, 5) for p in range(2, 3)]:
        # s = t.pool(wl, p, bi)
        s = t.by_signal(wl, p)
        t.by_var()
        if s:
            a = StockPosition(s, start=start, end=end, freq="D")
            print(f"Window Length: {wl}; Persis period: {p}; Pool Size: {len(s)}; Return Accumulative: {a.cumret}")
