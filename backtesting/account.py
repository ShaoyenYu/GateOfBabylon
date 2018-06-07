import numpy as np
from backtesting import basetype, loader
from utils.decofactory import common
from utils.config import default_engine
from utils.algorithm.perf import api


class Account:
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
        return self.resample(df, self.freq)

    @property
    @common.unhash_cache()
    def return_series_rf(self):
        df = self.data_loader.load_riskfree()
        df /= 100 * 365
        return self.resample(df, self.freq).iloc[1:]

    @property
    @common.unhash_cache()
    def perf(self):
        calculator = api.Calculator(
            p=self.price_series.values,
            r_rf=self.return_series_rf.values,
            t=np.array([x.timestamp() for x in self.price_series.index.to_pydatetime()]),
            period={"d": 250, "w": 52}[self.freq]
        )
        return calculator

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
        return np.nanmean(self.perf.accumulative_return)
        # return api.accumulative_return(self.price_series.values).mean()


