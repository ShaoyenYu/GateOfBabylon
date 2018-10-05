import datetime as dt
import numpy as np
from abc import ABC, abstractmethod
from typing import Union
from utils.backtesting import loader
from utils.algorithm.perf import api
from utils.configcenter import config as cfg
from utils.decofactory import common
from utils.timeutils.basetype import TsProcessor

TimeType = Union[dt.date, dt.datetime]


class Position(ABC, TsProcessor):
    freq2period = {"d": 250, "w": 52, "m": 12}

    def __init__(self, positions=None, start=None, end=None, freq="d"):
        TsProcessor.__init__(self, start, end, freq)
        self.positions = positions

    @property
    @abstractmethod
    def price_series(self):
        pass

    @property
    def return_series(self):
        return (self.price_series / self.price_series.shift(1) - 1)[1:]

    @property
    def time_series(self):
        return self.price_series.index

    @property
    def _default_perf_args(self):
        kw = {
            "p": self.price_series.values,
            "t": np.array([x.timestamp() for x in self.time_series.to_pydatetime()]),
            "period": self.freq2period[self.freq[:1].lower()]
        }
        return kw

    @property
    def perf_args(self):
        return self._default_perf_args

    @property
    @common.unhash_clscache()
    def perf(self):
        return api.Calculator(**self.perf_args)

    @property
    def cumret(self):
        return np.nanmean(self.perf.accumulative_return)


class Stocks(Position):
    engine = cfg.default_engine

    def __init__(self, positions=None, start=None, end=None, freq="d"):
        if positions is None:
            sql = "SELECT DISTINCT stock_id FROM stock_info " \
                  "WHERE name NOT LIKE '*%%' " \
                  f"AND initial_public_date < '{start - dt.timedelta(180)}'"
            positions = [x[0] for x in cfg.default_engine.execute(sql).fetchall()]

        self.data_loader = loader.StockDataLoader(positions, start, end)
        Position.__init__(self, positions, start, end, freq)

    @property
    @common.unhash_clscache()
    def price_series(self):
        return self.resample(self.data_loader.load_price(), self.freq)

    @property
    @common.unhash_clscache()
    def turnover_series(self):
        return self.data_loader.load_turnover("b")

    @property
    @common.unhash_clscache()
    def type_sws(self):
        return self.data_loader.load_type_sws()

    @property
    def perf_args(self):
        bm = Benchmark(None, self.start, self.end, self.freq)
        bmrf = RiskfreeBenchmark(None, self.start, self.end, self.freq)
        return {**self._default_perf_args, "p_bm": bm.price_series.values, "r_rf": bmrf.return_series.values}


class Benchmark(Position):
    def __init__(self, positions=None, start=None, end=None, freq="d"):
        positions = positions or ["000300"]

        Position.__init__(self, positions, start, end, freq)
        self.data_loader = loader.BenchmarkLoader(positions, start, end)

    @property
    @common.unhash_clscache()
    def price_series(self):
        return self.resample(self.data_loader.load_price(), self.freq)

    @property
    def perf_args(self):
        bmrf = RiskfreeBenchmark(None, self.start, self.end, self.freq)
        return {**self._default_perf_args, "r_rf": bmrf.return_series.values}


class RiskfreeBenchmark(Position):
    trans = {"d": 250, "w": 52, "m": 12}

    def __init__(self, positions=None, start=None, end=None, freq="d"):
        positions = positions or ["y1"]

        Position.__init__(self, positions, start, end, freq)
        self.data_loader = loader.RiskfreeBenchmark(positions, start, end)

    @property
    @common.unhash_clscache()
    def original_series(self):
        df = self.data_loader.load_return()
        df /= (100 * self.trans[self.freq[:1].lower()])
        return self.resample(df, self.freq)

    @property
    @common.unhash_clscache()
    def price_series(self):
        df = self.original_series.copy()
        df.iloc[0] = 0
        return 1 + df.cumsum()

    @property
    @common.unhash_clscache()
    def return_series(self):
        return self.original_series[1:]


class BaseStrategy:
    def __init__(self, start: TimeType = None, end: TimeType = None):
        self.start = start
        self.end = end

    def position(self, *args, **kwargs):
        pass