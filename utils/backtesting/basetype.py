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


class TechIndicatorMixin:
    freq2period = {"d": 250, "w": 52, "m": 12}

    start = end = freq = price_series = time_series = None

    def __init__(self, bm=None, bm_rf=None):
        self._bm = bm
        self._bm_rf = bm_rf or RiskfreeBenchmark(None, self.start, self.end, self.freq)

    def flush(self):
        try:
            delattr(self, "_perf")
        except AttributeError:
            pass

    @property
    def bm(self):
        return self._bm

    @bm.setter
    def bm(self, value):
        self._bm = value
        self.flush()

    @property
    def bm_rf(self):
        return self._bm_rf

    @bm_rf.setter
    def bm_rf(self, value):
        self._bm_rf = value
        self.flush()

    @property
    def load_args(self):
        kws = {
            "p": self.price_series.values,
            "t": np.array([x.timestamp() for x in self.time_series.to_pydatetime()]),
            "period": self.freq2period[self.freq[:1].lower()],
        }
        if self._bm:
            kws["p_bm"] = self._bm.price_series.values
        kws["r_rf"] = self._bm_rf.return_series.values

        print(kws)
        return kws

    @property
    @common.unhash_clscache()
    def perf(self):
        return api.Calculator(**self.load_args)

    @property
    def cumret(self):
        return np.nanmean(self.perf.accumulative_return)


class FinTimeSeries(TsProcessor, ABC):
    def __init__(self, start, end, freq):
        TsProcessor.__init__(self, start, end, freq)

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


class Position(FinTimeSeries, ABC):
    def __init__(self, positions=None, start=None, end=None, freq="d", bm=None, bm_rf=None):
        FinTimeSeries.__init__(self, start, end, freq)
        self.positions = positions


class Stocks(Position, TechIndicatorMixin):
    engine = cfg.default_engine

    def __init__(self, positions=None, start=None, end=None, freq="d", bm=None, bm_rf=None):
        if positions is None:
            sql = "SELECT DISTINCT stock_id FROM `babylon`.`stock_info` "
            positions = [x[0] for x in cfg.default_engine.execute(sql).fetchall()]
        if not bm:
            bm = Benchmark(["000300"], start, end, freq)

        self.data_loader = loader.StockDataLoader(positions, start, end)
        Position.__init__(self, positions, start, end, freq, bm, bm_rf)
        TechIndicatorMixin.__init__(self, bm, bm_rf)

    @property
    def mask(self):
        from pandas import DataFrame
        from numpy import timedelta64
        df_ld = self.data_loader.load_listeddate()
        v = ((df_ld.index.values.reshape(len(df_ld), 1) - df_ld.values) / (86400 * 1e9)) > timedelta64(180)
        return DataFrame(v, df_ld.index, df_ld.columns)

    @property
    @common.unhash_clscache()
    def price_series(self):
        p = self.resample(self.data_loader.load_price(), self.freq)
        m = self.mask.reindex_like(p).ffill()
        return p.where(m).dropna(how="all", axis=1)

    @common.hash_clscache(paramhash=True)
    def turnover_series(self, bs="b"):
        return self.data_loader.load_turnover(bs)

    @property
    @common.unhash_clscache()
    def type_sws(self):
        return self.data_loader.load_type_sws()


class Benchmark(Position, TechIndicatorMixin):
    def __init__(self, positions=None, start=None, end=None, freq="d", bm=None, bm_rf=None):
        Position.__init__(self, positions or ["000300"], start, end, freq, bm, bm_rf)
        TechIndicatorMixin.__init__(self, bm, bm_rf)
        self.data_loader = loader.BenchmarkLoader(positions, start, end)

    @property
    @common.unhash_clscache()
    def price_series(self):
        return self.resample(self.data_loader.load_price(), self.freq)


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
