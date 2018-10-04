import calendar as cld
import datetime as dt
import pandas as pd
from dateutil.relativedelta import relativedelta
from typing import Union
from utils.algorithm.perf import api
from utils.backtesting.basetype import Stocks
from utils.decofactory import common

TimeType = Union[dt.date, dt.datetime]


class BaseStrategy:
    def __init__(self, start: TimeType = None, end: TimeType = None):
        self.start = start
        self.end = end

    def position(self, *args, **kwargs):
        pass


class T1(BaseStrategy):
    """
    trend
    """

    def __init__(self, start, end):
        BaseStrategy.__init__(self, start, end)
        self.stocks = Stocks(None, start, end, freq="D")

    @classmethod
    def _signal_by(cls, df, wl, direction="+"):
        tmp = df.rolling(wl).mean()
        if direction == "+":
            return (tmp >= tmp.shift(wl)).applymap(int)
        elif direction == "-":
            return (tmp <= tmp.shift(wl)).applymap(int)

    def price_signal(self, wl):
        return self._signal_by(self.stocks.return_series.dropna(axis=1, how="any"), wl)

    def trans_signal(self, wl):
        return self._signal_by(self.stocks.turnover_series.dropna(axis=1, how="any"), wl)

    @common.unhash_cache()
    def signal(self, wl):
        return self.price_signal(wl) * self.trans_signal(wl)

    def by_signal(self, wl, period):
        s = self.signal(wl)
        lp = s.loc[s.index[-period:]].all()
        return set(lp.loc[lp].index)

    def by_mdd(self, bin_):
        mdd = api.max_drawdown(self.stocks.price_series)
        return set(mdd[(mdd >= bin_[0]) & (mdd < bin_[1])].index)

    def by_var(self):
        import numpy as np
        var = api.VaR(self.stocks.return_series.values)
        return set(self.stocks.return_series.columns[var < np.nanmean(var)])

    def by_std(self):
        import numpy as np
        val = api.standard_deviation(self.stocks.return_series.values)
        return set(self.stocks.return_series.columns[val < np.nanmean(val)])

    def position(self, wl, period, bin_):
        return list(self.by_signal(wl, period).intersection(self.by_mdd(bin_)))


class T2(BaseStrategy):
    """
    1.数据频率用周或者月;
    2.换仓周期为月度或者季度;
    3.剔除周期(月或季)收益率为负数的股票;
    4.通过正收益周期占比挑选每个行业前20的股票;
    5.通过夏普比从20个中再挑10个 ->
    6.通过GARCH(1，1)模型来做价格预测，计算每只股票预测收益率，
    以此排序作为是否购买股票的依据，第一次建仓挑出预测收益率最大
    且为正数股票(若股票超过十只则购买十只，不够十只则按实际数量购买)
    """

    def __init__(self, start=None, end=None, freq="d", **kwargs):
        BaseStrategy.__init__(self, start, end)
        self.freq = freq
        months, weeks, days = kwargs.get("months", 2), kwargs.get("weeks", 0), kwargs.get("days", 0)

        if not self.start:
            self.start = self.end - relativedelta(months=months, weeks=weeks, days=days)

            if self.end.day == cld.monthrange(self.end.year, self.end.month)[1]:
                y, m = self.start.year, self.start.month
                self.start = dt.date(y, m, cld.monthrange(y, m)[1])

            self.start += dt.timedelta(1)

        self.stocks = Stocks(None, self.start, self.end, self.freq)

    def by_return(self):
        r = self.stocks.perf.accumulative_return
        ids_rge0 = set(self.stocks.price_series.columns[r > 0])
        return ids_rge0

    def by_prop(self):
        from functools import partial
        import numpy as np

        r1 = self.stocks.perf.periods_pos_prop
        r2 = self.stocks.perf.sharpe_a
        df = pd.DataFrame(np.array([r1, r2]).T, self.stocks.price_series.columns).reset_index()
        df["type"] = df["stock_id"].apply(lambda x: self.stocks.type_sws.get(x))
        df = df.dropna(subset=["type"])

        f = partial(pd.Series.rank, ascending=False, pct=False)
        df["pct0"] = df.groupby("type")[0].apply(f)
        df["pct1"] = df.groupby("type")[1].apply(f)
        df = df.sort_values(by=["type", "pct0", "pct1"], ascending=[True, True, True])
        type_first10 = df.groupby("type")["stock_id"].apply(lambda x: set(list(x)[:1]))
        return type_first10.to_dict()

    @property
    @common.unhash_cache()
    def position(self):
        res = self.by_return()
        tmp = set()
        for tp, p1 in self.by_prop().items():
            tmp = tmp.union(p1)
        return res.intersection(tmp)


def main():
    s, e = dt.date(2018, 3, 1), dt.date(2018, 5, 15)
    start, end = dt.datetime(2018, 8, 1), dt.datetime(2018, 9, 29)
    # q = StockPosition(None, start=s, end=e, freq="d")
    t = T2(s, e, "w")
    a = Stocks(t.position, start, end, freq="d")
    print(a.cumret)
