from backtesting.account import StockPosition
from utils.decofactory import common
from utils.algorithm.perf import api


class Strategy:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(f"_{k}", v)
        self._pool = []

    def init(self):
        pass


class T1(Strategy):
    """
    trend
    """

    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.stocks = StockPosition(None, start, end, freq="D")

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

    def by_mdd(self, bin):
        mdd = api.max_drawdown(self.stocks.price_series)
        return set(mdd[(mdd >= bin[0]) & (mdd < bin[1])].index)

    def by_var(self):
        import numpy as np
        var = api.VaR(self.stocks.return_series.values)
        return set(self.stocks.return_series.columns[var < np.nanmean(var)])

    def by_std(self):
        import numpy as np
        val = api.standard_deviation(self.stocks.return_series.values)
        return set(self.stocks.return_series.columns[val < np.nanmean(val)])

    def pool(self, wl, period, bin):
        return list(self.by_signal(wl, period).intersection(self.by_mdd(bin)))


class T2:
    """
    1.数据频率用周或者月;
    2.换仓周期为月度或者季度;
    3.剔除周期(月或季)收益率为负数的股票;
    4.通过正收益周期占比挑选每个行业前20的股票;
    5.通过夏普比从20个中再挑10个;
    6.通过GARCH(1，1)模型来做价格预测，计算每只股票预测收益率，
    以此排序作为是否购买股票的依据，第一次建仓挑出预测收益率最大
    且为正数股票(若股票超过十只则购买十只，不够十只则按实际数量购买)
    """

    import datetime as dt
    s, e = dt.date(2018, 5, 1), dt.date(2018, 6, 6)
    q = StockPosition(None, start=s, end=e, freq="d")
    q.price_series
    api.periods_pos_prop(q.return_series.values)

    def __init__(self, start, end, freq="d"):
        self.start, self.end, freq = start, end, freq
        self.stocks = StockPosition(None, start, end, freq)

    def by_return(self):
        r = api.accumulative_return(self.stocks.price_series.values)
        ids_rge0 = set(self.stocks.price_series.columns[r>0])
        return ids_rge0

    def by_prop(self):
        r = api.periods_pos_prop(self.stocks.return_series.values)

