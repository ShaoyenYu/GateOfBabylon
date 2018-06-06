from backtesting import account
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
        self.stocks = account.StockPosition(None, start, end, freq="D")

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
