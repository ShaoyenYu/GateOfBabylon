import datetime as dt
import numpy as np
import pandas as pd
from utils.timeutils import const


def _date2datetime(date):
    return dt.datetime(date.year, date.month, date.day)


class VarTimeSeries:
    """
    Variable Timeseries

    """

    def __init__(self, ts_frame: pd.DataFrame, start, end, freq="d", **kwargs):
        """

        Args:
            ts_frame: pandas.Series/DataFrame(with Datetime Index)
            start: datetime.date
            end: datetime.date
            freq: str, optional {"d"}, default "d"

            **kwargs:
                fill: bool, default False
                    whether to fill NaN value of input data
                shift: int, default 0
                    shift period after input data gets resampled
                lmbd: callable, or None, default None
                    function to apply to the series/dataframe

        """

        self._ts_frame = ts_frame
        self.start = start
        self.end = end
        self.freq = freq
        self.fill = kwargs.get("fill", False)
        self.shift = kwargs.get("shift", 0)
        self.lmbd = kwargs.get("lmbd", None)
        self.check = kwargs.get("check", False)

    @property
    def value_series(self):
        where = (self._ts_frame.index >= _date2datetime(self.start)) & (self._ts_frame.index <= _date2datetime(self.end))
        if self.fill:
            ts = self.resample(self._ts_frame.fillna(method="ffill").loc[where]).fillna(method="ffill")[self.shift:]
        else:
            ts = self.resample(self._ts_frame.loc[where])[self.shift:]

        if self.lmbd:
            if ts.ndim == 1:
                return ts.apply(self.lmbd)
            elif ts.ndim == 2:
                return ts.applymap(self.lmbd)

        if self.check:
            r = (ts / ts.shift(1))[1:] - 1
            m = ((r == 0) | (~r.notnull())).all(0)
            ts = ts[ts.columns[~m]]

            # 检查首点
            c1 = ((ts.index >= _date2datetime(self.start + dt.timedelta(1))) & (ts.index <= _date2datetime(self.start + dt.timedelta(15))))
            c2 = (ts.index >= dt.datetime(self.end.year, self.end.month, 15))
            x1, x2 = [ts.loc[x].notnull().any() for x in (c1, c2)]
            x = x1 & x2
            ts = ts[x[x].index]

        return ts

    @property
    def time_series(self):
        return self.value_series.index.tolist()

    @property
    def value_mtx(self):
        return self.value_series.values

    @property
    def tmstmp_series(self):
        return [x.to_pydatetime().timestamp() for x in self.time_series]

    @property
    def tmstmp_mtx(self):
        return np.array(self.tmstmp_series)

    def resample(self, frame):
        if self.freq == "d":
            dr = pd.bdate_range(self.start, self.end, freq=const.bday_chn)
            return frame.reindex(dr)

    def __hash__(self):
        return hash((self.start, self.end))
