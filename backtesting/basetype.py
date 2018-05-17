import datetime as dt
import pandas as pd
from dataclasses import dataclass


@dataclass
class TsProcessor:
    start: dt.datetime
    end: dt.datetime
    freq: str = None

    @property
    def date_range(self):
        return pd.date_range(self.start, self.end, self.freq)

    def resample(self, data: pd.DataFrame, freq=None):
        return data.resample(rule=freq or self.freq)

