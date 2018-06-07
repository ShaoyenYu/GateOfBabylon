import datetime as dt
import pandas as pd
from utils.timeutils import const


class TsProcessor:
    def __init__(self, start: dt.datetime, end: dt.datetime, freq: str=None):
        self.start = start
        self.end = end
        self.freq = freq

    @property
    def date_range(self):
        return pd.date_range(self.start, self.end, self.freq)

    def resample(self, data: pd.DataFrame, freq=None):
        if freq is None:
            freq = const.bday_chn
            closed = label = "left"
        else:
            if freq == "d":
                freq = const.bday_chn
                closed = label = "left"
            elif freq[0].lower() == "w":
                closed = label = "right"
                if len(freq) == 1:
                    weekmask = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
                    freq = f"{freq}-{weekmask[data.index[-1].weekday()]}"
            elif freq[0].lower == "m":
                freq = "m"
                closed = label = "right"

        dr = pd.date_range(self.start, self.end, freq=freq)
        return data.resample(rule=freq, closed=closed, label=label).last().reindex(dr)
