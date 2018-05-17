import datetime as dt
import pandas as pd
from dataclasses import dataclass
from util.sqlfactory import constructor
from util.config import default_engine


@dataclass
class StockDataLoader:
    stock_ids: list
    start: dt.datetime
    end: dt.datetime

    def load_price(self):
        sql = "SELECT stock_id, date, close as value FROM babylon.stock_kdata_d " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        return pd.read_sql(sql, default_engine)

    def load_turnover(self, bs):
        types = {"b": "买盘", "s": "卖盘", "m": "中性"}
        sql = "SELECT stock_id, date, amount FROM babylon.stock_turnover " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}' " \
              f"AND type = '{types[bs]}'"
        return pd.read_sql(sql, default_engine)
