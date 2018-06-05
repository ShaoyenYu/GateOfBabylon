import datetime as dt
import pandas as pd
from dataclasses import dataclass
from utils.sqlfactory import constructor
from utils.config import default_engine


@dataclass
class StockDataLoader:
    stock_ids: list
    start: dt.datetime
    end: dt.datetime
    engine = default_engine

    def load_price(self):
        sql = "SELECT stock_id, date, close_badj as value " \
              "FROM babylon.stock_kdata_d " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        return pd.read_sql(sql, self.engine)

    def load_turnover(self, bs):
        types = {"b": "买盘", "s": "卖盘", "m": "中性"}
        sql = "SELECT stock_id, date, volume as value " \
              "FROM babylon.stock_turnover " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}' " \
              f"AND type = '{types[bs]}'"
        return pd.read_sql(sql, self.engine)

    def load_type(self):
        sql = f"SELECT stock_id, industry FROM stock_info WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)})"
        return pd.read_sql(sql, self.engine)
