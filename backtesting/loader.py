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
        sql = "SELECT stock_id, date, close as value " \
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

    def load_type_sws(self):
        sql = f"SELECT stock_id, type_name FROM stock_type_sws WHERE stock_id IN ({constructor.sqlfmt(self.stock_ids)})"
        df = pd.read_sql(sql, self.engine)
        return dict(zip(df["stock_id"], df["type_name"]))

    def load_riskfree(self):
        sql = f"SELECT date, y1 as value " \
              f"FROM ratio_treasury_bond " \
              f"WHERE date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)
        return df.set_index("date")["value"]
