import datetime as dt
import pandas as pd
from dataclasses import dataclass
from utils.configcenter import config as cfg
from utils.sqlfactory import constructor


@dataclass
class TsLoader:
    ids: list
    start: dt.datetime
    end: dt.datetime
    engine = cfg.default_engine


class StockDataLoader(TsLoader):
    def load_price(self):
        sql = "SELECT stock_id, date, close as value " \
              "FROM babylon.stock_kdata_d " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return df.pivot(index="date", columns="stock_id", values="value")

    def load_turnover(self, bs):
        types = {"b": "买盘", "s": "卖盘", "m": "中性"}
        sql = "SELECT stock_id, date, volume as value " \
              "FROM babylon.stock_turnover " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}' " \
              f"AND type = '{types[bs]}'"
        df = pd.read_sql(sql, self.engine)

        return df.pivot(index="date", columns="stock_id", values="value")

    def load_type_sws(self):
        sql = f"SELECT stock_id, type_name FROM stock_type_sws WHERE stock_id IN ({constructor.sqlfmt(self.ids)})"
        df = pd.read_sql(sql, self.engine)

        return dict(zip(df["stock_id"], df["type_name"]))

    def load_riskfree(self):
        sql = f"SELECT date, y1 as value " \
              f"FROM ratio_treasury_bond " \
              f"WHERE date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return df.set_index("date")["value"]


class BenchmarkLoader(TsLoader):
    def load_price(self):
        sql = "SELECT index_id, date, close as value " \
              "FROM index_kdata_d " \
              f"WHERE index_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return df.pivot(index="date", columns="index_id", values="value")


class RiskfreeBenchmark(TsLoader):
    def load_return(self):
        sql = f"SELECT {constructor.sqlfmt(self.ids, 'col')}, `date` " \
              f"FROM ratio_treasury_bond " \
              f"WHERE date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return df.set_index("date")
