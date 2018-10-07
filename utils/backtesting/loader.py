import datetime as dt
import numpy as np
import pandas as pd
from functools import wraps
from utils.configcenter import config as cfg
from utils.sql import constructor


def debug(func):
    @wraps(func)
    def wrapper(this, *args, **kwargs):
        print(f"loading data...({this.__class__}.{func.__name__})")
        res = func(this, *args, **kwargs)
        print(f"done...({func.__name__})")
        return res

    return wrapper


class TsLoader:
    def __init__(self, ids: list, start: dt.datetime, end: dt.datetime, engine=None):
        self.ids = ids
        self.start = start
        self.end = end
        self.engine = engine or cfg.default_engine

    @staticmethod
    def date2datetime64(dataframe):
        return pd.DataFrame(dataframe, dataframe.index.astype("M8[ns]"), dataframe.columns)


class StockDataLoader(TsLoader):
    @debug
    def load_listeddate(self):
        sql = "SELECT `stock_id`, `initial_public_date` as `listed_date` " \
              "FROM babylon.stock_info " \
              f"WHERE `stock_id` in ({constructor.sqlfmt(self.ids)})"
        df = pd.read_sql(sql, self.engine).dropna(subset=["listed_date"])

        df["listed_date"] = df["listed_date"].values.astype(np.datetime64)
        df.index = df["listed_date"]

        df = df.pivot(columns="stock_id", values="listed_date")
        df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq="d")).ffill()  # to hold a full date range
        return df

    @debug
    def load_price(self):
        sql = "SELECT stock_id, date, close as value " \
              "FROM babylon.stock_kdata_d " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return self.date2datetime64(df.pivot(index="date", columns="stock_id", values="value"))

    @debug
    def load_turnover(self, bs):
        types = {"b": "买盘", "s": "卖盘", "m": "中性"}
        sql = "SELECT stock_id, date, volume as value " \
              "FROM babylon.stock_turnover " \
              f"WHERE stock_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}' " \
              f"AND type = '{types[bs]}'"
        df = pd.read_sql(sql, self.engine)

        return self.date2datetime64(df.pivot(index="date", columns="stock_id", values="value"))

    @debug
    def load_type_sws(self):
        sql = f"SELECT stock_id, type_name FROM stock_type_sws WHERE stock_id IN ({constructor.sqlfmt(self.ids)})"
        df = pd.read_sql(sql, self.engine)

        return dict(zip(df["stock_id"], df["type_name"]))

    @debug
    def load_riskfree(self):
        sql = f"SELECT date, y1 as value " \
              f"FROM ratio_treasury_bond " \
              f"WHERE date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)
        self.date2datetime64(df)

        return self.date2datetime64(df.set_index("date")["value"])


class BenchmarkLoader(TsLoader):
    @debug
    def load_price(self):
        sql = "SELECT index_id, date, close as value " \
              "FROM index_kdata_d " \
              f"WHERE index_id IN ({constructor.sqlfmt(self.ids)}) " \
              f"AND date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return self.date2datetime64(df.pivot(index="date", columns="index_id", values="value"))


class RiskfreeBenchmark(TsLoader):
    @debug
    def load_return(self):
        sql = f"SELECT {constructor.sqlfmt(self.ids, 'col')}, `date` " \
              f"FROM ratio_treasury_bond " \
              f"WHERE date BETWEEN '{self.start}' AND '{self.end}'"
        df = pd.read_sql(sql, self.engine)

        return self.date2datetime64(df.set_index("date"))
