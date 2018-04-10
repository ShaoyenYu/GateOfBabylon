import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from dateutil.relativedelta import relativedelta
import pandas as pd
from util import io, config as cfg


ENGINE = cfg.default_engine


class StockDataCrawler:
    table_of_data = {
        "s5k": "stock_kdata_5",
        "s15k": "stock_kdata_15",
        "s30k": "stock_kdata_30",
        "s60k": "stock_kdata_60",
        "sdk": "stock_kdata_d",
    }

    def __init__(self, stock_id, **kwargs):
        """

        Args:
            stock_id: str, or list<str>
            **kwargs:
                date_start: datetime.date
                date_end: datetime.date
                ktype: str, optional {"D", "5", "15", "30", "60"}
                pool_size: int

        """

        self.stock_id = [stock_id] if type(stock_id) is str else stock_id
        self.ktype = kwargs.get("ktype")
        self.date_end = kwargs.get("date_end")
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(weeks=1))

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)

    @classmethod
    def _reshape_kdata(cls, stock_id, ktype, start, end):
        """

        Args:
            stock_id: str
            ktype: str, optional {"D", "5", "15", "30", "60"}
            start: datetime.date
            end: datetime.date

        Returns:
            pandas.DataFrame{
                index: <datetime.date>
                columns: ["stock_id"<str>, "date"<datetime.date>,
                         "open", "close", "open_fadj", "close_fadj", "open_badj", "close_badj"<float>,
                         "high", "low", "high_fadj", "low_fadj", "high_badj", "low_badj", "volume"<float>],
            }

        """

        start = start.strftime("%Y-%m-%d") if start is not None else "1985-01-01"
        end = end.strftime("%Y-%m-%d") if end is not None else None

        try:

            adj_type = (None, "qfq", "hfq")
            dfs = {
                adj_type: ts.get_k_data(stock_id, ktype=ktype, autype=adj_type, start=start, end=end, retry_count=10)
                for
                adj_type in adj_type
            }
            for k in dfs:
                dfs[k].index = dfs[k]["date"]
                if k in {"qfq", "hfq"}:
                    del dfs[k]["date"]
                    del dfs[k]["volume"]
                else:
                    dfs[k]["stock_id"] = dfs[k]["code"]
                del dfs[k]["code"]
            result = dfs[None].join(dfs["qfq"], how="outer", rsuffix="_fadj").join(dfs["hfq"], how="outer",
                                                                                   rsuffix="_badj")
            return result

        except Exception:
            pass

    @classmethod
    def store_data(cls, data: pd.DataFrame, datatype: str):
        table = cls.table_of_data[datatype]
        if data is not None:
            io.to_sql(table, ENGINE, data)

    def crwal_kdata(self):
        """
        采集k线数据并入库

        Returns:

        """

        f = partial(self._reshape_kdata, ktype=self.ktype)
        if self.ktype == "D":
            f = partial(f, start=self.date_start, end=self.date_end)

        f_store = partial(self.store_data, datatype=f"s{self.ktype}k".lower())

        # 多线程异步采集, 存储
        for sid in self.stock_id:
            self.thread_pool.apply_async(f, args=(sid,), callback=f_store)

        self.thread_pool.close()
        self.thread_pool.join()


def test():
    import datetime as dt
    sdc = StockDataCrawler(["000001", "000002"], date_end=dt.date(2018, 2, 25), ktype="D")
    sdc.crwal_kdata()
