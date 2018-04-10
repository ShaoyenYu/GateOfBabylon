import datetime as dt
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from dateutil.relativedelta import relativedelta
import pandas as pd
from util import io, config as cfg


ENGINE = cfg.default_engine


class KdataCrawler:
    tables = {}

    def __init__(self, code, **kwargs):
        """

        Args:
            id_: str, or list<str>
            **kwargs:
                date_start: datetime.date
                date_end: datetime.date
                ktype: str, optional {"D", "5", "15", "30", "60"}
                pool_size: int

        """

        self.code = [code] if type(code) is str else code
        self.ktype = kwargs.get("ktype")
        self.date_end = kwargs.get("date_end", dt.date.today())
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(weeks=1))

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)

    @classmethod
    def reshaped_kdata(cls, code, ktype, start, end, index=False):
        """
        封装tushare库get_k_data接口, 采集股票和基准的k线数据;

        Args:
            code: str
            ktype: str, optional {"D", "5", "15", "30", "60"}
            start: datetime.date
            end: datetime.date

        Returns:
            pandas.DataFrame{
                index: <datetime.date>
                columns: ["code"<str>, "date"<datetime.date>,
                         "open", "close", "open_fadj", "close_fadj", "open_badj", "close_badj"<float>,
                         "high", "low", "high_fadj", "low_fadj", "high_badj", "low_badj", "volume"<float>],
            }

        """

        start = start.strftime("%Y-%m-%d") if start is not None else "1985-01-01"
        end = end.strftime("%Y-%m-%d") if end is not None else None

        try:

            adj_type = (None, "qfq", "hfq")
            dfs = {
                adj_type: ts.get_k_data(code, ktype=ktype, autype=adj_type, start=start, end=end, index=index, retry_count=10)
                for
                adj_type in adj_type
            }  # 调取不复权, 前复权, 后复权的k线数据

            for k in dfs:  # 合并三种复权数据
                dfs[k].index = dfs[k]["date"]
                if k in {"qfq", "hfq"}:
                    del dfs[k]["date"]
                    del dfs[k]["volume"]
                else:
                    dfs[k][cls.code_name] = dfs[k]["code"]
                del dfs[k]["code"]
            result = dfs[None].join(dfs["qfq"], how="outer", rsuffix="_fadj").join(dfs["hfq"], how="outer", rsuffix="_badj")
            return result

        except Exception as e:
            print(e)


class IndexKdataCrawler(KdataCrawler):
    tables = {
        "s5k": "stock_kdata_5",
        "s15k": "stock_kdata_15",
        "s30k": "stock_kdata_30",
    }
    code_name = "index_id"


class StockKdataCrawler(KdataCrawler):
    tables = {
        "s5k": "stock_kdata_5",
        "s15k": "stock_kdata_15",
        "s30k": "stock_kdata_30",
        "s60k": "stock_kdata_60",
        "sdk": "stock_kdata_d",
    }
    code_name = "stock_id"

    @classmethod
    def store_data(cls, data: pd.DataFrame, datatype: str):
        table = cls.tables[datatype]
        if data is not None:
            io.to_sql(table, ENGINE, data)

    def crwal(self):
        """
        采集k线数据并入库

        Returns:

        """

        f = partial(self.reshaped_kdata, ktype=self.ktype)
        if self.ktype == "D":
            f = partial(f, start=self.date_start, end=self.date_end)

        f_store = partial(self.store_data, datatype=f"s{self.ktype}k".lower())

        # 多线程异步采集, 存储
        for sid in self.code:
            self.thread_pool.apply_async(f, args=(sid,), callback=f_store)

        self.thread_pool.close()
        self.thread_pool.join()


def test():
    import datetime as dt
    sdc = StockKdataCrawler(["000001", "000002"], date_end=dt.date(2018, 2, 25), ktype="D")
    sdc.crwal()
