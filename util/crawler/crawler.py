import datetime as dt
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from dateutil.relativedelta import relativedelta
import pandas as pd
from util import io, config as cfg

DEFAULT_ENGINE = cfg.default_engine


class KdataCrawler:
    tables = {}
    code_name = None
    index = False

    def __init__(self, code=None, **kwargs):
        """
        实例化crawler

        Args:
            code: str, list<str>, or None, default None
                code为None时, 时候需要在子类实现load_codes方法

            **kwargs:
                date_start: datetime.date
                date_end: datetime.date
                ktype: str, optional {"D", "5", "15", "30", "60"}
                engine: sqlalchemy.engine.base.Engine
                pool_size: int

        """

        self._code = [code] if type(code) is str else code
        self.ktype = kwargs.get("ktype")
        self.date_end = kwargs.get("date_end", dt.date.today())
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(weeks=1))
        self.engine = kwargs.get("engine", DEFAULT_ENGINE)

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)

    def load_codes(self):
        """
        若初始实例时传入的code为None, 加载需要采集的代码至_code属性.

        Returns:

        """

        raise NotImplementedError(
            "Must implement `load_codes` method if instance initialized without `code` parameter.")

    @property
    def code(self):
        if not self._code:
            self.load_codes()
        return self._code

    @classmethod
    def reshaped_kdata(cls, code, ktype, start=None, end=None, index=False):
        """
        封装tushare.get_k_data接口, 采集股票和基准的k线数据.

        Args:
            code: str
            ktype: str, optional {"D", "5", "15", "30", "60"}
            start: datetime.date, or None
            end: datetime.date, or None

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
                adj_type: ts.get_k_data(code, ktype=ktype, autype=adj_type, start=start, end=end,
                                        index=index or cls.index, retry_count=10)
                for
                adj_type in adj_type
            }  # 调取不复权, 前复权, 后复权的k线数据

            for k in dfs:  # 合并三种复权数据
                dfs[k].index = dfs[k]["date"]
                if k in {"qfq", "hfq"}:
                    del dfs[k]["date"]
                    del dfs[k]["volume"]
                else:
                    dfs[k][cls.code_name] = code
                del dfs[k]["code"]
            result = dfs[None].join(
                dfs["qfq"], how="outer", rsuffix="_fadj").join(
                dfs["hfq"], how="outer", rsuffix="_badj")
            return result

        except Exception as e:
            print(e)

    def store(self, data: pd.DataFrame):
        if data is not None:
            io.to_sql(self.tables[self.ktype], self.engine, data)

    def crwal(self):
        """
        采集k线数据并入库

        Returns:

        """

        f = partial(self.reshaped_kdata, ktype=self.ktype)
        if self.ktype == "D":
            f = partial(f, start=self.date_start, end=self.date_end)

        # 多线程异步采集, 存储
        [self.thread_pool.apply_async(f, args=(sid,), callback=self.store) for sid in self.code]

        self.thread_pool.close()
        self.thread_pool.join()


class IndexKdataCrawler(KdataCrawler):
    tables = {
        "5": "index_kdata_5",
        "15": "index_kdata_15",
        "30": "index_kdata_30",
        "60": "index_kdata_60",
        "D": "index_kdata_d_test",
    }
    code_name = "index_id"
    index = True  # 调用reshaped_kdata方法时, 是否采集指数

    def load_codes(self):
        self._code = sorted([x[0] for x in self.engine.execute("SELECT DISTINCT index_id FROM index_info").fetchall()])


class StockKdataCrawler(KdataCrawler):
    tables = {
        "5": "stock_kdata_5",
        "15": "stock_kdata_15",
        "30": "stock_kdata_30",
        "60": "stock_kdata_60",
        "D": "stock_kdata_d",
    }
    code_name = "stock_id"

    def load_codes(self):
        self._code = sorted([x[0] for x in self.engine.execute("SELECT DISTINCT stock_id FROM stock_info").fetchall()])
