import datetime as dt
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from dateutil.relativedelta import relativedelta
import pandas as pd
from util import io, config as cfg

DEFAULT_ENGINE = cfg.default_engine


def date2str(date):
    return date.strftime("%Y-%m-%d")


class BaseCrawler:
    def __init__(self, **kwargs):
        self.engine = kwargs.get("engine", DEFAULT_ENGINE)

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)


class KdataCrawler(BaseCrawler):
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
        super().__init__(**kwargs)

        self._code = [code] if type(code) is str else code
        self.ktype = kwargs.get("ktype")
        self.date_end = kwargs.get("date_end", dt.date.today())
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(weeks=1))

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

        start = date2str(start) if start is not None else "1985-01-01"
        end = date2str(end) if end is not None else None

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


class TickCrawler(BaseCrawler):
    code_name = "stock_id"

    def __init__(self, code, date_start, date_end, kwargs):
        super().__init__(**kwargs)

        self._code = [code] if type(code) is str else code
        self.date_start = date_start
        self.date_end = date_end

        self.engine = kwargs.get("engine", DEFAULT_ENGINE)

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)

    @classmethod
    def _safe_float(cls, x):
        try:
            return float(x)
        except:
            return None

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
    def reshaped_tickdata(cls, code, date):
        """

        Args:
            code:
            date: datetime.date

        Returns:

        """

        try:
            res = ts.get_tick_data(code, date2str(date))
        except:
            return pd.DataFrame(columns=[cls.code_name, "time", "price", "change", "vaolume", "amount", "type"])

        res["time"] = res["time"].apply(
            lambda x: dt.datetime(*[date.year, date.month, date.day, *[int(x) for x in x.split(":")]]))
        res["change"] = res["change"].apply(lambda x: cls._safe_float(x))
        res[cls.code_name] = code
        return res

    def crawl(self):
        """
        采集股票分笔数据

        Returns:

        """

        date_ranges = pd.date_range(self.date_start, self.date_end)

        # 多线程异步采集, 存储
        for date in date_ranges:
            for code in self.code:
                self.thread_pool.apply_async(self.reshaped_tickdata(code, date), callback=self.store)

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


class StockTickCrawler(TickCrawler):

    def load_codes(self):
        self._code = sorted([x[0] for x in self.engine.execute("SELECT DISTINCT stock_id FROM stock_info").fetchall()])

    def store(self):
        pass


def test():
    # 股票K线
    StockKdataCrawler.reshaped_kdata("000001", ktype="D", start=dt.date(2018, 4, 11), end=dt.date(2018, 4, 12))
    StockKdataCrawler("000001", ktype="D", date_start=dt.date(2018, 4, 11), date_end=dt.date(2018, 4, 12)).crwal()

    # 股票历史分笔
    TickCrawler.reshaped_tickdata("000001", dt.date(2018, 4, 9))
