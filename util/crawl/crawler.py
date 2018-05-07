import datetime as dt
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from dateutil.relativedelta import relativedelta
import pandas as pd
from util import io, config as cfg
from util.decofactory import common
import calendar as cld

DEFAULT_ENGINE = cfg.default_engine


def date2str(date):
    return date.strftime("%Y-%m-%d")


# 采集工具类基类
class BaseCrawler:
    def __init__(self, **kwargs):
        self.engine = kwargs.get("engine", DEFAULT_ENGINE)

        self.pool_size = kwargs.get("pool_size", 8)
        self.thread_pool = ThreadPool(self.pool_size)


# 基本面数据
class BasicDataCrawler(BaseCrawler):
    code_name = "stock_id"

    def __init__(self, year, season, **kwargs):
        super().__init__(**kwargs)

        self.year = year
        self.season = season
        self.year_season = year * 100 + season

    @classmethod
    def guess_report_date(cls, year, season, report_date):
        m, d = [int(x) for x in report_date.split("-")]
        earliest_report_date = dt.date(year, season * 3, cld.monthrange(year, season * 3)[1])
        cur_report_date = dt.date(year, m, d)

        while cur_report_date < earliest_report_date:
            cur_report_date = dt.date(cur_report_date.year + 1, m, d)
        return cur_report_date

    @classmethod
    def base(cls, ):
        pass

    def performance(self, year, season):
        pass

    def revenue(self):
        df = ts.get_profit_data(self.year, self.season).rename(columns={"code": "stock_id", "gross_profit_rate": "gross_profit_ratio"}).drop(labels=["name"], axis=1)
        df["season"] = self.year_season
        return df

    def cashflow(self):
        df = ts.get_cashflow_data(self.year, self.season).rename(columns={"code": "stock_id", "rateofreturn": "ofreturn_ratio"}).drop(labels=["name"], axis=1)
        df["season"] = self.year_season
        return df

    def crawl(self):
        # io.to_sql("babylon.stock_revenue", self.engine, self.revenue())
        io.to_sql("babylon.stock_cashflow", self.engine, self.cashflow())


# K线数据基类
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
    def kdata(cls, code, ktype, start=None, end=None, index=False):
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

        except KeyError:
            print(f"{code} has no data during {start}-{end}")

    def store(self, data: pd.DataFrame):
        if data is not None:
            io.to_sql(self.tables[self.ktype], self.engine, data)

    @common.log
    def crwal(self):
        """
        采集k线数据并入库

        Returns:

        """

        f = partial(self.kdata, ktype=self.ktype)
        if self.ktype == "D":
            f = partial(f, start=self.date_start, end=self.date_end)

        # 多线程异步采集, 存储
        [self.thread_pool.apply_async(f, args=(sid,), callback=self.store) for sid in self.code]

        self.thread_pool.close()
        self.thread_pool.join()


# 分笔数据基类
class TickCrawler(BaseCrawler):
    code_name = "stock_id"

    def __init__(self, code=None, **kwargs):
        super().__init__(**kwargs)

        self._code = [code] if type(code) is str else code
        self.date_end = kwargs.get("date_end", dt.date.today())
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(days=0 + (self.date_end.weekday() - 4) * (self.date_end.weekday() in (5, 6))))

    @classmethod
    def _safe_float(cls, x):
        try:
            return float(x)
        except:
            return None

    @classmethod
    def _create_tickdata_table(cls, year_season, engine):
        """

        Args:
            year_season: str
            "YYYYMM"

        Returns:

        """

        sql = f"""
                CREATE TABLE `stock_tickdata_{year_season}` (
                  `stock_id` varchar(10) NOT NULL,
                  `time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE current_timestamp(),
                  `price` decimal(6,2) DEFAULT NULL,
                  `change` decimal(4,2) DEFAULT NULL,
                  `volume` mediumint(9) unsigned DEFAULT NULL,
                  `amount` int(12) DEFAULT NULL,
                  `type` enum('买盘','卖盘','中性盘') DEFAULT NULL,
                  PRIMARY KEY (`stock_id`,`time`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        print(sql)
        try:
            engine.execute(sql)
        except:
            pass

    @classmethod
    def _check_table(cls, date_range, engine):
        ys_lst = sorted(set([x.strftime("%Y%m") for x in date_range]))
        for ys in ys_lst:
            cls._create_tickdata_table(ys, engine)

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
    def tickdata(cls, code, date):
        """
        封装tushare.get_tick_data()接口, 采集股票分笔交易数据;

        Args:
            code:
            date: datetime.date

        Returns:

        """
        try:
            res = ts.get_tick_data(code, date2str(date), src="tt", pause=2, retry_count=10)
            if res.loc[0, "time"] == 'alert("当天没有数据");':  # 该参数下无正确数据返回
                return pd.DataFrame(columns=[cls.code_name, "time", "price", "change", "vaolume", "amount", "type"])

            res["time"] = res["time"].apply(
                lambda x: dt.datetime(*[date.year, date.month, date.day, *[int(x) for x in x.split(":")]]))
            res["change"] = res["change"].apply(lambda x: cls._safe_float(x))
            res[cls.code_name] = code
            print(res)
            return res
        except Exception:
            print(Exception, code, date)

    def store(self, data: pd.DataFrame):
        """
            将分笔数据存表存储;

        Args:
            data:

        Returns:

        """

        if data is not None:
            original_cols = data.columns
            data["__ys"] = data["time"].apply(lambda x: x.strftime("%Y%m"))
            year_season = set(data["__ys"])
            for ys in year_season:
                io.to_sql(f"stock_tickdata_{ys}", self.engine, data.loc[data["__ys"] == ys, original_cols])

    def crawl(self):
        """
        采集股票分笔数据

        Returns:

        """

        date_ranges = pd.date_range(self.date_start, self.date_end, freq="B")
        self._check_table(date_ranges, self.engine)

        # 多线程异步采集, 存储
        for date in date_ranges[::-1]:
            for code in self.code:
                self.thread_pool.apply_async(self.tickdata, args=(code, date), callback=self.store)

        self.thread_pool.close()
        self.thread_pool.join()


class IndexKdataCrawler(KdataCrawler):
    tables = {
        "5": "index_kdata_5",
        "15": "index_kdata_15",
        "30": "index_kdata_30",
        "60": "index_kdata_60",
        "D": "index_kdata_d",
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


# def test():
#     # 股票K线
#     StockKdataCrawler.kdata("000001", ktype="D", start=dt.date(2018, 4, 11), end=dt.date(2018, 4, 12))
#     StockKdataCrawler("000001", ktype="D", date_start=dt.date(2018, 4, 11), date_end=dt.date(2018, 4, 12)).crwal()
#
#     # 股票历史分笔
#     StockTickCrawler(date_start=dt.date(2018, 1, 1), date_end=dt.date(2018, 1, 31)).crawl()
#     # TickCrawler.tickdata("000001", dt.date(2018, 4, 9))
#     # TickCrawler.tickdata("000001", dt.date(2018, 4, 9))
#
#     # 股票财报数据
#     q1 = BasicDataCrawler(2017, 3).revenue()
#     BasicDataCrawler(2017, 4).crawl()
#     for y, s in [(y, s) for y in range(2005, 2018) for s in (1, 2, 3, 4)]:
#         print(y, s)
#         BasicDataCrawler(y, s).crawl()
#
#     for q in [q1, q2]:
#         print(q.loc[q.stock_id == "300504"])
#
#
# def test2():
#     import matplotlib.pyplot as plt
#     plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
#     plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
#
#     def check(sid):
#         sql = f"""
#         SELECT t1.stock_id, t1.close, t1.close_fadj, t1.season, t1.date, t2.roe, t2.eps, t2.business_income, t2.net_profits, t2.net_profit_ratio
#     FROM
#     (SELECT stock_id, close, (YEAR(date) * 100 + floor(MONTH(date) / 4) + 1) as season, date, close_fadj FROM stock_kdata_d) t1
#     JOIN `stock_revenue` t2 ON t1.stock_id = t2.stock_id AND t1.season = t2.season
#     WHERE t1.stock_id = "{sid}";"""
#         d = pd.read_sql(sql, DEFAULT_ENGINE)
#         q = d[["close", "eps", "roe"]]
#         q.index = d["date"]
#
#         # d1 = (q - q.mean()) / q.std()
#         d1 = (q - q.min()) / (q.max() - q.min())
#         shift_day = int(90 * 1.15)
#         plt.plot(d1["close"], "r")
#         # plt.plot(d1["eps"].shift(0), "g")
#         plt.plot(d1["eps"].shift(shift_day), "g")
#         plt.plot(d1["roe"].shift(shift_day), "b")
#         sname = DEFAULT_ENGINE.execute(f"SELECT name FROM stock_info WHERE stock_id = '{sid}'").fetchone()[0]
#         plt.title(f"{sid} {sname}")
#         # plt.plot(d1["close"] / d1["eps"], "b")
#
#         # plt.plot(q["eps"])
#         plt.show()
#     # sids = [str(x).zfill(6) for x in pd.read_clipboard(header=None)[0].tolist()]
#     sids = [x[0] for x in DEFAULT_ENGINE.execute("SELECT stock_id FROM stock_info WHERE industry = '银行'")]
#
#     for sid in sids:
#         check(sid)
