import numpy as np
import pandas as pd
import datetime as dt
from functools import partial, reduce
from multiprocessing.dummy import Pool as ThreadPool
from utils.configcenter import config as cfg
from utils.timeutils import const


def check_tickdata(start: dt.datetime, end: dt.datetime):
    engine = cfg.default_engine

    stocks_to_test = ("000001", "600000", "002766", "002777", "002598", "601727")

    def fetch_one(date, stock_id):
        datetime = date.strftime("%Y%m%d")
        datetime_start, datetime_end = (f"{datetime}{time_part}" for time_part in ("000000", "235959"))
        sql = "SELECT DATE_FORMAT(`time`, '%%Y%%m%%d') t, COUNT(stock_id) as cnt " \
              f"FROM `stock_tickdata_{datetime[:6]}` " \
              f"WHERE stock_id = '{stock_id}' AND `time` BETWEEN '{datetime_start}' AND '{datetime_end}'" \
              "GROUP BY t"
        df = pd.read_sql(sql, engine)
        df["stock_id"] = stock_id
        if len(df) != 0:
            return df
        return pd.DataFrame({"t": [datetime], "cnt": [np.nan], "stock_id": [stock_id]})

    def fetch():
        p = ThreadPool(20)
        dates = pd.date_range(start, end, freq=const.bday_chn)
        res = []
        for stock_id in stocks_to_test:
            f = partial(fetch_one, stock_id=stock_id)
            res.extend(p.map(f, dates))
        res = pd.concat(res)

        return res

    def stats():
        res = fetch()
        grouped = res[res["cnt"].isna()].groupby("t")

        s1 = grouped["t"].count()
        s2 = grouped["stock_id"].apply(lambda x: ",".join(sorted(x)))
        s3 = pd.Series(s1 / len(stocks_to_test), name="t_pct")
        return pd.concat([s1, s2, s3], axis=1)

    return stats()


def main():
    start, end = dt.datetime(2017, 10, 1), dt.datetime(2017, 10, 30)
    print(check_tickdata(start, end))

    # import tushare as ts
    # ts.get_tick_data("600399", "2017-12-05", src="tt")


if __name__ == '__main__':
    main()
