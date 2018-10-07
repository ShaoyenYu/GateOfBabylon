import numpy as np
import pandas as pd
import datetime as dt
from functools import partial, reduce
from multiprocessing.dummy import Pool as ThreadPool
from utils.configcenter import config as cfg
from utils.timeutils import const


def check_tickdata(start: dt.datetime, end: dt.datetime):
    engine = cfg.default_engine

    stocks_to_test = ("000001", "600000")

    def fetch_one(date):
        datetime = date.strftime("%Y%m%d")
        datetime_start, datetime_end = (f"{datetime}{time_part}" for time_part in ("000000", "235959"))
        for idx, stock_id in enumerate(stocks_to_test, start=1):
            sql = "SELECT DATE_FORMAT(`time`, '%%Y%%m%%d') t, COUNT(stock_id) as cnt " \
                  f"FROM `stock_tickdata_{datetime[:6]}` " \
                  f"WHERE stock_id = '{stock_id}' AND `time` BETWEEN '{datetime_start}' AND '{datetime_end}'" \
                  "GROUP BY t"
            df = pd.read_sql(sql, engine)
            if len(df) != 0:
                return df
            else:
                print(f"no data for sample[{idx}]({datetime}, {stock_id}), continue...")
                continue
        return pd.DataFrame({"t": [datetime], "cnt": [np.nan]})

    def fetch():
        dates = pd.date_range(start, end, freq=const.bday_chn)
        p = ThreadPool(20)
        res = pd.concat(p.map(fetch_one, dates))
        return res

    def stats():
        res = fetch()
        print(res)
        return res[res["cnt"].isna()]["t"].apply(lambda x: dt.datetime(*(int(x) for x in [x[:4], x[4:6], x[6:8]])))

    return stats()


def main():
    start, end = dt.datetime(2018, 1, 1), dt.datetime(2018, 9, 30)
    errors = check_tickdata(start, end)


if __name__ == '__main__':
    main()
