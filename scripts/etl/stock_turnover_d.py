import datetime as dt
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from utils.io import sql
from utils.configcenter import config as cfg
from utils.sql import constructor


ENGINE = cfg.default_engine


def fetch_ids():
    return [x[0] for x in ENGINE.execute("SELECT stock_id FROM stock_info")]


def fetch_data(stock_ids, date):
    sql = f"SELECT stock_id, type, SUM(volume) as volume, SUM(amount) as amount " \
          f"FROM `stock_tickdata_{date.strftime('%Y%m')}` " \
          f"WHERE `stock_id` IN ({constructor.sqlfmt(stock_ids)}) " \
          f"AND `time` BETWEEN '{date.strftime('%Y%m%d')}000000' AND '{date.strftime('%Y%m%d')}235959' " \
          f"GROUP BY stock_id, type"
    df = pd.read_sql(sql, ENGINE)
    df["date"] = date
    return df


def save_to_db(dataframe):
    print(dataframe)
    sql.to_sql("stock_turnover", ENGINE, dataframe)


def main(start=dt.date.today() - dt.timedelta(7), end=dt.date.today()):
    ids = fetch_ids()
    p = ThreadPool(8)
    for date in pd.date_range(start, end):
        p.apply_async(fetch_data, args=(ids, date), callback=save_to_db)
    p.close()
    p.join()


if __name__ == "__main__":
    main((dt.date.today() - dt.timedelta(7)), dt.date.today())
