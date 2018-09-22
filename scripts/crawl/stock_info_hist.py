import datetime as dt
import pandas as pd
import tushare as ts
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from utils.io import sql
from utils.configcenter import config as cfg


def trans_date(datetime_num):
    try:
        return dt.datetime.strptime(str(datetime_num), "%Y%m%d").date()
    except:
        return None


def fetch(date, engine):
    print(date)
    cols_info = ["name", "industry", "area", "timeToMarket", "date", "stock_id"]
    cols_info_ = ["name", "industry", "area", "initial_public_date", "date", "stock_id"]

    cols_valuation = ["pe", "pb", "outstanding", "totals", "totalAssets", "liquidAssets", "fixedAssets", "reserved",
                      "reservedPerShare", "esp", "bvps", "stock_id", "date"]
    cols_valuation_ = ["pe", "pb", "float_share", "total_share", "total_asset", "liquid_asset", "fixed_asset",
                       "reserved", "reservedps", "eps", "bvps", "stock_id", "date"]
    try:
        err_list = {}
        df = ts.get_stock_basics(date.strftime("%Y-%m-%d"))
        df["timeToMarket"] = df["timeToMarket"].apply(lambda x: trans_date(x))
        df["date"] = date
        df["stock_id"] = df.index
        df["name"] = df["name"].apply(lambda x: x.replace(" ", ""))
        df_info = df[cols_info]
        df_info.columns = cols_info_
        df_valuation = df[cols_valuation]
        df_valuation.columns = cols_valuation_
        with engine.connect() as conn:
            sql.to_sql("stock_info_hist", conn, df_info)
            sql.to_sql("stock_valuation", conn, df_valuation)
            conn.close()
        return True
    except Exception as e:
        err_list = (date, e, df_info, df_valuation)
        print(date, e)
    finally:
        return err_list


def main(start=dt.date.today() - dt.timedelta(7), end=dt.date.today()):
    print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {__name__}; START;")
    dates = pd.date_range(start, end, freq="B")
    tasks = [date.date() for date in dates]
    pool = ThreadPool(20)
    engine = cfg.default_engine
    errors = pool.map(partial(fetch, engine=engine), tasks)
    pool.close()
    pool.join()
    print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {__name__}; RECORDS NUM: {len(tasks)}; DONE;")
    return errors


if __name__ == "__main__":
    main(dt.date.today() - dt.timedelta(7), dt.date.today())
