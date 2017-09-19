from utils import io, config as cfg
import datetime as dt
import pandas as pd
import tushare as tu
from multiprocessing.dummy import Pool as ThreadPool

engine = cfg.default_engine

cols_info = ["name", "industry", "area", "timeToMarket", "date", "stock_id"]
cols_info_ = ["name", "industry", "area", "initial_public_date", "date", "stock_id"]


cols_valuation = ["pe", "pb", "outstanding", "totals", "totalAssets", "liquidAssets", "fixedAssets", "reserved",
                  "reservedPerShare", "esp", "bvps", "stock_id", "date"]
cols_valuation_ = ["pe", "pb", "float_share", "total_share", "total_asset", "liquid_asset", "fixed_asset",
                   "reserved",
                   "reservedps", "eps", "bvps", "stock_id", "date"]


def trans_date(datetime_num):
    try:
        return dt.datetime.strptime(str(datetime_num), "%Y%m%d").date()
    except:
        return None


def fetch(date):
    print(date)
    try:
        err_list = {}
        df = tu.get_stock_basics(date.strftime("%Y-%m-%d"))
        df["timeToMarket"] = df["timeToMarket"].apply(lambda x: trans_date(x))
        df["date"] = date
        df["stock_id"] = df.index
        df["name"] = df["name"].apply(lambda x: x.replace(" ", ""))
        df_info = df[cols_info]
        df_info.columns = cols_info_
        df_valuation = df[cols_valuation]
        df_valuation.columns = cols_valuation_
        with engine.connect() as conn:
            io.to_sql("stock_info_hist", conn, df_info)
            io.to_sql("stock_valuation", conn, df_valuation)
            conn.close()
        return True
    except Exception as e:
        err_list[date] = (e, df_info, df_valuation)
        print(date, e)
    finally:
        return err_list


def main():
    dates = pd.date_range(dt.date(2016, 8, 9), dt.date(2017, 9, 18), freq="B")
    tasks = [date.date() for date in dates]
    pool = ThreadPool(20)
    errors = pool.map(fetch, tasks)
    pool.close()
    pool.join()
    return errors

if __name__ == "__main__":
    q = main()
