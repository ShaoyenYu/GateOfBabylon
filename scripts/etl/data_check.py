import pandas as pd
import datetime as dt
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from utils.configcenter import config as cfg
from utils.timeutils import const


def check_tickdata(start: dt.datetime, end: dt.datetime):
    engine = cfg.default_engine

    stocks_to_test = ("000001", "600000", "002766", "002777", "002598", "601727")
    stocks_to_test = pd.read_sql("SELECT stock_id FROM stock_info", engine)["stock_id"].tolist()

    def fetch_one(stock_id, date):
        # print(date, stock_id)

        def _fill(df_tmp):
            if len(df_tmp) == 0:
                df_tmp = df_tmp.reindex(index=[0])
                df_tmp["cnt"] = 0
            df_tmp["stock_id"] = stock_id
            df_tmp["t"] = datetime
            return df_tmp
            # return pd.DataFrame({"t": [datetime], "cnt": [np.nan], "stock_id": [stock_id]})

        datetime = date.strftime("%Y%m%d")
        datetime_start, datetime_end = (f"{datetime}{time_part}" for time_part in ("000000", "235959"))
        sql_1 = f"SELECT 1 as cnt FROM stock_kdata_d WHERE stock_id = '{stock_id}' AND `date` = '{date}'"
        sql_2 = "SELECT COUNT(stock_id) as cnt " \
                f"FROM `stock_tickdata_{datetime[:6]}` " \
                f"WHERE stock_id = '{stock_id}' AND `time` BETWEEN '{datetime_start}' AND '{datetime_end}'" \
                "GROUP BY DATE_FORMAT(`time`, '%%Y%%m%%d')"
        df1 = pd.read_sql(sql_1, engine)
        df2 = pd.read_sql(sql_2, engine)
        df1, df2 = (_fill(x) for x in (df1, df2))
        return df1, df2,

    def fetch(date):
        p = ThreadPool(100)
        res = []
        f = partial(fetch_one, date=date)
        res.extend(p.map(f, stocks_to_test))
        p.close()
        p.join()
        res1 = pd.concat([x[0] for x in res])
        res2 = pd.concat([x[1] for x in res])

        res = res1.merge(res2, on=["stock_id", "t"], how="outer")
        res[["cnt_x", "cnt_y"]].fillna(0, inplace=True)
        res["cnt_y"] = res["cnt_y"].apply(lambda x: int(x > 0))
        res["delta"] = list(map(lambda x, y: f"{x}{y}", res["cnt_x"], res["cnt_y"]))
        res["_"] = 1
        g = res.groupby(["t", "delta"])["_"].sum()
        return g

    dates = pd.date_range(start, end, freq=const.bday_chn)[::-1]
    results = []
    for date in dates:
        tmp = fetch(date)
        print(tmp)
        results.append(tmp)

    return results


def main():
    start, end = dt.datetime(2018, 6, 1), dt.datetime(2018, 10, 11)
    check_tickdata(start, end)

    # import tushare as ts
    # ts.get_tick_data()
    # ts.get_tick_data("600399", "2017-12-05", src="tt")
    # ts.set_token("a3a0919479f5cda6382245a184c405856f805e090529c1bed7c31036")
    # pro = ts.pro_api()
    #
    # df = pro.index_weight(index_code='399300.SZ', trade_date='20180903')


if __name__ == '__main__':
    main()
