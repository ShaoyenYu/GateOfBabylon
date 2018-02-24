from importlib import reload
import datetime as dt
import pandas as pd
from util.algorithm import indicator
reload(indicator)

# TODO 使用unittest进行单元测试

def main():
    from util import config as cfg
    import wrapcache
    import hashlib, pickle

    def get_stockprice(conn, num):
        # 20120925 - 20170925
        sids = str(
            tuple(pd.read_sql(f"SELECT stock_id FROM stock_info LIMIT 0, {num}", conn)["stock_id"])
        )
        df = pd.read_sql(f"SELECT stock_id, date, close_fadj FROM stock_kdata_d WHERE stock_id IN {sids}", conn)
        df.index = pd.Index(df.date.tolist(), name="datetime")
        df = df[(df.index >= dt.datetime(2012, 9, 25)) & (df.index <= dt.datetime(2017, 9, 25))]
        grouped = df.groupby(["date", "stock_id"])["close_fadj"]
        res = grouped.last().unstack()
        return res

    def get_index(conn):
        # 000300-沪深300, 上证50-000016, 国债指数-000012
        # 20120925 - 20170925
        df = pd.read_sql("SELECT index_id, date, close_fadj FROM index_kdata_d WHERE index_id IN ('000300', '000016', '000012')", conn)
        df.index = pd.Index(df.date.tolist(), name="datetime")
        df = df[(df.index >= dt.datetime(2012, 9, 25)) & (df.index <= dt.datetime(2017, 9, 25))]
        grouped = df.groupby(["date", "index_id"])["close_fadj"]
        res = grouped.last().unstack()
        bm = res[["000300", "000016"]]
        rf = res[["000012"]]
        return bm, rf

    def init_frame():
        return pd.DataFrame([[3, 1, 4, 2], [5, 3, 3, 1]]).T

    engine = cfg.default_engine

    q = get_stockprice(engine, 20)
    q_y1 = q.loc[q.index >= dt.datetime(2016,9,25)]
    p_bm, p_rf = get_index(engine)
    t = init_frame()
    # bm["000001_"] = q["000001"].tolist()

    reload(indicator)
    hashlib.md5(pickle.dumps(q)).hexdigest()

    r = indicator.Derivative.return_series(q)
    r_bm = indicator.Derivative.return_series(p_bm)
    r_rf = indicator.Derivative.return_series(p_rf)
    er = indicator.Derivative.excess_return(q, p_bm)
    er_a = indicator.Derivative.excess_return_a(q, p_bm, 250)
    r_a = indicator.Derivative.annualized_return(q, 250, "a")
    r_a = indicator.Derivative.annualized_return(q, 250, "m")
    rbm_a = indicator.Derivative.annualized_return(p_bm, 250, "a")
    std = indicator.Derivative.standard_deviation(q)
    sp = indicator.Derivative.sharpe_a(q, p_rf, 250)
    sp = indicator.Derivative.sharpe_a(q, p_bm, 250)
    ddev = indicator.Derivative.downside_deviation(q, p_bm, 250, 5)
    dd = indicator.Derivative.drawdown(q)
    dd = indicator.Derivative.drawdown(q, False)
    mdd = indicator.Derivative.max_drawdown(q)
    pperiods = indicator.Derivative.positive_periods(q)
    nperiods = indicator.Derivative.negative_periods(q)
    odds = indicator.Derivative.odds(q, p_bm)

    rs = indicator.Derivative.return_series(r)
    racc = indicator.Derivative.accumulative_return(r)

    ra2 = indicator.Derivative.annualized_return(r, 250, method="m")
    ra3 = indicator.Derivative.annualized_return(r, 250, "auto")

    racc_bm = indicator.Derivative.accumulative_return()
    del q["date"]
    g = q.groupby(["stock_id"])
    g.plot()


    # transform multi-index series to dataframe
    er_a = indicator.Derivative.excess_return_a(q, p_bm, 250, method="m")
    a = er_a.stack()
    b = pd.DataFrame(a, columns=["excess_return_a"])
    c = b.reset_index()
