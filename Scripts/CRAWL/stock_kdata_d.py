from utils import io, config as cfg
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


def fetch_stockid(engine):
    ids = pd.read_sql("SELECT DISTINCT stock_id FROM stock_info", engine)["stock_id"].tolist()
    return ids


def fetch_kdata(stock_id, ktype, start=None, end=None):
    """

    Args:
        stock_id:
        start:
        end:
        autype:

    Returns:

    """
    print(f"fetching {stock_id}")
    try:
        start = start.strftime("%Y-%m-%d") if start is not None else "1985-01-01"
        end = end.strftime("%Y-%m-%d") if end is not None else None

        autypes = (None, "qfq", "hfq")
        dfs = {
            autype: ts.get_k_data(stock_id, ktype=ktype, autype=autype, start=start, end=end) for autype in autypes
        }
        for k in dfs:
            dfs[k].index = dfs[k]["date"]
            if k in {"qfq", "hfq"}:
                del dfs[k]["date"]
                del dfs[k]["volume"]
            else:
                dfs[k]["stock_id"] = dfs[k]["code"]
            del dfs[k]["code"]
        result = dfs[None].join(dfs["qfq"], how="outer", rsuffix="_fadj").join(dfs["hfq"], how="outer", rsuffix="_badj")

        return result

    except Exception as e:
        return (stock_id, str(e))


def main():
    engine = cfg.default_engine
    stock_ids = fetch_stockid(engine)
    errs = []
    tpool = ThreadPool(50)
    try:
        results = tpool.map(partial(fetch_kdata, ktype="D"), stock_ids)
        tpool.close()
        tpool.join()

        with engine.connect() as conn:
            for result in results:
                if type(result) is pd.DataFrame:
                    io.to_sql("stock_kdata_d", conn, result)
                else:
                    errs.append(result)
            conn.close()
        return errs

    except Exception as e:
        return results, errs, e


if __name__ == "__main__":
    res = main()

# # 加了ktype参数, 并且ktype !=0 时, start, end参数失效;
# q5 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21")
# q5 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype=None)
# q51 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="qfq")
# q52 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="hfq")
#
# for df in (q5, q51, q52):
#     df.index = df["date"]
#
# q5_qfq = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="qfq")
# q5_hfq = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="hfq")
# q15 = ts.get_k_data('002337', ktype="15", start="2017-09-15", end="2017-09-21")
# q30 = ts.get_k_data('002337', ktype="30", start="2017-09-15", end="2017-09-21")
# q60 = ts.get_k_data('002337', ktype="60", start="2017-09-15", end="2017-09-21")
# import datetime as dt
# q = ts.get_h_data("000001", start=dt.date(1990, 1, 1).strftime("%Y-%m-%d"))
# q2 = fetch_kdata("000810", ktype="D", start=dt.date(2017, 7, 1), end=dt.date(2017,7,7))
#
# q = ts.get_h_data("000001")
# q = ts.get_tick_data()
# qd = ts.get_k_data('002337', ktype="D", start="19900101")
# qw = ts.get_k_data('002337', ktype="W")
# qm = ts.get_k_data('002337', ktype="M")
# ts.get_hist_data()


