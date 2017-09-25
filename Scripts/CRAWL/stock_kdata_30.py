import datetime as dt
from utils import io, config as cfg
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


def fetch_stockid(engine):
    ids = pd.read_sql("SELECT DISTINCT stock_id FROM stock_info", engine)["stock_id"].tolist()
    return ids


def fetch_kdata(stock_id, ktype):
    """

    Args:
        stock_id:
        start:
        end:
        autype:

    Returns:

    """
    try:
        autypes = (None, "qfq", "hfq")
        dfs = {
            autype: ts.get_k_data(stock_id, ktype=ktype, autype=autype) for autype in autypes
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
    print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {__name__}; START;")
    engine = cfg.default_engine
    stock_ids = fetch_stockid(engine)
    errs = []
    tpool = ThreadPool(50)
    try:
        results = tpool.map(partial(fetch_kdata, ktype="30"), stock_ids)
        tpool.close()
        tpool.join()

        with engine.connect() as conn:
            for result in results:
                try:
                    if type(result) is pd.DataFrame:
                        io.to_sql("stock_kdata_30", conn, result)
                    else:
                        errs.append(result)
                except Exception as e:
                    errs.append(result)
                    continue
            conn.close()
        print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {__name__}; RECORDS NUM: {len(results)}; DONE;")
        return errs

    except Exception as e:
        return results, errs, e


if __name__ == "__main__":
    res = main()
