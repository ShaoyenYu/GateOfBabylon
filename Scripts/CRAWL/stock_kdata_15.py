from utils import io, config as cfg
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


def fetch_stockid(engine):
    ids = pd.read_sql("SELECT DISTINCT stock_id FROM stock_info", engine)["stock_id"].tolist()
    return ids


def fetch_k_price(stock_id, ktype):
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
    engine = cfg.default_engine
    stock_ids = fetch_stockid(engine)
    errs = []
    tpool = ThreadPool(50)
    try:
        results = tpool.map(partial(fetch_k_price, ktype="15"), stock_ids)
        tpool.close()
        tpool.join()

        with engine.connect() as conn:
            for result in results:
                if type(result) is pd.DataFrame:
                    io.to_sql("stock_kdata_15", conn, result)
                else:
                    errs.append(result)
            conn.close()
        return errs

    except Exception as e:
        return results, errs, e


if __name__ == "__main__":
    res = main()
