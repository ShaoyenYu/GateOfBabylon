import datetime as dt
from util import io, config as cfg
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


def fetch_indexid(engine):
    ids = pd.read_sql("SELECT DISTINCT index_id FROM index_info", engine)["index_id"].tolist()
    return ids


def fetch_kdata(index_id, ktype):
    """

    Args:
        index_id:
        start:
        end:
        autype:

    Returns:

    """
    try:
        autypes = (None, "qfq", "hfq")
        dfs = {
            autype: ts.get_k_data(index_id, ktype=ktype, autype=autype, index=True) for autype in autypes
        }
        for k in dfs:
            dfs[k].index = dfs[k]["date"]
            if k in {"qfq", "hfq"}:
                del dfs[k]["date"]
                del dfs[k]["volume"]
            else:
                dfs[k]["index_id"] = index_id
                # dfs[k]["index_id"] = dfs[k]["code"]
            del dfs[k]["code"]
        result = dfs[None].join(dfs["qfq"], how="outer", rsuffix="_fadj").join(dfs["hfq"], how="outer", rsuffix="_badj")

        return result

    except Exception as e:
        return (index_id, str(e))


def main():
    print(f"TIME: {dt.datetime.now()}; SCRIPT_NAME: {__name__}; START;")
    engine = cfg.default_engine
    index_ids = fetch_indexid(engine)
    errs = []
    tpool = ThreadPool(50)
    try:
        results = tpool.map(partial(fetch_kdata, ktype="5"), index_ids)
        tpool.close()
        tpool.join()

        with engine.connect() as conn:
            for result in results:
                try:
                    if type(result) is pd.DataFrame:
                        io.to_sql("index_kdata_5", conn, result)
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
