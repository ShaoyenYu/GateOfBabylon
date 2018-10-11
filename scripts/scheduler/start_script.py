from multiprocessing import Pool
from scripts.crawl import (
    index_kdata_5, index_kdata_15, index_kdata_30, index_kdata_60, index_kdata_d,
    stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d,
    stock_info_hist, ratio_treasury_bond, stock_tickdata_ts
)
from scripts.etl import (
    stock_info, stock_turnover_d
)

TASK_GROUPS = {
    1: (index_kdata_5, index_kdata_15, index_kdata_30, index_kdata_60, index_kdata_d,
        stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d,
        stock_info_hist, ratio_treasury_bond, stock_tickdata_ts),
    2: (stock_turnover_d,),
    3: (stock_info,)
}

TASKS_NOT_TS = {stock_info}


def main():
    import datetime as dt
    start, end = dt.datetime(2018, 6, 6), dt.datetime(2018, 6, 15)
    for _, tasks in TASK_GROUPS.items():
        p = Pool(8)
        for task in tasks:
            if task not in TASKS_NOT_TS:
                p.apply_async(task.main, args=(start, end))
            else:
                p.apply_async(task.main)
        p.close()
        p.join()


if __name__ == "__main__":
    main()
