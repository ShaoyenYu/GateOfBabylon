from multiprocessing import Pool
from scripts.crawl import (
    index_kdata_5, index_kdata_15, index_kdata_30, index_kdata_60, index_kdata_d,
    stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d,
    stock_info_hist, ratio_treasury_bond, stock_tickdata_d
)
from scripts.etl import (
    stock_info, stock_turnover_d
)

tasks_tsdata = (
    index_kdata_5, index_kdata_15, index_kdata_30, index_kdata_60, index_kdata_d,
    stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d,
    stock_info_hist, ratio_treasury_bond, stock_tickdata_d, stock_turnover_d
)

task_secdata = (
    stock_info,
)


def main():
    import datetime as dt
    start, end = dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)
    p = Pool(8)
    for task in tasks_tsdata:
        p.apply_async(task.main, args=(start, end))
    p.close()
    p.join()

    for task in task_secdata:
        task.main()


if __name__ == "__main__":
    main()
