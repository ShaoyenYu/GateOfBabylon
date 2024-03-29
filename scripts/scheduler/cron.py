from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from scripts.etl import (
    stock_info, stock_turnover_d
)
from scripts.crawl import (
    stock_info_hist, stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d,
    index_kdata_5, index_kdata_15, index_kdata_30, index_kdata_60, index_kdata_d,
    stock_tickdata_ts, ratio_treasury_bond
)

jobstores = {
    # "mongo": MongoDBJobStore(),  # equal to {"type": "mongodb"},
    # "default": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite")
    "default": MemoryJobStore()
}
job_defaults = {
    "coalesce": False,
    "max_instances": 20
}
executors = {
    "default": ThreadPoolExecutor(max_workers=20),  # equal to "default": {"type": "threadpool", "max_workers": 20},
    "processpool": ProcessPoolExecutor(max_workers=10)
}


def main():
    #
    scheduler = BackgroundScheduler()

    # .. do something else here, maybe add jobs etc.
    # Crawl stock info
    scheduler.add_job(stock_info_hist.main, "cron", minute=30)
    scheduler.add_job(stock_info.main, "cron", minute=35)
    scheduler.add_job(stock_turnover_d.main, "cron", day_of_week="mon-sun", hour="20,21", minute="5")

    # Crawl stock price
    scheduler.add_job(stock_kdata_5.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="15,45")
    scheduler.add_job(stock_kdata_15.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="40")
    scheduler.add_job(stock_kdata_30.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="50")
    scheduler.add_job(stock_kdata_60.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="5")
    scheduler.add_job(stock_kdata_d.main, "cron", day_of_week="mon-sun", hour="5,11,17,23", minute="5")

    # Crawl index
    scheduler.add_job(index_kdata_5.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="15,45")
    scheduler.add_job(index_kdata_15.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="40")
    scheduler.add_job(index_kdata_30.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="50")
    scheduler.add_job(index_kdata_60.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="5")
    scheduler.add_job(index_kdata_d.main, "cron", day_of_week="mon-sun", hour="5,11,17,23", minute="5")
    scheduler.add_job(ratio_treasury_bond.main, "cron", day_of_week="mon-sun", hour="18,19", minute="10")

    # Crawl stock tick data
    scheduler.add_job(stock_tickdata_ts.main, "cron", day_of_week="mon-sun", hour="19", minute="5")

    # Configure & Start
    scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)
    scheduler.start()
    # scheduler.shutdown(wait=False)
    # scheduler.remove_all_jobs()
    # scheduler.get_jobs()


if __name__ == "__main__":
    main()
