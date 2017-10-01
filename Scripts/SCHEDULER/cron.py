from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from Scripts.ETL import stock_info
from Scripts.CRAWL import (
    stock_info_hist, stock_kdata_5, stock_kdata_15, stock_kdata_30, stock_kdata_60, stock_kdata_d
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
    "processpool": ProcessPoolExecutor(max_workers=5)
}


def main():
    #
    scheduler = BackgroundScheduler()

    # .. do something else here, maybe add jobs etc.
    scheduler.add_job(stock_info_hist.main, "cron", minute=30)
    scheduler.add_job(stock_info.main, "cron", minute=35)
    scheduler.add_job(stock_kdata_5.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="15,45")
    scheduler.add_job(stock_kdata_15.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="40")
    scheduler.add_job(stock_kdata_30.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="50")
    scheduler.add_job(stock_kdata_60.main, "cron", day_of_week="mon-sun", hour="10-12,14-16,18,21,23", minute="5")
    scheduler.add_job(stock_kdata_d.main, "cron", day_of_week="mon-sun", hour="5,11,17,23", minute="5")
    # scheduler.add_job(stock_kdata_5.main, "cron", minute=5)
    # scheduler.add_job(stock_kdata_15.main, "cron", minute=10)
    # scheduler.add_job(stock_kdata_30.main, "cron", minute=15)
    # scheduler.add_job(stock_kdata_60.main, "cron", minute=20)

    scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

    scheduler.start()
    # scheduler.shutdown(wait=False)
    # scheduler.remove_all_jobs()
    # scheduler.get_jobs()


if __name__ == "__main__":
    main()
