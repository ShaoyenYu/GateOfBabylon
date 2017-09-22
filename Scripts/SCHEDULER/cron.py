from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from Scripts.CRAWL import stock_info_hist
from Scripts.ETL import stock_info

jobstores = {
    # "mongo": MongoDBJobStore(),  # equal to {"type": "mongodb"},
    # "default": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite")
    "default": MemoryJobStore()
}
job_defaults = {
    "coalesce": False,
    "max_instances": 3
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

    scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

    scheduler.start()
    # scheduler.shutdown(wait=False)
    # scheduler.remove_all_jobs()
    # scheduler.get_jobs()


if __name__ == "__main__":
    main()
