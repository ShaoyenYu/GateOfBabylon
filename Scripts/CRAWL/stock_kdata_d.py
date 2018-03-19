import datetime as dt
import pandas as pd
from util import config as cfg
from util.crawler import crawler
from util.decofactory import common


def fetch_stockid(engine):
    ids = pd.read_sql("SELECT DISTINCT stock_id FROM stock_info", engine)["stock_id"].tolist()
    return sorted(ids)


@common.log
def crawl():
    engine = cfg.default_engine
    stock_ids = fetch_stockid(engine)
    date_end = dt.date.today()

    sdc = crawler.StockDataCrawler(stock_ids, date_end=date_end, ktype="D")
    sdc.crwal_kdata()


def main():
    crawl()


if __name__ == "__main__":
    main()
