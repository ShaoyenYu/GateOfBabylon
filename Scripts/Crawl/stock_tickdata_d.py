import datetime as dt
from util.crawl import crawler


def main():
    crawler.StockTickCrawler(date_start=dt.date.today() - dt.timedelta(2), date_end=dt.date.today()).crawl()


if __name__ == "__main__":
    main()
