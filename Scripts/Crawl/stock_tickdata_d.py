import datetime as dt
from utils.crawler import tucrawler


def main():
    tucrawler.StockTickCrawler().crawl()


def test():
    tucrawler.StockTickCrawler(date_start=dt.date.today(), date_end=dt.date.today()).crawl()


if __name__ == "__main__":
    main()
