import datetime as dt
from util.crawl import crawler


def main():
    crawler.StockTickCrawler().crawl()


def test():
    crawler.StockTickCrawler(date_start=dt.date.today() - dt.timedelta(16), date_end=dt.date(2018, 4, 15)).crawl()


if __name__ == "__main__":
    main()
