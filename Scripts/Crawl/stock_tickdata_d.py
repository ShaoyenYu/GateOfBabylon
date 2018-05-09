import datetime as dt
from util.crawl import crawler


def main():
    crawler.StockTickCrawler().crawl()


def test():
    crawler.StockTickCrawler(date_start=dt.date(2018, 5, 2), date_end=dt.date(2018, 5, 8)).crawl()


if __name__ == "__main__":
    main()
