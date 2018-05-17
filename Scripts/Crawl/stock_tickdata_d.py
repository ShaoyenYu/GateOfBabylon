import datetime as dt
from util.crawl import crawler


def main():
    crawler.StockTickCrawler().crawl()


def test():
    crawler.StockTickCrawler(date_start=dt.date.today(), date_end=dt.date.today()).crawl()


if __name__ == "__main__":
    main()
