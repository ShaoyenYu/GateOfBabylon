from utils.crawler import tucrawler


def main(start=None, end=None):
    tucrawler.StockTickCrawler(date_start=start, date_end=end).crawl()


if __name__ == "__main__":
    main()
