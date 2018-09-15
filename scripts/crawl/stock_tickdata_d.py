from crawler.tucrawler import basetype


def main(start=None, end=None):
    basetype.StockTickCrawler(date_start=start, date_end=end).crawl()


if __name__ == "__main__":
    main()
