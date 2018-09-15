from crawler.tucrawler import basetype


def main(start=None, end=None):
    basetype.StockKdataCrawler(ktype="5", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    main()
