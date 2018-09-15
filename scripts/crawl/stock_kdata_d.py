from crawler.tucrawler import basetype


def main(start=None, end=None):
    basetype.StockKdataCrawler(ktype="D", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    import datetime as dt
    main(dt.datetime(2018, 7, 22), dt.datetime(2018, 8, 5))
