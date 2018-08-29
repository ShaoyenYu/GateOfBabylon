from utils.crawler import tucrawler


def main(start=None, end=None):
    tucrawler.StockKdataCrawler(ktype="30", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    main()
