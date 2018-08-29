from utils.crawler import tucrawler


def main(start=None, end=None):
    tucrawler.IndexKdataCrawler(ktype="D", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    main()
