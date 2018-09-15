from crawler.tucrawler import basetype


def main(start=None, end=None):
    basetype.IndexKdataCrawler(ktype="D", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    main()
