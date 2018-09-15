from crawler.tucrawler import basetype


def main(start=None, end=None):
    basetype.IndexKdataCrawler(ktype="30", date_start=start, date_end=end).crwal()


if __name__ == "__main__":
    main()
