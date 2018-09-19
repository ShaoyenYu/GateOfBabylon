import inspect
from functools import reduce
from scrapy.crawler import CrawlerProcess
from crawler.scrapy_crawler.spiders import (
    index_csi, stock_finance)


def fetch_class_in_module(module):
    return [x[1] for x in filter(lambda x: inspect.isclass(x[1]), inspect.getmembers(module))]


def main():
    tasks = [index_csi, stock_finance]
    spiders = reduce(lambda x, y: [*x, *y], (fetch_class_in_module(mod) for mod in tasks))
    process = CrawlerProcess()
    [process.crawl(spider) for spider in spiders]
    process.start()
    # cur, max_pool_size = 0, 1
    # for idx, spicer in enumerate(spiders, 1):
    #     if idx % max_pool_size != 0:
    #         print("-add", spicer)
    #         process.crawl(spicer)
    #     else:
    #         print("!add", spicer)
    #         process.crawl(spicer)
    #         print("start crawl")
    #         process.start(stop_after_crawl=False)
    #         print("join")
    #         process.stop()
    #         print("continue")
    # process.start(True)


if __name__ == '__main__':
    main()
