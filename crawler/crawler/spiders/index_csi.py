import datetime as dt
import pandas as pd
import re
import scrapy
from crawler.crawler.items.items import IndexCsiInfoItem, IndexCsiQuoteItem
from utils import io
from utils.configcenter import config as cfg
from urllib.request import quote


class IndexSpider(scrapy.Spider):
    name = "index_csi_spider"

    interval = "5年"

    def start_requests(self):
        # http://www.csindex.com.cn/zh-CN/indices/index

        from urllib.request import quote
        url = f"http://www.csindex.com.cn/zh-CN/indices/index?" \
              f"page=1&page_size=50&by=desc&order={quote('指数代码')}" \
              f"&data_type=json&class_1=1&class_2=2&class_3=3&class_4=4&class_5=5&class_6=6"
        yield scrapy.Request(url, callback=self.parse_1)

    def parse_1(self, resp):
        total_page = int(re.search("\"total\":(\d*?),", resp.text).group(1))
        offset = 50
        _ = divmod(total_page, offset)
        total = _[0] + bool(_[1])

        for page_no in range(1, total):
            url = f"http://www.csindex.com.cn/zh-CN/indices/index" \
                  f"?page={page_no}&page_size={offset}&by=desc&order={quote('指数代码')}" \
                  f"&data_type=json&class_1=1&class_2=2&class_3=3&class_4=4&class_5=5&class_6=6"
            yield scrapy.Request(url, callback=self.parse_2)

    def parse_2(self, resp):
        patt = "(\"index_code\":\".*?\")," \
               "(\"indx_sname\":\".*?\")," \
               "(\"index_ename\":\".*?\")" \
               ".*?" \
               "(\"base_point\":\"\d*\.\d*?\")," \
               "(\"base_date\":\".*?\")"
        indexes = [eval("{" + ",".join(x) + "}") for x in re.findall(patt, resp.text)]

        for i_dict in indexes:
            api_index_quote = f"http://www.csindex.com.cn/zh-CN/indices/index-detail/{i_dict['index_code']}?" \
                              f"earnings_performance={quote(self.interval)}&data_type=json"
            yield scrapy.Request(api_index_quote, callback=self.parse_3, meta={"data": i_dict})

    def parse_3(self, resp):
        index_value = eval(resp.text.replace("null", "None"))  # list
        df_tmp = pd.DataFrame(index_value)
        for col_name, val in resp.meta["data"].items():
            df_tmp[col_name] = val
        df_tmp["tradedate"] = df_tmp["tradedate"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
        df_tmp["base_date"] = df_tmp["base_date"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())

        mapping = {
            "index_code": "index_id", "indx_sname": "name", "index_ename": "name_en",
            "base_point": "base_value", "tradedate": "date", "tclose": "close"
        }
        df_tmp.rename(columns=mapping, inplace=True)
        df_tmp["index_id"] = df_tmp["index_id"].apply(lambda x: x + ".CSI")

        res_1 = df_tmp[["index_id", "name", "name_en", "base_date", "base_value"]][:1]
        res_2 = df_tmp[["index_id", "date", "close"]]

        io.to_sql("babylon.index_info", cfg.default_engine, res_1)
        io.to_sql("babylon.index_quote_d", cfg.default_engine, res_2)
        # for d in res_1:
        #     yield IndexCsiInfoItem(d)
        #
        # for d in res_2:
        #     yield IndexCsiQuoteItem(d)


class IndexCsiConstituteSpider(scrapy.Spider):
    name = "index_csi_constitute_spider"

    def start_requests(self):
        # http://www.csindex.com.cn/zh-CN/downloads/indices?lb=%E5%85%A8%E9%83%A8&xl=%E5%85%A8%E9%83%A8
        url = f"http://www.csindex.com.cn/zh-CN/downloads/indices?" \
              f"page=1&page_size=30&lb={quote('全部')}&xl={quote('全部')}" \
              f"&data_type=json"

        yield scrapy.Request(url, callback=self.parse_1)

    def parse_1(self, resp):
        total_page = int(re.search("\"total_page\":(\d*?),", resp.text).group(1))
        for page_no in range(1, total_page + 1):
            url = f"http://www.csindex.com.cn/zh-CN/downloads/indices?" \
                  f"page={page_no}&page_size=30&lb={quote('全部')}&xl={quote('全部')}" \
                  f"&data_type=json"
            yield scrapy.Request(url, callback=self.parse_2)

    def parse_2(self, resp):
        pass


def main():
    from scrapy import cmdline
    cmdline.execute("scrapy crawl index_csi_spider".split())
    cmdline.execute("scrapy crawl index_csi_constitute_spider".split())


if __name__ == "__main__":
    main()
