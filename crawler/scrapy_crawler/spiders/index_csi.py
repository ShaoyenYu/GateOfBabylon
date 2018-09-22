import datetime as dt
import pandas as pd
import re
import scrapy
import xlrd
from utils.io import sql
from utils.configcenter import config as cfg
from urllib.request import quote


class IndexSpider(scrapy.Spider):
    name = "index_csi_spider"

    interval = "5年"

    def start_requests(self):
        # http://www.csindex.com.cn/zh-CN/indices/index

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
        index_value = eval(resp.text.replace("null", "None"))  # -> list
        df_tmp = pd.DataFrame(index_value)
        for col_name, val in resp.meta["data"].items():
            df_tmp[col_name] = val
        try:
            df_tmp["tradedate"] = df_tmp["tradedate"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
            df_tmp["base_date"] = df_tmp["base_date"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
        except Exception as e:
            from scrapy.exceptions import IgnoreRequest
            raise IgnoreRequest(e)

        mapping = {
            "index_code": "index_id", "indx_sname": "name", "index_ename": "name_en",
            "base_point": "base_value", "tradedate": "date", "tclose": "close"
        }
        df_tmp.rename(columns=mapping, inplace=True)
        df_tmp["index_id"] = df_tmp["index_id"].apply(lambda x: f"{str(x).zfill(6)}.CSI")

        res_1 = df_tmp[["index_id", "name", "name_en", "base_date", "base_value"]][:1]
        res_2 = df_tmp[["index_id", "date", "close"]]

        sql.to_sql("babylon.index_info", cfg.default_engine, res_1)
        sql.to_sql("babylon.index_quote_d", cfg.default_engine, res_2)


class IndexCsiConstituteSpider(scrapy.Spider):
    name = "index_csi_constitute_spider"

    def start_requests(self):
        # http://www.csindex.com.cn/zh-CN/downloads/indices?lb=%E5%85%A8%E9%83%A8&xl=%E5%85%A8%E9%83%A8
        url = f"http://www.csindex.com.cn/zh-CN/downloads/indices?" \
              f"page=1&page_size=250&lb={quote('全部')}&xl={quote('全部')}" \
              f"&data_type=json"

        yield scrapy.Request(url, callback=self.parse_1)

    def parse_1(self, resp):
        total_page = int(re.search("\"total_page\":(\d*?),", resp.text).group(1))
        for page_no in range(1, total_page + 1):
            url = f"http://www.csindex.com.cn/zh-CN/downloads/indices?" \
                  f"page={page_no}&page_size=250&lb={quote('全部')}&xl={quote('全部')}" \
                  f"&data_type=json"
            yield scrapy.Request(url, callback=self.parse_2)

    def parse_2(self, resp):
        l_urls = [x[-5:] for x in eval(re.search("\"list\":(\[.*\])", resp.text).group(1).replace("null", "None"))]

        for urls in l_urls:
            if urls[0]:  # perf
                yield scrapy.Request(urls[0].replace("\\", ""), callback=self.parse_baseinfo)
            if urls[3]:  # cons
                yield scrapy.Request(urls[3].replace("\\", ""), callback=self.parse_constituents)
            if urls[4]:  # weights
                yield scrapy.Request(urls[4].replace("\\", ""), callback=self.parse_weight)

    @staticmethod
    def parse_baseinfo(resp):
        cols_quotes = {
            "日期Date": "date", "指数代码Index Code": "index_id", "开盘Open": "open", "最高High": "high", "最低Low": "low",
            "收盘Close": "close",
            "成交量（股）Volume(share)": "volume", "成交量（万元）Volume(10 thousand CNY)": "volume",
            "成交量（手）Volume(100 shares)": "volume", "成交金额（元）Turnover": "turnover",
        }
        cols_derivatives = {
            "日期Date": "date", "指数代码Index Code": "index_id", "市盈率1（总股本）P/E1": "pe1",
            "市盈率2（计算用股本）P/E2": "pe2", "股息率1（总股本）D/P1": "dp1", "股息率2（计算用股本）D/P2": "dp2"
        }
        df = pd.read_excel(xlrd.open_workbook(file_contents=resp.body), engine="xlrd").rename(columns={**cols_quotes, **cols_derivatives})
        df["index_id"] = df["index_id"].apply(lambda x: f"{str(x).zfill(6)}.CSI")

        sql.to_sql("babylon.index_quote_d", cfg.default_engine, df[list(set(cols_quotes.values()))])
        sql.to_sql("babylon.index_fina_derivative", cfg.default_engine, df[list(cols_derivatives.values())])

    @staticmethod
    def parse_constituents(resp):
        cols = {
            "日期Date": "date", "指数代码Index Code": "index_id", "成分券代码Constituent Code": "stock_id"
        }
        df = pd.read_excel(xlrd.open_workbook(file_contents=resp.body), engine="xlrd").rename(columns=cols)
        df["index_id"] = df["index_id"].apply(lambda x: f"{str(x).zfill(6)}.CSI")
        df["stock_id"] = df["stock_id"].apply(lambda x: f"{str(x).zfill(6)}")

        sql.to_sql("babylon.index_constituents", cfg.default_engine, df[list(cols.values())])

    @staticmethod
    def parse_weight(resp):
        cols = {
            "日期Date": "date", "指数代码Index Code": "index_id", "成分券代码Constituent Code": "stock_id", "权重(%)Weight(%)": "weight"
        }
        df = pd.read_excel(xlrd.open_workbook(file_contents=resp.body), engine="xlrd").rename(columns=cols)
        df["index_id"] = df["index_id"].apply(lambda x: f"{str(x).zfill(6)}.CSI")
        df["stock_id"] = df["stock_id"].apply(lambda x: f"{str(x).zfill(6)}")
        df["weight"] /= 100

        sql.to_sql("babylon.index_constituents", cfg.default_engine, df[list(cols.values())])


def main():
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess()
    for spider in (IndexSpider, IndexCsiConstituteSpider):
        process.crawl(spider)
    process.start()


if __name__ == "__main__":
    main()
