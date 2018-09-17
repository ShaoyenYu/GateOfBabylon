import datetime as dt
import pandas as pd
import re
import scrapy
from utils import io
from utils.configcenter import config as cfg


class FinanceDataSpider(scrapy.Spider):
    name = "stock_finance_sina"
    base_url = "http://money.finance.sina.com.cn"

    def start_requests(self):
        stock_ids = [x[0] for x in cfg.default_engine.execute("SELECT DISTINCT stock_id FROM babylon.stock_info").fetchall()]
        api_types = ("BalanceSheet", "ProfitStatement", "CashFlow")
        api_url = self.base_url + "/corp/go.php/vFD_{api_type}/stockid/{stock_id}/ctrl/part/displaytype/1.phtml"
        for api_type in api_types[:1]:
            for stock_id in stock_ids:
                yield scrapy.Request(
                    api_url.format(stock_id=stock_id, api_type=api_type),
                    callback=self.parse_balance_sheet, meta={"data": {"stock_id": stock_id}})

    def parse_balance_sheet(self, resp):
        trs = resp.xpath("//table[@id='BalanceSheetNewTable0']/tbody/tr")
        patt = "type=(.*?)&"
        for tr in trs:
            api_url = tr.xpath("./td/a/@href").extract_first()
            txt = tr.xpath("./td/a/text()").extract_first()
            if api_url:
                col = re.search(patt, api_url).group(1).lower()
                yield scrapy.Request(f"{self.base_url}{api_url}", callback=self.parse_page, meta={"data": resp.meta["data"], "col": col, "save_to": "stock_fina_balancesheet", "comm": txt})

    def parse_page(self, resp):
        data = resp.xpath("//table[@id='Table1']/tbody/tr/td/text()").extract()
        assert len(data) % 3 == 0

        if len(data) > 0:
            res = pd.DataFrame([data[i: i+2] for i in range(0, len(data), 3)], columns=["date", resp.meta["col"]])
            res["stock_id"] = resp.meta["data"]["stock_id"]
            res[resp.meta["col"]] = res[resp.meta["col"]].apply(lambda x: re.sub("\s|,", "", x))
            res[resp.meta["col"]] = res[resp.meta["col"]].apply(lambda x: float(x) if x != "" else None)
            try:
                io.to_sql(resp.meta["save_to"], cfg.default_engine, res)
            except:
                sql = f"ALTER TABLE `{resp.meta['save_to']}` ADD COLUMN `{resp.meta['col']}` decimal(20,4) DEFAULT NULL COMMENT '{resp.meta['comm']}'"
                print(sql)
                cfg.default_engine.execute(sql)
                io.to_sql(resp.meta["save_to"], cfg.default_engine, res)


def main():
    from scrapy import cmdline
    cmdline.execute("scrapy crawl stock_finance_sina".split())


if __name__ == "__main__":
    main()

# # txt = tr.xpath("./td/a/text()").extract_first()
# print(f"`{re.match(patt, url).group(1).lower()}` decimal(20, 4) DEFAULT NULL COMMENT '{txt}',")
