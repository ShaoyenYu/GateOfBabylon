import datetime as dt
import re
import requests
import pandas as pd
import numpy as np
from functools import reduce
from lxml import html
from multiprocessing.dummy import Pool as ThreadPool


def auto_retry(func):
    def wrapper(*args, **kwargs):
        success = False
        has_failed = False

        while not success:
            resp = func(*args, **kwargs)
            if resp.status_code != 200:
                has_failed = True
                print(f"failed<{resp.status_code}>: {resp.url}")
                continue
            if has_failed:
                print("success after retry")
            return resp

    return wrapper


def get_tick_data(code, date):
    def is_valid(vals_tr, vals_th):
        rows, invalid = divmod(len(vals_tr), 4)
        assert invalid == 0  # four value each row
        assert rows == len(vals_th)

    def get_total_page():
        patt = "\[\d*?.*?]\](?=;;)"
        resp = auto_retry(requests.get)(api_main, headers=headers)

        return eval(re.search(patt, resp.text).group())[-1][0]

    def get_data(page_no):
        api_tick = f"{api_main}&page={page_no}"
        resp = auto_retry(requests.get)(api_tick, headers=headers)
        try:
            page = html.fromstring(resp.text)
            vals_tr = page.xpath("//table/tbody/tr/td/text()")
            vals_th = page.xpath("//table/tbody/tr/th/text()")
            is_valid(vals_tr, vals_th)

            res = [[vals_th[i], *vals_tr[i * 4: (i + 1) * 4]] for i in range(len(vals_th))]
            return res
        except Exception as e:
            print(resp.status_code, resp.reason)
            print(page_no, e)

    y, m, d = [str(x).zfill(2) for x in date.timetuple()[:3]]
    api_main = f"http://market.finance.sina.com.cn/transHis.php?symbol={code}&date={y}-{m}-{d}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Cache-Control": "max-age=0",
        "Referer": f"{api_main}&page={0}",
        "Connection": "keep-alive",
        # "Cookie": "Cookie: UOR=www.google.com,finance.sina.com.cn,; U_TRS1=000000a1.f3f5582a.5bba264b.fbc7b39c;"
        #           " SR_SEL=1_511; SINAGLOBAL=61.171.95.161_1538926156.673167;"
        #           " SUB=_2AkMs5rvkf8NxqwJRmPEVxG_gaYlxzAHEieKauko_JRMyHRl-yD83qm8-tRB6B2aVC5HGz75O1nOJ1qupVUQmaSEV6p34; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWiwiMLZfZCqLfR.J4obNji; FINA_V5_HQ=0; FINA_DMHQ=1; lxlrttp=1538731187; U_TRS2=000000a1.ec3712e7.5bbdfb66.406397c8; Apache=10.71.2.96_1539177602.641546; FINANCE2=5b12a7c18dd931a12e4b6bace03ef18d; vjuids=201c3f19e.1665e22295d.0.99774df313e61; vjlast=1539177589.1539177589.30; sinaH5EtagStatus=y; ULV=1539177611499:6:6:6:10.71.2.96_1539177602.641546:1539177559037; FIN_ALL_VISITED=sz000001%2Csh000001%2Csz000063%2Csz002939; FINA_V_S_2=sz000001,sh000001,sz000008,sz000590,sz000063,sz000591,sz002939",
        "Host": "market.finance.sina.com.cn",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    }

    total_pages = get_total_page()
    print(f"total_pages: {total_pages}")
    with ThreadPool(200) as pool:
        results = pool.map(get_data, range(1, total_pages + 1))
    results = reduce(lambda l1, l2: [*l1, *l2], results)
    df_res = pd.DataFrame(results, columns=["time", "close", "change", "volume", "amount"])

    df_res["time"] = df_res["time"].apply(lambda x: f"{y}-{m}-{d} {x}")
    df_res["change"] = df_res["change"].replace("--", np.nan).astype(np.float)
    df_res["volume"] = df_res["volume"].replace("--", np.nan).astype(np.float)
    df_res["amount"] = df_res["amount"].apply(lambda x: np.float(x.replace(",", "")))

    return df_res


def main():
    code = "sz000001"
    date = dt.datetime(2017, 5, 9)
    get_tick_data(code, date)


if __name__ == '__main__':
    main()
