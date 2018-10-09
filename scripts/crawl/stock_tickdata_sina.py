import datetime as dt
import re
import requests
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
            print(res)
            return res
        except Exception as e:
            print(resp.status_code, resp.reason)
            print(page_no, e)

    def collect(l):
        nonlocal res
        res.extend(l)

    y, m, d = [str(x).zfill(2) for x in date.timetuple()[:3]]
    api_main = f"http://market.finance.sina.com.cn/transHis.php?symbol={code}&date={y}-{m}-{d}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Host": "market.finance.sina.com.cn",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    }

    res = []
    pool = ThreadPool(25)
    total_pages = get_total_page()
    print(f"total_pages: {total_pages}")
    for page in range(1, total_pages + 1):
        pool.apply_async(get_data(page), callback=collect)
    pool.close()
    pool.join()
    return res


def main():
    code = "sz000001"
    date = dt.datetime(2017, 5, 9)
    get_tick_data(code, date)


if __name__ == '__main__':
    main()
