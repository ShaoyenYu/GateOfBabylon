import datetime as dt
import requests
import numpy as np
import pandas as pd
from lxml import html
from utils import config as cfg, io


def get_ratio(date):
    end = date
    start = end - dt.timedelta(7)

    api = f"http://yield.chinabond.com.cn/cbweb-pbc-web/pbc" \
          f"/historyQuery?startDate={start.strftime('%Y-%m-%d')}&endDate={date.strftime('%Y-%m-%d')}" \
          f"&gjqx=0&qxId=hzsylqx&locale=cn_ZH"
    r = requests.get(api)
    t = html.fromstring(r.content)
    tds = t.xpath("//div[@id='gjqxData']/table//tr/td/text()")
    cols = ["index_id", "date", "m3", "m6", "y1", "y3", "y5", "y7", "y10", "y30"]
    data = np.array(tds[18:])
    assert len(data) % len(cols) == 0
    data = data.reshape(int((len(data) / len(cols))), len(cols))
    res = pd.DataFrame(data, columns=cols)[cols[1:]]
    return res


def main():
    dr = pd.date_range(start=dt.date.today() - dt.timedelta(7), end=dt.date.today(), freq="D")

    for d in dr:
        io.to_sql("ratio_treasury_bond", cfg.default_engine, get_ratio(d))


if __name__ == "__main__":
    main()
