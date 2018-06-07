import requests

api_kdata_d = f"http://yunhq.sse.com.cn:32041/v1/sh1/dayk/600001?" \
          f"callback=jQuery111205248554352587773_1522762505985" \
          f"&select=date%2Copen%2Chigh%2Clow%2Cclose%2Cvolume" \
          f"&begin=-2&end=-1" \
          f"&_=1522762506015"

api_info = f"http://yunhq.sse.com.cn:32041/v1/sh1/snap/600004?" \
           f"callback=jQuery111207386427267146132_1522755313249" \
           f"&select=name%2Clast%2Cchg_rate%2Cchange%2Camount%2Cvolume%2Copen%2Cprev_close%2Cask%2Cbid%2Chigh%2Clow%2Ctradephase" \
           f"&_=1522755313250"

api_kdata_minute = f"http://yunhq.sse.com.cn:32041/v1/sh1/line/600000?" \
                   f"callback=jQuery1112012928519294989238_1522763953249" \
                   f"&begin=-5&end=0&select=time%2Cprice%2Cvolume&" \
                   f"_=1522763953311"

headers = {"Referer": "http://www.sse.com.cn/market/price/trends/"}

import datetime as dt
import time
from dateutil.relativedelta import relativedelta

r = requests.get(api_kdata_d, headers=headers)
r.content

r = requests.get(api_info, headers=headers)
r.content


r2 = requests.get(api_kdata_minute, headers=headers)
r2.content

def get_stock():
    pass