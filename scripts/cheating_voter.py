import datetime as dt
import re

import numpy as np
import requests as req


def cheat(n, vote_who, t="", rt=""):
    t = t or "14__cfo28TfC4v--EJlCNe5LkxZoG296MpToi0U_T7k1sF0Ogwv1r1ntiBG58lOuJ5SfLOBVkVltQJcOWbVxhmMsw"
    rt = rt or "14_yqsPik_ku7tjuk4UCdD8w_6mEUI6KePezbOwe9-NqgWRLat9wJw5fRbLk7Gg2M-YJueD-EXeuslbkhTYvOHX5g"

    CANDIDATES = (
        "0310e35dbadedf825e53",  # 0 上海盈沛投资管理有限公司
        "0310e3233541e3a0bfe9",  # 1 上海尚近投资管理合伙企业（有限合伙）
        "0310365b5298c76edbc2",  # 2 上海垒土资产管理有限公司
        "031017a8e1029641b89f",  # 3 北京东方引擎投资管理有限公司
        "0310b91bf2fceb7be846",  # 4 广州中卓投资有限公司
        "0310db36b7e1f3e229bb",  # 5 深圳市明曜投资管理有限公司
        "0310fdf1123910328689",  # 6 北京传家堡资产管理有限公司
        "0310c6d10a61381ff2d9",  # 7 上海景熙资产管理有限公司
        "0310729b03c9c741c67c",  # 8 上海希瓦资产管理有限公司
        "0310e8e35f4a5b393a21",  # 9 上海庐雍资产管理有限公司
    )

    def generate_fakeid():
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        chars = np.array(list(alphabet + alphabet.upper() + "".join((str(x) for x in range(10)))))
        return "".join(chars[np.random.randint(0, len(chars) - 1, size=(28,))])

    def get_token():
        headers_entry = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Connection": "keep-alive",
            "Host": "fyb.eastmoney.com",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        }
        # code=0310是"最佳新锐私募基金公司"
        api_entry = f"http://fyb.eastmoney.com/2018/h5/h5-vote.html?" \
                    f"code=0310&" \
                    f"o={fake_id}" \
                    f"&t={t}" \
                    f"&rt={rt}"
        resp = req.get(api_entry, headers=headers_entry)
        try:
            cookie = resp.headers["Set-Cookie"].split("; path")[0]
            verified_code = re.search("var wap_yzm = \"(.*?)\"", resp.text).group(1)
        except Exception as e:
            print("微信授权过期, 请重新登录")
        return verified_code, cookie

    def vote(vote_who="0310e35dbadedf825e53"):
        # 0310e35dbadedf825e53 盈沛
        api_vote = "http://fyb.eastmoney.com/2018/api/Vote?" \
                   f"callback=jQuery18307065082452813327_{int(dt.datetime.now().timestamp() * 1e3)}" \
                   "&type=0310" \
                   f"&uid={fake_id}" \
                   f"&codes={vote_who}" \
                   f"&yzm={varified_code}" \
                   f"&_={int(dt.datetime.now().timestamp() * 1e3 + 9527)}"
        headers_vote = {
            "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Connection": "keep-alive",
            "Cookie": f"ASP.NET_SessionId={cookie}",
            "Host": "fyb.eastmoney.com",
            "Referer": "http://fyb.eastmoney.com/2018/h5/h5-vote.html?"
                       "code=0310"
                       f"&o={fake_id}"
                       f"&t={t}"
                       f"&rt={rt}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        r = req.get(api_vote, headers=headers_vote)
        print(r.status_code)
        return r.status_code == 200

    success_num = 0
    while success_num <= n:
        fake_id = generate_fakeid()
        varified_code, cookie = get_token()
        is_success = vote(CANDIDATES[vote_who])
        success_num += int(is_success)


cheat(77, vote_who=7)
