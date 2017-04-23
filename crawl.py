import datetime as dt
import pandas as pd, numpy as np
from utils import config as cfg, io
from WindPy import w as wind

wind.start()

engine = cfg.default_engine

q = pd.read_sql("SELECT * FROM tb1", engine)

wind.wset()

w.wset("sectorconstituent", "date=2017-04-20;sectorid=a001010900000000")

w.wset("sectorconstituent","date=2017-04-24;sectorid=a001010900000000")


def fetch_swsinfo():
    date_str = dt.date.today().strftime("%Y-%m-%d")
    result = pd.DataFrame()
    for wind_sid, mp_sws in tm.items():
        params_options = "date={date};sectorid={sectorid}".format(date=date_str, sectorid=wind_sid)
        data = wind.wset("sectorconstituent", params_options).Data
        tmp = pd.DataFrame(data).T[[1, 2]]
        tmp[3] = mp_sws[0]
        tmp[4] = mp_sws[1]
        result = result.append(tmp)
    result.columns = ["subject_id", "subject_name", "type_code_sws", "type_name_sws"]
    return result


def fetch_priceinfo(date_s, date_e):
    date_s_str = date_s.strftime("%Y-%m-%d")
    date_e_str = date_e.strftime("%Y-%m-%d")
    result = pd.DataFrame()
    ipo_lists = wind.wset("listedsecuritygeneralview", "sectorid=a001010100000000").Data[0]
    cols_query = ["sec_name", "lastradeday_s", "close", "mkt_cap_ard", "mkt_cap_float", "pe_ttm", "val_pe_deducted_ttm", "pe_lyr", "trade_status"]
    cols_db = ["subject_name", "date", "closing_price", "market_price", "circulated_price", "pe_ttm", "pe_deducted_tmm", "pe_lyr", "status"]
    for security_id in ipo_lists:
        print(security_id)
        cols_query_str = ",".join(cols_query)
        q = wind.wsd(security_id, cols_query_str, date_s_str, date_e_str, "unit=1;currencyType=")
        if q.ErrorCode == 0:
            q = q.Data
        else:
            print("Error: ", security_id)
            continue
        d = pd.DataFrame(q).T
        d.columns = cols_db
        d["subject_id"] = security_id
        result = result.append(d)

    result = result.dropna(subset=["date"])
    result["date"] = result["date"].apply(lambda x: x.date())
    result.index = range(len(result))
    result.market_price = result.market_price.astype(np.float) / 100000000
    result.circulated_price = result.circulated_price.astype(np.float) / 100000000
    return result

# w.wsd("000001.SZ", "pe_ttm,val_pe_deducted_ttm,pe_lyr,ev,mkt_cap_ard,ev3", "2016-11-01", "2016-12-31", "unit=1;currencyType=")
date_s, date_e = dt.date(2016, 11, 1), dt.date(2016, 12, 31)
d = fetch_priceinfo(date_s, date_e)
# 150251.SZ

# Days=Alldays
# Days=Alldays
# pd.DataFrame(wind.wset("listedsecuritygeneralview", "date=2016-01-05;sectorid=a001010100000000").Data).T
# pd.DataFrame(wind.wset("listedsecuritygeneralview", "date=2016-01-05;sectorid=a001010100000000").Data).T

def main():
    tasks = {
        "security_type_mapping": fetch_swsinfo,
        # "security_price": fetch_priceinfo
    }
    for tb, func in tasks.items():
        data = func()
        io.to_sql(tb, engine_rd, data, "update")


if __name__ == "__main__":
    main()
