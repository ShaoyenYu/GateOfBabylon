from utils import io, config as cfg
import datetime as dt
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool

# 加了ktype参数, 并且ktype !=0 时, start, end参数失效;
q = ts.get_h_data("002337")
q1 = ts.get_k_data('002337', ktype="1", start="2017-09-16", end="2017-09-21")
q5 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21")
q5_qfq = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="qfq")
q5_hfq = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="hfq")
q15 = ts.get_k_data('002337', ktype="15", start="2017-09-15", end="2017-09-21")
q30 = ts.get_k_data('002337', ktype="30", start="2017-09-15", end="2017-09-21")
q60 = ts.get_k_data('002337', ktype="60", start="2017-09-15", end="2017-09-21")
qd = ts.get_k_data('002337', ktype="D")
qw = ts.get_k_data('002337', ktype="W")
qm = ts.get_k_data('002337', ktype="M")

ts.get_hist_data()

for df, fn in zip([q5, q5_qfq, q5_hfq, q15, q30, q60, qd, qw, qm],
                  ["q5", "q5_qfq", "q5_hfq", "q15", "q30", "q60", "qd", "qw", "qm"]):
    df.to_csv(f"c:/Users/Yu/Desktop/tmp/{fn}.csv")

q = q5.copy()
q.index = pd.DatetimeIndex(q.date)
q_s11 = q.shift(11)
q["open"]

del q["date"]






def resample(df_hfq, df_lfq):
    df_hfq_ = df_hfq.copy(False)
    df_lfq = df_lfq.copy(False)


from matplotlib import pyplot as plt
