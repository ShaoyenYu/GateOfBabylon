from utils import io, config as cfg
import datetime as dt
import pandas as pd
import tushare as ts
from multiprocessing.dummy import Pool as ThreadPool

# 加了ktype参数, 并且ktype !=0 时, start, end参数失效;
q1 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21")
q2 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="hfq")
q3 = ts.get_k_data('002337', ktype="5", start="2017-09-16", end="2017-09-21", autype="hfq")
ts.get_k_data('002337', ktype="15", start="2017-09-15", end="2017-09-21")
ts.get_k_data('002337', ktype="30", start="2017-09-15", end="2017-09-21")
ts.get_k_data('002337', ktype="60", start="2017-09-15", end="2017-09-21")
ts.get_k_data('002337', ktype="D", start="2017-09-15", end="2017-09-21")
ts.get_k_data('002337', ktype="W", start="2017-09-15", end="2017-09-21")
ts.get_k_data('002337', ktype="M", start="2017-09-15", end="2017-09-21")

ts.get_k_data('002337', start="2017-09-15", end="2017-09-21")
