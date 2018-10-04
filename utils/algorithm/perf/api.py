from utils.algorithm.perf.impl import api
from utils.decofactory.scic import sample_check, align, auto
from utils.decofactory.common import unhash_cache
import numpy as np

ERROR_VAL = np.nan


@auto([0])
@align([0])
def accumulative_return(p):
    try:
        return sample_check((0,), 2)(api.accumulative_return)(p)

    except AssertionError:
        return ERROR_VAL


@auto([0])
@align([0, 1])
def return_a(p, t):
    try:
        return sample_check((0,), 4)(api.return_a)(p, t)

    except AssertionError:
        return ERROR_VAL


def excess_return_a(p, p_bm, t):
    return return_a(p, t) - return_a(p_bm, t)


@auto([0])
@align([0])
def periods_pos(r):
    try:
        return sample_check((0,), 1)(api.periods_pos)(r)

    except AssertionError:
        return np.nan


@auto([0])
@align([0])
def periods_pos_prop(r):
    try:
        return sample_check((0,), 1)(api.periods_pos_prop)(r)

    except AssertionError:
        return ERROR_VAL


@auto([0])
@align([0])
def periods_neg(r):
    try:
        return sample_check((0,), 1)(api.periods_neg)(r)

    except AssertionError:
        return ERROR_VAL


@auto([0])
@align([0])
def periods_neg_prop(r):
    try:
        return sample_check((0,), 1)(api.periods_neg_prop)(r)

    except AssertionError:
        return ERROR_VAL


@auto([0])
@align([0])
def VaR(r, m=1000, alpha=0.05):
    try:
        return sample_check((0,), 2)(api.VaR)(r, m, alpha)

    except AssertionError:
        return ERROR_VAL


@auto([0, 1])
@align([0, 1])
def tracking_error_a(r, r_bm, period):
    try:
        return sample_check((0, 1,), 3)(api.tracking_error_a)(r, r_bm, period)

    except AssertionError:
        return ERROR_VAL


def info_a(p, p_bm, r, r_bm, t, period):
    return excess_return_a(p, p_bm, t) / tracking_error_a(r, r_bm, period)


@auto([0])
@align([0])
def standard_deviation(r):
    try:
        return sample_check((0,), 3)(api.standard_deviation)(r)

    except AssertionError:
        return ERROR_VAL


def standard_deviation_a(r, period_y):
    return standard_deviation(r) * period_y ** .5


def sharpe_a(p, p_rf, r, t, period_y):
    return excess_return_a(p, p_rf, t) / standard_deviation_a(r, period_y)


class AcceleratedCalSeries:
    def __init__(self, p=None, p_bm=None, r_rf=None):
        self.p = p
        self.p_bm = p_bm
        self.r_rf = r_rf

    @property
    @unhash_cache()
    def p_rf(self):
        return np.array([1, *(1 + np.nancumsum(self.r_rf))])

    @property
    @unhash_cache()
    def r(self):
        return self.p[1:] / self.p[:-1] - 1

    @property
    @unhash_cache()
    def r_bm(self):
        return self.p_bm[1:] / self.p_bm[:-1] - 1


class Calculator:
    def __init__(self, p, p_bm=None, r_rf=None, t=None, period=None):
        self.pf = p if isinstance(p, AcceleratedCalSeries) else AcceleratedCalSeries(p=p)
        self.bm = p_bm if isinstance(p_bm, AcceleratedCalSeries) else AcceleratedCalSeries(p_bm=p_bm)
        self.rf = r_rf if isinstance(r_rf, AcceleratedCalSeries) else AcceleratedCalSeries(r_rf=r_rf)
        self.t = t
        self.period = period

    # Return
    @property
    def accumulative_return(self):
        return accumulative_return(self.pf.p)

    @property
    def return_a(self):
        return return_a(self.pf.p, self.t)

    @property
    def excess_return_a(self):
        return excess_return_a(self.pf.p, self.bm.p_bm, self.t)

    @property
    def periods_pos(self):
        return periods_pos(self.pf.r)

    @property
    def periods_pos_prop(self):
        return periods_pos_prop(self.pf.r)

    # Risk
    @property
    def standard_deviation(self):
        return standard_deviation(self.pf.r)

    @property
    def standard_deviation_a(self):
        return standard_deviation_a(self.pf.r, self.period)

    @property
    def tracking_error_a(self):
        return tracking_error_a(self.pf.r, self.bm.r_bm, self.period)

    @property
    def VaR(self):
        return VaR(self.pf.r)

    @property
    def periods_neg(self):
        return periods_neg(self.pf.r)

    @property
    def periods_neg_prop(self):
        return periods_neg_prop(self.pf.r)

    # Risk-Adjusted Return
    @property
    def info_a(self):
        return info_a(self.pf.p, self.bm.p_bm, self.pf.r, self.bm.r_bm, self.t, self.period)

    @property
    def sharpe_a(self):
        return sharpe_a(self.pf.p, self.rf.p_rf, self.pf.r, self.t, self.period)
