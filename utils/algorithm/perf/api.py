from utils.algorithm.perf.impl import api
from utils.decofactory.scic import vectorize, align_first, sample_check
from utils.decofactory.common import unhash_inscache
import numpy as np


ERROR_VAL = np.nan


@vectorize
@align_first(1)
def value_at_risk(r, m=1000, alpha=0.05):
    try:
        return sample_check((0,), 2)(api.value_at_risk)(r, m, alpha)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(2)
def tracking_error_a(r, r_bm, period):
    try:
        return sample_check((0, 1,), 3)(api.tracking_error_a)(r, r_bm, period)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(1)
def accumulative_return(p):
    try:
        return sample_check((0,), 2)(api.accumulative_return)(p)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(2)
def return_a(p, t):
    try:
        return sample_check((0,), 4)(api.return_a)(p, t)

    except AssertionError:
        return ERROR_VAL


def excess_return_a(p, p_bm, t):
    return return_a(p, t) - return_a(p_bm, t)


def info_a(p, p_bm, r, r_bm, t, period):
    return excess_return_a(p, p_bm, t) / tracking_error_a(r, r_bm, period)


@vectorize
@align_first(1)
def periods_pos(r):
    try:
        return sample_check((0,), 1)(api.periods_pos)(r)

    except AssertionError:
        return np.nan


@vectorize
@align_first(1)
def periods_pos_prop(r):
    try:
        return sample_check((0,), 1)(api.periods_pos_prop)(r)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(1)
def periods_neg(r):
    try:
        return sample_check((0,), 1)(api.periods_neg)(r)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(1)
def periods_neg_prop(r):
    try:
        return sample_check((0,), 1)(api.periods_neg_prop)(r)

    except AssertionError:
        return ERROR_VAL


@vectorize
@align_first(1)
def standard_deviation(r):
    try:
        return sample_check((0,), 3)(api.standard_deviation)(r)

    except AssertionError:
        return ERROR_VAL


def standard_deviation_a(r, period_y):
    return standard_deviation(r) * period_y ** .5


def sharpe_a(p, p_rf, r, t, period_y):
    return excess_return_a(p, p_rf, t) / standard_deviation_a(r, period_y)


class CalculableSeries:
    def __init__(self, p, p_bm=None, r_rf=None, t=None, period=None):
        self.p = p
        self.p_bm = p_bm
        self.r_rf = r_rf
        self.t = t
        self.period = period

    @property
    @unhash_inscache()
    def p_rf(self):
        return np.array([1, *(1 + np.nancumsum(self.r_rf))])

    @property
    @unhash_inscache()
    def r(self):
        return self.p[1:] / self.p[:-1] - 1

    @property
    @unhash_inscache()
    def r_bm(self):
        return self.p_bm[1:] / self.p_bm[:-1] - 1

    @property
    @unhash_inscache()
    def mask_p(self):
        return ~np.isnan(self.p)

    @property
    @unhash_inscache()
    def mask_p_bm(self):
        return ~np.isnan(self.p_bm)


class Calculator:

    def __init__(self, p, p_bm=None, r_rf=None, t=None, period=None):
        self.ts = CalculableSeries(p, p_bm, r_rf, t, period)

    # Return
    @property
    def accumulative_return(self):
        return accumulative_return(self.ts.p)

    @property
    def return_a(self):
        return return_a(self.ts.p, self.ts.t)

    @property
    def excess_return_a(self):
        return excess_return_a(self.ts.p, self.ts.p_bm, self.ts.t)

    @property
    def periods_pos(self):
        return periods_pos(self.ts.r)

    @property
    def periods_pos_prop(self):
        return periods_pos_prop(self.ts.r)

    # Risk
    @property
    def standard_deviation(self):
        return standard_deviation(self.ts.r)

    @property
    def standard_deviation_a(self):
        return standard_deviation_a(self.ts.r, self.ts.period)

    @property
    def tracking_error_a(self):
        return tracking_error_a(self.ts.r, self.ts.r_bm, self.ts.period)

    @property
    def value_at_risk(self):
        return value_at_risk(self.ts.r)

    @property
    def periods_neg(self):
        return periods_neg(self.ts.r)

    @property
    def periods_neg_prop(self):
        return periods_neg_prop(self.ts.r)

    # Risk-Adjusted Return
    @property
    def info_a(self):
        return info_a(self.ts.p, self.ts.p_bm, self.ts.r, self.ts.r_bm, self.ts.t, self.ts.period)

    @property
    def sharpe_a(self):
        return sharpe_a(self.ts.p, self.ts.p_rf, self.ts.r, self.ts.t, self.ts.period)
