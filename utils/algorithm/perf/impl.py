import numpy as np
from utils.metafactory.overloading import MultipleMeta

__all__ = ["api"]


class Api(metaclass=MultipleMeta):
    def accumulative_return(self, p):
        return p[-1] / p[0] - 1

    def return_a(self, p: np.ndarray, period: int):
        return (1 + self.accumulative_return(p)) ** (period / (len(p) - 1)) - 1

    def return_a(self, p: np.ndarray, t: np.ndarray):
        return (1 + self.accumulative_return(p)) ** (365 * 86400 / (t[-1] - t[0])) - 1

    def excess_return_a(self, p: np.ndarray, p_bm: np.ndarray, t: np.ndarray):
        return self.return_a(p, t) - self.return_a(p_bm, t)

    def excess_return_a(self, p: np.ndarray, p_bm: np.ndarray, period: int):
        return self.return_a(p, period) - self.return_a(p_bm, period)

    def periods_pos(self, r: np.ndarray):
        return (r > 0).sum()

    def periods_pos_prop(self, r: np.ndarray):
        return self.periods_pos(r) / len(r)

    def odds(self, r: np.ndarray, r_bm: np.ndarray):
        return (r > r_bm).sum() / len(r)

    def max_return(self, r: np.ndarray):
        return r.max()

    def min_return(self, r: np.ndarray):
        return r.min()

    # Risk
    def standard_deviation(self, r: np.ndarray, ddof=1):
        return np.std(r, ddof=ddof)

    def standard_deviation_a(self, r: np.ndarray, period_y: int):
        return self.standard_deviation(r) * period_y ** .5

    def downside_deviation_a(self, r: np.ndarray, r_rf: np.ndarray, period_y: int, order: int = 2):
        delta = r - r_rf
        delta[delta > 0] = 0
        # todo 20180603 N阶年化下行标准差公式确认
        return period_y ** (1 / order) * (-(delta ** order).sum() / (len(r) - 1)) ** (1 / order)

    def drawdown(self, p: np.ndarray) -> np.ndarray:
        return np.maximum.accumulate(p) / p - 1

    def average_drawdown(self, r: np.ndarray):
        return -np.where(r > 0, 0, r).mean()

    def pain_index(self, p: np.ndarray):
        return self.drawdown(p)[1:].mean()

    def max_drawdown(self, p: np.ndarray):
        return (1 - p / np.maximum.accumulate(p)).max()

    def tracking_error_a(self, r: np.ndarray, r_bm: np.ndarray, period_y: int):
        return self.standard_deviation(r - r_bm) * period_y ** .5

    def periods_neg(self, r: np.ndarray):
        return (r < 0).sum()

    def periods_neg_prop(self, r: np.ndarray):
        return self.periods_neg(r) / len(r)

    def unbias(self, s: np.ndarray):
        return 1 + 1 / (len(s) - 1)

    def moment(self, s: np.ndarray, order: int):
        return ((s - s.mean()) ** order).sum() / len(s)

    def skewness(self, r: np.ndarray):
        return self.unbias(r) * self.moment(r, 3) / self.standard_deviation(r, ddof=1) ** 3

    def kurtosis(self, r: np.ndarray):
        return self.unbias(r) * self.moment(r, 4) / self.standard_deviation(r, ddof=1) ** 4

    def var_series(self, r: np.ndarray, m: int = 1000, alpha: float = .05) -> np.ndarray:
        np.random.seed(527)  # set seed for random
        n = len(r)
        j = int((n - 1) * alpha + 1)
        g = ((n - 1) * alpha + 1) - j

        # equivalent implementation
        # random_choice = np.random.randint(n, size=(m, n))
        # return_series_sorted = np.sort(r[random_choice])

        return_series_sorted = np.sort(np.random.choice(r, size=(m, n)))
        var_series = (g - 1) * return_series_sorted[:, j - 1] - g * return_series_sorted[:, j]
        return var_series

    def VaR(self, r: np.ndarray, m: int = 1000, alpha: float = .05):
        """
        Value at risk, aka. VaR

        Args:
            r: np.array
            m: int, default 1000
                times of sampling
            alpha: float, default 0.05
                confidence level

        Returns:
            float

        """

        var_series = self.var_series(r, m, alpha)
        var_alpha = np.where(var_series < 0, 0, var_series).mean()
        return var_alpha

    def CVaR(self, r: np.ndarray, m: int = 1000, alpha: float = .05):
        np.random.seed(527)  # set seed for random
        n = len(r)
        j = int((n - 1) * alpha + 1)
        g = ((n - 1) * alpha + 1) - j

        return_series_sorted = np.sort(np.random.choice(r, size=(m, n)))
        var_series = (g - 1) * return_series_sorted[:, j - 1] - g * return_series_sorted[:, j]

        cvar_series = np.where(
            return_series_sorted < -var_series.reshape((len(var_series), 1)), return_series_sorted, np.nan)
        return np.nanmean(np.nanmean(cvar_series, 1))

    # Risk-Adjusted Return
    def sharpe_a(self, p: np.ndarray, p_rf: np.ndarray, r: np.ndarray, t: np.ndarray, period_y: int):
        return self.excess_return_a(p, p_rf, t) / self.standard_deviation_a(r, period_y)

    def calmar_a(self, p: np.ndarray, p_rf: np.ndarray, t: np.ndarray):
        return self.excess_return_a(p, p_rf, t) - self.max_drawdown(p)

    def sortino_a(self, p: np.ndarray, p_rf: np.ndarray, t: np.ndarray, r: np.ndarray, r_rf, period_y):
        return self.excess_return_a(p, p_rf, t) / self.downside_deviation_a(r, r_rf, period_y, 2)

    def treynor_a(self, p, p_rf, t, r, r_bm, r_rf):
        return self.excess_return_a(p, p_rf, t) / self.beta(r, r_bm, r_rf)

    def info_a(self, p: np.ndarray, p_bm: np.ndarray, r: np.ndarray, r_bm: np.ndarray, t: np.ndarray, period_y: int):
        return self.excess_return_a(p, p_bm, t) / self.tracking_error_a(r, r_bm, period_y)

    def alpha(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        s_b = delta_bf.sum()

        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * s_b
        down = T * (delta_bf ** 2).sum() - s_b ** 2

        beta = up / down
        return delta_pf.mean() - beta * delta_bf.mean()

    def beta(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        s_b = delta_bf.sum()

        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * s_b
        down = T * (delta_bf ** 2).sum() - s_b ** 2
        return up / down

    def jensen_a(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray, period_y):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        s_b = delta_bf.sum()

        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * s_b
        down = T * (delta_bf ** 2).sum() - s_b ** 2

        beta = up / down
        alpha = delta_pf.mean() - beta * delta_bf.mean()
        return (1 + alpha) ** period_y - 1

    def adjusted_jensen_a(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray, period_y):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        s_b = delta_bf.sum()

        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * s_b
        down = T * (delta_bf ** 2).sum() - s_b ** 2

        beta = up / down
        alpha = delta_pf.mean() - beta * delta_bf.mean()
        return ((1 + alpha) ** period_y - 1) / beta

    def excess_pl(self, r: np.ndarray, r_bm: np.ndarray):
        er = r - r_bm
        up = np.where(er < 0, 0, er)
        down = np.where(er > 0, 0, -er)
        return up.sum() / down.sum()

    def omega(self, r: np.ndarray, r_bm: np.ndarray):
        er = r - r_bm
        up = np.where(er < 0, 0, er)
        down = np.where(er > 0, 0, -er)
        return up.sum() / down.sum() * (er < 0).sum() / (er > 0).sum()

    def hurst_holder(self, r: np.ndarray):
        ys, zs = r.cumsum(), r - r.mean()
        return np.log((ys.max() - ys.min()) / ((zs ** 2).sum() / (len(r) - 1)) ** .5) / np.log(len(r))

    def corrcoef_spearman(self, r: np.ndarray, r_bm: np.ndarray):
        return np.corrcoef(r, r_bm)[0, 1]

    def unsystematic_risk(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        s_pb, s_p, s_b = (delta_pf * delta_bf).sum(), delta_pf.sum(), delta_bf.sum()

        up = T * s_pb - s_p * s_b
        down = T * (delta_bf ** 2).sum() - s_b ** 2
        beta = up / down
        alpha = delta_pf.mean() - beta * delta_bf.mean()
        return (((delta_pf ** 2).sum() - alpha * s_p - beta * s_pb) / (T - 2)) ** .5

    def pain_ratio(self, p: np.ndarray, p_rf: np.ndarray, t: np.ndarray):
        return self.excess_return_a(p, p_rf, t) / self.pain_index(p)


api = Api()  # new Api instance, and register overloading methods


def test():
    r = np.array([.1, .2, -.1, .3, .15, ])
    m, alpha = 1000, .05
    api.VaR(r, m, alpha)
    api.CVaR(r, m, alpha)
    np.nancumprod(np.where(r > 0, r, np.nan))
    #
    # a1 = np.array([1, 2, 3, 4, 5])
    # a2 = np.array([2, 1, 4, 3, 5])


if __name__ == "__main__":
    test()
