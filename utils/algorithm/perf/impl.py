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
    def standard_deviation(self, r: np.ndarray):
        return np.std(r, ddof=1)

    def standard_deviation_a(self, r: np.ndarray, period_y: int):
        return self.standard_deviation(r) * period_y ** .5

    def downside_deviation_a(self, r: np.ndarray, r_rf: np.ndarray, period_y: int, order: int=2):
        delta = r - r_rf
        delta[delta > 0] = 0
        # todo 20180603 N阶年化下行标准差公式确认
        return period_y ** (1 / order) * (-(delta ** order).sum() / (len(r) - 1)) ** (1 / order)

    def drawdown(self, p: np.ndarray) -> np.ndarray:
        return np.maximum.accumulate(p) / p - 1

    def pain_index(self, p: np.ndarray):
        return self.drawdown(p)[1:].mean()

    def pain_ratio(self, p: np.ndarray, p_rf: np.ndarray, t: np.ndarray):
        return self.excess_return_a(p, p_rf, t) / self.pain_index(p)

    def max_drawdown(self, p: np.ndarray):
        return (1 - p / np.maximum.accumulate(p)).max()

    def tracking_error_a(self, r: np.ndarray, r_bm: np.ndarray, period_y: int):
        return self.standard_deviation(r - r_bm) * period_y ** .5

    def periods_neg(self, r: np.ndarray):
        return (r < 0).sum()

    def periods_neg_prop(self, r: np.ndarray):
        return self.periods_neg(r) / len(r)

    def var_series(self, r: np.ndarray, m : int=1000, alpha :float=.05):
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

    def VaR(self, r: np.ndarray, m: int=1000, alpha: float=.05):
        """
        Value at risk, aka. VaR

        Args:
            r: np.array
            m: int, default 1000
                times of sampling
            alpha: float, default 0.05
                confidence level

        Returns:
            float, or np.array[float]

        """

        var_series = self.var_series(r, m, alpha)
        var_series[var_series < 0] = 0
        var_alpha = var_series.mean()
        return var_alpha

    def CVaR(self, r: np.ndarray, m: int = 1000, alpha: float = .05):
        np.random.seed(527)  # set seed for random
        n = len(r)
        j = int((n - 1) * alpha + 1)
        g = ((n - 1) * alpha + 1) - j

        return_series_sorted = np.sort(np.random.choice(r, size=(m, n)))
        var_series = (g - 1) * return_series_sorted[:, j - 1] - g * return_series_sorted[:, j]
        ix = return_series_sorted < -var_series.reshape((len(var_series), 1))

        # todo 20180603 seeking better way for boolean indexing M * N matrix, instead of using list comprehension
        cvar_series = np.array([return_series_sorted[i][ix[i]].mean() for i in range(len(ix))])
        return np.nanmean(cvar_series)

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

    def jensen_a(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray, period_y):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * delta_bf.sum()
        down = T * (delta_bf ** 2).sum() - delta_bf.sum() ** 2

        beta = up / down
        alpha = delta_pf.mean() - beta * delta_bf.mean()
        return (1 + alpha) ** period_y - 1

    def beta(self, r: np.ndarray, r_bm: np.ndarray, r_rf: np.ndarray):
        T = len(r)
        delta_pf, delta_bf = r - r_rf, r_bm - r_rf
        up = T * (delta_pf * delta_bf).sum() - delta_pf.sum() * delta_bf.sum()
        down = T * (delta_bf ** 2).sum() - delta_bf.sum() ** 2
        return up / down




api = Api()  # new Api instance, and register overloading methods
