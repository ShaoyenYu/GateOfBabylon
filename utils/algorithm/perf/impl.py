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

    # Risk
    def standard_deviation(self, r: np.ndarray):
        return np.std(r, ddof=1)

    def standard_deviation_a(self, r: np.ndarray, period_y: int):
        return self.standard_deviation(r) * period_y ** .5

    def tracking_error_a(self, r: np.ndarray, r_bm: np.ndarray, period_y: int):
        return self.standard_deviation(r - r_bm) * period_y ** .5

    def periods_neg(self, r: np.ndarray):
        return (r < 0).sum()

    def periods_neg_prop(self, r: np.ndarray):
        return self.periods_neg(r) / len(r)

    def value_at_risk(self, r: np.ndarray, m: int=1000, alpha: float=.05):
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

        np.random.seed(527)  # set seed for random
        n = len(r)
        j = int((n - 1) * alpha + 1)
        g = ((n - 1) * alpha + 1) - j

        # np.random.seed(527)  # set seed for random
        # random_choice = np.random.randint(n, size=(m, n))
        # return_series_sorted = np.sort(r[random_choice])

        return_series_sorted = np.sort(np.random.choice(r, size=(m, n)))
        var_alpha = sum((g - 1) * return_series_sorted[:, j - 1] - g * return_series_sorted[:, j]) / m

        return max(0, var_alpha)

    # Risk-Adjusted Return
    def info_a(self, p: np.ndarray, p_bm: np.ndarray, r: np.ndarray, r_bm: np.ndarray, t: np.ndarray, period_y: int):
        return self.excess_return_a(p, p_bm, t) / self.tracking_error_a(r, r_bm, period_y)

    def sharpe_a(self, p: np.ndarray, p_rf: np.ndarray, r: np.ndarray, t: np.ndarray, period_y: int):
        return self.excess_return_a(p, p_rf, t) / self.standard_deviation_a(r, period_y)


api = Api()  # new Api instance, and register overloading methods
