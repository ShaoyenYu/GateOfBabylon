import pandas as pd

def filter():
    """

    Returns:
        stock_ids

    """

class Account:
    def __init__(self, balance):
        pass


class Strategy:
    """
        策略类以资产账户实例为参数初始化

    """

    def __init__(self, account):
        self._account = account

    def hanlde_data(self):
        data = pd.DataFrame()

        pass


# class DataYesStrategy(Strategy):
#     # 第一部分：策略参数
#     start = '2017-01-01'                      # 回测起始时间
#     end = '2017-11-15'                       # 回测结束时间
#     benchmark = 'HS300'                       # 策略参考标准
#     universe = ['000001.XSHE', '600000.XSHG']        # 证券池，支持股票和基金
#     capital_base = 100000                      # 起始资金
#     freq = 'd'                             # 策略类型，'d'表示日间策略使用日线回测，'m'表示日内策略使用分钟线回测
#     refresh_rate = 1                         # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd'时间间隔的单位为交易日，若freq = 'm'时间间隔为分钟
#
#     # 第二部分：初始化策略，回测期间只运行一次，可以用于设置全局变量
#     # account是回测期间的虚拟交易账户，存储上述全局变量参数信息，并在整个策略执行期间更新并维护可用现金、证券的头寸、每日交易指令明细、历史行情数据等。
#     def initialize(self, account):
#         # account.i = 1
#         pass
#
#     # 第三部分：策略每天下单逻辑，执行完成后，会输出每天的下单指令列表
#     # 这个函数在每个交易日开盘前被调用，模拟每个交易日开盘前，交易策略会根据历史数据或者其他信息进行交易判断，生成交易指令
#     def handle_data(self, account):
#         # account.get_attribute_history：表示获取所有证券过去20天的closePrice数据，返回数据类型为 dict，键为每个证券的secID
#         hist = account.get_attribute_history('closePrice', 20)
#         for stk in account.universe:
#             # 计算股票过去5天收盘平均值
#             ma5 = hist[stk][-5:].mean()
#             # 计算股票过去20天收盘平均值
#             ma20 = hist[stk][:].mean()
#
#             # 如果5日均线大于20日均线，并且该股票当前没有持仓，则买入100手
#             # account.valid_secpos：表示当前交易日持有数量大于0的证券头寸。数据类型为字典，键为证券代码，值为头寸。
#             if ma5 > ma20 and stk not in account.valid_secpos:
#                 order(stk, 10000)
#             # 如果5日均线小于20日均线，则该股票全部卖出
#             elif ma5 <= ma20:
#                 order_to(stk, 0)



from CAL.PyCAL import *

cal = Calendar('CHINA.SSE')
start = '2016-01-01'  # 回测起始时间
end = '2017-12-31'
# 回测结束时间
stock1 = '601939.XSHG'
stock2 = '601288.XSHG'
benchmark = '801192.ZICN'  # 策略参考标准
universe = ['601939.XSHG', '601288.XSHG']
freq = 'd'  # 策略类型，'d'表示日间策略使用日线回测，'m'表示日内策略使用分钟线回测
coef = 1.9577
constant = -0.7354
std = 0.1472
refresh_rate = 1  # 调仓频率，表示执行handle_data的时间间隔，若freq = 'd'时间间隔的单位为交易日，若freq = 'm'时间间隔为分钟
# 配置账户信息，支持多资产多账户
accounts = {
    'my_account': AccountConfig(account_type='security', capital_base=10000000)
}


def initialize(context):
    pass


def handle_data(context):
    account = context.get_account('my_account')
    interval = cal.bizDatesNumber(startDate='2016-01-01', endDate=context.current_date, includeFirst=True,
                                  includeLast=True) - 1
    close1 = context.history(stock1, 'closePrice', 1, freq='1d', rtype='frame', style='ast')['closePrice']  # 建行收盘价
    close2 = context.history(stock2, 'closePrice', 1, freq='1d', rtype='frame', style='ast')['closePrice']  # 农行收盘价
    if interval < 10:  # 打印前十天的收盘价和上下界
        print('%s %f' % ('上界是： ', coef * close2.iloc[0][0] + constant + 2 * std))
        print('%s %f' % ('建行收盘价是: ', close1.iloc[0][0]))
        print('%s %f' % ('下界是： ', coef * close2.iloc[0][0] + constant - 2 * std))
        print('-----------------------------------')
    if close1.iloc[0][0] > coef * close2.iloc[0][0] + constant + 2 * std:
        order_to(stock1, 0)
        order_pct_to(stock2, 1)
    if close1.iloc[0][0] < coef * close2.iloc[0][0] + constant - 2 * std:
        order_to(stock2, 0)
        order_pct_to(stock1, 1)