# -*- coding: utf-8 -*-

#%% 0.0 文件位置以及文件名
# mypath = 'E://Strategies//Python'

# import sys
# sys.path.append(mypath)
# 
#%% 0.1 导入工具包
import datetime
import pandas as pd
import numpy as np
from datasamples import FuturesData
from genfactors import RiskFactors
from backtest import DailyTrading
from measures import PerformMeasures
from matplotlib import pyplot as plt
import seaborn as sns

#%% 0.2 MySQL setting
MySQL_HOST = "106.14.96.31"
MySQL_USER = "root"
MySQL_PASSWORD = "SHzb1234"
MySQL_DBNAME = "future_data"
MySQL_PORT = 3306

#%% 1.0 风险因子汇总
factor_names = ['幅度动量因子',
                '平均趋势度因子',
                '整体趋势度因子',
                '市场强弱因子',
                '毛刺制约因子',
                '乖离率因子',
                'RSV因子',
                'William因子',
                'PC因子',
                'VR因子',
                '市场情绪因子',
                '市场能量因子',
                '标准化动量因子',
                '成交量加权动量因子',
                '趋势反转制衡因子',
                '跳空制衡因子']

#%% 1.1 备选品种汇总
instruments = ['a', 'ag', 'al', 'AP',
               'bu',
               'c', 'CF', 'cs', 'cu',
               'eb',
               'FG', 'fu',
               'hc',
               'i',
               'j', 'jd', 'jm',
               'l',
               'm', 'MA',
               'ni',
               'OI',
               'p', 'pb', 'pg', 'pp',
               'rb', 'RM', 'ru',
               'sc', 'SM', 'sn', 'sp', 'SR',
               'TA',
               'UR',
               'v',
               'y',
               'ZC', 'zn']

#%% 1.2 因子权重&时间窗口&平均模式设置
NFct = len(factor_names)
wFct = [0]*NFct           # 因子权重 factors weights
Lags = [18]*NFct           # 时间窗口 rolling windows
Modes = ['simple']*NFct        # 加权模式 weighting methods

for i in range(0, NFct):
    
    if i == 0:            # 0. 幅度动量因子
        wFct[i] = 0
        Lags[i] = 17
        Modes[i] = 'linear'
    
    elif i == 1:          # 1. 平均趋势度因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 2:          # 2. 整体趋势度因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 3:          # 3. 市场强弱因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 4:          # 4. 毛刺制约因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 5:          # 5. 乖离率因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 6:          # 6. RSV因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 7:          # 7. William因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 8:          # 8. PC因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 9:          # 9. VR因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 10:         # 10. 市场情绪因子 有效因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 11:         # 11. 市场能量因子 有效因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 12:         # 12. 标准化动量因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 13:         # 13. 成交量加权动量因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 14:         # 14. 趋势反转制衡因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    
    elif i == 15:         # 15. 跳空制衡因子
        wFct[i] = 0
        Lags[i] = 18
        Modes[i] = 'linear'
    else:
        pass

wFct = np.array(wFct)

#%% 1.3 参数设置
t0 = 20160104            # 初始日期 start date
tn = 20200831            # 结束日期 end date
I = 10000000             # 初始资金 initial investment
lev = 2                  # 交易杠杆 leverage 
fBuy = 1/2 * I * lev     # 多头资金 investment in long portfolio
fSell = 1/2 * I * lev    # 空头资金 investment in short portfolio
h = 1                    # 持有周期 holding period
fee = 1.0                # 交易滑点 coefficient of transaction cost
rf = 0.03                # 无风险利率 risk-free interest rate
NBuy = 5                 # 做多品种个数 number of longs
NSell = 5                # 做空品种个数 number of shorts
NAsset = NBuy + NSell    # 组合中品种数 number of assets in portfolio

# ----------------------------------------------------
#%% 3. 主函数：
# ------------------------------------------------------

if __name__ == '__main__':
    
#     forecast = input('模式(1=预测, 2=回测): ')
#     if forecast == '1':
#         todaydate = datetime.date.today()
#         weekday = datetime.date.isoweekday(todaydate)
#         if weekday == 5:
#             tomorrowdate = todaydate + datetime.timedelta(days=3)
#         else:
#             tomorrowdate = todaydate + datetime.timedelta(days=1)
#         tomorrowday = tomorrowdate.day
#         tomorrowmonth = tomorrowdate.month
#         tomorrowyear = tomorrowdate.year
#         tomorrow = int(tomorrowyear*10000+tomorrowmonth*100+tomorrowday)
#         tn = tomorrow
#     else:
#         pass

    # print('*' * 50)
    # tic_toc_0 = datetime.datetime.now()
    # print('程序开始: ', tic_toc_0)
    
    # # 从数据库中获取期货指数OHLCV数据
    # GetData = FuturesData(MySQL_HOST, MySQL_USER, MySQL_PASSWORD, MySQL_DBNAME, MySQL_PORT, instruments)
    # DataStruct, Sizes, Ticks = GetData.fetching()
    # tic_toc_1 = datetime.datetime.now()
    # print('数据样本下载完成: ', tic_toc_1, ' 耗时: ', tic_toc_1-tic_toc_0)
    io = r"C:\Users\HP\Desktop\Test.csv"
    DataStruct = pd.read_csv(io)
    DataStruct.set_index(["Unnamed: 0"], inplace=True)
    DataStruct.index.name = None
    Sizes = {'CF': 5, 'FG': 20, 'IC': 200, 'IF': 300, 'IH': 300, 'JR': 20, 'LR': 20, 'MA': 10, 'OI': 10, 'PM': 50, 'RI': 20, 'RM': 10, 'RS': 10, 'SF': 5, 'SM': 5, 'SR': 10, 'T': 10000, 'TA': 5, 'TF': 10000, 'WH': 20, 'ZC': 100, 'a': 10, 'ag': 15, 'al': 5, 'au': 1000, 'b': 10, 'bb': 500, 'bu': 10, 'c': 10, 'cs': 10, 'cu': 5, 'fb': 10, 'fu': 10, 'hc': 10, 'i': 100, 'j': 100, 'jd': 5, 'jm': 60, 'l': 5, 'm': 10, 'ni': 1, 'p': 10, 'pb': 5, 'pp': 5, 'rb': 10, 'ru': 10, 'sn': 1, 'v': 5, 'wr': 10, 'y': 10, 'zn': 5, 'AP': 10, 'sc': 1000, 'CJ': 5, 'eb': 5, 'eg': 10, 'pg': 20, 'rr': 10, 'SA': 20, 'sp': 10, 'ss': 5, 'UR': 20, 'CY': 5, 'lu': 10, 'nr': 10}
    Ticks = {'CF': 5.0, 'FG': 1.0, 'IC': 0.2, 'IF': 0.2, 'IH': 0.2, 'JR': 1.0, 'LR': 1.0, 'MA': 1.0, 'OI': 1.0, 'PM': 1.0, 'RI': 1.0, 'RM': 1.0, 'RS': 1.0, 'SF': 1.0, 'SM': 2.0, 'SR': 1.0, 'T': 0.005, 'TA': 2.0, 'TF': 0.005, 'WH': 1.0, 'ZC': 0.2, 'a': 1.0, 'ag': 1.0, 'al': 5.0, 'au': 0.02, 'b': 1.0, 'bb': 0.05, 'bu': 2.0, 'c': 1.0, 'cs': 1.0, 'cu': 10.0, 'fb': 0.5, 'fu': 1.0, 'hc': 1.0, 'i': 0.5, 'j': 0.5, 'jd': 1.0, 'jm': 0.5, 'l': 5.0, 'm': 1.0, 'ni': 10.0, 'p': 2.0, 'pb': 5.0, 'pp': 1.0, 'rb': 1.0, 'ru': 5.0, 'sn': 10.0, 'v': 5.0, 'wr': 1.0, 'y': 2.0, 'zn': 5.0, 'AP': 1.0, 'sc': 0.1, 'CJ': 5.0, 'eb': 1.0, 'eg': 1.0, 'pg': 1.0, 'rr': 1.0, 'SA': 1.0, 'sp': 2.0, 'ss': 5.0, 'UR': 1.0, 'CY': 5.0, 'lu': 1.0, 'nr': 5.0} 
    
    i1 = wFct[1]
    i3 = wFct[3]
    i8 = wFct[8]
    i10 = wFct[10]
    i11 = wFct[11]
    i12 = wFct[12]
    # i=1、3、8、10、11、12
    df = pd.DataFrame()
    result = 0
    n = 0
    for i1 in range(0,10):
        wFct[3] = 0
        wFct[1] = i1
        for i3 in range(0,10):
            wFct[8] = 0
            wFct[3] = i3
            for i8 in range(0,10):
                wFct[10] = 0
                wFct[8] = i8
                for i10 in range(0,10):
                    wFct[11] = 0
                    wFct[10] = i10
                    for i11 in range(0,10):
                        wFct[12] = 0
                        wFct[11] = i11
                        for i12 in range(0,3):
                            wFct[12] = i12
                            print('finish:' + str(n))
                            GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
                            TotalScore =  GenFactors.scoring()
                            Investment = DailyTrading(DataStruct, TotalScore, t0, tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, 'equal')
                            account, win_rate = Investment.tradings()
                            Values = account.values[:,1]  # wealth 
                            Performance = PerformMeasures(I, Values, rf, frequency='day')
                            R, sigma, Sharpe = Performance.Sharpe()
                            maxDrawdown, MAR = Performance.MAR(R)
                            hwm, nt = Performance.highwatermark()
                            dts = pd.to_datetime(account.index, format='%Y%m%d')
                            ss = {"单位净值": round(Values[-1]/I, 4),
                                  "夏普比率": round(Sharpe, 4),
                                  'MAR比率': round(MAR, 4),
                                  '年化收益率': str(R*100)[0:7]+'%',
                                  '交易总胜率': str(win_rate*100)[0:7]+'%',
                                  '历史高点(净值)': round(hwm/I,4),
                                  '历史高点(日期)': dts[int(nt)].date()}
                            if round(MAR, 4) > result:
                                print(result,wFct)
                                result = round(MAR, 4)
                            df = df.append(ss,ignore_index=True)
                            n = n + 1
                            
                            # GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
                            # GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
                            # TotalScore =  GenFactors.scoring()
                            # Investment = DailyTrading(DataStruct, TotalScore, t0, tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, 'equal')
                            # account, win_rate = Investment.tradings()
                            # Values = account.values[:,1]  # wealth 
                            # Performance = PerformMeasures(I, Values, rf, frequency='day')
                            # R, sigma, Sharpe = Performance.Sharpe()
                            # maxDrawdown, MAR = Performance.MAR(R)
                            # hwm, nt = Performance.highwatermark()
                            # dts = pd.to_datetime(account.index, format='%Y%m%d')
                            # ss = {"单位净值": round(Values[-1]/I, 4),
                            #       "夏普比率": round(Sharpe, 4),
                            #       'MAR比率': round(MAR, 4),
                            #       '年化收益率': str(R*100)[0:7]+'%',
                            #       '交易总胜率': str(win_rate*100)[0:7]+'%',
                            #       '历史高点(净值)': round(hwm/I,4),
                            #       '历史高点(日期)': dts[int(nt)].date()}
                            # print(ss,wFct)
    
    # for i in range(3,15):
    #     NBuy = i
    #     NSell = i 
    #     NAsset = NBuy + NSell
    #     GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
    #     TotalScore =  GenFactors.scoring()
    #     Investment = DailyTrading(DataStruct, TotalScore, t0, tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, 'equal')
    #     account, win_rate = Investment.tradings()
    #     Values = account.values[:,1]  # wealth 
    #     Performance = PerformMeasures(I, Values, rf, frequency='day')
    #     R, sigma, Sharpe = Performance.Sharpe()
    #     maxDrawdown, MAR = Performance.MAR(R)
    #     hwm, nt = Performance.highwatermark()
    #     dts = pd.to_datetime(account.index, format='%Y%m%d')
    #     ss = {"单位净值": round(Values[-1]/I, 4),
    #           "夏普比率": round(Sharpe, 4),
    #           'MAR比率': round(MAR, 4),
    #           '年化收益率': str(R*100)[0:7]+'%',
    #           '交易总胜率': str(win_rate*100)[0:7]+'%',
    #           '历史高点(净值)': round(hwm/I,4),
    #           '历史高点(日期)': dts[int(nt)].date()}
    #     print(ss)

    # NBuy = 0
    # NSell = 0
    # NAsset = NBuy + NSell
    # df = pd.DataFrame()
    # for i in range(3,16):
    #     Nbuy = i
    #     NSell = i
    #     NAsset = NBuy + NSell
    #     GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
    #     TotalScore =  GenFactors.scoring()
    #     Investment = DailyTrading(DataStruct, TotalScore, t0, tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, 'equal')
    #     account, win_rate = Investment.tradings()
    #     Values = account.values[:,1]  # wealth 
    #     Performance = PerformMeasures(I, Values, rf, frequency='day')
    #     R, sigma, Sharpe = Performance.Sharpe()
    #     maxDrawdown, MAR = Performance.MAR(R)
    #     hwm, nt = Performance.highwatermark()
    #     dts = pd.to_datetime(account.index, format='%Y%m%d')
    #     ss = {"单位净值": round(Values[-1]/I, 4),
    #           "夏普比率": round(Sharpe, 4),
    #           'MAR比率': round(MAR, 4),
    #           '年化收益率': str(R*100)[0:7]+'%',
    #           '交易总胜率': str(win_rate*100)[0:7]+'%',
    #           '历史高点(净值)': round(hwm/I,4),
    #           '历史高点(日期)': dts[int(nt)].date(),
    #           'index':'Nbuy = '+str(i),}
    #     df=df.append(ss,ignore_index=True)
    #     NBuy = NBuy + 1
    #     NSell = NSell + 1
    #     print('Nbuy='+ str(i) +'已完成计算')
    # df.set_index(["index"], inplace=True)

    # 计算各品种因子得分
    # GenFactors = RiskFactors(DataStruct, instruments, Lags, Modes, wFct)
    # TotalScore = GenFactors.scoring()
    
    # # 输入回测系统
    # Investment = DailyTrading(DataStruct, TotalScore, t0, tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, 'equal')
    # account, win_rate = Investment.tradings()
    
    # # 考量业绩
    # Values = account.values[:,1]  # wealth 
    # Performance = PerformMeasures(I, Values, rf, frequency='day')
    # R, sigma, Sharpe = Performance.Sharpe()
    # maxDrawdown, MAR = Performance.MAR(R)
    # hwm, nt = Performance.highwatermark()
    
    # 画图
    # dts = pd.to_datetime(account.index, format='%Y%m%d')
    # plt.plot(dts, Values/I, linewidth=1.0)
    # tic_toc_4 = datetime.datetime.now()
    # print('程序结束: ', tic_toc_4, ' 总耗时: ', tic_toc_4-tic_toc_0)
    
    # # 绩效
    # print('*'*50)
    # print('起始日: {T0}   本金: ￥{Initial}'.format(T0=dts[0].date(), Initial=I))
    # print('结算日: {Tn}   市值: ￥{Vn}'.format(Tn=dts[-1].date(), Vn=int(Values[-1])))
    # print('单位净值:', round(Values[-1]/I, 4))
    # print('夏普比率:', round(Sharpe, 4))
    # print('MAR比率:', round(MAR, 4))
    # print('年化收益率:', str(R*100)[0:7]+'%')
    # print('年化波动率: ', str(sigma*100)[0:6]+'%')
    # print('最大回撤率: ', str(maxDrawdown*100)[0:6]+'%')
    # print('交易总胜率:', str(win_rate*100)[0:7]+'%')
    # print('历史高点(净值):', round(hwm/I,4))
    # print('历史高点(日期):', dts[int(nt)].date())
    # print('*'*50)


