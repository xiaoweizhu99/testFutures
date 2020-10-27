# -*- coding: utf-8 -*-
"""
Created on Mon May 11 15:54:50 2020

@author: wenrui zhang ZhaoBei Capital

Notes:
    1. pandas.DataFrame.ewm(span=Lag, adjust=False).mean()
        ==
       Matlab: movavg(data, 'exponential', Lag);
       
    2. pandas.DataFrame.apply(lambda row: row.x * row.y, axis=1)
        ==
       Matlab: x .* y;
       
    3. numpy.dot(a, b)
       ==
       Matlab: a * b
       
    4. 制约因子: 当该因子值 >0 时, 则该品种上涨概率减小;
               当该因子值 <0 时, 则该品种下跌概率减小;
               所以，该因子应当被赋予负权重
"""

import numpy as np
import pandas as pd

class RiskFactors(object):

    def __init__(self, DataStruct, instruments, lags, mode, weight):
        self.DataStruct = DataStruct                                 # 包含所有品种OHLCV数据
        self.instruments = instruments                               # 所有备选品种list
        self.Lag = lags                                              # 各个因子的Lag值
        self.mode = mode                                             # 计算各个因子的加权平均模式
        self.weight = np.array(weight)                               # 计算总分的weights
        self.T = len(DataStruct)                                     # 样本长度 Sample period
    
    def scoring(self):
        # ScoreStruct = pd.DataFrame(self.DataStruct.index)            # ScoreStruct记录具体分数
        # ScoreStruct = ScoreStruct.astype(int)                        # 转换为int
        # ScoreStruct.columns = ['date']                               # 以DataStruct时间为准
        # ScoreStruct = ScoreStruct.set_index('date')                  # 将data列为index
        TotalScore = pd.DataFrame(self.DataStruct.index)             # TotalScore记录加权后的总份数
        TotalScore = TotalScore.astype(int)                          # 转换为int
        TotalScore.columns = ['date']                                # 以DataStruct时间为准
        TotalScore = TotalScore.set_index('date')                    # 将data列为index
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        
        for i in self.instruments:
            data = self.DataStruct[[i+'_Open', i+'_High', i+'_Low', i+'_Close', i+'_Volume', i+'_OpenInterest']]
            scores = self.genfactors(data, i)
            # print(i)
            # ScoreStruct = ScoreStruct.merge(scores, how='outer', on=None, left_on=None, right_on=None, left_index=True, right_index=True)
            # for m in range(len(self.weight)):
            #     scores.iloc[:,m] = (scores.iloc[:,m]-scores.iloc[:,m].mean())/scores.iloc[:,m].std()
            TotalScore[i] = np.dot(scores.values, self.weight)       # scores*weight (1xNFct)*(NFct*1)
        # ic = pd.Series(ICvalue).dropna()
        # meanIC = np.mean(ic)
        return TotalScore

    def genfactors(self, data, name):
        dt0idx = data.index.get_loc(data.first_valid_index())        # 交易首日index
        Open = data[name+'_Open']                                    # 当日开盘价O(t)
        High = data[name+'_High']                                    # 当日最高价H(t)
        Low = data[name+'_Low']                                      # 当日最低价L(t)
        Close = data[name+'_Close']                                  # 当日收盘价C(t)
        Volume = data[name+'_Volume']                                # 当日成交量V(t)
        OpenInt = data[name+'_OpenInterest']                         # 当日持仓量I(t)
        if len(self.Lag) == len(self.mode):
            result = pd.DataFrame(data.index)                        # 取data index为首列
            result.columns = ['date']                                # 将首列记为date        
            result = result.set_index('date')                        # 将date记为index
            
            ICScore = pd.DataFrame(data.index) 
            ICScore.columns = ['date'] 
            ICScore = ICScore.set_index('date')
            
            dClose = Close.diff(1)                                   # 当日dC(t) = C(t) - C(t-1)
            rClose = Close / Close.shift(1)                          # 当日rC(t) = C(t) / C(t-1)
            Jump = Open / Close.shift(1) - 1                         # 当日跳空率 Jump(t) = O(t) / C(t-1) - 1
            CMO = Close - Open                                       # 当日涨跌幅 CMO(t) = C(t) - O(t)
            LogRt = np.log(Close / Open)                             # 当日收益率 R(t) = ln( C(t) / O(t) )
            Gain = (Close.shift(-18) - Close) / Close      # 收益率
            HML = High - Low                                         # 当日振幅度 HML(t) = H(t) - L(t)
            Vlt = High / Low - 1                                     # 当日振幅率 Volatile(t) = HML(t) / L(t)
            UpVol = Volume.where(dClose>0).fillna(0)                 # 上涨日当日成交量
            UpVol.values[0] = np.nan
            DnVol = Volume.where(dClose<=0).fillna(0)                # 下跌日当日成交量
            DnVol.values[0] = np.nan
            FundsFlow = OpenInt / OpenInt.shift(1)                   # 当日资金流向 I(t) / I(t-1)
            for i in range(0,len(self.Lag)):
                L = self.Lag[i]                                      # 该因子Lag
                Md = self.mode[i]                                    # 该因子Mode
                if i == 0:     # 0. 动量因子
                    try:
                        scores = np.log(rClose)                      # [-0.1, 0.1]
                        if Md == 'simple':
                            result[name+'_MTM_SMA'] = scores.rolling(L).mean().shift(1)
                            ICScore[name+'_gain_MTM_SMA'] = Gain
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            result[name+'_MTM_WMA'] = scores.rolling(L).apply(lambda rows: np.dot(rows, weights)/(weights.sum()), raw=True).shift(1)
                            ICScore[name+'_gain_MTM_WMA'] = Gain
                        elif Md == 'exponential':
                            result[name+'_MTM_EMA'] = scores.ewm(span=L, adjust=False).mean().shift(1)
                            ICScore[name+'_gain_MTM_EMA'] = Gain
                        else:
                            print('Please enter a valid Mode for MTM factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} MTM: {Error}'.format(Name=name, Error=e))
                            
                elif i == 1:    # 1. 平均趋势度因子:
                    try:
                        scores = CMO / HML                           # 当日趋势度 [-1, 1]
                        scores = scores.fillna(0)
                        scores.values[0:dt0idx] = np.nan
                        sumVol = Volume.rolling(L, min_periods=2).sum()
                        VWscores = (scores * Volume / sumVol) * L    # 成交量加权
                        scoresSD = VWscores.rolling(L).std()
                        if Md == 'simple':
                            scoresMA = VWscores.rolling(L).mean()
                            scores = scoresMA / scoresSD
                            result[name+'_TRD_SMA'] = scores
                            ICScore[name+'_gain_TRD_SMA'] = Gain
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            scoresMA = VWscores.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            scores = scoresMA / scoresSD
                            result[name+'_TRD_WMA'] = scores.shift(1)
                            ICScore[name+'_gain_TRD_WMA'] = Gain
                        elif Md == 'exponential':
                            scoresMA = VWscores.ewm(span=L, adjust=False).mean()
                            scores = scoresMA / scoresSD
                            result[name+'_TRD_EMA'] = scores
                            ICScore[name+'_gain_TRD_EMA'] = Gain
                        else:
                            print('Please enter a valid Mode for TRD factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} ATRD: {Error}'.format(Name=name, Error=e))
                        
                elif i == 2:    # 2. 整体趋势度因子:
                    try:
                        Trend = Close.shift(1) - Open.shift(L)             # 过去L天涨跌
                        maxHigh = High.rolling(L).max().shift(1)           # 过去L天最高价
                        minLow = Low.rolling(L).min().shift(1)             # 过去L天最低价
                        result[name+'_TTRD'] = Trend / (maxHigh - minLow)  # [-1, 1]
                        ICScore[name+'_gain_TTRD'] = Gain
                    except Exception as e:
                        print('Error in {Name} TTRD: {Error}'.format(Name=name, Error=e))
                        
                elif i == 3:    # 3. 市场强弱因子:
                    try:
                        UpChange = dClose.where(dClose > 0).fillna(0)      # 上涨日涨幅
                        UpChange.iloc[0] = np.nan
                        DnChange = dClose.where(dClose <= 0).fillna(0)     # 下跌日跌幅
                        DnChange.iloc[0] = np.nan
                        if Md == 'simple':
                            sumChange = dClose.rolling(L).sum()
                            sumUpChange = UpChange.rolling(L).sum()
                            sumDnChange = DnChange.rolling(L).sum()
                            scoresUp = sumChange.where(sumChange>0) / sumUpChange.where(sumChange>0)
                            scoresDn = sumChange.where(sumChange<=0) / abs(sumDnChange.where(sumChange<=0))
                            result[name+'_STH_SMA'] = scoresUp.fillna(scoresDn).shift(1)  # [-1, 1]
                            ICScore[name+'_gain_STH_SMA'] = Gain
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            sumChange = dClose.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            sumUpChange = UpChange.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            sumDnChange = DnChange.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            scoresUp = sumChange.where(sumChange>0) / sumUpChange.where(sumChange>0)
                            scoresDn = sumChange.where(sumChange<=0) / abs(sumDnChange.where(sumChange<=0))
                            result[name+'_STH_WMA'] = scoresUp.fillna(scoresDn).shift(1)  # [-1, 1]
                            ICScore[name+'_gain_STH_WMA'] = Gain
                        elif Md == 'exponential':
                            sumChange = dClose.ewm(span=L, adjust=False).mean()
                            sumUpChange = UpChange.ewm(span=L, adjust=False).mean()
                            sumDnChange = DnChange.ewm(span=L, adjust=False).mean()
                            scoresUp = sumChange.where(sumChange>0) / sumUpChange.where(sumChange>0)
                            scoresDn = sumChange.where(sumChange<=0) / abs(sumDnChange.where(sumChange<=0))
                            result[name+'_STH_EMA'] = scoresUp.fillna(scoresDn).shift(1)  # [-1, 1]
                            ICScore[name+'_gain_STH_EMA'] = Gain
                        else:
                            print('Please enter a valid Mode for STH factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} STH: {Error}'.format(Name=name, Error=e))
                        
                elif i == 4:    # 4. 毛刺制衡因子:
                    try:
                        Trend = Close.shift(1) - Open.shift(L)          # 过去L天涨跌
                        maxHigh = High.rolling(L).max().shift(1)        # 过去L天最高价
                        minLow = Low.rolling(L).min().shift(1)          # 过去L天最低价
                        scores_high = 1 - Close.shift(1) / maxHigh      # 回落幅度 [0,1]
                        scores_high = scores_high.where(Trend>0)        # 只在上涨时关注回落幅度
                        scores_low = 1 - Close.shift(1) / minLow        # 回升幅度 [-1,0]
                        scores_low = scores_low.where(Trend<=0)         # 只在下跌时关注回升幅度
                        result[name+'_Burr'] = scores_high.fillna(0) + scores_low.fillna(0)  # [-1, 1]
                        ICScore[name+'_gain_Burr'] = Gain
                    except Exception as e:
                        print('Error in {Name} Burr: {Error}'.format(Name=name, Error=e))
                        
                elif i == 5:    # 5. 乖离率因子:
                    try:
                        if Md == 'simple':
                            vlt_MA = Vlt.rolling(L).mean()              # 过去L天平均震荡幅度
                            Close_MA = Close.rolling(L).mean()          # 过去L天价格均线
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            vlt_MA = Vlt.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            Close_MA = Close.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                        elif Md == 'exponential':
                            vlt_MA = Vlt.ewm(span=L, adjust=False).mean()
                            Close_MA = Close.ewm(span=L, adjust=False).mean()
                        else:
                            print('Please enter a valid Mode for BIAS factor at position {k}.'.fotmat(k=i))
                        bias = (Close - Close_MA) / Close_MA            # 当日乖离率 N(0,s)
                        scores = bias / vlt_MA                          # 协整乖离率 N(0,1)
                        result[name+'_BIAS'] = scores.shift(1)
                        ICScore[name+'_gain_BIAS'] = Gain
                    except Exception as e:
                        print('Error in {Name} BIAS: {Error}'.format(Name=name, Error=e))
                        
                elif i == 6:    # 6. RSV因子:
                    try:
                        maxHigh = High.rolling(L).max().shift(1)        # 过去L天最高价
                        minLow = Low.rolling(L).min().shift(1)          # 过去L天最低价
                        result[name+'_RSV'] = (Close.shift(1) - minLow) / (maxHigh - minLow)
                        ICScore[name+'_gain_RSV'] = Gain
                    except Exception as e:
                        print('Error in {Name} RSV: {Error}'.format(Name=name, Error=e))
                    
                elif i == 7:    # 7. William因子:
                    try:
                        maxHigh = High.rolling(L).max().shift(1)        # 过去L天最高价
                        minLow = Low.rolling(L).min().shift(1)          # 过去L天最低价
                        result[name+'_William'] = (maxHigh - Close.shift(1)) / (maxHigh - minLow)
                        ICScore[name+'_gain_William'] = Gain
                    except Exception as e:
                        print('Error in {Name} William: {Error}'.format(Name=name, Error=e))
                        
                elif i == 8:    # 8. PC因子:
                    try:
                        scores = (rClose - 1) / FundsFlow                  # N(0,s)
                        ICScore[name+'_gain_PC_SMA'] = Gain
                        if Md == 'simple':
                            result[name+'_PC_SMA'] = scores.rolling(L).mean().shift(1)
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            result[name+'_PC_WMA'] = scores.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True).shift(1)
                        elif Md == 'exponential':
                            result[name+'_PC_EMA'] = scores.ewm(span=L, adjust=False).mean().shift(1)
                        else:
                            print('Please enter a valid Mode for PC factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} PC: {Error}'.format(Name=name, Error=e))
                    
                elif i == 9:    # 9. VR因子:
                    try:
                        ICScore[name+'_gain_VR'] = Gain
                        if Md == 'simple':
                            sumUpVol = UpVol.rolling(L).sum()           # 过去L天上涨日总成交量
                            sumDnVol = DnVol.rolling(L).sum()           # 过去L天下跌日总成交量
                            result[name+'_VR_SMA'] = (sumUpVol / sumDnVol - 1).shift(1)    # N(0,s)
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            sumUpVol = UpVol.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            sumDnVol = DnVol.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True)
                            result[name+'_VR_WMA'] = (sumUpVol / sumDnVol - 1).shift(1)
                        elif Md == 'exponential':
                            sumUpVol = UpVol.ewm(span=L, adjust=False).mean()
                            sumDnVol = DnVol.ewm(span=L, adjust=False).mean()
                            result[name+'_VR_EMA'] = (sumUpVol / sumDnVol - 1).shift(1)
                        else:
                            print('Please enter a valid Mode for VR factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} VR: {Error}'.format(Name=name, Error=e))
                    
                elif i == 10:    # 10. 市场情绪因子:
                    try:
                        nUp = (dClose.where(dClose>0) / dClose.where(dClose>0)) / L
                        nUp = nUp.fillna(0)
                        result[name+'_PSY'] = nUp.rolling(L).sum().shift(1)    # [0,1]
                        ICScore[name+'_gain_PSY'] = Gain
                    except Exception as e:
                        print('Error in {Name} PSY: {Error}'.format(Name=name, Error=e))
                    
                elif i == 11:    # 11. 市场能量因子:
                    try:
                        sumdClose = dClose.rolling(L).sum()             # 过去L天价格净涨跌
                        AbsdClose = abs(dClose)                         # 价格涨跌绝对值
                        sumAbsdClose = AbsdClose.rolling(L).sum()       # 过去L天价格总波动
                        energy = sumdClose / sumAbsdClose               # [0,1]
                        nUp = (dClose.where(dClose>0) / dClose.where(dClose>0)) / L
                        nUp = nUp.fillna(0)
                        PSY = nUp.rolling(L).sum()                      # [0,1]
                        scores = 0.5 * (PSY + energy)                   # [0,1]
                        result[name+'_ENGY'] = scores.shift(1)
                        ICScore[name+'_gain_ENGY'] = Gain
                    except Exception as e:
                        print('Error in {Name} ENGY: {Error}'.format(Name=name, Error=e))
                    
                elif i == 12:    # 12. 标准化动量因子:
                    try:
                        meanRt = LogRt.rolling(L).mean()                # N(0,s)
                        stdRt = LogRt.rolling(L).std()                  # s>0
                        scores = meanRt / stdRt               # N(0,1)
                        result[name+'_SMTM'] = scores.shift(1)          # N(0,1)
                        ICScore[name+'_gain_SMTM'] = Gain
                    except Exception as e:
                        print('Error in {Name} SMTM: {Error}'.format(Name=name, Error=e))
                    
                elif i == 13:    # 13. 成交量加权动量因子
                    try:
                        sumVol = Volume.rolling(L, min_periods=1).sum()
                        VWRt = LogRt * Volume / sumVol
                        ICScore[name+'_gain_VMTM'] = Gain
                        if Md == 'simple':
                            result[name+'_VMTM_SMA'] = VWRt.rolling(L).mean().shift(1)
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            result[name+'_VMTM_WMA'] = VWRt.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True).shift(1)
                        elif Md == 'exponential':
                            result[name+'_VMTM_EMA'] = VWRt.ewm(span=L, adjust=False).mean().shift(1)
                        else:
                            print('Please enter a valid Mode for VMTM factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} VMTM: {Error}'.format(Name=name, Error=e))
                    
                elif i == 14:    # 14. 趋势反转制衡因子
                    try:
                        rRt = CMO / CMO.shift(1)
                        N_switch = rRt.where(rRt<0) / rRt.where(rRt<0)
                        N_switch = N_switch.fillna(0)
                        N_switch.values[0:dt0idx+1] = np.nan
                        ICScore[name+'_gain_SWC'] = Gain
                        if Md == 'simple':
                            result[name+'_SWC_SMA'] = N_switch.rolling(L).mean().shift(1)
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            result[name+'_SWC_WMA'] = N_switch.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True).shift(1)
                        elif Md == 'exponential':
                            result[name+'_SWC_EMA'] = N_switch.ewm(span=L, adjust=False).mean().shift(1)
                        else:
                            print('Please enter a valid Mode for DMA factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} SWC: {Error}'.format(Name=name, Error=e))
                
                elif i == 15:    # 15. 跳空制衡因子
                    try:
                        Trend = Close.shift(1) - Open.shift(L)               # C(t-1) - O(t-L)
                        stdJump = Jump.rolling(L).std()                      # s>0
                        Jump = Jump / stdJump                                # 标准化跳空率
                        JumpUp = Jump.where(Jump>=0)                         # Jump(t) if Jump(t)>0
                        JumpUp = JumpUp.fillna(0)
                        JumpUp.values[0:dt0idx+1] = np.nan
                        JumpDn = Jump.where(Jump<0)                          # Jump(t) if Jump(t)<0
                        JumpDn = JumpDn.fillna(0)
                        JumpDn.values[0:dt0idx+1] = np.nan
                        ICScore[name+'_gain_Jump'] = Gain
                        if Md == 'simple':
                            JumpUp_MA = JumpUp.rolling(L).mean().shift(1)    # AvgJumpUp(t-1)
                            JumpDn_MA = JumpDn.rolling(L).mean().shift(1)    # AvgJumpDn(t-1)
                            result[name+'_Jump_SMA'] = JumpUp_MA.where(Trend>=0).fillna(0) + JumpDn_MA.where(Trend<0).fillna(0)
                        elif Md == 'linear':
                            weights = np.arange(1, L+1)
                            JumpUp_MA = JumpUp.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True).shift(1)
                            JumpDn_MA = JumpDn.rolling(L).apply(lambda row: np.dot(row, weights)/weights.sum(), raw=True).shift(1)
                            result[name+'_Jump_WMA'] = JumpUp_MA.where(Trend>=0).fillna(0) + JumpDn_MA.where(Trend<0).fillna(0)
                        elif Md == 'exponential':
                            JumpUp_MA = JumpUp.ewm(span=L, adjust=False).mean().shift(1)
                            JumpDn_MA = JumpDn.ewm(span=L, adjust=False).mean().shift(1)
                            result[name+'_Jump_EMA'] = JumpUp_MA.where(Trend>=0).fillna(0) + JumpDn_MA.where(Trend<0).fillna(0)
                        else:
                            print('Please enter a valid Mode for Jump factor at position {k}.'.fotmat(k=i))
                    except Exception as e:
                        print('Error in {Name} Jump: {Error}'.format(Name=name, Error=e))
            
                else:
                    pass
            
            result.values[0:dt0idx+max(self.Lag)+1] = np.nan    # maxLag之前分数清零以便计算最后总分
            ICScore.values[0:dt0idx+max(self.Lag)+1] = np.nan
        else:
            print('Please make sure Lags and Modes have the same length of inputs.')
            result = None
            ICScore = None

        return result



