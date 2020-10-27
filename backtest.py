# -*- coding: utf-8 -*-
"""
Created on Thu May 28 15:31:47 2020

@author: wenrui zhang zhaobei
"""

import numpy as np
import pandas as pd

class DailyTrading(object):

    def __init__(self, DataStruct, TotalScore, T0, Tn, instruments, Sizes, Ticks, I, fBuy, fSell, NBuy, NSell, fee, Mode):
        self.DataStruct = DataStruct.loc[T0:Tn]                         # 回测样本
        self.TotalScore = TotalScore.loc[T0:Tn]                         # 各品种评分
        self.startDate = T0                                             # 起始日期
        self.dailyRank = self.TotalScore.rank(axis=1, method='dense', ascending=False)  # 每日排名
        self.instruments = instruments                                  # 被选品种
        self.Sizes = Sizes                                              # 合约乘数
        self.Ticks = Ticks                                              # 最小变动价位
        self.I = I                                                      # 账户初始资金
        self.fBuy = fBuy                                                # 多仓资金(市值)
        self.fSell = fSell                                              # 空仓资金(市值)
        self.NBuy = NBuy                                                # 做多资产数
        self.NSell = NSell                                              # 做空资产数
        self.fee = fee                                                  # 交易滑点(手续费)
        self.Mode = Mode                                                # 多(空)仓资金分配模式


    def price(self, t, i, direction):
        Pt = self.DataStruct.loc[t, i+'_RealOpen']                      # 该品种主力合约当日开盘价
        if direction == 'buy':
            Pt = Pt + self.fee * self.Ticks[i]                          # 买入, 则实际成本 > Pt
        elif direction == 'sell':
            Pt = Pt - self.fee * self.Ticks[i]                          # 卖出, 则实际成本 < Pt
        else:
            print('Please enter either buy or sell')
        
        return Pt


    def tradings(self):
        account = pd.DataFrame(self.DataStruct.index)                   # 绘制账户资产明细
        account.columns = ['date']                                      # 以T0为起始时间
        account = account.set_index('date')                             # 将日期作为索引index
        account['remMV'] = self.fBuy + self.fSell                       # 初始化剩余仓位市值为fBuy+fSell
        account['Wealth'] = self.I                                      # 初始化资产价值为I
        account['PnL'] = 0                                              # 初始化PnL为0
        
        for i in range(1, self.NBuy+1):
            account['Long_'+str(i)] = ''                                # 初始化Long品种
            account['Long_'+str(i)+'_Pos'] = np.nan                     # 初始化Long品种手数
            account['Long_'+str(i)+'_Vt'] = np.nan                      # 初始化Long品种市值
        
        for i in range(1, self.NSell+1):
            account['Short_'+str(i)] = ''                               # 初始化Short品种
            account['Short_'+str(i)+'_Pos'] = np.nan                    # 初始化Short品种手数
            account['Short_'+str(i)+'_Vt'] = np.nan                     # 初始化Short品种市值
        
        # for i in self.instruments:
        #     account[i+'_Pt'] = self.DataStruct[i+'_RealOpen']       # 记录各品种主力合约每日开盘价
        
        win = 0                                                         # 当日账面盈利则win+1
        loss = 0                                                        # 当日账面亏损则loss+1
        VLong = 0                                                       # 当日多仓市值
        VShort = 0                                                      # 当日空仓市值
        Longs = {}                                                      # 当日做多品种手数
        Shorts = {}                                                     # 当日做空品种手数
        
        for t in self.DataStruct.index:                                 # 以交易日期为for loop索引:
            rankt = self.dailyRank.loc[t].dropna()                      # 当日可交易品种分数排名
            rankt_dict = rankt.to_dict()                                # 以字典形式保存
            rankt = sorted(rankt_dict.items(), key=lambda d:d[1])       # 按名次排序
            
            # 1. 如果当日该品种排名前NBuy, 则做多主力合约:
            long_t = [rankt[i][0] for i in range(0,self.NBuy)]
            
            # 2. 如果当日该品种排名后NSell, 则做空主力合约:
            short_t = [rankt[i][0] for i in range(len(rankt)-self.NSell, len(rankt))]
            
            if t == self.startDate:                                     # 交易起始日
                for i in long_t:                                        # 当日买入合约
                    i_rank = str(int(rankt_dict[i]))                    # 记录当日排名
                    account.loc[t,'Long_'+i_rank] = i                   # 添加进相应Long位置
                    if self.Mode == 'equal':
                        fund = self.fBuy / self.NBuy                    # 等额分配资金
                    elif self.Mode == 'differ':
                        fund = self.fBuy[int(i_rank)]                   # 差额分配资金(资金必须已预分配好)
                    else:
                        print('Please enter a valid fund mode')
                        continue
                        
                    i_Pt = self.price(t, i, 'buy')                      # 该品种当日实际买入价格
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    i_n = int(fund / (i_Pt * i_size))                   # 该品种最大开仓手数
                    i_V = i_n * i_Pt * i_size                           # 该品种当日市值
                    Longs[i] = i_n                                      # 记录该品种手数
                    VLong += i_V                                        # 统计多仓市值
                    account.loc[t,'Long_'+i_rank+'_Pos'] = i_n          # 添加进相应Long_Pos位置
                    account.loc[t,'Long_'+i_rank+'_Vt'] = i_V           # 添加进相应Long_Vt位置
                    account.loc[t,'remMV'] -= i_V                       # 剩余仓位市值
                        
                for i in short_t:                                       # 当日卖出合约
                    i_rank = str(int(len(rankt) - rankt_dict[i]) + 1)   # 记录当日排名
                    account.loc[t,'Short_'+i_rank] = i                  # 添加进相应Short位置
                    if self.Mode == 'equal':
                        fund = self.fSell / self.NSell                  # 等额分配资金
                    elif self.Mode == 'differ':
                        fund = self.fSell[int(i_rank)]                  # 差额分配资金(资金必须已预分配好)
                    else:
                        print('Please enter a valid fund mode')
                        continue
                        
                    i_Pt = self.price(t, i, 'sell')                     # 该品种当日实际卖出价格
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    i_n = - int(fund / (i_Pt * i_size))                 # 该品种最大开仓手数
                    i_V = - i_n * i_Pt * i_size                         # 该品种当日市值
                    Shorts[i] = i_n                                     # 记录该品种手数
                    VShort += i_V                                       # 统计空仓市值
                    account.loc[t,'Short_'+i_rank+'_Pos'] = i_n         # 添加进相应Short_Pos位置
                    account.loc[t,'Short_'+i_rank+'_Vt'] = i_V          # 添加进相应Short_Vt位置
                    account.loc[t,'remMV'] -= i_V                       # 剩余仓位市值
                    
            else:                                                       # 其他交易日期
                oldVLong = VLong                                        # 上一日多仓市值
                oldVShort = VShort                                      # 上一日空仓市值
                oldLongs = Longs                                        # 上一日多仓
                oldShorts = Shorts                                      # 上一日空仓
                oldVLongPV = 0                                          # 上一日多仓现市值
                oldVShortPV = 0                                         # 上一日空仓现市值
                VLong = 0                                               # 当日多仓市值
                VShort = 0                                              # 当日空仓市值
                Longs = {}                                              # 当日做多品种手数
                Shorts = {}                                             # 当日做空品种手数
                for i in long_t:                                        # 当日买入合约
                    i_rank = str(int(rankt_dict[i]))                    # 记录当日排名
                    account.loc[t,'Long_'+i_rank] = i                   # 添加进相应Long位置
                    if self.Mode == 'equal':
                        fund = self.fBuy / self.NBuy                    # 等额分配资金
                    elif self.Mode == 'differ':
                        fund = self.fBuy[int(i_rank)]                   # 差额分配资金(资金必须已预分配好)
                    else:
                        print('Please enter a valid fund mode')
                        continue
                        
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    if i in oldLongs:                                   # 如果上一日已有持仓
                        i_n = oldLongs[i]                               # 当日持仓手数不变
                        i_Pt = self.DataStruct.loc[t, i+'_RealOpen']    # 该品种当日实际价格
                    else:                                               # 如果上一日没有持仓
                        i_Pt = self.price(t, i, 'buy')                  # 该品种当日实际买入价格
                        i_n = int(fund / (i_Pt * i_size))               # 该品种最大开仓手数
                        
                    i_V = i_n * i_Pt * i_size                           # 该品种当日市值
                    Longs[i] = i_n                                      # 记录该品种手数
                    VLong += i_V                                        # 统计多仓市值
                    account.loc[t,'Long_'+i_rank+'_Pos'] = i_n          # 添加进相应Long_Pos位置
                    account.loc[t,'Long_'+i_rank+'_Vt'] = i_V           # 添加进相应Long_Vt位置
                    account.loc[t,'remMV'] -= i_V                       # 剩余仓位市值
                        
                for i in short_t:                                       # 当日卖出合约
                    i_rank = str(int(len(rankt) - rankt_dict[i]) + 1)   # 记录当日排名
                    account.loc[t,'Short_'+i_rank] = i                  # 添加进相应Short位置
                    if self.Mode == 'equal':
                        fund = self.fSell / self.NSell                  # 等额分配资金
                    elif self.Mode == 'differ':
                        fund = self.fSell[int(i_rank)]                  # 差额分配资金(资金必须已预分配好)
                    else:
                        print('Please enter a valid fund mode')
                        continue
                        
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    if i in oldShorts:                                  # 如果上一日已有持仓
                        i_n = oldShorts[i]                              # 当日持仓手数不变
                        i_Pt = self.DataStruct.loc[t, i+'_RealOpen']    # 该品种当日实际价格
                    else:                                               # 如果上一日没有持仓
                        i_Pt = self.price(t, i, 'sell')                 # 该品种当日实际卖出价格
                        if np.isnan(i_Pt):
                            print(i, t)
                            continue
                        i_n = - int(fund / (i_Pt * i_size))             # 该品种最大开仓手数
                        
                    i_V = - i_n * i_Pt * i_size                         # 该品种当日市值
                    Shorts[i] = i_n                                     # 记录该品种手数
                    VShort += i_V                                       # 统计空仓市值
                    account.loc[t,'Short_'+i_rank+'_Pos'] = i_n         # 添加进相应Short_Pos位置
                    account.loc[t,'Short_'+i_rank+'_Vt'] = i_V          # 添加进相应Short_Vt位置
                    account.loc[t,'remMV'] -= i_V                       # 剩余仓位市值
                    
                for i, i_n in oldLongs.items():                         # 以上一日多仓品种for loop索引:
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    if i in Longs:                                      # (1) 如果当日依然有持仓
                        i_Pt = self.DataStruct.loc[t, i+'_RealOpen']    #     该品种当日实际价格
                    else:                                               # (2) 如果当日不再有持仓
                        i_Pt = self.price(t, i, 'sell')                 #     该品种当日实际卖出价格
                        if np.isnan(i_Pt):
                            print(i, t)
                            continue
                    oldVLongPV += i_n * i_Pt * i_size                   # 上一日多仓现市值
                
                for i, i_n in oldShorts.items():                        # 以上一日空仓品种for loop索引:
                    i_size = self.Sizes[i]                              # 该品种合约乘数
                    if i in Shorts:                                     # (1) 如果当日依然有持仓
                        i_Pt = self.DataStruct.loc[t, i+'_RealOpen']    #     该品种当日实际价格
                    else:                                               # (2) 如果当日不再有持仓
                        i_Pt = self.price(t, i, 'buy')                  #     该品种当日实际买入价格
                        if np.isnan(i_Pt):
                            print(i, t)
                            continue
                    oldVShortPV -= i_n * i_Pt * i_size                  # 上一日空仓现市值
                
                longPnL = oldVLongPV - oldVLong                         # 当日多仓盈亏
                shortPnL = oldVShort - oldVShortPV                      # 当日空仓盈亏
                account.loc[t,'PnL'] = longPnL + shortPnL               # 当日净盈亏
                account.loc[t,'Wealth'] = account.shift(1).loc[t,'Wealth'] + longPnL + shortPnL
                
                if np.isnan(longPnL+shortPnL):
                    print(t, account.loc[t])
                
                if longPnL + shortPnL > 0:                              # 如果当天盈利,则判赢
                    win += 1                                            # 统计盈利天数
                else:                                                   # 如果当天亏损,则判输
                    loss += 1                                           # 统计亏损天数
                
        win_rate = win / (win + loss)                                   # 交易胜率
        return account, win_rate

