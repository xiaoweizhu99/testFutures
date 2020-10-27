# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 16:51:39 2020

@author: wenrui zhang zhaobei
"""

import numpy as np

class PerformMeasures(object):

    def __init__(self, I, Values, rf, frequency='day'):
        self.I = I                                  # initial investment
        self.Values = Values                        # assets values
        self.rf = rf                                # risk-free interest rate
        self.frequency = frequency
        if self.frequency == 'year':                # yearly
            self.n = 1
        elif self.frequency == 'quarter':           # quarterly
            self.n = 4
            self.Values = Values
        elif self.frequency == 'month':             # monthly
            self.n = 12
        elif self.frequency == 'week':              # weekly
            self.n = 52
        elif self.frequency == 'day':               # daily
            self.n = 252
        elif self.frequency == 'hour':              # hourly
            self.n = 252 * 24
        else:
            print('Please enter a correct frequency.')
            pass
        self.T = len(self.Values)

    def Sharpe(self):
        T = self.T                                  # investment period
        Rt = self.Values / self.I - 1
        Rs = []
        for ti in range(0, T):
            ARt = Rt[ti] / ((ti+1)/self.n)
            Rs.append(ARt)
        Rss = np.array(Rs)
        sigma = np.std(Rss[44:])
        Sharpe = (Rs[-1] - self.rf) / sigma
        return Rs[-1], sigma, Sharpe

    def MAR(self, Rt):
        Values = self.Values
        T = self.T
        drawdown = [0]
        for ti in range(1, T):
            peak = max( Values[0:ti+1] )
            ddt = min( drawdown[ti-1], Values[ti] / peak - 1 )
            drawdown.append(ddt)
        maxDrawdown = abs(drawdown[-1])
        MAR = Rt / maxDrawdown
        return maxDrawdown, MAR

    def highwatermark(self):
        Values = self.Values
        hwm = Values.max()
        nt = np.argmax(Values)
        return hwm, nt





