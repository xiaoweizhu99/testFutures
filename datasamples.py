# -*- coding: utf-8 -*-
"""
Created on Mon May 11 11:39:51 2020

@author: wenrui zhang
"""

import pandas as pd
import glob
import re
import datetime
import mysql.connector as db
from sqlalchemy import create_engine


class FuturesData(object):

    def __init__(self, host, user, password, db_name, port, instruments, datapath='E:\\Strategies\\TB_Data'):
        self.MySQL_HOST = host
        self.MySQL_USER = user
        self.MySQL_PASSWORD = password
        self.MySQL_DBNAME = db_name
        self.MySQL_PORT = port
        self.instruments = instruments
        self.DATAPATH = datapath
        self.mydb = db.connect(
                               host=self.MySQL_HOST,
                               user=self.MySQL_USER,
                               password=self.MySQL_PASSWORD,
                               db=self.MySQL_DBNAME,
                               port=self.MySQL_PORT,
                               auth_plugin='mysql_native_password'
                               )
        self.mycursor = self.mydb.cursor()

#%% 从数据库中下载数据
    def fetching(self, stock_index='IF'):
        todaydate = datetime.date.today()
        weekday = datetime.date.isoweekday(todaydate)
        if weekday == 5:
            tomorrowdate = todaydate + datetime.timedelta(days=3)
        else:
            tomorrowdate = todaydate + datetime.timedelta(days=1)
        tomorrowday = tomorrowdate.day
        tomorrowmonth = tomorrowdate.month
        tomorrowyear = tomorrowdate.year
        tomorrow = int(tomorrowyear*10000+tomorrowmonth*100+tomorrowday)
        
        # 0. Build MySQL connection:
        db_connection_str = 'mysql+mysqlconnector://root:SHzb1234@106.14.96.31/future_data'
        db_connection = create_engine(db_connection_str,connect_args={'auth_plugin': 'mysql_native_password'})
        
        # 1. Fetching stock index futures DataFrame
        df = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + stock_index + '000" AND date >= "2011-01-04" ORDER BY date asc', con=db_connection)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y%m%d').astype(int)
        
        newrow = {
                  'date': tomorrow,
                  'open': df['close'].values[-1],
                  'high': 1,
                  'low': 1,
                  'close': 1,
                  'vol': 1,
                  'ccl': 1
                  }
        df = df.append(newrow, ignore_index=True)
        df['date'] = df['date'].astype(int)
        df = df.set_index('date')
        df.columns = [stock_index+'_Open',
                      stock_index+'_High',
                      stock_index+'_Low',
                      stock_index+'_Close',
                      stock_index+'_Volume',
                      stock_index+'_OpenInterest']
        
        # 2. Fetching commodity futures DataFrame
        contracts = self.instruments
        for i in contracts:
            if i == 'IF' or i == 'IC' or i == 'IH':
                continue
            if i == 'au' or i == 'b' or i == 'RI' or i == 'rr' or i == 'WH' or i == 'wr' :
                continue
            
            # 2.1 contracts index data:
            if len(i) > 1 and i != 'bu' and i != 'OI' and i != 'SF' and i != 'SM' and i != 'ZC':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2011-01-04" ORDER BY date asc', con=db_connection)
            elif len(i) > 1 and i == 'bu':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2015-01-05" ORDER BY date asc', con=db_connection)
            elif len(i) > 1 and i == 'OI':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2013-01-04" ORDER BY date asc', con=db_connection)
            elif len(i) > 1 and i == 'SF':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2016-08-01" ORDER BY date asc', con=db_connection)
            elif len(i) > 1 and i == 'SM':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2016-07-25" ORDER BY date asc', con=db_connection)
            elif len(i) > 1 and i == 'ZC':
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'000" and date >= "2015-08-03" ORDER BY date asc', con=db_connection)
            else:
                df_index = pd.read_sql('SELECT date, open, high, low, close, vol, ccl FROM future_day WHERE name = "' + i +'9000" and date >= "2011-01-04" ORDER BY date asc', con=db_connection)
            
            # contracts index date column as primary index:
            df_index['date'] = pd.to_datetime(df_index['date'])
            df_index['date'] = df_index['date'].dt.strftime('%Y%m%d').astype(int)
            
            newrow = {
                      'date': tomorrow,
                      'open': df_index['close'].values[-1],
                      'high': 1,
                      'low': 1,
                      'close': 1,
                      'vol': 1,
                      'ccl': 1
                      }
            df_index = df_index.append(newrow, ignore_index=True)
            df_index['date'] = df_index['date'].astype(int)
            df_index = df_index.set_index('date')
            
            # 2.2 main contracts data:
            df_main = pd.read_sql('SELECT trading_day, open_price, close_price FROM future_all_day_major_cleaned WHERE product_id = "' + i +'" and trading_day >= "2011-01-01" ORDER BY trading_day asc', con=db_connection)
            
            # main contracts date column as primary index:
            df_main['trading_day'] = pd.to_datetime(df_main['trading_day'])
            df_main['trading_day'] = df_main['trading_day'].dt.strftime('%Y%m%d').astype(int)
            
            newrow = {
                      'trading_day': tomorrow,
                      'open_price': df_main['close_price'].values[-1],
                      'close_price': 1
                      }
            df_main = df_main.append(newrow, ignore_index=True)
            df_main['trading_day'] = df_main['trading_day'].astype(int)
            df_main = df_main.set_index('trading_day')
            
            # 2.3 joining two data samples:
            dfi = df_index.join(df_main, how = 'inner')
            dfi.columns = [i+'_Open',
                           i+'_High',
                           i+'_Low',
                           i+'_Close',
                           i+'_Volume',
                           i+'_OpenInterest',
                           i+'_RealOpen',
                           i+'_RealClose']
            
            # 2.4 merging DataStruct
            df = df.join(dfi, how = 'outer')
            
        # 3. Fetching basic info:
        sql = "SELECT name, volume_multiple, price_tick from future_data.future_info;"
        self.mycursor.execute(sql)
        dta = self.mycursor.fetchall()
        sizes = {}
        ticks = {}
        for (name, contract_size, price_tick) in dta:
            sizes[name] = contract_size
            ticks[name] = price_tick
        
        return df, sizes, ticks

#%% 当日主力合约
    def get_major(self, instrument):
        sql = "SELECT name FROM future_main_date_combined WHERE enddate IS NULL AND type='{i}';".format(i=instrument)
        self.mycursor.execute(sql)
        major = self.mycursor.fetchone()
        return major[0]

#%%
    def construct(self):
        datapath = self.DATAPATH
        datafiles = glob.glob(datapath + "\\*.csv")
        DataStruct = {}
        DateList = pd.DataFrame()
        todaydate = datetime.date.today()
        weekday = datetime.date.isoweekday(todaydate)
        if weekday == 5:
            tomorrowdate = todaydate + datetime.timedelta(days=3)
        else:
            tomorrowdate = todaydate + datetime.timedelta(days=1)
        tomorrowday = tomorrowdate.day
        tomorrowmonth = tomorrowdate.month
        tomorrowyear = tomorrowdate.year
        tomorrow = tomorrowyear*10000+tomorrowmonth*100+tomorrowday
        for file in datafiles:
            b = re.search(r"(?<=vsc.).*?(?=\\)",file[::-1]).group(0)
            name = b[::-1]
            name = name.replace(re.findall(r'\d+', name)[0], '')
            df = pd.read_csv(file, index_col=None, header=None)
            df.columns = ['Date', 'Time', name+'_Open', name+'_High', name+'_Low', name+'_Close', name+'_Volume', name+'_OpenInterest', name+'_RealOpen', name+'_RealClose']
            newrow = {'Date': tomorrow,
                      'Time': 0,
                      'Open': df['Close'].values[-1],
                      'High': 1,
                      'Low': 1,
                      'Close': 1,
                      'Volume': 1,
                      'OpenInterest': 1,
                      'RealOpen': df['RealClose'].values[-1],
                      'RealClose': 1}
            df = df.append(newrow, ignore_index=True)
            df = df.set_index('Date')
            DataStruct[name] = df
            
            # for date in idatelist:
            #     try:
            #         idx = DateList.index(date)
            #     except ValueError:
            #         DateList.append(date)
            
            
        DateList.sort()
        for i in range(0, len(DateList)):
            date = DateList[i]
            for key, data in DataStruct.items():
                idatelist = data.iloc[:,0].tolist()
                if date in idatelist:
                    ti = idatelist.index(date)
                    V_t = data.values[ti,6]
                    if V_t <= 200 and ti >= 10:
                        V_t9 = data.values[ti-1,6]
                        V_t8 = data.values[ti-2,6]
                        V_t7 = data.values[ti-3,6]
                        V_t6 = data.values[ti-4,6]
                        # V_t5 = data.values[ti-5,6]
                        # V_t4 = data.values[ti-6,6]
                        # V_t3 = data.values[ti-7,6]
                        # V_t2 = data.values[ti-8,6]
                        # V_t1 = data.values[ti-9,6]
                        if (V_t6 <= 100 and V_t7 <= 100 and
                            V_t8 <= 100 and V_t9 <= 100):
                            DataStruct[key] = DataStruct[key][ti+1:]
                            DataStruct[key] = DataStruct[key].sort_values(by=0)
                else:
                    if i > 0 and DateList[i-1] in idatelist:
                        ti = idatelist.index(DateList[i-1])
                        newrow = {0:date, 1:0, 2:data.values[ti,2], 3:data.values[ti,3], 4:data.values[ti,4], 5:data.values[ti,5], 6:data.values[ti,6], 7:data.values[ti,7], 8:data.values[ti,8], 9:data.values[ti,9]}
                        DataStruct[key] = DataStruct[key].append(newrow, ignore_index=True)
                        DataStruct[key] = DataStruct[key].sort_values(by=0)

        return DataStruct, DateList

#%%
    def addparams(self, name, dataframe):
        T = len(dataframe)
        if name == 'a9000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'ag000':
            multiplier = [15] * T
            minmove = [1] * T
            
        if name == 'al000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'AP000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'au000':
            multiplier = [1000] * T
            minmove = [0.05] * T
            
        if name == 'b9000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'bb000':
            multiplier = [500] * T
            minmove = [0.05] * T
            
        if name == 'bu000':
            multiplier = [10] * T
            minmove = [2] * T
            
        if name == 'c9000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'CF000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'CJ000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'cs000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'cu000':
            multiplier = [5] * T
            minmove = [10] * T
            
        if name == 'CY000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'eb000':
            multiplier = [5] * T
            minmove = [1] * T
            
        if name == 'eg000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'fb000':
            multiplier = [10] * T
            minmove = [5] * T
            
        if name == 'FG000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'fu000':
            ds = 20170925
            dtlist = dataframe.iloc[:,0].tolist()
            di = dtlist.index(ds)
            dataframe = dataframe[di+1:]
            T = len(dataframe)
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'hc000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'i9000':
            multiplier = [100] * T
            minmove = [0.5] * T
            
        if name == 'j9000':
            multiplier = [100] * T
            minmove = [0.5] * T
            
        if name == 'jd000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'jm000':
            multiplier = [60] * T
            minmove = [0.5] * T
            
        if name == 'JR000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'l9000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'LR000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'm9000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'MA000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'ni000':
            multiplier = [1] * T
            minmove = [10] * T
            
        if name == 'nr000':
            multiplier = [10] * T
            minmove = [5] * T
            
        if name == 'OI000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'p9000':
            multiplier = [10] * T
            minmove = [2] * T
            
        if name == 'pb000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'pg000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'PM000':
            multiplier = [50] * T
            minmove = [1] * T
            
        if name == 'pp000':
            multiplier = [5] * T
            minmove = [1] * T
            
        if name == 'rb000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'RI000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'RM000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'rr000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'RS000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'ru000':
            multiplier = [10] * T
            minmove = [5] * T
            
        if name == 'SA000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'sc000':
            multiplier = [1000] * T
            minmove = [0.1] * T
            
        if name == 'SF000':
            multiplier = [5] * T
            minmove = [2] * T
            
        if name == 'SM000':
            multiplier = [5] * T
            minmove = [2] * T
            
        if name == 'sn000':
            multiplier = [1] * T
            minmove = [10] * T
            
        if name == 'sp000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'SR000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'ss000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'TA000':
            multiplier = [5] * T
            minmove = [2] * T
            
        if name == 'UR000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'v9000':
            multiplier = [5] * T
            minmove = [5] * T
            
        if name == 'WH000':
            multiplier = [20] * T
            minmove = [1] * T
            
        if name == 'wr000':
            multiplier = [10] * T
            minmove = [1] * T
            
        if name == 'y9000':
            multiplier = [10] * T
            minmove = [2] * T
            
        if name == 'ZC000':
            multiplier = [100] * T
            minmove = [0.2] * T
            
        if name == 'zn000':
            multiplier = [5] * T
            minmove = [5] * T
            
        dataframe.loc[:,8] = pd.Series(multiplier)
        dataframe.loc[:,9] = pd.Series(minmove)
        return dataframe

