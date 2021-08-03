import akshare as ak
import pandas as pd
import numpy as np
import stockstats
import datetime
from sqlalchemy import create_engine
import time
engine=create_engine("mysql+pymysql://root:21180294@localhost:3306/akshareChinaAdata")


# 在k线基础上计算MACD，并将结果存储在df上面(dif,dea,bar)
def calc_macd(df, fastperiod=12, slowperiod=26, signalperiod=9):
    ewma12 = df['close'].ewm(alpha=2 / 13,adjust=False).mean()
    ewma26 = df['close'].ewm(alpha=2 / 27,adjust=False).mean()
    df['dif'] = ewma12-ewma26
    df['dea'] = df['dif'].ewm(alpha=2 / 10,adjust=False).mean()
    df['bar'] = (df['dif']-df['dea'])*2
    # df['macd'] = 0
    # series = df['dif']>0
    # df.loc[series[series == True].index, 'macd'] = 1
    return df

# 在k线基础上计算KDF，并将结果存储在df上面(k,d,j)
def calc_kdj(df):
    low_list = df['low'].rolling(9, min_periods=9).min()
    low_list.fillna(value=df['low'].expanding().min(), inplace=True)
    high_list = df['high'].rolling(9, min_periods=9).max()
    high_list.fillna(value=df['high'].expanding().max(), inplace=True)
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['k'] = pd.DataFrame(rsv).ewm(alpha=1/3, adjust=False).mean()
    df['d'] = df['k'].ewm(alpha=1/3, adjust=False).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']
    # df['kdj'] = 0
    # series = df['k']>df['d']
    # df.loc[series[series == True].index, 'kdj'] = 1
    # # df.loc[series[(series == True) & (series.shift() == False)].index, 'kdjcross'] = 1
    # # df.loc[series[(series == False) & (series.shift() == True)].index, 'kdjcross'] = -1
    return df

# 在k线基础上计算DMA，并将结果存储在df上面(DDD，DDDMA)
def calc_dma(df):
    df['dma1'] = df['close'].rolling(10).mean()
    df['dma2'] = df['close'].rolling(50).mean()
    df['ddd'] = df['dma1'] - df['dma2']
    df['dddma'] = df['ddd'].rolling(10).mean()
    return df

def calc_ma(df):
    df['ma5'] = df['close'].rolling(5).mean().fillna(method='bfill')
    df['ma10'] = df['close'].rolling(10).mean().fillna(method='bfill')
    df['ma20'] = df['close'].rolling(20).mean().fillna(method='bfill')
    df['ma5'] = df['ma5'].astype('float64')
    df['ma10'] = df['ma10'].astype('float64')
    df['ma20'] = df['ma20'].astype('float64')
    return df

#-------------START!
nowdate = datetime.datetime.now()
newdate = nowdate.strftime('%Y%m%d')
now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

process=0 #进度计数
#usstockcode=pd.DataFrame()
#usstockcode= ak.get_us_stock_name() #取所有美国的代码
#print(usstockcode)
stockcode=pd.read_csv('akshareChinaAstockcode.csv')
stockcode.rename(columns={"代码":'code'},inplace=True)
stockcode['code'].astype('string')
print('stockcode',stockcode)
unliststockdf=pd.DataFrame(columns={'code'})

with open('akshareChinaAdatalastcode.txt','r') as fi:
    f1= fi.readline()
    print(f1)
if f1!='301039':
    lastcode = int(f1) #最后一个股票代码
    stockcode.set_index(['code'],inplace=True)
    stockcode=stockcode.loc[lastcode:]
    stockcode=stockcode.reset_index()
    print('断点同步：'+str(len(stockcode)))

else:
    print("数据更新从头开始")

for code in stockcode['code']:
    if code<10:scode='00000'+str(code)
    if code>10 and code<100:scode='0000'+str(code)
    if code>100 and code<1000:scode='000'+str(code)
    if code>1000 and code<10000:scode='00'+str(code)
    if code>10000 and code<100000:scode='0'+str(code)
    if code>100000:scode=str(code)
    print('读akshare接口 '+scode)

    process=process+1
    stockdailydf= ak.stock_zh_a_hist(symbol=scode,adjust="",start_date='20210301')

    stockdailydf.rename(columns={"开盘": "open", "收盘": "close","最高":"high","最低":"low","日期":"date"}, inplace=True)

    stockdailydf = stockdailydf.reset_index()
    if stockdailydf.empty== True: continue
    d = str(stockdailydf.loc[len(stockdailydf)-1,'date'])
    tempd=d.split('-',2)[0]
    if int(tempd)<2021:
        unliststockdf.loc[len(unliststockdf), 'code'] = code
        continue

    calc_macd(stockdailydf)  # 计算MACD值，数据存于DataFrame中
    calc_kdj(stockdailydf)  # 计算KDJ值，数据存于DataFrame中
    calc_dma(stockdailydf)  # 计算DMA值，数据存于DF中
    calc_ma(stockdailydf)  # 计算MA均线

    # 引入stockstats，计算CR、RSI指标
    stock = stockstats.StockDataFrame.retype(stockdailydf)
    stock.get('cr')
    stock.get('boll')
    stock.get('rsi_6')
    stock.get('rsi_12')

    stockdailydf[np.isinf(stockdailydf)] = np.nan #判断有inf替换nan！！！

    stockdailydf = stockdailydf.reset_index(drop=False)

    tablename = str(scode)
    has_table = engine.dialect.has_table(engine.connect(), tablename)
    if has_table == False:
        # 这里要判断表不存在创建新表 create table if not exists
        # 获得行情数据 ts_code, dataframe清洗数据
        stockdailydf.to_sql(name=str(scode), con=engine)  # df大表存入数据库akshareChinaA
        print('新建' + str(scode) + '数据至' + str(newdate) + '===>进度 ' + str(
            format(process / len(stockcode), '.4%')))

    else:  # 表存在，判断是否需要更新
        SQLSelectExist = 'SELECT * FROM akshareChinaAdata.`' + tablename + '`'  # 取到已存在的表里的数据
        dfExist = pd.read_sql_query(SQLSelectExist, engine)
        if stockdailydf.loc[len(stockdailydf) - 1, 'date'] in (dfExist['date'].values) :  # 如果接口取到df表的最后一行在数据库的Date列里
            print(str(scode) + '已经是最新数据===>进度 ' + str(format(process / len(stockcode), '.4%')))
            #stockdailydf.to_sql(name=code, con=engine, if_exists='replace')  # df大表存入数据库ACSQuant
        else:
            print(str(scode) + '添加新数据' + newdate + '数据--最新日期' + newdate + '===>进度 ' + str(
                format(process / len(stockcode), '.4%')))
            stockdailydf.to_sql(name=str(scode), con=engine, if_exists='replace')  # df大表存入数据库ACSQuant
    print(code, stockdailydf)
    with open('akshareChinaAdatalastcode.txt','w+') as f:
        f.write(str(scode))
unliststockdf.to_csv('akshareChinaAunlist.csv')
