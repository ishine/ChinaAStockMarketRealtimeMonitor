import akshare as ak
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:21180294@localhost:3306/ACSQuant")
from datetime import datetime
import stockstats
import time


def calc_macd(df, fastperiod=12, slowperiod=26, signalperiod=9):
    df['最新价'] = df['close'].astype(float)
    ewma12 = df['close'].ewm(alpha=2 / 13, adjust=False).mean()
    ewma26 = df['close'].ewm(alpha=2 / 27, adjust=False).mean()
    df['dif'] = ewma12 - ewma26
    df['dea'] = df['dif'].ewm(alpha=2 / 10, adjust=False).mean()
    df['bar'] = (df['dif'] - df['dea']) * 2
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
    df['k'] = pd.DataFrame(rsv).ewm(alpha=1 / 3, adjust=False).mean()
    df['d'] = df['k'].ewm(alpha=1 / 3, adjust=False).mean()
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
    df['ma20'] = df['close'].rolling(10).mean().fillna(method='bfill')
    df['ma25'] = df['close'].rolling(25).mean().fillna(method='bfill')
    df['ma55'] = df['close'].rolling(55).mean().fillna(method='bfill')
    df['ma60'] = df['close'].rolling(60).mean().fillna(method='bfill')
    df['ma200'] = df['close'].rolling(200).mean().fillna(method='bfill')
    df['ma5'] = df['ma5'].astype('float64')
    df['ma20'] = df['ma20'].astype('float64')
    df['ma25'] = df['ma25'].astype('float64')
    df['ma55'] = df['ma55'].astype('float64')
    df['ma60'] = df['ma60'].astype('float64')
    df['ma200'] = df['ma200'].astype('float64')
    return df


def calc_mavol(df):
    df['mavol5'] = df['volume'].rolling(5).mean().fillna(method='bfill')
    df['mavol60'] = df['volume'].rolling(60).mean().fillna(method='bfill')
    df['mavol5'] = df['mavol5'].astype('float64')
    df['mavol60'] = df['mavol60'].astype('float64')
    return df


def judgepe(dfstockhisdata, tablename):
    index = 0
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'] != None and dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'] < \
            dfstockhisdata['pe'].min():
        print(str(nowdate) + '  ' + tablename + '  pe历史最低')
        index = 1
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'] != None and dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'pe'] < dfstockhisdata.loc[(len(dfstockhisdata) - 90):(len(dfstockhisdata) - 1),
                                         'pe'].min():
        print(str(nowdate) + '  ' + tablename + '  pe90天最低')
        index = 2
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'] != None and dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'pe'] < dfstockhisdata.loc[(len(dfstockhisdata) - 30):(len(dfstockhisdata) - 1),
                                         'pe'].min():
        print(str(nowdate) + '  ' + tablename + '  pe30天最低')
        index = 3
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'] != None and dfstockhisdata.loc[
                                                                     len(dfstockhisdata) - 8:len(dfstockhisdata) - 1,
                                                                     'pe'].min() < dfstockhisdata['pe'].min():
        print(str(nowdate) + '  ' + tablename + '   最近7天pe平均值低于历史平均值')
        index = 4
    return index


def judgemacd(dfstockhisdata, tablename):
    index = 0
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < dfstockhisdata['dif'].min():
        # MACD-DIF历史最低')
        index = 1
    if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < dfstockhisdata.loc[
                                                                 (len(dfstockhisdata) - 90):(len(dfstockhisdata) - 1),
                                                                 'dif'].min()):
        # MACD-DIF90天最低')
        index = 2
    if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < dfstockhisdata.loc[
                                                                 (len(dfstockhisdata) - 30):(len(dfstockhisdata) - 1),
                                                                 'dif'].min()):
        # MACD-DIF30天最低')
        index = 3
    if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'bar'] > 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'bar'] < 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < 0):
        # MACD金叉且DIF小于0')
        index = 4
    if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'bar'] < 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'bar'] > 0) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] > 0):
        # MACD死叉且DIF大于0')
        index = 5
    return index


def judgedma(dfstockhisdata, tablename):
    index = 0
    if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] != None) and (
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < (dfstockhisdata['ddd'].min())):
        # print(dfstockhisdata.loc[len(dfstockhisdata) - 1, 'DDD'], dfstockhisdata['DDD'].min())
        # print(tablename+'  DMA-DDD历史最低')
        index = 1

    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] != None:
        if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < 0) and (
                dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < dfstockhisdata.loc[(len(dfstockhisdata) - 90):(
                len(dfstockhisdata) - 1), 'ddd'].min()):
            # print(tablename+'  DMA-DDD90天最低')
            index = 2
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] != None:
        if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < 0) and (
                dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < dfstockhisdata.loc[(len(dfstockhisdata) - 30):(
                len(dfstockhisdata) - 1), 'ddd'].min()):
            # print(tablename+'  DMA-DDD30天最低')
            index = 3
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] != None:
        if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < 0) and (
                dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < dfstockhisdata.loc[(len(dfstockhisdata) - 10):(
                len(dfstockhisdata) - 1), 'ddd'].min()):
            # print(tablename+'  DMA-DDD10天最低')
            index = 4
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] != None and dfstockhisdata.loc[
        len(dfstockhisdata) - 2, 'ddd'] != None and dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dddma'] != None and \
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'dddma'] != None:
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 1, 'dddma'] and (
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ddd'] < dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'dddma']):
            # print(tablename+'  DMA金叉')
            index = 5
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] < dfstockhisdata.loc[
            len(dfstockhisdata) - 1, 'dddma'] and (
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ddd'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'dddma']):
            # 'DMA死叉'
            index = 6
    return index


def judgedkj(dfstockhisdata):
    index = 0
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'j'] > dfstockhisdata.loc[len(dfstockhisdata) - 1, 'k']:
        index = 1  # KDJ是上升
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'j'] < dfstockhisdata.loc[len(dfstockhisdata) - 1, 'k'] and \
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'j'] > dfstockhisdata.loc[len(dfstockhisdata) - 2, 'k']:
        index = 2  # KDJ是死叉
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'j'] > dfstockhisdata.loc[len(dfstockhisdata) - 1, 'k'] and \
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'j'] < dfstockhisdata.loc[len(dfstockhisdata) - 2, 'k']:
        index = 3  # KDJ是金叉
    else:
        index = 4
    return index


def judgema(dfstockhisdata):
    index = 0
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] != None and dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'ma25'] != None and dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] != None and \
            dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma25'] != None:

        if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 1, 'ma20']) and (
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] <= dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma20']):
            index = 1  # 5日上穿10日当天
        if (dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 1, 'ma25']) and (
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] <= dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma25']):
            index = 2  # 5日上穿25日当天
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] > dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma20'] and \
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] >= dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma20']:
            index = 3  # 5日持续在10日上方
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] > dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma25'] and \
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] >= dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma25']:
            index = 4  # 5日持续在25日上方
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] < dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma20'] and \
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma20']:
            index = 5  # 5日下穿10日当天
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] < dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma25'] and \
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] > dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma25']:
            index = 6  # 5日下穿25日当天

        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5'] < dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma25'] and \
                dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma5'] < dfstockhisdata.loc[
            len(dfstockhisdata) - 2, 'ma25']:
            index = 8  # 5日持续在25日下方

    else:
        index = 0
    return index


def viewma(maindex):
    if maindex == 1:
        vma = 'MA5日上穿ma20日当天，短线上攻'
    if maindex == 2:
        vma = 'MA5日上穿MA25日当天'
    if maindex == 3:
        vma = '  MA5日持续在10日上方'
    if maindex == 4:
        vma = '  MA5日持续在25日上方'
    if maindex == 5:
        vma = 'MA5日下穿ma20日当天，短线了结'
    if maindex == 6:
        vma = 'MA5日下穿MA25日当天，清盘走人'
    if maindex == 8:
        vma = 'MA5日下穿MA25日当天，不能操作'
    else:
        vma = ''
    return vma


def priceandma(dfstockhisdata):
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'close'] >= float(
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma5']):
        priceandma = '  Close高于MA5，走势强势'
    else:
        priceandma = '  Close低于MA5，走势弱势'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'close'] <= float(
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma25']):
        priceandma = '  Close低于MA20，股价低迷'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'close'] <= float(
            dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma20']):
        priceandma = '  Close低于ma20，短线卖出'

    return priceandma


def judgeamount(dfstockhisdata):
    judgeamount = ''
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'amount'] > (
            2.5 * dfstockhisdata.loc[len(dfstockhisdata) - 6:len(dfstockhisdata) - 2, 'amount'].mean()):
        judgeamount = '  注意放量！'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'amount'] > (
            3 * dfstockhisdata.loc[len(dfstockhisdata) - 30:len(dfstockhisdata) - 2, 'amount'].mean()):
        judgeamount = '  月内放量！'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'amount'] > (
            3 * dfstockhisdata.loc[len(dfstockhisdata) - 90:len(dfstockhisdata) - 2, 'amount'].mean()):
        judgeamount = '  3月内超级放量！'
    return judgeamount


def judgecr(dfstockhisdata):
    index = 0
    todaycr = dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr']
    todaycr1 = dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr-ma1']
    todaycr2 = dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr-ma2']
    todaycr3 = dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr-ma3']
    yesterdaycr = dfstockhisdata.loc[len(dfstockhisdata) - 2, 'cr']
    yesterdaycr1 = dfstockhisdata.loc[len(dfstockhisdata) - 2, 'cr-ma1']
    yesterdaycr2 = dfstockhisdata.loc[len(dfstockhisdata) - 2, 'cr-ma2']
    yesterdaycr3 = dfstockhisdata.loc[len(dfstockhisdata) - 2, 'cr-ma3']
    if (todaycr > todaycr1 and todaycr > todaycr2) and (yesterdaycr < yesterdaycr1 or yesterdaycr < yesterdaycr2):
        # CR上穿CR1和CR2，短线买入
        index = 1
    if (todaycr - todaycr1) / todaycr1 < 0.6:
        # (CR-CR1)/CR1<0.6,短线反弹，是不是<0.6?
        index = 2
    if (todaycr > todaycr1 and todaycr > todaycr2 and todaycr > todaycr3) and (
            yesterdaycr < yesterdaycr1 and yesterdaycr < yesterdaycr2 and yesterdaycr < yesterdaycr3):
        # 当天CR上穿CR1，CR2，CR3，前一天CR低于CR1，CR2，CR2，短线强势买入
        index = 3
    if (todaycr < todaycr1 and todaycr < todaycr2) and (yesterdaycr > yesterdaycr1 or yesterdaycr > yesterdaycr2):
        # CR下穿CR1和CR2，卖出信号
        index = 4
    if (todaycr < todaycr1 and todaycr < todaycr2 and todaycr < todaycr3) and (
            yesterdaycr < yesterdaycr1 and yesterdaycr < yesterdaycr2 and yesterdaycr > yesterdaycr3):
        # 当天CR下穿CR3且低于CR1，CR2，将暴跌，清仓出局
        index = 5
    if (todaycr - todaycr1) / todaycr1 > 1.6:
        # (CR-CR1)/CR110.6,短线要跌,是不是>1.6?
        index = 6
    return index


def viewcr(cr, dfstockhisdata):
    viewcr = ''
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr'] < 50: viewcr = '  CR指标超跌可买入！'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr'] > 250: viewcr = '  CR指标超涨要清仓！'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'cr'] > 40 or dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'cr'] < 250: viewcr = ''
    if cr == 1: viewcr = viewcr + ' CR上穿CR1和CR2，短线买入'
    if cr == 2: viewcr = viewcr + ' (CR-CR1)/CR1<0.6,短线反弹??'
    if cr == 3: viewcr = viewcr + ' 当天CR上穿CR1，CR2，CR3短线强势买入'
    if cr == 4: viewcr = viewcr + ' CR下穿CR1和CR2，卖出信号'
    if cr == 5: viewcr = viewcr + ' 当天CR下穿CR3且低于CR1，CR2，将暴跌，清仓出局'
    if cr == 6: viewcr = viewcr + ' (CR-CR1)/CR110.6,短线要跌,是不是>1.6??'
    return viewcr


def judgeamountandprice(dfstockhisdata):
    judgeamountandprice = ''
    temp = pd.DataFrame()
    temp['high'] = dfstockhisdata['high']
    temp['low'] = dfstockhisdata['low']
    temp['avgprice'] = (temp['high'] + temp['low']) / 2  # 均价

    if len(temp) < 7:
        return judgeamountandprice
    else:
        # print('1-3volume',dfstockhisdata.loc[len(dfstockhisdata)-1,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-2,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-3,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-3:len(dfstockhisdata)-1,'volume'].mean())
        # print('4-6volume',dfstockhisdata.loc[len(dfstockhisdata)-4,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-5,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-6,'volume'],dfstockhisdata.loc[len(dfstockhisdata)-6:len(dfstockhisdata)-4,'volume'].mean())
        # print('1-3price',temp.loc[len(temp)-1,'avgprice'],temp.loc[len(temp)-2,'avgprice'],temp.loc[len(temp)-3,'avgprice'],temp.loc[len(temp)-3:len(temp)-1,'avgprice'].mean())
        # print('4-6price',temp.loc[len(temp)-4,'avgprice'],temp.loc[len(temp)-5,'avgprice'],temp.loc[len(temp)-6,'avgprice'],temp.loc[len(temp)-6:len(temp)-4,'avgprice'].mean())
        l3avg = (temp.loc[len(temp) - 1, 'avgprice'] + temp.loc[len(temp) - 2, 'avgprice'] + temp.loc[
            len(temp) - 3, 'avgprice']) / 3
        l6avg = (temp.loc[len(temp) - 4, 'avgprice'] + temp.loc[len(temp) - 5, 'avgprice'] + temp.loc[
            len(temp) - 6, 'avgprice']) / 3
        # print('2avg',l3avg,l6avg)
        l3vol = dfstockhisdata.loc[len(dfstockhisdata) - 3:len(dfstockhisdata) - 1, 'volume'].mean()
        l6vol = dfstockhisdata.loc[len(dfstockhisdata) - 6:len(dfstockhisdata) - 4, 'volume'].mean()
        if l3vol / l6vol > 1.1 and l3avg / l6avg < 1.015 and l3avg / l6avg > 0.995:
            judgeamountandprice = '量增价平，买入机会'
        if l3vol / l6vol < 1.1 and l3vol / l6vol > 0.9 and l3avg / l6avg > 1.015:
            judgeamountandprice = '量平价升，持续买入'
        if l3vol / l6vol < 0.9 and (l3avg / l6avg > 1.015):
            judgeamountandprice = '量减价升，继续持有'
        if l3vol / l6vol < 0.9 and (l3avg / l6avg < 0.98):
            judgeamountandprice = '量减价平or价跌，尽快卖出'
    return judgeamountandprice


def viewrsi_6(dfstockhisdata):
    viewrsi_6 = ''
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'rsi_6'] > 80: viewrsi_6 = '  RSI指标超高须卖出！'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'rsi_6'] > 50 and dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'rsi_6'] < 80: viewrsi_6 = '  RSI指标强可买入'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'rsi_6'] > 20 and dfstockhisdata.loc[
        len(dfstockhisdata) - 1, 'rsi_6'] < 50: viewrsi_6 = '  RSI指标弱观望'
    if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'rsi_6'] < 20: viewrsi_6 = '  RSI指标超低可逢低买进！'
    return viewrsi_6


def isindfopportunity(tablename, dfopportunity):
    if tablename in dfopportunity['code']:
        isin = 0
    else:
        isin = 1
    return isin


def qrrstr(qrrvalue):
    qrrstr = ''
    if qrrvalue > 10: qrrstr = '  需要反向操作'
    if qrrvalue > 5 and qrrvalue < 10: qrrstr = '  潜力巨大，已经启动'
    if qrrvalue > 2.5 and qrrvalue < 5: qrrstr = '  需确认是否突破阻力位，若是可以建仓'
    if qrrvalue > 1.5 and qrrvalue < 2.5: qrrstr = '  若股价温和缓升，继续持股；若下跌需平仓'
    qrrstr = '\n                      \033[1;31;40m量比：' + str(qrrvalue) + qrrstr + '\033[0m'
    return qrrstr


def turnoverrate(torvalue):
    turnoverratestr = ''
    if torvalue < 2: turnoverratestr = '  冷清无方向'
    if torvalue >= 2 and torvalue <= 5: turnoverratestr = '  相对活跃，适当介入'
    if torvalue >= 5 and torvalue <= 10: turnoverratestr = '  活跃，介入'
    if torvalue >= 10 and torvalue <= 15: turnoverratestr = '  非常活跃，大举买入或卖出'
    if torvalue >= 15: turnoverratestr = '  极度活跃，大举买入或清仓'
    turnoverratestr = '\n                      \033[1;31;40m换手率：' + str(torvalue) + turnoverratestr + '\033[0m'
    return turnoverratestr


def daytoweek(dfhistemp):
    dfhistemp['date'] = pd.to_datetime(dfhistemp['date'])
    period_type = 'W'
    # print('1',dfhistemp)
    # print(dfhistemp.columns)
    dfin = pd.DataFrame()
    dfin['date'] = dfhistemp['date']
    dfin['open'] = dfhistemp['open']
    dfin['high'] = dfhistemp['high']
    dfin['low'] = dfhistemp['low']
    dfin['close'] = dfhistemp['close']
    dfin['volume'] = dfhistemp['volume']
    dfin['amount'] = dfhistemp['amount']
    dfin.set_index('date', inplace=True)
    # print('2',dfhistemp)
    # print('dfin',dfin)
    dfweekdata = dfin.resample(period_type).last()
    dfweekdata['open'] = dfin['open'].resample(period_type).first()
    dfweekdata['high'] = dfin['high'].resample(period_type).max()
    dfweekdata['low'] = dfin['low'].resample(period_type).min()
    dfweekdata['volume'] = dfin['volume'].resample(period_type).sum()
    dfweekdata = dfweekdata[dfweekdata['volume'].notnull()]
    dfweekdata['amount'] = dfweekdata['close'] * dfweekdata['volume']
    # dfweekdata.drop(columns={'振幅', '涨跌幅', '涨跌额', '换手率'}, inplace=True)
    dfweekdata.reset_index(inplace=True)
    calc_macd(dfweekdata)
    # calc_kdj(dfweekdata)
    calc_dma(dfweekdata)
    return (dfweekdata)


def judgeweek(dfweekdata):
    weekmacd = ''
    weekdma = ''
    blank = '                      '

    if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] - dfweekdata['dif'].mean() < 0:
        # MACDdif低位
        # macde金叉
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] >= dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] < dfweekdata.loc[len(dfweekdata) - 2, 'dea']:
            weekmacd = blank + '周线MACD低位金叉'
        # macde下跌
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] < dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 1, 'dif'] < dfweekdata.loc[len(dfweekdata) - 2, 'dif'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] < dfweekdata.loc[len(dfweekdata) - 3, 'dif']:
            weekmacd = blank + '周线MACD低位下跌'
        # macde上涨
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] > dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 1, 'dif'] > dfweekdata.loc[len(dfweekdata) - 2, 'dif'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] > dfweekdata.loc[len(dfweekdata) - 3, 'dif']:
            weekmacd = blank + '周线MACD低位上涨'
    else:
        # MACDdif高位
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] <= dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] > dfweekdata.loc[len(dfweekdata) - 2, 'dea']:
            weekmacd = blank + '周线MACD高位死叉，谨慎操作'
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] < dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 1, 'dif'] < dfweekdata.loc[len(dfweekdata) - 2, 'dif'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] < dfweekdata.loc[len(dfweekdata) - 3, 'dif']:
            weekmacd = blank + '周线MACD高位下跌，谨慎操作'
        if dfweekdata.loc[len(dfweekdata) - 1, 'dif'] > dfweekdata.loc[len(dfweekdata) - 1, 'dea'] and dfweekdata.loc[
            len(dfweekdata) - 1, 'dif'] > dfweekdata.loc[len(dfweekdata) - 2, 'dif'] and dfweekdata.loc[
            len(dfweekdata) - 2, 'dif'] > dfweekdata.loc[len(dfweekdata) - 3, 'dif']:
            weekmacd = blank + '周线MACD高位上涨，谨慎操作'
    if dfweekdata.loc[len(dfweekdata) - 1, 'bar'] > -0.5 and dfweekdata.loc[
        len(dfweekdata) - 1, 'bar'] < 0 and \
            dfweekdata.loc[len(dfweekdata) - 1, 'bar'] > dfweekdata.loc[
        len(dfweekdata) - 2, 'bar'] and \
            dfweekdata.loc[len(dfweekdata) - 2, 'bar'] > dfweekdata.loc[
        len(dfweekdata) - 3, 'bar'] and \
            dfweekdata.loc[len(dfweekdata) - 1, 'dif'] < 0 and dfweekdata.loc[
        len(dfweekdata) - 1, 'ddd'] < 0 and \
            dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] > dfweekdata.loc[len(dfweekdata) - 2, 'ddd']:
        weekmacd = blank + '周线MACD即将翻正'

    # print(dfweekdata.loc[len(dfweekdata)-1,'ddd'])
    # print(dfweekdata['ddd'].mean)
    if np.isnan(dfweekdata.loc[len(dfweekdata) - 1, 'ddd']) != True and np.isnan(dfweekdata['ddd'].mean()) != True:
        if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] - dfweekdata['ddd'].mean < 0:
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] >= dfweekdata.loc[len(dfweekdata) - 1, 'dddma'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 2, 'dddma']:
                weekdma = blank + '周线DMA低位金叉'
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 3, 'ddd']:
                weekdma = blank + '周线DMA低位下跌'
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] > dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] > dfweekdata.loc[len(dfweekdata) - 3, 'ddd']:
                weekdma = blank + '周线DMA低位上涨'
        else:
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 1, 'dddma'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] >= dfweekdata.loc[len(dfweekdata) - 2, 'dddma']:
                weekdma = blank + '周线DMA高位死叉，谨慎操作'
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] < dfweekdata.loc[len(dfweekdata) - 3, 'ddd']:
                weekdma = blank + '周线DMA高位下跌，谨慎操作'
            if dfweekdata.loc[len(dfweekdata) - 1, 'ddd'] > dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] and \
                    dfweekdata.loc[len(dfweekdata) - 2, 'ddd'] > dfweekdata.loc[len(dfweekdata) - 3, 'ddd']:
                weekdma = blank + '周线DMA高位上涨，谨慎操作'
    week = weekmacd + weekdma
    return week


def TFST(df):  # 25-60
    TFSTvalue = 0
    # ma25坚决向上
    if df.loc[len(df) - 1, 'ma25'] > df.loc[len(df) - 2, 'ma25'] and df.loc[len(df) - 2, 'ma25'] > df.loc[
        len(df) - 3, 'ma25'] and df.loc[len(df) - 1, 'ma25']> df.loc[len(df) - 5, 'ma25'] and df.loc[len(df) - 1, 'ma5'] >= df.loc[len(df) - 1, 'ma60']:
        if df.loc[len(df) - 1, 'ma5'] >= df.loc[len(df) - 1, 'ma25'] and df.loc[len(df) - 2, 'ma5'] < df.loc[
            len(df) - 2, 'ma25'] and df.loc[len(df) - 1, 'mavol5'] >= df.loc[len(df) - 1, 'mavol60']:
            # ma5上穿ma25当天，且mavol5大于等于mavol60
            TFSTvalue = 1  # 25-60买入信号
        if df.loc[len(df) - 1, 'ma5'] - df.loc[len(df) - 1, 'ma25'] > 0 and df.loc[len(df) - 2, 'ma5'] - df.loc[
            len(df) - 2, 'ma25'] > 0 and df.loc[len(df) - 3, 'ma5'] - df.loc[len(df) - 3, 'ma25'] > 0 and (
                df.loc[len(df) - 1, 'ma5'] - df.loc[len(df) - 1, 'ma25']) < (
                df.loc[len(df) - 2, 'ma5'] - df.loc[len(df) - 2, 'ma25']) and df.loc[len(df) - 1, 'ma5'] - df.loc[
            len(df) - 1, 'ma25'] < 0.5 and df.loc[len(df) - 1, 'ma5'] < df.loc[len(df) - 2, 'ma5'] and df.loc[
            len(df) - 1, 'mavol5'] >= df.loc[len(df) - 1, 'mavol60']:
            # ma5回踩ma25，且mavol5大于等于mavol60
            TFSTvalue = 2  # 25-60买入信号
        if df.loc[len(df) - 1, 'ma25'] - df.loc[len(df) - 1, 'ma5'] > 0 and df.loc[len(df) - 1, 'ma25'] - df.loc[len(df) - 1, 'ma5'] < 0.2 and df.loc[len(df) - 1, 'ma5'] > df.loc[
            len(df) - 2, 'ma5'] and df.loc[len(df) - 2, 'ma5'] > df.loc[len(df) - 3, 'ma5']:
            TFSTvalue = 4  # ma5即将上穿ma25，留意机会
        if df.loc[len(df) - 1, 'mavol5'] >= df.loc[len(df) - 1, 'mavol60'] and df.loc[len(df) - 2, 'mavol5'] < df.loc[
            len(df) - 2, 'mavol60']:
            TFSTvalue = 5  # vol5-vol60金叉，留意机会
        if df.loc[len(df)-1,'ma5']>=df.loc[len(df)-1,'ma25'] and df.loc[len(df)-2,'ma5']<df.loc[len(df)-2,'ma25'] and df.loc[len(df)-1,'mavol5']<df.loc[len(df)-2,'mavol60'] and df.loc[len(df)-1,'mavol5']>df.loc[len(df)-2,'mavol5']:
            TFSTvalue=6  #ma5上穿ma25，mavol5小于mavol60
    return TFSTvalue

def MA2055(df): #中线MA20-55
    MA2055value='                      '
    if df.loc[len(df)-1,'ma20']>df.loc[len(df)-1,'ma55'] and df.loc[len(df)-2,'ma20']>df.loc[len(df)-2,'ma55'] and df.loc[len(df)-3,'ma20']>df.loc[len(df)-3,'ma55']:
        MA2055value=MA2055value+'MA20-55中线多头'
    if df.loc[len(df)-1,'ma20']<df.loc[len(df)-1,'ma55'] and df.loc[len(df)-2,'ma20']<df.loc[len(df)-2,'ma55'] and df.loc[len(df)-3,'ma20']<df.loc[len(df)-3,'ma55']:
        MA2055value=MA2055value+'MA20-55中线空头'
    if df.loc[len(df)-1,'ma20']>=df.loc[len(df)-1,'ma55'] and df.loc[len(df)-2,'ma20']<df.loc[len(df)-2,'ma55']:
        MA2055value=MA2055value+'MA20-55中线金叉转涨'
    if df.loc[len(df)-1,'ma20']<df.loc[len(df)-1,'ma55'] and df.loc[len(df)-2,'ma20']>df.loc[len(df)-2,'ma55']:
        MA2055value=MA2055value+'MA20-55中线死叉转跌'
    return(MA2055value)

def MA200(df): #长线MA200
    MA200value=' '
    if len(df) < 200: return(MA200value)
    if df.loc[len(df)-1,'close']>df.loc[len(df)-1,'ma200'] and df.loc[len(df)-2,'close']<df.loc[len(df)-2,'ma200'] and (df.loc[len(df)-2,'close']+df.loc[len(df)-3,'close']+df.loc[len(df)-4,'close'])/3<df.loc[len(df)-1,'ma200']:
        MA200value=MA200value+'MA200模型：股价突破ma200，长线上涨突破信号确认'
    return(MA200value)

def plus2560(df): #激进版25-60,KDJ金叉当天，MACD低点，MA5在MA25下且最后一天拐头，MA25多头
    plus2560value=' '
    if len(df) < 5: return(plus2560value)
    if df.loc[len(df)-1,'j']>df.loc[len(df)-1,'k'] and df.loc[len(df)-2,'j']<= df.loc[len(df)-2,'k'] and df.loc[len(df)-1,'j']>df.loc[len(df)-2,'j'] and df.loc[len(df)-1,'bar']> df.loc[len(df)-2,'bar'] and df.loc[len(df)-1,'ma5']>df.loc[len(df)-2,'ma5'] and df.loc[len(df)-1,'ma5']<df.loc[len(df)-1,'ma25'] and df.loc[len(df)-1,'ma25']>df.loc[len(df)-2,'ma25'] and df.loc[len(df)-2,'ma25']>df.loc[len(df)-3,'ma25']:
        #and df.loc[len(df)-1,'rsi_14']>df.loc[len(df)-2,'rsi_14'] and df.loc[len(df)-2,'rsi_14']<df.loc[len(df)-3,'rsi_14']:
        plus2560value=plus2560value+'25-60激进买点，参考VOL，谨慎确认！！KDJ金叉当天，MACD低点，MA5在MA25下且最后一天拐头，MA25多头'
    return(plus2560value)


# ------------START
#sqlselectstockcode = 'SELECT ts_code FROM ACSQuant.chinaaindustry;'
#stockcodefromtable = pd.read_sql_query(sqlselectstockcode, engine)
#liststockcode = stockcodefromtable['ts_code']
liststockcode=pd.read_csv('chinaacode0715-2.csv')
sumcodenumber = len(liststockcode)  # industry表里股票数量

nowdate = datetime.date(datetime.now())
dfhold = pd.read_csv('chinaaholdlist.csv')
# 文件名
upcsvname = str(nowdate) + 'up.csv'
downcsvname = str(nowdate) + 'down.csv'

while time.strftime('%H:%M', time.localtime()) > '09:30' and time.strftime('%H:%M',
                                                                           time.localtime()) < '23:30':  # 09-16HK,US时间需要修改！！！！！
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    process = 0  # 进度计数

    # dfopportunity = pd.read_csv(upcsvname)#读已有的当日opportunity文件
    #    print('还没有当日opportunity文件')
    dfstockrealtime = ak.stock_zh_a_spot_em()  # 取东财实时数据
    print('取到EM实时数据', nowtime)

    upwarddf = pd.DataFrame(
        columns=[ 'time', 'code', 'name','price', 'opportunity'])
    # upwarddf是上涨机会结果df表
    downwarddf = pd.DataFrame(
        columns=['time', 'code', 'name','price', 'opportunity'])
    # downwarddf是下跌机会结果df表

    # 开始遍历industry里的每一只股票
    print('==================新循环开始======================', nowtime)

    for tablename in liststockcode['symbol']:

        # SQLSelectAllData = 'SELECT * FROM ACSQuant.`' + tablename + '`'
        # dfstockhisdata = pd.read_sql_query(SQLSelectAllData, engine) #数据库里取全部历史数据
        code = tablename.split('.', 2)[0]

        dfstockhisdata = ak.stock_zh_a_hist(symbol=code, adjust="", start_date='20210101')
        # print(dfstockhisdata.columns)
        dfstockhisdata.rename(
            columns={"开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "日期": "date", '成交量': 'volume',
                     '成交额': 'amount', '涨跌额': 'chgv', '振幅': 'diff', '涨跌幅': 'chg', '换手率': 'turnoverrate','量比':'qrr'},
            inplace=True)

        df = dfstockrealtime[dfstockrealtime['代码'] == code]
        df = df.reset_index()
        # print(code,df)
        # index    序号      代码    名称    最新价   涨跌幅  ...     今开     昨收    量比  换手率 市盈率-动态   市净率
        # df是dfrealtime，只有当天一行数据

        # print('dfstockhisdata行数',len(dfstockhisdata))

        dftemp = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pe', 'pb', 'ps'])
        dftemp['date'] = dfstockhisdata['date']
        # dftemp['index']=dfstockhisdata['index']
        dftemp['open'] = dfstockhisdata['open']
        dftemp['high'] = dfstockhisdata['high']
        dftemp['low'] = dfstockhisdata['low']
        dftemp['close'] = dfstockhisdata['close']
        dftemp['volume'] = dfstockhisdata['volume']
        dftemp['amount'] = dfstockhisdata['amount']
        # dftemp['pe']=dfstockhisdata['pe']
        # dftemp['pb']=dfstockhisdata['pb']
        # dftemp['ps']=dfstockhisdata['ps']
        # 引入stockstats，计算CR、RSI指标
        dftemp = stockstats.StockDataFrame.retype(dftemp)

        dftemp.get('cr')
        dftemp.get('boll')
        dftemp.get('rsi_6')
        dftemp.get('rsi_14')
        dftemp.get('rsi_70')
        dftemp['cr'] = dftemp['cr'].replace(np.inf, np.nan)
        dftemp['rs_6'] = dftemp['rs_6'].replace(np.inf, np.nan)
        dftemp['rsi_6'] = dftemp['rsi_6'].replace(np.inf, np.nan)
        dftemp['rsi_14'] = dftemp['rsi_14'].replace(np.inf, np.nan)
        dftemp['rsi_70'] = dftemp['rsi_70'].replace(np.inf, np.nan)

        dftemp = dftemp.reset_index(drop=False)
        # print('dftemp',dftemp)
        # print(dftemp.loc[len(dftemp)-1,'cr'],dftemp.loc[len(dftemp)-1,'rsi_6'])
        dfstockhisdata = dftemp

        # 计算日线各种指标
        calc_macd(dfstockhisdata)  # 计算MACD值，数据存于DataFrame中
        calc_kdj(dfstockhisdata)  # 计算KDJ值，数据存于DataFrame中
        calc_dma(dfstockhisdata)  # 计算DMA值，数据存于DF中
        calc_ma(dfstockhisdata)  # 计算MA均线
        # print('MA5-MA25',tablename,dfstockhisdata.loc[len(dfstockhisdata)-1,'ma5'],dfstockhisdata.loc[len(dfstockhisdata)-1,'ma25'],dfstockhisdata.loc[len(dfstockhisdata)-2,'ma5'],dfstockhisdata.loc[len(dfstockhisdata)-2,'ma25'])
        calc_mavol(dfstockhisdata)  # 计算MAVOL均线
        # print('VOL5-VOL60',tablename,dfstockhisdata.loc[len(dfstockhisdata)-1,'mavol5'],dfstockhisdata.loc[len(dfstockhisdata)-1,'mavol60'],dfstockhisdata.loc[len(dfstockhisdata)-2,'mavol5'],dfstockhisdata.loc[len(dfstockhisdata)-2,'mavol60'])
        lastdate = dfstockhisdata.loc[len(dfstockhisdata) - 1, 'date']
        # pe=judgepe(dfstockhisdata,tablename)
        macd = judgemacd(dfstockhisdata, tablename)
        dma = judgedma(dfstockhisdata, tablename)
        penow = str(dfstockhisdata.loc[len(dfstockhisdata) - 1, 'pe'])
        kdj = judgedkj(dfstockhisdata)
        ma = judgema(dfstockhisdata)
        cr = judgecr(dfstockhisdata)
        vpriceandma = priceandma(dfstockhisdata)
        vamount = judgeamount(dfstockhisdata)
        vcr = viewcr(cr, dfstockhisdata)
        vrsi_6 = viewrsi_6(dfstockhisdata)
        vamountandprice = judgeamountandprice(dfstockhisdata)

        dfhistemp = dfstockhisdata
        dfweekdata = daytoweek(dfhistemp)  # 日线转周线
        TFSTvalue = TFST(dfstockhisdata)  # 25-60
        MA2055value=MA2055(dfstockhisdata) #MA20-55
        MA200value = MA200(dfstockhisdata)  # MA200
        plus2560value = plus2560(dfstockhisdata)  # plus25-60

        for o in range(len(dfhold) - 1):
            if code == str(dfhold.loc[o, 'code']):
                print('持股中，·1·  code in dfhold', code)
                upcolor = '\033[1;31;43m'
                downcolor = '\033[1;32;43m'
            else:
                upcolor = '\033[1;31;40m'
                downcolor = '\033[1;32;40m'

        # -------日线判断
        #MA25多头向上且RIS70小于60时RSI14-70
        if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ma25'] > dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma25'] and dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ma25'] >dfstockhisdata.loc[len(dfstockhisdata) - 3, 'ma25'] and dfstockhisdata.loc[len(dfstockhisdata) - 3, 'ma25'] > dfstockhisdata.loc[len(dfstockhisdata) - 4, 'ma25'] and dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_70']<60:
            if dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14']>=dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_70'] and dfstockhisdata.loc[len(dfstockhisdata)-2,'rsi_14']<dfstockhisdata.loc[len(dfstockhisdata)-2,'rsi_70']and (dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14']+dfstockhisdata.loc[len(dfstockhisdata)-2,'rsi_14']+dfstockhisdata.loc[len(dfstockhisdata)-3,'rsi_14'])/3<dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_70']:
                print(nowtime + '  \033[1;31;40m' + tablename  +df.loc[0, '名称']+'  RSI14-70买入信号！！\033[0m',dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14'],dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_70'])
                print(MA2055value)
        if dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14']<20 and tablename in dfhold:
            print(nowtime + '  \033[1;31;40m' + tablename  + '  RSI14超卖，短线低点确认！！\033[0m',dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14'])
            print(MA2055value)
        if dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14']>80 and tablename in dfhold:
            print(nowtime + '  \033[1;31;40m' + tablename  + '  RSI14超买，短线高点逃顶确认！！\033[0m',dfstockhisdata.loc[len(dfstockhisdata)-1,'rsi_14'])
            print(MA2055value)

        #25-60
        if TFSTvalue == 1:
            print(nowtime + '  \033[1;31;40m' + tablename + df.loc[0, '名称'] + '  25-60买入信号！！\033[0m')
            print(MA2055value)
            upwarddf.loc[len(upwarddf) + 1] = {'time': nowtime,'code': tablename,'name':df.loc[0, '名称'],'price':df.loc[len(df)-1,'最新价'],'opportunity': '25-60买入信号！！'}

        if TFSTvalue==4:
            print(nowtime + '  \033[1;31;40m' + tablename + df.loc[0, '名称'] + '  25-60 ma5即将上穿ma25，留意机会！！\033[0m')
            print(MA2055value)
            upwarddf.loc[len(upwarddf) + 1] = {'time': nowtime,'code': tablename,'name':df.loc[0, '名称'],'price':df.loc[len(df)-1,'最新价'],'opportunity': '25-60 ma5即将上穿ma25，留意机会！！'}

        if TFSTvalue == 5:
            print(nowtime + '  \033[1;31;40m' + tablename + df.loc[0, '名称'] + '  25-60 vol5-vol60金叉，留意机会！！\033[0m')
            print(MA2055value)
            upwarddf.loc[len(upwarddf) + 1] = {'time': nowtime,'code': tablename,'name':df.loc[0, '名称'],'price':df.loc[len(df)-1,'最新价'],'opportunity': '25-60 vol5-vol60金叉，留意机会！！'}

        if TFSTvalue==6:
            print(nowtime + '  \033[1;31;40m' + tablename + df.loc[0, '名称'] + '  25-60 ma5-ma25金叉，mavol5<mavol60留意机会！！\033[0m')
            print(MA2055value)
            upwarddf.loc[len(upwarddf) + 1] = {'time': nowtime,'code': tablename,'name':df.loc[0, '名称'],'price':df.loc[len(df)-1,'最新价'],'opportunity': '25-60 ma5-ma25金叉，mavol5<mavol60留意机会！！'}


        #量价-换手率
        if df.loc[0, '量比'] != '-' and int(df.loc[0, '量比']) > 1 and int(df.loc[0, '换手率'] >= 2):
            if dfstockhisdata.loc[len(dfstockhisdata) - 1, 'bar'] > -0.5 and dfstockhisdata.loc[
                len(dfstockhisdata) - 1, 'bar'] < 0 and dfstockhisdata.loc[len(dfstockhisdata) - 1, 'bar'] > \
                    dfstockhisdata.loc[len(dfstockhisdata) - 2, 'bar'] and dfstockhisdata.loc[
                len(dfstockhisdata) - 2, 'bar'] > dfstockhisdata.loc[len(dfstockhisdata) - 3, 'bar'] and \
                    dfstockhisdata.loc[len(dfstockhisdata) - 1, 'dif'] < 0 and dfstockhisdata.loc[
                len(dfstockhisdata) - 1, 'ddd'] < 0 and dfstockhisdata.loc[len(dfstockhisdata) - 1, 'ddd'] > \
                    dfstockhisdata.loc[len(dfstockhisdata) - 2, 'ddd']:
                print(nowtime + '  \033[1;31;40m' + tablename + df.loc[0, '名称'] + '  MACD接近翻正\033[0m' + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if vamountandprice in {'量增价平，买入机会', '量平价升，持续买入', '量减价升，继续持有'} and vamount != '':
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[
                    0, '名称'] + '\033[0m  ' + vamountandprice + ' ' + '\033[1;33;40m' + vamount + '\033[0m')
            if vamountandprice == '量减价平or价跌，尽快卖出' and vamount != '':
                print(str(lastdate) + '\033[0m  ' + downcolor + tablename + df.loc[
                    0, '名称'] + '\033[0m  ' + vamountandprice + ' ' + '\033[1;33;40m' + vamount + '\033[0m')
            if vamountandprice == '' and vamount != '':
                print(str(lastdate) + '\033[0m  ' + tablename + df.loc[
                    0, '名称'] + ' ' + '\033[1;33;40m' + vamount + '\033[0m')
                if int(df.loc[0, '涨跌幅']) >= 3:
                    print('                      ' + '涨跌幅' + str(df.loc[0, '涨跌幅']) + ' 换手率' + str(
                        df.loc[0, '换手率']) + ' 量比' + str(df.loc[0, '量比']))
                    print(judgeweek(dfweekdata))
            if cr == 1:
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(df.loc[
                                                                                                                len(df) - 1, '最新价']) + '  CR上穿CR1和CR2，短线买入' + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if cr == 3:
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  当天CR上穿CR1，CR2，CR3短线强势买入' + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))

        #MA25均线向上时的MACD和DMA
        if dfstockhisdata.loc[len(dfstockhisdata)-1,'ma25']>dfstockhisdata.loc[len(dfstockhisdata)-2,'ma25'] and dfstockhisdata.loc[len(dfstockhisdata)-2,'ma25']>dfstockhisdata.loc[len(dfstockhisdata)-3,'ma25'] and dfstockhisdata.loc[len(dfstockhisdata)-3,'ma25']>dfstockhisdata.loc[len(dfstockhisdata)-4,'ma25']:
            if macd == 4 and dma == 5 and kdj == 1:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'DMA金叉/MACD金叉,pe=' + str(penow) + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(
                    str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                        df.loc[len(df) - 1, '最新价']) + '  DMA金叉/MACD金叉/KDJ上升,pe=' + penow + viewma(
                        ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                        df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
                if ma == 1:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': '均线：MA5日上穿10日均线金叉,pe=' + str(penow) + vpriceandma + vamount + vcr + vrsi_6}
                    print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(df.loc[len(df) - 1, '最新价']) + '  均线：MA5日上穿10日均线金叉,pe=' + penow + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))

            if macd == 4 and dma == 5 and kdj == 3:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'DMA金叉/MACD金叉,pe=' + str(penow) + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(
                    str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                        df.loc[len(df) - 1, '最新价']) + '  DMA金叉/MACD金叉/KDJ金叉,pe=' + penow + viewma(
                        ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                        df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
                if ma == 1:
                    #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': '均线：MA5日上穿10日均线金叉,pe=' + str(penow) + vpriceandma + vamount + vcr + vrsi_6}
                    print(
                        str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                            df.loc[
                                len(df) - 1, '最新价']) + '  均线：MA5日上穿10日均线金叉,pe=' + penow + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                            df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))

            if macd == 4 and dma == 5 and (kdj == 4 or kdj == 2):
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'DMA金叉/MACD金叉,pe=' + str(penow) + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(
                    str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                        df.loc[len(df) - 1, '最新价']) + '  DMA金叉/MACD金叉/KDJ下降,pe=' + penow + viewma(
                        ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                        df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
                if ma == 1:
                    #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': '均线：MA5日上穿10日均线金叉,pe=' + str(penow) + vpriceandma + vamount + vcr + vrsi_6}
                    print(
                        str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                            df.loc[
                                len(df) - 1, '最新价']) + '  均线：MA5日上穿10日均线金叉,pe=' + penow + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                            df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))

            if macd == 6 and dma == 5:  # MACD高位金叉
               #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'time': nowtime, 'code': tablename,'price': df.loc[len(df) - 1, 'price'],'opportunity': 'DMA金叉/MACD高位金叉' + + viewma(ma) + priceandma(dfstockhisdata) + vamount + vcr + vrsi_6}
                print(
                    str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[
                        0, '名称'] + '\033[0m' + '  DMA高位金叉/MACD金叉' + viewma(
                        ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                        df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if macd == 1:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'MACD-DIF历史最低' + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(
                    str(lastdate) + '\033[0m  ' + upcolor + tablename + '\033[0m' + str(
                        df.loc[len(df) - 1, '最新价']) + '  MACD-DIF历史最低' + viewma(
                        ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                        df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if macd == 2:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'MACD-DIF90天最低' + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  MACD-DIF90天最低' + viewma(
                    ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if macd == 3:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'MACD-DIF30天最低' + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  MACD-DIF30天最低' + viewma(
                    ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if macd == 4:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'MACD金叉且DIF小于0' + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  MACD金叉且DIF小于0' + viewma(
                    ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
                if ma == 1:
                    #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': '均线：MA5日上穿10日均线金叉,pe=' + str(penow) + vpriceandma + vamount + vcr + vrsi_6}
                    print(
                        str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                            df.loc[len(df) - 1, '最新价']) + '  均线：MA5日上穿10日均线金叉,pe=' + penow + priceandma(
                            dfstockhisdata) + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                            df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
            if dma == 3:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'DMA30天最低' + viewma(ma) + vpriceandma + vamount + vcr + vrsi_6}
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  DMA30天最低' + viewma(
                    ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
            if dma == 4:
                #upwarddf.loc[len(upwarddf) + 1] = {'date': lastdate, 'code': tablename,'price': df.loc[len(df) - 1, '最新价'],'opportunity': 'DMA10天最低' + viewma(ma) + priceandma(dfstockhisdata) + vamount + vcr + vrsi_6}
                print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] + '\033[0m' + str(
                    df.loc[len(df) - 1, '最新价']) + '  DMA10天最低' + viewma(
                    ma) + vpriceandma + '\033[1;33;40m' + vamount + '\033[0m' + vcr + vrsi_6 + qrrstr(
                    df.loc[0, '量比']) + turnoverrate(df.loc[0, '换手率']))
                print(judgeweek(dfweekdata))
        #MA200
        if MA200value!=' ':
            print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称'] +MA200value+ '\033[0m')

        # 激进版25-60,KDJ金叉后一天，MACD低点，MA5在MA25下且最后一天拐头，MA25多头，RSI-14低点拐头
        if plus2560value != ' ':
            print(str(lastdate) + '\033[0m  ' + upcolor + tablename + df.loc[0, '名称']  + plus2560value+ '\033[0m')
            upwarddf.loc[len(upwarddf) + 1] = {'time': nowtime,'code': tablename,'name':df.loc[0, '名称'],'price':df.loc[len(df)-1,'最新价'],'opportunity': '激进版25-60'}

    print('===============================本次循环结束，机会列表===============================\n', upwarddf)

    # save csv

    #upwarddf.to_csv(upcsvname, index=False, mode='a')
    #downwarddf.to_csv(downcsvname, index=False, mode='a')
else:
    print(str(nowdate) + '已收盘')

