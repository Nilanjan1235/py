from smartapi import SmartConnect
from breeze_connect import BreezeConnect
import pandas as pd
import requests
import numpy as np
from datetime import datetime,date,time,timedelta
import threading
import talib as ta
from talib.abstract import *
from time import time, sleep
import warnings
warnings.filterwarnings('ignore')

per_trade_fund = 20000
ema_short = 10
ema_long = 20
regression = 14
timeFrame = 60 + 5
mul = 1.5
atr = 14
TRADED_SYMBOL = []

session_key = '1443187'
api_key_icici = 'w+m1950Lf82551z135q24Y9pk2$70b41'
secret_key_icici = ')O961o_uk83l10x361154tr~404Aje11'

apikey = 'ExrLviMc'
username = 'C142810'
pwd = 'Chillout@69'


def get_options_ltp(symbol,token):
    
    ltp= obj.ltpData('NFO',symbol,token)
    Ltp= ltp['data']['ltp']
    return Ltp

def get_future_ltp(symbol,token):
    
    ltp= obj.ltpData('NSE',symbol,token)
    Ltp= ltp['data']['ltp']
    return Ltp

def script_token_map():

    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    d = requests.get(url).json()
    token_df = pd.DataFrame.from_dict(d)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'])
    token_df = token_df.astype({'strike' : float})
    token_df['symbol'] = token_df['symbol'].astype(str)
    return token_df

def calloptions_info(name,token):
       
    df = script_token_map()
    Ltp = get_future_ltp (name,token)
    symbol_token = df[(df.exch_seg == 'NFO') & (df.instrumenttype == 'OPTIDX') & (df.name == name ) & (df['symbol'].str.endswith('CE'))].sort_values(['expiry', 'strike'],
              ascending = [True, True])
    symbol_token = symbol_token.reset_index(inplace=False)
    symbol_token = symbol_token.drop(['index'], inplace= False,axis=1)
    symbol_token['strike'] = symbol_token['strike'] /100

    
    for i in range(len(symbol_token)) :
        if symbol_token.strike.iloc[i] > Ltp:
            break
    return(symbol_token.token.iloc[i],symbol_token.symbol.iloc[i],symbol_token.name.iloc[i],symbol_token.expiry.iloc[i],symbol_token.strike.iloc[i],symbol_token.lotsize.iloc[i])

def putoptions_info(name,token):
    
    df = script_token_map()
    Ltp = get_future_ltp (name,token)
    symbol_token = df[(df.exch_seg == 'NFO') & (df.instrumenttype == 'OPTIDX') & (df.name == name) & (df['symbol'].str.endswith('PE'))].sort_values(['expiry', 'strike'],
              ascending = [True, True])
    symbol_token = symbol_token.reset_index(inplace=False)
    symbol_token = symbol_token.drop(['index'], inplace= False,axis=1)
    symbol_token['strike'] = symbol_token['strike'] /100
    
    for i in range(len(symbol_token)) :
        if symbol_token.strike.iloc[i] > Ltp:
            break
    return(symbol_token.token.iloc[i-1],symbol_token.symbol.iloc[i-1],symbol_token.name.iloc[i-1],symbol_token.expiry.iloc[i-1],symbol_token.strike.iloc[i-1],symbol_token.lotsize.iloc[i-1])

def future_info(name):
    
    df = pd.read_csv('FONSEScripMaster.csv' )
    df['expirydate'] = pd.to_datetime(df['expirydate'])
    symbol_tokendf = df[(df.exch_seg == 'NFO') & (df.instrumentname == 'FUTIDX') & (df.name == name)].sort_values(by='expirydate')
    now = date.today()
   
                             
    for i in range(len(symbol_tokendf)) :
        if now <= symbol_tokendf['expirydate'].iloc[i]:
            break

    return symbol_tokendf.iloc[i]

def candle_data(df,interval='1minute') :

    
    expiry_date = df.expirydate
    expiry_date_format = expiry_date.strftime("%Y-%m-%d %H:%M")
    stock_name=df['name']
    
    to_date= datetime.now()
    from_date = to_date - timedelta(days= 30)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")

    data=breeze.get_historical_data(interval=interval,
                                from_date= from_date_format ,
                                to_date= to_date_format,
                                stock_code= stock_name,
                                exchange_code="NFO",
                                product_type="futures",
                                expiry_date= expiry_date_format,
                                right="others",
                                strike_price="0")
    
    data_df = pd.DataFrame(data['Success'])
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df = data_df.dropna(axis=0)
    return calculate_indicator(data_df)

def calculate_indicator(df):
    
    
    df["ema_long"] = ta.EMA(df['close'],timeperiod=ema_long).round(2)
    df["ema_short"] = ta.EMA(df['close'],timeperiod=ema_short).round(2)
    df["Linear_regression"] = ta.LINEARREG_SLOPE(df['close'], timeperiod= regression)
    
    df['CROSS_UP'] = df['CROSS_DOWN'] = 0
    df = df.round(decimals=2)
    df = df.astype({'close' : float})

    for i in range(20,len(df)):
        if df['ema_short'][i-2]<= df['ema_long'][i-2] and df['ema_short'][i-1] > df['ema_long'][i-1] and df['ema_short'][i] > df['ema_long'][i] and df['close'][i-1] > df['ema_short'][i-1] and df["Linear_regression"][i-1] > -.5 and df["Linear_regression"][i] > df["Linear_regression"][i-1]:
            df['CROSS_UP'][i] = 1
        if df['ema_short'][i-2] >= df['ema_long'][i-2] and df['ema_short'][i-1] < df['ema_long'][i-1] and df['ema_short'][i] < df['ema_long'][i] and df['close'][i-1] < df['ema_short'][i-1] and df["Linear_regression"][i-1] < .5  and df["Linear_regression"][i] < df["Linear_regression"][i-1] :
            df['CROSS_DOWN'][i] = 1

    print(df.tail(5))
    return df

def main_indicator(name):
    
    df_old = future_info(name)
    df = candle_data(df_old)
    return df

def call_candle_data(name1,name2,token):
    
    df_old=calloptions_info(name1,token)
    df = options_candle_data (df_old,name2,'Call')
    return df

def put_candle_data(name1,name2,token):
    
    df_old=putoptions_info(name1,token)
    df = options_candle_data (df_old,name2,'Put')
    return df

def options_candle_data(df,name,right,interval='1minute') :
     
    expiry_date = df[3]
    expiry_date_format = expiry_date.strftime("%Y-%m-%d %H:%M")
    
    strike = df[4]

    to_date= datetime.now()
    from_date = to_date - timedelta(days= 30)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")

    data=breeze.get_historical_data(interval=interval,
                                from_date= from_date_format ,
                                to_date= to_date_format,
                                stock_code= name,
                                exchange_code="NFO",
                                product_type="Options",
                                expiry_date= expiry_date_format,
                                right= right,
                                strike_price= strike )
    
    data_df = pd.DataFrame(data['Success'])
    
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df = data_df.dropna(axis=0)
    data_df = data_df.astype({'close' : float})
    data_df["Linear_regression"] = ta.LINEARREG_SLOPE(data_df['close'], timeperiod= regression)
    df['ATR'] = ta.ATR(df['high'], df['low'],df['close'], timeperiod= atr)
    data_df = data_df.round(decimals=2)
    return data_df



def place_order(token,symbol,qty,buy_sell):

    try:
        orderparams = {
        "variety": "NORMAL",
        "tradingsymbol": symbol,
        "symboltoken": token,
        "transactiontype": buy_sell,
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": "0",
        "squareoff": "0",
        "stoploss": "0",
        "quantity": qty

        }
        orderId=obj.placeOrder(orderparams)
        print("The order id is: {}".format(orderId))
    except Exception as e:
        print("Order placement failed: {}".format(e.message))

def target_order_nifty(token,symbol,qty,ltp,ATR):

    ltp_new = get_options_ltp(symbol,token)

    if (ltp_new >= (ltp + (mul*ATR)) or ltp_new <= (ltp - ATR)) :
        res1= place_order(token,symbol,qty,'SELL')
        print(res1)
        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}')
        TRADED_SYMBOL.clear()

def target_order_banknifty(token,symbol,qty,ltp,ATR):
    
    ltp_new = get_options_ltp(symbol,token)

    if (ltp_new >= (ltp + (mul*ATR)) or ltp_new <= (ltp - ATR)) :
        res1= place_order(token,symbol,qty,'SELL')
        print(res1)
        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}')
        TRADED_SYMBOL.clear()


def exit():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) != 0  :

        symbol= TRADED_SYMBOL[0]
        token = TRADED_SYMBOL[1]
        qty = TRADED_SYMBOL[2]
        ltp= TRADED_SYMBOL[3]
        ATR= TRADED_SYMBOL[4]

        if 'NIFTY' in symbol :
            target_order_nifty(token,symbol,qty,ltp,ATR)
            
        if 'BANKNIFTY' in symbol :
            target_order_banknifty(token,symbol,qty,ltp,ATR)

def login():
    
    refresh_token= data ['data']["refreshToken"]
    d=obj.getProfile(refresh_token)
    e = d['data']['clientcode']
    f= breeze.get_funds()['Success']['bank_account']

    if ((e == 'C142810') or (e == 'STAC1082')) and ((f == '38125477606') or (f == '33200124763')):
        
        print('logged in Bitches $$$$$' )
        interval = timeFrame - datetime.now().second
        print(f"Countdown started {interval} sec")
        sleep(interval)
        checkSignal()
        
    else:
        d= obj.terminateSession(e)['data']
        print(d)


def checkSignal():
    start = time()
    global TRADED_SYMBOL
    
    if len(TRADED_SYMBOL) == 0 :
        
        df_nifty=main_indicator('NIFTY')
        df_bank=main_indicator('CNXBAN')
        

        if len(df_nifty) != 0 :

            if df_nifty['CROSS_UP'].iloc[-1] == 1:

                df1= call_candle_data('NIFTY','NIFTY','26000')
                ATR = df1['ATR'].iloc[-1]
                ATR_mean = df1['ATR'].mean()

                if df1["Linear_regression"].iloc[-1] > 0 and ATR > ATR_mean:

                     
                    token=calloptions_info('NIFTY','26000')[0]
                    symbol =calloptions_info('NIFTY','26000')[1]
                    lotsize = int(calloptions_info('NIFTY','26000')[5])
                    print(symbol)
                
                    ltp = get_options_ltp(symbol,token)
                    qty_old = per_trade_fund / ltp
                    qty= int(qty_old/lotsize) * lotsize
                    
                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')

                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)
                    TRADED_SYMBOL.append(ltp)
                    TRADED_SYMBOL.append(ATR)

            if df_nifty['CROSS_DOWN'].iloc[-1] == 1:

                df2= put_candle_data('NIFTY','NIFTY','26000')
                ATR = df2['ATR'].iloc[-1]
                ATR_mean = df2['ATR'].mean()

                if df2["Linear_regression"].iloc[-1] > 0 and ATR > ATR_mean:
                    
                    token= putoptions_info('NIFTY','26000')[0]
                    symbol = putoptions_info('NIFTY','26000')[1]
                    lotsize = int(putoptions_info('NIFTY','26000')[5])
                    print(symbol)
                
                    ltp = get_options_ltp(symbol,token)
                    qty_old = per_trade_fund / ltp
                    qty= int(qty_old/lotsize) * lotsize

                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)
                    TRADED_SYMBOL.append(ltp)
                    TRADED_SYMBOL.append(ATR)


        if len(df_bank) != 0 :

            if df_bank['CROSS_UP'].iloc[-1] == 1:

                df3= call_candle_data('BANKNIFTY','CNXBAN','26009')
                ATR = df3['ATR'].iloc[-1]
                ATR_mean = df3['ATR'].mean()

                if df3["Linear_regression"].iloc[-1] > 0 and ATR > ATR_mean:

                    
                    token=calloptions_info('BANKNIFTY','26009')[0]
                    symbol = calloptions_info('BANKNIFTY','26009')[1]
                    lotsize = int(calloptions_info('BANKNIFTY','26009')[5])
                    print(symbol)
                
                    ltp = get_options_ltp(symbol,token)
                    qty_old = per_trade_fund / ltp
                    qty= int(qty_old/lotsize) * lotsize
                    
                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')

                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)
                    TRADED_SYMBOL.append(ltp)
                    TRADED_SYMBOL.append(ATR)

            if df_bank['CROSS_DOWN'].iloc[-1] == 1:

                df4= put_candle_data('BANKNIFTY','CNXBAN','26009')
                ATR = df4['ATR'].iloc[-1]
                ATR_mean = df4['ATR'].mean()

                if df4["Linear_regression"].iloc[-1] > 0 and ATR > ATR_mean:

                    
                    token= putoptions_info('BANKNIFTY','26009')[0]
                    symbol = putoptions_info('BANKNIFTY','26009')[1]
                    lotsize = int(putoptions_info('BANKNIFTY','26009')[5])
                    print(symbol)
                
                    ltp = get_options_ltp(symbol,token)
                    qty_old = per_trade_fund / ltp
                    qty= int(qty_old/lotsize) * lotsize

                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)
                    TRADED_SYMBOL.append(ltp)
                    TRADED_SYMBOL.append(ATR)

    else:
        exit() 

    interval = timeFrame - (time()-start)   
    print(interval)
    threading.Timer(interval, checkSignal).start()

if __name__ == '__main__':
    
    obj=SmartConnect(api_key=apikey)
    data = obj.generateSession(username,pwd)
    breeze = BreezeConnect(api_key=api_key_icici) 
    breeze.generate_session(api_secret=secret_key_icici,session_token=session_key)
    login()