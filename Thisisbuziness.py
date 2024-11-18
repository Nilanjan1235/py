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

per_trade_fund = 40000
ema_short = 10
ema_long = 20
regression = 14
timeFrame = 60 + 5
TRADED_SYMBOL = []

session_key = ''
api_key_icici = 'w+m1950Lf82551z135q24Y9pk2$70b41'
secret_key_icici = ')O961o_uk83l10x361154tr~404Aje11'

apikey = 'ExrLviMc'
username = 'C142810'
pwd = 'Chillout@69'

def nifty_ltp():
    
    ltp= obj.ltpData('NSE','NIFTY','26000')
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


def nifty_calloptions_info():
    
    df = script_token_map()
    Ltp = nifty_ltp()
    symbol_token = df[(df.exch_seg == 'NFO') & (df.instrumenttype == 'OPTIDX') & (df.name == 'NIFTY') & (df['symbol'].str.endswith('CE'))].sort_values(['expiry', 'strike'],
              ascending = [True, True])
    symbol_token = symbol_token.reset_index(inplace=False)
    symbol_token = symbol_token.drop(['index'], inplace= False,axis=1)
    symbol_token['strike'] = symbol_token['strike'] /100

    
    for i in range(len(symbol_token)) :
        if symbol_token.strike.iloc[i] > Ltp:
            break
    return(symbol_token.token.iloc[i],symbol_token.symbol.iloc[i],symbol_token.name.iloc[i],symbol_token.expiry.iloc[i],symbol_token.strike.iloc[i])
    

def nifty_putoptions_info():
    
    df = script_token_map()
    Ltp = nifty_ltp()
    symbol_token = df[(df.exch_seg == 'NFO') & (df.instrumenttype == 'OPTIDX') & (df.name == 'NIFTY') & (df['symbol'].str.endswith('PE'))].sort_values(['expiry', 'strike'],
              ascending = [True, True])
    symbol_token = symbol_token.reset_index(inplace=False)
    symbol_token = symbol_token.drop(['index'], inplace= False,axis=1)
    symbol_token['strike'] = symbol_token['strike'] /100
    
    for i in range(len(symbol_token)) :
        if symbol_token.strike.iloc[i] > Ltp:
            break
    return(symbol_token.token.iloc[i-1],symbol_token.symbol.iloc[i-1],symbol_token.name.iloc[i-1],symbol_token.expiry.iloc[i-1],symbol_token.strike.iloc[i-1])

def nifty_future_info():
    
    df = pd.read_csv('FONSEScripMaster.csv' )
    df['expirydate'] = pd.to_datetime(df['expirydate'])
    symbol_tokendf = df[(df.exch_seg == 'NFO') & (df.instrumentname == 'FUTIDX') & (df.name == 'NIFTY')].sort_values(by='expirydate')
    return symbol_tokendf

def nifty_candle_data(interval='1minute') :
    
    
    df = nifty_future_info() 
    expiry_date = df.expirydate.iloc[0]
    expiry_date_format = expiry_date.strftime("%Y-%m-%d %H:%M")
    stock_name=df.name.iloc[0]

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

    return data_df

def nifty_calculate_indicator():
    
    df = nifty_candle_data()
    df["ema_long"] = ta.EMA(df['close'],timeperiod=ema_long).round(2)
    df["ema_short"] = ta.EMA(df['close'],timeperiod=ema_short).round(2)
    df["Linear_regression"] = ta.LINEARREG_SLOPE(df['close'], timeperiod= regression)
    
    df['CROSS_UP'] = df['CROSS_DOWN'] = 0
    df = df.round(decimals=2)
    df = df.astype({'close' : float})

    for i in range(20,len(df)):
        if df['ema_short'][i-1]<= df['ema_long'][i-1] and df['ema_short'][i] > df['ema_long'][i] and df['close'][i] > df['ema_short'][i] and df["Linear_regression"][i] > 0 :
            df['CROSS_UP'][i] = 1
        if df['ema_short'][i-1] >= df['ema_long'][i-1] and df['ema_short'][i] < df['ema_long'][i] and df['close'][i] < df['ema_short'][i] and df["Linear_regression"][i] < 0 :
            df['CROSS_DOWN'][i] = 1


    print(df.tail(10))
    return df

def calloptions_candle_data(interval='1minute') :
    
    
    df = nifty_calloptions_info()
    expiry_date = df[3]
    expiry_date_format = expiry_date.strftime("%Y-%m-%d %H:%M")
    stock_name=df[2]
    strike = df[4]

    to_date= datetime.now()
    from_date = to_date - timedelta(days= 30)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")

    data=breeze.get_historical_data(interval=interval,
                                from_date= from_date_format ,
                                to_date= to_date_format,
                                stock_code= stock_name,
                                exchange_code="NFO",
                                product_type="Options",
                                expiry_date= expiry_date_format,
                                right="Call",
                                strike_price= strike )
    
    data_df = pd.DataFrame(data['Success'])
    
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df = data_df.dropna(axis=0)
    data_df = data_df.astype({'close' : float})
    data_df["Linear_regression"] = ta.LINEARREG_SLOPE(data_df['close'], timeperiod= regression)
    data_df = data_df.round(decimals=2)
    return data_df
    

def putoptions_candle_data(interval='1minute') :
    
    
    df = nifty_putoptions_info()
    expiry_date = df[3]
    expiry_date_format = expiry_date.strftime("%Y-%m-%d %H:%M")
    stock_name=df[2]
    strike = df[4]

    to_date= datetime.now()
    from_date = to_date - timedelta(days= 30)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")

    data=breeze.get_historical_data(interval=interval,
                                from_date= from_date_format ,
                                to_date= to_date_format,
                                stock_code= stock_name,
                                exchange_code="NFO",
                                product_type="Options",
                                expiry_date= expiry_date_format,
                                right="Put",
                                strike_price= strike )
    
    data_df = pd.DataFrame(data['Success'])
    
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df = data_df.dropna(axis=0)
    data_df = data_df.astype({'close' : float})
    data_df["Linear_regression"] = ta.LINEARREG_SLOPE(data_df['close'], timeperiod= regression)
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

def target_order_call(token,symbol,qty):
    
    df= calloptions_candle_data()
    if df["Linear_regression"].iloc[-1] < 0:
        res1= place_order(token,symbol,qty,'SELL')
        print(res1)
        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}')
   

def target_order_put(token,symbol,qty):
    
    df= putoptions_candle_data() 
    if df["Linear_regression"].iloc[-1] < 0:
        res1= place_order(token,symbol,qty,'SELL')
        print(res1)
        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}') 


def checkSignal():
    start = time()
    global TRADED_SYMBOL
    
    if len(TRADED_SYMBOL) == 0 :
        df=nifty_calculate_indicator()
     
        if len(df) != 0 :
            if df['CROSS_UP'].iloc[-1] == 1 :

                df1= calloptions_candle_data()
                if df1["Linear_regression"].iloc[-1] > 0:

                    call_strike = nifty_calloptions_info()
                    token= call_strike[0]
                    symbol = call_strike[1]
                    print(symbol)
                
                    ltp = df1["close"].iloc[-1]
                    qty = int(per_trade_fund / ltp*50)
                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')

                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)

            if df['CROSS_DOWN'].iloc[-1] == 1 :

                df2= putoptions_candle_data()
                if df2["Linear_regression"].iloc[-1] > 0:

                    put_strike = nifty_putoptions_info()
                    token= put_strike[0]
                    symbol = put_strike[1]
                    print(symbol)
                
                    ltp = df2["close"].iloc[-1]
                    qty = int(per_trade_fund / ltp*50) 
                    res1= place_order(token,symbol,qty,'BUY') #buy order
                    print(res1)
                    print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                    TRADED_SYMBOL.append(symbol)
                    TRADED_SYMBOL.append(token)
                    TRADED_SYMBOL.append(qty)
                
                  
    interval = timeFrame - (time()-start)   
    print(interval)
    threading.Timer(interval, checkSignal).start()

def exit():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) != 0  :

        symbol= TRADED_SYMBOL[0]
        token = TRADED_SYMBOL[1]
        qty = TRADED_SYMBOL[2]

        if 'CE' in symbol :
            target_order_call(token,symbol,qty)
            
        if 'PE' in symbol :
            target_order_put(token,symbol,qty)


if __name__ == '__main__':
    
    obj=SmartConnect(api_key=apikey)
    data = obj.generateSession(username,pwd)
    breeze = BreezeConnect(api_key=api_key_icici) 
    breeze.generate_session(api_secret=secret_key_icici,session_token=session_key)
    
    interval = timeFrame - datetime.now().second
    print(f"Code run after {interval} sec")
    sleep(interval)
    checkSignal()
    exit()
