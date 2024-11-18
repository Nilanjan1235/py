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


ema_short = 20
ema_mid =50
ema_long = 200
regression = 14
timeFrame = 60 + 5
mul = 1
sl_mul = 0.5
TRADED_SYMBOL = []


session_key = '1605294'
api_key_icici = 'w+m1950Lf82551z135q24Y9pk2$70b41'
secret_key_icici = ')O961o_uk83l10x361154tr~404Aje11'

apikey_chiru = 'ExrLviMc'
username_chiru = 'C142810'
pwd_chiru = 'Chillout@69'

apikey_me = '4jz3PPa7'
username_me = 'STAC1082'
pwd_me = 'Marooned@14'


def get_options_ltp(symbol,token):
    
    ltp= obj_1.ltpData('NFO',symbol,token)
    Ltp= ltp['data']['ltp']
    return Ltp

def get_future_ltp(symbol,token):
    
    ltp= obj_1.ltpData('NSE',symbol,token)
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
    return data_df

def calculate_indicator(df):
    
    df["ema_long"] = ta.EMA(df['close'], timeperiod=ema_long)
    df["ema_short"] = ta.EMA(df['close'], timeperiod=ema_short)
    df["ema_mid"] = ta.EMA(df['close'], timeperiod=ema_mid)
    df['CROSS_UP'] = df['CROSS_DOWN'] = 0
    df = df.astype({'close': float})

    for i in range(20,len(df)):
        if df['ema_short'][i-3]< df['ema_mid'][i-3] and df['ema_short'][i-2]< df['ema_mid'][i-2] and df['ema_short'][i-1] > df['ema_mid'][i-1] and df['ema_short'][i] > df['ema_mid'][i] and df['close'][i] > df['ema_short'][i] :
            df['CROSS_UP'][i] = 1
        if df['ema_short'][i-3] > df['ema_mid'][i-3] and df['ema_short'][i-2] > df['ema_mid'][i-2] and df['ema_short'][i-1] < df['ema_mid'][i-1] and df['ema_short'][i] < df['ema_mid'][i] and df['close'][i] < df['ema_short'][i]   :
            df['CROSS_DOWN'][i] = 1
        if df['ema_mid'][i-3] < df['ema_long'][i-3] and df['ema_mid'][i-2]< df['ema_long'][i-2] and df['ema_mid'][i-1] > df['ema_long'][i-1] and df['ema_mid'][i] > df['ema_long'][i] and df['close'][i] > df['ema_mid'][i] :
            df['CROSS_UP'][i] = 1
        if df['ema_mid'][i-3] > df['ema_long'][i-3] and df['ema_mid'][i-2] > df['ema_long'][i-2] and df['ema_mid'][i-1] < df['ema_long'][i-1] and df['ema_mid'][i] < df['ema_long'][i] and df['close'][i] < df['ema_mid'][i]   :
            df['CROSS_DOWN'][i] = 1

    print(df.tail(5))
    return df

def main_indicator(name):
    
    df_old = future_info(name)
    df1 = candle_data(df_old)
    df_new = calculate_indicator(df1)
    return df_new

def call_candle_data(name1,name2,token,interval='1minute'):
    
    df_old=calloptions_info(name1,token)
    df = options_candle_data (df_old,name2,'Call',interval)
    return df

def put_candle_data(name1,name2,token,interval='1minute'):
    
    df_old=putoptions_info(name1,token)
    df = options_candle_data (df_old,name2,'Put',interval)
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
    data_df = data_df.drop(['open_interest'], inplace= False,axis=1)
    data_df = data_df.dropna(axis=0)
    data_df = data_df.astype({'close' : float})
    data_df = data_df.astype({'volume' : float})
    data_df["Linear_regression"] = ta.LINEARREG_SLOPE(data_df['close'], timeperiod= 14)
    data_df['ATR'] = ta.ATR(data_df['high'], data_df['low'],data_df['close'], timeperiod= 14)
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
        orderId_1=obj_1.placeOrder(orderparams)
        orderId_2=obj_2.placeOrder(orderparams)
        print("The order id is: {}".format(orderId_1))
        print("The order id is: {}".format(orderId_2))

    except Exception as e:
        print("Order placement failed: {}".format(e.message))

def target_order_nifty(token,symbol,qty,ltp,ATR_last):

    price = get_options_ltp(symbol,token)
    if (price >= (ltp + (mul*ATR_last))) or (price <= (ltp - (sl_mul*ATR_last)))  :
        res1= place_order(token,symbol,qty,'SELL')

        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}')
        TRADED_SYMBOL.clear()

def target_order_banknifty(token,symbol,qty,ltp,ATR_last):
    
    price = get_options_ltp(symbol,token)
    if (price >= (ltp + (mul*ATR_last))) or (price <= (ltp - (sl_mul*ATR_last))) :
        res1= place_order(token,symbol,qty,'SELL')

        print(f'Order Exited for {symbol} QTY {qty} at {datetime.now()}')
        TRADED_SYMBOL.clear()

def login():
    
    refresh_token= data ['data']["refreshToken"]
    d=obj_1.getProfile(refresh_token)
    e = d['data']['clientcode']
    f= breeze.get_funds()['Success']['bank_account']

    if ((e == 'C142810') or (e == 'STAC1082')) and ((f == '38125477606') or (f == '33200124763')):
        
        print('logged in' )
        interval = timeFrame - datetime.now().second
        print(f"Countdown started {interval} sec")
        sleep(interval)
        checkSignal()
        
    else:
        d= obj_1.terminateSession(e)['data']
        print(d)

def exit():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) != 0  :

        symbol= TRADED_SYMBOL[0]
        token = TRADED_SYMBOL[1]
        qty = TRADED_SYMBOL[2]
        ltp= TRADED_SYMBOL[3]
        ATR_last = TRADED_SYMBOL[4]
        

        if 'NIFTY' in symbol :
            target_order_nifty(token,symbol,qty,ltp,ATR_last)
            
        if 'BANKNIFTY' in symbol :
            target_order_banknifty(token,symbol,qty,ltp,ATR_last)


def checkSignal():
    start = time()
    global TRADED_SYMBOL
    
    if len(TRADED_SYMBOL) == 0 :
        
        df_nifty=main_indicator('NIFTY')

        if len(df_nifty) != 0 :

            if df_nifty['CROSS_UP'].iloc[-1] == 1 or df_nifty['CROSS_UP'].iloc[-2] == 1:

                df= call_candle_data('NIFTY','NIFTY','26000')
                ATR_last = df['ATR'].iloc[-1]
                token=calloptions_info('NIFTY','26000')[0]
                symbol =calloptions_info('NIFTY','26000')[1]
                lotsize = int(calloptions_info('NIFTY','26000')[5])
                print(symbol)             
                ltp = get_options_ltp(symbol,token)
                qty_old = per_trade_fund / ltp
                qty= int(qty_old/lotsize) * lotsize                 
                res1= place_order(token,symbol,qty,'BUY') #buy order
                print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                TRADED_SYMBOL.append(symbol)
                TRADED_SYMBOL.append(token)
                TRADED_SYMBOL.append(qty)
                TRADED_SYMBOL.append(ltp)
                TRADED_SYMBOL.append(ATR_last)


            if df_nifty['CROSS_DOWN'].iloc[-1] or df_nifty['CROSS_DOWN'].iloc[-2] == 1:

                df= put_candle_data('NIFTY','NIFTY','26000')
                ATR_last = df['ATR'].iloc[-1]
                token= putoptions_info('NIFTY','26000')[0]
                symbol = putoptions_info('NIFTY','26000')[1]
                lotsize = int(putoptions_info('NIFTY','26000')[5])
                print(symbol)               
                ltp = get_options_ltp(symbol,token)
                qty_old = per_trade_fund / ltp
                qty= int(qty_old/lotsize) * lotsize
                res1= place_order(token,symbol,qty,'BUY') #buy order       
                print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                TRADED_SYMBOL.append(symbol)
                TRADED_SYMBOL.append(token)
                TRADED_SYMBOL.append(qty)
                TRADED_SYMBOL.append(ltp)
                TRADED_SYMBOL.append(ATR_last)

    if len(TRADED_SYMBOL) == 0 :

        df_bank=main_indicator('CNXBAN')

        if len(df_bank) != 0 :

            if df_bank['CROSS_UP'].iloc[-1] or df_bank['CROSS_UP'].iloc[-2] == 1:

                df= call_candle_data('BANKNIFTY','CNXBAN','26009')
                ATR_last = df['ATR'].iloc[-1]
                token=calloptions_info('BANKNIFTY','26009')[0]
                symbol = calloptions_info('BANKNIFTY','26009')[1]
                lotsize = int(calloptions_info('BANKNIFTY','26009')[5])
                print(symbol)
                ltp = get_options_ltp(symbol,token)
                qty_old = per_trade_fund / ltp
                qty= int(qty_old/lotsize) * lotsize                  
                res1= place_order(token,symbol,qty,'BUY') #buy order       
                print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                TRADED_SYMBOL.append(symbol)
                TRADED_SYMBOL.append(token)
                TRADED_SYMBOL.append(qty)
                TRADED_SYMBOL.append(ltp)
                TRADED_SYMBOL.append(ATR_last)
                    
            if df_bank['CROSS_DOWN'].iloc[-1] or df_bank['CROSS_DOWN'].iloc[-2] == 1:

                df= put_candle_data('BANKNIFTY','CNXBAN','26009')
                ATR_last = df['ATR'].iloc[-1]
                token= putoptions_info('BANKNIFTY','26009')[0]
                symbol = putoptions_info('BANKNIFTY','26009')[1]
                lotsize = int(putoptions_info('BANKNIFTY','26009')[5])
                print(symbol)
                ltp = get_options_ltp(symbol,token)
                qty_old = per_trade_fund / ltp
                qty= int(qty_old/lotsize) * lotsize
                res1= place_order(token,symbol,qty,'BUY') #buy order        
                print(f'Order Placed for {symbol} QTY {qty} at {datetime.now()}')
                    
                TRADED_SYMBOL.append(symbol)
                TRADED_SYMBOL.append(token)
                TRADED_SYMBOL.append(qty)
                TRADED_SYMBOL.append(ltp)
                TRADED_SYMBOL.append(ATR_last)
                   
    else:
        exit() 

    interval = timeFrame - (time()-start) 
    print(interval)  
    threading.Timer(interval, checkSignal).start()

if __name__ == '__main__':
    
    obj_1=SmartConnect(api_key=apikey_chiru)
    data = obj_1.generateSession(username_chiru,pwd_chiru)
    obj_2=SmartConnect(api_key=apikey_me)
    data = obj_2.generateSession(username_me,pwd_me)
    session_key = input('Enter session key = ')
    breeze = BreezeConnect(api_key=api_key_icici) 
    breeze.generate_session(api_secret=secret_key_icici,session_token=session_key)
    per_trade_fund = int(input('Enter per trade fund = '))
    login()
    

    