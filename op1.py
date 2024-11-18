import keyboard
from smartapi import SmartConnect
import pandas as pd
import requests
import numpy as np
from datetime import datetime,date,time,timedelta
from time import time, sleep
import warnings
warnings.filterwarnings('ignore')

apikey_chiru = 'ExrLviMc'
username_chiru = 'C142810'
pwd_chiru = 'Chillout@69'

obj_1=SmartConnect(api_key=apikey_chiru)
data = obj_1.generateSession(username_chiru,pwd_chiru)

print('press (a) to buy nifty call, (q) to buy banknifty call')
print('press (s) to buy nifty put, (w) to buy banknifty put')
print('press (d) to exit trade, (r) to add lot ')

TRADED_SYMBOL = []
per_trade_fund = 5000
per_trade_fund_newlot = 5000

def get_future_ltp(symbol,token):
    
    ltp= obj_1.ltpData('NSE',symbol,token)
    Ltp= ltp['data']['ltp']
    return Ltp

def get_options_ltp(symbol,token):
    
    ltp= obj_1.ltpData('NFO',symbol,token)
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
        
        print("The order id is: {}".format(orderId_1))
        

    except Exception as e:
        print("Order placement failed: {}".format(e.message))

def entry_nifty_call():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) == 0  :

        token=calloptions_info('NIFTY','26000')[0]
        symbol =calloptions_info('NIFTY','26000')[1]
        lotsize = int(calloptions_info('NIFTY','26000')[5])
        print(symbol)             
        ltp = get_options_ltp(symbol,token)
        qty_old = per_trade_fund / ltp
        qty= int(qty_old/lotsize) * lotsize                 
        res1= place_order(token,symbol,qty,'BUY') #buy order
        print("order placed")
        
        TRADED_SYMBOL.append(symbol)
        TRADED_SYMBOL.append(token)
        TRADED_SYMBOL.append(lotsize)
        TRADED_SYMBOL.append(qty)
        

def entry_banknifty_call():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) == 0  :

        token=calloptions_info('BANKNIFTY','26009')[0]
        symbol =calloptions_info('BANKNIFTY','26009')[1]
        lotsize = int(calloptions_info('BANKNIFTY','26009')[5])
        print(symbol)             
        ltp = get_options_ltp(symbol,token)
        qty_old = per_trade_fund / ltp
        qty= int(qty_old/lotsize) * lotsize                 
        res1= place_order(token,symbol,qty,'BUY') #buy order
        print("order placed")
        
        TRADED_SYMBOL.append(symbol)
        TRADED_SYMBOL.append(token)
        TRADED_SYMBOL.append(lotsize)
        TRADED_SYMBOL.append(qty)
        

def entry_nifty_put():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) == 0  :

        token= putoptions_info('NIFTY','26000')[0]
        symbol = putoptions_info('NIFTY','26000')[1]
        lotsize = int(putoptions_info('NIFTY','26000')[5])
        print(symbol)               
        ltp = get_options_ltp(symbol,token)
        qty_old = per_trade_fund / ltp
        qty= int(qty_old/lotsize) * lotsize
        res1= place_order(token,symbol,qty,'BUY') #buy order
        print("order placed")
            
        TRADED_SYMBOL.append(symbol)
        TRADED_SYMBOL.append(token)
        TRADED_SYMBOL.append(lotsize)
        TRADED_SYMBOL.append(qty)
        

def entry_banknifty_put():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) == 0  :

        token= putoptions_info('BANKNIFTY','26009')[0]
        symbol = putoptions_info('BANKNIFTY','26009')[1]
        lotsize = int(putoptions_info('BANKNIFTY','26009')[5])
        print(symbol)               
        ltp = get_options_ltp(symbol,token)
        qty_old = per_trade_fund / ltp
        qty= int(qty_old/lotsize) * lotsize
        res1= place_order(token,symbol,qty,'BUY') #buy order
        print("order placed")
            
        TRADED_SYMBOL.append(symbol)
        TRADED_SYMBOL.append(token)
        TRADED_SYMBOL.append(lotsize)
        TRADED_SYMBOL.append(qty)
        

def add_lot():
    
    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) != 0  :

        symbol= TRADED_SYMBOL[0]
        token = TRADED_SYMBOL[1]  
        lotsize = TRADED_SYMBOL[2]
        qty = TRADED_SYMBOL[3]

        ltp_new = get_options_ltp(symbol,token)
        qty_old = per_trade_fund_newlot / ltp_new
        qty_1= int(qty_old/lotsize) * lotsize
        qty_lot = int(qty_1 / lotsize)
        res1= place_order(token,symbol,qty_1,'BUY')
        qty_new = qty + qty_1
        print(f"{qty_lot} lot added")
        TRADED_SYMBOL.remove(qty)
        TRADED_SYMBOL.append(qty_new)

def exit():

    global TRADED_SYMBOL

    if len(TRADED_SYMBOL) != 0  :

        symbol= TRADED_SYMBOL[0]
        token = TRADED_SYMBOL[1]
        qty = TRADED_SYMBOL[3]

        res1= place_order(token,symbol,qty,'SELL')
        print("order exited")
        TRADED_SYMBOL.clear()

while True:

        if keyboard.read_key() == 'a':
            print('processing trade')
            entry_nifty_call()
        if keyboard.read_key() == 's':
            print('processing trade')
            entry_nifty_put()
        if keyboard.read_key() == 'q':
            print('processing trade')
            entry_banknifty_call()
        if keyboard.read_key() == 'w':
            print('processing trade')
            entry_banknifty_put()
        if keyboard.read_key() == 'r':
            print('processing trade')
            add_lot()
        if keyboard.read_key() == 'd':
            print('processing trade')
            exit()


        


  

