from huobi.HuobiWebsocket import HuobiWebsocket
if __name__ == '__main__':
    hb = HuobiWebsocket(endpoint='wss://api.huobipro.com/ws',
                        symbol='btcusdt',
                        api_key=None,
                        api_secret=None)
    while hb.connected():
        #hb.get_kline(ktype='1min')
        df = hb.get_trade(sleep_sec=1)
        if df.shape[0]: 
            print( df.head(2).transpose() )
