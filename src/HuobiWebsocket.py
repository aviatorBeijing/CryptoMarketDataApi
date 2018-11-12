'''
Created on Nov 7, 2018

@author: junma
'''
import websocket
import threading
import traceback
from time import sleep
import json
import logging
import urllib
import math
import gzip
import pandas as pd
from random import randint

logging.basicConfig(level=logging.INFO)

def on_error( e ):
    print('$')
    logging.warn( e )
    raise websocket.WebSocketException(e)

class HuobiWebsocket:
    def __init__(self, endpoint:str, 
                 symbol:str, 
                 api_key:str=None, 
                 api_secret:str=None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.endpoint = endpoint
        self.symbol = symbol

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        wsURL = self.__get_url()
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')
    
    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()
        
    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close(),
                                         on_open=self.__on_open(),
                                         on_error=on_error, #self.__on_error(),
                                         header=self.__get_auth())
        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        print( 'sending command ...')
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))
        
    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        #while not {'instrument', 'trade', 'quote'} <= set(self.data):
        #    sleep(0.1)
        return
    
    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        genericSubs = ["margin"]

        subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]
        subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('https', 'wss').replace('http','ws')
        #urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)
    def __on_message(self, ws, compressed):
        '''Handler for parsing WS messages.'''
        message=gzip.decompress(compressed).decode('utf-8')
        msgObj = json.loads(message)
        if 'ping' in msgObj.keys():
            ws.send('{"pong":%s}'%(msgObj['ping']))
            logging.debug("responding to 'ping'...")
        else:
            self.data = msgObj
    
    def __on_error(self, error="default error"):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)

    def __on_open(self):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')
    
    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            """
            expires = generate_nonce()
            return [
                "api-expires: " + str(expires),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', expires, ''),
                "api-key:" + self.api_key
                ]
            """
            return []
        else:
            self.logger.info("Not authenticating.")
            return []
        
    def get_kline(self, ktype='15min', sleep_sec=1 ):
        klineparams = "market.{0}.kline.{1}".format(self.symbol, ktype)
        params = """{"sub": "%s","id": "%s"}"""%( klineparams,
                                                  'id_kline_{0}_{1}'.format(self.symbol, ktype) )
        self.ws.send( str(params))
        if 'tick' in self.data:
            d = self.data['tick']
            d['timestamp'] = self.data['ts']
            df = pd.DataFrame( [d] )
            self.logger.info(self.data['ts'])
            self.logger.info(df.transpose())
        
        sleep(sleep_sec)
        return self.data
    
    def get_trade(self, sleep_sec=1):
        tradeparams = "market.%s.trade.detail"%self.symbol
        params = """{"req": "%s","id": "%s"}"""%( 
            tradeparams,
            'id_trade_{0}_{1}'.format(self.symbol, randint(10,10000)) )
        self.ws.send( str(params))
        
        df = pd.DataFrame()
        if 'data' in self.data:
            df = pd.DataFrame( self.data['data'])
            #self.logger.info( df.head(1).transpose() )
            
        sleep(sleep_sec)
        return df
    
    def connected(self):
        return self.ws.sock.connected
    
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
    
