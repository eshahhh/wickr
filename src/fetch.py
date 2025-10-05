import os
import requests
import pandas as pd
import json
import logging
import asyncio
from datetime import datetime
from collections import deque
import websockets

logger = logging.getLogger(__name__)

class BinanceDataFetcher:
    def __init__(self, data_dir="data"):
        self.base_url = "https://api.binance.com/api/v3"
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
    
    def fetch_klines(self, symbol="BTCUSDT", interval="5m", limit=100):
        endpoint = f"{self.base_url}/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            print(f"Fetching {limit} {interval} candles for {symbol}...")
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("No data received")
                return None
            
            df = self._process_klines(data)
            print(f"Fetched {len(df)} candles")
            return df
            
        except Exception as e:
            print(e)
            return None
    
    def _process_klines(self, raw_data):
        columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades_count',
            'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
        ]
        
        df = pd.DataFrame(raw_data, columns=columns)
        df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_timestamp'] = pd.to_datetime(df['close_time'], unit='ms')
        
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trades_count', 'close_timestamp']]
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    def save(self, df, filename="btcusdt_5m_candles.csv"):
        try:
            filepath = os.path.join(self.data_dir, filename)
            df.to_csv(filepath, index=False)
            print(f"Data saved to {filepath}")
            return True
        except Exception as e:
            print(e)
            return False


class BinanceWebSocketClient:
    def __init__(self, symbol="BTCUSDT", interval="1s", buffer_size=35):
        self.symbol = symbol
        self.interval = interval
        self.buffer_size = buffer_size
        self.candle_buffer = deque(maxlen=buffer_size)
        self.latest_price = None
        self.is_connected = False
        self.callbacks = {
            'on_candle_closed': [],
            'on_price_update': []
        }
    
    def register_callback(self, event, callback):
        if event in self.callbacks:
            self.callbacks[event].append(callback)
    
    async def _trigger_callbacks(self, event, *args, **kwargs):
        for callback in self.callbacks.get(event, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    callback(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in callback for {event}: {e}")
    
    async def fetch_initial_candles(self):
        logger.info(f"Fetching initial {self.buffer_size} candles for {self.symbol}...")
        
        fetcher = BinanceDataFetcher()
        df = fetcher.fetch_klines(
            symbol=self.symbol,
            interval=self.interval,
            limit=self.buffer_size
        )
        
        if df is None or df.empty:
            logger.error("Failed to fetch initial candles")
            return False
        
        for _, row in df.iterrows():
            candle = {
                'timestamp': pd.to_datetime(row['timestamp']),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row['volume']),
                'trades_count': int(row['trades_count']),
            }
            self.candle_buffer.append(candle)
        
        if len(self.candle_buffer) > 0:
            self.latest_price = self.candle_buffer[-1]['close']
        
        logger.info(f"Initialized buffer with {len(self.candle_buffer)} candles")
        logger.info(f"Latest price: ${self.latest_price}")
        return True
    
    def get_buffer_as_dataframe(self):
        if not self.candle_buffer:
            return pd.DataFrame()
        return pd.DataFrame(list(self.candle_buffer))
    
    async def connect_and_stream(self):
        symbol_lower = self.symbol.lower()
        uri = f"wss://stream.binance.com:9443/ws/{symbol_lower}@kline_{self.interval}"
        
        while True:
            try:
                logger.info(f"Connecting to Binance WebSocket: {uri}")
                async with websockets.connect(uri) as websocket:
                    self.is_connected = True
                    logger.info("Connected to Binance WebSocket!")
                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            
                            if 'k' in data:
                                kline = data['k']
                                is_closed = kline['x']
                                
                                candle = {
                                    'timestamp': pd.to_datetime(kline['t'], unit='ms'),
                                    'open': float(kline['o']),
                                    'high': float(kline['h']),
                                    'low': float(kline['l']),
                                    'close': float(kline['c']),
                                    'volume': float(kline['v']),
                                    'trades_count': int(kline['n']),
                                }
                                
                                self.latest_price = candle['close']
                                
                                await self._trigger_callbacks(
                                    'on_price_update',
                                    price=candle['close'],
                                    timestamp=candle['timestamp'],
                                    is_closed=is_closed
                                )
                                
                                if is_closed:
                                    logger.info(f"New candle closed: {candle['timestamp']} - ${candle['close']}")
                                    
                                    self.candle_buffer.append(candle)
                                    
                                    await self._trigger_callbacks(
                                        'on_candle_closed',
                                        candle=candle,
                                        buffer=self.candle_buffer
                                    )
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            
            except websockets.exceptions.WebSocketException as e:
                self.is_connected = False
                logger.error(f"WebSocket error: {e}")
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                self.is_connected = False
                logger.error(f"Unexpected error: {e}", exc_info=True)
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

def main():
    print("Crypto Signal Generator")
    
    fetcher = BinanceDataFetcher(data_dir="data")
    df = fetcher.fetch_klines(symbol="BTCUSDT", interval="5m", limit=100)
    
    if df is not None and not df.empty:
        fetcher.save(df, "btcusdt_5m_candles.csv")
    else:
        print("Failed to fetch data")
        return 1
    
    return 0

if __name__ == "__main__":
    main()
