import os
import requests
import pandas as pd
import json
from datetime import datetime

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
