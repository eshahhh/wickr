import pandas as pd
import pandas_ta as ta
from .base import BaseIndicator
import logging

class MACDIndicator(BaseIndicator):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        super().__init__("MACD")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.parameters = {
            'fast_period': fast_period,
            'slow_period': slow_period,
            'signal_period': signal_period
        }
    
    def calculate(self, df):
        if not self.validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        
        min_periods = max(self.slow_period, self.fast_period) + self.signal_period
        if len(df) < min_periods:
            logging.error("MACD calculation failed: Not enough data points. Have %d, need %d", len(df), min_periods)
            raise ValueError(f"Not enough data points. Need at least {min_periods} rows")
        
        macd_data = ta.macd(
            df['close'],
            fast=self.fast_period,
            slow=self.slow_period,
            signal=self.signal_period
        )
        
        macd_df = pd.DataFrame(index=df.index)
        macd_df['macd'] = macd_data[f'MACD_{self.fast_period}_{self.slow_period}_{self.signal_period}']
        macd_df['signal'] = macd_data[f'MACDs_{self.fast_period}_{self.slow_period}_{self.signal_period}']
        macd_df['histogram'] = macd_data[f'MACDh_{self.fast_period}_{self.slow_period}_{self.signal_period}']
        
        return macd_df
    
    def get_signals(self, df):
        macd_data = self.calculate(df)
        
        signals_df = macd_data.copy()
        signals_df['signal_type'] = 0
        
        macd_cross_up = (
            (macd_data['macd'] > macd_data['signal']) &
            (macd_data['macd'].shift(1) <= macd_data['signal'].shift(1))
        )
        
        macd_cross_down = (
            (macd_data['macd'] < macd_data['signal']) &
            (macd_data['macd'].shift(1) >= macd_data['signal'].shift(1))
        )
        
        signals_df.loc[macd_cross_up, 'signal_type'] = 1
        signals_df.loc[macd_cross_down, 'signal_type'] = -1
        
        signals_df['bullish_momentum'] = (
            (macd_data['macd'] > macd_data['signal']) &
            (macd_data['histogram'] > 0)
        )
        
        signals_df['bearish_momentum'] = (
            (macd_data['macd'] < macd_data['signal']) &
            (macd_data['histogram'] < 0)
        )
        
        return signals_df
    
    def get_divergence(self, df, lookback=10):
        macd_data = self.calculate(df)
        price = df['close']
        
        divergence = pd.Series(0, index=df.index, name='macd_divergence')
        
        for i in range(lookback, len(df)):
            price_trend = price.iloc[i] - price.iloc[i-lookback]
            macd_trend = macd_data['macd'].iloc[i] - macd_data['macd'].iloc[i-lookback]
            
            if price_trend < 0 and macd_trend > 0:
                divergence.iloc[i] = 1
            elif price_trend > 0 and macd_trend < 0:
                divergence.iloc[i] = -1
        
        return divergence
    
    def get_zero_line_cross(self, df):
        macd_data = self.calculate(df)
        
        crosses_df = pd.DataFrame(index=df.index)
        crosses_df['macd'] = macd_data['macd']
        
        crosses_df['zero_cross_up'] = (
            (macd_data['macd'] > 0) &
            (macd_data['macd'].shift(1) <= 0)
        )
        
        crosses_df['zero_cross_down'] = (
            (macd_data['macd'] < 0) &
            (macd_data['macd'].shift(1) >= 0)
        )
        
        return crosses_df
