import pandas as pd
import pandas_ta as ta
from .base import BaseIndicator
import logging

class RSIIndicator(BaseIndicator):
    def __init__(self, period=14):
        super().__init__("RSI")
        self.period = period
        self.parameters = {'period': period}
    
    def calculate(self, df):
        if not self.validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        if len(df) < self.period:
            logging.error("RSI calculation failed: Not enough data points. Have %d, need %d", len(df), self.period)
            raise ValueError(f"Not enough data points. Need at least {self.period} rows")
        rsi_values = ta.rsi(df['close'], length=self.period)
        return rsi_values
    
    def get_signals(self, df, overbought=70, oversold=30):
        rsi_values = self.calculate(df)
        signals_df = pd.DataFrame(index=df.index)
        signals_df['rsi'] = rsi_values
        signals_df['overbought'] = rsi_values > overbought
        signals_df['oversold'] = rsi_values < oversold
        signals_df['signal'] = 0
        signals_df.loc[signals_df['oversold'], 'signal'] = 1
        signals_df.loc[signals_df['overbought'], 'signal'] = -1
        return signals_df
    
    def get_divergence(self, df, lookback=5):
        rsi_values = self.calculate(df)
        price = df['close']
        divergence = pd.Series(0, index=df.index, name='rsi_divergence')
        for i in range(lookback, len(df)):
            price_trend = price.iloc[i] - price.iloc[i-lookback]
            rsi_trend = rsi_values.iloc[i] - rsi_values.iloc[i-lookback]
            if price_trend < 0 and rsi_trend > 0:
                divergence.iloc[i] = 1
            elif price_trend > 0 and rsi_trend < 0:
                divergence.iloc[i] = -1
        return divergence
