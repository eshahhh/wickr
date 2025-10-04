import pandas as pd
import pandas_ta as ta
import numpy as np
import logging
from .base import BaseIndicator

class BollingerBandsIndicator(BaseIndicator):
    def __init__(self, period=20, std_dev=2.0):
        super().__init__("Bollinger Bands")
        self.period = period
        self.std_dev = std_dev
        self.parameters = {'period': period, 'std_dev': std_dev}
    
    def calculate(self, df):
        if not self.validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        if len(df) < self.period:
            logging.error("Bollinger Bands calculation failed: Not enough data points. Have %d, need %d", len(df), self.period)
            raise ValueError(f"Not enough data points. Need at least {self.period} rows")
        bb_data = ta.bbands(df['close'], length=self.period, std=self.std_dev)
        bb_df = pd.DataFrame(index=df.index)
        
        lower_col = [col for col in bb_data.columns if col.startswith('BBL_')]
        middle_col = [col for col in bb_data.columns if col.startswith('BBM_')]
        upper_col = [col for col in bb_data.columns if col.startswith('BBU_')]
        
        if not lower_col or not middle_col or not upper_col:
            raise ValueError(f"Bollinger Bands columns not found. Available columns: {list(bb_data.columns)}")
        
        bb_df['bb_lower'] = bb_data[lower_col[0]]
        bb_df['bb_middle'] = bb_data[middle_col[0]]
        bb_df['bb_upper'] = bb_data[upper_col[0]]
        bb_df['bb_width'] = (bb_df['bb_upper'] - bb_df['bb_lower']) / bb_df['bb_middle']
        bb_df['bb_percent'] = (df['close'] - bb_df['bb_lower']) / (bb_df['bb_upper'] - bb_df['bb_lower'])
        return bb_df
    
    def get_signals(self, df):
        bb_data = self.calculate(df)
        signals_df = bb_data.copy()
        signals_df['close'] = df['close']
        signals_df['touching_upper'] = df['close'] >= bb_data['bb_upper']
        signals_df['touching_lower'] = df['close'] <= bb_data['bb_lower']
        lower_bounce = (df['close'].shift(1) <= bb_data['bb_lower'].shift(1)) & (df['close'] > bb_data['bb_lower'])
        upper_bounce = (df['close'].shift(1) >= bb_data['bb_upper'].shift(1)) & (df['close'] < bb_data['bb_upper'])
        signals_df['signal'] = 0
        signals_df.loc[lower_bounce, 'signal'] = 1
        signals_df.loc[upper_bounce, 'signal'] = -1
        bb_width_ma = bb_data['bb_width'].rolling(window=20).mean()
        signals_df['squeeze'] = bb_data['bb_width'] < bb_width_ma * 0.8
        signals_df['expansion'] = bb_data['bb_width'] > bb_width_ma * 1.2
        return signals_df
    
    def get_volatility_signals(self, df):
        bb_data = self.calculate(df)
        vol_df = pd.DataFrame(index=df.index)
        vol_df['bb_width'] = bb_data['bb_width']
        vol_df['bb_width_ma'] = bb_data['bb_width'].rolling(window=10).mean()
        vol_df['bb_width_std'] = bb_data['bb_width'].rolling(window=10).std()
        vol_df['low_volatility'] = bb_data['bb_width'] < (vol_df['bb_width_ma'] - vol_df['bb_width_std'])
        vol_df['high_volatility'] = bb_data['bb_width'] > (vol_df['bb_width_ma'] + vol_df['bb_width_std'])
        vol_df['normal_volatility'] = ~(vol_df['low_volatility'] | vol_df['high_volatility'])
        vol_df['volatility_breakout'] = vol_df['low_volatility'].shift(1) & vol_df['high_volatility']
        return vol_df
    
    def get_mean_reversion_signals(self, df):
        bb_data = self.calculate(df)
        mr_df = pd.DataFrame(index=df.index)
        mr_df['bb_percent'] = bb_data['bb_percent']
        mr_df['close'] = df['close']
        mr_df['oversold_zone'] = bb_data['bb_percent'] < 0.2
        mr_df['overbought_zone'] = bb_data['bb_percent'] > 0.8
        mr_df['neutral_zone'] = (bb_data['bb_percent'] >= 0.4) & (bb_data['bb_percent'] <= 0.6)
        mr_df['mean_reversion_buy'] = mr_df['oversold_zone'].shift(1) & ~mr_df['oversold_zone'] & (bb_data['bb_percent'] > bb_data['bb_percent'].shift(1))
        mr_df['mean_reversion_sell'] = mr_df['overbought_zone'].shift(1) & ~mr_df['overbought_zone'] & (bb_data['bb_percent'] < bb_data['bb_percent'].shift(1))
        mr_df['distance_from_mean'] = abs(df['close'] - bb_data['bb_middle']) / bb_data['bb_middle']
        return mr_df
    
    def get_trend_continuation_signals(self, df):
        bb_data = self.calculate(df)
        tc_df = pd.DataFrame(index=df.index)
        tc_df['walking_upper_band'] = (df['close'] >= bb_data['bb_upper']) & (df['close'].shift(1) >= bb_data['bb_upper'].shift(1)) & (df['close'].shift(2) >= bb_data['bb_upper'].shift(2))
        tc_df['walking_lower_band'] = (df['close'] <= bb_data['bb_lower']) & (df['close'].shift(1) <= bb_data['bb_lower'].shift(1)) & (df['close'].shift(2) <= bb_data['bb_lower'].shift(2))
        tc_df['strong_uptrend'] = bb_data['bb_percent'] > 0.8
        tc_df['strong_downtrend'] = bb_data['bb_percent'] < 0.2
        bb_width_ma = bb_data['bb_width'].rolling(window=5).mean()
        tc_df['squeeze_to_expansion'] = (bb_data['bb_width'].shift(1) < bb_width_ma.shift(1)) & (bb_data['bb_width'] > bb_width_ma)
        return tc_df
