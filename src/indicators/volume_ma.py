import pandas as pd
import pandas_ta as ta
import numpy as np
import logging
from .base import BaseIndicator

class VolumeMaIndicator(BaseIndicator):
    def __init__(self, short_period=10, long_period=30):
        super().__init__("Volume MA")
        self.short_period = short_period
        self.long_period = long_period
        self.parameters = {
            'short_period': short_period,
            'long_period': long_period
        }
    
    def calculate(self, df):
        if not self.validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        
        if len(df) < self.long_period:
            logging.error("Volume MA calculation failed: Not enough data points. Have %d, need %d", len(df), self.long_period)
            raise ValueError(f"Not enough data points. Need at least {self.long_period} rows")
        
        vol_df = pd.DataFrame(index=df.index)
        vol_df['volume'] = df['volume']
        vol_df['vol_sma_short'] = df['volume'].rolling(window=self.short_period).mean()
        vol_df['vol_sma_long'] = df['volume'].rolling(window=self.long_period).mean()
        vol_df['vol_ema_short'] = ta.ema(df['volume'], length=self.short_period)
        vol_df['vol_ema_long'] = ta.ema(df['volume'], length=self.long_period)
        vol_df['vol_ratio_short'] = df['volume'] / vol_df['vol_sma_short']
        vol_df['vol_ratio_long'] = df['volume'] / vol_df['vol_sma_long']
        vol_df['vol_std'] = df['volume'].rolling(window=self.long_period).std()
        vol_df['vol_zscore'] = (df['volume'] - vol_df['vol_sma_long']) / vol_df['vol_std']
        return vol_df
    
    def get_volume_anomalies(self, df, spike_threshold=2.0):
        vol_data = self.calculate(df)
        anomaly_df = vol_data.copy()
        anomaly_df['volume_spike'] = vol_data['vol_ratio_long'] > spike_threshold
        anomaly_df['volume_dryup'] = vol_data['vol_ratio_long'] < 0.5
        anomaly_df['extreme_volume'] = abs(vol_data['vol_zscore']) > 2.0
        anomaly_df['volume_trend'] = vol_data['vol_sma_short'] > vol_data['vol_sma_long']
        price_change = df['close'].pct_change()
        volume_change = df['volume'].pct_change()
        anomaly_df['accumulation'] = (price_change > 0) & (volume_change > 0) & (vol_data['vol_ratio_short'] > 1.2)
        anomaly_df['distribution'] = (price_change < 0) & (volume_change > 0) & (vol_data['vol_ratio_short'] > 1.2)
        return anomaly_df
    
    def get_volume_price_analysis(self, df):
        vol_data = self.calculate(df)
        vpa_df = pd.DataFrame(index=df.index)
        price_change = df['close'].pct_change()
        volume_change = df['volume'].pct_change()
        vpa_df['price_up_volume_up'] = (price_change > 0) & (volume_change > 0)
        vpa_df['price_up_volume_down'] = (price_change > 0) & (volume_change < 0)
        vpa_df['price_down_volume_up'] = (price_change < 0) & (volume_change > 0)
        vpa_df['price_down_volume_down'] = (price_change < 0) & (volume_change < 0)
        vpa_df['strong_bullish'] = (price_change > 0.01) & (vol_data['vol_ratio_short'] > 1.5)
        vpa_df['strong_bearish'] = (price_change < -0.01) & (vol_data['vol_ratio_short'] > 1.5)
        vpa_df['weak_bullish'] = (price_change > 0) & (vol_data['vol_ratio_short'] < 0.8)
        vpa_df['weak_bearish'] = (price_change < 0) & (vol_data['vol_ratio_short'] < 0.8)
        vpa_df['vol_weighted_change'] = price_change * vol_data['vol_ratio_short']
        return vpa_df
    
    def get_volume_trend_analysis(self, df):
        vol_data = self.calculate(df)
        trend_df = pd.DataFrame(index=df.index)
        trend_df['vol_ma_trend'] = vol_data['vol_sma_short'] > vol_data['vol_sma_long']
        trend_df['vol_momentum'] = vol_data['vol_sma_short'].pct_change()
        price_change = df['close'].diff()
        obv = np.where(price_change > 0, df['volume'], np.where(price_change < 0, -df['volume'], 0)).cumsum()
        trend_df['obv'] = obv
        trend_df['obv_ma'] = pd.Series(obv).rolling(window=self.short_period).mean()
        trend_df['vroc'] = df['volume'].pct_change(periods=self.short_period) * 100
        trend_df['vol_5d'] = df['volume'].rolling(window=5).mean()
        trend_df['vol_20d'] = df['volume'].rolling(window=20).mean()
        trend_df['vol_ratio_5_20'] = trend_df['vol_5d'] / trend_df['vol_20d']
        vol_ma_20 = df['volume'].rolling(window=20).mean()
        vol_std_20 = df['volume'].rolling(window=20).std()
        trend_df['volume_breakout_up'] = df['volume'] > (vol_ma_20 + 2 * vol_std_20)
        trend_df['volume_breakout_down'] = df['volume'] < (vol_ma_20 - vol_std_20)
        return trend_df
    
    def get_climax_signals(self, df):
        vol_data = self.calculate(df)
        climax_df = pd.DataFrame(index=df.index)
        price_change = df['close'].pct_change()
        high_volume = vol_data['vol_ratio_long'] > 2.0
        price_ma = df['close'].rolling(window=5).mean()
        uptrend = df['close'] > price_ma.shift(5)
        climax_df['buying_climax'] = uptrend & high_volume & (price_change < 0.005)
        downtrend = df['close'] < price_ma.shift(5)
        climax_df['selling_climax'] = downtrend & high_volume & (price_change > -0.005)
        vol_ma_5 = vol_data['vol_sma_short'].rolling(window=5).mean()
        climax_df['volume_exhaustion'] = (vol_data['vol_sma_short'] > vol_ma_5 * 1.5) & (vol_data['vol_sma_short'].shift(1) > vol_data['vol_sma_short'])
        return climax_df
