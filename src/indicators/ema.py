import pandas as pd
import pandas_ta as ta
from .base import BaseIndicator

class EMAIndicator(BaseIndicator):
    def __init__(self, periods=[12, 26]):
        super().__init__("EMA")
        self.periods = periods
        self.parameters = {'periods': periods}
    
    def calculate(self, df):
        if not self.validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        
        max_period = max(self.periods)
        if len(df) < max_period:
            raise ValueError(f"Not enough data points. Need at least {max_period} rows")
        
        ema_df = pd.DataFrame(index=df.index)
        for period in self.periods:
            ema_values = ta.ema(df['close'], length=period)
            ema_df[f'ema_{period}'] = ema_values
        
        return ema_df
    
    def get_signals(self, df):
        if len(self.periods) < 2:
            raise ValueError("Need at least 2 periods for crossover signals")
        
        ema_data = self.calculate(df)
        signals_df = ema_data.copy()
        
        fast_period = min(self.periods)
        slow_period = sorted(self.periods)[1]
        
        fast_ema = ema_data[f'ema_{fast_period}']
        slow_ema = ema_data[f'ema_{slow_period}']
        
        golden_cross = (
            (fast_ema > slow_ema) &
            (fast_ema.shift(1) <= slow_ema.shift(1))
        )
        
        death_cross = (
            (fast_ema < slow_ema) &
            (fast_ema.shift(1) >= slow_ema.shift(1))
        )
        
        signals_df['signal'] = 0
        signals_df.loc[golden_cross, 'signal'] = 1
        signals_df.loc[death_cross, 'signal'] = -1
        
        signals_df['bullish_trend'] = fast_ema > slow_ema
        signals_df['bearish_trend'] = fast_ema < slow_ema
        signals_df['price_above_fast'] = df['close'] > fast_ema
        signals_df['price_above_slow'] = df['close'] > slow_ema
        
        return signals_df
    
    def get_trend_strength(self, df):
        ema_data = self.calculate(df)
        trend_df = pd.DataFrame(index=df.index)
        
        for period in self.periods:
            ema_col = f'ema_{period}'
            trend_df[f'{ema_col}_slope'] = ema_data[ema_col].diff()
        
        if len(self.periods) >= 3:
            sorted_periods = sorted(self.periods)
            ema_cols = [f'ema_{p}' for p in sorted_periods]
            
            bullish_alignment = True
            for i in range(len(ema_cols) - 1):
                bullish_alignment &= (ema_data[ema_cols[i]] > ema_data[ema_cols[i+1]])
            
            bearish_alignment = True
            for i in range(len(ema_cols) - 1):
                bearish_alignment &= (ema_data[ema_cols[i]] < ema_data[ema_cols[i+1]])
            
            trend_df['bullish_alignment'] = bullish_alignment
            trend_df['bearish_alignment'] = bearish_alignment
        
        fastest_period = min(self.periods)
        fastest_ema = ema_data[f'ema_{fastest_period}']
        trend_df['price_momentum'] = (df['close'] - fastest_ema) / fastest_ema * 100
        
        return trend_df
    
    def get_support_resistance(self, df, period=None):
        if period is None:
            period = max(self.periods)
        
        ema_data = self.calculate(df)
        ema_values = ema_data[f'ema_{period}']
        
        sr_df = pd.DataFrame(index=df.index)
        sr_df['ema'] = ema_values
        sr_df['close'] = df['close']
        
        price_touch_threshold = 0.005
        
        sr_df['touching_ema'] = (
            abs(df['close'] - ema_values) / ema_values < price_touch_threshold
        )
        
        sr_df['support'] = (
            (df['close'] > ema_values) &
            (df['low'] <= ema_values * (1 + price_touch_threshold)) &
            sr_df['touching_ema']
        )
        
        sr_df['resistance'] = (
            (df['close'] < ema_values) &
            (df['high'] >= ema_values * (1 - price_touch_threshold)) &
            sr_df['touching_ema']
        )
        
        return sr_df
