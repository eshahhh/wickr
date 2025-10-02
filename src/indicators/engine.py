import pandas as pd
import numpy as np
from .rsi import RSIIndicator
from .macd import MACDIndicator
from .ema import EMAIndicator
from .bollinger_bands import BollingerBandsIndicator
from .volume_ma import VolumeMaIndicator

class IndicatorEngine:
    def __init__(self, config=None):
        default_config = {
            'rsi': {'period': 14},
            'macd': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
            'ema': {'periods': [12, 26]},
            'bollinger_bands': {'period': 20, 'std_dev': 2.0},
            'volume_ma': {'short_period': 10, 'long_period': 30}
        }
        self.config = config if config else default_config
        self.indicators = {
            'rsi': RSIIndicator(**self.config['rsi']),
            'macd': MACDIndicator(**self.config['macd']),
            'ema': EMAIndicator(**self.config['ema']),
            'bollinger_bands': BollingerBandsIndicator(**self.config['bollinger_bands']),
            'volume_ma': VolumeMaIndicator(**self.config['volume_ma'])
        }

    def calculate_all_indicators(self, df):
        if not self._validate_data(df):
            raise ValueError("DataFrame must contain required OHLCV columns")
        result_df = df.copy()
        try:
            rsi_data = self.indicators['rsi'].calculate(df)
            result_df['rsi'] = rsi_data
            macd_data = self.indicators['macd'].calculate(df)
            result_df = pd.concat([result_df, macd_data], axis=1)
            ema_data = self.indicators['ema'].calculate(df)
            result_df = pd.concat([result_df, ema_data], axis=1)
            bb_data = self.indicators['bollinger_bands'].calculate(df)
            result_df = pd.concat([result_df, bb_data], axis=1)
            vol_data = self.indicators['volume_ma'].calculate(df)
            result_df = pd.concat([result_df, vol_data], axis=1)
            return result_df
        except Exception as e:
            raise

    def get_trading_signals(self, df):
        signals_df = pd.DataFrame(index=df.index)
        rsi_signals = self.indicators['rsi'].get_signals(df)
        macd_signals = self.indicators['macd'].get_signals(df)
        ema_signals = self.indicators['ema'].get_signals(df)
        bb_signals = self.indicators['bollinger_bands'].get_signals(df)
        vol_anomalies = self.indicators['volume_ma'].get_volume_anomalies(df)
        signals_df['rsi_signal'] = rsi_signals['signal']
        signals_df['macd_signal'] = macd_signals['signal_type']
        signals_df['ema_signal'] = ema_signals['signal']
        signals_df['bb_signal'] = bb_signals['signal']
        signals_df['volume_spike'] = vol_anomalies['volume_spike']
        signals_df['volume_dryup'] = vol_anomalies['volume_dryup']
        signals_df['bullish_signals'] = (
            (signals_df['rsi_signal'] == 1) |
            (signals_df['macd_signal'] == 1) |
            (signals_df['ema_signal'] == 1) |
            (signals_df['bb_signal'] == 1)
        ).astype(int)
        signals_df['bearish_signals'] = (
            (signals_df['rsi_signal'] == -1) |
            (signals_df['macd_signal'] == -1) |
            (signals_df['ema_signal'] == -1) |
            (signals_df['bb_signal'] == -1)
        ).astype(int)
        signals_df['bullish_strength'] = (
            (signals_df['rsi_signal'] == 1).astype(int) +
            (signals_df['macd_signal'] == 1).astype(int) +
            (signals_df['ema_signal'] == 1).astype(int) +
            (signals_df['bb_signal'] == 1).astype(int)
        )
        signals_df['bearish_strength'] = (
            (signals_df['rsi_signal'] == -1).astype(int) +
            (signals_df['macd_signal'] == -1).astype(int) +
            (signals_df['ema_signal'] == -1).astype(int) +
            (signals_df['bb_signal'] == -1).astype(int)
        )
        signals_df['composite_signal'] = 0
        strong_buy = (
            (signals_df['bullish_strength'] >= 3) &
            ~signals_df['volume_dryup']
        )
        strong_sell = (
            (signals_df['bearish_strength'] >= 3) &
            ~signals_df['volume_dryup']
        )
        moderate_buy = (
            (signals_df['bullish_strength'] >= 2) &
            (signals_df['bearish_strength'] == 0)
        )
        moderate_sell = (
            (signals_df['bearish_strength'] >= 2) &
            (signals_df['bullish_strength'] == 0)
        )
        signals_df.loc[strong_buy, 'composite_signal'] = 2
        signals_df.loc[moderate_buy, 'composite_signal'] = 1
        signals_df.loc[moderate_sell, 'composite_signal'] = -1
        signals_df.loc[strong_sell, 'composite_signal'] = -2
        return signals_df

    def get_market_regime(self, df):
        regime_df = pd.DataFrame(index=df.index)
        ema_data = self.indicators['ema'].calculate(df)
        bb_data = self.indicators['bollinger_bands'].calculate(df)
        vol_data = self.indicators['volume_ma'].calculate(df)
        if 'ema_12' in ema_data.columns and 'ema_26' in ema_data.columns:
            regime_df['trend_strength'] = abs(
                (ema_data['ema_12'] - ema_data['ema_26']) / ema_data['ema_26']
            )
        regime_df['volatility'] = bb_data['bb_width']
        regime_df['volatility_ma'] = regime_df['volatility'].rolling(window=20).mean()
        regime_df['volume_regime'] = vol_data['vol_ratio_long']
        regime_df['trending'] = regime_df['trend_strength'] > 0.02
        regime_df['high_volatility'] = (
            regime_df['volatility'] > regime_df['volatility_ma'] * 1.5
        )
        regime_df['low_volume'] = regime_df['volume_regime'] < 0.7
        regime_df['regime'] = 'unknown'
        strong_trend = regime_df['trending'] & ~regime_df['low_volume']
        regime_df.loc[strong_trend, 'regime'] = 'trending'
        volatile_market = (
            regime_df['high_volatility'] & 
            ~regime_df['low_volume'] & 
            ~regime_df['trending']
        )
        regime_df.loc[volatile_market, 'regime'] = 'volatile'
        ranging_market = (
            ~regime_df['high_volatility'] & 
            ~regime_df['trending']
        )
        regime_df.loc[ranging_market, 'regime'] = 'ranging'
        low_volume_market = regime_df['low_volume']
        regime_df.loc[low_volume_market, 'regime'] = 'low_volume'
        return regime_df

    def get_indicator_summary(self, df):
        if len(df) == 0:
            return {}
        latest_idx = df.index[-1]
        summary = {}
        rsi_value = self.indicators['rsi'].calculate(df).iloc[-1]
        summary['rsi'] = {
            'value': round(rsi_value, 2),
            'signal': 'overbought' if rsi_value > 70 else 'oversold' if rsi_value < 30 else 'neutral'
        }
        macd_data = self.indicators['macd'].calculate(df)
        summary['macd'] = {
            'macd': round(macd_data['macd'].iloc[-1], 4),
            'signal': round(macd_data['signal'].iloc[-1], 4),
            'histogram': round(macd_data['histogram'].iloc[-1], 4),
            'trend': 'bullish' if macd_data['macd'].iloc[-1] > macd_data['signal'].iloc[-1] else 'bearish'
        }
        bb_data = self.indicators['bollinger_bands'].calculate(df)
        bb_position = bb_data['bb_percent'].iloc[-1]
        summary['bollinger_bands'] = {
            'position': round(bb_position, 3),
            'signal': 'overbought' if bb_position > 0.8 else 'oversold' if bb_position < 0.2 else 'neutral',
            'squeeze': bb_data['bb_width'].iloc[-1] < bb_data['bb_width'].rolling(window=20).mean().iloc[-1] * 0.8
        }
        vol_data = self.indicators['volume_ma'].calculate(df)
        summary['volume'] = {
            'ratio': round(vol_data['vol_ratio_long'].iloc[-1], 2),
            'signal': 'high' if vol_data['vol_ratio_long'].iloc[-1] > 1.5 else 'low' if vol_data['vol_ratio_long'].iloc[-1] < 0.7 else 'normal'
        }
        return summary

    def _validate_data(self, df):
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        return all(col in df.columns for col in required_columns)

    def save_enriched_data(self, df, filepath):
        try:
            enriched_df = self.calculate_all_indicators(df)
            enriched_df.to_csv(filepath, index=False)
            return True
        except Exception as e:
            return False
