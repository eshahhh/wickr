import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from fetch import BinanceDataFetcher
from indicators.engine import IndicatorEngine
import logging

class SignalGenerator:
    def __init__(self, config_path=None, config=None, indicator_engine=None):
        if config_path:
            self.config_path = Path(config_path)
        else:
            self.config_path = Path(__file__).resolve().parents[1] / "config.json"
        
        if config:
            self.config = config
        else:
            self.config = load_config_file(self.config_path)
        
        indicator_cfg = self.config.get("indicator_parameters")
        if indicator_engine:
            self.indicator_engine = indicator_engine
        else:
            self.indicator_engine = IndicatorEngine(config=indicator_cfg)
        
        self.thresholds = self.config.get("thresholds", {})
        self.signal_settings = self.config.get("signal_settings", {})
        self.strategies = self.config.get("strategies", [])
        self.condition_reasons = build_condition_reasons(self.thresholds)

    def generate_signals(self, df, symbol="BTCUSDT"):
        logging.info("SignalGenerator: Starting signal generation for dataframe with %d rows", len(df))
        if df is None or df.empty:
            return []
        indicator_df = self.indicator_engine.calculate_all_indicators(df).copy()
        ensure_timestamp_column(indicator_df, df)
        condition_map = compute_conditions(indicator_df, self.thresholds)
        return evaluate_strategies(indicator_df, condition_map, symbol, self.strategies, self.signal_settings, self.condition_reasons)

    def save_signals(self, signals, filepath):
        destination = Path(filepath).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = list(signals)
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        return destination

    def print_signals(self, signals):
        for signal in signals:
            timestamp = signal.get("timestamp")
            name = signal.get("signal")
            price = signal.get("price")
            reasons = ", ".join(signal.get("reason", []))
            print(f"[{timestamp}] {name} at {price}: {reasons}")

def ensure_timestamp_column(indicator_df, original_df):
    if "timestamp" in indicator_df.columns:
        indicator_df["timestamp"] = pd.to_datetime(indicator_df["timestamp"], utc=False)
        return
    if "timestamp" in original_df.columns:
        indicator_df["timestamp"] = pd.to_datetime(original_df["timestamp"], utc=False)
        return
    index = original_df.index
    if isinstance(index, pd.DatetimeIndex):
        indicator_df["timestamp"] = index
    else:
        indicator_df["timestamp"] = pd.to_datetime(index)

def compute_conditions(indicator_df, thresholds):
    false_series = pd.Series(False, index=indicator_df.index)
    conditions = {}
    rsi_thresholds = thresholds.get("rsi", {})
    if "rsi" in indicator_df.columns:
        rsi = indicator_df["rsi"]
        oversold_level = rsi_thresholds.get("oversold", 30)
        overbought_level = rsi_thresholds.get("overbought", 70)
        conditions["rsi_oversold"] = rsi.lt(oversold_level).fillna(False)
        conditions["rsi_overbought"] = rsi.gt(overbought_level).fillna(False)
    macd_columns = {"macd", "signal"}
    if macd_columns.issubset(indicator_df.columns):
        macd = indicator_df["macd"]
        macd_signal = indicator_df["signal"]
        macd_histogram = indicator_df.get("histogram")
        macd_cross_up = macd.gt(macd_signal) & macd.shift(1).le(macd_signal.shift(1))
        macd_cross_down = macd.lt(macd_signal) & macd.shift(1).ge(macd_signal.shift(1))
        conditions["macd_bullish_cross"] = macd_cross_up.fillna(False)
        conditions["macd_bearish_cross"] = macd_cross_down.fillna(False)
        if macd_histogram is not None:
            min_histogram = thresholds.get("macd", {}).get("min_histogram", 0.0)
            conditions["macd_hist_positive"] = macd_histogram.gt(min_histogram).fillna(False)
            conditions["macd_hist_negative"] = macd_histogram.lt(-min_histogram).fillna(False)
    ema_columns = [col for col in indicator_df.columns if col.startswith("ema_")]
    if len(ema_columns) >= 2:
        try:
            sorted_emas = sorted(ema_columns, key=lambda col: int(col.split("_")[1]))
        except (IndexError, ValueError):
            sorted_emas = sorted(ema_columns)
        fast_col = sorted_emas[0]
        slow_col = sorted_emas[1]
        fast_ema = indicator_df[fast_col]
        slow_ema = indicator_df[slow_col]
        conditions["ema_bullish"] = fast_ema.gt(slow_ema).fillna(False)
        conditions["ema_bearish"] = fast_ema.lt(slow_ema).fillna(False)
    if {"close", "bb_lower"}.issubset(indicator_df.columns):
        close = indicator_df["close"]
        bb_lower = indicator_df["bb_lower"]
        tolerance = thresholds.get("bollinger", {}).get("touch_tolerance", 0.01)
        lower_touch = (close <= bb_lower) | (close.sub(bb_lower).abs() <= close.abs() * tolerance)
        conditions["price_touch_lower_band"] = lower_touch.fillna(False)
    if {"close", "bb_upper"}.issubset(indicator_df.columns):
        close = indicator_df["close"]
        bb_upper = indicator_df["bb_upper"]
        tolerance = thresholds.get("bollinger", {}).get("touch_tolerance", 0.01)
        upper_touch = (close >= bb_upper) | (close.sub(bb_upper).abs() <= close.abs() * tolerance)
        conditions["price_touch_upper_band"] = upper_touch.fillna(False)
    if "vol_ratio_long" in indicator_df.columns:
        volume_thresholds = thresholds.get("volume", {})
        ratio_min = volume_thresholds.get("ratio_long_min", 1.5)
        ratio_max = volume_thresholds.get("dryup_ratio_max", 0.5)
        vol_ratio = indicator_df["vol_ratio_long"]
        conditions["volume_spike"] = vol_ratio.gt(ratio_min).fillna(False)
        conditions["volume_dryup"] = vol_ratio.lt(ratio_max).fillna(False)
    else:
        conditions["volume_spike"] = false_series
        conditions["volume_dryup"] = false_series
    if "bb_width" in indicator_df.columns:
        low_vol_threshold = thresholds.get("bollinger", {}).get("low_volatility_width")
        if low_vol_threshold is not None:
            conditions["low_volatility"] = indicator_df["bb_width"].lt(low_vol_threshold).fillna(False)
    return conditions

def evaluate_strategies(indicator_df, conditions, symbol, strategies, signal_settings, condition_reasons):
    if not strategies:
        return []
    min_confluence = signal_settings.get("min_confluence_count", 1)
    ignore_low_volatility = signal_settings.get("ignore_low_volatility", False)
    min_interval = signal_settings.get("min_signal_interval_minutes", 0)
    min_interval_delta = timedelta(minutes=min_interval)
    low_volatility_series = conditions.get("low_volatility")
    last_signal_times = {}
    generated_signals = []
    for idx in indicator_df.index:
        row_conditions = {name: bool(series.loc[idx]) for name, series in conditions.items()}
        confluence_count = sum(1 for is_true in row_conditions.values() if is_true)
        if ignore_low_volatility and low_volatility_series is not None:
            if bool(low_volatility_series.loc[idx]):
                continue
        timestamp = normalize_timestamp(indicator_df.at[idx, "timestamp"])
        price = safe_float(indicator_df.at[idx, "close"]) if "close" in indicator_df.columns else None
        for strategy in strategies:
            if not strategy.get("enabled", True):
                continue
            required_conditions = strategy.get("conditions", [])
            if not required_conditions:
                continue
            if not all(row_conditions.get(condition, False) for condition in required_conditions):
                continue
            strategy_min = strategy.get("min_confluence", len(required_conditions))
            effective_min = max(min_confluence, strategy_min)
            if confluence_count < effective_min:
                continue
            signal_name = strategy.get("signal", "NEUTRAL")
            last_time = last_signal_times.get(signal_name)
            if min_interval > 0 and last_time is not None and timestamp is not None:
                if (timestamp - last_time) < min_interval_delta:
                    continue
            reasons = [condition_reasons.get(condition, condition) for condition in required_conditions]
            signal_entry = {
                "timestamp": timestamp.isoformat() if timestamp else None,
                "symbol": symbol,
                "price": price,
                "signal": signal_name,
                "strategy": strategy.get("name"),
                "direction": strategy.get("direction"),
                "reason": reasons,
                "confluence": confluence_count,
                "conditions": {condition: row_conditions.get(condition, False) for condition in required_conditions},
                "indicators": extract_indicator_context(indicator_df.loc[idx]),
            }
            generated_signals.append(signal_entry)
            if timestamp is not None:
                last_signal_times[signal_name] = timestamp
    return generated_signals

def extract_indicator_context(row):
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    numeric_keys = ["rsi", "macd", "signal", "histogram", "ema_12", "ema_26", 
                    "bb_lower", "bb_upper", "bb_width", "bb_percent", 
                    "vol_ratio_long", "volume"]
    context = {}
    for key in numeric_keys:
        if key in row.index:
            val = row[key]
            if isinstance(val, pd.Series):
                val = val.iloc[0]
            if pd.notna(val):
                context[key] = safe_float(val)
    return context

def build_condition_reasons(thresholds):
    rsi_thresholds = thresholds.get("rsi", {})
    volume_thresholds = thresholds.get("volume", {})
    bollinger_thresholds = thresholds.get("bollinger", {})
    return {
        "rsi_oversold": f"RSI<{rsi_thresholds.get('oversold', 30)}",
        "rsi_overbought": f"RSI>{rsi_thresholds.get('overbought', 70)}",
        "macd_bullish_cross": "MACD crossover up",
        "macd_bearish_cross": "MACD crossover down",
        "macd_hist_positive": "MACD histogram positive",
        "macd_hist_negative": "MACD histogram negative",
        "ema_bullish": "EMA fast above slow",
        "ema_bearish": "EMA fast below slow",
        "price_touch_lower_band": "Price touching lower Bollinger Band",
        "price_touch_upper_band": "Price touching upper Bollinger Band",
        "volume_spike": f"Volume > {volume_thresholds.get('ratio_long_min', 1.5)}x long MA",
        "volume_dryup": f"Volume < {volume_thresholds.get('dryup_ratio_max', 0.5)}x long MA",
        "low_volatility": f"BB width < {bollinger_thresholds.get('low_volatility_width', 0.0)}",
    }

def normalize_timestamp(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return pd.to_datetime(value).to_pydatetime()
    except (TypeError, ValueError):
        return None

def safe_float(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def load_config_file(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)

def load_dataframe(path):
    filepath = Path(path)
    parse_dates = ["timestamp"] if "timestamp" in peek_columns(filepath) else None
    if filepath.suffix == ".csv":
        return pd.read_csv(filepath, parse_dates=parse_dates)
    else:
        return pd.read_json(filepath)

def peek_columns(path):
    if not path.exists() or path.suffix not in {".csv", ".json"}:
        return []
    if path.suffix == ".csv":
        sample = pd.read_csv(path, nrows=1)
    else:
        sample = pd.read_json(path, lines=False)
    return list(sample.columns)

def run_cli(symbol="BTCUSDT", interval="1s", limit=5000, data_file=None, 
            output="data/signals1K1s.json", config=None, save=True):
    if config:
        generator = SignalGenerator(config_path=config)
    else:
        generator = SignalGenerator()
    if data_file:
        df = load_dataframe(data_file)
    else:
        fetcher = BinanceDataFetcher(data_dir="data")
        df = fetcher.fetch_klines(symbol=symbol, interval=interval, limit=limit)
        if df is None:
            print("Failed to fetch data from Binance.")
            return 1
    signals = generator.generate_signals(df, symbol=symbol)
    if signals:
        generator.print_signals(signals)
    else:
        print("No signals generated with the current configuration.")
    if save:
        destination = generator.save_signals(signals, output)
        print(f"Saved {len(signals)} signals to {destination}")
    return 0

if __name__ == "__main__":
    run_cli()
