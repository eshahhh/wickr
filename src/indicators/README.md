RSI (Relative Strength Index)
- Computation: RSI measures the magnitude of recent price changes to evaluate overbought or oversold conditions. It computes average gains and average losses over a lookback period. The typical steps are:
  - Compute price changes between consecutive closes.
  - Separate positive changes (gains) and negative changes (losses).
  - Compute an exponential or smoothed moving average of gains and losses over the configured period.
  - Compute the Relative Strength (RS) as the ratio of average gain to average loss.
  - Convert RS to the RSI value on a 0–100 scale using the standard formula RSI = 100 - (100 / (1 + RS)).

- Signal logic: The indicator flags values above the overbought threshold (commonly 70) and below the oversold threshold (commonly 30). Signals are discrete: -1 for overbought (bearish), +1 for oversold (bullish), and 0 otherwise. A divergence routine inspects short lookback windows to detect when price and RSI move in opposite directions, marking potential hidden or regular divergences.

MACD (Moving Average Convergence Divergence)
- Computation: MACD highlights momentum by subtracting a longer-period exponential moving average (EMA) of price from a shorter-period EMA.
  - Compute the fast EMA (shorter period) and slow EMA (longer period) of closing prices.
  - MACD line = fast EMA - slow EMA.
  - Signal line = EMA of the MACD line over a signal period.
  - Histogram = MACD line - Signal line.

- Signal logic: Crossovers between the MACD line and the signal line generate directional signals: a crossing above is bullish (+1), while a crossing below is bearish (-1). Momentum is assessed by the histogram sign and whether MACD is above/below the signal line. Divergence detection compares price direction to MACD direction over a lookback window. Zero-line crossings of the MACD itself are also noted as momentum regime shifts.

EMA (Exponential Moving Average)
- Computation: EMA is a weighted moving average that gives more weight to recent prices. For a list of configured periods, EMAs for each period are computed using the standard exponential smoothing formula.

- Signal logic: The implementation focuses on crossovers between a faster EMA and a slower EMA. A golden cross (fast EMA crossing above slow EMA) is considered bullish (+1), and a death cross (fast EMA crossing below slow EMA) is bearish (-1). Additional outputs indicate whether the price is above each EMA, whether the EMAs are aligned (for multi-period lists), and simple slope/price momentum measures computed as EMA differences or percentage distance between price and the fastest EMA.

Bollinger Bands
- Computation: Bollinger Bands create an envelope around a moving average using standard deviation to define the band widths.
  - Compute a moving average (middle band) of closing prices over a defined period.
  - Compute the standard deviation of closing prices over the same period.
  - Lower band = middle band - (std_dev * standard deviation).
  - Upper band = middle band + (std_dev * standard deviation).
  - Band width = (upper - lower) / middle, and a percentile position of price within the bands is computed as (close - lower) / (upper - lower).

- Signal logic: Signals include touches or bounces off the upper or lower bands: a price that was at or below the lower band and then moves above it can be interpreted as a mean-reversion buy; conversely, a price moving down from above the upper band can be a sell. The code also computes "squeeze" conditions (narrower band width than its recent average) and expansions (band width significantly larger than its recent average) to flag low- and high-volatility regimes. Additional trend-continuation signals look for consecutive closes beyond a band.

Volume Moving Averages and Volume Analysis
- Computation: Volume moving averages include simple and exponential moving averages over short and long windows. Ratios of current volume to these averages and rolling standard deviations are computed to normalize volume and derive z-scores.

- Signal logic: The module identifies anomalies such as volume spikes (volume ratio above a threshold), dry-ups (volume ratio below a threshold), and extreme volume via z-score. It also computes rules for accumulation/distribution by comparing price direction and volume changes together with elevated short-term volume. Trend-oriented outputs include a short vs long volume MA comparison, on-balance volume (OBV), and volume rate-of-change metrics. Climax signals aim to detect buying or selling exhaustion by combining high relative volume with modest price reversal characteristics.

Indicator Engine
- Composition: The engine wires the individual indicators together to produce an enriched DataFrame of indicator columns, trading signals, regime classification, and a small human-readable summary.

- Signal aggregation: The engine computes signals from the sub-indicators and aggregates them into bullish/bearish counts, strength measures, and a composite integer signal where 2 = strong buy, 1 = moderate buy, 0 = neutral, -1 = moderate sell, -2 = strong sell. Aggregation uses simple logical rules: counts of indicators signaling the same direction, with volume dry-ups suppressing strong signals.