# Gate.io Spread Trading Backtest

## Quick Start

### 1. Install dependencies

```bash
pip install pandas numpy requests
```

### 2. Run backtest

```bash
python run_backtest.py
```

This will:

- Download 6 months of historical data for top 20 symbols
- Run backtest with 7% min spread, $10 @ 20x
- Generate detailed report
- Save results to `backtest_trades.csv`

## Parameters

Edit `run_backtest.py` to change:

```python
run_full_backtest(
    days=180,          # 6 months of data
    max_symbols=20,    # Top 20 symbols
    min_spread=7.0,    # 7% minimum entry spread
    exit_spread=0.2,   # 0.2% exit spread
    position_size=10.0, # $10 per position
    leverage=20        # 20x leverage
)
```

## Output Files

- `historical_data/` - Downloaded candle data
- `backtest_trades.csv` - All simulated trades
- `backtest.log` - Detailed log file

## Interpreting Results

### Good Strategy

- ✅ Win Rate > 60%
- ✅ Total P&L > 0
- ✅ Profit Factor > 1.5

### Bad Strategy

- ❌ Win Rate < 50%
- ❌ Total P&L < 0
- ❌ High fees vs profit

## ⚠️ Important Notes

This backtest uses **SIMULATED** mark-last spread:

- Real data needed: separate mark_price and last_price ticks
- Currently: spreads are randomized (±0.5%) for testing
- **Results are APPROXIMATE only!**

For accurate backtest:

1. Download real tick data with both mark and last prices
2. Re-run with actual spread data

## Next Steps

If backtest shows profit:

1. ✅ Review trade distribution
2. ✅ Check if spread 7% appears frequently enough
3. ✅ Adjust parameters if needed
4. ✅ Run live test with small positions

If backtest shows loss:

1. ❌ Try different parameters (lower min_spread)
2. ❌ Test different time periods
3. ❌ Consider different strategy
