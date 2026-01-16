"""
Complete Backtest Pipeline
1. Download historical data
2. Run backtest
3. Generate report
"""
import logging
from data_downloader import GateIODataDownloader
from backtest import SpreadBacktester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backtest.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_full_backtest(days: int = 180, max_symbols: int = 20,
                     min_spread: float = 7.0, exit_spread: float = 0.2,
                     position_size: float = 10.0, leverage: int = 20):
    """
    Run complete backtest pipeline
    
    Args:
        days: Number of days to download (180 = 6 months)
        max_symbols: Max symbols to test (None = all)  
        min_spread: Minimum entry spread %
        exit_spread: Exit spread %
        position_size: Position size in USD
        leverage: Leverage multiplier
    """
    print("="*70)
    print("GATE.IO SPREAD TRADING BACKTEST")
    print("="*70)
    print(f"Period: {days} days (~{days//30} months)")
    print(f"Max Symbols: {max_symbols if max_symbols else 'ALL'}")
    print(f"Strategy: Min Spread {min_spread}%, Exit {exit_spread}%")
    print(f"Position: ${position_size} @ {leverage}x")
    print("="*70)
    
    # Step 1: Download data
    print("\n[STEP 1/3] Downloading historical data...")
    print("-"*70)
    
    downloader = GateIODataDownloader()
    results = downloader.download_all_symbols(
        days=days,
        interval="15m",  # 15-minute candles (better availability)
        max_symbols=max_symbols
    )
    
    successful = sum(1 for r in results.values() if r.get('success'))
    print(f"\n✓ Downloaded data for {successful} symbols")
    
    if successful == 0:
        print("❌ No data downloaded! Cannot run backtest.")
        return
    
    # Step 2: Run backtest
    print("\n[STEP 2/3] Running backtest...")
    print("-"*70)
    
    backtester = SpreadBacktester(
        min_spread_pct=min_spread,
        exit_spread_pct=exit_spread,
        position_size_usd=position_size,
        leverage=leverage
    )
    
    backtest_results = backtester.backtest_all_files("historical_data")
    
    # Step 3: Generate report
    print("\n[STEP 3/3] Generating report...")
    print("-"*70)
    
    backtester.print_summary()
    
    # Save results
    backtester.save_results("backtest_trades.csv")
    
    # Symbol breakdown
    print("\n" + "="*70)
    print("PER-SYMBOL PERFORMANCE")
    print("="*70)
    print(f"{'Symbol':<15} {'Trades':<8} {'Win Rate':<10} {'Total P&L':<12}")
    print("-"*70)
    
    for symbol, result in sorted(backtest_results.items(), 
                                 key=lambda x: x[1].get('total_pnl', 0), 
                                 reverse=True):
        if result.get('trades', 0) > 0:
            win_rate = (result['winning'] / result['trades'] * 100) if result['trades'] > 0 else 0
            pnl = result.get('total_pnl', 0)
            pnl_str = f"${pnl:+.4f}"
            print(f"{symbol:<15} {result['trades']:<8} {win_rate:>6.1f}%    {pnl_str}")
    
    print("="*70)
    
    # Final verdict
    stats = backtester.get_statistics()
    
    print("\n" + "="*70)
    print("VERDICT")
    print("="*70)
    
    if stats['total_trades'] == 0:
        print("❌ NO TRADES GENERATED!")
        print(f"   Reason: Min spread {min_spread}% may be too high.")
        print(f"   Suggestion: Try lower min_spread (e.g., 3-5%)")
    elif stats['total_pnl'] > 0:
        if stats['win_rate'] >= 60:
            print(f"✅ STRATEGY PROFITABLE!")
            print(f"   Total P&L: ${stats['total_pnl']:.4f}")
            print(f"   Win Rate: {stats['win_rate']:.1f}%")
            print(f"   Recommendation: GOOD TO GO with these parameters!")
        else:
            print(f"⚠️  STRATEGY PROFITABLE but low win rate")
            print(f"   Total P&L: ${stats['total_pnl']:.4f}")
            print(f"   Win Rate: {stats['win_rate']:.1f}% (< 60%)")
            print(f"   Recommendation: Consider adjusting exit_spread")
    else:
        print(f"❌ STRATEGY UNPROFITABLE")
        print(f"   Total P&L: ${stats['total_pnl']:.4f}")
        print(f"   Win Rate: {stats['win_rate']:.1f}%")
        print(f"   Recommendation: DO NOT USE these parameters!")
        print(f"   Try: Lower min_spread or optimize exit_spread")
    
    print("="*70)
    
    return stats


if __name__ == "__main__":
    # Run backtest with user parameters
    # Quick test: 30 days, 5 symbols
    # 7% min spread, $10 @ 20x, 2% exit
    
    stats = run_full_backtest(
        days=30,           # 1 month for quick test
        max_symbols=5,     # Top 5 symbols
        min_spread=7.0,    # 7% min spread
        exit_spread=2.0,   # 2% exit
        position_size=10.0, # $10
        leverage=20        # 20x
    )
