"""
Spread Trading Strategy Backtester
Tests mark-last spread strategy on historical Gate.io data
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import os
import glob

logger = logging.getLogger(__name__)


class SpreadBacktester:
    def __init__(self, min_spread_pct: float = 7.0, exit_spread_pct: float = 0.2,
                 position_size_usd: float = 10.0, leverage: int = 20,
                 maker_fee: float = 0.0002, taker_fee: float = 0.0005):
        """
        Initialize backtester
        
        Args:
            min_spread_pct: Minimum spread % to enter (7%)
            exit_spread_pct: Spread % to exit (0.2%)
            position_size_usd: Position size in USD ($10)
            leverage: Leverage multiplier (20x)
            maker_fee: Maker fee (0.02%)
            taker_fee: Taker fee (0.05%)
        """
        self.min_spread = min_spread_pct
        self.exit_spread = exit_spread_pct
        self.position_size_usd = position_size_usd
        self.leverage = leverage
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        
        self.trades = []
        self.equity_curve = []
        
    def calculate_spread(self, mark_price: float, last_price: float) -> Tuple[float, str]:
        """
        Calculate spread and direction
        
        Returns:
            (spread_pct, direction) where direction is 'LONG' or 'SHORT'
        """
        if mark_price > last_price:
            spread = ((mark_price - last_price) / last_price) * 100
            return spread, 'LONG'
        elif mark_price < last_price:
            spread = ((last_price - mark_price) / mark_price) * 100
            return spread, 'SHORT'
        else:
            return 0.0, 'NONE'
    
    def calculate_pnl(self, entry_price: float, exit_price: float, 
                     side: str, size_contracts: int, quanto: float = 0.0001) -> Dict:
        """
        Calculate P&L for a trade
        
        Args:
            entry_price: Entry price
            exit_price: Exit price
            side: 'LONG' or 'SHORT'
            size_contracts: Number of contracts
            quanto: Quanto multiplier (default 0.0001 for most pairs)
        
        Returns:
            Dict with P&L details
        """
        # Price difference
        if side == 'LONG':
            price_diff = exit_price - entry_price
        else:  # SHORT
            price_diff = entry_price - exit_price
        
        # Gross P&L
        gross_pnl = size_contracts * price_diff * quanto
        
        # Fees (assuming market orders = taker fee)
        position_value = size_contracts * entry_price * quanto
        entry_fee = position_value * self.taker_fee
        exit_fee = abs(size_contracts * exit_price * quanto) * self.taker_fee
        total_fees = entry_fee + exit_fee
        
        # Net P&L
        net_pnl = gross_pnl - total_fees
        
        # ROI (based on margin used)
        margin_used = position_value / self.leverage
        roi_pct = (net_pnl / margin_used * 100) if margin_used > 0 else 0
        
        return {
            'gross_pnl': gross_pnl,
            'fees': total_fees,
            'net_pnl': net_pnl,
            'roi_pct': roi_pct,
            'margin_used': margin_used
        }
    
    def backtest_symbol(self, df: pd.DataFrame, symbol: str, 
                       mark_col: str = 'close', last_col: str = 'close',
                       quanto: float = 0.0001) -> Dict:
        """
        Backtest strategy on one symbol
        
        Note: Since we only have OHLC data, we approximate:
        - mark_price ≈ close price
        - last_price ≈ close price (same candle)
        
        For real backtest, need separate mark_price and last_price data!
        
        Args:
            df: DataFrame with OHLC data
            symbol: Symbol name
            mark_col: Column for mark_price approximation
            last_col: Column for last_price approximation
            quanto: Quanto multiplier
        """
        logger.info(f"Backtesting {symbol} with {len(df)} candles...")
        
        position = None  # Current position: {'side', 'entry_price', 'entry_time', 'entry_spread', 'size'}
        symbol_trades = []
        
        for idx, row in df.iterrows():
            timestamp = row['timestamp']
            mark_price = row[mark_col]
            # Simulate last_price variation (±0.5% from mark for testing)
            # In real data, this would be actual last_price
            last_price_variation = np.random.uniform(-0.005, 0.005)
            last_price = mark_price * (1 + last_price_variation)
            
            spread, direction = self.calculate_spread(mark_price, last_price)
            
            # Check entry
            if position is None and spread >= self.min_spread:
                # Calculate position size
                size_contracts = int((self.position_size_usd * self.leverage) / (last_price * quanto))
                
                if size_contracts > 0:
                    position = {
                        'side': direction,
                        'entry_price': last_price,
                        'entry_time': timestamp,
                        'entry_spread': spread,
                        'size': size_contracts
                    }
                    logger.debug(f"  ENTER {direction} @ {last_price:.6f}, spread={spread:.2f}%")
            
            # Check exit
            elif position is not None:
                # Calculate current spread in position direction
                if position['side'] == 'LONG':
                    if mark_price > last_price:
                        current_spread = ((mark_price - last_price) / last_price) * 100
                    else:
                        current_spread = 0  # Reversed
                else:  # SHORT
                    if mark_price < last_price:
                        current_spread = ((last_price - mark_price) / mark_price) * 100
                    else:
                        current_spread = 0  # Reversed
                
                # Exit condition
                if current_spread <= self.exit_spread:
                    # Calculate P&L
                    pnl_data = self.calculate_pnl(
                        entry_price=position['entry_price'],
                        exit_price=last_price,
                        side=position['side'],
                        size_contracts=position['size'],
                        quanto=quanto
                    )
                    
                    hold_time = (timestamp - position['entry_time']).total_seconds() / 60  # minutes
                    
                    trade = {
                        'symbol': symbol,
                        'side': position['side'],
                        'entry_time': position['entry_time'],
                        'exit_time': timestamp,
                        'hold_time_min': hold_time,
                        'entry_price': position['entry_price'],
                        'exit_price': last_price,
                        'entry_spread': position['entry_spread'],
                        'exit_spread': current_spread,
                        'size': position['size'],
                        **pnl_data
                    }
                    
                    symbol_trades.append(trade)
                    self.trades.append(trade)
                    
                    logger.debug(f"  EXIT {position['side']} @ {last_price:.6f}, "
                               f"P&L=${pnl_data['net_pnl']:.4f}, hold={hold_time:.1f}min")
                    
                    position = None
        
        # Summary for this symbol
        if symbol_trades:
            winning = [t for t in symbol_trades if t['net_pnl'] > 0]
            total_pnl = sum(t['net_pnl'] for t in symbol_trades)
            win_rate = len(winning) / len(symbol_trades) * 100
            
            logger.info(f"  {symbol}: {len(symbol_trades)} trades, "
                       f"WR={win_rate:.1f}%, P&L=${total_pnl:.4f}")
        
        return {
            'symbol': symbol,
            'trades': len(symbol_trades),
            'winning': len([t for t in symbol_trades if t['net_pnl'] > 0]),
            'total_pnl': sum(t['net_pnl'] for t in symbol_trades) if symbol_trades else 0
        }
    
    def backtest_all_files(self, data_dir: str = "historical_data") -> Dict:
        """Backtest all CSV files in directory"""
        csv_files = glob.glob(f"{data_dir}/*.csv")
        
        if not csv_files:
            logger.error(f"No CSV files found in {data_dir}")
            return {}
        
        logger.info(f"Found {len(csv_files)} CSV files to backtest")
        
        results = {}
        
        for csv_file in csv_files:
            symbol = os.path.basename(csv_file).split('_')[0]
            
            try:
                df = pd.read_csv(csv_file)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                result = self.backtest_symbol(df, symbol)
                results[symbol] = result
                
            except Exception as e:
                logger.error(f"Error backtesting {symbol}: {e}")
                results[symbol] = {'error': str(e)}
        
        return results
    
    def get_statistics(self) -> Dict:
        """Calculate trading statistics"""
        if not self.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'total_fees': 0,
                'avg_pnl': 0,
                'avg_win': 0,
                'avg_loss': 0,
                'best_trade': 0,
                'worst_trade': 0,
                'avg_hold_time': 0,
                'profit_factor': 0
            }
        
        winning = [t for t in self.trades if t['net_pnl'] > 0]
        losing = [t for t in self.trades if t['net_pnl'] <= 0]
        
        total_pnl = sum(t['net_pnl'] for t in self.trades)
        total_fees = sum(t['fees'] for t in self.trades)
        
        gross_profit = sum(t['net_pnl'] for t in winning) if winning else 0
        gross_loss = abs(sum(t['net_pnl'] for t in losing)) if losing else 0
        
        return {
            'total_trades': len(self.trades),
            'winning_trades': len(winning),
            'losing_trades': len(losing),
            'win_rate': len(winning) / len(self.trades) * 100,
            'total_pnl': total_pnl,
            'total_fees': total_fees,
            'avg_pnl': total_pnl / len(self.trades),
            'avg_win': gross_profit / len(winning) if winning else 0,
            'avg_loss': gross_loss / len(losing) if losing else 0,
            'best_trade': max(t['net_pnl'] for t in self.trades),
            'worst_trade': min(t['net_pnl'] for t in self.trades),
            'avg_hold_time': sum(t['hold_time_min'] for t in self.trades) / len(self.trades),
            'profit_factor': gross_profit / gross_loss if gross_loss > 0 else 0
        }
    
    def save_results(self, filename: str = "backtest_results.csv"):
        """Save all trades to CSV"""
        if not self.trades:
            logger.warning("No trades to save")
            return
        
        df = pd.DataFrame(self.trades)
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(self.trades)} trades to {filename}")
    
    def print_summary(self):
        """Print backtest summary"""
        stats = self.get_statistics()
        
        print("\n" + "="*60)
        print("BACKTEST RESULTS")
        print("="*60)
        print(f"Strategy: Mark-Last Spread Trading")
        print(f"Min Spread: {self.min_spread}%")
        print(f"Exit Spread: {self.exit_spread}%")
        print(f"Position Size: ${self.position_size_usd} @ {self.leverage}x")
        print("-"*60)
        print(f"Total Trades: {stats['total_trades']}")
        print(f"Win Rate: {stats['win_rate']:.2f}% ({stats['winning_trades']}W / {stats['losing_trades']}L)")
        print(f"")
        print(f"Total P&L: ${stats['total_pnl']:.4f}")
        print(f"Total Fees: ${stats['total_fees']:.4f}")
        print(f"Avg P&L: ${stats['avg_pnl']:.4f}")
        print(f"Avg Win: ${stats['avg_win']:.4f}")
        print(f"Avg Loss: ${stats['avg_loss']:.4f}")
        print(f"")
        print(f"Best Trade: ${stats['best_trade']:.4f}")
        print(f"Worst Trade: ${stats['worst_trade']:.4f}")
        print(f"Avg Hold Time: {stats['avg_hold_time']:.1f} min")
        print(f"Profit Factor: {stats['profit_factor']:.2f}")
        print("="*60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Initialize backtester with strategy parameters
    backtester = SpreadBacktester(
        min_spread_pct=7.0,
        exit_spread_pct=0.2,
        position_size_usd=10.0,
        leverage=20
    )
    
    # Run backtest on all downloaded data
    results = backtester.backtest_all_files("historical_data")
    
    # Print results
    backtester.print_summary()
    
    # Save trades
    backtester.save_results("backtest_trades.csv")
