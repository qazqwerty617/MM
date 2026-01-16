"""
Trade Analytics Module
Tracks all trades, calculates P&L, and logs to CSV
"""
import csv
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class TradeAnalytics:
    def __init__(self, csv_file: str = "trades.csv", stats_file: str = "daily_stats.csv"):
        self.csv_file = csv_file
        self.stats_file = stats_file
        self.trades = []  # All completed trades
        self.daily_pnl = 0.0
        
        # Initialize CSV files if they don't exist
        self._init_csv_files()
        
    def _init_csv_files(self):
        """Initialize CSV files with headers if they don't exist"""
        # Trades CSV
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'symbol', 'side', 'entry_price', 'exit_price',
                    'size', 'leverage', 'entry_spread', 'exit_spread',
                    'pnl_usd', 'pnl_percent', 'hold_time_seconds', 'hold_time_minutes'
                ])
        
        # Daily stats CSV
        if not os.path.exists(self.stats_file):
            with open(self.stats_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'date', 'total_trades', 'winning_trades', 'losing_trades',
                    'win_rate', 'total_pnl', 'avg_pnl', 'best_trade', 'worst_trade',
                    'avg_hold_time_minutes'
                ])
    
    def log_trade(self, trade_data: Dict) -> Dict:
        """
        Log a completed trade and calculate P&L
        
        trade_data should contain:
        - symbol: str
        - side: str (long/short)
        - entry_price: float
        - exit_price: float
        - size: int (contracts)
        - leverage: int
        - entry_spread: float (%)
        - exit_spread: float (%)
        - entry_time: float (timestamp)
        - exit_time: float (timestamp)
        - quanto_multiplier: float
        """
        try:
            # Calculate P&L
            pnl_data = self._calculate_pnl(trade_data)
            
            # Add to trades list
            self.trades.append(pnl_data)
            self.daily_pnl += pnl_data['pnl_usd']
            
            # Write to CSV
            self._write_trade_to_csv(pnl_data)
            
            logger.info(f"Logged trade: {pnl_data['symbol']} P&L: ${pnl_data['pnl_usd']:.4f}")
            
            return pnl_data
            
        except Exception as e:
            logger.error(f"Error logging trade: {e}")
            return None
    
    def _calculate_pnl(self, trade_data: Dict) -> Dict:
        """Calculate P&L for a trade"""
        symbol = trade_data['symbol']
        side = trade_data['side']
        entry_price = float(trade_data['entry_price'])
        exit_price = float(trade_data['exit_price'])
        size = abs(int(trade_data['size']))
        leverage = int(trade_data['leverage'])
        quanto = float(trade_data.get('quanto_multiplier', 0.0001))
        
        # Use REAL P&L from Gate.io if available
        if 'real_pnl_usd' in trade_data and trade_data['real_pnl_usd'] is not None:
            pnl_usd = float(trade_data['real_pnl_usd'])
            logger.info(f"Using REAL P&L from Gate.io: ${pnl_usd:.4f}")
        else:
            # Fallback: Calculate manually
            if side == 'long':
                price_diff = exit_price - entry_price
            else:  # short
                price_diff = entry_price - exit_price
            
            pnl_usd = size * price_diff * quanto
            logger.warning(f"Using MANUAL P&L calculation: ${pnl_usd:.4f}")
        
        # P&L percentage (based on margin used)
        position_value = size * entry_price * quanto
        margin_used = position_value / leverage
        pnl_percent = (pnl_usd / margin_used * 100) if margin_used > 0 else 0
        
        # Hold time
        entry_time = trade_data.get('entry_time', 0)
        exit_time = trade_data.get('exit_time', 0)
        hold_time_seconds = exit_time - entry_time
        hold_time_minutes = hold_time_seconds / 60
        
        return {
            'timestamp': datetime.fromtimestamp(exit_time).strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'size': size,
            'leverage': leverage,
            'entry_spread': trade_data.get('entry_spread', 0),
            'exit_spread': trade_data.get('exit_spread', 0),
            'pnl_usd': pnl_usd,
            'pnl_percent': pnl_percent,
            'hold_time_seconds': hold_time_seconds,
            'hold_time_minutes': hold_time_minutes,
            'margin_used': margin_used
        }
    
    def _write_trade_to_csv(self, pnl_data: Dict):
        """Write trade to CSV file"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    pnl_data['timestamp'],
                    pnl_data['symbol'],
                    pnl_data['side'],
                    f"{pnl_data['entry_price']:.8f}",
                    f"{pnl_data['exit_price']:.8f}",
                    pnl_data['size'],
                    pnl_data['leverage'],
                    f"{pnl_data['entry_spread']:.2f}",
                    f"{pnl_data['exit_spread']:.2f}",
                    f"{pnl_data['pnl_usd']:.4f}",
                    f"{pnl_data['pnl_percent']:.2f}",
                    f"{pnl_data['hold_time_seconds']:.1f}",
                    f"{pnl_data['hold_time_minutes']:.2f}"
                ])
        except Exception as e:
            logger.error(f"Error writing to CSV: {e}")
    
    def get_statistics(self) -> Dict:
        """Calculate trading statistics"""
        if not self.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'best_trade': None,
                'worst_trade': None,
                'avg_hold_time': 0
            }
        
        winning = [t for t in self.trades if t['pnl_usd'] > 0]
        losing = [t for t in self.trades if t['pnl_usd'] <= 0]
        
        total_pnl = sum(t['pnl_usd'] for t in self.trades)
        avg_pnl = total_pnl / len(self.trades)
        win_rate = (len(winning) / len(self.trades) * 100) if self.trades else 0
        
        best_trade = max(self.trades, key=lambda x: x['pnl_usd']) if self.trades else None
        worst_trade = min(self.trades, key=lambda x: x['pnl_usd']) if self.trades else None
        
        avg_hold = sum(t['hold_time_minutes'] for t in self.trades) / len(self.trades)
        
        return {
            'total_trades': len(self.trades),
            'winning_trades': len(winning),
            'losing_trades': len(losing),
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'best_trade': best_trade,
            'worst_trade': worst_trade,
            'avg_hold_time': avg_hold
        }
    
    def get_symbol_performance(self) -> Dict[str, Dict]:
        """Get performance grouped by symbol"""
        symbol_stats = {}
        
        for trade in self.trades:
            symbol = trade['symbol']
            if symbol not in symbol_stats:
                symbol_stats[symbol] = {
                    'trades': 0,
                    'wins': 0,
                    'total_pnl': 0
                }
            
            symbol_stats[symbol]['trades'] += 1
            if trade['pnl_usd'] > 0:
                symbol_stats[symbol]['wins'] += 1
            symbol_stats[symbol]['total_pnl'] += trade['pnl_usd']
        
        # Calculate win rate for each symbol
        for symbol, stats in symbol_stats.items():
            stats['win_rate'] = (stats['wins'] / stats['trades'] * 100) if stats['trades'] > 0 else 0
        
        return symbol_stats
    
    def save_daily_stats(self):
        """Save daily statistics to CSV"""
        try:
            stats = self.get_statistics()
            today = datetime.now().strftime('%Y-%m-%d')
            
            with open(self.stats_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    today,
                    stats['total_trades'],
                    stats['winning_trades'],
                    stats['losing_trades'],
                    f"{stats['win_rate']:.2f}",
                    f"{stats['total_pnl']:.4f}",
                    f"{stats['avg_pnl']:.4f}",
                    f"{stats['best_trade']['pnl_usd']:.4f}" if stats['best_trade'] else 0,
                    f"{stats['worst_trade']['pnl_usd']:.4f}" if stats['worst_trade'] else 0,
                    f"{stats['avg_hold_time']:.2f}"
                ])
            
            logger.info(f"Saved daily stats: {stats['total_trades']} trades, P&L: ${stats['total_pnl']:.4f}")
            
        except Exception as e:
            logger.error(f"Error saving daily stats: {e}")
    
    def format_trade_summary(self, pnl_data: Dict) -> str:
        """Format trade summary for Telegram"""
        pnl_usd = pnl_data['pnl_usd']
        emoji = "💚" if pnl_usd > 0 else "❌"
        sign = "+" if pnl_usd >= 0 else ""
        
        # Convert to cents for precision
        pnl_cents = pnl_usd * 100
        
        msg = f"{emoji} **Trade Closed**\n"
        msg += f"📊 **Symbol:** {pnl_data['symbol']}\n"
        msg += f"📈 **Side:** {pnl_data['side'].upper()}\n"
        msg += f"💰 **P&L:** {sign}${pnl_usd:.4f} ({sign}{pnl_cents:.2f}¢)\n"
        msg += f"📊 **ROI:** {sign}{pnl_data['pnl_percent']:.2f}%\n"
        msg += f"⏱ **Hold Time:** {pnl_data['hold_time_minutes']:.1f} min\n"
        msg += f"📍 **Entry:** ${pnl_data['entry_price']:.6f} ({pnl_data['entry_spread']:.2f}% spread)\n"
        msg += f"📍 **Exit:** ${pnl_data['exit_price']:.6f} ({pnl_data['exit_spread']:.2f}% spread)\n"
        msg += f"🔢 **Size:** {pnl_data['size']} contracts @ {pnl_data['leverage']}x"
        
        return msg
