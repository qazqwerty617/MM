"""
Periodic Report Generator
Sends trading statistics to Telegram every N hours
"""
import threading
import time
import logging
from datetime import datetime
from typing import Dict

logger = logging.getLogger(__name__)


class PeriodicReporter:
    def __init__(self, analytics, telegram_notifier, interval_hours: int = 8):
        """
        Initialize periodic reporter
        
        Args:
            analytics: TradeAnalytics instance
            telegram_notifier: TelegramNotifier instance
            interval_hours: How often to send reports (default: 8 hours)
        """
        self.analytics = analytics
        self.telegram = telegram_notifier
        self.interval_seconds = interval_hours * 3600
        self.running = False
        self.thread = None
        
    def start(self):
        """Start the periodic reporter"""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._reporter_loop, daemon=True)
        self.thread.start()
        logger.info(f"Periodic reporter started (every {self.interval_seconds/3600:.1f} hours)")
    
    def stop(self):
        """Stop the periodic reporter"""
        self.running = False
    
    def _reporter_loop(self):
        """Background loop that sends reports"""
        while self.running:
            # Wait for next report time
            time.sleep(self.interval_seconds)
            
            if self.running:
                try:
                    self.send_report()
                except Exception as e:
                    logger.error(f"Error sending periodic report: {e}")
    
    def send_report(self, force: bool = False):
        """
        Generate and send trading report
        
        Args:
            force: Send immediately even if not time yet
        """
        try:
            stats = self.analytics.get_statistics()
            symbol_perf = self.analytics.get_symbol_performance()
            
            # Build report message
            report = self._format_report(stats, symbol_perf)
            
            # Send to Telegram
            self.telegram.send_message(report)
            logger.info("Sent periodic trading report")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
    
    def _format_report(self, stats: Dict, symbol_perf: Dict) -> str:
        """Format trading statistics as Telegram message"""
        
        # Header
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        msg = f"📊 **Trading Report** - {now}\n"
        msg += "="*40 + "\n\n"
        
        # Overall stats
        if stats['total_trades'] == 0:
            msg += "❌ No trades yet\n"
            return msg
        
        total_pnl = stats['total_pnl']
        pnl_emoji = "💚" if total_pnl >= 0 else "❌"
        sign = "+" if total_pnl >= 0 else ""
        
        msg += f"**Total Trades:** {stats['total_trades']}\n"
        msg += f"**Win Rate:** {stats['win_rate']:.1f}% ({stats['winning_trades']}W / {stats['losing_trades']}L)\n"
        msg += f"**Total P&L:** {pnl_emoji} {sign}${total_pnl:.4f}\n"
        msg += f"**Avg P&L:** ${stats['avg_pnl']:.4f}\n"
        msg += f"**Total Fees:** ${stats['total_fees']:.4f}\n\n"
        
        # Per-symbol breakdown
        if symbol_perf:
            msg += "**Per Symbol:**\n"
            msg += "-"*40 + "\n"
            
            # Sort by total P&L (best to worst)
            sorted_symbols = sorted(
                symbol_perf.items(),
                key=lambda x: x[1]['total_pnl'],
                reverse=True
            )
            
            for symbol, perf in sorted_symbols:
                pnl = perf['total_pnl']
                emoji = "💚" if pnl >= 0 else "❌"
                sign = "+" if pnl >= 0 else ""
                
                msg += f"{emoji} **{symbol}** - {sign}${pnl:.4f}\n"
                msg += f"   ({perf['trades']} trades, {perf['win_rate']:.0f}% WR)\n"
            
            msg += "\n"
        
        # Best & Worst trades
        if stats['best_trade'] and stats['worst_trade']:
            msg += "**Extremes:**\n"
            msg += f"🏆 Best: ${stats['best_trade']:.4f}\n"
            msg += f"💔 Worst: ${stats['worst_trade']:.4f}\n\n"
        
        # Footer
        msg += "="*40 + "\n"
        msg += f"✨ Keep trading! Next report in 8h\n"
        
        return msg
    
    def send_manual_report(self):
        """Send report immediately (manual trigger)"""
        logger.info("Sending manual report...")
        self.send_report(force=True)
