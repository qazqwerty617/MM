"""
Telegram Command Handler for Gate.io Spread Bot
Allows controlling the bot via Telegram commands
"""
import threading
import requests
import time
import logging

logger = logging.getLogger(__name__)


class TelegramCommandHandler:
    """
    Handle Telegram commands for bot control
    
    Commands:
    /stop - Disable auto-trading
    /start_trading - Enable auto-trading
    /status - Show bot status
    /positions - Show open positions
    /stats - Show trading statistics
    """
    
    def __init__(self, bot_token: str, allowed_user_id: int, bot_instance):
        self.bot_token = bot_token
        self.allowed_user_id = allowed_user_id
        self.bot = bot_instance
        self.running = False
        self.last_update_id = 0
        self.poll_thread = None
        
    def start(self):
        """Start listening for commands"""
        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_updates, daemon=True)
        self.poll_thread.start()
        logger.info("Telegram command handler started")
        
    def stop(self):
        """Stop listening for commands"""
        self.running = False
        if self.poll_thread:
            self.poll_thread.join(timeout=5)
        logger.info("Telegram command handler stopped")
    
    def _poll_updates(self):
        """Poll for Telegram updates"""
        while self.running:
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
                params = {
                    "offset": self.last_update_id + 1,
                    "timeout": 30,
                    "allowed_updates": ["message"]
                }
                
                response = requests.get(url, params=params, timeout=35)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("ok") and data.get("result"):
                        for update in data["result"]:
                            self._handle_update(update)
                            self.last_update_id = update["update_id"]
                            
            except requests.exceptions.Timeout:
                continue
            except Exception as e:
                logger.error(f"Error polling Telegram: {e}")
                time.sleep(5)
    
    def _handle_update(self, update):
        """Handle a single update"""
        try:
            message = update.get("message", {})
            user_id = message.get("from", {}).get("id")
            text = message.get("text", "")
            chat_id = message.get("chat", {}).get("id")
            
            # Only allow commands from authorized user
            if user_id != self.allowed_user_id:
                logger.warning(f"Unauthorized command attempt from user {user_id}")
                return
            
            # Handle commands
            if text.startswith("/"):
                command = text.split()[0].lower()
                self._execute_command(command, chat_id)
                
        except Exception as e:
            logger.error(f"Error handling update: {e}")
    
    def _execute_command(self, command: str, chat_id: int):
        """Execute a command"""
        try:
            if command == "/stop":
                self._cmd_stop(chat_id)
            elif command in ["/start_trading", "/start"]:
                self._cmd_start(chat_id)
            elif command == "/status":
                self._cmd_status(chat_id)
            elif command == "/positions":
                self._cmd_positions(chat_id)
            elif command == "/stats":
                self._cmd_stats(chat_id)
            elif command == "/help":
                self._cmd_help(chat_id)
            else:
                self._send_message(chat_id, f"Unknown command: {command}\nUse /help for list of commands")
                
        except Exception as e:
            logger.error(f"Error executing command {command}: {e}")
            self._send_message(chat_id, f"❌ Error: {e}")
    
    def _cmd_stop(self, chat_id: int):
        """Disable trading"""
        if hasattr(self.bot, 'trading_enabled'):
            self.bot.trading_enabled = False
            self._send_message(chat_id, "🛑 **Trading DISABLED**\n\nBot will continue monitoring but won't open new positions.\nExisting positions will still be managed.\n\nUse /start_trading to re-enable.")
            logger.info("Trading disabled via Telegram command")
        else:
            self._send_message(chat_id, "❌ Could not disable trading")
    
    def _cmd_start(self, chat_id: int):
        """Enable trading"""
        if hasattr(self.bot, 'trading_enabled'):
            self.bot.trading_enabled = True
            self._send_message(chat_id, "✅ **Trading ENABLED**\n\nBot will now open positions when signals detected.\n\nUse /stop to disable.")
            logger.info("Trading enabled via Telegram command")
        else:
            self._send_message(chat_id, "❌ Could not enable trading")
    
    def _cmd_status(self, chat_id: int):
        """Show bot status"""
        try:
            trading_status = "✅ ENABLED" if getattr(self.bot, 'trading_enabled', False) else "🛑 DISABLED"
            
            # Get open positions count
            positions_count = 0
            if hasattr(self.bot, 'trading_manager') and self.bot.trading_manager:
                positions_count = len(self.bot.trading_manager.open_positions)
            
            # Get stats
            stats = self.bot.stats if hasattr(self.bot, 'stats') else {}
            
            msg = f"📊 **Bot Status**\n\n"
            msg += f"🔄 Trading: {trading_status}\n"
            msg += f"📈 Open Positions: {positions_count}/3\n"
            msg += f"📊 Spreads Found: {stats.get('spreads_found', 0)}\n"
            msg += f"✅ Positions Opened: {stats.get('positions_opened', 0)}\n"
            msg += f"💰 Positions Closed: {stats.get('positions_closed', 0)}\n"
            msg += f"\n⚙️ Config:\n"
            msg += f"  • Min Spread: 7%\n"
            msg += f"  • Exit Spread: 2%\n"
            msg += f"  • Position Size: $10 @ 20x\n"
            msg += f"  • Take Profit: 50% ROI"
            
            self._send_message(chat_id, msg)
            
        except Exception as e:
            self._send_message(chat_id, f"❌ Error getting status: {e}")
    
    def _cmd_positions(self, chat_id: int):
        """Show open positions"""
        try:
            if not hasattr(self.bot, 'trading_manager') or not self.bot.trading_manager:
                self._send_message(chat_id, "❌ Trading manager not available")
                return
            
            positions = self.bot.trading_manager.open_positions
            
            if not positions:
                self._send_message(chat_id, "📭 No open positions")
                return
            
            msg = "📊 **Open Positions**\n\n"
            
            for symbol, pos in positions.items():
                side = pos['side'].upper()
                entry = pos['entry_price']
                size = pos['size']
                partial = "✅ TP taken" if pos.get('partial_closed') else "⏳ Waiting"
                
                msg += f"**{symbol}**\n"
                msg += f"  • Side: {side}\n"
                msg += f"  • Entry: ${entry:.6f}\n"
                msg += f"  • Size: {size} contracts\n"
                msg += f"  • Status: {partial}\n\n"
            
            self._send_message(chat_id, msg)
            
        except Exception as e:
            self._send_message(chat_id, f"❌ Error: {e}")
    
    def _cmd_stats(self, chat_id: int):
        """Show trading statistics"""
        try:
            if not hasattr(self.bot, 'trading_manager') or not self.bot.trading_manager:
                self._send_message(chat_id, "❌ Trading manager not available")
                return
            
            stats = self.bot.trading_manager.analytics.get_statistics()
            
            if stats['total_trades'] == 0:
                self._send_message(chat_id, "📭 No trades yet")
                return
            
            msg = "📈 **Trading Statistics**\n\n"
            msg += f"📊 Total Trades: {stats['total_trades']}\n"
            msg += f"✅ Win Rate: {stats['win_rate']:.1f}%\n"
            msg += f"💰 Total P&L: ${stats['total_pnl']:.4f}\n"
            msg += f"📊 Avg P&L: ${stats['avg_pnl']:.4f}\n"
            msg += f"⏱ Avg Hold: {stats['avg_hold_time']:.1f} min\n"
            
            if stats['best_trade']:
                msg += f"\n🏆 Best: {stats['best_trade']['symbol']} +${stats['best_trade']['pnl_usd']:.4f}"
            if stats['worst_trade']:
                msg += f"\n💔 Worst: {stats['worst_trade']['symbol']} ${stats['worst_trade']['pnl_usd']:.4f}"
            
            self._send_message(chat_id, msg)
            
        except Exception as e:
            self._send_message(chat_id, f"❌ Error: {e}")
    
    def _cmd_help(self, chat_id: int):
        """Show help"""
        msg = "🤖 **Bot Commands**\n\n"
        msg += "/stop - Disable auto-trading\n"
        msg += "/start_trading - Enable auto-trading\n"
        msg += "/status - Show bot status\n"
        msg += "/positions - Show open positions\n"
        msg += "/stats - Show trading statistics\n"
        msg += "/help - Show this message"
        
        self._send_message(chat_id, msg)
    
    def _send_message(self, chat_id: int, text: str):
        """Send message to Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }
            requests.post(url, data=data, timeout=10)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
