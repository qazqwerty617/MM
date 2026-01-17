"""
Gate.io Fair Price Spread Monitoring & Trading Bot
Monitors spread between Mark Price and Last Price on Gate.io USDT perpetual futures
Automatically opens and manages trading positions based on spreads
"""
import yaml
import logging
import time
import sys
import threading
from colorama import init, Fore, Style
from gateio_client import GateIOClient
from spread_detector import SpreadDetector
from telegram_notifier import TelegramNotifier
from telegram_commands import TelegramCommandHandler
from gateio_trading_manager import GateIOTradingManager

# Initialize colorama for colored console output
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GateIOSpreadBot:
    def __init__(self, config_path: str = 'config_gateio.yaml'):
        # Load .env file for secrets (if exists)
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass  # dotenv not installed, use config file
        
        import os
        
        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Override with environment variables (for security)
        self.config['telegram']['bot_token'] = os.getenv('TELEGRAM_BOT_TOKEN', self.config['telegram'].get('bot_token', ''))
        self.config['telegram']['chat_id'] = int(os.getenv('TELEGRAM_CHAT_ID', self.config['telegram'].get('chat_id', 0)))
        self.config['gateio']['api_key'] = os.getenv('GATEIO_API_KEY', self.config['gateio'].get('api_key', ''))
        self.config['gateio']['api_secret'] = os.getenv('GATEIO_API_SECRET', self.config['gateio'].get('api_secret', ''))
        
        # Initialize components
        self.gateio_client = GateIOClient(
            base_url=self.config['gateio']['base_url'],
            ws_url=self.config['gateio']['websocket_url'],
            api_key=self.config['gateio']['api_key'],
            api_secret=self.config['gateio']['api_secret']
        )
        
        self.spread_detector = SpreadDetector(
            min_threshold=self.config['spread']['min_threshold']
        )
        
        # Get symbol whitelist if configured
        self.symbol_whitelist = self.config['spread'].get('symbol_whitelist', None)
        
        self.telegram_notifier = TelegramNotifier(
            bot_token=self.config['telegram']['bot_token'],
            chat_id=self.config['telegram']['chat_id']
        )
        
        # Initialize trading manager FIRST (if enabled)
        self.trading_enabled = self.config['trading']['enabled']
        self.trading_manager = None
        
        if self.trading_enabled:
            self.trading_manager = GateIOTradingManager(
                api_key=self.config['gateio']['api_key'],
                api_secret=self.config['gateio']['api_secret'],
                base_url=self.config['gateio']['base_url'],
                position_size_usd=self.config['trading']['position_size_usd'],
                leverage=self.config['trading']['leverage'],
                max_positions=self.config['trading']['max_positions'],
                exit_spread_threshold=self.config['trading']['exit_spread_threshold']
            )
            logger.info("Trading Manager enabled")
        else:
            logger.info("Trading disabled - monitoring only mode")
        
        # Initialize periodic reporter AFTER trading_manager (8-hour reports)
        if self.trading_manager:
            from periodic_reporter import PeriodicReporter
            self.periodic_reporter = PeriodicReporter(
                analytics=self.trading_manager.analytics,
                telegram_notifier=self.telegram_notifier,
                interval_hours=8
            )
        else:
            self.periodic_reporter = None
        
        # Track symbols to monitor
        self.symbols = []
        self.running = False
        
        # Performance statistics
        self.stats = {
            'tickers_processed': 0,
            'spreads_found': 0,
            'positions_opened': 0,
            'positions_closed': 0,
            'errors': 0
        }
        
        # Initialize Telegram command handler for remote control
        self.command_handler = TelegramCommandHandler(
            bot_token=self.config['telegram']['bot_token'],
            allowed_user_id=self.config['telegram']['chat_id'],
            bot_instance=self
        )
        
        logger.info("Gate.io Spread Bot initialized")
    
    def on_ticker_update(self, ticker_data: dict):
        """Callback for WebSocket ticker updates - OPTIMIZED"""
        try:
            symbol = ticker_data['symbol']
            
            # Filter by whitelist if configured
            if self.symbol_whitelist and symbol not in self.symbol_whitelist:
                return  # Skip symbols not in whitelist
            
            mark_price = ticker_data['mark_price']
            last_price = ticker_data['last']
            
            # Skip invalid data
            if mark_price <= 0 or last_price <= 0:
                return
            
            # Detect spread opportunity
            opportunity = self.spread_detector.detect_spread(symbol, mark_price, last_price)
            
            if opportunity:
                self.stats['spreads_found'] += 1
                
                # Log to console with color
                if opportunity.signal_type == 'LONG':
                    print(f"{Fore.GREEN}✓ {opportunity}{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}✓ {opportunity}{Style.RESET_ALL}")
                
                # Send Telegram alert (non-blocking)
                self.telegram_notifier.send_spread_alert(opportunity)
                
                # Trading logic
                if self.trading_enabled and self.trading_manager:
                    # Try to open position (has internal lock and checks)
                    success = self.trading_manager.open_position(opportunity)
                    if success:
                        self.stats['positions_opened'] += 1
                        
                        # Get position details for notification
                        position = self.trading_manager.open_positions.get(symbol)
                        if position:
                            entry_price = position['entry_price']
                            msg = f"📈 Opened {opportunity.signal_type} position on {symbol} @ ${entry_price:.6f}\n"
                            msg += f"Spread: {opportunity.spread_percent:.2f}%, Size: {abs(position['size'])} contracts"
                        else:
                            msg = f"📈 Opened {opportunity.signal_type} position on {symbol}"
                        
                        print(f"{Fore.CYAN}{msg.split(chr(10))[0]}{Style.RESET_ALL}")
                        self.telegram_notifier.send_message(msg)
            
            # Check exit conditions for ALL open positions on EVERY ticker update
            # CRITICAL: This must run even if no spread opportunity detected!
            if self.trading_enabled and self.trading_manager:
                if self.trading_manager.has_position(symbol):
                    # Get position info to know which direction
                    positions = self.trading_manager.get_open_positions()
                    position = positions.get(symbol)
                    
                    if position:
                        # Calculate spread in the SAME direction as entry
                        # For LONG: we entered when mark > last, check if still mark > last
                        # For SHORT: we entered when mark < last, check if still mark < last
                        
                        if position['side'] == 'long':
                            # LONG position: we want mark_price > last_price
                            # Exit if difference becomes too small OR reverses
                            if mark_price > last_price:
                                current_spread = ((mark_price - last_price) / last_price) * 100
                            else:
                                # Reversed: mark now < last, should exit immediately!
                                current_spread = 0
                                logger.info(f"LONG position {symbol} reversed! Mark={mark_price}, Last={last_price}")
                        else:  # SHORT position
                            # SHORT position: we want mark_price < last_price  
                            # Exit if difference becomes too small OR reverses
                            if mark_price < last_price:
                                current_spread = ((last_price - mark_price) / mark_price) * 100
                            else:
                                # Reversed: mark now > last, should exit immediately!
                                current_spread = 0
                                logger.info(f"SHORT position {symbol} reversed! Mark={mark_price}, Last={last_price}")
                        
                        # Log spread for debugging every 100 ticks
                        if self.stats['tickers_processed'] % 100 == 0:
                            logger.info(f"{symbol} {position['side'].upper()}: spread={current_spread:.2f}%, exit_threshold={self.trading_manager.exit_spread_threshold}%")
                        
                        # Check if we should exit (pass last_price for P&L calculation)
                        take_profit_roi = self.config['trading'].get('take_profit_roi', 50.0)
                        trade_data = self.trading_manager.check_exit_conditions(
                            symbol, 
                            current_spread, 
                            last_price,
                            take_profit_roi=take_profit_roi
                        )
                        if trade_data:
                            self.stats['positions_closed'] += 1
                            
                            # Send detailed P&L notification
                            msg = self.trading_manager.analytics.format_trade_summary(trade_data)
                            print(f"{Fore.YELLOW}📉 Closed {symbol}: P&L=${trade_data['pnl_usd']:.4f}{Style.RESET_ALL}")
                            self.telegram_notifier.send_message(msg)
            
            # Update stats
            self.stats['tickers_processed'] += 1
                
        except Exception as e:
            logger.error(f"Error processing ticker update: {e}")
            self.stats['errors'] += 1
    
    def start(self):
        """Start the bot"""
        try:
            print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Gate.io Fair Price Spread Bot{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Minimum Spread: {self.config['spread']['min_threshold']}%{Style.RESET_ALL}")
            
            if self.trading_enabled:
                print(f"{Fore.GREEN}Trading: ENABLED{Style.RESET_ALL}")
                print(f"{Fore.GREEN}  Position Size: ${self.config['trading']['position_size_usd']} @ {self.config['trading']['leverage']}x{Style.RESET_ALL}")
                print(f"{Fore.GREEN}  Max Positions: {self.config['trading']['max_positions']}{Style.RESET_ALL}")
                print(f"{Fore.GREEN}  Exit Threshold: {self.config['trading']['exit_spread_threshold']}%{Style.RESET_ALL}")
            else:
                print(f"{Fore.YELLOW}Trading: DISABLED (Monitoring Only){Style.RESET_ALL}")
            
            print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
            
            # Send test message
            logger.info("Sending test message to Telegram...")
            if self.telegram_notifier.send_test_message():
                print(f"{Fore.GREEN}✓ Telegram connection successful{Style.RESET_ALL}\n")
            else:
                print(f"{Fore.RED}✗ Telegram connection failed{Style.RESET_ALL}\n")
                return
            
            # Fetch all futures contracts
            logger.info("Fetching USDT perpetual futures contracts...")
            self.contracts = self.gateio_client.get_all_symbols()
            
            if not self.contracts:
                logger.error("No contracts found!")
                return
            
            print(f"{Fore.GREEN}✓ Found {len(self.contracts)} USDT perpetual futures pairs{Style.RESET_ALL}")
            
            # Sync existing positions from Gate.io (if any)
            if self.trading_manager:
                self.trading_manager.sync_positions_from_gateio()
            
            print(f"{Fore.YELLOW}Monitoring for spreads >= {self.config['spread']['min_threshold']}%...{Style.RESET_ALL}\n")
            
            # Subscribe to WebSocket tickers for real-time updates
            logger.info("Subscribing to WebSocket tickers...")
            self.running = True
            self.gateio_client.subscribe_tickers(self.on_ticker_update)
            
            print(f"{Fore.GREEN}✓ WebSocket connected - monitoring live data{Style.RESET_ALL}\n")
            
            # Start Telegram command handler for remote control
            self.command_handler.start()
            print(f"{Fore.GREEN}✓ Telegram command handler started - use /help for commands{Style.RESET_ALL}\n")
            
            # Start statistics display thread
            def show_stats():
                while self.running:
                    time.sleep(30)  # Show stats every 30 seconds
                    if self.running and self.trading_manager:
                        stats = self.trading_manager.analytics.get_statistics()
                        
                        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
                        print(f"{Fore.CYAN}📊 Bot Statistics{Style.RESET_ALL}")
                        print(f"  Tickers Processed: {self.stats['tickers_processed']}")
                        print(f"  Spreads Found: {self.stats['spreads_found']}")
                        print(f"  Positions Opened: {self.stats['positions_opened']}")
                        print(f"  Positions Closed: {self.stats['positions_closed']}")
                        
                        if stats['total_trades'] > 0:
                            pnl_color = Fore.GREEN if stats['total_pnl'] >= 0 else Fore.RED
                            print(f"\n{Fore.CYAN}💰 Trading Performance{Style.RESET_ALL}")
                            print(f"  Total Trades: {stats['total_trades']}")
                            print(f"  Win Rate: {Fore.GREEN}{stats['win_rate']:.1f}%{Style.RESET_ALL} ({stats['winning_trades']}W / {stats['losing_trades']}L)")
                            print(f"  Total P&L: {pnl_color}${stats['total_pnl']:.4f}{Style.RESET_ALL}")
                            print(f"  Avg P&L: ${stats['avg_pnl']:.4f}")
                            print(f"  Avg Hold Time: {stats['avg_hold_time']:.1f} min")
                            
                            if stats['best_trade']:
                                print(f"  Best Trade: {Fore.GREEN}{stats['best_trade']['symbol']} +${stats['best_trade']['pnl_usd']:.4f}{Style.RESET_ALL}")
                            if stats['worst_trade']:
                                print(f"  Worst Trade: {Fore.RED}{stats['worst_trade']['symbol']} ${stats['worst_trade']['pnl_usd']:.4f}{Style.RESET_ALL}")
                        
                        print(f"  Errors: {self.stats['errors']}")
                        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
            
            stats_thread = threading.Thread(target=show_stats, daemon=True)
            stats_thread.start()
            
            # Start periodic reporter
            if self.periodic_reporter:
                self.periodic_reporter.start()
                logger.info("Periodic 8-hour reports enabled")
            
            # Keep the bot running
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            self.stop()
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            self.stop()
    
    def stop(self):
        """Stop the bot"""
        logger.info("Shutting down bot...")
        self.running = False
        
        # Stop periodic reporter
        if self.periodic_reporter:
            self.periodic_reporter.stop()
        
        # Stop command handler
        if hasattr(self, 'command_handler'):
            self.command_handler.stop()
        
        # Stop WebSocket
        self.gateio_client.stop()
        print(f"\n{Fore.YELLOW}Bot stopped{Style.RESET_ALL}")


if __name__ == "__main__":
    try:
        bot = GateIOSpreadBot()
        bot.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
