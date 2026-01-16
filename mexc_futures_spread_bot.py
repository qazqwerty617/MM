"""
MEXC Fair Price Spread Monitoring & Trading Bot
Monitors spread between Fair Price and Last Price on MEXC USDT-M futures
Automatically opens and manages trading positions based on spreads
"""
import yaml
import logging
import time
import sys
import threading
from colorama import init, Fore, Style
from mexc_client import MEXCClient
from spread_detector import SpreadDetector
from telegram_notifier import TelegramNotifier
from trading_manager import TradingManager

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


class MEXCSpreadBot:
    def __init__(self, config_path: str = 'config.yaml'):
        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize components
        self.mexc_client = MEXCClient(
            base_url=self.config['mexc']['base_url'],
            ws_url=self.config['mexc']['websocket_url']
        )
        
        self.spread_detector = SpreadDetector(
            min_threshold=self.config['spread']['min_threshold']
        )
        
        self.telegram_notifier = TelegramNotifier(
            bot_token=self.config['telegram']['bot_token'],
            chat_id=self.config['telegram']['chat_id']
        )
        
        # Initialize trading manager (if enabled)
        self.trading_enabled = self.config['trading']['enabled']
        self.trading_manager = None
        
        if self.trading_enabled:
            self.trading_manager = TradingManager(
                api_key=self.config['mexc']['api_key'],
                api_secret=self.config['mexc']['api_secret'],
                base_url=self.config['mexc']['base_url'],
                position_size_usd=self.config['trading']['position_size_usd'],
                leverage=self.config['trading']['leverage'],
                max_positions=self.config['trading']['max_positions'],
                exit_spread_threshold=self.config['trading']['exit_spread_threshold']
            )
            logger.info("Trading Manager enabled")
        else:
            logger.info("Trading disabled - monitoring only mode")
        
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
        
        logger.info("MEXC Spread Bot initialized")
    
    def on_ticker_update(self, ticker_data: dict):
        """Callback for WebSocket ticker updates - OPTIMIZED"""
        try:
            symbol = ticker_data['symbol']
            fair_price = ticker_data['fairPrice']
            last_price = ticker_data['lastPrice']
            
            # Skip invalid data
            if fair_price <= 0 or last_price <= 0:
                return
            
            # Detect spread opportunity
            opportunity = self.spread_detector.detect_spread(symbol, fair_price, last_price)
            
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
                    # Try to open position if we don't have one already
                    if self.trading_manager.can_open_position() and not self.trading_manager.has_position(symbol):
                        success = self.trading_manager.open_position(opportunity)
                        if success:
                            self.stats['positions_opened'] += 1
                            msg = f"📈 Opened {opportunity.signal_type} position on {symbol}"
                            print(f"{Fore.CYAN}{msg}{Style.RESET_ALL}")
                            self.telegram_notifier.send_message(msg)
            
            # Check exit conditions for existing positions
            if self.trading_enabled and self.trading_manager:
                if self.trading_manager.has_position(symbol):
                    # Calculate current spread
                    if fair_price > last_price:
                        current_spread = ((fair_price - last_price) / last_price) * 100
                    elif fair_price < last_price:
                        current_spread = ((last_price - fair_price) / fair_price) * 100
                    else:
                        current_spread = 0
                    
                    # Check if we should exit
                    if self.trading_manager.check_exit_conditions(symbol, current_spread):
                        self.stats['positions_closed'] += 1
                        msg = f"📉 Closed position on {symbol} (spread: {current_spread:.2f}%)"
                        print(f"{Fore.YELLOW}{msg}{Style.RESET_ALL}")
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
            print(f"{Fore.CYAN}MEXC Fair Price Spread Bot{Style.RESET_ALL}")
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
            
            # Get all USDT-M symbols
            logger.info("Fetching USDT-M futures symbols...")
            self.symbols = self.mexc_client.get_all_symbols()
            
            if not self.symbols:
                logger.error("No symbols found!")
                return
            
            print(f"{Fore.GREEN}✓ Found {len(self.symbols)} USDT-M futures pairs{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Monitoring for spreads >= {self.config['spread']['min_threshold']}%...{Style.RESET_ALL}\n")
            
            # Subscribe to WebSocket tickers for real-time updates
            logger.info("Subscribing to WebSocket tickers...")
            self.running = True
            self.mexc_client.subscribe_tickers(self.on_ticker_update)
            
            print(f"{Fore.GREEN}✓ WebSocket connected - monitoring live data{Style.RESET_ALL}\n")
            
            # Start statistics display thread
            def show_stats():
                while self.running:
                    time.sleep(30)  # Show stats every 30 seconds
                    if self.running:
                        print(f"\n{Fore.CYAN}📊 Stats: Tickers={self.stats['tickers_processed']}, "
                              f"Spreads={self.stats['spreads_found']}, "
                              f"Opened={self.stats['positions_opened']}, "
                              f"Closed={self.stats['positions_closed']}, "
                              f"Errors={self.stats['errors']}{Style.RESET_ALL}\n")
            
            stats_thread = threading.Thread(target=show_stats, daemon=True)
            stats_thread.start()
            
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
        self.mexc_client.stop()
        print(f"\n{Fore.YELLOW}Bot stopped{Style.RESET_ALL}")


if __name__ == "__main__":
    try:
        bot = MEXCSpreadBot()
        bot.start()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
