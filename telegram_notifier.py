"""
Telegram Notifier Module - OPTIMIZED
Async notifications with queue system for high performance
"""
import asyncio
import logging
from telegram import Bot
from telegram.error import TelegramError
from spread_detector import SpreadOpportunity
from queue import Queue
import threading

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: int):
        self.bot = Bot(token=bot_token)
        self.bot_token = bot_token  # Store for direct API access
        self.chat_id = chat_id
        self.message_queue = Queue()
        self.sender_thread = None
        self.running = False
        
        # Start message sender thread
        self._start_sender()
        
    def _start_sender(self):
        """Start background thread for sending messages"""
        self.running = True
        self.sender_thread = threading.Thread(target=self._message_sender, daemon=True)
        self.sender_thread.start()
    
    def _message_sender(self):
        """Background worker that sends queued messages"""
        while self.running:
            try:
                if not self.message_queue.empty():
                    message = self.message_queue.get()
                    self._send_sync(message)
                else:
                    import time
                    time.sleep(0.1)  # Fixed: was asyncio.sleep which caused RuntimeWarning
            except Exception as e:
                logger.error(f"Error in message sender: {e}")
    
    def _send_sync(self, message: str):
        """Synchronously send message using requests"""
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.bot.token}/sendMessage"
            data = {
                'chat_id': self.chat_id,
                'text': message
            }
            response = requests.post(url, json=data, timeout=5)
            response.raise_for_status()
            logger.debug(f"Message sent: {message[:50]}...")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    def send_spread_alert(self, opportunity: SpreadOpportunity):
        """Queue spread alert for sending (non-blocking)"""
        try:
            # Format message according to user's preference
            signal_emoji = "🟢" if opportunity.signal_type == "LONG" else "🔴"
            
            message = (
                f"{opportunity.symbol} {opportunity.spread_percent:.2f}%\n\n"
                f"Fair price: {opportunity.fair_price:.6f}\n"
                f"Last price: {opportunity.last_price:.6f}\n\n"
                f"Side: {signal_emoji} {opportunity.signal_type.lower()}"
            )
            
            # Add to queue for sending
            self.message_queue.put(message)
            logger.info(f"Queued alert for {opportunity.symbol} ({opportunity.signal_type})")
            
        except Exception as e:
            logger.error(f"Error queuing alert: {e}")
    
    def send_message(self, text: str):
        """Queue any message for sending (non-blocking)"""
        self.message_queue.put(text)
    
    def send_test_message(self):
        """Send a test message to verify Telegram connection"""
        try:
            message = "🤖 MEXC Fair Price Spread Bot - Запущен и работает!"
            self._send_sync(message)
            logger.info("Test message sent successfully")
            return True
        except Exception as e:
            logger.error(f"Error sending test message: {e}")
            return False
    
    def stop(self):
        """Stop the notifier"""
        self.running = False
