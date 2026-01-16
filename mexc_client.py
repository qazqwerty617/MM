"""
MEXC Futures API Client - OPTIMIZED
High-performance WebSocket with ping/pong keep-alive and fallback mechanisms
"""
import requests
import json
import websocket
import threading
import time
from typing import List, Dict, Optional, Callable
import logging

logger = logging.getLogger(__name__)


class MEXCClient:
    def __init__(self, base_url: str, ws_url: str):
        self.base_url = base_url
        self.ws_url = ws_url
        self.ws = None
        self.ws_thread = None
        self.ping_thread = None
        self.running = False
        self.callbacks = {}
        self.last_ping_time = 0
        self.ping_interval = 30  # Send ping every 30 seconds
        
    def get_all_symbols(self) -> List[str]:
        """Get all USDT-M futures symbols"""
        try:
            url = f"{self.base_url}/api/v1/contract/detail"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if not data.get('success'):
                logger.error(f"Failed to get symbols: {data}")
                return []
            
            # Filter only USDT perpetual contracts
            symbols = []
            for contract in data.get('data', []):
                symbol = contract.get('symbol', '')
                if symbol.endswith('_USDT'):
                    symbols.append(symbol)
            
            logger.info(f"Found {len(symbols)} USDT-M futures symbols")
            return symbols
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching symbols: {e}")
            return []
    
    def get_ticker_rest(self, symbol: str) -> Optional[Dict]:
        """Get ticker data via REST API (fallback)"""
        try:
            url = f"{self.base_url}/api/v1/contract/ticker/{symbol}"
            response = requests.get(url, timeout=3)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success') and data.get('data'):
                ticker_data = data['data']
                return {
                    'symbol': symbol,
                    'lastPrice': float(ticker_data.get('lastPrice', 0)),
                    'fairPrice': float(ticker_data.get('fairPrice', 0))
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error getting ticker for {symbol}: {e}")
            return None
    
    def _send_ping(self):
        """Send periodic ping to keep connection alive"""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_ping_time >= self.ping_interval:
                    if self.ws and self.ws.sock and self.ws.sock.connected:
                        ping_msg = {"method": "ping"}
                        self.ws.send(json.dumps(ping_msg))
                        self.last_ping_time = current_time
                        logger.debug("Sent ping")
                time.sleep(5)
            except Exception as e:
                logger.debug(f"Ping error: {e}")
    
    def subscribe_tickers(self, callback: Callable):
        """Subscribe to all tickers via WebSocket for real-time updates"""
        self.callbacks['tickers'] = callback
        self.running = True
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle pong response
                if data.get('channel') == 'pong' or data.get('msg') == 'pong':
                    logger.debug("Received pong")
                    return
                
                # Handle ticker updates
                if data.get('channel') == 'push.tickers':
                    tickers = data.get('data', [])
                    if tickers:
                        logger.debug(f"Received {len(tickers)} tickers")
                    
                    for ticker in tickers:
                        try:
                            symbol = ticker.get('symbol')
                            fair_price = ticker.get('fairPrice')
                            last_price = ticker.get('lastPrice')
                            
                            if symbol and fair_price is not None and last_price is not None:
                                ticker_data = {
                                    'symbol': symbol,
                                    'fairPrice': float(fair_price),
                                    'lastPrice': float(last_price)
                                }
                                
                                if 'tickers' in self.callbacks:
                                    self.callbacks['tickers'](ticker_data)
                        except Exception as e:
                            logger.debug(f"Error processing ticker: {e}")
                            
            except json.JSONDecodeError as e:
                logger.debug(f"Failed to parse message: {message[:100]}")
            except Exception as e:
                logger.error(f"Error in on_message: {e}")
        
        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
            if self.running:
                logger.info("Attempting to reconnect WebSocket in 5 seconds...")
                time.sleep(5)
                if self.running:
                    self.subscribe_tickers(callback)
        
        def on_open(ws):
            logger.info("WebSocket connected successfully")
            self.last_ping_time = time.time()
            
            # Subscribe to all tickers
            subscribe_msg = {
                "method": "sub.tickers",
                "param": {}
            }
            ws.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to all tickers")
            
            # Start ping thread
            self.ping_thread = threading.Thread(target=self._send_ping, daemon=True)
            self.ping_thread.start()
        
        # Create WebSocket connection with ping/pong
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(ping_interval=30, ping_timeout=10)
        )
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        logger.info("WebSocket thread started with ping/pong")
    
    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
        logger.info("WebSocket connection stopped")
