"""
Gate.io Futures API Client - OPTIMIZED
High-performance WebSocket with REST API support for USDT perpetual futures
"""
import requests
import json
import websocket
import threading
import time
import hmac
import hashlib
from typing import List, Dict, Optional, Callable
import logging

logger = logging.getLogger(__name__)


class GateIOClient:
    def __init__(self, base_url: str, ws_url: str, api_key: str = "", api_secret: str = ""):
        self.base_url = base_url
        self.ws_url = ws_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws = None
        self.ws_thread = None
        self.ping_thread = None
        self.running = False
        self.callbacks = {}
        self.last_ping_time = 0
        self.ping_interval = 30
        
    def _generate_signature(self, method: str, path: str, query_string: str = "", body_payload: str = "") -> tuple:
        """Generate Gate.io API signature"""
        timestamp = str(int(time.time()))
        
        # Hash body payload with SHA512
        hashed_payload = hashlib.sha512(body_payload.encode()).hexdigest()
        
        # Create signature string
        sign_string = f"{method}\n{path}\n{query_string}\n{hashed_payload}\n{timestamp}"
        
        # Generate HMAC-SHA512 signature
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_string.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()
        
        return timestamp, signature
    
    def get_all_symbols(self) -> List[str]:
        """Get all USDT perpetual futures contracts"""
        try:
            url = f"{self.base_url}/futures/usdt/contracts"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract contract names
            symbols = [contract['name'] for contract in data]
            
            logger.info(f"Found {len(symbols)} USDT perpetual futures contracts")
            return symbols
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching symbols: {e}")
            return []
    
    def get_ticker_rest(self, symbol: str) -> Optional[Dict]:
        """Get ticker data via REST API (fallback)"""
        try:
            url = f"{self.base_url}/futures/usdt/tickers?contract={symbol}"
            response = requests.get(url, timeout=3)
            response.raise_for_status()
            
            data = response.json()
            if data and len(data) > 0:
                ticker = data[0]
                return {
                    'contract': ticker.get('contract'),
                    'last': float(ticker.get('last', 0)),
                    'mark_price': float(ticker.get('mark_price', 0))
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error getting ticker for {symbol}: {e}")
            return None
    
    def _send_ping(self):
        """Send periodic application ping to keep connection alive"""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_ping_time >= self.ping_interval:
                    if self.ws and self.ws.sock and self.ws.sock.connected:
                        ping_msg = {
                            "time": int(current_time),
                            "channel": "futures.ping"
                        }
                        self.ws.send(json.dumps(ping_msg))
                        self.last_ping_time = current_time
                        logger.debug("Sent application ping")
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
                if data.get('event') == 'update' and data.get('channel') == 'futures.pong':
                    logger.debug("Received pong")
                    return
                
                # Handle ticker updates
                if data.get('event') == 'update' and data.get('channel') == 'futures.tickers':
                    result = data.get('result', [])
                    if result:
                        logger.debug(f"Received {len(result)} tickers")
                    
                    for ticker in result:
                        try:
                            contract = ticker.get('contract')
                            mark_price = ticker.get('mark_price')
                            last = ticker.get('last')
                            
                            if contract and mark_price is not None and last is not None:
                                ticker_data = {
                                    'symbol': contract,
                                    'mark_price': float(mark_price),
                                    'last': float(last)
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
                "time": int(time.time()),
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": ["!all"]  # Subscribe to all contracts
            }
            ws.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to all futures tickers")
            
            # Start ping thread
            self.ping_thread = threading.Thread(target=self._send_ping, daemon=True)
            self.ping_thread.start()
        
        # Create WebSocket connection
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(ping_interval=60, ping_timeout=10)
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
