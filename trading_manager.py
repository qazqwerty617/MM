"""
Trading Manager Module
Manages automatic trading on MEXC Futures based on spread opportunities
"""
import time
import hmac
import hashlib
import requests
from typing import Dict, List, Optional
from datetime import datetime
import logging
from spread_detector import SpreadOpportunity

logger = logging.getLogger(__name__)


class Position:
    """Represents an open trading position"""
    def __init__(self, symbol: str, side: str, entry_price: float, 
                 size: float, leverage: int, entry_spread: float):
        self.symbol = symbol
        self.side = side  # 'LONG' or 'SHORT'
        self.entry_price = entry_price
        self.size = size
        self.leverage = leverage
        self.entry_spread = entry_spread
        self.entry_time = datetime.now()
        self.order_id = None
    
    def __str__(self):
        return f"{self.symbol} {self.side} @ {self.entry_price} (spread: {self.entry_spread:.2f}%)"


class TradingManager:
    def __init__(self, api_key: str, api_secret: str, base_url: str,
                 position_size_usd: float, leverage: int, max_positions: int,
                 exit_spread_threshold: float):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.position_size_usd = position_size_usd
        self.leverage = leverage
        self.max_positions = max_positions
        self.exit_spread_threshold = exit_spread_threshold
        
        # Track open positions
        self.open_positions: Dict[str, Position] = {}
        
        logger.info(f"Trading Manager initialized: ${position_size_usd} @ {leverage}x, "
                   f"max {max_positions} positions, exit at {exit_spread_threshold}%")
    
    def _sign_request(self, params: Dict) -> str:
        """Generate signature for authenticated API requests"""
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _get_contract_size(self, symbol: str) -> Optional[float]:
        """Get contract size for a symbol"""
        try:
            url = f"{self.base_url}/api/v1/contract/detail"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            if data.get('success'):
                for contract in data.get('data', []):
                    if contract.get('symbol') == symbol:
                        return float(contract.get('contractSize', 0.0001))
            
            logger.warning(f"Contract size not found for {symbol}, using default 0.0001")
            return 0.0001
            
        except Exception as e:
            logger.error(f"Error getting contract size for {symbol}: {e}")
            return 0.0001
    
    def can_open_position(self) -> bool:
        """Check if we can open a new position"""
        return len(self.open_positions) < self.max_positions
    
    def open_position(self, opportunity: SpreadOpportunity) -> bool:
        """
        Open a trading position based on spread opportunity
        
        Returns:
            True if position opened successfully, False otherwise
        """
        try:
            # Check if we can open more positions
            if not self.can_open_position():
                logger.info(f"Max positions ({self.max_positions}) reached, skipping {opportunity.symbol}")
                return False
            
            # Check if we already have a position on this symbol
            if opportunity.symbol in self.open_positions:
                logger.info(f"Already have position on {opportunity.symbol}, skipping")
                return False
            
            # Get contract size
            contract_size = self._get_contract_size(opportunity.symbol)
            
            # Calculate position size in contracts
            # Position value = position_size_usd * leverage
            # Number of contracts = position_value / (entry_price * contract_size)
            entry_price = opportunity.last_price
            position_value = self.position_size_usd * self.leverage
            num_contracts = int(position_value / (entry_price * contract_size))
            
            if num_contracts < 1:
                logger.warning(f"Position size too small for {opportunity.symbol}, skipping")
                return False
            
            # Determine order side
            # LONG opportunity -> open LONG position (BUY)
            # SHORT opportunity -> open SHORT position (SELL)
            order_side = 1 if opportunity.signal_type == 'LONG' else 2  # 1=Open long, 2=Open short
            
            # Prepare order parameters
            timestamp = int(time.time() * 1000)
            params = {
                'symbol': opportunity.symbol,
                'price': entry_price,
                'vol': num_contracts,
                'side': order_side,
                'type': 1,  # 1=Limit order
                'openType': 2,  # 2=Isolated margin
                'leverage': self.leverage,
                'timestamp': timestamp,
                'recvWindow': 5000
            }
            
            # Sign the request
            params['sign'] = self._sign_request(params)
            
            # Send order request
            url = f"{self.base_url}/api/v1/private/order/submit"
            headers = {
                'ApiKey': self.api_key,
                'Request-Time': str(timestamp),
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, json=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('success'):
                order_id = result.get('data')
                
                # Create position object
                position = Position(
                    symbol=opportunity.symbol,
                    side=opportunity.signal_type,
                    entry_price=entry_price,
                    size=num_contracts,
                    leverage=self.leverage,
                    entry_spread=opportunity.spread_percent
                )
                position.order_id = order_id
                
                # Store position
                self.open_positions[opportunity.symbol] = position
                
                logger.info(f"✓ Opened {position}")
                return True
            else:
                logger.error(f"Failed to open position on {opportunity.symbol}: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error opening position on {opportunity.symbol}: {e}")
            return False
    
    def check_exit_conditions(self, symbol: str, current_spread: float) -> bool:
        """
        Check if position should be closed based on current spread
        
        Returns:
            True if position was closed, False otherwise
        """
        if symbol not in self.open_positions:
            return False
        
        position = self.open_positions[symbol]
        
        # Exit condition: spread dropped to exit threshold or below
        if current_spread <= self.exit_spread_threshold:
            logger.info(f"Exit condition met for {symbol}: spread {current_spread:.2f}% <= {self.exit_spread_threshold}%")
            return self.close_position(symbol)
        
        return False
    
    def close_position(self, symbol: str) -> bool:
        """
        Close an open position
        
        Returns:
            True if position closed successfully, False otherwise
        """
        try:
            if symbol not in self.open_positions:
                logger.warning(f"No open position found for {symbol}")
                return False
            
            position = self.open_positions[symbol]
            
            # Determine close side (opposite of open)
            # Close LONG -> SELL, Close SHORT -> BUY
            close_side = 4 if position.side == 'LONG' else 3  # 3=Close short, 4=Close long
            
            # Prepare close order parameters
            timestamp = int(time.time() * 1000)
            params = {
                'symbol': symbol,
                'vol': position.size,
                'side': close_side,
                'type': 5,  # 5=Market order for closing
                'timestamp': timestamp,
                'recvWindow': 5000
            }
            
            # Sign the request
            params['sign'] = self._sign_request(params)
            
            # Send close order request
            url = f"{self.base_url}/api/v1/private/order/submit"
            headers = {
                'ApiKey': self.api_key,
                'Request-Time': str(timestamp),
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, json=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('success'):
                # Remove position from tracking
                del self.open_positions[symbol]
                
                hold_time = (datetime.now() - position.entry_time).total_seconds()
                logger.info(f"✓ Closed {position} after {hold_time:.0f}s")
                return True
            else:
                logger.error(f"Failed to close position on {symbol}: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error closing position on {symbol}: {e}")
            return False
    
    def get_position_count(self) -> int:
        """Get number of open positions"""
        return len(self.open_positions)
    
    def has_position(self, symbol: str) -> bool:
        """Check if we have an open position on symbol"""
        return symbol in self.open_positions
