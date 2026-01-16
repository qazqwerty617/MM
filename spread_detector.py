"""
Spread Detection Module - OPTIMIZED
Fast and accurate spread calculation with caching
Works with both MEXC (fairPrice) and Gate.io (mark_price)
"""
from typing import Optional, Dict
import logging
import time

logger = logging.getLogger(__name__)


class SpreadOpportunity:
    def __init__(self, symbol: str, mark_price: float, last_price: float, 
                 spread_percent: float, signal_type: str):
        self.symbol = symbol
        self.mark_price = mark_price  # Fair Price (MEXC) or Mark Price (Gate.io)
        self.fair_price = mark_price  # Alias for backwards compatibility
        self.last_price = last_price
        self.last = last_price  # Alias for Gate.io
        self.spread_percent = spread_percent
        self.signal_type = signal_type  # 'LONG' or 'SHORT'
        self.timestamp = time.time()
    
    def __str__(self):
        return (f"{self.symbol} {self.spread_percent:.2f}% "
                f"(Fair: {self.fair_price:.6f}, Last: {self.last_price:.6f}, {self.signal_type})")


class SpreadDetector:
    def __init__(self, min_threshold: float):
        self.min_threshold = min_threshold
        self.last_alert = {}  # Cache to prevent duplicate alerts
        self.alert_cooldown = 60  # seconds
    
    def detect_spread(self, symbol: str, mark_price: float, last_price: float) -> Optional[SpreadOpportunity]:
        """
        Detect spread opportunity between Mark Price and Last Price
        
        Strategy:
        - LONG: Mark Price > Last Price (market undervalued, mark price higher)
        - SHORT: Mark Price < Last Price (market overvalued, mark price lower)
        
        Works with:
        - MEXC: fairPrice = mark_price
        - Gate.io: mark_price = mark_price
        
        Returns:
            SpreadOpportunity if spread >= threshold, None otherwise
        """
        if mark_price <= 0 or last_price <= 0:
            return None
        
        # Check cooldown to prevent spam on same symbol
        current_time = time.time()
        if symbol in self.last_alert:
            if current_time - self.last_alert[symbol] < self.alert_cooldown:
                return None
        
        # Calculate spread in both directions
        # LONG signal: Mark Price > Last Price (mark price is higher)
        # SHORT signal: Mark Price < Last Price (mark price is lower)
        
        if mark_price > last_price:
            # Potential LONG signal
            # Spread = ((mark_price - last_price) / last_price) * 100
            spread_percent = ((mark_price - last_price) / last_price) * 100
            
            if spread_percent >= self.min_threshold:
                self.last_alert[symbol] = current_time
                return SpreadOpportunity(
                    symbol=symbol,
                    mark_price=mark_price,
                    last_price=last_price,
                    spread_percent=spread_percent,
                    signal_type='LONG'
                )
        
        elif mark_price < last_price:
            # Potential SHORT signal
            # Spread = ((last_price - mark_price) / mark_price) * 100
            spread_percent = ((last_price - mark_price) / mark_price) * 100
            
            if spread_percent >= self.min_threshold:
                self.last_alert[symbol] = current_time
                return SpreadOpportunity(
                    symbol=symbol,
                    mark_price=mark_price,
                    last_price=last_price,
                    spread_percent=spread_percent,
                    signal_type='SHORT'
                )
        
        return None
