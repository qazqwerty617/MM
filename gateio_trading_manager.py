"""
Gate.io Futures Trading Manager - USING OFFICIAL SDK
Manages positions, orders, and risk for Gate.io USDT perpetual futures
"""
import gate_api
from gate_api import ApiClient, Configuration, FuturesApi, FuturesOrder
from gate_api import FuturesPriceTriggeredOrder, FuturesInitialOrder, FuturesPriceTrigger
from gate_api.exceptions import ApiException, GateApiException
import logging
import time
from typing import Optional, Dict
from spread_detector import SpreadOpportunity
from trade_analytics import TradeAnalytics

logger = logging.getLogger(__name__)


class GateIOTradingManager:
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
        
        # Initialize Gate.io API client
        config = Configuration(key=api_key, secret=api_secret, host=base_url)
        self.api_client = ApiClient(config)
        self.futures_api = FuturesApi(self.api_client)
        self.settle = "usdt"  # USDT perpetual futures
        
        # Track open positions
        self.open_positions = {}  # {symbol: {'side', 'size', 'entry_price', 'entry_time', 'entry_spread', 'quanto', ...}}
        
        # Position cache for performance (refresh every 30s)
        self.position_cache = {}
        self.cache_time = 0
        self.cache_ttl = 30  # seconds
        
        # Lock for thread-safe position management
        import threading
        self.position_lock = threading.Lock()
        
        # Initialize analytics
        self.analytics = TradeAnalytics()
        
        logger.info(f"Trading Manager initialized: ${position_size_usd} @ {leverage}x, "
                   f"max {max_positions} positions, exit at {exit_spread_threshold}%")
    
    def calculate_unrealized_pnl(self, symbol: str, current_price: float) -> Dict:
        """
        Calculate unrealized P&L and ROI for open position
        
        Returns:
            Dict with 'pnl_usd', 'roi_pct', 'pnl_cents'
        """
        if symbol not in self.open_positions:
            return {'pnl_usd': 0, 'roi_pct': 0, 'pnl_cents': 0}
        
        position = self.open_positions[symbol]
        entry_price = position['entry_price']
        size = position['size']
        side = position['side']
        quanto = position.get('quanto_multiplier', 0.0001)
        
        # Calculate price difference
        if side == 'long':
            price_diff = current_price - entry_price
        else:  # short
            price_diff = entry_price - current_price
        
        # Unrealized P&L (before fees)
        unrealized_pnl = size * price_diff * quanto
        
        # ROI based on margin used
        position_value = size * entry_price * quanto
        margin_used = position_value / self.leverage
        roi_pct = (unrealized_pnl / margin_used * 100) if margin_used > 0 else 0
        
        return {
            'pnl_usd': unrealized_pnl,
            'roi_pct': roi_pct,
            'pnl_cents': unrealized_pnl * 100
        }
    
    def get_contract_info(self, symbol: str) -> Optional[Dict]:
        """Get contract specification"""
        try:
            contract = self.futures_api.get_futures_contract(self.settle, symbol)
            return contract.to_dict()
        except Exception as e:
            logger.error(f"Error getting contract info for {symbol}: {e}")
            return None
    
    def calculate_position_size(self, symbol: str, price: float) -> int:
        """Calculate position size in contracts"""
        try:
            # Get contract info to find quanto multiplier
            contract = self.futures_api.get_futures_contract(self.settle, symbol)
            
            if not contract or not contract.quanto_multiplier:
                logger.warning(f"Could not get contract info for {symbol}, using estimate")
                quantity = self.position_size_usd * self.leverage / price
                return int(round(quantity, 0))
            
            quanto = float(contract.quanto_multiplier)
            
            # Calculate quantity
            # position_value = quantity * price * quanto
            # We want: position_value = position_size_usd * leverage ($2 margin * 20x = $40 volume)
            quantity = (self.position_size_usd * self.leverage) / (price * quanto)
            
            logger.info(f"{symbol}: margin=${self.position_size_usd}, leverage={self.leverage}x, price=${price:.6f}, quanto={quanto}, → {int(quantity)} contracts")
            
            # Respect minimum order size
            min_size = float(contract.order_size_min) if contract.order_size_min else 1.0
            if abs(quantity) < min_size:
                quantity = min_size if quantity > 0 else -min_size
            
            return int(round(quantity, 0))
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            # Fallback
            return int(round(self.position_size_usd * self.leverage / price, 0))
    
    def update_leverage(self, symbol: str):
        """Update leverage for the contract"""
        try:
            self.futures_api.update_position_leverage(self.settle, symbol, str(self.leverage), cross_leverage_limit="0")
            logger.info(f"Set leverage to {self.leverage}x for {symbol}")
        except GateApiException as e:
            # Leverage might already be set, ignore this error
            logger.debug(f"Leverage update message for {symbol}: {e.message}")
        except Exception as e:
            logger.warning(f"Error updating leverage for {symbol}: {e}")
    
    def set_cross_margin(self, symbol: str):
        """Set position to use cross margin mode"""
        try:
            # Set dual mode to false (single position mode) with cross margin
            # mode_value: "single" for one-way mode, "dual" for hedge mode
            from gate_api import PositionsMode
            
            # Try to get current mode first
            try:
                dual_mode = self.futures_api.get_dual_mode(self.settle)
                logger.debug(f"Current dual mode: {dual_mode}")
            except:
                pass
            
            # Update position mode to single (one-way) if needed
            # This is required before we can use cross margin
            try:
                self.futures_api.update_dual_mode(self.settle, False)  # False = single position mode
            except GateApiException as e:
                if "already" not in str(e.message).lower():
                    logger.debug(f"Dual mode message: {e.message}")
            
            # Now set margin mode to cross for this position
            # We do this by setting cross_leverage_limit in the leverage update
            # Already done in update_leverage() with cross_leverage_limit="0"
            
            logger.info(f"Set cross margin mode for {symbol}")
            
        except Exception as e:
            logger.warning(f"Error setting cross margin for {symbol}: {e}")
    
    def get_real_positions_cached(self, force_refresh: bool = False) -> Dict:
        """
        Get actual positions with caching (refreshes every 30s)
        """
        current_time = time.time()
        
        # Return cache if still valid
        if not force_refresh and (current_time - self.cache_time) < self.cache_ttl:
            return self.position_cache
        
        # Refresh cache
        try:
            positions = self.futures_api.list_positions(self.settle)
            real_positions = {}
            
            for pos in positions:
                if int(pos.size) == 0:
                    continue  # Skip closed positions
                
                pos_symbol = pos.contract
                size = int(pos.size)
                side = 'long' if size > 0 else 'short'
                
                real_positions[pos_symbol] = {
                    'side': side,
                    'size': abs(size),
                    'entry_price': float(pos.entry_price) if pos.entry_price else 0,
                    'mark_price': float(pos.mark_price) if pos.mark_price else 0,
                    'unrealised_pnl': float(pos.unrealised_pnl) if pos.unrealised_pnl else 0
                }
            
            self.position_cache = real_positions
            self.cache_time = current_time
            return real_positions
            
        except Exception as e:
            logger.error(f"Error refreshing positions: {e}")
            return self.position_cache  # Return stale cache on error
    
    def sync_positions_from_gateio(self):
        """
        Sync open_positions from Gate.io on startup
        This ensures we track positions that were opened before bot restart
        """
        try:
            real_positions = self.get_real_positions_cached(force_refresh=True)
            
            logger.info(f"Syncing {len(real_positions)} existing positions from Gate.io...")
            
            for symbol, pos_data in real_positions.items():
                # Add to local tracking
                self.open_positions[symbol] = {
                    'side': pos_data['side'],
                    'size': pos_data['size'],
                    'entry_price': pos_data['entry_price'],
                    'entry_time': time.time(),  # Unknown actual time, use now
                    'entry_spread': 0,  # Unknown
                    'order_id': 'synced',
                    'quanto_multiplier': 0.0001  # Default
                }
                logger.info(f"  Synced {symbol}: {pos_data['side'].upper()} x{pos_data['size']}")
            
            logger.info(f"Position sync complete: {len(self.open_positions)} positions tracked")
            
        except Exception as e:
            logger.error(f"Error syncing positions: {e}")
    
    def open_position(self, opportunity: SpreadOpportunity) -> bool:
        """Open a new position based on spread opportunity"""
        # Use lock to prevent race condition with multiple simultaneous signals
        with self.position_lock:
            try:
                symbol = opportunity.symbol
                
                # Double-check we can still open after acquiring lock
                if not self.can_open_position():
                    logger.info(f"Cannot open {symbol}: already at max {self.max_positions} positions")
                    return False
                
                # CRITICAL: Check REAL positions - FORCE REFRESH (no cache!)
                # Cache can be stale, must check Gate.io API directly
                real_positions = self.get_real_positions_cached(force_refresh=True)
                if symbol in real_positions:
                    existing = real_positions[symbol]
                    logger.warning(f"BLOCKED: {symbol} already has {existing['side'].upper()} position on Gate.io (size={existing['size']})")
                    return False
                
                # Set up cross margin mode and leverage
                self.set_cross_margin(symbol)
                self.update_leverage(symbol)
                
                # Calculate position size
                entry_price = opportunity.last  # Use last price for entry
                size = self.calculate_position_size(symbol, entry_price)
                
                if size == 0:
                    logger.error(f"Invalid position size: {size}")
                    return False
                
                # Gate.io uses signed size:
                # Positive = LONG (buy to open)
                # Negative = SHORT (sell to open)
                if opportunity.signal_type == "SHORT":
                    size = -abs(size)
                else:
                    size = abs(size)
                
                # Create order using official SDK
                # IMPORTANT: text field must start with 't-' on Gate.io
                order = FuturesOrder(
                    contract=symbol,
                    size=size,  # Signed: positive=long, negative=short
                    price="0",  # Market order
                    tif="ioc",  # Immediate or cancel
                    text="t-spread"
                )
                
                logger.info(f"Opening {opportunity.signal_type} position on {symbol}: {abs(size)} contracts")
                
                try:
                    order_response = self.futures_api.create_futures_order(self.settle, order)
                    
                    # Get contract info for quanto
                    contract = self.futures_api.get_futures_contract(self.settle, symbol)
                    quanto = float(contract.quanto_multiplier) if contract and contract.quanto_multiplier else 0.0001
                    
                    # Track position with all data needed for analytics
                    self.open_positions[symbol] = {
                        'side': opportunity.signal_type.lower(),
                        'size': abs(size),
                        'entry_price': entry_price,
                        'entry_time': time.time(),
                        'entry_spread': opportunity.spread_percent,
                        'order_id': order_response.id,
                        'quanto_multiplier': quanto
                    }
                    
                    logger.info(f"Successfully opened position on {symbol}, order ID: {order_response.id}")
                    return True
                    
                except GateApiException as ex:
                    logger.error(f"Gate API error opening position on {symbol}: {ex.label} - {ex.message}")
                    return False
                    
            except Exception as e:
                logger.error(f"Error opening position on {symbol}: {e}")
                return False
    
    def close_position(self, symbol: str, exit_price: float, exit_spread: float) -> Optional[Dict]:
        """
        Close an existing position and return trade data
        
        Args:
            symbol: Contract symbol
            exit_price: Current market price (for P&L calculation)
            exit_spread: Current spread percentage
            
        Returns:
            Dict with trade data if successful, None if failed
        """
        try:
            # First check if position exists in our tracking
            if symbol not in self.open_positions:
                logger.warning(f"No tracked position for {symbol}")
                return None
            
            # Verify position actually exists on Gate.io
            real_positions = self.get_real_positions_cached(force_refresh=True)  # Force refresh on close
            if symbol not in real_positions:
                logger.warning(f"Position {symbol} not found on Gate.io - removing from tracking")
                del self.open_positions[symbol]
                return None
            
            position = self.open_positions[symbol]
            real_pos = real_positions[symbol]
            size = real_pos['size']  # Use real size from API
            
            # Close position by opening opposite order WITH reduce_only=True
            # CRITICAL: In dual mode, must use reduce_only to close, not open opposite!
            # Without reduce_only, this would OPEN a new position in opposite direction
            close_size = -size if position['side'] == 'long' else size  # Opposite sign
            
            # Create CLOSING order with reduce_only=True
            order = FuturesOrder(
                contract=symbol,
                size=close_size,  # Opposite direction
                price="0",  # Market order
                tif="ioc",
                text="t-close",
                reduce_only=True  # CRITICAL: This makes it a closing order, not opening!
            )
            
            logger.info(f"Closing {position['side'].upper()} position on {symbol}: {abs(close_size)} contracts (reduce_only=True)")
            
            try:
                order_response = self.futures_api.create_futures_order(self.settle, order)
                
                # Wait a moment for position to close
                import time
                time.sleep(1)
                
                # Get REAL P&L from Gate.io closed position history
                try:
                    # Fetch recent closed positions
                    closed_positions = self.futures_api.list_position_close(self.settle, contract=symbol, limit=1)
                    
                    if closed_positions and len(closed_positions) > 0:
                        closed_pos = closed_positions[0]
                        
                        # Use REAL P&L from Gate.io
                        real_pnl = float(closed_pos.pnl) if closed_pos.pnl else 0
                        real_pnl_usd = real_pnl  # Already in USD
                        
                        logger.info(f"✅ Got REAL P&L from Gate.io: ${real_pnl_usd:.4f}")
                    else:
                        # Fallback to manual calculation
                        logger.warning("Could not fetch closed position, using manual P&L")
                        real_pnl_usd = None
                        
                except Exception as e:
                    logger.error(f"Error fetching closed position: {e}")
                    real_pnl_usd = None
                
                # Prepare trade data
                trade_data = {
                    'symbol': symbol,
                    'side': position['side'],
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'size': position['size'],
                    'leverage': self.leverage,
                    'entry_spread': position.get('entry_spread', 0),
                    'exit_spread': exit_spread,
                    'entry_time': position['entry_time'],
                    'exit_time': time.time(),
                    'quanto_multiplier': position.get('quanto_multiplier', 0.0001),
                    'order_id': order_response.id,
                    'real_pnl_usd': real_pnl_usd  # REAL P&L from Gate.io!
                }
                
                # Log to analytics
                pnl_data = self.analytics.log_trade(trade_data)
                
                # Remove from open positions
                del self.open_positions[symbol]
                
                logger.info(f"Successfully closed position on {symbol}, order ID: {order_response.id}")
                
                return pnl_data
                
            except GateApiException as ex:
                logger.error(f"Gate API error closing position on {symbol}: {ex.label} - {ex.message}")
                return None
                
        except Exception as e:
            logger.error(f"Error closing position on {symbol}: {e}")
            return None
    
    def close_partial_position(self, symbol: str, exit_price: float, exit_spread: float, 
                               percent: int = 50) -> Optional[Dict]:
        """Close partial position (50%) for take-profit"""
        try:
            if symbol not in self.open_positions:
                return None
            
            real_positions = self.get_real_positions_cached(force_refresh=True)
            if symbol not in real_positions:
                return None
            
            position = self.open_positions[symbol]
            real_pos = real_positions[symbol]
            total_size = real_pos['size']
            
            # Calculate partial size
            partial_size = int(total_size * percent / 100)
            if partial_size == 0:
                return None
            
            # Create partial close order with reduce_only
            close_size = -partial_size if position['side'] == 'long' else partial_size
            
            order = FuturesOrder(
                contract=symbol,
                size=close_size,
                price="0",
                tif="ioc",
                text="t-partial",
                reduce_only=True
            )
            
            logger.info(f"🎯 Partial close {percent}% of {symbol}: {abs(close_size)} of {total_size} contracts")
            
            try:
                order_response = self.futures_api.create_futures_order(self.settle, order)
                
                # Wait for partial close to settle
                import time
                time.sleep(1)
                
                # Get REAL P&L from Gate.io for this partial close
                try:
                    # Fetch recent closed positions
                    closed_positions = self.futures_api.list_position_close(self.settle, contract=symbol, limit=1)
                    
                    if closed_positions and len(closed_positions) > 0:
                        closed_pos = closed_positions[0]
                        
                        # Use REAL P&L from Gate.io
                        real_pnl = float(closed_pos.pnl) if closed_pos.pnl else 0
                        real_pnl_usd = real_pnl
                        
                        logger.info(f"✅ Got REAL partial P&L from Gate.io: ${real_pnl_usd:.4f}")
                    else:
                        logger.warning("Could not fetch partial close P&L")
                        real_pnl_usd = None
                        
                except Exception as e:
                    logger.error(f"Error fetching partial close P&L: {e}")
                    real_pnl_usd = None
                
                # Calculate P&L
                trade_data = {
                    'symbol': symbol,
                    'side': position['side'],
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'size': partial_size,
                    'leverage': self.leverage,
                    'entry_spread': position.get('entry_spread', 0),
                    'exit_spread': exit_spread,
                    'entry_time': position['entry_time'],
                    'exit_time': time.time(),
                    'quanto_multiplier': position.get('quanto_multiplier', 0.0001),
                    'order_id': order_response.id,
                    'real_pnl_usd': real_pnl_usd  # REAL P&L from Gate.io!
                }
                
                pnl_data = self.analytics.log_trade(trade_data)
                position['size'] = total_size - partial_size  # Update tracked size
                
                logger.info(f"✅ Partial close successful: P&L=${pnl_data['pnl_usd']:.4f}")
                
                return pnl_data
                
            except GateApiException as ex:
                logger.error(f"Gate API error partial closing position on {symbol}: {ex.label} - {ex.message}")
                return None
            
        except Exception as e:
            logger.error(f"Error partial closing {symbol}: {e}")
            return None
    
    def set_stop_loss_at_entry(self, symbol: str) -> bool:
        """
        Set stop-loss at entry price for remaining position
        Used after partial take-profit to protect the rest
        """
        try:
            if symbol not in self.open_positions:
                return False
            
            position = self.open_positions[symbol]
            entry_price = position['entry_price']
            side = position['side']
            
            # Get current real position size
            real_positions = self.get_real_positions_cached(force_refresh=True)
            if symbol not in real_positions:
                logger.warning(f"No real position for {symbol}, cannot set stop-loss")
                return False
            
            remaining_size = real_positions[symbol]['size']
            
            # Create stop-loss order
            # For LONG: if price drops to entry, sell
            # For SHORT: if price rises to entry, buy
            
            if side == 'long':
                # Stop-loss for LONG: sell when price <= entry
                trigger_order = FuturesPriceTriggeredOrder(
                    initial=FuturesInitialOrder(
                        contract=symbol,
                        size=-int(remaining_size),  # Negative to close long
                        price="0",  # Market order when triggered
                        tif="ioc",
                        text="t-stoploss",
                        reduce_only=True
                    ),
                    trigger=FuturePriceTrigger(
                        strategy_type=0,  # Price trigger
                        price_type=0,  # Last price
                        price=str(entry_price),
                        rule=2  # <= (price drops to or below)
                    )
                )
            else:  # SHORT
                # Stop-loss for SHORT: buy when price >= entry
                trigger_order = FuturesPriceTriggeredOrder(
                    initial=FuturesInitialOrder(
                        contract=symbol,
                        size=int(remaining_size),  # Positive to close short
                        price="0",
                        tif="ioc",
                        text="t-stoploss",
                        reduce_only=True
                    ),
                    trigger=FuturePriceTrigger(
                        strategy_type=0,
                        price_type=0,
                        price=str(entry_price),
                        rule=1  # >= (price rises to or above)
                    )
                )
            
            # Place the stop-loss order
            response = self.futures_api.create_price_triggered_order(self.settle, trigger_order)
            
            logger.info(f"✅ Set STOP-LOSS at entry ${entry_price:.6f} for {symbol} ({remaining_size} contracts)")
            
            # Store stop-loss order ID
            position['stop_loss_order_id'] = response.id
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting stop-loss for {symbol}: {e}")
            return False
    
    def check_exit_conditions(self, symbol: str, current_spread: float, exit_price: float, 
                              take_profit_roi: float = 50.0) -> Optional[Dict]:
        """
        Check if position should be exited
        
        Args:
            symbol: Contract symbol
            current_spread: Current spread percentage
            exit_price: Current market price
            take_profit_roi: ROI threshold for partial take-profit (default 50%)
        
        Returns:
            Trade data if position closed, None otherwise
        """
        if symbol not in self.open_positions:
            return None
        
        position = self.open_positions[symbol]
        
        # Calculate unrealized P&L and ROI
        pnl_data = self.calculate_unrealized_pnl(symbol, exit_price)
        current_roi = pnl_data['roi_pct']
        
        # PARTIAL TAKE PROFIT: Close 50% if ROI >= take_profit_roi
        if current_roi >= take_profit_roi and position.get('partial_closed') != True:
            logger.info(f"🎯 TAKE PROFIT triggered for {symbol}: ROI={current_roi:.1f}% >= {take_profit_roi}%")
            
            # Close 50% of position
            partial_result = self.close_partial_position(symbol, exit_price, current_spread, percent=50)
            
            if partial_result:
                # Mark that we took partial profit
                position['partial_closed'] = True
                logger.info(f"✅ Closed 50% of {symbol} at +{current_roi:.1f}% ROI, holding rest")
                
                # IMMEDIATELY set stop-loss at entry price for remaining 50%
                if self.set_stop_loss_at_entry(symbol):
                    logger.info(f"🛡️ STOP-LOSS set at entry price - position now RISK-FREE!")
                
                return partial_result
        
        # FULL EXIT: Close remaining when spread collapses
        if current_spread <= self.exit_spread_threshold:
            return self.close_position(symbol, exit_price, current_spread)
        
        return None
    
    def can_open_position(self) -> bool:
        """Check if we can open a new position"""
        return len(self.open_positions) < self.max_positions
    
    def has_position(self, symbol: str) -> bool:
        """Check if we have an open position for symbol"""
        return symbol in self.open_positions
    
    def get_open_positions(self) -> Dict:
        """Get all open positions"""
        return self.open_positions.copy()
