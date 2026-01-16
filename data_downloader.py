"""
Gate.io Historical Data Downloader
Downloads mark_price and last_price candle data for backtesting
"""
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import logging
from typing import List, Dict
import os

logger = logging.getLogger(__name__)


class GateIODataDownloader:
    def __init__(self, settle: str = "usdt"):
        self.settle = settle
        self.base_url = "https://api.gateio.ws/api/v4"
        
    def get_all_contracts(self) -> List[str]:
        """Get all USDT perpetual futures contracts"""
        try:
            url = f"{self.base_url}/futures/{self.settle}/contracts"
            response = requests.get(url)
            response.raise_for_status()
            
            contracts = response.json()
            symbols = [c['name'] for c in contracts if c.get('in_delisting') == False]
            
            logger.info(f"Found {len(symbols)} active contracts")
            return symbols
            
        except Exception as e:
            logger.error(f"Error fetching contracts: {e}")
            return []
    
    def download_candles(self, symbol: str, interval: str = "1m", 
                        from_timestamp: int = None, to_timestamp: int = None,
                        limit: int = 1000) -> pd.DataFrame:
        """
        Download candlestick data
        
        Args:
            symbol: Contract symbol (e.g., BTC_USDT)
            interval: Candle interval (1m, 5m, 15m, 1h, 4h, 1d)
            from_timestamp: Start timestamp
            to_timestamp: End timestamp  
            limit: Max candles per request (max 1000)
        """
        try:
            url = f"{self.base_url}/futures/{self.settle}/candlesticks"
            
            params = {
                'contract': symbol,
                'interval': interval,
                'limit': limit
            }
            
            if from_timestamp:
                params['from'] = from_timestamp
            if to_timestamp:
                params['to'] = to_timestamp
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            candles = response.json()
            
            if not candles:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(candles, columns=[
                'timestamp', 'volume', 'close', 'high', 'low', 'open', 
                'volume_quote', 'volume_settle'
            ])
            
            # Convert types
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            for col in ['close', 'high', 'low', 'open', 'volume']:
                df[col] = pd.to_numeric(df[col])
            
            return df
            
        except Exception as e:
            logger.error(f"Error downloading candles for {symbol}: {e}")
            return pd.DataFrame()
    
    def download_historical_range(self, symbol: str, days: int = 180,
                                  interval: str = "1m") -> pd.DataFrame:
        """
        Download historical data for specified number of days
        
        Args:
            symbol: Contract symbol
            days: Number of days to download
            interval: Candle interval
        """
        logger.info(f"Downloading {days} days of {interval} data for {symbol}...")
        
        all_data = []
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        current_from = start_time
        request_count = 0
        
        while current_from < end_time:
            # Calculate next batch end time (1000 candles ahead)
            if interval == "1m":
                batch_seconds = 1000 * 60  # 1000 minutes
            elif interval == "5m":
                batch_seconds = 1000 * 5 * 60
            elif interval == "15m":
                batch_seconds = 1000 * 15 * 60
            elif interval == "1h":
                batch_seconds = 1000 * 60 * 60
            else:
                batch_seconds = 1000 * 60  # Default to 1m
            
            batch_to = min(current_from + batch_seconds, end_time)
            
            # Download batch
            df = self.download_candles(
                symbol=symbol,
                interval=interval,
                from_timestamp=current_from,
                to_timestamp=batch_to,
                limit=1000
            )
            
            if not df.empty:
                all_data.append(df)
                progress = ((current_from - start_time) / (end_time - start_time) * 100)
                logger.info(f"  Downloaded {len(df)} candles (progress: {progress:.1f}%)")
            
            current_from = batch_to
            request_count += 1
            
            # Rate limit: wait between requests
            if request_count % 5 == 0:
                time.sleep(0.5)  # Pause every 5 requests
        
        if not all_data:
            logger.warning(f"No data downloaded for {symbol}")
            return pd.DataFrame()
        
        # Combine all batches
        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.sort_values('timestamp').reset_index(drop=True)
        
        logger.info(f"Total {len(combined)} candles downloaded for {symbol}")
        
        return combined
    
    def save_to_csv(self, df: pd.DataFrame, symbol: str, output_dir: str = "historical_data"):
        """Save DataFrame to CSV"""
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{output_dir}/{symbol}_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(filename, index=False)
        
        logger.info(f"Saved {len(df)} candles to {filename}")
        return filename
    
    def download_all_symbols(self, days: int = 180, interval: str = "1m",
                            max_symbols: int = None):
        """Download data for all symbols"""
        symbols = self.get_all_contracts()
        
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        logger.info(f"Starting download for {len(symbols)} symbols, {days} days each")
        
        results = {}
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] Processing {symbol}")
            
            try:
                df = self.download_historical_range(symbol, days, interval)
                
                if not df.empty:
                    filename = self.save_to_csv(df, symbol)
                    results[symbol] = {
                        'success': True,
                        'candles': len(df),
                        'file': filename
                    }
                else:
                    results[symbol] = {'success': False, 'error': 'No data'}
                    
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                results[symbol] = {'success': False, 'error': str(e)}
            
            # Rate limiting between symbols
            if i < len(symbols):
                time.sleep(2)
        
        # Summary
        successful = sum(1 for r in results.values() if r.get('success'))
        logger.info(f"\nDownload complete: {successful}/{len(symbols)} symbols successful")
        
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    downloader = GateIODataDownloader()
    
    # Example: Download 30 days of 1m data for top 10 symbols
    results = downloader.download_all_symbols(days=30, interval="1m", max_symbols=10)
    
    print("\nResults:")
    for symbol, result in results.items():
        if result.get('success'):
            print(f"✓ {symbol}: {result['candles']} candles")
        else:
            print(f"✗ {symbol}: {result.get('error')}")
