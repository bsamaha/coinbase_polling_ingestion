import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv
from src.services.coinbase_client import CoinbaseClient
from src.models.product import Product
from src.models.candle import Candle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoDataCollector:
    def __init__(self):
        logger.info("Initializing CryptoDataCollector")
        load_dotenv()
        
        api_key = os.getenv('COINBASE_API_KEY')
        api_secret = os.getenv('COINBASE_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError("API credentials not found in environment variables")
        
        logger.info("Creating Coinbase client")
        self.client = CoinbaseClient(api_key=api_key, api_secret=api_secret)

    def get_utc_now(self) -> datetime:
        """Get current UTC time"""
        return datetime.now(timezone.utc)

    async def get_products(self) -> List[Product]:
        """Get all available USD trading pairs"""
        logger.info("Fetching products from Coinbase")
        try:
            products = await self.client.get_products()
            logger.info(f"Successfully fetched products")
            return products
        except Exception as e:
            logger.error(f"Error fetching products: {e}", exc_info=True)
            return []

    async def get_candles(self, product_id: str) -> List[Candle]:
        """Get the last 3 five-minute candles for a given product"""
        logger.debug(f"Fetching candles for {product_id}")
        end_time = self.get_utc_now()
        start_time = end_time - timedelta(minutes=15)
        
        try:
            candles = await self.client.get_candles(
                product_id=product_id,
                start=start_time,
                end=end_time,
                granularity="FIVE_MINUTE"
            )
            logger.debug(f"Successfully fetched candles for {product_id}")
            return candles
        except Exception as e:
            logger.error(f"Error fetching candles for {product_id}: {e}", exc_info=True)
            return []

    async def process_product(self, product: Product) -> Dict[str, Any]:
        """Process a single product"""
        logger.debug(f"Processing product {product.product_id}")
        candles = await self.get_candles(product.product_id)
        
        if candles:
            print(f"\nCollected data for {product.product_id}")
            print("-" * 50)
            for i, candle in enumerate(candles):
                print(f"Candle {i + 1}:")
                print(f"  Time: {candle.datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Open: ${float(candle.open):.2f}")
                print(f"  High: ${float(candle.high):.2f}")
                print(f"  Low: ${float(candle.low):.2f}")
                print(f"  Close: ${float(candle.close):.2f}")
                print(f"  Volume: {float(candle.volume):.8f}")
                print()
        else:
            logger.warning(f"No candles found for {product.product_id}")
        
        return {
            'product_id': product.product_id,
            'candles': [candle.to_dict() for candle in candles],
            'timestamp': self.get_utc_now().isoformat()
        }

    async def collect_data(self) -> List[Dict[str, Any]]:
        """Main data collection routine"""
        current_time = self.get_utc_now()
        logger.info(f"Starting data collection at {current_time.isoformat()}")
        
        try:
            products = await self.get_products()
            logger.info(f"Found {len(products)} USD trading pairs")
            
            if not products:
                logger.error("No products found to process")
                return []
            
            tasks = [self.process_product(product) for product in products]
            results = await asyncio.gather(*tasks)
            
            logger.info("Data collection completed successfully")
            return results
        except Exception as e:
            logger.error(f"Error in collect_data: {e}", exc_info=True)
            return []

async def main() -> None:
    try:
        logger.info("Starting application")
        collector = CryptoDataCollector()
        
        logger.info("Running initial data collection")
        await collector.collect_data()
        
        logger.info("Starting polling cycle (every 5 minutes)")
        while True:
            await asyncio.sleep(300)
            await collector.collect_data()
            
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True) 