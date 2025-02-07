import os
import asyncio
import logging
import requests  # Ensure this module is installed in your docker image
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
        
        # Initialize InfluxDB client
        influxdb_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        influxdb_token = os.getenv('INFLUXDB_TOKEN')
        influxdb_org = os.getenv('INFLUXDB_ORG', 'my-org')
        influxdb_bucket = os.getenv('INFLUXDB_BUCKET', 'crypto_data')
        
        if not influxdb_token:
            raise ValueError("InfluxDB token not found in environment variables")
        
        from src.services.influxdb_client import InfluxDBClientWrapper
        self.influxdb_client = InfluxDBClientWrapper(
            url=influxdb_url,
            token=influxdb_token,
            org=influxdb_org,
            bucket=influxdb_bucket
        )

    def get_utc_now(self) -> datetime:
        """Get current UTC time"""
        return datetime.now(timezone.utc)

    async def get_products(self) -> List[Product]:
        """Get all available USD trading pairs"""
        logger.info("Fetching products from Coinbase")
        try:
            products = await self.client.get_products()
            logger.info("Successfully fetched products")
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
        """Process a single product by fetching candle data and writing it to InfluxDB."""
        logger.debug(f"Processing product {product.product_id}")
        candles = await self.get_candles(product.product_id)
        
        if candles:
            logger.info(f"Collected {len(candles)} candles for {product.product_id}")
            try:
                # Write candle data to InfluxDB
                self.influxdb_client.write_candles(product.product_id, candles)
                logger.info(f"Successfully wrote candles for {product.product_id} to InfluxDB")
            except Exception as e:
                logger.error(f"Error writing candles for {product.product_id} to InfluxDB: {e}", exc_info=True)
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

async def wait_for_influxdb(url: str, retry_interval: float = 5):
    """Wait for InfluxDB to be fully initialized and healthy before proceeding."""
    while True:
        try:
            # Attempt to fetch the health endpoint in a thread (since requests is blocking)
            response = await asyncio.to_thread(requests.get, f"{url}/health", timeout=3)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "pass":
                    logger.info("InfluxDB is healthy!")
                    break
                else:
                    logger.warning(f"InfluxDB health check returned non-pass status: {data}")
            else:
                logger.warning(f"InfluxDB health endpoint returned status: {response.status_code}")
        except Exception as e:
            logger.warning(f"InfluxDB not ready yet: {e}")
        logger.info("Waiting for InfluxDB to be ready...")
        await asyncio.sleep(retry_interval)

async def main() -> None:
    try:
        logger.info("Starting application")
        
        # Read InfluxDB URL from environment variables
        influxdb_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        logger.info("Waiting for InfluxDB to initialize...")
        await wait_for_influxdb(influxdb_url)
        
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