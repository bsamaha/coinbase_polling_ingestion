from typing import List, Optional
from datetime import datetime
import logging
import asyncio
import time
from coinbase.rest import RESTClient
from src.models.product import Product
from src.models.candle import Candle

logger = logging.getLogger(__name__)

class CoinbaseClient:
    def __init__(self, api_key: str, api_secret: str):
        self.client = RESTClient(api_key=api_key, api_secret=api_secret)
        
        # Public endpoint rate limiting (10 RPS)
        self.public_semaphore = asyncio.Semaphore(10)
        self.public_request_times = []
        
        # Private endpoint rate limiting (30 RPS)
        self.private_semaphore = asyncio.Semaphore(30)
        self.private_request_times = []

    async def _rate_limited_request(self, func, *args, is_public=True, **kwargs):
        """Execute a rate-limited API request"""
        semaphore = self.public_semaphore if is_public else self.private_semaphore
        request_times = self.public_request_times if is_public else self.private_request_times
        max_rps = 10 if is_public else 30

        async with semaphore:
            # Clean up old request times
            current_time = time.time()
            request_times = [t for t in request_times if current_time - t < 1.0]
            
            # If we've hit the rate limit, wait until we can make another request
            if len(request_times) >= max_rps:
                wait_time = 1.0 - (current_time - request_times[0])
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
            
            try:
                result = func(*args, **kwargs)
                request_times.append(time.time())
                
                # Update the request times list
                if is_public:
                    self.public_request_times = request_times
                else:
                    self.private_request_times = request_times
                    
                return result
                
            except Exception as e:
                if "Too many errors" in str(e) or "429" in str(e):
                    logger.warning(f"Rate limit hit, backing off for 2 seconds...")
                    await asyncio.sleep(2)  # Longer backoff for rate limit errors
                    return await self._rate_limited_request(func, *args, is_public=is_public, **kwargs)
                raise

    async def get_products(self) -> List[Product]:
        """Get all available trading pairs (public endpoint)"""
        try:
            response = await self._rate_limited_request(
                self.client.get_products,
                is_public=True
            )
            
            if not hasattr(response, 'products'):
                logger.error("Invalid response format - missing products")
                return []
                
            products = []
            for product_data in response.products:
                product = Product.from_response(product_data)
                if product and product.status == 'online':  # Only include active products
                    products.append(product)
            
            logger.info(f"Found {len(products)} active trading pairs")
            return products
            
        except Exception as e:
            logger.error(f"Error fetching products: {e}", exc_info=True)
            return []

    async def get_candles(
        self, 
        product_id: str, 
        start: datetime, 
        end: datetime, 
        granularity: str = "FIVE_MINUTE"
    ) -> List[Candle]:
        """Get candles for a given product (public endpoint)"""
        try:
            # Convert granularity to the format expected by the API
            granularity_map = {
                "ONE_MINUTE": "ONE_MINUTE",
                "FIVE_MINUTE": "FIVE_MINUTE",
                "FIFTEEN_MINUTE": "FIFTEEN_MINUTE",
                "THIRTY_MINUTE": "THIRTY_MINUTE",
                "ONE_HOUR": "ONE_HOUR",
                "TWO_HOUR": "TWO_HOUR",
                "SIX_HOUR": "SIX_HOUR",
                "ONE_DAY": "ONE_DAY"
            }

            api_granularity = granularity_map.get(granularity.upper(), "FIVE_MINUTE")

            # Convert datetime objects to Unix timestamps
            start_timestamp = int(start.timestamp())
            end_timestamp = int(end.timestamp())

            response = await self._rate_limited_request(
                self.client.get_public_candles,
                product_id=product_id,
                start=str(start_timestamp),  # API expects string
                end=str(end_timestamp),      # API expects string
                granularity=api_granularity,
                is_public=True
            )
            
            if not hasattr(response, 'candles'):
                logger.error(f"Invalid candle response format for {product_id}")
                return []

            candles = []
            for candle_data in response.candles:
                candle = Candle.from_response(candle_data)
                if candle:
                    candles.append(candle)

            # Sort candles by time and return the last 3
            sorted_candles = sorted(candles, key=lambda x: x.datetime)
            return sorted_candles[-3:] if sorted_candles else []
            
        except Exception as e:
            logger.error(f"Error fetching candles for {product_id}: {e}", exc_info=True)
            return [] 