from dataclasses import dataclass
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

@dataclass
class Product:
    product_id: str
    base_name: str
    quote_name: str
    status: str
    price: Optional[str] = None
    volume_24h: Optional[str] = None

    @classmethod
    def from_response(cls, product_data: Any) -> Optional['Product']:
        """Create a Product instance from API response"""
        try:
            # Handle both object and dictionary access
            if isinstance(product_data, dict):
                return cls(
                    product_id=product_data['product_id'],
                    base_name=product_data['base_name'],
                    quote_name=product_data['quote_name'],
                    status=product_data['status'],
                    price=product_data.get('price'),
                    volume_24h=product_data.get('volume_24h')
                )
            else:
                return cls(
                    product_id=product_data.product_id,
                    base_name=product_data.base_name,
                    quote_name=product_data.quote_name,
                    status=product_data.status,
                    price=getattr(product_data, 'price', None),
                    volume_24h=getattr(product_data, 'volume_24h', None)
                )
        except Exception as e:
            logger.error(f"Error creating Product from response: {e}")
            return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'product_id': self.product_id,
            'base_name': self.base_name,
            'quote_name': self.quote_name,
            'status': self.status,
            'price': self.price,
            'volume_24h': self.volume_24h
        } 