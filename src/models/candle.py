from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional

@dataclass
class Candle:
    start: str  # Unix timestamp string from API
    high: str
    low: str
    open: str
    close: str
    volume: str

    @classmethod
    def from_response(cls, candle_data: Any) -> Optional['Candle']:
        try:
            if isinstance(candle_data, dict):
                return cls(**candle_data)
            else:
                return cls(
                    start=candle_data.start,
                    high=candle_data.high,
                    low=candle_data.low,
                    open=candle_data.open,
                    close=candle_data.close,
                    volume=candle_data.volume
                )
        except Exception as e:
            return None

    def to_dict(self) -> Dict[str, str]:
        return {
            'start': self.start,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'close': self.close,
            'volume': self.volume
        }

    @property
    def datetime(self) -> datetime:
        """Convert Unix timestamp string to datetime object"""
        return datetime.fromtimestamp(int(self.start)) 