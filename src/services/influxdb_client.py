from typing import List
import logging
from influxdb_client import InfluxDBClient # type: ignore
from influxdb_client.client.write_api import WriteOptions, WriteType
from influxdb_client.client.write.point import Point
from src.models.candle import Candle

logger = logging.getLogger(__name__)

class InfluxDBClientWrapper:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        write_options = WriteOptions(
            write_type=WriteType.batching,
            batch_size=500,
            flush_interval=10_000
        )
        self.write_api = self.client.write_api(write_options=write_options)
        self.org = org
        self.bucket = bucket
        logger.info("Initialized InfluxDB client with batching write options")

    def write_candles(self, product_id: str, candles: List[Candle]) -> None:
        """
        Write a list of Candle objects to InfluxDB.
        Each candle is mapped to a point in the 'candles' measurement with the product_id as a tag.
        """
        points = []
        for candle in candles:
            try:
                point = (
                    Point("candles")
                    .tag("product_id", product_id)
                    .field("open", float(candle.open))
                    .field("high", float(candle.high))
                    .field("low", float(candle.low))
                    .field("close", float(candle.close))
                    .field("volume", float(candle.volume))
                    .time(candle.datetime)
                )
                points.append(point)
            except Exception as e:
                logger.error(f"Error creating point for candle: {e}", exc_info=True)
                
        if points:
            try:
                self.write_api.write(bucket=self.bucket, record=points)
                logger.info(f"Wrote {len(points)} candles for product {product_id} to InfluxDB")
            except Exception as e:
                logger.error(f"Error writing points to InfluxDB: {e}", exc_info=True)