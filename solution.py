import requests
import sqlite3
import psycopg2
from datetime import datetime
import time
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class DataCollector:
    def __init__(self):
        self.symbols = ["BTCUSDT", "ETHUSDT"]
        self.sampling_frequency = 10  # seconds
        self.base_url = "https://api.binance.com/api/v3/ticker/price"
        self.pg_conn = psycopg2.connect(
            dbname="crypto_data",
            user="rg2506",
            password="",
            host="localhost",
            port="5432"
        )
        self.setup_postgres()

    
    def setup_postgres(self):
        cursor = self.pg_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS downsampled_prices (
                date DATE,
                symbol TEXT,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                avg_price REAL
            )
        ''')
        self.pg_conn.commit()
    
    def fetch_prices(self) -> List[Dict]:
        """Fetch current prices for all symbols"""
        results = []
        for symbol in self.symbols:
            try:
                response = requests.get(f"{self.base_url}?symbol={symbol}")
                response.raise_for_status()
                data = response.json()
                results.append({
                    'timestamp': datetime.now(),
                    'symbol': symbol,
                    'price': float(data['price'])
                })
            except Exception as e:
                logger.error(f"Error fetching price for {symbol}: {str(e)}")
        return results

    def run(self):
        try:
            for i in range(2):
                # Fetch and store prices
                current_prices = self.fetch_prices()
                
                # Log current prices
                for price_data in current_prices:
                    logger.info(f"{price_data['symbol']}: ${price_data['price']:.2f}")
                

                time.sleep(self.sampling_frequency)
                
        except KeyboardInterrupt:
            logger.info("Stopping Data Ingestion...")
        finally:
            self.pg_conn.close()


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
