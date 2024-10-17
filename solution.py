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
        self.sampling_frequency = 2 #wait time in seconds
        self.base_url = "https://api.binance.com/api/v3/ticker/price"
        self.postgres_conn = psycopg2.connect(
            dbname="crypto_data",
            user="rg2506",
            password="",
            host="localhost",
            port="5432"
        )
        self.setup_postgres()

        self.sqlite_conn = sqlite3.connect('raw_data.db')
        self.setup_sqlite_db()

    def setup_sqlite_db(self):
        """Initialize SQLite database for raw data"""
        cursor = self.sqlite_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_prices (
                timestamp TEXT,
                symbol TEXT,
                price REAL
            )
        ''')
        self.sqlite_conn.commit()
    
    def setup_postgres(self):
        cursor = self.postgres_conn.cursor()
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
        self.postgres_conn.commit()
    
    def fetch_prices(self) -> List[Dict]:
        """Fetch current prices for all symbols"""
        results = []
        for symbol in self.symbols:
            try:
                response = requests.get(f"{self.base_url}?symbol={symbol}")
                response.raise_for_status()
                data = response.json()
                results.append({
                    'timestamp': datetime.now().isoformat(),
                    'symbol': symbol,
                    'price': float(data['price'])
                })
            except Exception as e:
                logger.error(f"Error fetching price for {symbol}: {str(e)}")
        return results
    
    def store_raw_data(self, raw_data: List[Dict]):
        """Store raw cryto data in SQLite"""
        cursor = self.sqlite_conn.cursor()
        for entry in raw_data:
            cursor.execute(
                "INSERT INTO raw_prices (timestamp, symbol, price) VALUES (?, ?, ?)",
                (entry['timestamp'], entry['symbol'], entry['price'])
            )
        self.sqlite_conn.commit()

    
    def run(self):
        try:
            for i in range(2):
                # Fetch and store prices
                current_prices = self.fetch_prices()
                self.store_raw_data(current_prices)
                
                # Log current prices
                for price_data in current_prices:
                    logger.info(f"{price_data['symbol']}: ${price_data['price']:.2f} at {price_data['timestamp']}")
                

                time.sleep(self.sampling_frequency)
                
        except KeyboardInterrupt:
            logger.info("Stopping Data Ingestion...")
        finally:
            self.postgres_conn.close()
            self.sqlite_conn.close()


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
