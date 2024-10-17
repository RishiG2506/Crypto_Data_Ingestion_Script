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
        
        self.running_metrics_conn = sqlite3.connect('running_metrics.db')
        self.setup_running_metric_db()

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
    
    def setup_running_metric_db(self):
        """Initialise SQLite database for running metrics (updated after every fetch)"""
        cursor = self.running_metrics_conn.cursor()
        
        # Create running metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS running_metrics (
                symbol TEXT PRIMARY KEY,
                latest_price REAL,
                latest_timestamp TEXT,
                daily_high REAL,
                daily_low REAL,
                processing_status TEXT
            )
        ''')

        for symbol in self.symbols:
            cursor.execute('''
                INSERT OR IGNORE INTO running_metrics 
                (symbol, processing_status) 
                VALUES (?, 'initialized')
            ''', (symbol,))
            
        self.running_metrics_conn.commit()
    
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

    
    def downsample_and_store(self):
        """Calculate EOD stats for each symbol and store in Postgres"""
        cursor = self.sqlite_conn.cursor()
        pg_cursor = self.postgres_conn.cursor()
        
        for symbol in self.symbols:
            # Get EOD stats from raw data (sqlite)
            cursor.execute('''
                SELECT 
                    date(timestamp) as date,
                    MIN(price) as low_price,
                    MAX(price) as high_price,
                    AVG(price) as avg_price
                FROM raw_prices 
                WHERE symbol = ?
                GROUP BY date(timestamp)
            ''', (symbol,))
            eod_stats = cursor.fetchone()

            cursor.execute('''
                SELECT 
                    price
                FROM raw_prices 
                WHERE symbol = ?
                ORDER BY timestamp
                LIMIT 1
            ''', (symbol,))
            opening_price = cursor.fetchone()

            cursor.execute('''
                SELECT 
                    price
                FROM raw_prices 
                WHERE symbol = ? 
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (symbol,))
            closing_price = cursor.fetchone()

            print(symbol)
            print(eod_stats)
            print(opening_price)

            
            # Store downsampled_data in PostgreSQL store
            pg_cursor.execute('''
                        INSERT INTO downsampled_prices
                        (date, symbol, open_price, high_price, low_price, close_price, avg_price)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                              ''',
                        (eod_stats[0], symbol, opening_price[0], eod_stats[2], eod_stats[1], closing_price[0], eod_stats[3]))
            
        
        self.postgres_conn.commit()
    
    def update_running_metrics(self, data: List[Dict]):
        """Real-time monitoring of metrics"""
        cursor = self.running_metrics_conn.cursor()
        
        for entry in data:
            symbol = entry['symbol']
            current_price = entry['price']
            current_timestamp = entry['timestamp']
            
            
            # Update all metrics
            cursor.execute('''
                UPDATE running_metrics 
                SET 
                    latest_price = ?,
                    latest_timestamp = ?,
                    daily_high = MAX(?, COALESCE(daily_high,?)),
                    daily_low = MIN(?, COALESCE(daily_low, ?)),
                    processing_status = 'active'
                WHERE symbol = ?
            ''', (
                current_price,
                current_timestamp,
                current_price, current_price,
                current_price, current_price,
                symbol
            ))
        
        self.running_metrics_conn.commit()

    
    def run(self):
        try:
            #2 iterations for testing purposes. To be inside while True:
            for i in range(2):
                # Fetch and store prices
                current_prices = self.fetch_prices()
                self.update_running_metrics(current_prices)
                self.store_raw_data(current_prices)
                
                # Log current prices
                for price_data in current_prices:
                    logger.info(f"{price_data['symbol']}: ${price_data['price']:.2f} at {price_data['timestamp']}")
                

                time.sleep(self.sampling_frequency)
            
            self.downsample_and_store()
                
        except KeyboardInterrupt:
            logger.info("Stopping Data Ingestion...")
        finally:
            self.postgres_conn.close()
            self.sqlite_conn.close()
            self.running_metrics_conn.close()


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
