import requests
import sqlite3
import psycopg2
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class DataCollector:
    def __init__(self):
        self.symbols = ["BTCUSDT", "ETHUSDT", "LTCBTC"]
        self.sampling_frequency = 5 #wait time in seconds
        self.base_url = "https://api.binance.com/api/v3/ticker/price"

        self.running_metrics = {
            symbol: {
                'latest_price': None,
                'high_price': None,
                'low_price': None,
                'open_price': None,
                'latest_timestamp': None,
                'avg_price': 0,
                'sample_count': 0
            } for symbol in self.symbols
        }

        self.postgres_conn = psycopg2.connect(
            dbname="crypto_data",
            user="default",  # Configure the username
            password="",     #Configire the password here
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
                hour INTEGER,
                symbol TEXT,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                avg_price REAL,
                sample_count INTEGER,
                PRIMARY KEY (date, hour, symbol)
            )
        ''')

        self.postgres_conn.commit()
    

    def update_running_metrics(self, symbol, price, timestamp):
        metrics = self.running_metrics[symbol]

        if not metrics['open_price']:
            metrics['open_price'] = price

        metrics['latest_price'] = price
        metrics['high_price'] = max(price, metrics['high_price']) if metrics['high_price'] is not None else price
        metrics['low_price'] = min(price, metrics['low_price']) if metrics['low_price'] is not None else price
        metrics['latest_timestamp'] = timestamp

        total_sum = metrics['avg_price']*metrics['sample_count']
        total_sum += price
        metrics['sample_count'] += 1
        metrics['avg_price'] = total_sum/metrics['sample_count']

    
    def fetch_prices(self) -> List[Dict]:
        """Fetch current prices for all symbols"""
        results = []
        for symbol in self.symbols:
            try:
                response = requests.get(f"{self.base_url}?symbol={symbol}")
                response.raise_for_status()
                data = response.json()
                price = float(data['price'])
                timestamp = datetime.now()

                self.update_running_metrics(symbol, price, timestamp)

                results.append({
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'price': price
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
                (entry['timestamp'].isoformat(), entry['symbol'], entry['price'])
            )
        self.sqlite_conn.commit()
    
    def clear_raw_data(self):
        """Clear raw data from SQLite at EOD"""
        cursor = self.sqlite_conn.cursor()
        cursor.execute("DELETE FROM raw_prices")
        self.sqlite_conn.commit()

        logger.info("Cleared Raw Price Data")

    

    def store_downsampled_data(self, hour_datetime: datetime):
        pg_cursor = self.postgres_conn.cursor()

        for symbol, metrics in self.running_metrics.items():
            if metrics['sample_count'] > 0:

                pg_cursor.execute('''
                    INSERT INTO downsampled_prices 
                    (date, hour, symbol, open_price, high_price, low_price, 
                     close_price, avg_price, sample_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    hour_datetime.date(),
                    hour_datetime.hour,
                    symbol,
                    metrics['open_price'],
                    metrics['high_price'],
                    metrics['low_price'],
                    metrics['latest_price'],
                    metrics['avg_price'],
                    metrics['sample_count']
                ))
        self.postgres_conn.commit()
        logger.info(f"Stored Hourly Metrics for Hour {hour_datetime.hour}")


    def reset_running_metrics(self):
        """Reset running metrics for new hour"""
        for metrics in self.running_metrics.values():
            metrics['open_price'] = None
            metrics['latest_price'] = None
            metrics['high_price'] = None
            metrics['low_price'] = None
            metrics['avg_price'] = 0
            metrics['sample_count'] = 0
            metrics['latest_timestamp'] = None

    
    
    def run(self):
        try:
            current_hour = datetime.now().hour
            while True:

                current_time = datetime.now()
                
                if current_time.hour != current_hour:
                    previous_hour = current_time - timedelta(hours=1)
                    previous_hour = previous_hour.replace(minute=0, second=0, microsecond=0)
                    self.store_downsampled_data(previous_hour)
                    self.reset_running_metrics()
                    current_hour = current_time.hour

                if current_time.hour == 0:
                    self.clear_raw_data()



                # Fetch and store prices
                current_prices = self.fetch_prices()
                self.store_raw_data(current_prices)
                
                # Log current prices
                for price_data in current_prices:
                    logger.info(f"{price_data['symbol']}: ${price_data['price']:.2f} at {price_data['timestamp']}")
                
                
                # # Optional Logging Statements to view the Running Metrics
                # for symbol in self.symbols:
                #         metrics = self.running_metrics[symbol]
                #         logger.info(
                #             f"{symbol} - Hour {current_hour}: "
                #             f"Current: ${metrics['latest_price']:.2f}, "
                #             f"High: ${metrics['high_price']:.2f}, "
                #             f"Low: ${metrics['low_price']:.2f}, "
                #             f"Avg: ${metrics['avg_price']:.2f}, "
                #             f"Samples: {metrics['sample_count']}"
                #         )
                

                time.sleep(self.sampling_frequency)

        except KeyboardInterrupt:
            logger.info("Stopping Data Ingestion...")
        finally:
            self.postgres_conn.close()
            self.sqlite_conn.close()


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
