import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from dataCollector import DataCollector

class TestDataCollector(unittest.TestCase):
    def setUp(self):
        # Mock database connections
        self.postgres_mock = patch('psycopg2.connect').start()
        self.sqlite_mock = patch('sqlite3.connect').start()
        
        # Create mock cursors
        self.pg_cursor_mock = Mock()
        self.sqlite_cursor_mock = Mock()
        
        self.postgres_mock.return_value.cursor.return_value = self.pg_cursor_mock
        self.sqlite_mock.return_value.cursor.return_value = self.sqlite_cursor_mock
        
        self.collector = DataCollector()

    def tearDown(self):
        patch.stopall()

    def test_initialization(self):
        """Test if DataCollector initializes with correct default values"""
        self.assertEqual(self.collector.symbols, ["BTCUSDT", "ETHUSDT", "LTCBTC"])
        self.assertEqual(self.collector.sampling_frequency, 5) 
        self.assertEqual(self.collector.base_url, "https://api.binance.com/api/v3/ticker/price")
        
        # Verify running metrics initialization
        for symbol in self.collector.symbols:
            metrics = self.collector.running_metrics[symbol]
            self.assertIsNone(metrics['latest_price'])
            self.assertIsNone(metrics['high_price'])
            self.assertIsNone(metrics['low_price'])
            self.assertIsNone(metrics['open_price'])
            self.assertIsNone(metrics['latest_timestamp'])
            self.assertEqual(metrics['avg_price'], 0)
            self.assertEqual(metrics['sample_count'], 0)

    def test_database_connections(self):
        """Test database connection parameters"""
        self.postgres_mock.assert_called_once_with(
            dbname="crypto_data",
            user="default",
            password="",
            host="localhost",
            port="5432"
        )
        self.sqlite_mock.assert_called_once_with('raw_data.db')

    def test_setup_sqlite_db(self):
        """Test SQLite database setup"""
        expected_query = '''
            CREATE TABLE IF NOT EXISTS raw_prices (
                timestamp TEXT,
                symbol TEXT,
                price REAL
            )
        '''
        self.sqlite_cursor_mock.execute.assert_called_with(expected_query)
        self.sqlite_mock.return_value.commit.assert_called()


    def test_setup_postgres(self):
        """Test PostgreSQL database setup"""
        expected_query = '''
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
        '''
        self.pg_cursor_mock.execute.assert_called_with(expected_query)
        self.postgres_mock.return_value.commit.assert_called()


    def test_update_running_metrics_after_first_update(self):
        """Test first update of running metrics for a symbol"""
        symbol = "BTCUSDT"
        price = 50000.00
        timestamp = datetime.now()
        
        self.collector.update_running_metrics(symbol, price, timestamp)
        metrics = self.collector.running_metrics[symbol]
        
        self.assertEqual(metrics['open_price'], price)
        self.assertEqual(metrics['latest_price'], price)
        self.assertEqual(metrics['high_price'], price)
        self.assertEqual(metrics['low_price'], price)
        self.assertEqual(metrics['latest_timestamp'], timestamp)
        self.assertEqual(metrics['avg_price'], price)
        self.assertEqual(metrics['sample_count'], 1)


    def test_update_running_metrics_after_3_updates(self):
        """Test multiple updates of running metrics"""
        symbol = "BTCUSDT"
        prices = [50000.00, 51000.00, 49000.00]
        timestamp = datetime.now()
        
        for price in prices:
            self.collector.update_running_metrics(symbol, price, timestamp)
        
        metrics = self.collector.running_metrics[symbol]
        self.assertEqual(metrics['open_price'], prices[0])
        self.assertEqual(metrics['latest_price'], prices[-1])
        self.assertEqual(metrics['high_price'], 51000.00)
        self.assertEqual(metrics['low_price'], 49000.00)
        self.assertEqual(metrics['avg_price'], sum(prices)/len(prices))

    @patch('requests.get')
    def test_fetch_prices_success(self, mock_get):
        """Test successful price fetching"""
        mock_responses = {
            "BTCUSDT": {"price": "50000.00"},
            "ETHUSDT": {"price": "3000.00"}
        }
        
        def mock_get_side_effect(url):
            symbol = url.split('=')[1]
            mock_response = Mock()
            mock_response.json.return_value = mock_responses[symbol]
            mock_response.raise_for_status.return_value = None
            return mock_response
        
        mock_get.side_effect = mock_get_side_effect
        
        results = self.collector.fetch_prices()
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['symbol'], "BTCUSDT")
        self.assertEqual(results[0]['price'], 50000.00)
        self.assertEqual(results[1]['symbol'], "ETHUSDT")
        self.assertEqual(results[1]['price'], 3000.00)

    @patch('requests.get')
    def test_fetch_prices_api_error(self, mock_get):
        """Test error handling in price fetching"""
        mock_get.side_effect = Exception("API Error")
        results = self.collector.fetch_prices()
        self.assertEqual(len(results), 0)


    def test_clear_raw_data(self):
        """Test clearing raw data from SQLite"""
        self.collector.clear_raw_data()
        self.sqlite_cursor_mock.execute.assert_called_with("DELETE FROM raw_prices")
        self.sqlite_mock.return_value.commit.assert_called()

    def test_store_downsampled_data(self):
        """Test storing downsampled data in PostgreSQL"""
        test_datetime = datetime.now()
        test_metrics = {
            'open_price': 49000.00,
            'high_price': 51000.00,
            'low_price': 48000.00,
            'latest_price': 50000.00,
            'avg_price': 49500.00,
            'sample_count': 10
        }
        
        self.collector.running_metrics['BTCUSDT'].update(test_metrics)
        self.collector.store_downsampled_data(test_datetime)
        
        self.pg_cursor_mock.execute.assert_called()
        self.postgres_mock.return_value.commit.assert_called()


    def test_reset_running_metrics(self):
        """Test resetting of running metrics"""

        self.collector.running_metrics['BTCUSDT'].update({
            'latest_price': 50000.00,
            'high_price': 51000.00,
            'low_price': 49000.00,
            'open_price': 49500.00,
            'avg_price': 50000.00,
            'sample_count': 10
        })
        
        self.collector.reset_running_metrics()
        
        for symbol in self.collector.symbols:
            metrics = self.collector.running_metrics[symbol]
            self.assertIsNone(metrics['latest_price'])
            self.assertIsNone(metrics['high_price'])
            self.assertIsNone(metrics['low_price'])
            self.assertIsNone(metrics['open_price'])
            self.assertEqual(metrics['avg_price'], 0)
            self.assertEqual(metrics['sample_count'], 0)

    def test_run_cleanup(self):
        with patch.object(self.collector, 'fetch_prices', side_effect=KeyboardInterrupt):
            self.collector.run()
            
        self.postgres_mock.return_value.close.assert_called()
        self.sqlite_mock.return_value.close.assert_called()

if __name__ == '__main__':
    unittest.main()