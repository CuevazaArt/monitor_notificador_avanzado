import unittest
import os
import sqlite3
from database import Database
from monitor import ListingMonitor

TEST_DB_PATH = "test_monitor.db"

class TestMonitorAndDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DB_PATH"] = TEST_DB_PATH
        cls.db = Database()
        cls.monitor = ListingMonitor()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM alerts")
            conn.execute("DELETE FROM simulated_balance")
            conn.execute("DELETE FROM simulated_trades")
            conn.execute("DELETE FROM performance_critiques")
            conn.execute("DELETE FROM adaptive_parameters")
            conn.commit()

    def test_extract_ticker(self):
        title1 = "Binance Futures Will Launch USD-Margined OUSDT Perpetual Contract (2026-06-24)"
        self.assertEqual(self.monitor.extract_ticker(title1), "OUSDT")

        title2 = "OKX to list RESOLVUSD, BICOUSD, TRUMPUSD and LITUSD X-Perps"
        self.assertEqual(self.monitor.extract_ticker(title2), "BICOUSD")

        title3 = "OKX will launch CARDS/USDT for spot trading"
        self.assertEqual(self.monitor.extract_ticker(title3), "CARDS")

    def test_database_alerts(self):
        is_new = self.db.log_alert("Binance", "12345", "Test Listing", "http://test.com", False)
        self.assertTrue(is_new)

        is_duplicate = self.db.log_alert("Binance", "12345", "Test Listing", "http://test.com", False)
        self.assertFalse(is_duplicate)

        self.assertEqual(self.db.get_total_alerts_count(hours=1), 1)

    def test_simulated_trading_balance(self):
        self.db.init_portfolio_balance(1000.0)
        self.assertEqual(self.db.get_balance("USD"), 1000.0)

        self.db.update_balance("USD", 900.0)
        self.db.update_balance("BTC", 0.05)
        
        self.assertEqual(self.db.get_balance("USD"), 900.0)
        self.assertEqual(self.db.get_balance("BTC"), 0.05)

    def test_open_close_trades(self):
        trade_id = self.db.open_simulated_trade("ETH", "SHORT_ARB", 3000.0, 0.1, "Test Entry")
        self.assertIsNotNone(trade_id)

        open_trades = self.db.get_open_trades()
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(open_trades[0]["ticker"], "ETH")

        self.db.close_simulated_trade(trade_id, 3100.0, 10.0, "Test Exit Time")
        
        open_trades_after = self.db.get_open_trades()
        self.assertEqual(len(open_trades_after), 0)

        summary = self.db.get_trading_summary()
        self.assertEqual(summary["total_trades"], 1)
        self.assertEqual(summary["profit_loss"], 10.0)

    def test_adaptive_learning_and_critique(self):
        # Probar parámetros adaptativos
        self.db.update_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", "280.0")
        val = self.db.get_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", "300")
        self.assertEqual(val, "280.0")

        # Probar registro de autocrítica
        trade_id = self.db.open_simulated_trade("SOL", "MID_TERM", 150.0, 1.0, "Entrada de prueba")
        self.db.close_simulated_trade(trade_id, 140.0, -10.0, "Salida con pérdidas")
        
        self.db.log_performance_critique(
            trade_id, "SOL", -10.0, 
            "La volatilidad del mercado causó pérdidas", 
            "Reducir MID_TERM_HOLD_SECONDS"
        )
        
        # Validar inserción leyendo directamente con una consulta simple
        with self.db.get_connection() as conn:
            row = conn.execute("SELECT * FROM performance_critiques WHERE ticker = 'SOL'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["profit_loss_usd"], -10.0)
            self.assertEqual(row["adaptive_action"], "Reducir MID_TERM_HOLD_SECONDS")

    def test_blacklist_and_highest_price(self):
        # 1. Probar registro de precio más alto observado (highest_price)
        trade_id = self.db.open_simulated_trade("LUNA", "SHORT_ARB", 1.0, 100.0, "Prueba stop")
        self.assertIsNotNone(trade_id)
        
        # Verificar precio inicial
        open_trades = self.db.get_open_trades()
        self.assertEqual(open_trades[0]["highest_price"], 1.0)
        
        # Actualizar highest_price
        self.db.update_highest_price(trade_id, 1.5)
        open_trades_after = self.db.get_open_trades()
        self.assertEqual(open_trades_after[0]["highest_price"], 1.5)

        # 2. Probar blacklist
        addr = "0xdeadbeef1234567890abcdef"
        self.assertFalse(self.db.is_blacklisted(addr))
        
        self.db.blacklist_contract(addr, "SCAM", "Tax token / Honeypot")
        self.assertTrue(self.db.is_blacklisted(addr))

if __name__ == "__main__":
    unittest.main()
