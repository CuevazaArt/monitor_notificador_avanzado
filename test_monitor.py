import unittest
import os
import sqlite3
from database import Database
from monitor import ListingMonitor

# Usar una base de datos temporal de pruebas
TEST_DB_PATH = "test_monitor.db"

class TestMonitorAndDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DB_PATH"] = TEST_DB_PATH
        cls.db = Database()
        cls.monitor = ListingMonitor()

    @classmethod
    def tearDownClass(cls):
        # Limpiar la base de datos de pruebas al terminar
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def setUp(self):
        # Limpiar tablas antes de cada prueba
        with self.db.get_connection() as conn:
            conn.execute("DELETE FROM alerts")
            conn.execute("DELETE FROM simulated_balance")
            conn.execute("DELETE FROM simulated_trades")
            conn.commit()

    def test_extract_ticker(self):
        # Caso 1: Paréntesis
        title1 = "Binance Futures Will Launch USD-Margined OUSDT Perpetual Contract (2026-06-24)"
        self.assertEqual(self.monitor.extract_ticker(title1), "OUSDT")

        # Caso 2: Par de trading con /USD
        title2 = "OKX to list RESOLVUSD, BICOUSD, TRUMPUSD and LITUSD X-Perps"
        # BICOUSD debería coincidir debido al patrón de palabras en mayúsculas
        self.assertEqual(self.monitor.extract_ticker(title2), "BICOUSD")

        # Caso 3: Palabras mayúsculas comunes ignoradas y ticker limpio
        title3 = "OKX will launch CARDS/USDT for spot trading"
        self.assertEqual(self.monitor.extract_ticker(title3), "CARDS")

    def test_database_alerts(self):
        # Registrar alerta única
        is_new = self.db.log_alert("Binance", "12345", "Test Listing", "http://test.com", False)
        self.assertTrue(is_new)

        # Intentar registrar duplicado
        is_duplicate = self.db.log_alert("Binance", "12345", "Test Listing", "http://test.com", False)
        self.assertFalse(is_duplicate)

        # Verificar contador en base de datos
        self.assertEqual(self.db.get_total_alerts_count(hours=1), 1)

    def test_simulated_trading_balance(self):
        # Inicializar balance
        self.db.init_portfolio_balance(1000.0)
        self.assertEqual(self.db.get_balance("USD"), 1000.0)

        # Actualizar balances
        self.db.update_balance("USD", 900.0)
        self.db.update_balance("BTC", 0.05)
        
        self.assertEqual(self.db.get_balance("USD"), 900.0)
        self.assertEqual(self.db.get_balance("BTC"), 0.05)

    def test_open_close_trades(self):
        # Abrir orden simulada
        trade_id = self.db.open_simulated_trade("ETH", "SHORT_ARB", 3000.0, 0.1, "Test Entry")
        self.assertIsNotNone(trade_id)

        # Verificar que el trade está abierto
        open_trades = self.db.get_open_trades()
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(open_trades[0]["ticker"], "ETH")
        self.assertEqual(open_trades[0]["status"], "OPEN")

        # Cerrar el trade con profit
        self.db.close_simulated_trade(trade_id, 3100.0, 10.0, "Test Exit Time")
        
        # Verificar que ya no hay trades abiertos y se actualizó el resumen
        open_trades_after = self.db.get_open_trades()
        self.assertEqual(len(open_trades_after), 0)

        summary = self.db.get_trading_summary()
        self.assertEqual(summary["total_trades"], 1)
        self.assertEqual(summary["closed_trades"], 1)
        self.assertEqual(summary["profit_loss"], 10.0)
        self.assertEqual(summary["win_rate"], 100.0)

if __name__ == "__main__":
    unittest.main()
