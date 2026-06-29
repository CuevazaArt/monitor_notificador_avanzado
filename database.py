import sqlite3
import os
import logging

DB_PATH = os.getenv("DB_PATH", "monitor.db")

class Database:
    def __init__(self):
        self.db_path = DB_PATH
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabla de Alertas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    article_code TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    is_delisting INTEGER NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source, article_code)
                )
            """)
            
            # Tabla de logs del sistema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla de métricas de APIs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    latency_ms REAL,
                    status_code INTEGER,
                    error_message TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ---- TABLAS DE LA CÁMARA DE COMPENSACIÓN (TRADING SIMULADO) ----
            
            # Tabla de balances
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS simulated_balance (
                    asset TEXT PRIMARY KEY,
                    amount REAL NOT NULL DEFAULT 0.0
                )
            """)

            # Tabla de operaciones (Trades)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS simulated_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    strategy_type TEXT NOT NULL, -- 'SHORT_ARB' o 'MID_TERM'
                    status TEXT NOT NULL,        -- 'OPEN' o 'CLOSED'
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    quantity REAL NOT NULL,
                    profit_loss_usd REAL DEFAULT 0.0,
                    timestamp_entry DATETIME DEFAULT CURRENT_TIMESTAMP,
                    timestamp_exit DATETIME,
                    reason TEXT
                )
            """)
            conn.commit()

    def log_alert(self, source, article_code, title, url, is_delisting):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO alerts (source, article_code, title, url, is_delisting)
                    VALUES (?, ?, ?, ?, ?)
                """, (source, article_code, title, url, int(is_delisting)))
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logging.error(f"Error al guardar alerta en la base de datos: {e}")
            self.log_system("ERROR", f"Error al guardar alerta: {e}")
            return False

    def log_system(self, level, message):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO system_logs (level, message)
                    VALUES (?, ?)
                """, (level, message))
                conn.commit()
        except Exception as e:
            logging.error(f"Error al escribir en system_logs: {e}")

    def log_api_metric(self, source, latency_ms, status_code, error_message=None):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO api_metrics (source, latency_ms, status_code, error_message)
                    VALUES (?, ?, ?, ?)
                """, (source, latency_ms, status_code, error_message))
                conn.commit()
        except Exception as e:
            logging.error(f"Error al guardar métrica de API: {e}")

    def get_metrics_summary(self, hours=12):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT source, AVG(latency_ms) as avg_latency, 
                           SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count,
                           COUNT(*) as total_requests
                    FROM api_metrics
                    WHERE timestamp >= datetime('now', ?)
                    GROUP BY source
                """, (f"-{hours} hours",))
                
                rows = cursor.fetchall()
                summary = {}
                for row in rows:
                    summary[row["source"]] = {
                        "avg_latency": round(row["avg_latency"] or 0, 2),
                        "errors": row["error_count"],
                        "total": row["total_requests"]
                    }
                return summary
        except Exception as e:
            logging.error(f"Error al obtener resumen de métricas: {e}")
            return {}

    def get_total_alerts_count(self, hours=12):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) as alert_count
                    FROM alerts
                    WHERE timestamp >= datetime('now', ?)
                """, (f"-{hours} hours",))
                row = cursor.fetchone()
                return row["alert_count"] if row else 0
        except Exception as e:
            logging.error(f"Error al obtener total de alertas: {e}")
            return 0

    # ---- MÉTODOS DE LA CÁMARA DE COMPENSACIÓN ----

    def init_portfolio_balance(self, initial_usd=1000.0):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Insertar USD si no existe
                cursor.execute("""
                    INSERT OR IGNORE INTO simulated_balance (asset, amount)
                    VALUES ('USD', ?)
                """, (initial_usd,))
                conn.commit()
        except Exception as e:
            logging.error(f"Error al inicializar balance: {e}")

    def get_balance(self, asset):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT amount FROM simulated_balance WHERE asset = ?", (asset,))
                row = cursor.fetchone()
                return row["amount"] if row else 0.0
        except Exception as e:
            logging.error(f"Error al obtener balance de {asset}: {e}")
            return 0.0

    def update_balance(self, asset, amount):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO simulated_balance (asset, amount)
                    VALUES (?, ?)
                    ON CONFLICT(asset) DO UPDATE SET amount = ?
                """, (asset, amount, amount))
                conn.commit()
        except Exception as e:
            logging.error(f"Error al actualizar balance de {asset}: {e}")

    def open_simulated_trade(self, ticker, strategy_type, entry_price, quantity, reason=""):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO simulated_trades (ticker, strategy_type, status, entry_price, quantity, reason)
                    VALUES (?, ?, 'OPEN', ?, ?, ?)
                """, (ticker, strategy_type, entry_price, quantity, reason))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logging.error(f"Error al registrar apertura de trade: {e}")
            return None

    def close_simulated_trade(self, trade_id, exit_price, profit_loss_usd, reason=""):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE simulated_trades
                    SET status = 'CLOSED', exit_price = ?, profit_loss_usd = ?, 
                        timestamp_exit = CURRENT_TIMESTAMP, reason = ?
                    WHERE id = ?
                """, (exit_price, profit_loss_usd, reason, trade_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Error al registrar cierre de trade: {e}")

    def get_open_trades(self):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM simulated_trades WHERE status = 'OPEN'")
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error al obtener trades abiertos: {e}")
            return []

    def get_trading_summary(self):
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) as closed_trades,
                        SUM(profit_loss_usd) as total_profit_loss,
                        SUM(CASE WHEN profit_loss_usd > 0 THEN 1 ELSE 0 END) as winning_trades
                    FROM simulated_trades
                """)
                row = cursor.fetchone()
                
                if not row or row["total_trades"] == 0:
                    return {"total_trades": 0, "profit_loss": 0.0, "win_rate": 0.0}
                
                closed = row["closed_trades"] or 1
                win_rate = (row["winning_trades"] / closed) * 100 if closed > 0 else 0.0
                
                return {
                    "total_trades": row["total_trades"],
                    "closed_trades": row["closed_trades"],
                    "profit_loss": round(row["total_profit_loss"] or 0.0, 2),
                    "win_rate": round(win_rate, 1)
                }
        except Exception as e:
            logging.error(f"Error al obtener resumen de trading: {e}")
            return {"total_trades": 0, "profit_loss": 0.0, "win_rate": 0.0}
