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
                return True  # Alerta nueva guardada con éxito
        except sqlite3.IntegrityError:
            return False  # Alerta duplicada, ya existía en la base de datos
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
                
                # Obtener latencia promedio por fuente en las últimas X horas
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
