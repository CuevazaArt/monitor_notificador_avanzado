import os
import time
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import Database

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("monitor.log", encoding="utf-8")
    ]
)

# Cargar variables de entorno
load_dotenv()

POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 5))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HEARTBEAT_INTERVAL_HOURS = float(os.getenv("HEARTBEAT_INTERVAL_HOURS", 12))

# APIs de Anuncios
BINANCE_API = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BYBIT_API = "https://api.bybit.com/v5/announcements/index"
OKX_API = "https://www.okx.com/api/v5/support/announcements"

# Encabezados HTTP comunes para evitar bloqueos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "clienttype": "web",
    "lang": "en"
}

class ListingMonitor:
    def __init__(self):
        self.db = Database()
        self.start_time = time.time()
        self.last_heartbeat_time = time.time()

    def get_uptime_str(self):
        uptime_seconds = int(time.time() - self.start_time)
        days = uptime_seconds // 86400
        hours = (uptime_seconds % 86400) // 3600
        minutes = (uptime_seconds % 3600) // 60
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        parts.append(f"{minutes}m")
        return " ".join(parts)

    def send_notification(self, title, url, source, is_delisting=False):
        emoji = "⚠️ [DESLISTADO]" if is_delisting else "🚀 [LISTADO]"
        message = (
            f"{emoji} **Nuevo Anuncio Detectado en {source}**\n\n"
            f"**Título:** {title}\n"
            f"**Enlace:** {url}"
        )
        self._dispatch_message(message)

    def send_heartbeat_report(self):
        logging.info("Compilando y enviando reporte de control (heartbeat) a Telegram...")
        
        # Obtener métricas de la base de datos
        summary = self.db.get_metrics_summary(hours=HEARTBEAT_INTERVAL_HOURS)
        total_alerts = self.db.get_total_alerts_count(hours=HEARTBEAT_INTERVAL_HOURS)
        uptime = self.get_uptime_str()
        
        # Determinar estado de salud
        system_status = "🟢 OK"
        failed_sources = []
        for src, metrics in summary.items():
            success_rate = ((metrics["total"] - metrics["errors"]) / metrics["total"]) * 100 if metrics["total"] > 0 else 0
            if success_rate < 80:
                system_status = "🟡 WARNING"
                failed_sources.append(f"{src} ({success_rate:.1f}% OK)")
        
        metrics_details = ""
        for src, metrics in summary.items():
            success_rate = ((metrics["total"] - metrics["errors"]) / metrics["total"]) * 100 if metrics["total"] > 0 else 0
            metrics_details += (
                f"• *{src}*:\n"
                f"  - Latencia Promedio: {metrics['avg_latency']} ms\n"
                f"  - Éxito Peticiones: {success_rate:.1f}% ({metrics['total'] - metrics['errors']}/{metrics['total']})\n"
            )
            
        status_msg = f" (Problemas con: {', '.join(failed_sources)})" if failed_sources else ""
        
        message = (
            f"🟢 *[CONTROL] Reporte de Salud del Monitor*\n\n"
            f"• *Estado General del Sistema:* {system_status}{status_msg}\n"
            f"• *Tiempo de Actividad (Uptime):* {uptime}\n"
            f"• *Alertas Detectadas (Últimas {HEARTBEAT_INTERVAL_HOURS}h):* {total_alerts}\n\n"
            f"*Métricas de Servicios Adyacentes (APIs)*:\n{metrics_details or 'Sin datos en este ciclo.'}\n"
            f"El monitor sigue escuchando 24/7 de forma perpetua."
        )
        
        self._dispatch_message(message, is_heartbeat=True)

    def _dispatch_message(self, message, is_heartbeat=False):
        # Enviar a Discord (Solo alertas reales)
        if DISCORD_WEBHOOK_URL and not is_heartbeat:
            try:
                requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
                logging.info("Notificación enviada a Discord.")
            except Exception as e:
                logging.error(f"Error al enviar notificación a Discord: {e}")
                self.db.log_system("ERROR", f"Fallo al notificar Discord: {e}")
                
        # Enviar a Telegram (Alertas y Heartbeats)
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message.replace("**", "*"), # Convertir formato negrita si se usa Markdown
                    "parse_mode": "Markdown"
                }
                requests.post(telegram_url, json=payload, timeout=5)
                logging.info("Notificación enviada a Telegram.")
            except Exception as e:
                logging.error(f"Error al enviar notificación a Telegram: {e}")
                self.db.log_system("ERROR", f"Fallo al notificar Telegram: {e}")

    def parse_and_check(self, title, code, url, source):
        if not title or not code:
            return
            
        title_lower = title.lower()
        # Palabras clave de listado/deslistado
        keywords_list = ["will list", "lists", "introduce", "launch", "listará", "presenta"]
        keywords_delist = ["will delist", "delisting", "deslistará", "removerá", "delist"]
        
        is_delisting = any(kw in title_lower for kw in keywords_delist)
        is_listing = any(kw in title_lower for kw in keywords_list)
        
        if is_listing or is_delisting:
            # Intentar registrar en base de datos.
            # Solo enviamos notificación si el registro es exitoso (nueva alerta única)
            is_new = self.db.log_alert(source, code, title, url, is_delisting)
            if is_new:
                logging.warning(f"¡NUEVA ALERTA REGISTRADA ({source})! {title}")
                self.send_notification(title, url, source, is_delisting)

    def monitor_binance(self):
        start = time.time()
        try:
            params = {
                "type": 1,
                "catalogId": 48,  # ID 48: Nuevos listados
                "pageNo": 1,
                "pageSize": 5
            }
            response = requests.get(BINANCE_API, params=params, headers=HEADERS, timeout=5)
            latency = (time.time() - start) * 1000
            
            if response.status_code == 200:
                self.db.log_api_metric("Binance", latency, 200)
                data = response.json()
                articles = data.get("data", {}).get("catalogs", [{}])[0].get("articles", [])
                for art in articles:
                    title = art.get("title")
                    code = art.get("code")
                    url = f"https://www.binance.com/en/support/announcement/{code}"
                    self.parse_and_check(title, str(code), url, "Binance")
            else:
                self.db.log_api_metric("Binance", latency, response.status_code, f"Status code: {response.status_code}")
                logging.error(f"Binance API devolvió código: {response.status_code}")
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.db.log_api_metric("Binance", latency, 0, str(e))
            self.db.log_system("ERROR", f"Error de red en Binance: {e}")
            logging.error(f"Excepción monitoreando Binance: {e}")

    def monitor_bybit(self):
        start = time.time()
        try:
            params = {
                "locale": "en-US",
                "limit": 5
            }
            response = requests.get(BYBIT_API, params=params, headers=HEADERS, timeout=5)
            latency = (time.time() - start) * 1000
            
            if response.status_code == 200:
                self.db.log_api_metric("Bybit", latency, 200)
                data = response.json()
                articles = data.get("result", {}).get("list", [])
                for art in articles:
                    title = art.get("title")
                    url = art.get("url")
                    code = url.strip("/").split("/")[-1] if url else None
                    self.parse_and_check(title, code, url, "Bybit")
            else:
                self.db.log_api_metric("Bybit", latency, response.status_code, f"Status code: {response.status_code}")
                logging.error(f"Bybit API devolvió código: {response.status_code}")
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.db.log_api_metric("Bybit", latency, 0, str(e))
            self.db.log_system("ERROR", f"Error de red en Bybit: {e}")
            logging.error(f"Excepción monitoreando Bybit: {e}")

    def monitor_okx(self):
        start = time.time()
        try:
            response = requests.get(OKX_API, headers=HEADERS, timeout=5)
            latency = (time.time() - start) * 1000
            
            if response.status_code == 200:
                self.db.log_api_metric("OKX", latency, 200)
                data = response.json()
                details = data.get("data", [{}])[0].get("details", [])
                for art in details:
                    title = art.get("title")
                    url = art.get("url")
                    code = url.strip("/").split("/")[-1] if url else None
                    self.parse_and_check(title, code, url, "OKX")
            else:
                self.db.log_api_metric("OKX", latency, response.status_code, f"Status code: {response.status_code}")
                logging.error(f"OKX API devolvió código: {response.status_code}")
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.db.log_api_metric("OKX", latency, 0, str(e))
            self.db.log_system("ERROR", f"Error de red en OKX: {e}")
            logging.error(f"Excepción monitoreando OKX: {e}")

    def run(self):
        logging.info("Inicializando bases de datos y cargando estado histórico...")
        self.db.log_system("SYSTEM", "Monitor iniciado en modo 24/7 continuo.")
        
        # Carga inicial silenciosa de anuncios existentes para evitar duplicar alertas en el primer ciclo
        self.monitor_binance()
        self.monitor_bybit()
        self.monitor_okx()
        
        # Enviar mensaje de confirmación de arranque
        startup_msg = "🟢 *[SISTEMA] Monitor de CEXs iniciado y operando en modo continuo 24/7.*"
        self._dispatch_message(startup_msg, is_heartbeat=True)
        logging.info("Arranque exitoso. Escaneando fuentes cada %d segundos...", POLLING_INTERVAL)
        
        while True:
            # Monitoreo de APIs con manejo interno de excepciones
            self.monitor_binance()
            self.monitor_bybit()
            self.monitor_okx()
            
            # Verificar si corresponde enviar el reporte de control periódico (heartbeat)
            current_time = time.time()
            if current_time - self.last_heartbeat_time >= HEARTBEAT_INTERVAL_HOURS * 3600:
                try:
                    self.send_heartbeat_report()
                except Exception as e:
                    logging.error(f"Fallo al compilar reporte de control: {e}")
                    self.db.log_system("ERROR", f"Fallo al compilar heartbeat: {e}")
                self.last_heartbeat_time = current_time
                
            time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    monitor = ListingMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        logging.info("Monitoreo detenido por el usuario.")
        monitor.db.log_system("SYSTEM", "Monitor detenido manualmente.")
