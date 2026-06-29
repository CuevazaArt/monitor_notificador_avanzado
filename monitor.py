import os
import time
import logging
import requests
from dotenv import load_dotenv

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

# APIs de Anuncios
BINANCE_API = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
BYBIT_API = "https://api2.bybit.com/fapi/beehive/public/v1/announcement/list"
OKX_API = "https://www.okx.com/api/v5/support/announcements"

# Encabezados HTTP comunes para evitar bloqueos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8"
}

class ListingMonitor:
    def __init__(self):
        self.seen_articles = set()

    def send_notification(self, title, url, source, is_delisting=False):
        emoji = "⚠️ [DESLISTADO]" if is_delisting else "🚀 [LISTADO]"
        message = (
            f"{emoji} **Nuevo Anuncio Detectado en {source}**\n\n"
            f"**Título:** {title}\n"
            f"**Enlace:** {url}"
        )
        
        # Enviar a Discord
        if DISCORD_WEBHOOK_URL:
            try:
                requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
                logging.info(f"Notificación enviada a Discord para: {title}")
            except Exception as e:
                logging.error(f"Error al enviar notificación a Discord: {e}")
                
        # Enviar a Telegram
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message.replace("**", "*"), # Telegram usa Markdown simple
                    "parse_mode": "Markdown"
                }
                requests.post(telegram_url, json=payload, timeout=5)
                logging.info(f"Notificación enviada a Telegram para: {title}")
            except Exception as e:
                logging.error(f"Error al enviar notificación a Telegram: {e}")

    def parse_and_check(self, title, code, url, source):
        if not title or not code:
            return
            
        unique_key = f"{source}_{code}"
        if unique_key not in self.seen_articles:
            self.seen_articles.add(unique_key)
            
            title_lower = title.lower()
            # Palabras clave de listado/deslistado
            keywords_list = ["will list", "lists", "introduce", "launch", "listará", "presenta"]
            keywords_delist = ["will delist", "delisting", "deslistará", "removerá", "delist"]
            
            is_delisting = any(kw in title_lower for kw in keywords_delist)
            is_listing = any(kw in title_lower for kw in keywords_list)
            
            if is_listing or is_delisting:
                logging.warning(f"¡ALERTA ({source})! {title} | Enlace: {url}")
                self.send_notification(title, url, source, is_delisting)

    def monitor_binance(self):
        try:
            payload = {
                "catalogId": 48,  # ID 48: Nuevos listados
                "pageNo": 1,
                "pageSize": 5
            }
            response = requests.post(BINANCE_API, json=payload, headers=HEADERS, timeout=5)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("data", {}).get("catalogs", [{}])[0].get("articles", [])
                for art in articles:
                    title = art.get("title")
                    code = art.get("code")
                    url = f"https://www.binance.com/en/support/announcement/{code}"
                    self.parse_and_check(title, code, url, "Binance")
        except Exception as e:
            logging.error(f"Error monitoreando Binance: {e}")

    def monitor_bybit(self):
        try:
            # Solicitar anuncios de Bybit
            params = {
                "limit": 5,
                "language": "en-US"
            }
            response = requests.get(BYBIT_API, params=params, headers=HEADERS, timeout=5)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("result", {}).get("list", [])
                for art in articles:
                    title = art.get("title")
                    code = art.get("id")
                    url = art.get("url") or f"https://announcements.bybit.com/en-US/article/{code}"
                    self.parse_and_check(title, code, url, "Bybit")
        except Exception as e:
            logging.error(f"Error monitoreando Bybit: {e}")

    def monitor_okx(self):
        try:
            # OKX
            response = requests.get(OKX_API, headers=HEADERS, timeout=5)
            if response.status_code == 200:
                data = response.json()
                articles = data.get("data", [])[:5]
                for art in articles:
                    title = art.get("title")
                    code = art.get("announcementId")
                    url = f"https://www.okx.com/support/hc/en-us/articles/{code}"
                    self.parse_and_check(title, code, url, "OKX")
        except Exception as e:
            logging.debug(f"Error monitoreando OKX: {e}")

    def run(self):
        logging.info("Inicializando bases de datos de anuncios...")
        
        # Carga inicial para evitar falsos positivos
        self.monitor_binance()
        self.monitor_bybit()
        self.monitor_okx()
        
        logging.info("Bucle de monitoreo activo iniciado. Buscando anuncios cada %d segundos...", POLLING_INTERVAL)
        
        while True:
            self.monitor_binance()
            self.monitor_bybit()
            self.monitor_okx()
            time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    monitor = ListingMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        logging.info("Monitoreo detenido por el usuario.")
