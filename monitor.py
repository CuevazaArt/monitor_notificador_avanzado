import os
import time
import logging
import re
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

# Encabezados HTTP comunes para evitar bloqueos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "clienttype": "web",
    "lang": "en"
}

# =====================================================================
# CLASES BASE Y FUENTES DE DATOS (ARQUITECTURA AGNÓSTICA)
# =====================================================================

class BaseSource:
    """Clase base abstracta para definir cualquier fuente de anuncios o datos."""
    def __init__(self, name):
        self.name = name

    def fetch_announcements(self):
        """
        Debe consultar la fuente y devolver una lista de diccionarios con el formato:
        [
            {"title": str, "code": str, "url": str},
            ...
        ]
        """
        raise NotImplementedError


class BinanceSource(BaseSource):
    def __init__(self):
        super().__init__("Binance")
        self.api_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

    def fetch_announcements(self):
        params = {
            "type": 1,
            "catalogId": 48,  # ID 48: Nuevos listados
            "pageNo": 1,
            "pageSize": 5
        }
        response = requests.get(self.api_url, params=params, headers=HEADERS, timeout=5)
        articles = []
        if response.status_code == 200:
            data = response.json()
            items = data.get("data", {}).get("catalogs", [{}])[0].get("articles", [])
            for item in items:
                articles.append({
                    "title": item.get("title"),
                    "code": str(item.get("code")),
                    "url": f"https://www.binance.com/en/support/announcement/{item.get('code')}"
                })
        else:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}")
        return articles


class BybitSource(BaseSource):
    def __init__(self):
        super().__init__("Bybit")
        self.api_url = "https://api.bybit.com/v5/announcements/index"

    def fetch_announcements(self):
        params = {
            "locale": "en-US",
            "limit": 5
        }
        response = requests.get(self.api_url, params=params, headers=HEADERS, timeout=5)
        articles = []
        if response.status_code == 200:
            data = response.json()
            items = data.get("result", {}).get("list", [])
            for item in items:
                url = item.get("url")
                code = url.strip("/").split("/")[-1] if url else None
                articles.append({
                    "title": item.get("title"),
                    "code": code,
                    "url": url
                })
        else:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}")
        return articles


class OKXSource(BaseSource):
    def __init__(self):
        super().__init__("OKX")
        self.api_url = "https://www.okx.com/api/v5/support/announcements"

    def fetch_announcements(self):
        response = requests.get(self.api_url, headers=HEADERS, timeout=5)
        articles = []
        if response.status_code == 200:
            data = response.json()
            details = data.get("data", [{}])[0].get("details", [])
            for item in details:
                url = item.get("url")
                code = url.strip("/").split("/")[-1] if url else None
                articles.append({
                    "title": item.get("title"),
                    "code": code,
                    "url": url
                })
        else:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}")
        return articles

# =====================================================================
# MOTOR PRINCIPAL DEL MONITOR
# =====================================================================

class ListingMonitor:
    def __init__(self):
        self.db = Database()
        self.start_time = time.time()
        self.last_heartbeat_time = time.time()
        
        # Lista agnóstica de fuentes de datos activas
        self.sources = [
            BinanceSource(),
            BybitSource(),
            OKXSource()
        ]

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

    def extract_ticker(self, title):
        """Extrae el ticker/símbolo en mayúsculas de un título de anuncio."""
        # 1. Buscar entre paréntesis: (TICKER)
        match = re.search(r'\(([A-Z0-9]{2,10})\)', title)
        if match:
            return match.group(1)
            
        # 2. Buscar pares de trading comunes: TICKER/USDT
        match = re.search(r'([A-Z0-9]{2,10})/USD', title)
        if match:
            return match.group(1)
            
        # 3. Buscar palabras en mayúsculas de 3 a 8 caracteres
        words = re.findall(r'\b([A-Z0-9]{3,8})\b', title)
        ignore_words = {
            "SPOT", "USDT", "USDC", "USD", "EUR", "JPY", "LIST", "NEW", 
            "CEX", "OKX", "BYBIT", "LAUNCH", "RWA", "VIP", "APR", "AI", "FUTURES"
        }
        for word in words:
            if word not in ignore_words:
                return word
        return None

    def check_onchain_status(self, ticker):
        """Vigilancia On-chain: Consulta DexScreener para buscar pools de liquidez activos."""
        url = f"https://api.dexscreener.com/latest/dex/search?q={ticker}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=5)
            if response.status_code == 200:
                data = response.json()
                pairs = data.get("pairs", [])
                if not pairs:
                    return None
                
                # Filtrar pares con liquidez informada
                valid_pairs = [p for p in pairs if p.get("liquidity", {}).get("usd") is not None]
                if not valid_pairs:
                    return None
                    
                # Seleccionar el par con mayor liquidez
                best_pair = max(valid_pairs, key=lambda p: p["liquidity"]["usd"])
                base_token = best_pair.get("baseToken", {})
                
                return {
                    "chain": best_pair.get("chainId", "desconocida").upper(),
                    "dex": best_pair.get("dexId", "desconocido").upper(),
                    "contract": base_token.get("address", "N/A"),
                    "liquidity_usd": best_pair.get("liquidity", {}).get("usd", 0),
                    "price_usd": best_pair.get("priceUsd", "0.0"),
                    "pair_url": best_pair.get("url", "")
                }
        except Exception as e:
            logging.error(f"Error en vigilancia on-chain (DexScreener) para {ticker}: {e}")
        return None

    def send_notification(self, title, url, source, is_delisting=False, onchain_info=None):
        emoji = "⚠️ [DESLISTADO]" if is_delisting else "🚀 [LISTADO]"
        message = (
            f"{emoji} **Nuevo Anuncio Detectado en {source}**\n\n"
            f"**Título:** {title}\n"
            f"**Enlace:** {url}"
        )
        
        if onchain_info:
            message += (
                f"\n\n🌐 **Vigilancia On-Chain (Confirmación):**\n"
                f"• Red / DEX: {onchain_info['chain']} / {onchain_info['dex']}\n"
                f"• Dirección de Contrato: `{onchain_info['contract']}`\n"
                f"• Liquidez del Pool: ${onchain_info['liquidity_usd']:,.2f} USD\n"
                f"• Precio Actual: ${onchain_info['price_usd']} USD\n"
                f"• Gráfico en tiempo real: [DexScreener]({onchain_info['pair_url']})"
            )
            
        self._dispatch_message(message)

    def send_heartbeat_report(self):
        logging.info("Compilando y enviando reporte de control (heartbeat) a Telegram...")
        
        summary = self.db.get_metrics_summary(hours=HEARTBEAT_INTERVAL_HOURS)
        total_alerts = self.db.get_total_alerts_count(hours=HEARTBEAT_INTERVAL_HOURS)
        uptime = self.get_uptime_str()
        
        # Determinar estado de salud del sistema
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
        if DISCORD_WEBHOOK_URL and not is_heartbeat:
            try:
                requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
                logging.info("Notificación enviada a Discord.")
            except Exception as e:
                logging.error(f"Error al enviar notificación a Discord: {e}")
                self.db.log_system("ERROR", f"Fallo al notificar Discord: {e}")
                
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message.replace("**", "*"),
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
        keywords_list = ["will list", "lists", "introduce", "launch", "listará", "presenta"]
        keywords_delist = ["will delist", "delisting", "deslistará", "removerá", "delist"]
        
        is_delisting = any(kw in title_lower for kw in keywords_delist)
        is_listing = any(kw in title_lower for kw in keywords_list)
        
        if is_listing or is_delisting:
            is_new = self.db.log_alert(source, code, title, url, is_delisting)
            if is_new:
                logging.warning(f"¡NUEVA ALERTA REGISTRADA ({source})! {title}")
                
                # Vigilancia On-Chain para confirmar eventos y actividades de listados
                onchain_info = None
                if is_listing:
                    ticker = self.extract_ticker(title)
                    if ticker:
                        logging.info(f"Ticker detectado ({ticker}). Buscando pools on-chain...")
                        onchain_info = self.check_onchain_status(ticker)
                
                self.send_notification(title, url, source, is_delisting, onchain_info)

    def run(self):
        logging.info("Inicializando bases de datos y cargando estado histórico...")
        self.db.log_system("SYSTEM", "Monitor iniciado en modo 24/7 continuo y con vigilancia on-chain.")
        
        # Carga inicial silenciosa
        for source in self.sources:
            try:
                source.fetch_announcements()
            except Exception:
                pass
        
        startup_msg = "🟢 *[SISTEMA] Monitor de CEXs iniciado, operando 24/7 y con vigilancia on-chain activa.*"
        self._dispatch_message(startup_msg, is_heartbeat=True)
        logging.info("Arranque exitoso. Escaneando fuentes cada %d segundos...", POLLING_INTERVAL)
        
        while True:
            # Procesar cada fuente de forma agnóstica
            for source in self.sources:
                start = time.time()
                try:
                    articles = source.fetch_announcements()
                    latency = (time.time() - start) * 1000
                    self.db.log_api_metric(source.name, latency, 200)
                    for art in articles:
                        self.parse_and_check(art["title"], art["code"], art["url"], source.name)
                except Exception as e:
                    latency = (time.time() - start) * 1000
                    self.db.log_api_metric(source.name, latency, 0, str(e))
                    self.db.log_system("ERROR", f"Error monitoreando {source.name}: {e}")
                    logging.error(f"Excepción en fuente {source.name}: {e}")
            
            # Control periódico de salud (heartbeat)
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
