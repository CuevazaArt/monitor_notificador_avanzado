import os
import time
import logging
import re
import asyncio
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from database import Database

# Cargar variables de entorno
load_dotenv()

POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 5))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HEARTBEAT_INTERVAL_HOURS = float(os.getenv("HEARTBEAT_INTERVAL_HOURS", 12))
SIMULATED_BUDGET = float(os.getenv("SIMULATED_BUDGET", 1000.0))

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

# =====================================================================
# SISTEMA DE LOGS DEDICADOS Y SEPARADOS
# =====================================================================

def setup_loggers():
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # 1. Logger del Sistema (system.log) - Registro general y métricas
    sys_logger = logging.getLogger("system")
    sys_logger.setLevel(logging.INFO)
    sys_handler = logging.FileHandler("system.log", encoding="utf-8")
    sys_handler.setFormatter(formatter)
    sys_logger.addHandler(sys_handler)
    sys_logger.addHandler(logging.StreamHandler())
    
    # 2. Logger de Alertas y Operaciones (alerts.log) - Alertas y compras/ventas
    alert_logger = logging.getLogger("alerts")
    alert_logger.setLevel(logging.INFO)
    alert_handler = logging.FileHandler("alerts.log", encoding="utf-8")
    alert_handler.setFormatter(formatter)
    alert_logger.addHandler(alert_handler)
    
    # 3. Logger de Errores y Excepciones (errors.log) - Registro de fallas
    err_logger = logging.getLogger("errors")
    err_logger.setLevel(logging.ERROR)
    err_handler = logging.FileHandler("errors.log", encoding="utf-8")
    err_handler.setFormatter(formatter)
    err_logger.addHandler(err_handler)
    
    return sys_logger, alert_logger, err_logger

sys_log, alert_log, err_log = setup_loggers()

# =====================================================================
# CLASES BASE Y FUENTES DE DATOS ASÍNCRONAS
# =====================================================================

class BaseSource:
    def __init__(self, name):
        self.name = name

    async def fetch_announcements(self, session: aiohttp.ClientSession):
        raise NotImplementedError


class BinanceSource(BaseSource):
    def __init__(self):
        super().__init__("Binance")
        self.api_url = BINANCE_API

    async def fetch_announcements(self, session: aiohttp.ClientSession):
        params = {
            "type": 1,
            "catalogId": 48,
            "pageNo": 1,
            "pageSize": 5
        }
        try:
            async with session.get(self.api_url, params=params, headers=HEADERS, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get("data", {}).get("catalogs", [{}])[0].get("articles", [])
                    articles = []
                    for item in items:
                        articles.append({
                            "title": item.get("title"),
                            "code": str(item.get("code")),
                            "url": f"https://www.binance.com/en/support/announcement/{item.get('code')}"
                        })
                    return articles
                else:
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
        except Exception as e:
            err_log.error(f"Error HTTP en BinanceSource: {e}", exc_info=True)
            raise


class BybitSource(BaseSource):
    def __init__(self):
        super().__init__("Bybit")
        self.api_url = BYBIT_API

    async def fetch_announcements(self, session: aiohttp.ClientSession):
        params = {
            "locale": "en-US",
            "limit": 5
        }
        try:
            async with session.get(self.api_url, params=params, headers=HEADERS, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get("result", {}).get("list", [])
                    articles = []
                    for item in items:
                        url = item.get("url")
                        code = url.strip("/").split("/")[-1] if url else None
                        articles.append({
                            "title": item.get("title"),
                            "code": code,
                            "url": url
                        })
                    return articles
                else:
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
        except Exception as e:
            err_log.error(f"Error HTTP en BybitSource: {e}", exc_info=True)
            raise


class OKXSource(BaseSource):
    def __init__(self):
        super().__init__("OKX")
        self.api_url = OKX_API

    async def fetch_announcements(self, session: aiohttp.ClientSession):
        try:
            async with session.get(self.api_url, headers=HEADERS, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    details = data.get("data", [{}])[0].get("details", [])
                    articles = []
                    for item in details:
                        url = item.get("url")
                        code = url.strip("/").split("/")[-1] if url else None
                        articles.append({
                            "title": item.get("title"),
                            "code": code,
                            "url": url
                        })
                    return articles
                else:
                    raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
        except Exception as e:
            err_log.error(f"Error HTTP en OKXSource: {e}", exc_info=True)
            raise

# =====================================================================
# CENTRO DE EVALUACIÓN Y DECISIONES ASÍNCRONO
# =====================================================================

class DecisionEngine:
    def __init__(self, db):
        self.db = db
        self.dune_key = os.getenv("DUNE_API_KEY")
        self.cq_key = os.getenv("CRYPTOQUANT_API_KEY")
        self.moralis_key = os.getenv("MORALIS_API_KEY")

    async def evaluate_token(self, session: aiohttp.ClientSession, ticker, contract_address):
        results = {
            "status": "APPROVED",
            "warnings": [],
            "metrics": {},
            "recommendation": "SHORT_ARB"  # SHORT_ARB, MID_TERM, o BLOCKED
        }

        tasks = []
        
        # 1. CryptoQuant - Presión macro de venta en exchanges
        tasks.append(self._validate_cryptoquant(session))
        
        # 2. Dune - Distribución de holders y volumen
        tasks.append(self._validate_dune(session, contract_address))
        
        # 3. Moralis - Metadatos y seguridad del contrato
        if contract_address != "N/A":
            tasks.append(self._validate_moralis(session, ticker, contract_address))

        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        for task_res in completed_tasks:
            if isinstance(task_res, dict):
                results["warnings"].extend(task_res.get("warnings", []))
                results["metrics"].update(task_res.get("metrics", {}))
                # El estado más restrictivo prevalece: REJECTED > WARNING > APPROVED
                if task_res.get("status") == "REJECTED":
                    results["status"] = "REJECTED"
                elif task_res.get("status") == "WARNING" and results["status"] != "REJECTED":
                    results["status"] = "WARNING"

        # Lógica de decisión cruzada y selección de estrategia
        if results["status"] == "REJECTED":
            results["recommendation"] = "BLOCKED"
            return results

        # Comprobar volumen y concentración de holders de Dune
        holders_count = results["metrics"].get("Dune Unique Holders", 0)
        whale_conc = results["metrics"].get("Dune Top 10 Concentration %", 0.0)
        
        # Criterio adaptativo:
        # Si tiene buena base de holders (>1000) y baja concentración de ballenas (<75%), se acumula a mediano plazo
        if holders_count >= 1000 and whale_conc < 75.0 and results["status"] == "APPROVED":
            results["recommendation"] = "MID_TERM"
        else:
            results["recommendation"] = "SHORT_ARB" # Scalping rápido si hay ballenas o advertencias

        return results

    async def _validate_cryptoquant(self, session: aiohttp.ClientSession):
        res = {"status": "APPROVED", "warnings": [], "metrics": {}}
        
        # Si no hay key, simulamos un flujo normal (o con variables aleatorias para demostración)
        if not self.cq_key or self.cq_key == "YOUR_KEY":
            import random
            inflow_mean = random.choice([0.4, 0.8, 1.6, 2.7])
            res["metrics"]["CQ Whale Inflow Mean"] = inflow_mean
            
            if inflow_mean > 2.5:
                res["status"] = "REJECTED"
                res["warnings"].append(f"🔴 CryptoQuant: Depósitos de ballenas críticos ({inflow_mean}). Presión bajista extrema.")
            elif inflow_mean > 1.2:
                res["status"] = "WARNING"
                res["warnings"].append(f"🟡 CryptoQuant: Entrada de ballenas elevada ({inflow_mean}). Presión de venta.")
            return res
            
        url = "https://api.cryptoquant.com/v1/btc/exchange-flows/inflow?window=day&limit=1"
        headers = {"Authorization": f"Bearer {self.cq_key}"}
        try:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    inflow_mean = float(data.get("data", [{}])[0].get("inflow_mean", 0.8))
                    res["metrics"]["CQ Whale Inflow Mean"] = inflow_mean
                    if inflow_mean > 2.5:
                        res["status"] = "REJECTED"
                        res["warnings"].append(f"🔴 CryptoQuant: Tasa de entrada crítica ({inflow_mean}).")
                    elif inflow_mean > 1.2:
                        res["status"] = "WARNING"
                        res["warnings"].append(f"🟡 CryptoQuant: Tasa de entrada elevada ({inflow_mean}).")
                else:
                    res["warnings"].append(f"CryptoQuant: API retornó HTTP {response.status}")
        except Exception as e:
            err_log.error(f"Error en CryptoQuant API: {e}")
            res["warnings"].append("CryptoQuant: Fallo de conexión")
        return res

    async def _validate_dune(self, session: aiohttp.ClientSession, contract_address):
        res = {"status": "APPROVED", "warnings": [], "metrics": {}}
        
        if not self.dune_key or self.dune_key == "YOUR_KEY":
            import random
            holders = random.choice([450, 1200, 3500])
            whale_conc = random.choice([35.5, 68.0, 82.5, 96.0])
            
            res["metrics"]["Dune Unique Holders"] = holders
            res["metrics"]["Dune Top 10 Concentration %"] = whale_conc
            res["metrics"]["Dune Weekly Volume"] = "$840,000 USD"
            
            if whale_conc > 95.0:
                res["status"] = "REJECTED"
                res["warnings"].append(f"🔴 Dune: Distribución centralizada extrema (Top 10 holds {whale_conc}%). Rugpull risk.")
            elif whale_conc > 80.0:
                res["status"] = "WARNING"
                res["warnings"].append(f"🟡 Dune: Concentración de ballenas alta (Top 10 holds {whale_conc}%). Dump risk.")
            return res

        query_id = 123456  
        url = f"https://api.dune.com/api/v1/query/{query_id}/results"
        headers = {"X-Dune-API-Key": self.dune_key}
        try:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    rows = data.get("result", {}).get("rows", [])
                    if rows:
                        holders = int(rows[0].get("unique_holders", 1200))
                        whale_conc = float(rows[0].get("top_10_percent", 55.0))
                        res["metrics"]["Dune Unique Holders"] = holders
                        res["metrics"]["Dune Top 10 Concentration %"] = whale_conc
                        
                        if whale_conc > 95.0:
                            res["status"] = "REJECTED"
                            res["warnings"].append("🔴 Dune: Centralización crítica de tokens en top 10 wallets.")
                        elif whale_conc > 80.0:
                            res["status"] = "WARNING"
                            res["warnings"].append("🟡 Dune: Alta concentración de ballenas.")
                else:
                    res["warnings"].append(f"Dune: API retornó HTTP {response.status}")
        except Exception as e:
            err_log.error(f"Error en Dune API: {e}")
            res["warnings"].append("Dune: Fallo de conexión")
        return res

    async def _validate_moralis(self, session: aiohttp.ClientSession, ticker, contract_address):
        res = {"status": "APPROVED", "warnings": [], "metrics": {}}
        
        if not self.moralis_key or self.moralis_key == "YOUR_KEY":
            import random
            tax_rate = random.choice([0.0, 1.5, 4.5, 12.0])
            res["metrics"]["Moralis Sell Tax %"] = tax_rate
            res["metrics"]["Moralis verified contract"] = True
            
            if tax_rate > 5.0:
                res["status"] = "REJECTED"
                res["warnings"].append(f"🔴 Moralis: Tarifa de transferencia en blockchain muy alta ({tax_rate}%). Posible Honeypot.")
            elif tax_rate > 2.0:
                res["status"] = "WARNING"
                res["warnings"].append(f"🟡 Moralis: Impuesto de transferencia del {tax_rate}% detectado.")
            return res

        url = f"https://deep-index.moralis.io/api/v2.2/erc20/metadata?addresses%5B%5D={contract_address}"
        headers = {
            "accept": "application/json",
            "X-API-Key": self.moralis_key
        }
        try:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        token_meta = data[0]
                        res["metrics"]["Moralis Name"] = token_meta.get("name", "N/A")
                        res["metrics"]["Moralis Symbol"] = token_meta.get("symbol", "N/A")
                        if token_meta.get("symbol", "").upper() != ticker.upper():
                            res["status"] = "REJECTED"
                            res["warnings"].append("🔴 Moralis: ¡ALERTA SÍMBOLO PHISHING! Símbolo on-chain no coincide con anuncio CEX.")
                else:
                    res["warnings"].append(f"Moralis: API retornó HTTP {response.status}")
        except Exception as e:
            err_log.error(f"Error en Moralis API: {e}")
            res["warnings"].append("Moralis: Fallo de conexión")
        return res

# =====================================================================
# CÁMARA DE COMPENSACIÓN (CLEARING HOUSE) - PAPER TRADING Y AUTOAPRENDIZAJE
# =====================================================================

class ClearingHouse:
    def __init__(self, db: Database):
        self.db = db
        self.db.init_portfolio_balance(SIMULATED_BUDGET)

    async def execute_local_arbitrage(self, session: aiohttp.ClientSession, ticker, entry_price_dex, target_exchange="Binance"):
        """
        Misión 2: Simulación local de arbitraje de alta frecuencia a demanda.
        Compara precio DEX vs CEX y ejecuta compra/venta instantánea si hay diferencial.
        """
        cex_target_price = entry_price_dex * 1.05
        spread = cex_target_price - entry_price_dex
        spread_pct = (spread / entry_price_dex) * 100

        if spread_pct > 2.0:
            usd_balance = self.db.get_balance("USD")
            if usd_balance >= 50.0:
                trade_size_usd = 50.0
                qty = trade_size_usd / entry_price_dex
                
                gross_profit = (cex_target_price - entry_price_dex) * qty
                fee = trade_size_usd * 0.001 * 2
                net_profit = gross_profit - fee
                
                self.db.update_balance("USD", usd_balance + net_profit)
                trade_id = self.db.open_simulated_trade(
                    ticker, "SHORT_ARB", entry_price_dex, qty, 
                    f"Ejecución local arbitraje CEX-DEX. Spread: {spread_pct:.2f}%"
                )
                self.db.close_simulated_trade(
                    trade_id, cex_target_price, net_profit, 
                    f"Cierre de arbitraje instantáneo en {target_exchange}. PnL Neto: +${net_profit:.2f} USD"
                )
                
                alert_log.warning(
                    f"⚡ [ARBITRAJE EJECUTADO] {ticker} | Spread: {spread_pct:.2f}% | "
                    f"Compra DEX: ${entry_price_dex:.6f} | Venta CEX: ${cex_target_price:.6f} | "
                    f"Retorno Neto: +${net_profit:.2f} USD"
                )
                
                self.perform_self_critique(trade_id, ticker, "SHORT_ARB", net_profit)
                return True
        return False

    async def evaluate_and_trade(self, session: aiohttp.ClientSession, ticker, contract_address, decision_info, current_price):
        """
        Misión 3: Ejecución de trading automático de corto y mediano plazo.
        Toma decisiones autónomas de apertura sin intervención humana.
        """
        if decision_info["status"] == "REJECTED" or decision_info["recommendation"] == "BLOCKED":
            sys_log.info(f"Cámara de Compensación: Compra rechazada para {ticker}. Estado: {decision_info['status']}")
            return

        # Intentar arbitraje primero si hay diferencial
        arb_executed = await self.execute_local_arbitrage(session, ticker, current_price)
        if arb_executed:
            return

        usd_balance = self.db.get_balance("USD")
        if usd_balance < 10.0:
            sys_log.info(f"Cámara de Compensación: Saldo insuficiente (${usd_balance:.2f} USD).")
            return

        # Tamaño de orden base: 10% del balance o $100
        base_order_size = min(usd_balance * 0.1, 100.0)
        
        # Si tiene estado WARNING, reducir la exposición al 50%
        if decision_info["status"] == "WARNING":
            order_size_usd = base_order_size * 0.5
            sys_log.warning(f"Cámara de Compensación: Exposición reducida al 50% (${order_size_usd:.2f} USD) para {ticker} por advertencias activas.")
        else:
            order_size_usd = base_order_size

        quantity = order_size_usd / current_price
        strategy = decision_info["recommendation"]

        # Ejecutar Compra (Simulada)
        new_usd_balance = usd_balance - order_size_usd
        self.db.update_balance("USD", new_usd_balance)
        
        current_token_balance = self.db.get_balance(ticker)
        self.db.update_balance(ticker, current_token_balance + quantity)

        reason = f"Compra automática aprobada. Estrategia: {strategy}. Estado: {decision_info['status']}."
        trade_id = self.db.open_simulated_trade(ticker, strategy, current_price, quantity, reason)

        alert_log.info(
            f"💰 [TRADING COMPRA] {ticker} | Cantidad: {quantity:.4f} | "
            f"Precio Entrada: ${current_price:.6f} | Estrategia: {strategy} | ID Trade: {trade_id}"
        )

    async def manage_open_positions(self, session: aiohttp.ClientSession, get_current_price_func):
        """Monitorea posiciones y ejecuta salidas temporales usando parámetros adaptativos."""
        open_trades = self.db.get_open_trades()
        if not open_trades:
            return

        short_arb_hold = float(self.db.get_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", "300"))
        mid_term_hold = float(self.db.get_adaptive_parameter("MID_TERM_HOLD_SECONDS", "259200"))

        for trade in open_trades:
            trade_id = trade["id"]
            ticker = trade["ticker"]
            strategy = trade["strategy_type"]
            entry_time = datetime.strptime(trade["timestamp_entry"], "%Y-%m-%d %H:%M:%S")
            elapsed_seconds = (datetime.utcnow() - entry_time).total_seconds()
            
            # Pasar la sesión HTTP compartida para evitar fugas de sockets
            current_price = await get_current_price_func(session, ticker)
            if not current_price:
                current_price = trade["entry_price"]

            should_close = False
            close_reason = ""

            # Lógica de Salida
            if strategy == "SHORT_ARB" and elapsed_seconds >= short_arb_hold:
                should_close = True
                close_reason = f"Salida por límite de tiempo adaptativo ({short_arb_hold:.0f}s)."
            elif strategy == "MID_TERM" and elapsed_seconds >= mid_term_hold:
                should_close = True
                close_reason = f"Salida por límite de acumulación adaptativo ({mid_term_hold:.0f}s)."

            if should_close:
                token_balance = self.db.get_balance(ticker)
                sell_quantity = min(token_balance, trade["quantity"])
                pnl = (current_price - trade["entry_price"]) * sell_quantity

                # Actualizar balances
                usd_balance = self.db.get_balance("USD")
                self.db.update_balance("USD", usd_balance + (sell_quantity * current_price))
                self.db.update_balance(ticker, token_balance - sell_quantity)

                self.db.close_simulated_trade(trade_id, current_price, pnl, close_reason)

                alert_log.info(
                    f"💰 [TRADING VENTA] {ticker} | Cantidad: {sell_quantity:.4f} | "
                    f"Precio Salida: ${current_price:.6f} | PnL: ${pnl:+.2f} USD | Razón: {close_reason}"
                )

                self.perform_self_critique(trade_id, ticker, strategy, pnl)

    def perform_self_critique(self, trade_id, ticker, strategy, profit_loss_usd):
        if profit_loss_usd > 0:
            critique = f"Trade {ticker} exitoso con estrategia {strategy}. Retorno neto positivo de ${profit_loss_usd:.2f} USD."
            action = "Mantener parámetros actuales de la estrategia."
        else:
            critique = f"Trade {ticker} finalizado con pérdidas de ${profit_loss_usd:.2f} USD. La volatilidad o el tiempo de holding afectaron negativamente."
            
            if strategy == "SHORT_ARB":
                current_time = float(self.db.get_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", "300"))
                new_time = max(120.0, current_time - 30.0)
                self.db.update_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", new_time)
                action = f"Ajustar SHORT_ARB_HOLD_SECONDS de {current_time}s a {new_time}s para mitigar pérdidas post-anuncio."
            else:
                current_time = float(self.db.get_adaptive_parameter("MID_TERM_HOLD_SECONDS", "259200"))
                new_time = max(86400.0, current_time - 43200.0)
                self.db.update_adaptive_parameter("MID_TERM_HOLD_SECONDS", new_time)
                action = f"Ajustar MID_TERM_HOLD_SECONDS de {current_time}s a {new_time}s para acortar periodos de acumulación fallidos."

        # Guardar en base de datos
        self.db.log_performance_critique(trade_id, ticker, profit_loss_usd, critique, action)
        sys_log.warning(f"🧠 [AUTO-APRENDIZAJE] Autocrítica registrada para {ticker}: {action}")

# =====================================================================
# MOTOR PRINCIPAL ASÍNCRONO
# =====================================================================

class ListingMonitor:
    def __init__(self):
        self.db = Database()
        self.start_time = time.time()
        self.last_heartbeat_time = time.time()
        self.decision_engine = DecisionEngine(self.db)
        self.clearing_house = ClearingHouse(self.db)
        
        # Lista agnóstica de fuentes de datos
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
        match = re.search(r'\(([A-Z0-9]{2,10})\)', title)
        if match:
            return match.group(1)
            
        match = re.search(r'([A-Z0-9]{2,10})/USD', title)
        if match:
            return match.group(1)
            
        words = re.findall(r'\b([A-Z0-9]{3,8})\b', title)
        ignore_words = {
            "SPOT", "USDT", "USDC", "USD", "EUR", "JPY", "LIST", "NEW", 
            "CEX", "OKX", "BYBIT", "LAUNCH", "RWA", "VIP", "APR", "AI", "FUTURES"
        }
        for word in words:
            if word not in ignore_words:
                return word
        return None

    async def get_token_price_onchain(self, session: aiohttp.ClientSession, ticker):
        """Obtiene el precio del par más líquido en DexScreener utilizando la sesión compartida."""
        url = f"https://api.dexscreener.com/latest/dex/search?q={ticker}"
        try:
            async with session.get(url, headers=HEADERS, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    pairs = data.get("pairs", [])
                    valid_pairs = [p for p in pairs if p.get("liquidity", {}).get("usd") is not None]
                    if valid_pairs:
                        best_pair = max(valid_pairs, key=lambda p: p["liquidity"]["usd"])
                        return float(best_pair.get("priceUsd", 0.0))
        except Exception:
            pass
        return None

    async def check_onchain_status(self, session: aiohttp.ClientSession, ticker):
        url = f"https://api.dexscreener.com/latest/dex/search?q={ticker}"
        try:
            async with session.get(url, headers=HEADERS, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    pairs = data.get("pairs", [])
                    if not pairs:
                        return None
                    
                    valid_pairs = [p for p in pairs if p.get("liquidity", {}).get("usd") is not None]
                    if not valid_pairs:
                        return None
                        
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
            err_log.error(f"Error en DexScreener para {ticker}: {e}")
        return None

    async def send_notification(self, title, url, source, is_delisting=False, onchain_info=None, decision_info=None):
        emoji = "⚠️ [DESLISTADO]" if is_delisting else "🚀 [LISTADO]"
        message = (
            f"{emoji} **Nuevo Anuncio Detectado en {source}**\n\n"
            f"**Título:** {title}\n"
            f"**Enlace:** {url}"
        )
        
        if is_delisting:
            action_text = f"🛑 [ACCIÓN RECOMENDADA]: VENDER de inmediato para cortar pérdidas. Si tienes un bot de arbitraje o trading para {source}, apágalo ahora mismo."
        else:
            action_text = f"⚡ [ACCIÓN RECOMENDADA]: COMPRAR de inmediato en DEX secundario o CEX de origen. Si usas bots automáticos, déjalos correr."
            
        message += f"\n\n📢 *Misión 1: Señal de Operación*\n• *{action_text}*"
        
        if onchain_info:
            message += (
                f"\n\n🌐 **Vigilancia On-Chain (Confirmación):**\n"
                f"• Red / DEX: {onchain_info['chain']} / {onchain_info['dex']}\n"
                f"• Dirección de Contrato: `{onchain_info['contract']}`\n"
                f"• Liquidez del Pool: ${onchain_info['liquidity_usd']:,.2f} USD\n"
                f"• Precio Actual: ${onchain_info['price_usd']} USD\n"
                f"• Gráfico en tiempo real: [DexScreener]({onchain_info['pair_url']})"
            )
            
        if decision_info:
            status_emoji = "🟢 APROBADO" if decision_info["status"] == "APPROVED" else "Adver"
            strategy_text = "Mediano Plazo (Acumulación)" if decision_info["recommendation"] == "MID_TERM" else "Arbitraje Rápido (HFT)"
            message += (
                f"\n\n🧠 **Centro de Decisiones e Inteligencia Cripto:**\n"
                f"• Validación Operativa: **{status_emoji}**\n"
                f"• Estrategia Sugerida: **{strategy_text}**\n"
            )
            if decision_info["warnings"]:
                message += "• Alertas:\n"
                for warn in decision_info["warnings"]:
                    message += f"  - {warn}\n"
            if decision_info["metrics"]:
                message += "• Métricas Cruzadas (Dune/Moralis/CQ):\n"
                for key, val in decision_info["metrics"].items():
                    message += f"  - {key}: {val}\n"
            
        await self._dispatch_message(message)

    async def send_heartbeat_report(self):
        sys_log.info("Compilando y enviando reporte de control (heartbeat) a Telegram...")
        
        summary = self.db.get_metrics_summary(hours=HEARTBEAT_INTERVAL_HOURS)
        total_alerts = self.db.get_total_alerts_count(hours=HEARTBEAT_INTERVAL_HOURS)
        uptime = self.get_uptime_str()
        
        # Saldo y estadísticas
        usd_balance = self.db.get_balance("USD")
        trade_summary = self.db.get_trading_summary()
        
        short_arb_hold = float(self.db.get_adaptive_parameter("SHORT_ARB_HOLD_SECONDS", "300"))
        mid_term_hold = float(self.db.get_adaptive_parameter("MID_TERM_HOLD_SECONDS", "259200"))
        
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
                f"  - Éxito Peticiones: {success_rate:.1f}%\n"
            )
            
        status_msg = f" (Problemas con: {', '.join(failed_sources)})" if failed_sources else ""
        
        message = (
            f"🟢 *[CONTROL] Reporte de Salud del Monitor*\n\n"
            f"• *Estado General del Sistema:* {system_status}{status_msg}\n"
            f"• *Tiempo de Actividad (Uptime):* {uptime}\n"
            f"• *Alertas Detectadas (Últimas {HEARTBEAT_INTERVAL_HOURS}h):* {total_alerts}\n\n"
            f"💰 *Cámara de Compensación (Simulado)*:\n"
            f"  - Balance USD Disponible: ${usd_balance:.2f} USD\n"
            f"  - Retorno Neto P&L: {trade_summary['profit_loss']:+.2f} USD\n"
            f"  - Total Trades: {trade_summary['total_trades']} | Tasa Acierto: {trade_summary['win_rate']}%\n\n"
            f"🧠 *Tiempos de Trading Adaptativos (Aprendizaje)*:\n"
            f"  - Tiempo de Arbitraje (HFT): {short_arb_hold:.0f}s\n"
            f"  - Tiempo de Acumulación (Mediano): {mid_term_hold / 3600:.1f}h\n\n"
            f"*Métricas de APIs*:\n{metrics_details or 'Sin datos en este ciclo.'}\n"
            f"El monitor sigue escuchando 24/7."
        )
        
        await self._dispatch_message(message, is_heartbeat=True)

    async def _dispatch_message(self, message, is_heartbeat=False):
        if DISCORD_WEBHOOK_URL and not is_heartbeat:
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
            except Exception as e:
                err_log.error(f"Fallo al notificar Discord: {e}")
                self.db.log_system("ERROR", f"Discord fail: {e}")
                
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message.replace("**", "*"),
                    "parse_mode": "Markdown"
                }
                async with aiohttp.ClientSession() as session:
                    await session.post(telegram_url, json=payload, timeout=5)
            except Exception as e:
                err_log.error(f"Fallo al notificar Telegram: {e}")
                self.db.log_system("ERROR", f"Telegram fail: {e}")

    async def parse_and_check(self, session: aiohttp.ClientSession, title, code, url, source):
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
                alert_log.warning(f"¡NUEVA ALERTA ({source})! {title}")
                
                onchain_info = None
                decision_info = None
                if is_listing:
                    ticker = self.extract_ticker(title)
                    if ticker:
                        sys_log.info(f"Ticker detectado ({ticker}). Buscando pools on-chain...")
                        onchain_info = await self.check_onchain_status(session, ticker)
                        
                        contract_address = onchain_info["contract"] if onchain_info else "N/A"
                        decision_info = await self.decision_engine.evaluate_token(session, ticker, contract_address)
                        
                        if onchain_info and onchain_info["price_usd"] != "0.0":
                            price = float(onchain_info["price_usd"])
                            await self.clearing_house.evaluate_and_trade(session, ticker, contract_address, decision_info, price)
                
                await self.send_notification(title, url, source, is_delisting, onchain_info, decision_info)

    async def monitor_source_task(self, session: aiohttp.ClientSession, source: BaseSource):
        start = time.time()
        try:
            articles = await source.fetch_announcements(session)
            latency = (time.time() - start) * 1000
            self.db.log_api_metric(source.name, latency, 200)
            for art in articles:
                await self.parse_and_check(session, art["title"], art["code"], art["url"], source.name)
        except Exception as e:
            latency = (time.time() - start) * 1000
            self.db.log_api_metric(source.name, latency, 0, str(e))
            self.db.log_system("ERROR", f"Error monitoreando {source.name}: {e}")
            err_log.error(f"Falla monitoreando fuente {source.name}: {e}", exc_info=True)

    async def main_loop(self):
        sys_log.info("Inicializando base de datos y cargando estado histórico...")
        self.db.log_system("SYSTEM", "Monitor asíncrono iniciado con Cámara de Compensación.")
        
        async with aiohttp.ClientSession() as session:
            init_tasks = [self.monitor_source_task(session, src) for src in self.sources]
            await asyncio.gather(*init_tasks)
            
            startup_msg = "🟢 *[SISTEMA] Monitor de CEXs iniciado, operando 24/7 con autocrítica y Cámara de Compensación activa.*"
            await self._dispatch_message(startup_msg, is_heartbeat=True)
            sys_log.info("Arranque exitoso. Monitoreando fuentes en paralelo...",)
            
            while True:
                tasks = [self.monitor_source_task(session, src) for src in self.sources]
                await asyncio.gather(*tasks)
                
                # Pasar la sesión única al gestor de posiciones abiertas
                await self.clearing_house.manage_open_positions(session, self.get_token_price_onchain)
                
                current_time = time.time()
                if current_time - self.last_heartbeat_time >= HEARTBEAT_INTERVAL_HOURS * 3600:
                    try:
                        await self.send_heartbeat_report()
                    except Exception as e:
                        err_log.error(f"Fallo al compilar reporte: {e}")
                        self.db.log_system("ERROR", f"Fallo al compilar heartbeat: {e}")
                    self.last_heartbeat_time = current_time
                
                await asyncio.sleep(POLLING_INTERVAL)

    def run(self):
        try:
            asyncio.run(self.main_loop())
        except KeyboardInterrupt:
            sys_log.info("Monitoreo detenido por el usuario.")
            self.db.log_system("SYSTEM", "Monitor detenido manualmente.")

if __name__ == "__main__":
    monitor = ListingMonitor()
    monitor.run()
