# Directivas de Desarrollo y Reglas del Proyecto (Workspace-Scoped Rules)

Este archivo define las restricciones de diseño, comportamiento y arquitectura para cualquier agente o desarrollador que trabaje en el repositorio **Monitor Notificador Avanzado**.

---

## 1. Directivas de la Cámara de Compensación y Autoaprendizaje

El sistema debe operar de forma perpetua 24/7 y cumplir estrictamente las tres misiones fundamentales:

### 📢 Misión 1: Señales y Alertas con Acciones Claras
*   Cada alerta de listado, deslistado o anomalía macro debe ir acompañada obligatoriamente de una directiva explícita de acción bursátil y técnica para el operador.
*   **Formato de Acción:** `⚡ [ACCIÓN RECOMENDADA]: COMPRAR ...` o `🛑 [ACCIÓN RECOMENDADA]: APAGAR BOTS ...`
*   No enviar alertas pasivas; el bot debe deducir la instrucción de mercado.

### ⚡ Misión 2: Arbitraje de Alta Frecuencia Local (A Demanda)
*   El bot debe mantener un simulador local de arbitraje de alta velocidad.
*   Al detectar liquidez on-chain, debe comparar de inmediato el precio DEX contra el precio proyectado en CEX. Si el spread es superior al 2.0%, debe ejecutar la orden de arbitraje simétrica (compra/venta) de forma instantánea, registrando las comisiones y guardando el P&L neto en `simulated_trades`.

### 🧠 Misión 3: Trading Autónomo de Mediano Plazo y Autoaprendizaje (Self-Critique)
*   El bot toma decisiones de compra/venta a corto y mediano plazo (acumulación a 3 días) de forma 100% autónoma basadas en el cruce de datos de Dune, Moralis y CryptoQuant.
*   **Autocrítica y Corrección de Parámetros:** Al cerrar cualquier posición, el motor debe evaluar el resultado financiero (PnL).
    *   Si hay pérdidas, el bot debe ejecutar un autoanálisis de error, guardando la crítica en la tabla `performance_critiques`.
    *   Debe reescribir dinámicamente sus propios parámetros operativos (ej: reducir `SHORT_ARB_HOLD_SECONDS` o `MID_TERM_HOLD_SECONDS`) en la tabla `adaptive_parameters` para corregir desviaciones y optimizar la tasa de acierto del algoritmo en ciclos futuros.

---

## 2. Pautas de Arquitectura de Código

1.  **Mantener la Asincronía:** Todas las peticiones HTTP y consultas de base de datos deben ser no-bloqueantes. No utilizar `requests` en hilos de ejecución síncronos; usar siempre `aiohttp` y `asyncio.gather` para paralelizar llamadas externas.
2.  **Aislamiento de Logs:** Mantener los manejadores de logs dedicados y separados:
    *   `system.log`: Depuración técnica general y latencia.
    *   `alerts.log`: Logs de operaciones bursátiles, compras, ventas y autocríticas.
    *   `errors.log`: Registro limpio de fallas y excepciones.
3.  **CI/CD Obligatorio:** Antes de confirmar o empujar (`git push`) cualquier cambio, se debe ejecutar la suite de pruebas unitarias locales:
    ```bash
    python -m unittest test_monitor.py
    ```
    Los cambios que rompan las pruebas existentes no deben ser integrados para evitar fallas en el flujo de trabajo de GitHub Actions.
