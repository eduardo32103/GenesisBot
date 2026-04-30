from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_MONEY_FLOW_SIGNAL_TYPES = [
    {
        "id": "strong_inflow",
        "label": "Entradas fuertes",
        "decision_use": "Elevar vigilancia cuando precio, volumen y contexto persistido apuntan a demanda superior a lo normal.",
        "required_inputs": ["price_change", "relative_volume", "volume_baseline", "timestamp"],
        "supporting_context": ["related_alerts", "sector_context", "macro_context"],
        "honest_language": ["compatible con entrada fuerte", "probable entrada fuerte", "no concluyente"],
    },
    {
        "id": "strong_outflow",
        "label": "Salidas fuertes",
        "decision_use": "Marcar presion de salida cuando el movimiento combina caida, volumen anomalo y contexto suficiente.",
        "required_inputs": ["price_change", "relative_volume", "volume_baseline", "timestamp"],
        "supporting_context": ["related_alerts", "sector_context", "macro_context"],
        "honest_language": ["compatible con salida fuerte", "probable salida fuerte", "no concluyente"],
    },
    {
        "id": "volume_breakout",
        "label": "Ruptura con volumen",
        "decision_use": "Distinguir rupturas con participacion real de movimientos sin confirmacion de volumen.",
        "required_inputs": ["breakout_reference", "price_change", "relative_volume", "timestamp"],
        "supporting_context": ["price_reference", "alert_validations"],
        "honest_language": ["ruptura con volumen confirmable", "ruptura compatible con volumen", "no concluyente"],
    },
    {
        "id": "price_volume_divergence",
        "label": "Divergencia volumen/precio",
        "decision_use": "Avisar cuando volumen y precio no cuentan la misma historia y la lectura requiere cautela.",
        "required_inputs": ["price_change", "relative_volume", "direction", "timestamp"],
        "supporting_context": ["historical_baseline", "related_alerts"],
        "honest_language": ["divergencia compatible", "confirmacion insuficiente", "no concluyente"],
    },
    {
        "id": "sector_pressure",
        "label": "Presion sectorial",
        "decision_use": "Separar movimiento propio del activo de presion amplia de su sector o grupo comparable.",
        "required_inputs": ["sector_proxy", "asset_move", "sector_move", "timestamp"],
        "supporting_context": ["sector_etf", "peer_group", "macro_context"],
        "honest_language": ["compatible con presion sectorial", "presion sectorial probable", "no concluyente"],
    },
    {
        "id": "risk_on_risk_off",
        "label": "Risk-on / risk-off",
        "decision_use": "Indicar si el movimiento encaja con apetito o rechazo general de riesgo.",
        "required_inputs": ["risk_proxy", "asset_move", "market_context", "timestamp"],
        "supporting_context": ["macro_context", "index_proxy", "safe_haven_proxy"],
        "honest_language": ["compatible con risk-on", "compatible con risk-off", "no concluyente"],
    },
    {
        "id": "rotation",
        "label": "Rotacion",
        "decision_use": "Detectar desplazamiento relativo entre activos, sectores o defensivos/ciclicos sin afirmar causa final.",
        "required_inputs": ["source_group_move", "target_group_move", "relative_strength", "timestamp"],
        "supporting_context": ["sector_context", "index_context", "money_flow_comparison"],
        "honest_language": ["compatible con rotacion", "rotacion probable", "no concluyente"],
    },
    {
        "id": "insufficient_confirmation",
        "label": "Confirmacion insuficiente / no concluyente",
        "decision_use": "Evitar elevar prioridad cuando faltan volumen, precio, contexto o fuente confiable.",
        "required_inputs": ["available_inputs", "missing_inputs", "timestamp"],
        "supporting_context": ["data_source", "reliability_status"],
        "honest_language": ["confirmacion insuficiente", "lectura no concluyente", "faltan datos"],
    },
]

_SOURCE_CATALOG = [
    {
        "id": "fmp_runtime_snapshot",
        "label": "Snapshot FMP persistido",
        "status": "available_if_persisted",
        "purpose": "Reutilizar consumo ya registrado de quote, intraday, EOD o news sin disparar consultas nuevas.",
    },
    {
        "id": "radar_snapshot",
        "label": "Radar / cartera",
        "status": "available",
        "purpose": "Aportar ticker, origen, fuente, referencia y estado actual del activo.",
    },
    {
        "id": "alert_events",
        "label": "Alertas persistidas",
        "status": "available",
        "purpose": "Aportar eventos relacionados y validaciones previas sin recalcular el motor.",
    },
    {
        "id": "macro_activity_snapshot",
        "label": "Macro / actividad persistida",
        "status": "available_if_persisted",
        "purpose": "Aportar contexto macro, actividad o eventos ya guardados por runtime.",
    },
    {
        "id": "sector_or_index_proxy",
        "label": "Proxy sectorial / indice",
        "status": "planned",
        "purpose": "Comparar activo contra sector, ETF o indice cuando exista mapeo confiable.",
    },
]

_HONESTY_RULES = [
    "No afirmar institucionalidad si la fuente solo muestra volumen o precio.",
    "No afirmar causalidad; usar probable, compatible con o no concluyente.",
    "No elevar prioridad si faltan volumen, precio, fuente o contexto minimo.",
    "Separar senal observada de interpretacion causal.",
    "Mostrar datos faltantes como confirmacion insuficiente.",
]


def get_money_flow_signal_model() -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "5.1",
        "name": "Modelo de senales Money Flow",
        "status": "contract_ready_detection_pending",
        "summary": {
            "objective": "Definir senales de flujo de capital para consumo futuro sin detectar todavia movimientos finales.",
            "detection_enabled": False,
            "causality_enabled": False,
            "fmp_live_queries_enabled": False,
            "dashboard_ready": True,
            "bot_ready": True,
        },
        "signal_types": _MONEY_FLOW_SIGNAL_TYPES,
        "source_catalog": _SOURCE_CATALOG,
        "honesty_rules": _HONESTY_RULES,
        "future_consumers": [
            "Fase 5.2 deteccion de movimientos relevantes",
            "Fase 5.3 capa causal probable",
            "Fase 5.4 integracion dashboard",
            "Fase 5.5 integracion bot/Jarvis",
        ],
        "forbidden_claims": [
            "institucionales comprando",
            "institucionales vendiendo",
            "causa confirmada",
            "compra recomendada",
            "venta recomendada",
        ],
    }
