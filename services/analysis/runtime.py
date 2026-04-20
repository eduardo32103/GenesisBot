from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass
class StockAnalysisRuntimeHooks:
    remap_ticker: Callable[[str], str]
    normalize_chart_timeframe: Callable[[str], str]
    perform_deep_analysis: Callable[..., str]
    render_stock_analysis_chart_safe: Callable[..., tuple[str | None, str | None]]
    send_message: Callable[..., Any]
    send_photo: Callable[..., Any]
    strip_html_for_telegram: Callable[[str], str]
    make_card: Callable[[str, list[str], str | None], str]
    get_display_name: Callable[[str], str]
    last_known_analysis: Mapping[str, Any]


def send_stock_analysis_with_chart(
    chat_id: str | int,
    ticker: str,
    timeframe: str = "1D",
    *,
    hooks: StockAnalysisRuntimeHooks,
    logger: logging.Logger | None = None,
) -> None:
    logger = logger or logging.getLogger("genesis.analysis.runtime")

    tk = hooks.remap_ticker(ticker)
    tf = hooks.normalize_chart_timeframe(timeframe)
    analysis_text = None

    try:
        analysis_text = hooks.perform_deep_analysis(tk, timeframe=tf)
    except Exception:
        logger.exception("Error generando analisis textual para %s", tk)
        analysis_text = hooks.make_card(
            f"ANALISIS FMP | {hooks.get_display_name(tk)}",
            [
                "El analisis textual profundo fallo en esta ejecucion.",
                "Active el modo de contingencia para no bloquear la grafica.",
                "Reintenta en unos segundos si quieres refrescar el contexto completo.",
            ],
            None,
        )

    if analysis_text:
        try:
            hooks.send_message(chat_id, analysis_text, parse_mode="HTML")
        except Exception:
            logger.exception("Error enviando analisis textual para %s", tk)
            try:
                hooks.send_message(chat_id, hooks.strip_html_for_telegram(analysis_text))
            except Exception:
                logger.exception("Error enviando analisis textual plano para %s", tk)

    chart_path = None
    chart_caption = None
    try:
        chart_path, chart_caption = hooks.render_stock_analysis_chart_safe(
            tk,
            hooks.last_known_analysis.get(tk),
            timeframe=tf,
        )
    except Exception:
        logger.exception("Error generando grafico tactico para %s", tk)
        hooks.send_message(
            chat_id,
            hooks.make_card(
                "GRAFICO TACTICO",
                ["No pude generar el grafico visual en este momento, pero el analisis textual si quedo listo."],
                None,
            ),
            parse_mode="HTML",
        )
        return

    if not chart_path or not os.path.exists(chart_path):
        hooks.send_message(
            chat_id,
            hooks.make_card(
                "GRAFICO TACTICO",
                ["No encontre un archivo de grafico valido para enviarlo, aunque el analisis textual si quedo listo."],
                None,
            ),
            parse_mode="HTML",
        )
        return

    try:
        with open(chart_path, "rb") as chart_file:
            hooks.send_photo(chat_id, chart_file, caption=chart_caption, parse_mode="HTML")
    except Exception:
        logger.exception("Error enviando grafico tactico para %s", tk)
        hooks.send_message(
            chat_id,
            hooks.make_card(
                "GRAFICO TACTICO",
                ["El grafico se genero, pero Telegram no permitio enviarlo en este intento."],
                None,
            ),
            parse_mode="HTML",
        )
