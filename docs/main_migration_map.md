# Main migration map

This file maps the current legacy `main.py` into the new module structure.

## Current status

- Completed in bridge mode:
  - settings and logging bootstrap wired from `main.py`
  - database runtime delegated to `infra/db/connection.py`
  - FMP quote/history/profile/news delegated to `integrations/fmp/client.py`
  - Google News fallback delegated to `integrations/news/client.py`
  - stock analysis + chart send flow delegated to `services/analysis/runtime.py`
  - Telegram polling runtime delegated to `app/telegram/bot.py`

- Still pending as full extraction:
  - pure movement of the remaining business logic out of `main.py`
  - repository adoption in alert and portfolio persistence
  - FastAPI wiring and worker-specific entrypoints

## First extraction wave

- `main.py:275` `get_db_connection`
  - Move to `infra/db/connection.py`
  - Then route all raw queries through repository classes in `infra/db/repositories/`

- `main.py:449` `genesis_strategic_report_v2`
  - Split into:
    - `services/geopolitics/build_geopolitical_report.py`
    - `services/portfolio/get_portfolio_snapshot.py`
    - `services/alerts/evaluate_alerts.py`

- `main.py:676` `save_state_to_telegram`
  - Move delivery and backup concerns to:
    - `infra/storage/backups.py`
    - `integrations/telegram/gateway.py`

- `main.py:747` `restore_state_from_telegram`
  - Move to:
    - `infra/storage/backups.py`
    - `integrations/telegram/gateway.py`

## Alert block

- `main.py:1400` `build_alert_policy_report`
  - Move to `services/alerts/`

- `main.py:1489` `build_alert_strategy_report`
  - Move to `services/alerts/`

- `main.py:1843` `_register_alert_event`
  - Move to `infra/db/repositories/alert_repository.py`

- `main.py:1908` `_send_alert_with_tracking`
  - Split into:
    - alert persistence in `infra/db/repositories/alert_repository.py`
    - channel dispatch in `integrations/telegram/gateway.py`
    - decision logic in `services/alerts/dispatch_alerts.py`

- `main.py:1996` `evaluate_pending_alert_validations`
  - Move to `services/scoring/validate_signal.py`
  - Trigger from `workers/validation_worker.py`

## Analysis and charts

- `main.py:4177` `genesis_strategic_report`
  - Split across:
    - `services/geopolitics/`
    - `services/risk/`
    - `services/portfolio/`

- `main.py:4580` `fetch_and_analyze_stock`
  - Move to `services/analysis/analyze_asset.py`

- `main.py:5694` `_render_stock_analysis_chart`
- `main.py:6213` `_render_stock_analysis_chart_v2`
- `main.py:6673` `_render_stock_analysis_chart_safe`
  - Consolidate into:
    - `services/analysis/build_chart_context.py`
    - `infra/storage/charts.py`

- `main.py:6847` `_perform_deep_analysis_fmp`
- `main.py:7318` `perform_deep_analysis`
  - Move to `services/analysis/analyze_asset.py`
  - Keep market-data fetching in `integrations/fmp/client.py`

## Telegram presentation layer

- `main.py:7528` `/start`
- `main.py:7660` photo handler
- `main.py:8197` text handler
  - Move to:
    - `app/telegram/handlers/start.py`
    - `app/telegram/handlers/analysis.py`
    - `app/telegram/handlers/alerts.py`
    - `app/telegram/handlers/geopolitics.py`
    - `app/telegram/handlers/portfolio.py`

## Workers and runtime

- `main.py:8683` `background_loop_proactivo`
  - Split by concern into:
    - `workers/market_scanner.py`
    - `workers/alert_worker.py`
    - `workers/geopolitics_worker.py`
    - `workers/backup_worker.py`

- `main.py:9381` `main`
  - Reduce to a bootstrap only
  - Final replacement path:
    - `app/bootstrap.py`
    - `app/telegram/bot.py`
    - `workers/runner.py`

## Rule during migration

No new feature should be added directly into `main.py`.
Any new work must land in the new modules first, then be wired temporarily from the legacy file if needed.
