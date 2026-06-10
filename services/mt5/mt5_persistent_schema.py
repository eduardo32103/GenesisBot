from __future__ import annotations

from typing import Any


PERSISTENT_INTELLIGENCE_SCHEMA_VERSION = "2026-06-10.mt5_persistent_intelligence.v1"

REQUIRED_TABLES: tuple[str, ...] = (
    "mt5_profile_state",
    "mt5_profile_performance",
    "mt5_shadow_trades",
    "mt5_decision_events",
    "mt5_risk_events",
    "mt5_strategy_registry",
    "mt5_degradation_registry",
    "mt5_research_rejection_registry",
    "mt5_candidate_rotation_runs",
    "mt5_adaptive_governor_state",
    "mt5_research_lessons",
)

TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "mt5_profile_state": ("symbol", "timeframe", "profile"),
    "mt5_profile_performance": ("symbol", "timeframe", "profile"),
    "mt5_shadow_trades": ("shadow_trade_id",),
    "mt5_strategy_registry": ("symbol", "timeframe", "profile"),
    "mt5_degradation_registry": ("symbol", "timeframe", "profile"),
    "mt5_research_rejection_registry": ("symbol", "timeframe", "family_pattern"),
    "mt5_candidate_rotation_runs": ("run_id",),
}

CREATE_SCHEMA_SQL = r"""
create extension if not exists pgcrypto with schema extensions;

create table if not exists public.mt5_profile_state (
  id uuid primary key default gen_random_uuid(),
  symbol text not null,
  timeframe text not null,
  profile text not null,
  status text not null,
  active boolean not null default false,
  applies_to_paper_shadow boolean not null default false,
  applies_to_real_trading boolean not null default false,
  degradation_reason text not null default '',
  registry_source text not null default '',
  updated_at timestamptz not null default now(),
  unique(symbol, timeframe, profile)
);

create table if not exists public.mt5_profile_performance (
  symbol text not null,
  timeframe text not null,
  profile text not null,
  trades_forward integer not null default 0,
  wins integer not null default 0,
  losses integer not null default 0,
  win_rate double precision not null default 0,
  profit_factor double precision not null default 0,
  expectancy double precision not null default 0,
  max_drawdown double precision not null default 0,
  consecutive_losses integer not null default 0,
  recent_closed integer not null default 0,
  recent_profit_factor double precision not null default 0,
  recent_expectancy double precision not null default 0,
  updated_at timestamptz not null default now(),
  primary key(symbol, timeframe, profile)
);

create table if not exists public.mt5_shadow_trades (
  shadow_trade_id text primary key,
  symbol text not null,
  timeframe text not null default '',
  profile text not null default '',
  side text not null default '',
  entry_price double precision,
  exit_price double precision,
  pnl double precision,
  pnl_pct double precision,
  r_multiple double precision,
  status text not null default '',
  opened_at timestamptz,
  closed_at timestamptz,
  exit_reason text not null default '',
  broker_touched boolean not null default false,
  order_executed boolean not null default false,
  order_policy text not null default 'journal_only_no_broker'
);

create table if not exists public.mt5_decision_events (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null default now(),
  symbol text not null default '',
  timeframe text not null default '',
  decision text not null default 'NO_TRADE',
  reason text not null default '',
  profile text not null default '',
  strategy_score double precision,
  momentum_score double precision,
  trend_score double precision,
  volatility_score double precision,
  risk_state text not null default '',
  risk_allowed boolean not null default false,
  risk_reason text not null default '',
  broker_touched boolean not null default false,
  order_executed boolean not null default false,
  order_policy text not null default 'journal_only_no_broker'
);

create table if not exists public.mt5_risk_events (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null default now(),
  symbol text not null default '',
  timeframe text not null default '',
  risk_state text not null default '',
  allowed boolean not null default false,
  reason text not null default '',
  circuit_breaker text not null default '',
  consecutive_losses integer not null default 0,
  drawdown double precision not null default 0,
  open_shadow_count integer not null default 0,
  recommended_action text not null default ''
);

create table if not exists public.mt5_strategy_registry (
  symbol text not null,
  timeframe text not null,
  profile text not null,
  family text not null default '',
  status text not null default '',
  source text not null default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key(symbol, timeframe, profile)
);

create table if not exists public.mt5_degradation_registry (
  symbol text not null,
  timeframe text not null,
  profile text not null,
  degradation_reason text not null default '',
  degraded_at timestamptz not null default now(),
  applies_to_paper_shadow boolean not null default false,
  applies_to_real_trading boolean not null default false,
  registry_version text not null default '',
  primary key(symbol, timeframe, profile)
);

create table if not exists public.mt5_research_rejection_registry (
  symbol text not null,
  timeframe text not null,
  family_pattern text not null,
  rejection_reason text not null default '',
  rejection_status text not null default '',
  reviewed_at_version text not null default '',
  allow_future_research boolean not null default false,
  allow_manual_override boolean not null default true,
  primary key(symbol, timeframe, family_pattern)
);

create table if not exists public.mt5_candidate_rotation_runs (
  run_id text primary key,
  timestamp timestamptz not null default now(),
  recommendation text not null default '',
  recommended_candidate jsonb not null default '{}'::jsonb,
  candidate_activated boolean not null default false,
  paper_forward_onboarding_started boolean not null default false,
  broker_touched boolean not null default false,
  order_executed boolean not null default false,
  order_policy text not null default 'journal_only_no_broker'
);

create table if not exists public.mt5_adaptive_governor_state (
  timestamp timestamptz primary key default now(),
  global_state text not null default '',
  recommended_next_action text not null default '',
  active_profiles jsonb not null default '[]'::jsonb,
  paused_profiles jsonb not null default '[]'::jsonb,
  degraded_profiles jsonb not null default '[]'::jsonb,
  circuit_breakers jsonb not null default '[]'::jsonb,
  open_shadow_trades integer not null default 0,
  broker_touched boolean not null default false,
  order_executed boolean not null default false,
  order_policy text not null default 'journal_only_no_broker'
);

create table if not exists public.mt5_research_lessons (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null default now(),
  family text not null default '',
  symbol text not null default '',
  timeframe text not null default '',
  lesson_type text not null default '',
  failure_pattern text not null default '',
  summary text not null default '',
  avoid_next jsonb not null default '[]'::jsonb,
  recommended_next_research_phase text not null default ''
);

alter table public.mt5_profile_state enable row level security;
alter table public.mt5_profile_performance enable row level security;
alter table public.mt5_shadow_trades enable row level security;
alter table public.mt5_decision_events enable row level security;
alter table public.mt5_risk_events enable row level security;
alter table public.mt5_strategy_registry enable row level security;
alter table public.mt5_degradation_registry enable row level security;
alter table public.mt5_research_rejection_registry enable row level security;
alter table public.mt5_candidate_rotation_runs enable row level security;
alter table public.mt5_adaptive_governor_state enable row level security;
alter table public.mt5_research_lessons enable row level security;

create index if not exists idx_mt5_profile_state_symbol_timeframe_profile
  on public.mt5_profile_state(symbol, timeframe, profile);
create index if not exists idx_mt5_profile_performance_symbol_timeframe_profile
  on public.mt5_profile_performance(symbol, timeframe, profile);
create index if not exists idx_mt5_shadow_trades_symbol_timeframe_profile
  on public.mt5_shadow_trades(symbol, timeframe, profile);
create index if not exists idx_mt5_shadow_trades_opened_at
  on public.mt5_shadow_trades(opened_at);
create index if not exists idx_mt5_decision_events_symbol_timeframe_profile
  on public.mt5_decision_events(symbol, timeframe, profile);
create index if not exists idx_mt5_decision_events_timestamp
  on public.mt5_decision_events(timestamp);
create index if not exists idx_mt5_risk_events_symbol_timeframe
  on public.mt5_risk_events(symbol, timeframe);
create index if not exists idx_mt5_risk_events_timestamp
  on public.mt5_risk_events(timestamp);
create index if not exists idx_mt5_strategy_registry_symbol_timeframe_profile
  on public.mt5_strategy_registry(symbol, timeframe, profile);
create index if not exists idx_mt5_degradation_registry_symbol_timeframe_profile
  on public.mt5_degradation_registry(symbol, timeframe, profile);
create index if not exists idx_mt5_research_rejection_registry_symbol_timeframe
  on public.mt5_research_rejection_registry(symbol, timeframe);
create index if not exists idx_mt5_candidate_rotation_runs_timestamp
  on public.mt5_candidate_rotation_runs(timestamp);
create index if not exists idx_mt5_adaptive_governor_state_timestamp
  on public.mt5_adaptive_governor_state(timestamp);
create index if not exists idx_mt5_research_lessons_symbol_timeframe
  on public.mt5_research_lessons(symbol, timeframe);
create index if not exists idx_mt5_research_lessons_timestamp
  on public.mt5_research_lessons(timestamp);
"""


def persistent_schema_status() -> dict[str, Any]:
    return {
        "ok": True,
        "schema_version": PERSISTENT_INTELLIGENCE_SCHEMA_VERSION,
        "required_tables": list(REQUIRED_TABLES),
        "table_count": len(REQUIRED_TABLES),
        "ddl_available": True,
        "ddl_applied_by_runtime": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
