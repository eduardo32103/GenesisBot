from __future__ import annotations

from pathlib import Path
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import persistent_intelligence_status


REPO_ROOT = Path(__file__).resolve().parents[2]
TASK_BOARD = Path("docs/GENESIS_AGENT_TASK_BOARD.md")
PROMPT_DIR = Path("docs/agent_tasks")
SAFETY_FLAGS = {
    "candidate_activated": False,
    "paper_forward_onboarding_started": False,
    "applies_to_real_trading": False,
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}


def run_agent_mission_control(
    *,
    repo_root: str | Path | None = None,
    db_state: dict[str, Any] | None = None,
    runtime_state: dict[str, Any] | None = None,
    research_state: dict[str, Any] | None = None,
    paper_observation_state: dict[str, Any] | None = None,
    load_db_state: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    board = read_agent_task_board(root)
    prompts = discover_agent_prompts(root)
    db = _db_state(db_state, load_db_state=load_db_state)
    runtime = dict(runtime_state or {})
    research = dict(research_state or {})
    paper = dict(paper_observation_state or {})
    blockers = _active_blockers(board, db, runtime, paper)
    safety = {
        "safety_state": "blocked" if blockers else "clear",
        "risk_governor_required": True,
        "capital_protection_required": True,
        "no_broker": True,
        "no_real_trading": True,
        **SAFETY_FLAGS,
    }
    next_tasks = _next_agent_tasks(board, prompts)

    return {
        "status": "agent_mission_control_ready",
        "mission_state": "blocked" if blockers else "ready",
        "current_phase": "xau_m15_paper_shadow_supervision",
        "active_blockers": blockers,
        "urgent_tasks": board.get("urgent", []),
        "blocked_tasks": board.get("blocked", []),
        "next_agent_tasks": next_tasks,
        "next_codex_prompts": prompts,
        "safety_state": safety,
        "db_state": db,
        "runtime_state": runtime,
        "research_state": research,
        "paper_observation_state": paper,
        "recommended_next_action": _recommended_next_action(board, blockers),
        **SAFETY_FLAGS,
    }


def read_agent_task_board(repo_root: str | Path | None = None) -> dict[str, list[str]]:
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    path = root / TASK_BOARD
    sections: dict[str, list[str]] = {
        "urgent": [],
        "active": [],
        "queued": [],
        "blocked": [],
        "done": [],
        "rejected": [],
        "next_recommended_task": [],
    }
    if not path.exists():
        return sections

    current = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            current = _section_key(line[3:])
            sections.setdefault(current, [])
            continue
        if not current or not line:
            continue
        item = _task_item(line)
        if item:
            sections[current].append(item)
    return sections


def discover_agent_prompts(repo_root: str | Path | None = None) -> list[dict[str, str]]:
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    prompt_root = root / PROMPT_DIR
    if not prompt_root.exists():
        return []
    prompts: list[dict[str, str]] = []
    for path in sorted(prompt_root.glob("*.md")):
        title = _first_heading(path)
        prompts.append(
            {
                "agent": title.replace(" Prompt", ""),
                "file": str(path.relative_to(root)).replace("\\", "/"),
                "recommended_use": "copy_prompt_for_scoped_subagent_task",
            }
        )
    return prompts


def _db_state(db_state: dict[str, Any] | None, *, load_db_state: bool) -> dict[str, Any]:
    if db_state is not None:
        return dict(db_state)
    if not load_db_state:
        return {"db_available": False, "db_degraded": True, "tables_ready": False, "reason": "db_state_not_loaded"}
    try:
        return persistent_intelligence_status(write_test_event=False)
    except Exception as exc:
        return {
            "db_available": False,
            "db_degraded": True,
            "tables_ready": False,
            "reason": "persistent_intelligence_status_failed",
            "error": str(exc)[:160],
            **SAFETY_FLAGS,
        }


def _active_blockers(
    board: dict[str, list[str]],
    db_state: dict[str, Any],
    runtime_state: dict[str, Any],
    paper_observation_state: dict[str, Any],
) -> list[str]:
    blockers = list(board.get("blocked", []))
    if db_state.get("db_degraded") is True or db_state.get("tables_ready") is False:
        blockers.append("persistent_intelligence_not_green")
    if runtime_state.get("runtime_context_recent") is False:
        blockers.append("runtime_context_not_recent")
    if paper_observation_state.get("open_shadow_count", 0) not in ("", None, 0):
        blockers.append("existing_paper_shadow_requires_lifecycle_review")
    return _dedupe(blockers)


def _next_agent_tasks(board: dict[str, list[str]], prompts: list[dict[str, str]]) -> list[dict[str, str]]:
    urgent = board.get("urgent", [])
    prompt_by_agent = {prompt["agent"].casefold(): prompt for prompt in prompts}
    tasks: list[dict[str, str]] = []
    for item in urgent[:5]:
        agent_name = item.split(":", 1)[0].strip()
        prompt = prompt_by_agent.get(f"{agent_name} prompt".casefold()) or _prompt_for_agent(prompt_by_agent, agent_name)
        tasks.append(
            {
                "agent": agent_name,
                "task": item,
                "prompt_file": prompt.get("file", "") if prompt else "",
            }
        )
    return tasks


def _prompt_for_agent(prompt_by_agent: dict[str, dict[str, str]], agent_name: str) -> dict[str, str] | None:
    clean = agent_name.casefold()
    for key, prompt in prompt_by_agent.items():
        if clean in key:
            return prompt
    return None


def _recommended_next_action(board: dict[str, list[str]], blockers: list[str]) -> str:
    if blockers:
        return "resolve_active_blockers_before_parallel_work"
    next_items = board.get("next_recommended_task", [])
    return next_items[0] if next_items else "assign_next_subagent_task"


def _section_key(title: str) -> str:
    return title.strip().casefold().replace(" ", "_").replace("/", "_")


def _task_item(line: str) -> str:
    stripped = line.strip()
    if stripped.startswith("- "):
        return stripped[2:].strip()
    if "." in stripped:
        number, rest = stripped.split(".", 1)
        if number.strip().isdigit():
            return rest.strip()
    return stripped if stripped and not stripped.startswith("#") else ""


def _first_heading(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return path.stem


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result
