from __future__ import annotations

from infra.scheduler.jobs import JOBS


def list_workers() -> tuple[str, ...]:
    return JOBS
