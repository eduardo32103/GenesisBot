from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_agent_mission_control import discover_agent_prompts, read_agent_task_board, run_agent_mission_control


class MT5AgentMissionControlTests(unittest.TestCase):
    def test_reads_task_board_sections(self) -> None:
        board = read_agent_task_board()

        self.assertIn("Shadow Lifecycle Agent", " ".join(board["urgent"]))
        self.assertIn("rejected", board)
        self.assertTrue(board["next_recommended_task"])

    def test_discovers_agent_prompts(self) -> None:
        prompts = discover_agent_prompts()
        files = {prompt["file"] for prompt in prompts}

        self.assertGreaterEqual(len(prompts), 10)
        self.assertIn("docs/agent_tasks/01_safety_sentinel.md", files)
        self.assertIn("docs/agent_tasks/08_shadow_lifecycle.md", files)

    def test_mission_control_is_read_only_and_safe(self) -> None:
        result = run_agent_mission_control(
            db_state={"db_available": True, "db_degraded": False, "tables_ready": True},
            runtime_state={"runtime_context_recent": True},
            paper_observation_state={"open_shadow_count": 0},
            load_db_state=False,
        )

        self.assertEqual(result["status"], "agent_mission_control_ready")
        self.assertEqual(result["current_phase"], "xau_m15_paper_shadow_supervision")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertEqual(result["safety_state"]["safety_state"], "blocked")

    def test_mission_control_adds_db_blocker_when_db_not_green(self) -> None:
        result = run_agent_mission_control(
            db_state={"db_available": False, "db_degraded": True, "tables_ready": False},
            runtime_state={"runtime_context_recent": True},
            paper_observation_state={"open_shadow_count": 0},
            load_db_state=False,
        )

        self.assertIn("persistent_intelligence_not_green", result["active_blockers"])
        self.assertEqual(result["recommended_next_action"], "resolve_active_blockers_before_parallel_work")

    def test_can_parse_isolated_board_for_agent_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "docs" / "agent_tasks").mkdir(parents=True)
            (root / "docs" / "GENESIS_AGENT_TASK_BOARD.md").write_text(
                "# Board\n\n## Urgent\n\n1. Safety Sentinel Agent: test safety.\n\n## Blocked\n\n- Nothing.\n",
                encoding="utf-8",
            )
            (root / "docs" / "agent_tasks" / "01_safety_sentinel.md").write_text(
                "# Safety Sentinel Agent Prompt\n",
                encoding="utf-8",
            )

            result = run_agent_mission_control(
                repo_root=root,
                db_state={"db_available": True, "db_degraded": False, "tables_ready": True},
                runtime_state={"runtime_context_recent": True},
                paper_observation_state={"open_shadow_count": 0},
                load_db_state=False,
            )

        self.assertEqual(result["urgent_tasks"], ["Safety Sentinel Agent: test safety."])
        self.assertEqual(result["next_agent_tasks"][0]["prompt_file"], "docs/agent_tasks/01_safety_sentinel.md")


if __name__ == "__main__":
    unittest.main()
