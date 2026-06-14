from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REQUIRED_FLAGS = (
    "broker_touched=false",
    "order_executed=false",
    "order_policy=journal_only_no_broker",
)


class GenesisSubagentSafetyContractTests(unittest.TestCase):
    def test_subagent_operating_system_defines_all_agents(self) -> None:
        text = (ROOT / "docs" / "GENESIS_SUBAGENT_OPERATING_SYSTEM.md").read_text(encoding="utf-8")
        for name in (
            "Coordinator Agent",
            "Safety Sentinel Agent",
            "DB Doctor Agent",
            "Runtime Bridge Agent",
            "Research Factory Agent",
            "Deep Validation Agent",
            "Strategy Tournament Agent",
            "Paper Observation Agent",
            "Shadow Lifecycle Agent",
            "QA / Red Team Agent",
            "Dashboard Reporter Agent",
        ):
            self.assertIn(name, text)
        for flag in REQUIRED_FLAGS:
            self.assertIn(flag, text)

    def test_agent_task_board_contains_current_xau_shadow_blocker(self) -> None:
        text = (ROOT / "docs" / "GENESIS_AGENT_TASK_BOARD.md").read_text(encoding="utf-8")

        self.assertIn("close or monitor the active XAUUSD M15 paper shadow", text)
        self.assertIn("explain the exact `safety_exit` detail", text)
        self.assertIn("Opening a new XAUUSD M15 paper shadow is blocked", text)

    def test_all_agent_prompts_include_safety_flags(self) -> None:
        prompt_dir = ROOT / "docs" / "agent_tasks"
        prompts = sorted(prompt_dir.glob("*.md"))

        self.assertEqual(len(prompts), 10)
        for path in prompts:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for flag in REQUIRED_FLAGS:
                    self.assertIn(flag, text)
                self.assertIn("Forbidden", text)

    def test_handoff_template_requires_validation_and_git_status(self) -> None:
        text = (ROOT / "docs" / "GENESIS_AGENT_HANDOFF_TEMPLATE.md").read_text(encoding="utf-8")

        self.assertIn("Files Touched", text)
        self.assertIn("Safety Flags", text)
        self.assertIn("Git status", text)
        for flag in REQUIRED_FLAGS:
            self.assertIn(flag, text)

    def test_subagent_gate_script_exists_and_prints_safety_flags(self) -> None:
        text = (ROOT / "scripts" / "run_genesis_subagent_gate.ps1").read_text(encoding="utf-8")

        self.assertIn("genesis_subagent_gate=pass", text)
        self.assertIn("Invoke-ForbiddenActivationScan", text)
        for flag in REQUIRED_FLAGS:
            self.assertIn(flag, text)


if __name__ == "__main__":
    unittest.main()
