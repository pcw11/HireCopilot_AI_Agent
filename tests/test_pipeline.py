"""pipeline.py 단위 테스트 (stdlib only, requests 미사용)."""

import unittest

from pipeline import (
    build_branch_actions,
    check_screening,
    interview_row,
    parse_gpa,
    retry_failed_outbox,
    run_pipeline,
)
from pipeline import OutboxAction, PipelineResult


def _base_payload(**overrides):
    p = {
        "candidate_name": "홍길동",
        "candidate_email": "hong@example.com",
        "position": "개발자",
        "degree": "학사 (4년제)",
        "gpa": "3.5",
        "experience": "1~3년",
        "hiring_opinion": "보류",
        "hiring_recommendation_reason": "기술 검증 필요",
        "summary": "요약",
        "concerns": ["주인의식"],
        "recommended_next_step": "2차 면접",
        "transcript": "면접관: 안녕하세요",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "scores": {"overall": 4.0},
    }
    p.update(overrides)
    return p


class TestParseGpa(unittest.TestCase):
    def test_plain(self):
        self.assertEqual(parse_gpa("3.5"), 3.5)

    def test_fraction_format(self):
        self.assertEqual(parse_gpa("4.2 / 4.5"), 4.2)

    def test_empty(self):
        self.assertIsNone(parse_gpa(""))


class TestScreening(unittest.TestCase):
    def test_pass(self):
        ok, _ = check_screening(_base_payload(), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertTrue(ok)

    def test_fail_email(self):
        ok, reason = check_screening(_base_payload(candidate_email="bad"), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertFalse(ok)
        self.assertIn("이메일", reason)

    def test_fail_gpa_at_threshold(self):
        ok, _ = check_screening(_base_payload(gpa="3.0"), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertFalse(ok)

    def test_fail_missing_gpa_when_required(self):
        ok, reason = check_screening(_base_payload(gpa=""), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertFalse(ok)
        self.assertIn("학점", reason)


class TestBranchActions(unittest.TestCase):
    def test_hold_includes_zoom_and_docs(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="보류"),
            admin_email="admin@test.com",
            admin_slack="U123",
            notion_database="DB",
            followup_questions="1. 질문",
        )
        targets = {a.target for a in actions}
        self.assertIn("outbox_zoom", targets)
        self.assertIn("outbox_docs", targets)
        self.assertIn("outbox_notion", targets)

    def test_reject_sends_email_outbox(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="비추천"),
            admin_email="",
            admin_slack="",
            notion_database="DB",
        )
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].target, "outbox_email")


class TestRunPipeline(unittest.TestCase):
    def test_interview_row_uses_payload_summary(self):
        row = interview_row(_base_payload(summary="최상위 요약", scores={"overall": 4.0, "summary": "잘못된 요약"}))
        self.assertEqual(row[16], "최상위 요약")

    def test_skip_interview_save(self):
        result = run_pipeline(_base_payload(), "", skip_outbox=True, skip_interview_save=True)
        self.assertTrue(result.interview_saved[0])
        self.assertIn("생략", result.interview_saved[1])

    def test_retry_only_failed(self):
        actions = [
            OutboxAction("outbox_email", ["t", "a@b.c", "s", "b", "f"]),
            OutboxAction("pipeline_log", ["t", "n", "b", "p", "d"]),
        ]
        result = PipelineResult(
            interview_saved=(True, "ok"),
            screening_passed=True,
            screening_reason="통과",
            branch="보류",
            actions=actions,
            action_results=[("outbox_email", False, "fail"), ("pipeline_log", True, "ok")],
        )

        calls = []

        def fake_post(payload, url):
            calls.append(payload["target"])
            return True, "ok"

        import pipeline as pl

        orig = pl.post_to_gas
        pl.post_to_gas = fake_post
        try:
            updated = retry_failed_outbox(result, "http://fake")
        finally:
            pl.post_to_gas = orig

        self.assertEqual(calls, ["outbox_email"])
        self.assertTrue(updated.action_results[0][1])

    def test_retry_skips_failed_pipeline_log(self):
        result = PipelineResult(
            interview_saved=(True, "ok"),
            screening_passed=False,
            screening_reason="fail",
            branch="filtered",
            actions=[OutboxAction("pipeline_log", ["t", "n", "b", "p", "d"])],
            action_results=[("pipeline_log", False, "fail")],
        )

        calls = []

        def fake_post(payload, url):
            calls.append(payload["target"])
            return True, "ok"

        import pipeline as pl

        orig = pl.post_to_gas
        pl.post_to_gas = fake_post
        try:
            updated = retry_failed_outbox(result, "http://fake")
        finally:
            pl.post_to_gas = orig

        self.assertEqual(calls, [])
        self.assertFalse(updated.action_results[0][1])


if __name__ == "__main__":
    unittest.main()
