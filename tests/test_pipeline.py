"""pipeline.py 단위 테스트 (stdlib only, 네트워크 미사용)."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta

from pipeline import (
    _next_day_2pm_kst_iso,
    build_branch_actions,
    check_screening,
    interview_row,
    load_pipeline_config,
    parse_gpa,
    post_to_gas,
    retry_failed_outbox,
    run_pipeline,
)
from pipeline import KST, OutboxAction, PipelineResult

TEST_CONFIG = {
    "webhook_url": "http://fake",
    "admin_email": "admin@test.com",
    "admin_slack": "U123",
    "notion_database": "DB",
    "min_gpa": 3.0,
    "require_gpa": True,
    "block_newgrad": False,
}


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

    def test_comma_decimal(self):
        self.assertEqual(parse_gpa("3,5"), 3.5)

    def test_no_number(self):
        self.assertIsNone(parse_gpa("좋음"))

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

    def test_pass_missing_gpa_when_not_required(self):
        ok, _ = check_screening(_base_payload(gpa=""), {"min_gpa": 3.0, "require_gpa": False, "block_newgrad": False})
        self.assertTrue(ok)

    def test_fail_no_degree(self):
        ok, reason = check_screening(_base_payload(degree="없음"), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertFalse(ok)
        self.assertIn("학위", reason)

    def test_fail_no_experience(self):
        ok, reason = check_screening(_base_payload(experience=""), {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False})
        self.assertFalse(ok)
        self.assertIn("경력", reason)

    def test_block_newgrad(self):
        ok, reason = check_screening(
            _base_payload(experience="신입 (경력 없음)"),
            {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": True},
        )
        self.assertFalse(ok)
        self.assertIn("신입", reason)

    def test_allow_newgrad_by_default(self):
        ok, _ = check_screening(
            _base_payload(experience="신입 (경력 없음)"),
            {"min_gpa": 3.0, "require_gpa": True, "block_newgrad": False},
        )
        self.assertTrue(ok)


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

    def test_hold_without_followup_skips_docs(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="보류"),
            admin_email="admin@test.com",
            admin_slack="U123",
            notion_database="DB",
            followup_questions=None,
        )
        targets = {a.target for a in actions}
        self.assertNotIn("outbox_docs", targets)
        self.assertIn("outbox_zoom", targets)

    def test_recommend_includes_notion_scheduled_and_admin_alerts(self):
        branch, actions = build_branch_actions(
            _base_payload(hiring_opinion="추천"),
            admin_email="admin@test.com",
            admin_slack="U123",
            notion_database="DB",
        )
        targets = [a.target for a in actions]
        self.assertEqual(branch, "추천")
        self.assertEqual(
            targets,
            ["outbox_notion", "outbox_scheduled", "outbox_slack", "outbox_email"],
        )

    def test_recommend_scheduled_row_targets_candidate(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="추천"),
            admin_email="",
            admin_slack="",
            notion_database="DB",
        )
        scheduled = next(a for a in actions if a.target == "outbox_scheduled")
        # row: timestamp | send_after_iso | to | subject | body | candidate_name
        self.assertEqual(scheduled.row[2], "hong@example.com")
        self.assertIn("합격", scheduled.row[3])
        self.assertEqual(scheduled.row[5], "홍길동")
        send_after = datetime.fromisoformat(scheduled.row[1])
        self.assertEqual((send_after.hour, send_after.minute), (14, 0))
        self.assertGreater(send_after, datetime.now(KST))

    def test_recommend_without_email_skips_scheduled(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="추천", candidate_email=""),
            admin_email="",
            admin_slack="",
            notion_database="DB",
        )
        targets = {a.target for a in actions}
        self.assertNotIn("outbox_scheduled", targets)

    def test_reject_sends_email_outbox(self):
        _, actions = build_branch_actions(
            _base_payload(hiring_opinion="비추천"),
            admin_email="",
            admin_slack="",
            notion_database="DB",
        )
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].target, "outbox_email")

    def test_unknown_opinion_creates_no_actions(self):
        branch, actions = build_branch_actions(
            _base_payload(hiring_opinion=""),
            admin_email="admin@test.com",
            admin_slack="U123",
            notion_database="DB",
        )
        self.assertEqual(branch, "unknown")
        self.assertEqual(actions, [])


class TestLoadPipelineConfigOverrides(unittest.TestCase):
    """관리자 콘솔이 저장한 recruiter_config.json "pipeline" 섹션이 .env보다 우선."""

    def test_recruiter_config_overrides_env_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "recruiter_config.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {"pipeline": {"min_gpa": 3.5, "require_gpa": False, "block_newgrad": True}},
                    f,
                )
            cfg = load_pipeline_config(config_path=path)
        self.assertEqual(cfg["min_gpa"], 3.5)
        self.assertFalse(cfg["require_gpa"])
        self.assertTrue(cfg["block_newgrad"])

    def test_missing_file_falls_back_to_defaults(self):
        cfg = load_pipeline_config(config_path=os.path.join("없는경로", "없는파일.json"))
        self.assertIsInstance(cfg["min_gpa"], float)
        self.assertIsInstance(cfg["require_gpa"], bool)

    def test_invalid_override_values_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "recruiter_config.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"pipeline": {"min_gpa": "숫자아님"}}, f)
            cfg = load_pipeline_config(config_path=path)
        self.assertIsInstance(cfg["min_gpa"], float)

    def test_partial_override_keeps_other_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "recruiter_config.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"pipeline": {"min_gpa": 2.5}}, f)
            cfg = load_pipeline_config(config_path=path)
        self.assertEqual(cfg["min_gpa"], 2.5)
        self.assertIsInstance(cfg["require_gpa"], bool)
        self.assertIsInstance(cfg["block_newgrad"], bool)


class TestScheduledTime(unittest.TestCase):
    def test_next_day_2pm_kst(self):
        iso = _next_day_2pm_kst_iso()
        dt = datetime.fromisoformat(iso)
        self.assertEqual((dt.hour, dt.minute, dt.second), (14, 0, 0))
        self.assertEqual(dt.utcoffset(), timedelta(hours=9))
        self.assertEqual(dt.date(), (datetime.now(KST) + timedelta(days=1)).date())


class _FakeResponse:
    def __init__(self, status_code, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class TestPostToGas(unittest.TestCase):
    def test_missing_url(self):
        ok, msg = post_to_gas({"target": "x", "row": []}, "")
        self.assertFalse(ok)
        self.assertIn("URL", msg)

    def _with_fake_requests(self, fake):
        import pipeline as pl

        orig = pl.requests
        pl.requests = fake
        return orig

    def test_302_redirect_followed_with_get(self):
        import pipeline as pl

        calls = {}

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def post(url, json=None, timeout=None, allow_redirects=None):
                calls["post"] = url
                return _FakeResponse(302, headers={"Location": "http://redirected"})

            @staticmethod
            def get(url, timeout=None, allow_redirects=None):
                calls["get"] = url
                return _FakeResponse(200)

        orig = self._with_fake_requests(FakeRequests)
        try:
            ok, msg = pl.post_to_gas({"target": "x", "row": []}, "http://gas")
        finally:
            pl.requests = orig

        self.assertTrue(ok)
        self.assertEqual(calls["post"], "http://gas")
        self.assertEqual(calls["get"], "http://redirected")

    def test_http_error_reported(self):
        import pipeline as pl

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def post(url, json=None, timeout=None, allow_redirects=None):
                return _FakeResponse(500, text="boom")

        orig = self._with_fake_requests(FakeRequests)
        try:
            ok, msg = pl.post_to_gas({"target": "x", "row": []}, "http://gas")
        finally:
            pl.requests = orig

        self.assertFalse(ok)
        self.assertIn("500", msg)

    def test_gas_json_error_reported(self):
        import pipeline as pl

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def post(url, json=None, timeout=None, allow_redirects=None):
                return _FakeResponse(200, text='{"result":"error","message":"mail quota exceeded"}')

        orig = self._with_fake_requests(FakeRequests)
        try:
            ok, msg = pl.post_to_gas({"target": "send_email_now", "row": []}, "http://gas")
        finally:
            pl.requests = orig

        self.assertFalse(ok)
        self.assertIn("mail quota exceeded", msg)

    def test_send_email_now_requires_sent_confirmation(self):
        import pipeline as pl

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def post(url, json=None, timeout=None, allow_redirects=None):
                return _FakeResponse(200, text='{"result":"success","target":"send_email_now"}')

        orig = self._with_fake_requests(FakeRequests)
        try:
            ok, msg = pl.post_to_gas({"target": "send_email_now", "row": []}, "http://gas")
        finally:
            pl.requests = orig

        self.assertFalse(ok)
        self.assertIn("새 배포", msg)

    def test_send_email_now_success_requires_sent_true(self):
        import pipeline as pl

        class FakeRequests:
            RequestException = Exception

            @staticmethod
            def post(url, json=None, timeout=None, allow_redirects=None):
                return _FakeResponse(200, text='{"result":"success","target":"send_email_now","sent":true}')

        orig = self._with_fake_requests(FakeRequests)
        try:
            ok, msg = pl.post_to_gas({"target": "send_email_now", "row": []}, "http://gas")
        finally:
            pl.requests = orig

        self.assertTrue(ok)
        self.assertIn("즉시 메일", msg)


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


class TestPipelineEndToEnd(unittest.TestCase):
    """README 역할 2 검증 항목: payload 하나를 넣었을 때
    분기별 action 목록 / 전송 결과 / 재전송 결과가 기대대로 나오는지."""

    def _run(self, payload, llm_fn=None, fail_targets=(), skip_interview_save=False):
        import pipeline as pl

        calls = []

        def fake_post(p, url):
            calls.append(p)
            if p["target"] in fail_targets:
                return False, "fail"
            return True, "ok"

        orig = pl.post_to_gas
        pl.post_to_gas = fake_post
        try:
            result = run_pipeline(
                payload,
                config=TEST_CONFIG,
                llm_fn=llm_fn,
                skip_interview_save=skip_interview_save,
            )
        finally:
            pl.post_to_gas = orig
        return result, calls

    def test_recommend_flow(self):
        result, calls = self._run(_base_payload(hiring_opinion="추천"))
        self.assertTrue(result.screening_passed)
        self.assertEqual(result.branch, "추천")
        self.assertEqual(
            [c["target"] for c in calls],
            ["interviews", "outbox_notion", "outbox_scheduled", "outbox_slack", "outbox_email", "pipeline_log"],
        )
        self.assertTrue(result.outbox_actions_ok)

    def test_hold_flow_includes_followup_docs(self):
        result, calls = self._run(
            _base_payload(hiring_opinion="보류"),
            llm_fn=lambda prompt: "1. 맞춤 질문",
        )
        self.assertEqual(result.branch, "보류")
        targets = [c["target"] for c in calls]
        for expected in ("outbox_slack", "outbox_email", "outbox_notion", "outbox_zoom", "outbox_docs"):
            self.assertIn(expected, targets)
        docs = next(c for c in calls if c["target"] == "outbox_docs")
        self.assertIn("1. 맞춤 질문", docs["row"][3])

    def test_hold_flow_regenerates_followup_on_resend(self):
        # 재전송(skip_interview_save) 경로에서도 llm_fn이 전달되면 docs가 생성되어야 한다.
        result, calls = self._run(
            _base_payload(hiring_opinion="보류"),
            llm_fn=lambda prompt: "1. 재생성 질문",
            skip_interview_save=True,
        )
        targets = [c["target"] for c in calls]
        self.assertNotIn("interviews", targets)
        self.assertIn("outbox_docs", targets)
        self.assertTrue(result.interview_saved[0])

    def test_reject_flow(self):
        result, calls = self._run(_base_payload(hiring_opinion="비추천"))
        self.assertEqual(result.branch, "비추천")
        self.assertEqual(
            [c["target"] for c in calls],
            ["interviews", "outbox_email", "pipeline_log"],
        )
        email = next(c for c in calls if c["target"] == "outbox_email")
        self.assertEqual(email["row"][1], "hong@example.com")

    def test_screening_fail_skips_outbox(self):
        result, calls = self._run(_base_payload(gpa="2.0", hiring_opinion="추천"))
        self.assertFalse(result.screening_passed)
        self.assertEqual(result.branch, "filtered")
        self.assertEqual([c["target"] for c in calls], ["interviews", "pipeline_log"])

    def test_retry_resends_only_failed_outbox(self):
        result, _ = self._run(
            _base_payload(hiring_opinion="추천"),
            fail_targets=("outbox_email",),
        )
        self.assertFalse(result.outbox_actions_ok)

        import pipeline as pl

        retry_calls = []

        def fake_post_ok(p, url):
            retry_calls.append(p["target"])
            return True, "ok"

        orig = pl.post_to_gas
        pl.post_to_gas = fake_post_ok
        try:
            updated = retry_failed_outbox(result, "http://fake")
        finally:
            pl.post_to_gas = orig

        self.assertEqual(retry_calls, ["outbox_email"])
        self.assertTrue(updated.outbox_actions_ok)


if __name__ == "__main__":
    unittest.main()
