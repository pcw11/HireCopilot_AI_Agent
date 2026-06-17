"""admin_store.py unit tests (stdlib only)."""

import json
import os
import tempfile
import unittest

from admin_store import (
    failed_outbox_count,
    fallback_priority_report,
    list_interview_records,
    pipeline_result_from_dict,
    save_interview_record,
    summarize_records,
    update_pipeline_result,
    update_review_status,
)
from pipeline import OutboxAction, PipelineResult


def _payload(**overrides):
    payload = {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "candidate_name": "홍길동",
        "candidate_email": "hong@example.com",
        "position": "개발자",
        "hiring_opinion": "보류",
    }
    payload.update(overrides)
    return payload


def _result(ok=False):
    return PipelineResult(
        interview_saved=(True, "ok"),
        screening_passed=True,
        screening_reason="통과",
        branch="보류",
        actions=[
            OutboxAction("outbox_email", ["t", "a@b.c", "s", "b", "f"]),
            OutboxAction("pipeline_log", ["t", "n", "b", "p", "d"]),
        ],
        action_results=[
            ("outbox_email", ok, "sent" if ok else "fail"),
            ("pipeline_log", True, "ok"),
        ],
    )


class TestAdminStore(unittest.TestCase):
    def test_save_upserts_same_interview(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            first = save_interview_record(_payload(candidate_name="홍길동"), _result(), path=path)
            second = save_interview_record(_payload(candidate_name="김철수"), _result(), path=path)

            records = list_interview_records(path=path)

        self.assertEqual(first["record_id"], second["record_id"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["payload"]["candidate_name"], "김철수")

    def test_pipeline_result_roundtrip_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            record = save_interview_record(_payload(), _result(ok=False), path=path)
            restored = pipeline_result_from_dict(record["pipeline_result"])
            self.assertIsNotNone(restored)
            update_pipeline_result(record["record_id"], _result(ok=True), path=path)
            records = list_interview_records(path=path)

        self.assertEqual(restored.actions[0].target, "outbox_email")
        self.assertEqual(failed_outbox_count(record), 1)
        self.assertEqual(failed_outbox_count(records[0]), 0)
        self.assertEqual(summarize_records(records)["hold"], 1)

    def test_legacy_failed_pipeline_log_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            legacy = {
                "record_id": "legacy",
                "recorded_at": "2026-01-01T00:00:00+00:00",
                "payload": _payload(),
                "pipeline_result": {
                    "interview_saved": [True, "ok"],
                    "screening_passed": True,
                    "screening_reason": "통과",
                    "branch": "보류",
                    "actions": [{"target": "pipeline_log", "row": ["t", "n", "b", "p", "d"]}],
                    "action_results": [["pipeline_log", False, "GAS 웹훅 URL 미설정"]],
                },
                "review": {"status": "not_required", "note": "", "reviewer": "", "updated_at": ""},
            }
            with open(path, "w", encoding="utf-8") as f:
                f.write(json.dumps(legacy, ensure_ascii=False))
                f.write("\n")

            records = list_interview_records(path=path)

        self.assertEqual(failed_outbox_count(records[0]), 0)
        self.assertEqual(
            records[0]["pipeline_result"]["action_results"][0],
            ["pipeline_log", True, "로컬 로그 전용 (GAS 전송 생략)"],
        )

    def test_update_unknown_record_id_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            save_interview_record(_payload(), _result(), path=path)
            updated = update_pipeline_result("없는아이디", _result(ok=True), path=path)
        self.assertIsNone(updated)

    def test_save_with_none_result_keeps_record(self):
        # 파이프라인이 실패해도(결과 None) 면접 스냅샷 자체는 저장되어야 한다.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            record = save_interview_record(_payload(), None, path=path)
            records = list_interview_records(path=path)
        self.assertIsNone(record["pipeline_result"])
        self.assertEqual(len(records), 1)
        self.assertEqual(failed_outbox_count(records[0]), 0)

    def test_list_orders_newest_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            save_interview_record(
                _payload(timestamp="2026-01-01T00:00:00+00:00", candidate_email="a@b.c"),
                _result(),
                path=path,
            )
            save_interview_record(
                _payload(timestamp="2026-02-01T00:00:00+00:00", candidate_email="x@y.z"),
                _result(),
                path=path,
            )
            records = list_interview_records(path=path)
        self.assertEqual(records[0]["payload"]["candidate_email"], "x@y.z")

    def test_recommended_candidate_starts_pending_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            result = PipelineResult(
                interview_saved=(True, "ok"),
                screening_passed=True,
                screening_reason="통과",
                branch="추천",
                actions=[],
                action_results=[],
            )
            record = save_interview_record(
                _payload(hiring_opinion="추천"),
                result,
                path=path,
            )
        self.assertEqual(record["review"]["status"], "pending")

    def test_priority_report_fallback_mentions_top_candidate(self):
        record = {
            "payload": {
                **_payload(candidate_name="김추천", hiring_opinion="추천"),
                "scores": {"overall": 4.7},
                "summary": "요약",
                "hiring_recommendation_reason": "고객 응대 역량 우수",
                "concerns": ["검증 필요"],
            },
            "pipeline_result": {
                "screening_passed": True,
                "screening_reason": "통과",
                "action_results": [],
            },
        }
        report = fallback_priority_report([record])

        self.assertIn("김추천", report)
        self.assertIn("먼저 확인할 후보자", report)

    def test_update_review_status_and_preserve_on_upsert(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "admin.jsonl")
            record = save_interview_record(_payload(hiring_opinion="추천"), _result(), path=path)
            updated = update_review_status(
                record["record_id"],
                "approved_sent",
                note="sent ok",
                reviewer="tester",
                path=path,
            )
            save_interview_record(_payload(hiring_opinion="추천", candidate_name="김철수"), _result(), path=path)
            records = list_interview_records(path=path)

        self.assertIsNotNone(updated)
        self.assertEqual(records[0]["review"]["status"], "approved_sent")
        self.assertEqual(records[0]["review"]["note"], "sent ok")
        self.assertEqual(records[0]["payload"]["candidate_name"], "김철수")


if __name__ == "__main__":
    unittest.main()
