"""admin_store.py unit tests (stdlib only)."""

import os
import tempfile
import unittest

from admin_store import (
    failed_outbox_count,
    list_interview_records,
    pipeline_result_from_dict,
    save_interview_record,
    summarize_records,
    update_pipeline_result,
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


if __name__ == "__main__":
    unittest.main()
