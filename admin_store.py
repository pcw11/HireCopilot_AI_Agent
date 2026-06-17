"""Local admin-console store for interview and pipeline snapshots.

The admin MVP deliberately avoids Google Sheets read credentials. Instead,
`app.py` writes a compact JSONL snapshot after each completed interview, and
`recruiter.py` reads that file for the integrated admin console.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

from pipeline import OutboxAction, PipelineResult

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DEFAULT_STORE_PATH = os.path.join(DATA_DIR, "admin_interviews.jsonl")


def make_record_id(payload: dict) -> str:
    """Create a stable id so one completed interview is not duplicated."""
    seed = "|".join(
        [
            str(payload.get("timestamp") or ""),
            str(payload.get("candidate_email") or ""),
            str(payload.get("position") or ""),
        ]
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def pipeline_result_to_dict(result: PipelineResult | None) -> dict | None:
    if result is None:
        return None
    return {
        "interview_saved": list(result.interview_saved),
        "screening_passed": result.screening_passed,
        "screening_reason": result.screening_reason,
        "branch": result.branch,
        "actions": [
            {"target": action.target, "row": action.row}
            for action in result.actions
        ],
        "action_results": [
            [target, ok, message]
            for target, ok, message in result.action_results
        ],
    }


def pipeline_result_from_dict(data: dict | None) -> PipelineResult | None:
    if not data:
        return None
    interview_saved = data.get("interview_saved") or [False, ""]
    return PipelineResult(
        interview_saved=(bool(interview_saved[0]), str(interview_saved[1])),
        screening_passed=bool(data.get("screening_passed")),
        screening_reason=str(data.get("screening_reason") or ""),
        branch=str(data.get("branch") or ""),
        actions=[
            OutboxAction(str(action.get("target") or ""), list(action.get("row") or []))
            for action in data.get("actions", [])
        ],
        action_results=[
            (str(item[0]), bool(item[1]), str(item[2]))
            for item in data.get("action_results", [])
            if len(item) >= 3
        ],
    )


def build_interview_record(payload: dict, result: PipelineResult | None) -> dict:
    return {
        "record_id": make_record_id(payload),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
        "pipeline_result": pipeline_result_to_dict(result),
        "review": _initial_review_state(payload, result),
    }


def _initial_review_state(payload: dict, result: PipelineResult | None) -> dict:
    needs_review = (
        payload.get("hiring_opinion") == "추천"
        and result is not None
        and result.screening_passed
    )
    return {
        "status": "pending" if needs_review else "not_required",
        "note": "",
        "reviewer": "",
        "updated_at": "",
    }


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                _normalize_pipeline_log_result(obj)
                records.append(obj)
    return records


def _normalize_pipeline_log_result(record: dict[str, Any]) -> None:
    """pipeline_log는 Zap/outbox 대상이 아니므로 과거 실패 기록을 UI에서 성공 처리한다."""
    result = record.get("pipeline_result")
    if not isinstance(result, dict):
        return
    normalized = []
    changed = False
    for item in result.get("action_results") or []:
        if (
            isinstance(item, list)
            and len(item) >= 3
            and item[0] == "pipeline_log"
            and item[1] is False
        ):
            normalized.append(["pipeline_log", True, "로컬 로그 전용 (GAS 전송 생략)"])
            changed = True
        else:
            normalized.append(item)
    if changed:
        result["action_results"] = normalized


def _write_jsonl(records: list[dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
    os.replace(tmp_path, path)


def save_interview_record(
    payload: dict,
    result: PipelineResult | None,
    *,
    path: str = DEFAULT_STORE_PATH,
) -> dict:
    """Upsert a completed interview snapshot and return the saved record."""
    record = build_interview_record(payload, result)
    records = _read_jsonl(path)
    replaced = False
    for i, existing in enumerate(records):
        if existing.get("record_id") == record["record_id"]:
            if existing.get("review"):
                record["review"] = existing["review"]
            records[i] = record
            replaced = True
            break
    if not replaced:
        records.append(record)
    _write_jsonl(records, path)
    return record


def list_interview_records(
    *,
    limit: int | None = None,
    newest_first: bool = True,
    path: str = DEFAULT_STORE_PATH,
) -> list[dict]:
    records = _read_jsonl(path)
    records.sort(
        key=lambda record: (
            record.get("payload", {}).get("timestamp")
            or record.get("recorded_at")
            or ""
        ),
        reverse=newest_first,
    )
    if limit is not None:
        return records[:limit]
    return records


def update_pipeline_result(
    record_id: str,
    result: PipelineResult,
    *,
    path: str = DEFAULT_STORE_PATH,
) -> dict | None:
    records = _read_jsonl(path)
    for record in records:
        if record.get("record_id") == record_id:
            record["recorded_at"] = datetime.now(timezone.utc).isoformat()
            record["pipeline_result"] = pipeline_result_to_dict(result)
            _write_jsonl(records, path)
            return record
    return None


def update_review_status(
    record_id: str,
    status: str,
    *,
    note: str = "",
    reviewer: str = "admin",
    path: str = DEFAULT_STORE_PATH,
) -> dict | None:
    records = _read_jsonl(path)
    for record in records:
        if record.get("record_id") == record_id:
            record["recorded_at"] = datetime.now(timezone.utc).isoformat()
            record["review"] = {
                "status": status,
                "note": note,
                "reviewer": reviewer,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_jsonl(records, path)
            return record
    return None


def failed_outbox_count(record: dict) -> int:
    result = record.get("pipeline_result") or {}
    count = 0
    for item in result.get("action_results", []):
        if len(item) >= 2 and item[0] != "pipeline_log" and not item[1]:
            count += 1
    return count


def candidate_priority_key(record: dict) -> tuple[int, float]:
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    opinion = payload.get("hiring_opinion")
    scores = payload.get("scores") or {}
    try:
        overall = float(scores.get("overall") or 0)
    except (TypeError, ValueError):
        overall = 0.0
    if failed_outbox_count(record):
        priority = 0
    elif opinion == "추천" and result.get("screening_passed"):
        priority = 1
    elif opinion == "보류" and result.get("screening_passed"):
        priority = 2
    elif not result.get("screening_passed"):
        priority = 4
    else:
        priority = 3
    return priority, -overall


def candidate_action_label(record: dict) -> str:
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    opinion = payload.get("hiring_opinion")
    if failed_outbox_count(record):
        return "알림 재전송 필요"
    if not result.get("screening_passed"):
        return "자격 미달 확인"
    if opinion == "추천":
        return "예약 메일 확인"
    if opinion == "보류":
        return "2차 면접 검토"
    if opinion == "비추천":
        return "결과 안내 확인"
    return "상세 확인"


def candidate_brief(record: dict) -> dict:
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    scores = payload.get("scores") or {}
    return {
        "name": payload.get("candidate_name", ""),
        "email": payload.get("candidate_email", ""),
        "position": payload.get("position", ""),
        "opinion": payload.get("hiring_opinion", ""),
        "overall": scores.get("overall", ""),
        "screening": "pass" if result.get("screening_passed") else "fail",
        "screening_reason": result.get("screening_reason", ""),
        "summary": payload.get("summary", ""),
        "reason": payload.get("hiring_recommendation_reason", ""),
        "strengths": payload.get("strengths") or [],
        "concerns": payload.get("concerns") or [],
        "next_step": payload.get("recommended_next_step", ""),
        "action": candidate_action_label(record),
    }


def fallback_priority_report(records: list[dict]) -> str:
    if not records:
        return "면접 기록이 없어 리포트를 생성할 수 없습니다."

    ranked = sorted(records, key=candidate_priority_key)
    lines = [
        "## AI 후보자 우선순위 리포트",
        "",
        "OpenAI 호출 없이 저장된 평가 점수와 채용 의견을 기준으로 만든 규칙 기반 리포트입니다.",
        "",
        "### 먼저 확인할 후보자",
    ]
    for idx, record in enumerate(ranked[:5], start=1):
        brief = candidate_brief(record)
        lines.append(
            f"{idx}. **{brief['name'] or '이름 없음'}** · {brief['position'] or '-'} · "
            f"{brief['opinion'] or '-'} · 점수 {brief['overall'] or '-'}"
        )
        lines.append(f"   - 처리: {brief['action']}")
        if brief["reason"]:
            lines.append(f"   - 판단 이유: {brief['reason']}")
        if brief["concerns"]:
            lines.append(f"   - 확인 필요: {', '.join(str(c) for c in brief['concerns'][:3])}")

    lines.extend(["", "### 운영 메모"])
    failed = [r for r in records if failed_outbox_count(r)]
    holds = [r for r in records if (r.get("payload") or {}).get("hiring_opinion") == "보류"]
    recommended = [r for r in records if (r.get("payload") or {}).get("hiring_opinion") == "추천"]
    lines.append(f"- 추천 후보 {len(recommended)}명, 보류 후보 {len(holds)}명입니다.")
    lines.append(f"- 알림 재전송이 필요한 기록은 {len(failed)}건입니다.")
    lines.append("- 보류 후보는 2차 면접 질문과 Zoom 생성 여부를 함께 확인하세요.")
    return "\n".join(lines)


def summarize_records(records: list[dict]) -> dict:
    summary = {
        "total": len(records),
        "recommended": 0,
        "hold": 0,
        "rejected": 0,
        "screening_passed": 0,
        "screening_failed": 0,
        "failed_outbox": 0,
    }
    for record in records:
        payload = record.get("payload") or {}
        result = record.get("pipeline_result") or {}
        opinion = payload.get("hiring_opinion")
        if opinion == "추천":
            summary["recommended"] += 1
        elif opinion == "보류":
            summary["hold"] += 1
        elif opinion == "비추천":
            summary["rejected"] += 1

        if result.get("screening_passed"):
            summary["screening_passed"] += 1
        else:
            summary["screening_failed"] += 1
        summary["failed_outbox"] += failed_outbox_count(record)
    return summary
