"""
면접 종료 후 2차 채용 파이프라인 (코드 중심).

흐름:
  1. interviews 시트에 면접 결과 저장 (GAS webhook_router.gs)
  2. Python에서 자격 필터 + hiring_opinion 분기
  3. outbox_* 시트에 행 기록
  4. 전용 Zapier ZAP: outbox 시트 New Row → Gmail / Slack / Notion / Docs / Zoom

outbox 시트 스키마 (GAS가 탭 없으면 헤더와 함께 자동 생성):

  interviews:       A~S 면접 DB (19열, 아래 interview_row 참고)
  outbox_email:     timestamp | to | subject | body | from_name
  outbox_slack:     timestamp | recipient | message
  outbox_notion:    timestamp | name | database | notes
  outbox_docs:      timestamp | candidate_name | candidate_email | content
  outbox_zoom:      timestamp | topic | start_time_iso | duration_min | candidate_name
  outbox_scheduled: timestamp | send_after_iso | to | subject | body | candidate_name
  pipeline_log:     timestamp | candidate_name | branch | screening | detail

outbox_scheduled (HITL 최종 합격):
  추천 분기에서 최종 합격 안내 메일을 예약 큐에 넣는다. 전용 Zap이
  New Row → Delay Until(send_after_iso) → Notion에서 후보 검색 →
  관리자가 체크박스를 켠 경우에만 → Gmail 발송 순으로 처리한다.
  즉 사람(관리자)의 Notion 체크가 최종 발송 게이트다.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable
from zoneinfo import ZoneInfo

import requests

KST = ZoneInfo("Asia/Seoul")

RECRUITER_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "recruiter_config.json"
)

FOLLOWUP_PROMPT = """당신은 채용 담당자를 돕는 HR 어시스턴트입니다.
아래 1차 AI 면접 정보를 바탕으로 2차 면접에서 사용할 맞춤형 질문 5~7개를 작성하세요.
각 질문은 번호 목록으로, 한국어로, 구체적이고 행동 기반(STAR)으로 작성하세요.

지원 포지션: {position}
채용 의견: {hiring_opinion}
추천/보류 사유: {hiring_recommendation_reason}
요약: {summary}
우려 사항: {concerns}
추천 다음 단계: {recommended_next_step}

--- 1차 면접 대화록 ---
{transcript}
"""


@dataclass
class OutboxAction:
    target: str
    row: list

    def to_payload(self) -> dict:
        return {"target": self.target, "row": self.row}


@dataclass
class PipelineResult:
    interview_saved: tuple[bool, str]
    screening_passed: bool
    screening_reason: str
    branch: str
    actions: list[OutboxAction] = field(default_factory=list)
    action_results: list[tuple[str, bool, str]] = field(default_factory=list)

    @property
    def outbox_actions_ok(self) -> bool:
        """pipeline_log 제외 outbox 전송 성공 여부."""
        outbox = [(t, ok, m) for t, ok, m in self.action_results if t != "pipeline_log"]
        return bool(outbox) and all(ok for _, ok, _ in outbox)

    @property
    def has_outbox_actions(self) -> bool:
        return any(a.target != "pipeline_log" for a in self.actions)


def _load_recruiter_filter_overrides(path: str) -> dict:
    """recruiter_config.json의 "pipeline" 섹션(관리자 콘솔에서 저장)을 읽는다."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    overrides = data.get("pipeline")
    return overrides if isinstance(overrides, dict) else {}


def load_pipeline_config(config_path: str | None = None) -> dict:
    """파이프라인 설정 로드.

    자격 필터(min_gpa / require_gpa / block_newgrad)는
    관리자 콘솔이 저장하는 recruiter_config.json "pipeline" 섹션이
    .env 값보다 우선한다.
    """
    min_gpa = os.getenv("PIPELINE_MIN_GPA", "3.0").strip()
    try:
        min_gpa_f = float(min_gpa)
    except ValueError:
        min_gpa_f = 3.0

    require_gpa = os.getenv("PIPELINE_REQUIRE_GPA", "true").strip().lower() not in (
        "0",
        "false",
        "no",
    )
    block_newgrad = os.getenv("PIPELINE_BLOCK_NEWGRAD", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    overrides = _load_recruiter_filter_overrides(config_path or RECRUITER_CONFIG_PATH)
    if "min_gpa" in overrides:
        try:
            min_gpa_f = float(overrides["min_gpa"])
        except (TypeError, ValueError):
            pass
    if "require_gpa" in overrides:
        require_gpa = bool(overrides["require_gpa"])
    if "block_newgrad" in overrides:
        block_newgrad = bool(overrides["block_newgrad"])

    return {
        "webhook_url": (
            os.getenv("GAS_WEBHOOK_URL", "").strip()
            or os.getenv("ZAPIER_WEBHOOK_URL", "").strip()
        ),
        "admin_email": os.getenv("ADMIN_EMAIL", "").strip(),
        "admin_slack": os.getenv("ADMIN_SLACK_USER_ID", "").strip(),
        "notion_database": os.getenv("NOTION_DATABASE_LABEL", "2026 보류 합격자 목록").strip(),
        "min_gpa": min_gpa_f,
        "require_gpa": require_gpa,
        "block_newgrad": block_newgrad,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_day_2pm_kst_iso() -> str:
    now = datetime.now(KST)
    target = (now + timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
    return target.isoformat()


def parse_gpa(gpa_str: str) -> float | None:
    if not gpa_str or not str(gpa_str).strip():
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", str(gpa_str).replace(",", "."))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def check_screening(payload: dict, config: dict | None = None) -> tuple[bool, str]:
    cfg = config or load_pipeline_config()
    min_gpa = cfg["min_gpa"]
    require_gpa = cfg["require_gpa"]
    block_newgrad = cfg["block_newgrad"]

    email = (payload.get("candidate_email") or "").strip()
    if "@" not in email:
        return False, "이메일 형식 불량 (@ 없음)"

    gpa_raw = (payload.get("gpa") or "").strip()
    gpa = parse_gpa(gpa_raw)
    if gpa_raw:
        if gpa is None:
            return False, f"학점 파싱 불가 (입력: {gpa_raw})"
        if gpa <= min_gpa:
            return False, f"학점 {min_gpa} 초과 필요 (입력: {gpa})"
    elif require_gpa:
        return False, "학점 미입력 (파이프라인 필수 조건)"

    degree = (payload.get("degree") or "").strip()
    if not degree or degree.lower() in ("없음", "none"):
        return False, "학위 없음"

    experience = (payload.get("experience") or "").strip()
    if not experience or experience.lower() in ("없음", "none"):
        return False, "경력 없음"
    if block_newgrad and "경력 없음" in experience:
        return False, "신입 제외 (PIPELINE_BLOCK_NEWGRAD=true)"

    return True, "통과"


def post_to_gas(payload: dict, webhook_url: str) -> tuple[bool, str]:
    if not webhook_url:
        return False, "GAS 웹훅 URL이 설정되지 않았습니다."
    try:
        r = requests.post(webhook_url, json=payload, timeout=15, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308):
            redirect_url = r.headers.get("Location")
            if redirect_url:
                r = requests.get(redirect_url, timeout=15, allow_redirects=True)
        if 200 <= r.status_code < 300:
            try:
                data = json.loads(r.text) if r.text else {}
            except json.JSONDecodeError:
                data = {}

            if isinstance(data, dict) and data.get("result") == "error":
                return False, f"GAS 오류: {data.get('message', r.text[:300])}"

            if payload.get("target") == "send_email_now":
                if isinstance(data, dict) and data.get("sent") is True:
                    return True, f"즉시 메일 발송 성공 (HTTP {r.status_code})"
                return (
                    False,
                    "GAS가 즉시 발송 완료(sent=true)를 반환하지 않았습니다. "
                    "Apps Script에 최신 webhook_router.gs를 붙여넣고 새 배포가 필요합니다.",
                )

            return True, f"전송 성공 (HTTP {r.status_code})"
        return False, f"HTTP {r.status_code}: {r.text[:300]}"
    except requests.RequestException as e:
        return False, f"네트워크 오류: {e}"


def interview_row(payload: dict) -> list:
    scores = payload.get("scores") or {}
    return [
        payload.get("timestamp") or "",
        payload.get("candidate_name") or "",
        payload.get("candidate_email") or "",
        payload.get("position") or "",
        payload.get("degree") or "",
        payload.get("gpa") or "",
        payload.get("experience") or "",
        payload.get("fit_level") or "",
        payload.get("hiring_opinion") or "",
        payload.get("hiring_recommendation_reason") or "",
        scores.get("overall", ""),
        scores.get("culture_fit", ""),
        scores.get("customer_response", ""),
        scores.get("ownership", ""),
        scores.get("communication", ""),
        scores.get("learning_agility", ""),
        payload.get("summary") or "",
        payload.get("recommended_next_step") or "",
        payload.get("transcript") or "",
    ]


def _email_action(to: str, subject: str, body: str, from_name: str = "채용팀") -> OutboxAction:
    return OutboxAction("outbox_email", [_now_iso(), to, subject, body, from_name])


def _slack_action(recipient: str, message: str) -> OutboxAction:
    return OutboxAction("outbox_slack", [_now_iso(), recipient, message])


def _notion_action(name: str, database: str, notes: str) -> OutboxAction:
    return OutboxAction("outbox_notion", [_now_iso(), name, database, notes])


def _docs_action(name: str, email: str, content: str) -> OutboxAction:
    return OutboxAction("outbox_docs", [_now_iso(), name, email, content])


def _zoom_action(topic: str, start_iso: str, duration_min: int, candidate_name: str) -> OutboxAction:
    return OutboxAction(
        "outbox_zoom",
        [_now_iso(), topic, start_iso, duration_min, candidate_name],
    )


def _scheduled_email_action(
    send_after_iso: str,
    to: str,
    subject: str,
    body: str,
    candidate_name: str,
) -> OutboxAction:
    return OutboxAction(
        "outbox_scheduled",
        [_now_iso(), send_after_iso, to, subject, body, candidate_name],
    )


def _log_action(name: str, branch: str, screening: str, detail: str) -> OutboxAction:
    return OutboxAction("pipeline_log", [_now_iso(), name, branch, screening, detail])


def _acceptance_email_html(name: str, position: str) -> str:
    return f"""<p>안녕하세요, {name}님.</p>
<p>{position} 포지션 채용 전형 결과, <strong>최종 합격</strong>하셨음을 안내드립니다. 축하드립니다!</p>
<p>입사 절차와 일정은 곧 별도 메일로 안내드리겠습니다.</p>
<p>감사합니다.<br>채용팀 드림</p>"""


def _reject_email_html(name: str) -> str:
    return f"""<p>안녕하세요, {name}님.</p>
<p>저희 채용에 관심과 성의 있는 지원에 감사드립니다.</p>
<p>아쉽게도 이번 전형에서는 다음 단계로 진행하기 어렵다는 말씀을 드립니다.</p>
<p>앞날의 성공을 응원합니다.<br>채용팀 드림</p>"""


def _admin_hold_alert(name: str, email: str, position: str, reason: str) -> str:
    return (
        f"🌟 *보류 지원자 검토 필요*\n\n"
        f"👤 지원자: {name}\n"
        f"📧 이메일: {email}\n"
        f"💼 포지션: {position}\n"
        f"📊 상태: 🟡 보류\n\n"
        f"사유: {reason}\n\n"
        f"→ outbox_docs / Notion / Zoom outbox를 확인하세요."
    )


def build_branch_actions(
    payload: dict,
    *,
    admin_email: str,
    admin_slack: str,
    notion_database: str,
    followup_questions: str | None = None,
) -> tuple[str, list[OutboxAction]]:
    name = payload.get("candidate_name") or "지원자"
    email = payload.get("candidate_email") or ""
    position = payload.get("position") or ""
    opinion = (payload.get("hiring_opinion") or "").strip()
    reason = payload.get("hiring_recommendation_reason") or ""
    summary = payload.get("summary") or ""
    actions: list[OutboxAction] = []

    notes = f"포지션: {position}\n의견: {opinion}\n사유: {reason}\n요약: {summary}"

    if opinion == "추천":
        actions.append(_notion_action(name, notion_database, notes))
        # HITL 최종 합격: 다음날 오후 2시(KST)까지 관리자가 Notion 체크박스를
        # 켜 두면 전용 Zap이 합격 메일을 발송한다 (체크 안 하면 미발송).
        if email:
            actions.append(
                _scheduled_email_action(
                    _next_day_2pm_kst_iso(),
                    email,
                    f"{name}님, 최종 합격을 축하드립니다!",
                    _acceptance_email_html(name, position),
                    name,
                )
            )
        if admin_slack:
            actions.append(
                _slack_action(
                    admin_slack,
                    f"✅ *추천 지원자* — {name} ({email}) / {position}\n"
                    "내일 오후 2시 전까지 Notion에서 최종 검토 후 승인 체크박스를 켜 주세요.\n"
                    "체크된 경우에만 최종 합격 메일이 자동 발송됩니다.",
                )
            )
        if admin_email:
            actions.append(
                _email_action(
                    admin_email,
                    f"[채용] 추천 지원자 — {name}",
                    f"<p>추천 판정 지원자입니다.</p>"
                    f"<p>이름: {name}<br>이메일: {email}<br>포지션: {position}</p>"
                    f"<p>사유: {reason}</p>",
                    "Hiring System",
                )
            )
        return "추천", actions

    if opinion == "보류":
        if admin_slack:
            actions.append(_slack_action(admin_slack, _admin_hold_alert(name, email, position, reason)))
        if admin_email:
            actions.append(
                _email_action(
                    admin_email,
                    f"[채용 검토] 보류 지원자 — {name}",
                    f"<p>보류 판정 지원자가 있습니다.</p>"
                    f"<p>이름: {name}<br>이메일: {email}<br>포지션: {position}</p>"
                    f"<p>사유: {reason}</p>",
                    "Hiring System",
                )
            )
        actions.append(_notion_action(name, notion_database, notes))
        actions.append(
            _zoom_action(
                f"추가 인터뷰 — {name}",
                _next_day_2pm_kst_iso(),
                30,
                name,
            )
        )
        if followup_questions:
            doc_content = (
                f"지원자: {name}\n이메일: {email}\n포지션: {position}\n\n"
                f"--- 2차 면접 맞춤 질문 ---\n\n{followup_questions}"
            )
            actions.append(_docs_action(name, email, doc_content))
        return "보류", actions

    if opinion == "비추천":
        if email:
            actions.append(
                _email_action(
                    email,
                    f"{name}님, 입사 지원에 감사드립니다.",
                    _reject_email_html(name),
                )
            )
        return "비추천", actions

    return "unknown", actions


def generate_followup_questions(payload: dict, llm_fn: Callable[[str], str]) -> str:
    concerns = payload.get("concerns") or []
    concerns_text = ", ".join(concerns) if concerns else "(없음)"
    prompt = FOLLOWUP_PROMPT.format(
        position=payload.get("position") or "",
        hiring_opinion=payload.get("hiring_opinion") or "",
        hiring_recommendation_reason=payload.get("hiring_recommendation_reason") or "",
        summary=payload.get("summary") or "",
        concerns=concerns_text,
        recommended_next_step=payload.get("recommended_next_step") or "",
        transcript=payload.get("transcript") or "",
    )
    return llm_fn(prompt).strip()


def _plan_actions(
    payload: dict,
    config: dict,
    llm_fn: Callable[[str], str] | None,
) -> tuple[bool, str, str, list[OutboxAction]]:
    passed, screening_reason = check_screening(payload, config)
    name = payload.get("candidate_name") or "익명"
    actions: list[OutboxAction] = []
    branch = "filtered"

    if passed:
        followup = None
        opinion = (payload.get("hiring_opinion") or "").strip()
        if opinion == "보류" and llm_fn:
            try:
                followup = generate_followup_questions(payload, llm_fn)
            except Exception as e:
                followup = f"(2차 질문 생성 실패: {e})"

        branch, actions = build_branch_actions(
            payload,
            admin_email=config["admin_email"],
            admin_slack=config["admin_slack"],
            notion_database=config["notion_database"],
            followup_questions=followup,
        )

    detail = (
        screening_reason
        if not passed
        else f"hiring_opinion={payload.get('hiring_opinion', '')}"
    )
    actions.append(_log_action(name, branch, "pass" if passed else "fail", detail))
    return passed, screening_reason, branch, actions


def _dispatch_actions(
    actions: list[OutboxAction],
    webhook_url: str,
) -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    if not webhook_url:
        for action in actions:
            results.append((action.target, False, "GAS 웹훅 URL 미설정"))
        return results
    for action in actions:
        ok, msg = post_to_gas(action.to_payload(), webhook_url)
        results.append((action.target, ok, msg))
    return results


def run_pipeline(
    payload: dict,
    webhook_url: str | None = None,
    *,
    config: dict | None = None,
    admin_email: str | None = None,
    admin_slack: str | None = None,
    notion_database: str | None = None,
    llm_fn: Callable[[str], str] | None = None,
    skip_outbox: bool = False,
    skip_interview_save: bool = False,
) -> PipelineResult:
    """면접 결과 저장 + 스크리닝 + outbox 액션 큐잉."""
    cfg = dict(config or load_pipeline_config())
    if webhook_url is not None:
        cfg["webhook_url"] = webhook_url
    if admin_email is not None:
        cfg["admin_email"] = admin_email
    if admin_slack is not None:
        cfg["admin_slack"] = admin_slack
    if notion_database is not None:
        cfg["notion_database"] = notion_database

    url = cfg["webhook_url"]

    if skip_interview_save:
        interview_saved = (True, "면접 행 저장 생략 (재전송)")
    else:
        interview_saved = post_to_gas(
            {"target": "interviews", "row": interview_row(payload)},
            url,
        )

    passed, screening_reason, branch, actions = _plan_actions(payload, cfg, llm_fn)

    action_results: list[tuple[str, bool, str]] = []
    if not skip_outbox:
        action_results = _dispatch_actions(actions, url)

    return PipelineResult(
        interview_saved=interview_saved,
        screening_passed=passed,
        screening_reason=screening_reason,
        branch=branch,
        actions=actions,
        action_results=action_results,
    )


def retry_failed_outbox(result: PipelineResult, webhook_url: str | None = None) -> PipelineResult:
    """실패한 outbox만 재전송 (interviews 행 중복 append 방지)."""
    url = webhook_url or load_pipeline_config()["webhook_url"]
    new_results = list(result.action_results)
    for i, action in enumerate(result.actions):
        prev = new_results[i] if i < len(new_results) else ("", False, "")
        if action.target == "pipeline_log":
            continue
        if prev[1]:
            continue
        ok, msg = post_to_gas(action.to_payload(), url)
        new_results[i] = (action.target, ok, msg)
    result.action_results = new_results
    return result
