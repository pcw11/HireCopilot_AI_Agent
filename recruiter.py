"""
HireCopilot - 통합 관리자 콘솔

실행: streamlit run recruiter.py --server.port 8502
"""

import json
import os
from datetime import datetime, timedelta, timezone

import streamlit as st
from dotenv import load_dotenv

from admin_store import (
    failed_outbox_count,
    list_interview_records,
    pipeline_result_from_dict,
    save_interview_record,
    summarize_records,
    update_pipeline_result,
    update_review_status,
)
from pipeline import (
    KST,
    load_pipeline_config,
    post_to_gas,
    retry_failed_outbox,
    run_pipeline,
)

load_dotenv(override=True)

RECRUITER_PASSWORD = os.getenv("RECRUITER_PASSWORD", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
RECRUITER_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recruiter_config.json")

DEFAULT_POSITIONS = [
    {"name": "Customer Success Associate (고객 성공 매니저)", "criteria": ""},
    {"name": "Customer Support Specialist (고객 지원 전문가)", "criteria": ""},
    {"name": "Account Manager (어카운트 매니저)", "criteria": ""},
    {"name": "Sales Development Representative (영업 개발 담당)", "criteria": ""},
    {"name": "Operations Coordinator (운영 조정관)", "criteria": ""},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_recruiter_config() -> dict:
    if os.path.exists(RECRUITER_CONFIG_PATH):
        try:
            with open(RECRUITER_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    default = {"positions": DEFAULT_POSITIONS, "general_criteria": "", "updated_at": ""}
    try:
        with open(RECRUITER_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    return default


def save_recruiter_config(config: dict) -> None:
    config["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(RECRUITER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def _ok_badge(ok: bool) -> str:
    return "✅ 정상" if ok else "⚠️ 확인 필요"


def _opinion_badge(opinion: str) -> str:
    return {
        "추천": "🟢 추천",
        "보류": "🟡 보류",
        "비추천": "🔴 비추천",
    }.get(opinion or "", opinion or "—")


def _format_record_label(record: dict) -> str:
    payload = record.get("payload") or {}
    name = payload.get("candidate_name") or "이름 없음"
    opinion = _opinion_badge(payload.get("hiring_opinion"))
    position = payload.get("position") or "포지션 없음"
    timestamp = (payload.get("timestamp") or record.get("recorded_at") or "")[:16]
    return f"{timestamp} | {name} | {position} | {opinion}"


def _result_rows(record: dict) -> list[dict]:
    result = record.get("pipeline_result") or {}
    actions = result.get("actions") or []
    rows = []
    for i, item in enumerate(result.get("action_results") or []):
        target = item[0] if len(item) > 0 else ""
        if target == "pipeline_log":
            continue
        ok = bool(item[1]) if len(item) > 1 else False
        message = item[2] if len(item) > 2 else ""
        action_row = actions[i].get("row", []) if i < len(actions) else []
        rows.append(
            {
                "target": target,
                "status": "성공" if ok else "실패",
                "message": message,
                "row": action_row,
            }
        )
    return rows


def _review_badge(review: dict | None) -> str:
    status = (review or {}).get("status") or "not_required"
    return {
        "pending": "⏳ 승인 대기",
        "approved_sent": "✅ 승인/발송 완료",
        "approved_send_failed": "⚠️ 승인됨 · 발송 실패",
        "rejected_by_admin": "🛑 관리자 반려",
        "held_by_admin": "🟡 추가 보류",
        "not_required": "—",
    }.get(status, status)


def _effective_review(record: dict) -> dict:
    review = record.get("review")
    if review:
        return review
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    if payload.get("hiring_opinion") == "추천" and result.get("screening_passed"):
        return {"status": "pending", "note": "", "updated_at": ""}
    return {"status": "not_required", "note": "", "updated_at": ""}


def _scheduled_action_row(record: dict) -> list | None:
    actions = (record.get("pipeline_result") or {}).get("actions") or []
    for action in actions:
        if action.get("target") == "outbox_scheduled":
            return list(action.get("row") or [])
    return None


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


def _record_table_row(record: dict) -> dict:
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    scores = payload.get("scores") or {}
    return {
        "처리": candidate_action_label(record),
        "일시": (payload.get("timestamp") or record.get("recorded_at") or "")[:16],
        "이름": payload.get("candidate_name", ""),
        "포지션": payload.get("position", ""),
        "의견": _opinion_badge(payload.get("hiring_opinion")),
        "점수": scores.get("overall", ""),
        "자격": "통과" if result.get("screening_passed") else "미달",
        "알림": "실패" if failed_outbox_count(record) else "정상",
    }


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
            f"{_opinion_badge(brief['opinion'])} · 점수 {brief['overall'] or '-'}"
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


def _build_ai_report_prompt(records: list[dict], max_candidates: int = 12) -> str:
    briefs = [candidate_brief(record) for record in sorted(records, key=candidate_priority_key)[:max_candidates]]
    return (
        "당신은 채용 담당자를 돕는 한국어 HR 분석가입니다.\n"
        "아래 AI 면접 결과 목록을 비교해 관리자용 우선순위 리포트를 작성하세요.\n"
        "형식은 Markdown으로 작성하고, 과장하지 말고 기록에 근거하세요.\n\n"
        "반드시 포함할 섹션:\n"
        "1. 먼저 검토할 후보자 Top 3\n"
        "2. 후보자별 강점과 리스크\n"
        "3. 2차 면접에서 확인할 질문\n"
        "4. 오늘 관리자 액션 체크리스트\n\n"
        f"후보자 데이터 JSON:\n{json.dumps(briefs, ensure_ascii=False, indent=2)}"
    )


def generate_priority_report(records: list[dict], *, use_openai: bool = True) -> tuple[str, str]:
    if not records:
        return "면접 기록이 없어 리포트를 생성할 수 없습니다.", "empty"
    if not use_openai or not OPENAI_API_KEY:
        return fallback_priority_report(records), "fallback"

    try:
        from openai import OpenAI

        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "당신은 채용 운영을 돕는 신중한 HR 분석가입니다."},
                {"role": "user", "content": _build_ai_report_prompt(records)},
            ],
            temperature=0.2,
        )
        text = (resp.choices[0].message.content or "").strip()
        return text or fallback_priority_report(records), "openai"
    except Exception as e:
        return f"{fallback_priority_report(records)}\n\n> OpenAI 리포트 생성 실패: {type(e).__name__}: {e}", "fallback_error"


def _unique_positions(records: list[dict]) -> list[str]:
    positions = sorted(
        {
            (r.get("payload") or {}).get("position", "").strip()
            for r in records
            if (r.get("payload") or {}).get("position", "").strip()
        }
    )
    return positions


def filter_records(
    records: list[dict],
    *,
    query: str = "",
    opinion: str = "전체",
    position: str = "전체",
    screening: str = "전체",
    failed_outbox_only: bool = False,
) -> list[dict]:
    """이름·이메일·포지션 검색 및 운영 필터."""
    filtered = list(records)
    q = query.strip().lower()
    if q:
        filtered = [
            r
            for r in filtered
            if q in (r.get("payload") or {}).get("candidate_name", "").lower()
            or q in (r.get("payload") or {}).get("candidate_email", "").lower()
            or q in (r.get("payload") or {}).get("position", "").lower()
        ]
    if opinion != "전체":
        filtered = [
            r for r in filtered if (r.get("payload") or {}).get("hiring_opinion") == opinion
        ]
    if position != "전체":
        filtered = [
            r for r in filtered if (r.get("payload") or {}).get("position") == position
        ]
    if screening == "통과":
        filtered = [r for r in filtered if (r.get("pipeline_result") or {}).get("screening_passed")]
    elif screening == "실패":
        filtered = [r for r in filtered if not (r.get("pipeline_result") or {}).get("screening_passed")]
    if failed_outbox_only:
        filtered = [r for r in filtered if failed_outbox_count(r) > 0]
    return filtered


def _render_record_filters(records: list[dict], key_prefix: str) -> list[dict]:
    query = st.text_input(
        "🔍 이름·이메일·포지션 검색",
        key=f"{key_prefix}_search",
        placeholder="검색어를 입력하세요",
    )
    with st.expander("필터 옵션", expanded=False):
        positions = _unique_positions(records)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.selectbox("채용 의견", ["전체", "추천", "보류", "비추천"], key=f"{key_prefix}_opinion")
        with col2:
            st.selectbox("포지션", ["전체", *positions], key=f"{key_prefix}_position")
        with col3:
            st.selectbox("자격 심사", ["전체", "통과", "실패"], key=f"{key_prefix}_screening")
        st.checkbox("알림 실패만 보기", key=f"{key_prefix}_failed_only")

    opinion = st.session_state.get(f"{key_prefix}_opinion", "전체")
    position = st.session_state.get(f"{key_prefix}_position", "전체")
    screening = st.session_state.get(f"{key_prefix}_screening", "전체")
    failed_only = st.session_state.get(f"{key_prefix}_failed_only", False)

    filtered = filter_records(
        records,
        query=query,
        opinion=opinion,
        position=position,
        screening=screening,
        failed_outbox_only=failed_only,
    )
    st.caption(f"{len(filtered)}명 표시 (전체 {len(records)}명)")
    return filtered


def _render_record_detail(record: dict) -> None:
    payload = record.get("payload") or {}
    result = record.get("pipeline_result") or {}
    scores = payload.get("scores") or {}

    st.markdown(f"**{payload.get('candidate_name', '-')}** · {payload.get('position', '-')} · `{record.get('record_id', '-')}`")

    summary_cols = st.columns(5)
    summary_cols[0].metric("채용 의견", _opinion_badge(payload.get("hiring_opinion")))
    summary_cols[1].metric("종합 점수", f"{scores.get('overall', '-')} / 5")
    summary_cols[2].metric("자격", "통과" if result.get("screening_passed") else "미달")
    summary_cols[3].metric("알림", "실패" if failed_outbox_count(record) else "정상")
    summary_cols[4].metric("검토", _review_badge(_effective_review(record)))

    tab_summary, tab_detail, tab_raw = st.tabs(["요약", "상세 정보", "원본"])

    with tab_summary:
        col_left, col_right = st.columns([2, 1])
        with col_left:
            st.markdown("**평가 요약**")
            st.write(payload.get("summary") or "(요약 없음)")
            if payload.get("hiring_recommendation_reason"):
                st.markdown("**채용 판단 이유**")
                st.write(payload["hiring_recommendation_reason"])
            st.markdown("**추천 다음 단계**")
            st.write(payload.get("recommended_next_step") or "(다음 단계 없음)")
        with col_right:
            st.markdown("**후보자 정보**")
            st.write(f"이메일: {payload.get('candidate_email', '-')}")
            st.write(f"학력: {payload.get('degree', '-')}")
            st.write(f"경력: {payload.get('experience', '-')}")
            st.write(f"학점: {payload.get('gpa', '-') or '-'}")
            st.write(f"적합도: {payload.get('fit_level', '-')}")

            st.markdown("**파이프라인**")
            saved = result.get("interview_saved", ["", ""])
            st.write(f"면접 저장: {saved[1] if len(saved) > 1 else '-'}")
            passed = result.get("screening_passed")
            st.write(f"자격 필터: {'통과' if passed else str(result.get('screening_reason', '-'))}")
            st.write(f"분기: {result.get('branch', '-')}")
            review = _effective_review(record)
            if review.get("updated_at"):
                st.caption(f"검토 기록: {review.get('updated_at')} · {review.get('note', '')}")

    with tab_detail:
        rubric_labels = {
            "culture_fit": "문화 적합도",
            "customer_response": "고객 응대",
            "ownership": "주인의식",
            "communication": "커뮤니케이션",
            "learning_agility": "학습 민첩성",
        }
        score_cols = st.columns(5)
        for col, (key, label) in zip(score_cols, rubric_labels.items()):
            col.metric(label, f"{scores.get(key, '-')} / 5")

        col_s, col_c = st.columns(2)
        with col_s:
            st.markdown("**강점**")
            for item in payload.get("strengths") or ["(기록 없음)"]:
                st.markdown(f"- {item}")
        with col_c:
            st.markdown("**우려사항**")
            for item in payload.get("concerns") or ["(기록 없음)"]:
                st.markdown(f"- {item}")

    with tab_raw:
        st.markdown("**평가 JSON**")
        st.json(payload)
        st.markdown("**대화록**")
        st.code(payload.get("transcript") or "(대화록 없음)", language="text")


def _clear_recruiter_ui_state() -> None:
    """로그아웃 시 관리자 UI 전용 세션 키만 정리."""
    for key in (
        "recruiter_positions",
        "interview_detail_select",
        "outbox_detail_select",
        "retry_record_select",
        "instant_record_select",
        "instant_action_select",
        "final_review_select",
    ):
        st.session_state.pop(key, None)
    for key in list(st.session_state.keys()):
        if key.startswith(("dash_", "iv_", "ob_")):
            st.session_state.pop(key, None)


# ---------------------------------------------------------------------------
# 시뮬레이션 / 수동 작업 helpers
# ---------------------------------------------------------------------------

SIM_SCENARIOS = {
    "🟢 추천 (Notion 등록 + 합격 메일 예약 + 관리자 알림)": {
        "hiring_opinion": "추천",
        "fit_level": "strong_match",
    },
    "🟡 보류 (2차 면접 준비: Zoom/Docs/Notion)": {
        "hiring_opinion": "보류",
        "fit_level": "needs_human_review",
    },
    "🔴 비추천 (지원자 탈락 메일)": {
        "hiring_opinion": "비추천",
        "fit_level": "weak_match",
    },
    "🚫 자격 미달 — 학점 3.0 이하": {
        "hiring_opinion": "추천",
        "fit_level": "possible_match",
        "gpa": "2.8",
    },
    "🚫 자격 미달 — 이메일 형식 오류": {
        "hiring_opinion": "추천",
        "fit_level": "possible_match",
        "bad_email": True,
    },
}


def _make_sim_payload(scenario: dict, name: str, email: str, position: str) -> dict:
    if scenario.get("bad_email"):
        email = email.replace("@", "_")
    return {
        "project_notice": "관리자 시뮬레이션",
        "candidate_name": name,
        "candidate_email": email,
        "position": position,
        "degree": "학사 (4년제)",
        "gpa": scenario.get("gpa", "3.8"),
        "experience": "1~3년",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scores": {
            "culture_fit": 4,
            "customer_response": 4,
            "ownership": 4,
            "communication": 4,
            "learning_agility": 4,
            "overall": 4.0,
        },
        "fit_level": scenario.get("fit_level", "possible_match"),
        "hiring_opinion": scenario["hiring_opinion"],
        "hiring_recommendation_reason": "관리자 콘솔 시뮬레이션으로 생성된 가상 평가입니다.",
        "summary": "관리자 콘솔에서 파이프라인 동작 확인을 위해 생성한 시뮬레이션 기록입니다.",
        "strengths": ["(시뮬레이션) 명확한 의사소통"],
        "concerns": ["(시뮬레이션) 주인의식 근거 부족"],
        "evidence_quotes": [],
        "recommended_next_step": "(시뮬레이션) 사람 면접 검토",
        "transcript": "면접관: (시뮬레이션) 자기소개 부탁드립니다.\n지원자: (시뮬레이션) 안녕하세요, 테스트 지원자입니다.",
    }


def _sim_llm_fn(_prompt: str) -> str:
    return (
        "1. (시뮬레이션) 고객 불만 상황에서 본인의 역할을 STAR 형식으로 설명해 주세요.\n"
        "2. (시뮬레이션) 팀 내 갈등을 조율했던 경험을 말씀해 주세요.\n"
        "3. (시뮬레이션) 새로운 업무를 빠르게 학습한 사례를 알려 주세요."
    )


def _send_scheduled_now(row: list, webhook_url: str) -> tuple[bool, str]:
    """예약 메일(outbox_scheduled 행)을 Delay/Zapier 대기 없이 즉시 발송한다.

    scheduled row: timestamp | send_after_iso | to | subject | body | candidate_name
    email row:     timestamp | to | subject | body | from_name
    """
    email_row = [
        datetime.now(timezone.utc).isoformat(),
        row[2] if len(row) > 2 else "",
        row[3] if len(row) > 3 else "",
        row[4] if len(row) > 4 else "",
        "채용팀",
    ]
    return post_to_gas({"target": "send_email_now", "row": email_row}, webhook_url)


MANUAL_OUTBOX_TARGETS = [
    "outbox_email",
    "outbox_slack",
    "outbox_notion",
    "outbox_docs",
    "outbox_zoom",
    "outbox_scheduled",
]


# ---------------------------------------------------------------------------
# UI sections
# ---------------------------------------------------------------------------

def render_dashboard(records: list[dict]) -> None:
    summary = summarize_records(records)
    cfg = load_pipeline_config()

    st.markdown("### 운영 요약")
    cols = st.columns(4)
    cols[0].metric("전체 면접", summary["total"])
    cols[1].metric("추천", summary["recommended"])
    cols[2].metric("보류", summary["hold"])
    cols[3].metric("비추천", summary["rejected"])

    failed_records = [r for r in records if failed_outbox_count(r) > 0]
    if failed_records:
        st.warning(f"알림 전송 실패 {len(failed_records)}건 — **알림/파이프라인** 메뉴에서 재전송하세요.")

    pending_review = [
        r for r in records
        if (r.get("payload") or {}).get("hiring_opinion") in ("추천", "보류")
        and (r.get("pipeline_result") or {}).get("screening_passed")
        and _effective_review(r).get("status") in ("pending", "not_required")
    ]
    cols2 = st.columns(4)
    cols2[0].metric("자격 통과", summary["screening_passed"])
    cols2[1].metric("자격 미달", summary["screening_failed"])
    cols2[2].metric("관리자 확인", len(pending_review))
    cols2[3].metric("알림 실패", summary["failed_outbox"])

    if records:
        st.divider()
        st.markdown("**지금 확인할 항목**")
        focus_records = sorted(records, key=candidate_priority_key)[:6]
        st.dataframe(
            [_record_table_row(record) for record in focus_records],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("**최근 면접**")
        st.dataframe(
            [_record_table_row(record) for record in records[:5]],
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("연결 상태 확인"):
        ok_api = bool(os.getenv("OPENAI_API_KEY", "").strip())
        ok_gas = bool(cfg["webhook_url"])
        c1, c2, c3 = st.columns(3)
        c1.write(f"OpenAI: {_ok_badge(ok_api)}")
        c2.write(f"시트 연결: {_ok_badge(ok_gas)}")
        c3.write(
            f"자격 기준: 학점>{cfg['min_gpa']}, "
            f"학점필수{'O' if cfg['require_gpa'] else 'X'}"
        )

    if not records:
        st.info("아직 면접 기록이 없습니다. 지원자가 `app.py`에서 면접을 완료하면 여기에 나타납니다.")


def render_interviews(records: list[dict]) -> None:
    st.markdown("### 지원자 목록")
    if not records:
        st.info("면접을 완료한 지원자가 없습니다.")
        return

    filtered = _render_record_filters(records, "iv")
    if not filtered:
        st.warning("필터 조건에 맞는 기록이 없습니다.")
        return

    overview_rows = []
    for record in filtered:
        payload = record.get("payload") or {}
        row = _record_table_row(record)
        overview_rows.append(
            {
                **row,
                "이메일": payload.get("candidate_email", ""),
                "학점": payload.get("gpa", ""),
                "최종 검토": _review_badge(_effective_review(record)),
            }
        )
    st.dataframe(overview_rows, use_container_width=True, hide_index=True)

    st.divider()
    selected = st.selectbox(
        "상세 조회할 지원자",
        options=filtered,
        format_func=_format_record_label,
        key="interview_detail_select",
    )
    _render_record_detail(selected)


def render_ai_report(records: list[dict]) -> None:
    st.markdown("### AI 후보자 우선순위 리포트")
    st.caption("저장된 면접 결과를 비교해 관리자가 먼저 볼 후보자와 확인 질문을 정리합니다.")

    if not records:
        st.info("면접 기록이 쌓이면 AI 리포트를 만들 수 있습니다.")
        return

    filtered = _render_record_filters(records, "ai")
    if not filtered:
        st.warning("필터 조건에 맞는 기록이 없습니다.")
        return

    col_a, col_b, col_c = st.columns(3)
    col_a.metric("분석 대상", len(filtered))
    col_b.metric("추천", sum(1 for r in filtered if (r.get("payload") or {}).get("hiring_opinion") == "추천"))
    col_c.metric("보류", sum(1 for r in filtered if (r.get("payload") or {}).get("hiring_opinion") == "보류"))

    use_openai = st.checkbox(
        "OpenAI로 리포트 생성",
        value=bool(OPENAI_API_KEY),
        help="키가 없거나 호출에 실패하면 규칙 기반 리포트로 대체됩니다.",
    )

    if st.button("리포트 생성", type="primary", use_container_width=True):
        report, mode = generate_priority_report(filtered, use_openai=use_openai)
        st.session_state["ai_priority_report"] = report
        st.session_state["ai_priority_mode"] = mode

    report = st.session_state.get("ai_priority_report")
    if not report:
        report, mode = generate_priority_report(filtered, use_openai=False)
        st.session_state["ai_priority_report"] = report
        st.session_state["ai_priority_mode"] = mode

    mode = st.session_state.get("ai_priority_mode", "fallback")
    if mode == "openai":
        st.success(f"OpenAI 모델 `{OPENAI_MODEL}`로 생성했습니다.")
    elif mode == "fallback_error":
        st.warning("OpenAI 호출에 실패해 규칙 기반 리포트를 표시합니다.")
    elif mode == "fallback":
        st.info("규칙 기반 리포트를 표시합니다. OpenAI 키가 있으면 더 자연스러운 분석을 생성할 수 있습니다.")

    st.markdown(report)
    st.download_button(
        "리포트 다운로드",
        data=report,
        file_name="hirecopilot_priority_report.md",
        mime="text/markdown",
        use_container_width=True,
    )


def render_final_review_queue(records: list[dict]) -> None:
    cfg = load_pipeline_config()
    review_records = [
        record
        for record in records
        if (record.get("payload") or {}).get("hiring_opinion") == "추천"
        and (record.get("pipeline_result") or {}).get("screening_passed")
    ]

    st.markdown("**추천 후보 예약 메일 큐**")
    st.caption("AI가 추천한 후보자의 최종 합격 메일 예약 상태를 확인하고, 필요하면 즉시 발송할 수 있습니다.")

    if not review_records:
        st.info("추천 후보자가 없습니다.")
        return

    pending_count = sum(
        1
        for record in review_records
        if _effective_review(record).get("status") == "pending"
    )
    sent_count = sum(
        1
        for record in review_records
        if _effective_review(record).get("status") == "approved_sent"
    )
    rejected_count = sum(
        1
        for record in review_records
        if _effective_review(record).get("status") == "rejected_by_admin"
    )

    col_pending, col_sent, col_rejected = st.columns(3)
    col_pending.metric("검토 대기", pending_count)
    col_sent.metric("즉시 발송", sent_count)
    col_rejected.metric("반려", rejected_count)

    st.dataframe(
        [
            {
                "상태": _review_badge(_effective_review(record)),
                "이름": (record.get("payload") or {}).get("candidate_name", ""),
                "포지션": (record.get("payload") or {}).get("position", ""),
                "이메일": (record.get("payload") or {}).get("candidate_email", ""),
                "종합 점수": ((record.get("payload") or {}).get("scores") or {}).get("overall", ""),
                "예약 메일": "있음" if _scheduled_action_row(record) else "없음",
            }
            for record in review_records
        ],
        use_container_width=True,
        hide_index=True,
    )

    selected = st.selectbox(
        "검토할 추천 후보자",
        options=review_records,
        format_func=lambda r: f"{_review_badge(_effective_review(r))} | {_format_record_label(r)}",
        key="final_review_select",
    )
    payload = selected.get("payload") or {}
    review = _effective_review(selected)
    scheduled_row = _scheduled_action_row(selected)

    st.markdown(
        f"**{payload.get('candidate_name', '-')}** · "
        f"{payload.get('position', '-')} · "
        f"{payload.get('candidate_email', '-')}"
    )
    st.write(payload.get("hiring_recommendation_reason") or payload.get("summary") or "검토 사유가 없습니다.")
    if review.get("updated_at"):
        st.caption(f"최근 검토: {_review_badge(review)} · {review.get('updated_at')} · {review.get('note', '')}")

    review_note = st.text_area(
        "관리자 메모",
        value=review.get("note", ""),
        placeholder="검토 메모를 남기면 발표 때 의사결정 로그를 보여주기 좋습니다.",
        key=f"review_note_{selected['record_id']}",
        height=80,
    )

    approve_col, hold_col, reject_col = st.columns(3)
    with approve_col:
        if st.button("📨 예약 메일 즉시 발송", type="primary", use_container_width=True):
            if not cfg["webhook_url"]:
                st.error("GAS_WEBHOOK_URL이 설정되지 않아 메일 큐를 보낼 수 없습니다.")
            elif not scheduled_row:
                st.error("이 후보자에는 예약 합격 메일(outbox_scheduled)이 없습니다.")
            else:
                ok, msg = _send_scheduled_now(scheduled_row, cfg["webhook_url"])
                status = "approved_sent" if ok else "approved_send_failed"
                note = f"메일 발송 성공: {msg}" if ok else f"메일 발송 실패: {msg}"
                update_review_status(selected["record_id"], status, note=note, reviewer="admin")
                if ok:
                    st.success("예약 메일을 즉시 발송했습니다.")
                else:
                    st.error(f"즉시 발송에 실패했습니다: {msg}")
                st.rerun()
    with hold_col:
        if st.button("🟡 추가 보류", use_container_width=True):
            update_review_status(selected["record_id"], "held_by_admin", note=review_note, reviewer="admin")
            st.success("추가 보류 상태로 저장했습니다.")
            st.rerun()
    with reject_col:
        if st.button("🛑 관리자 반려", use_container_width=True):
            update_review_status(selected["record_id"], "rejected_by_admin", note=review_note, reviewer="admin")
            st.success("관리자 반려 상태로 저장했습니다.")
            st.rerun()


def render_outbox(records: list[dict]) -> None:
    st.markdown("### 알림 · 파이프라인")
    if not records:
        st.info("처리할 기록이 없습니다.")
        return

    failed_records = [record for record in records if failed_outbox_count(record) > 0]
    st.caption("재전송 시 면접 기록은 중복 저장되지 않고, 실패한 알림만 다시 보냅니다.")

    render_final_review_queue(records)
    st.divider()

    filtered = _render_record_filters(records, "ob")
    if not filtered:
        st.warning("필터 조건에 맞는 기록이 없습니다.")
        return

    selected = st.selectbox(
        "outbox 상태를 확인할 지원자",
        options=filtered,
        format_func=_format_record_label,
        key="outbox_detail_select",
    )
    payload = selected.get("payload") or {}
    result = selected.get("pipeline_result") or {}
    st.markdown(
        f"**{payload.get('candidate_name', '-')}** · "
        f"{_opinion_badge(payload.get('hiring_opinion'))} · "
        f"분기 `{result.get('branch', '-')}` · "
        f"자격 {'✅' if result.get('screening_passed') else '🚫'}"
    )

    rows = _result_rows(selected)
    if rows:
        failed_rows = [r for r in rows if r["status"] == "실패" and r["target"] != "pipeline_log"]
        if failed_rows:
            st.error(f"실패한 outbox 액션 {len(failed_rows)}건")
        st.dataframe(
            [
                {
                    "target": row["target"],
                    "status": row["status"],
                    "message": row["message"],
                }
                for row in rows
            ],
            use_container_width=True,
            hide_index=True,
        )
        with st.expander("전송 row 원본 보기"):
            st.json(rows)
    else:
        st.info("이 기록에는 outbox 전송 결과가 없습니다. (파이프라인 미실행 또는 드라이런 기록일 수 있습니다)")

    st.divider()
    st.markdown("**알림 재전송**")
    if not failed_records:
        st.success("실패한 알림이 없습니다.")
        return

    retry_target = st.selectbox(
        "재전송할 지원자",
        options=failed_records,
        format_func=_format_record_label,
        key="retry_record_select",
    )
    if st.button("실패한 알림 다시 보내기", type="primary", use_container_width=True):
        result = pipeline_result_from_dict(retry_target.get("pipeline_result"))
        if result is None:
            st.error("재전송할 파이프라인 결과를 복원하지 못했습니다.")
            return
        updated = retry_failed_outbox(result)
        update_pipeline_result(retry_target["record_id"], updated)
        st.success("재전송을 시도했습니다. 최신 결과로 새로고침합니다.")
        st.rerun()


def render_simulator(records: list[dict]) -> None:
    st.markdown("### 테스트 도구")
    st.caption("면접 없이 파이프라인과 outbox 전송 흐름을 점검합니다. (운영자 전용)")
    cfg = load_pipeline_config()

    tab_scenario, tab_instant, tab_manual = st.tabs(
        ["시나리오 실행", "예약/액션 즉시 실행", "수동 outbox 전송"]
    )

    # --- 1) 시나리오 시뮬레이터 ---
    with tab_scenario:
        st.markdown("가상 지원자 payload를 만들어 파이프라인 전체(필터 → 분기 → outbox)를 실행해 봅니다.")
        with st.form("sim_form"):
            scenario_label = st.selectbox("시나리오", options=list(SIM_SCENARIOS.keys()))
            col1, col2 = st.columns(2)
            with col1:
                sim_name = st.text_input("지원자 이름", value="[테스트] 홍길동")
                sim_position = st.text_input("포지션", value="개발자")
            with col2:
                sim_email = st.text_input("지원자 이메일", value=cfg["admin_email"] or "test@example.com")
            mode = st.radio(
                "실행 모드",
                options=["드라이런 (전송 없이 액션 미리보기)", "실제 전송 (GAS → 시트 → Zap)"],
                horizontal=True,
            )
            save_record = st.checkbox("관리자 콘솔 기록에 저장", value=True)
            save_interviews = st.checkbox("interviews 시트에도 행 저장 (실제 전송 시)", value=False)
            run_sim = st.form_submit_button("▶️ 시나리오 실행", use_container_width=True, type="primary")

        if run_sim:
            dry = mode.startswith("드라이런")
            payload = _make_sim_payload(SIM_SCENARIOS[scenario_label], sim_name, sim_email, sim_position)
            result = run_pipeline(
                payload,
                config=cfg,
                llm_fn=_sim_llm_fn,
                skip_outbox=dry,
                skip_interview_save=dry or not save_interviews,
            )

            st.markdown(
                f"**분기:** {_opinion_badge(result.branch) if result.branch in ('추천', '보류', '비추천') else result.branch}"
                f" &nbsp;|&nbsp; **자격 필터:** {'✅ 통과' if result.screening_passed else f'🚫 {result.screening_reason}'}"
            )

            action_rows = [
                {
                    "target": a.target,
                    "row 미리보기": " | ".join(str(v)[:40] for v in a.row),
                }
                for a in result.actions
                if a.target != "pipeline_log"
            ]
            st.markdown("**계획된 액션**" + (" (드라이런 — 전송되지 않음)" if dry else ""))
            st.dataframe(action_rows, use_container_width=True, hide_index=True)

            if not dry:
                st.markdown("**전송 결과**")
                st.dataframe(
                    [
                        {"target": t, "status": "✅ 성공" if ok else "⚠️ 실패", "message": m}
                        for t, ok, m in result.action_results
                        if t != "pipeline_log"
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            if save_record:
                record = save_interview_record(payload, result)
                st.success(f"관리자 콘솔 기록에 저장했습니다 (record_id: {record['record_id']}). 새로고침하면 목록에 표시됩니다.")

    # --- 2) 예약/액션 즉시 실행 ---
    with tab_instant:
        st.markdown(
            "예약된 합격 메일(`outbox_scheduled`)을 **Delay 대기 없이 즉시 발송**하거나, "
            "기록된 outbox 액션을 그대로 다시 보냅니다."
        )
        if not cfg["webhook_url"]:
            st.warning("GAS_WEBHOOK_URL이 설정되지 않아 전송할 수 없습니다.")
        actionable = [
            r for r in records
            if any(
                a.get("target") != "pipeline_log"
                for a in (r.get("pipeline_result") or {}).get("actions") or []
            )
        ]
        if not actionable:
            st.info("outbox 액션이 기록된 면접이 없습니다. 시나리오 실행 탭에서 기록을 만들어 보세요.")
            outbox_actions = []
        else:
            selected = st.selectbox(
                "지원자 선택",
                options=actionable,
                format_func=_format_record_label,
                key="instant_record_select",
            )
            actions = (selected.get("pipeline_result") or {}).get("actions") or []
            outbox_actions = [a for a in actions if a.get("target") != "pipeline_log"]

        if outbox_actions:
            action = st.selectbox(
                "액션 선택",
                options=outbox_actions,
                format_func=lambda a: f"{a['target']} — " + " | ".join(str(v)[:30] for v in a.get("row", [])[:4]),
                key="instant_action_select",
            )
            row = action.get("row", [])

            if action["target"] == "outbox_scheduled":
                st.markdown(
                    f"- **수신자:** {row[2] if len(row) > 2 else '-'}\n"
                    f"- **제목:** {row[3] if len(row) > 3 else '-'}\n"
                    f"- **원래 예약 시각:** {row[1] if len(row) > 1 else '-'}"
                )
                if st.button("📨 지금 즉시 발송 (예약/승인 생략)", type="primary", use_container_width=True):
                    ok, msg = _send_scheduled_now(row, cfg["webhook_url"])
                    if ok:
                        st.success(f"즉시 발송 완료 — {msg}")
                    else:
                        st.error(f"전송 실패: {msg}")
            else:
                if st.button("♻️ 이 액션 그대로 재전송", use_container_width=True):
                    ok, msg = post_to_gas({"target": action["target"], "row": row}, cfg["webhook_url"])
                    if ok:
                        st.success(f"`{action['target']}` 재전송 완료 — {msg}")
                    else:
                        st.error(f"전송 실패: {msg}")

    # --- 3) 수동 outbox 전송 ---
    with tab_manual:
        st.markdown("필드를 직접 입력해 outbox 행을 보냅니다. 각 전용 Zap이 제대로 연결되었는지 점검할 때 사용하세요.")
        target = st.selectbox("outbox 탭", options=MANUAL_OUTBOX_TARGETS, key="manual_target")
        now_iso = datetime.now(timezone.utc).isoformat()
        row = None

        if target == "outbox_email":
            to = st.text_input("to", value=cfg["admin_email"], key="m_email_to")
            subject = st.text_input("subject", value="[테스트] HireCopilot 수동 전송", key="m_email_subject")
            body = st.text_area("body (HTML 가능)", value="<p>관리자 콘솔 수동 테스트 메일입니다.</p>", key="m_email_body")
            row = [now_iso, to, subject, body, "채용팀"]
        elif target == "outbox_slack":
            recipient = st.text_input("recipient (Slack user ID)", value=cfg["admin_slack"], key="m_slack_rcpt")
            message = st.text_area("message", value="🧪 관리자 콘솔 수동 테스트 메시지입니다.", key="m_slack_msg")
            row = [now_iso, recipient, message]
        elif target == "outbox_notion":
            n_name = st.text_input("name", value="[테스트] 수동 항목", key="m_notion_name")
            n_db = st.text_input("database", value=cfg["notion_database"], key="m_notion_db")
            n_notes = st.text_area("notes", value="관리자 콘솔 수동 테스트 항목입니다.", key="m_notion_notes")
            row = [now_iso, n_name, n_db, n_notes]
        elif target == "outbox_docs":
            d_name = st.text_input("candidate_name", value="[테스트] 홍길동", key="m_docs_name")
            d_email = st.text_input("candidate_email", value="test@example.com", key="m_docs_email")
            d_content = st.text_area("content", value="관리자 콘솔 수동 테스트 문서 내용입니다.", key="m_docs_content")
            row = [now_iso, d_name, d_email, d_content]
        elif target == "outbox_zoom":
            z_topic = st.text_input("topic", value="[테스트] 수동 미팅", key="m_zoom_topic")
            z_minutes = st.number_input("지금부터 몇 분 뒤 시작?", min_value=5, max_value=1440, value=15, key="m_zoom_delay")
            z_duration = st.number_input("duration_min", min_value=10, max_value=180, value=30, key="m_zoom_dur")
            z_name = st.text_input("candidate_name", value="[테스트] 홍길동", key="m_zoom_name")
            start_iso = (datetime.now(KST) + timedelta(minutes=int(z_minutes))).isoformat()
            st.caption(f"start_time_iso: `{start_iso}`")
            row = [now_iso, z_topic, start_iso, int(z_duration), z_name]
        elif target == "outbox_scheduled":
            s_to = st.text_input("to", value=cfg["admin_email"], key="m_sched_to")
            s_subject = st.text_input("subject", value="[테스트] 예약 메일", key="m_sched_subject")
            s_body = st.text_area("body", value="<p>예약 발송 테스트입니다.</p>", key="m_sched_body")
            s_minutes = st.number_input("지금부터 몇 분 뒤 발송?", min_value=1, max_value=10080, value=5, key="m_sched_delay")
            send_after = (datetime.now(KST) + timedelta(minutes=int(s_minutes))).isoformat()
            st.caption(f"send_after_iso: `{send_after}`")
            row = [now_iso, send_after, s_to, s_subject, s_body, "[테스트] 홍길동"]

        if st.button("🚀 전송", type="primary", use_container_width=True, key="manual_send_btn"):
            ok, msg = post_to_gas({"target": target, "row": row}, cfg["webhook_url"])
            if ok:
                st.success(f"`{target}` 전송 완료 — {msg}")
            else:
                st.error(f"전송 실패: {msg}")


SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1swaf7dyRsVRxepLJAXVoPO3YRNV0aPYmcBLL4_tPnbE"


def render_filter_settings(config: dict) -> None:
    st.subheader("🚦 자격 필터 (2차 파이프라인)")
    st.caption(
        "면접 종료 후 outbox 큐잉 전에 적용되는 자동 필터입니다. "
        "여기서 저장한 값이 .env의 PIPELINE_* 설정보다 우선하며, 재시작 없이 다음 면접부터 반영됩니다."
    )

    effective = load_pipeline_config()

    col_f1, col_f2 = st.columns([1, 1])
    with col_f1:
        filter_min_gpa = st.number_input(
            "학점 커트라인 (이 값 초과만 통과, 동점은 탈락)",
            min_value=0.0,
            max_value=4.5,
            value=float(effective["min_gpa"]),
            step=0.1,
            format="%.1f",
            key="filter_min_gpa_input",
        )
    with col_f2:
        filter_require_gpa = st.checkbox(
            "학점 미입력 시 탈락",
            value=bool(effective["require_gpa"]),
            key="filter_require_gpa_input",
            help="끄면 학점을 입력하지 않은 지원자도 통과합니다 (입력했다면 커트라인은 그대로 적용). 지원자 온보딩의 학점 필수 표시도 함께 바뀝니다.",
        )
        filter_block_newgrad = st.checkbox(
            "신입(경력 없음) 지원자 탈락",
            value=bool(effective["block_newgrad"]),
            key="filter_block_newgrad_input",
        )

    st.info(
        f"현재 적용 중: 학점 > **{effective['min_gpa']}** · "
        f"학점 필수 **{'ON' if effective['require_gpa'] else 'OFF'}** · "
        f"신입 제외 **{'ON' if effective['block_newgrad'] else 'OFF'}**"
    )

    st.markdown("**고정 필터 (항상 적용)**")
    st.markdown(
        "- 이메일에 `@` 포함\n"
        "- 학위 입력 (\"없음\"은 탈락)\n"
        "- 경력 입력 (\"없음\"은 탈락)"
    )

    if st.session_state.pop("filter_settings_saved", False):
        st.success("자격 필터가 저장되었습니다. 다음 면접부터 적용됩니다.")

    if st.button("필터 저장", use_container_width=True, type="primary", key="save_filters_btn"):
        save_recruiter_config(
            {
                "positions": config.get("positions", []),
                "general_criteria": config.get("general_criteria", ""),
                "pipeline": {
                    "min_gpa": float(filter_min_gpa),
                    "require_gpa": bool(filter_require_gpa),
                    "block_newgrad": bool(filter_block_newgrad),
                },
            }
        )
        st.session_state["filter_settings_saved"] = True
        st.rerun()


def render_recruiting_settings(config: dict) -> None:
    st.subheader("채용 담당 설정")
    st.caption("지원 포지션과 포지션별 AI 면접 중점 평가 기준을 관리합니다.")

    if "recruiter_positions" not in st.session_state:
        st.session_state.recruiter_positions = list(config.get("positions", []))

    with st.expander("새 포지션 추가"):
        new_pos_name = st.text_input(
            "포지션 이름",
            key="new_pos_name_input",
            placeholder="예: IT 개발자, 마케팅 담당자",
        )
        new_pos_criteria = st.text_area(
            "중점 평가 기준",
            key="new_pos_criteria_input",
            height=100,
            placeholder="이 포지션에서 중요하게 보는 역량, 경험, 태도 등을 자유롭게 서술하세요.",
        )
        if st.button("포지션 추가", key="add_pos_btn"):
            if new_pos_name.strip():
                st.session_state.recruiter_positions.append(
                    {"name": new_pos_name.strip(), "criteria": new_pos_criteria.strip()}
                )
                st.rerun()
            else:
                st.error("포지션 이름을 입력하세요.")

    if st.session_state.recruiter_positions:
        for i, pos in enumerate(st.session_state.recruiter_positions):
            with st.expander(f"{pos['name']}", expanded=True):
                updated_name = st.text_input("포지션 이름", value=pos["name"], key=f"pos_name_{i}")
                updated_criteria = st.text_area(
                    "중점 평가 기준",
                    value=pos.get("criteria", ""),
                    key=f"pos_criteria_{i}",
                    height=120,
                )
                col_save, col_del = st.columns([3, 1])
                with col_save:
                    if st.button("수정 저장", key=f"update_pos_{i}"):
                        st.session_state.recruiter_positions[i] = {
                            "name": updated_name.strip(),
                            "criteria": updated_criteria.strip(),
                        }
                        st.rerun()
                with col_del:
                    if st.button("삭제", key=f"del_pos_{i}"):
                        st.session_state.recruiter_positions.pop(i)
                        st.rerun()
    else:
        st.info("등록된 포지션이 없습니다. 위에서 포지션을 추가하세요.")

    st.divider()
    st.markdown("**공통 채용 중점 사항**")
    general_criteria = st.text_area(
        "모든 포지션에 공통으로 적용될 채용 기준",
        value=config.get("general_criteria", ""),
        height=120,
        placeholder="예: 팀워크와 커뮤니케이션을 중시합니다. 자기 주도적 학습 능력을 가진 인재를 선호합니다.",
        key="general_criteria_input",
    )

    if st.session_state.pop("recruiter_settings_saved", False):
        st.success("채용 설정이 저장되었습니다. 다음 면접부터 적용됩니다.")

    if st.button("설정 저장", use_container_width=True, type="primary"):
        save_recruiter_config(
            {
                "positions": st.session_state.recruiter_positions,
                "general_criteria": general_criteria.strip(),
                # 자격 필터는 별도 탭에서 관리 — 기존 값 보존
                "pipeline": config.get("pipeline") or {},
            }
        )
        st.session_state["recruiter_settings_saved"] = True
        st.rerun()

    if config.get("updated_at"):
        st.caption(f"마지막 저장: {config['updated_at']}")


# ---------------------------------------------------------------------------
# App shell
# ---------------------------------------------------------------------------

_NAV_ITEMS = [
    "대시보드",
    "지원자 관리",
    "AI 리포트",
    "알림/파이프라인",
    "테스트 도구",
    "설정",
]

st.set_page_config(
    page_title="HireCopilot — 관리자",
    page_icon="👔",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128,128,128,0.18);
        border-radius: 8px;
        padding: 14px 16px;
    }
    div[data-testid="stMetricValue"] { font-weight: 700; font-size: 1.6rem; }
    div[data-testid="stSidebar"] { background: var(--secondary-background-color); }
    .rc-hero {
        background: var(--secondary-background-color);
        border: 1px solid rgba(128,128,128,0.18);
        padding: 18px 22px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .rc-hero h1 { margin: 0; font-size: 1.5rem; }
    .rc-hero p { margin: 6px 0 0; opacity: 0.72; font-size: 0.9rem; }
    .rc-login {
        max-width: 400px;
        margin: 60px auto;
        padding: 36px 32px;
        border-radius: 8px;
        background: var(--secondary-background-color);
        border: 1px solid rgba(128,128,128,0.2);
        box-shadow: 0 4px 24px rgba(0,0,0,0.06);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if not st.session_state.get("recruiter_authenticated"):
    st.markdown(
        '<div class="rc-login">'
        '<h2 style="text-align:center;margin-top:0;">👔 채용 관리자</h2>'
        '<p style="text-align:center;opacity:0.7;">HireCopilot 관리자 콘솔</p>'
        '</div>',
        unsafe_allow_html=True,
    )
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        if not RECRUITER_PASSWORD:
            st.caption("암호가 설정되지 않아 바로 들어갈 수 있습니다.")
        pw = st.text_input("암호", type="password", key="recruiter_login_pw", label_visibility="collapsed", placeholder="암호 입력")
        if st.button("로그인", key="recruiter_login_btn", use_container_width=True, type="primary"):
            if (RECRUITER_PASSWORD and pw == RECRUITER_PASSWORD) or not RECRUITER_PASSWORD:
                st.session_state.recruiter_authenticated = True
                st.rerun()
            else:
                st.error("암호가 올바르지 않습니다.")
    st.stop()

config = load_recruiter_config()
records = list_interview_records(limit=100)

with st.sidebar:
    st.markdown("### HireCopilot")
    st.caption("채용 관리자")
    st.divider()
    page = st.radio(
        "메뉴",
        _NAV_ITEMS,
        label_visibility="collapsed",
        key="recruiter_nav",
    )
    st.divider()
    st.caption(f"면접 기록 {len(records)}건")
    if st.button("🔄 새로고침", use_container_width=True):
        st.rerun()
    if st.button("로그아웃", key="recruiter_logout", use_container_width=True):
        st.session_state.recruiter_authenticated = False
        st.session_state.pop("recruiter_nav", None)
        _clear_recruiter_ui_state()
        st.rerun()

st.markdown(
    """
    <div class="rc-hero">
      <h1>채용 관리자 콘솔</h1>
      <p>면접 결과 확인 · 알림 관리 · 채용 설정</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if page == "대시보드":
    render_dashboard(records)
elif page == "지원자 관리":
    render_interviews(records)
elif page == "AI 리포트":
    render_ai_report(records)
elif page == "알림/파이프라인":
    render_outbox(records)
elif page == "테스트 도구":
    render_simulator(records)
elif page == "설정":
    tab_recruit, tab_filter = st.tabs(["채용 기준", "자격 필터"])
    with tab_recruit:
        render_recruiting_settings(config)
    with tab_filter:
        render_filter_settings(config)
