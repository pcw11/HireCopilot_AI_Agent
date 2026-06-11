"""
HireCopilot - 통합 관리자 콘솔

실행: streamlit run recruiter.py --server.port 8502
"""

import json
import os
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv

from admin_store import (
    failed_outbox_count,
    list_interview_records,
    pipeline_result_from_dict,
    summarize_records,
    update_pipeline_result,
)
from pipeline import load_pipeline_config, retry_failed_outbox

load_dotenv(override=True)

RECRUITER_PASSWORD = os.getenv("RECRUITER_PASSWORD", "").strip()
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
    return "정상" if ok else "확인 필요"


def _format_record_label(record: dict) -> str:
    payload = record.get("payload") or {}
    name = payload.get("candidate_name") or "이름 없음"
    opinion = payload.get("hiring_opinion") or "의견 없음"
    position = payload.get("position") or "포지션 없음"
    timestamp = (payload.get("timestamp") or record.get("recorded_at") or "")[:16]
    return f"{timestamp} | {name} | {position} | {opinion}"


def _result_rows(record: dict) -> list[dict]:
    result = record.get("pipeline_result") or {}
    actions = result.get("actions") or []
    rows = []
    for i, item in enumerate(result.get("action_results") or []):
        target = item[0] if len(item) > 0 else ""
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


# ---------------------------------------------------------------------------
# UI sections
# ---------------------------------------------------------------------------

def render_dashboard(records: list[dict]) -> None:
    st.subheader("운영 대시보드")
    summary = summarize_records(records)
    cfg = load_pipeline_config()

    cols = st.columns(4)
    cols[0].metric("최근 면접", summary["total"])
    cols[1].metric("추천", summary["recommended"])
    cols[2].metric("보류", summary["hold"])
    cols[3].metric("비추천", summary["rejected"])

    cols = st.columns(3)
    cols[0].metric("자격 통과", summary["screening_passed"])
    cols[1].metric("자격 실패", summary["screening_failed"])
    cols[2].metric("실패 outbox", summary["failed_outbox"])

    st.divider()
    st.markdown("**시스템 설정 상태**")
    status_rows = [
        {"항목": "OpenAI API Key", "상태": _ok_badge(bool(os.getenv("OPENAI_API_KEY", "").strip()))},
        {"항목": "GAS Webhook", "상태": _ok_badge(bool(cfg["webhook_url"]))},
        {"항목": "관리자 이메일", "상태": _ok_badge(bool(cfg["admin_email"]))},
        {"항목": "관리자 Slack", "상태": _ok_badge(bool(cfg["admin_slack"]))},
    ]
    st.dataframe(status_rows, use_container_width=True, hide_index=True)

    if not records:
        st.info("아직 관리자 콘솔에 기록된 면접이 없습니다. 지원자 앱에서 면접을 완료하면 여기에 표시됩니다.")


def render_interviews(records: list[dict]) -> None:
    st.subheader("지원자 / 면접 결과")
    if not records:
        st.info("표시할 면접 기록이 없습니다.")
        return

    overview_rows = []
    for record in records:
        payload = record.get("payload") or {}
        result = record.get("pipeline_result") or {}
        overview_rows.append(
            {
                "이름": payload.get("candidate_name", ""),
                "이메일": payload.get("candidate_email", ""),
                "포지션": payload.get("position", ""),
                "학점": payload.get("gpa", ""),
                "적합도": payload.get("fit_level", ""),
                "채용 의견": payload.get("hiring_opinion", ""),
                "분기": result.get("branch", ""),
                "outbox 실패": failed_outbox_count(record),
            }
        )
    st.dataframe(overview_rows, use_container_width=True, hide_index=True)

    selected = st.selectbox(
        "상세 조회할 지원자",
        options=records,
        format_func=_format_record_label,
        key="interview_detail_select",
    )
    payload = selected.get("payload") or {}
    result = selected.get("pipeline_result") or {}

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.markdown("**평가 요약**")
        st.write(payload.get("summary") or "(요약 없음)")
        st.markdown("**추천 다음 단계**")
        st.write(payload.get("recommended_next_step") or "(다음 단계 없음)")
    with col_b:
        st.markdown("**파이프라인**")
        st.write(f"면접 저장: {result.get('interview_saved', ['', ''])[1] if result else '-'}")
        st.write(f"자격 필터: {result.get('screening_reason', '-')}")
        st.write(f"분기: {result.get('branch', '-')}")

    if payload.get("concerns"):
        st.markdown("**우려사항**")
        for concern in payload["concerns"]:
            st.markdown(f"- {concern}")

    with st.expander("평가 JSON 보기"):
        st.json(payload)
    with st.expander("대화록 보기"):
        st.code(payload.get("transcript") or "(대화록 없음)", language="text")


def render_outbox(records: list[dict]) -> None:
    st.subheader("outbox / 파이프라인 상태")
    if not records:
        st.info("표시할 파이프라인 기록이 없습니다.")
        return

    failed_records = [record for record in records if failed_outbox_count(record) > 0]
    st.caption("실패 outbox 재전송은 interviews 행을 다시 저장하지 않고, 실패한 outbox/pipeline_log 요청만 다시 보냅니다.")

    selected = st.selectbox(
        "outbox 상태를 확인할 지원자",
        options=records,
        format_func=_format_record_label,
        key="outbox_detail_select",
    )
    rows = _result_rows(selected)
    if rows:
        st.dataframe(
            [
                {"target": row["target"], "status": row["status"], "message": row["message"]}
                for row in rows
            ],
            use_container_width=True,
            hide_index=True,
        )
        with st.expander("전송 row 원본 보기"):
            st.json(rows)
    else:
        st.info("이 기록에는 outbox 전송 결과가 없습니다.")

    st.divider()
    st.markdown("**실패 outbox 재전송**")
    if not failed_records:
        st.success("현재 로컬 기록 기준 실패한 outbox가 없습니다.")
        return

    retry_target = st.selectbox(
        "재전송할 지원자",
        options=failed_records,
        format_func=_format_record_label,
        key="retry_record_select",
    )
    if st.button("실패 outbox만 재전송", type="primary", use_container_width=True):
        result = pipeline_result_from_dict(retry_target.get("pipeline_result"))
        if result is None:
            st.error("재전송할 파이프라인 결과를 복원하지 못했습니다.")
            return
        updated = retry_failed_outbox(result)
        update_pipeline_result(retry_target["record_id"], updated)
        st.success("재전송을 시도했습니다. 최신 결과로 새로고침합니다.")
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

    if st.button("설정 저장", use_container_width=True, type="primary"):
        save_recruiter_config(
            {
                "positions": st.session_state.recruiter_positions,
                "general_criteria": general_criteria.strip(),
            }
        )
        st.success("채용 설정이 저장되었습니다. 다음 면접부터 적용됩니다.")

    if config.get("updated_at"):
        st.caption(f"마지막 저장: {config['updated_at']}")


# ---------------------------------------------------------------------------
# App shell
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="HireCopilot - 통합 관리자 콘솔",
    page_icon="👔",
    layout="wide",
)
st.title("HireCopilot 통합 관리자 콘솔")
st.caption("면접 운영 현황, 파이프라인 상태, 채용 기준 설정을 한 곳에서 확인합니다.")

if not st.session_state.get("recruiter_authenticated"):
    st.subheader("채용 담당자 인증")
    pw = st.text_input("암호", type="password", key="recruiter_login_pw")
    if st.button("로그인", key="recruiter_login_btn", use_container_width=True):
        if (RECRUITER_PASSWORD and pw == RECRUITER_PASSWORD) or not RECRUITER_PASSWORD:
            st.session_state.recruiter_authenticated = True
            st.rerun()
        else:
            st.error("암호가 올바르지 않습니다.")
    st.stop()

config = load_recruiter_config()
records = list_interview_records(limit=100)

with st.sidebar:
    st.header("관리 메뉴")
    if st.button("새로고침", use_container_width=True):
        st.rerun()
    if st.button("로그아웃", key="recruiter_logout", use_container_width=True):
        st.session_state.recruiter_authenticated = False
        st.session_state.pop("recruiter_positions", None)
        st.rerun()

tab_dashboard, tab_interviews, tab_outbox, tab_settings = st.tabs(
    ["대시보드", "지원자/면접 결과", "outbox/파이프라인", "채용 담당 설정"]
)

with tab_dashboard:
    render_dashboard(records)

with tab_interviews:
    render_interviews(records)

with tab_outbox:
    render_outbox(records)

with tab_settings:
    render_recruiting_settings(config)
