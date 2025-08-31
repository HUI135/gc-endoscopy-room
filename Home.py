import streamlit as st
import time
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import menu
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from datetime import date

# set_page_config는 가장 먼저 호출
st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪", layout="wide")

st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# --- 기본 설정 및 함수 ---
USER_PASSWORD = st.secrets["passwords"]["user"]
ADMIN_PASSWORD = st.secrets["passwords"]["admin"]
ADMINISTRATOR1 = st.secrets["passwords"]["administrator1"]
ADMINISTRATOR2 = st.secrets["passwords"]["administrator2"]
ADMINISTRATOR3 = st.secrets["passwords"]["administrator3"]

# --- 공지사항 데이터 관리 (JSON 파일 사용) ---
NOTICES_FILE = "notices.json"

def load_notices():
    """JSON 파일에서 공지사항 데이터를 로드합니다."""
    try:
        with open(NOTICES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []  # 빈 리스트 반환

def save_notices(notices):
    """공지사항 데이터를 JSON 파일에 저장합니다."""
    try:
        with open(NOTICES_FILE, "w", encoding="utf-8") as f:
            json.dump(notices, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"공지사항 저장 오류: {e}")

# 공지사항 초기화
if "notices" not in st.session_state:
    st.session_state["notices"] = load_notices()

@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

@st.cache_data(show_spinner=False)
def load_mapping_data():
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        mapping_worksheet = sheet.worksheet("매핑")
        mapping_data = mapping_worksheet.get_all_records()
        return pd.DataFrame(mapping_data)
    except Exception as e:
        st.error(f"매핑 시트 로딩 오류: {e}")
        return None

def get_employee_name(employee_id):
    df_map = load_mapping_data()
    if df_map is None: return None
    try:
        employee_id_int = int(employee_id)
        employee_row = df_map[df_map["사번"] == employee_id_int]
        return employee_row.iloc[0]["이름"] if not employee_row.empty else None
    except (ValueError, IndexError):
        return None

# --- 세션 상태 초기화 ---
if "login_success" not in st.session_state:
    st.session_state["login_success"] = False
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False
if "admin_mode" not in st.session_state:
    st.session_state["admin_mode"] = False

# --- UI 및 로직 시작 ---
image_url = 'http://www.snuh.org/upload/about/hi/15e707df55274846b596e0d9095d2b0e.png'
st.markdown(f"""
    <div style="display: flex; align-items: center;">
        <img src="{image_url}" width="130" alt="SNUH Logo">
        <div style="margin-left: 20px;">
            <h1 style="margin-bottom: 0;">🏥 강남센터 내시경실 시스템</h1>
            <div style='font-size: 14px; color: grey;'>오류 문의: 헬스케어연구소 데이터 연구원 김희연 (hui135@snu.ac.kr)</div>
        </div>
    </div>
""", unsafe_allow_html=True)
st.divider()

# --- 로그인 처리 ---
if not st.session_state["login_success"]:
    with st.form("login_form"):
        password = st.text_input("🔹 비밀번호를 입력해주세요.", type="password")
        employee_id = st.text_input("🔹 사번(5자리)을 입력해주세요.")
        submitted = st.form_submit_button("확인")
        if submitted:
            if password != USER_PASSWORD:
                st.error("비밀번호를 다시 확인해주세요.")
            elif employee_id:
                employee_name = get_employee_name(employee_id)
                if employee_name:
                    st.session_state["login_success"] = True
                    st.session_state["employee_id"] = int(employee_id)
                    st.session_state["name"] = employee_name
                    st.session_state["is_admin"] = int(employee_id) in [ADMINISTRATOR1, ADMINISTRATOR2, ADMINISTRATOR3]
                    st.rerun()
                else:
                    st.error("사번이 매핑된 이름이 없습니다.")
            else:
                st.warning("사번을 입력해주세요.")

# --- 로그인 성공 후 처리 ---
if st.session_state["login_success"]:
    st.markdown(f"#### 👋 {st.session_state['name']}님, 안녕하세요!")
    st.info("왼쪽 사이드바의 메뉴에서 원하시는 작업을 선택해주세요.")
    
    # 관리자일 경우, 관리자 모드 전환 옵션 제공
    if st.session_state["is_admin"]:
        if st.session_state["admin_mode"]:
            st.success("관리자 모드가 활성화되었습니다. 사이드바에서 관리자 메뉴를 이용하세요.")
        else:
            with st.expander("🔑 관리자 모드로 전환하기"):
                admin_password = st.text_input("관리자 비밀번호를 입력하세요.", type="password", key="admin_password")
                if st.button("관리자 인증"):
                    if admin_password == ADMIN_PASSWORD:
                        st.session_state["admin_mode"] = True
                        st.rerun()
                    else:
                        st.error("관리자 비밀번호가 틀렸습니다.")

    # --- 공지사항 및 오류 보고 섹션 ---
    st.divider()
    st.subheader("📢 공지사항 및 오류 보고")
    if not st.session_state["notices"]:  # 공지사항이 비어 있는지 확인
        st.info("현재 등록된 공지사항이 없습니다.")
    else:
        notices_df = pd.DataFrame(st.session_state["notices"]).sort_values(by="날짜", ascending=False)  # 최신순 정렬
        with st.expander("ℹ️ 공지사항 목록 보기", expanded=True):
            for idx, row in notices_df.iterrows():
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.markdown(f"- **{row['제목']}** ({row['날짜']})")
                    st.markdown(f'<div style="padding-left: 20px; padding-bottom: 10px;">{row["내용"]}</div>', unsafe_allow_html=True)
                with col2:
                    if st.session_state["is_admin"] and st.session_state["admin_mode"]:
                        if st.button("삭제", key=f"delete_notice_{idx}"):
                            st.session_state["notices"].pop(idx)
                            save_notices(st.session_state["notices"])
                            st.success("공지사항이 성공적으로 삭제되었습니다.")
                            time.sleep(1)
                            st.rerun()
                st.write()

    # --- 관리자용 공지사항 입력 폼 ---
    if st.session_state["is_admin"] and st.session_state["admin_mode"]:
        with st.expander("📝 공지사항 추가 [관리자 전용]", expanded=False):
            with st.form(key="add_notice_form"):
                notice_title = st.text_input("공지사항 제목")
                notice_content = st.text_area("공지사항 내용")
                notice_date = st.date_input("공지사항 날짜", value=date.today())
                submit_notice = st.form_submit_button("➕추가")
                
                if submit_notice and notice_title and notice_content:
                    new_notice = {
                        "제목": notice_title,
                        "내용": notice_content,
                        "날짜": notice_date.strftime("%Y-%m-%d")
                    }
                    st.session_state["notices"].append(new_notice)
                    save_notices(st.session_state["notices"])
                    st.success("공지사항이 성공적으로 추가되었습니다.")
                    time.sleep(1)
                    st.rerun()

    with st.expander("⚠️ 오류사항 보고하기"):
        with st.form(key="error_report_form"):
            error_report = st.text_area("오류 보고", placeholder="발생한 오류나 개선 제안을 자세히 설명해주세요.")
            submit_button = st.form_submit_button("💌 전송")

            if submit_button and error_report:
                try:
                    sender_email = st.secrets["email"]["sender_email"]
                    sender_password = st.secrets["email"]["sender_password"]
                    receiver_email = "hui135@snu.ac.kr"

                    user_name = st.session_state.get('name', '익명')
                    user_id = st.session_state.get('employee_id', '알 수 없음')
                    
                    subject = f"강남센터 내시경실 시스템 오류 보고 - {user_name} ({user_id})"
                    body = f"""
사용자: {user_name} (사번: {user_id})
오류 보고 시간: {time.strftime('%Y-%m-%d %H:%M:%S')}
내용:
{error_report}
"""
                    msg = MIMEMultipart()
                    msg["From"] = sender_email
                    msg["To"] = receiver_email
                    msg["Subject"] = subject
                    msg.attach(MIMEText(body, "plain"))

                    with smtplib.SMTP("smtp.gmail.com", 587) as server:
                        server.starttls()
                        server.login(sender_email, sender_password)
                        server.sendmail(sender_email, receiver_email, msg.as_string())

                    st.success("오류 보고가 성공적으로 전송되었습니다! 감사합니다.")
                    
                except smtplib.SMTPAuthenticationError:
                    st.error("이메일 인증에 실패했습니다. Streamlit secrets에 저장된 '앱 비밀번호'가 올바른지 확인해주세요.")
                except Exception as e:
                    st.error(f"오류 보고 전송 중 예상치 못한 문제가 발생했습니다: {e}")
                    
            elif submit_button and not error_report:
                st.warning("오류 내용을 입력해주세요.")