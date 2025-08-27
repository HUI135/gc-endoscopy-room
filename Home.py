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

# set_page_config는 가장 먼저 호출
st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪", layout="wide")

# 그 이후에 다른 Streamlit 명령 포함 가능
st.session_state.current_page = os.path.basename(__file__) # 이 부분은 menu.py와 연관되어 있으므로 그대로 둡니다.

# menu.py의 menu() 함수 호출
menu.menu() # menu.py 파일이 없으므로, 실행을 위해 임시로 주석 처리합니다. 실제 환경에서는 주석을 해제하세요.

# --- 기본 설정 및 함수 (기존과 동일) ---
# st.secrets에서 비밀번호를 불러오는 것은 보안상 좋은 방법입니다.
USER_PASSWORD = st.secrets["passwords"]["user"]
ADMIN_PASSWORD = st.secrets["passwords"]["admin"]
ADMINISTRATOR1 = st.secrets["passwords"]["administrator1"]
ADMINISTRATOR2 = st.secrets["passwords"]["administrator2"]
ADMINISTRATOR3 = st.secrets["passwords"]["administrator3"]

@st.cache_resource
def get_gspread_client():
    """Google 스프레드시트 API 클라이언트를 생성하고 캐시합니다."""
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

@st.cache_data
def load_mapping_data():
    """매핑 데이터를 Google 시트에서 불러와 DataFrame으로 변환하고 캐시합니다."""
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
    """사번을 사용하여 직원 이름을 조회합니다."""
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
        st.divider()
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

# --- 오류 보고 섹션 ---
st.divider()
st.subheader("📢 오류 보고")
with st.form(key="error_report_form"):
    error_report = st.text_area("오류 발생 시 내용을 입력해주시면, 확인 후 수정하겠습니다:", placeholder="발생한 오류나 개선 제안을 자세히 설명해주세요.")
    submit_button = st.form_submit_button("💌 전송")

    if submit_button and error_report:
        try:
            # 이메일 설정
            sender_email = st.secrets["email"]["sender_email"]
            
            # --- 💡 중요: 오류 해결 지점 ---
            # Google 계정에 2단계 인증이 설정된 경우, 일반 비밀번호 대신 '앱 비밀번호'를 사용해야 합니다.
            # 1. Google 계정 관리 페이지로 이동합니다. (myaccount.google.com)
            # 2. '보안' 탭으로 이동하여 'Google에 로그인하는 방법' 섹션에서 '2단계 인증'을 사용 설정합니다.
            # 3. 2단계 인증 페이지 하단에서 '앱 비밀번호'를 선택합니다.
            # 4. 앱 선택에서 '메일', 기기 선택에서 'Windows 컴퓨터' (또는 기타)를 선택하고 '생성'을 누릅니다.
            # 5. 생성된 16자리 비밀번호를 복사하여 Streamlit의 secrets.toml 파일에 저장합니다.
            #
            # 예시 (secrets.toml 파일):
            # [email]
            # sender_email = "your_email@gmail.com"
            # sender_password = "abcd efgh ijkl mnop"  # 여기에 생성된 16자리 앱 비밀번호를 입력
            
            sender_password = st.secrets["email"]["sender_password"]
            receiver_email = "hui135@snu.ac.kr"

            # 이메일 메시지 구성
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

            # Gmail SMTP 서버 연결 및 메일 전송
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
