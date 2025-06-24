import streamlit as st
import time
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import menu # 1단계에서 만든 menu.py를 import

st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪", layout="wide")

# menu.py의 menu() 함수를 호출하여 사이드바를 생성합니다.
menu.menu()

# --- 기본 설정 및 함수 (기존과 동일) ---
USER_PASSWORD = st.secrets["passwords"]["user"]
ADMIN_PASSWORD = st.secrets["passwords"]["admin"]
ADMINISTRATOR1 = st.secrets["passwords"]["administrator1"]
ADMINISTRATOR2 = st.secrets["passwords"]["administrator2"]

@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

@st.cache_data
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
        <img src="{image_url}" width="130">
        <div style="margin-left: 20px;">
            <h1 style="margin-bottom: 0;">🏥 강남센터 내시경실 시스템</h1>
            <div style='font-size: 14px; color: grey;'>오류 문의: 헬스케어연구소 데이터 연구원 김희연 (hui135@snu.ac.kr)</div>
        </div>
    </div>
""", unsafe_allow_html=True)
st.divider()

# --- 로그인 처리 ---
if not st.session_state["login_success"]:
    password = st.text_input("비밀번호를 입력해주세요.", type="password")
    employee_id = st.text_input("사번(5자리)을 입력해주세요.")
    if st.button("확인"):
        if password != USER_PASSWORD:
            st.error("비밀번호를 다시 확인해주세요.")
        elif employee_id:
            employee_name = get_employee_name(employee_id)
            if employee_name:
                st.session_state["login_success"] = True
                st.session_state["employee_id"] = int(employee_id)
                st.session_state["name"] = employee_name
                st.session_state["is_admin"] = int(employee_id) in [ADMINISTRATOR1, ADMINISTRATOR2]
                st.rerun()
            else:
                st.error("사번이 매핑된 이름이 없습니다.")

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