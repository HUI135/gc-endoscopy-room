from googleapiclient.discovery import build  # import도 필요!
import streamlit as st
import time
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound

st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪", layout="wide")

USER_PASSWORD = st.secrets["passwords"]["user"]
ADMIN_PASSWORD = st.secrets["passwords"]["admin"]
ADMINISTRATOR1 = st.secrets["passwords"]["administrator1"]
ADMINISTRATOR2 = st.secrets["passwords"]["administrator2"]

# 상단 정보 표시
image_url = 'http://www.snuh.org/upload/about/hi/15e707df55274846b596e0d9095d2b0e.png'
title_html = "<h1 style='display: inline-block; margin: 0;'>🏥 강남센터 내시경실 시스템</h1>"
contact_info_html = """
<div style='text-align: left; font-size: 14px; color: grey;'>
오류 문의: 헬스케어연구소 데이터 연구원 김희연 (hui135@snu.ac.kr)</div>"""

col1, col2 = st.columns([1, 4])
with col1:
    st.image(image_url, width=100)
with col2:
    st.markdown(title_html, unsafe_allow_html=True)
    st.markdown(contact_info_html, unsafe_allow_html=True)
st.divider()

# 세션 상태 초기화
if "login_success" not in st.session_state:
    st.session_state["login_success"] = False
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False
if "is_admin_authenticated" not in st.session_state:
    st.session_state["is_admin_authenticated"] = False
if "gspread_client" not in st.session_state:
    st.session_state["gspread_client"] = None
if "sheet" not in st.session_state:
    st.session_state["sheet"] = None
if "mapping_df" not in st.session_state:
    st.session_state["mapping_df"] = None

# ✅ 구글 시트 클라이언트 생성 함수
def get_gspread_client():
    if st.session_state["gspread_client"] is None:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        st.session_state["gspread_client"] = gspread.authorize(credentials)
    return st.session_state["gspread_client"]

def extract_spreadsheet_id(url):
    try:
        return url.split("/d/")[1].split("/")[0]
    except Exception:
        st.error("❌ 구글 시트 URL에서 ID 추출 실패. URL 형식을 확인하세요.")
        return None

# ✅ 구글 시트 열기 (캐싱)
def get_sheet():
    if st.session_state["sheet"] is None:
        url = st.secrets["google_sheet"]["url"]
        gc = get_gspread_client()
        st.session_state["sheet"] = gc.open_by_url(url)
    return st.session_state["sheet"]

# ✅ 매핑 데이터 불러오기 (캐싱)
def load_mapping_data():
    if st.session_state["mapping_df"] is None:
        try:
            sheet = get_sheet()
            mapping_worksheet = sheet.worksheet("매핑")
            mapping_data = mapping_worksheet.get_all_records()
            st.session_state["mapping_df"] = pd.DataFrame(mapping_data)
        except WorksheetNotFound:
            st.error("매핑 시트를 찾을 수 없습니다. 확인해 주세요.")
            return None
        except Exception as e:
            st.error(f"매핑 시트에서 데이터를 불러오는 데 문제가 발생했습니다: {e}")
            return None
    return st.session_state["mapping_df"]

# 사번으로 이름 찾기
def get_employee_name(employee_id):
    mapping_df = load_mapping_data()
    if mapping_df is None:
        return None
    try:
        employee_id_int = int(employee_id)
        employee_id_str = str(employee_id_int).zfill(5)
        employee_row = mapping_df[mapping_df["사번"] == employee_id_int]
        if not employee_row.empty:
            return employee_row.iloc[0]["이름"]
        else:
            return None
    except ValueError:
        st.error("사번은 숫자만 입력 가능합니다.")
        return None

# 로그인 정보 입력
if not st.session_state["login_success"]:
    password = st.text_input("비밀번호를 입력해주세요.", type="password")
    employee_id = st.text_input("사번(5자리)을 입력해주세요.")

    if st.button("확인"):
        if password != USER_PASSWORD:
            st.error("비밀번호를 다시 확인해주세요.")
        elif employee_id:
            try:
                employee_id_int = int(employee_id)
                employee_id_str = str(employee_id_int).zfill(5)
                if len(employee_id_str) != 5:
                    st.error("사번은 5자리 숫자를 입력해 주셔야 합니다.")
                else:
                    employee_name = get_employee_name(employee_id)
                    if employee_name:
                        st.session_state["login_success"] = True
                        st.session_state["employee_id"] = employee_id_int
                        st.session_state["name"] = employee_name
                        st.success(f"{employee_name}({employee_id_str})님으로 접속하셨습니다.")
                        time.sleep(0.5)
                    else:
                        st.error("사번이 매핑된 이름이 없습니다.")
            except ValueError:
                st.error("사번은 숫자만 입력 가능합니다.")

# 로그인 성공 후 처리
if st.session_state["login_success"]:
    # 관리자 여부 확인
    is_admin = st.session_state["employee_id"] in [ADMINISTRATOR1, ADMINISTRATOR2]
    st.session_state["is_admin"] = is_admin

    if is_admin and not st.session_state["is_admin_authenticated"]:
        st.write(" ")
        admin_password = st.text_input("관리자 페이지 접근을 위한 비밀번호를 입력해주세요.", type="password", key="admin_password")
        if st.button("관리자 인증"):
            if admin_password == ADMIN_PASSWORD:
                st.session_state["is_admin_authenticated"] = True
                st.success("승인되었습니다. 관리자 페이지에 접속합니다.")
                time.sleep(2)
                st.switch_page("pages/4 [관리자]_스케쥴_관리.py")
            else:
                st.error("비밀번호가 틀렸습니다. 다시 시도해 주세요.")
    elif st.session_state["is_admin_authenticated"]:
        st.switch_page("pages/4 [관리자]_스케쥴_관리.py")
    else:
        # 일반 사용자: 기본 페이지로 이동
        st.switch_page("pages/1 📅_마스터_수정.py")