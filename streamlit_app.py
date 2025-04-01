import streamlit as st
import time
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound

# 페이지 설정
st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪")

# 상단 정보 표시
image_url = 'http://www.snuh.org/upload/about/hi/15e707df55274846b596e0d9095d2b0e.png'
title_html = "<h1 style='display: inline-block; margin: 0;'>🏥 강남센터 내시경실 시스템</h1>"
contact_info_html = """
<div style='text-align: left; font-size: 14px; color: grey;'>
오류 문의: 헬스케어연구소 데이터 연구원 김희연 (hui135@snu.ac.kr)</div>"""

col1, col2 = st.columns([1, 4])
with col1:
    st.image(image_url, width=200)
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

# ✅ 구글 시트 클라이언트 생성 함수
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    # ✨ JSON처럼 강제 파싱 (줄바꿈 처리 문제 해결)
    service_account_info = dict(st.secrets["gspread"])
    # 🟢 private_key 줄바꿈 복원
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 구글 시트 URL
url = "https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?gid=0#gid=0"
gc = get_gspread_client()
sheet = gc.open_by_url(url)

# 로그인 정보 입력
if not st.session_state["login_success"]:
    password = st.text_input("비밀번호를 입력해주세요.", type="password")
    employee_id = st.text_input("사번(5자리)을 입력해주세요")

    # "매핑" 시트에서 사번과 이름 매핑 불러오기
    def get_employee_name(employee_id):
        try:
            mapping_worksheet = sheet.worksheet("매핑")  # "매핑" 시트 불러오기
            mapping_data = mapping_worksheet.get_all_records()  # 시트에서 모든 데이터 가져오기
            mapping_df = pd.DataFrame(mapping_data)
            
            employee_id_int = int(employee_id)
            employee_id_str = str(employee_id_int).zfill(5)

            # 사번에 해당하는 이름 찾기
            employee_row = mapping_df[mapping_df["사번"] == employee_id_int]
            
            if not employee_row.empty:
                return employee_row.iloc[0]["이름"]
            else:
                return None  # 사번이 없으면 None 반환
        except WorksheetNotFound:
            st.error("매핑 시트를 찾을 수 없습니다. 확인해 주세요.")
            return None
        except Exception as e:
            st.error(f"매핑 시트에서 데이터를 불러오는 데 문제가 발생했습니다: {e}")
            return None
        except ValueError:
            st.error("사번은 숫자만 입력 가능합니다.")

    # 로그인 버튼 클릭 시 처리
    if st.button("확인"):
        if password != "rkdskatpsxj":
            st.error("비밀번호를 다시 확인해주세요.")
        elif employee_id:
            try:
                employee_id_int = int(employee_id)
                employee_id_str = str(employee_id_int).zfill(5)
                if len(employee_id_str) != 5:
                    st.error("사번은 5자리 숫자를 입력해 주셔야 합니다.")
                else:
                    # 이름 매핑
                    employee_name = get_employee_name(employee_id)

                    if employee_name:
                        # 로그인 성공
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
    # 관리자 확인
    if st.session_state["employee_id"] == 65579 and not st.session_state["is_admin_authenticated"]:
        st.write(" ")
        admin_password = st.text_input("관리자 페이지 접근을 위한 비밀번호를 입력해주세요.", type="password", key="admin_password")
        if st.button("관리자 인증"):
            if admin_password == "rkdtmdwn":
                st.session_state["is_admin_authenticated"] = True
                st.session_state["is_admin"] = True
                st.success("승인되었습니다. 관리자 페이지에 접속합니다.")
                time.sleep(2)
                st.switch_page("pages/3 [관리자]_스케쥴_관리.py")
                st.stop()  # 페이지 이동 후 코드 실행 중단
            else:
                st.error("비밀번호가 틀렸습니다. 다시 시도해 주세요.")
    elif st.session_state["is_admin_authenticated"]:
        # 이미 인증된 관리자는 바로 페이지 이동
        st.switch_page("pages/3 [관리자]_스케쥴_관리.py")
        st.stop()
    else:
        # 일반 사용자 페이지로 이동
        st.switch_page("pages/0 🔍_내_스케쥴_보기.py")
        st.stop()
