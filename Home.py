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
from datetime import date

# set_page_config는 가장 먼저 호출
st.set_page_config(page_title="GC 내시경 마스터", page_icon="🧪", layout="wide")
st.info("09/15 업데이트 내역: 모바일 UI 개선, 캘린더에 휴관일 표시 추가", icon="📢")
st.success("검토")
st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# --- 기본 설정 및 함수 ---
USER_PASSWORD = st.secrets["passwords"]["user"]
ADMIN_PASSWORD = st.secrets["passwords"]["admin"]
ADMINISTRATOR1 = st.secrets["passwords"]["administrator1"]
ADMINISTRATOR2 = st.secrets["passwords"]["administrator2"]
ADMINISTRATOR3 = st.secrets["passwords"]["administrator3"]

# --- [변경] 공지사항 데이터 관리 (구글 시트 사용) ---

# @st.cache_resource는 gspread 클라이언트 같이 한 번만 생성해야 하는 리소스에 사용
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# @st.cache_data는 데이터 자체를 캐싱할 때 사용. ttl(time-to-live)로 캐시 유효기간 설정 가능
@st.cache_data(ttl=600, show_spinner=False) # 10분 동안 캐시 유지
def load_notices_from_sheet():
    """Google Sheet에서 공지사항 데이터를 로드합니다."""
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = sheet.worksheet("공지사항")  # '공지사항' 시트를 선택
        notices = worksheet.get_all_records()
        # gspread가 빈 값을 None으로 가져올 수 있으므로, 안전하게 빈 문자열로 변환
        df = pd.DataFrame(notices).fillna("")
        return df.to_dict('records')
    except WorksheetNotFound:
        st.error("'공지사항' 시트를 찾을 수 없습니다. Google Sheet에 해당 이름의 시트가 있는지 확인해주세요.")
        return []
    except Exception as e:
        st.error(f"공지사항 로딩 중 오류 발생: {e}")
        return []

def add_notice_to_sheet(notice):
    """Google Sheet에 새 공지사항을 추가합니다."""
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = sheet.worksheet("공지사항")
        # 구글 시트의 헤더 순서('제목', '내용', '날짜')에 맞게 값을 리스트로 전달
        worksheet.append_row([notice["제목"], notice["내용"], notice["날짜"]])
        st.cache_data.clear()  # 데이터가 변경되었으므로 캐시를 초기화
    except Exception as e:
        st.error(f"공지사항 추가 중 오류 발생: {e}")

def delete_notice_from_sheet(notice_to_delete):
    """Google Sheet에서 특정 공지사항을 삭제합니다."""
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = sheet.worksheet("공지사항")
        
        # 삭제할 공지사항의 '제목'으로 모든 일치하는 셀을 찾음
        cell_list = worksheet.findall(notice_to_delete['제목'])
        
        # 찾은 셀들을 역순으로 순회 (삭제 시 행 번호가 바뀌는 문제를 피하기 위함)
        for cell in reversed(cell_list):
            row_values = worksheet.row_values(cell.row)
            # 해당 행의 제목과 날짜가 삭제하려는 공지사항과 일치하는지 한 번 더 확인
            if row_values[0] == notice_to_delete['제목'] and row_values[2] == notice_to_delete['날짜']:
                worksheet.delete_rows(cell.row)
                st.cache_data.clear() # 데이터가 변경되었으므로 캐시를 초기화
                return True # 삭제 성공
        return False # 일치하는 항목을 찾지 못함
    except Exception as e:
        st.error(f"공지사항 삭제 중 오류 발생: {e}")
        return False

# --- [변경] 세션 상태 초기화 시 구글 시트에서 공지사항 로드 ---
if "notices" not in st.session_state:
    st.session_state["notices"] = load_notices_from_sheet()


# @st.cache_data(show_spinner=False)
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

# 수정 후
def get_employee_name(employee_id):
    if 'df_map' not in st.session_state or st.session_state.get('df_map') is None:
        st.session_state.df_map = load_mapping_data()

    df_map = st.session_state.df_map
    if df_map is None:
        return None
    try:
        # ✅ 1. 시트에서 가져온 사번을 문자로 바꾸고, 5자리로 맞춤 (예: '1' -> '00001')
        df_map["사번"] = df_map["사번"].astype(str).str.zfill(5)
        # ✅ 2. 사용자가 입력한 사번도 5자리로 맞춤
        employee_id_padded = str(employee_id).zfill(5)
        
        employee_row = df_map[df_map["사번"] == employee_id_padded]
        return employee_row.iloc[0]["이름"] if not employee_row.empty else None
    except (ValueError, IndexError):
        return None

# --- 세션 상태 초기화 ---
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


# st.session_state에 "login_success"가 없으면 False로 초기화
if "login_success" not in st.session_state:
    st.session_state["login_success"] = False

# --- [변경] 로그인 로직을 처리할 콜백 함수 정의 ---
def attempt_login():
    """폼 제출 시 호출될 로그인 처리 함수"""
    # key를 이용해 위젯의 현재 값에 접근합니다.
    password_input = st.session_state.get("password_input", "")
    employee_id_input = st.session_state.get("employee_id_input", "")

    if password_input != USER_PASSWORD:
        st.error("비밀번호를 다시 확인해주세요.")
    elif employee_id_input:
        employee_name = get_employee_name(employee_id_input)
        if employee_name:
            st.session_state["login_success"] = True
            st.session_state["employee_id"] = employee_id_input # ✅ int() 변환 제거, 문자열 그대로 저장
            st.session_state["name"] = employee_name
            # ✅ is_admin 확인 시에는 int()로 변환하여 비교 (ADMINISTRATOR 변수들이 숫자일 경우)
            st.session_state["is_admin"] = int(employee_id_input) in [ADMINISTRATOR1, ADMINISTRATOR2, ADMINISTRATOR3]
            st.rerun()
        else:
            st.error("사번이 매핑된 이름이 없습니다.")
    else:
        st.warning("사번을 입력해주세요.")

# --- [변경] 로그인 UI 및 로직 ---
if not st.session_state["login_success"]:
    with st.form("login_form"):
        st.text_input("🔹 비밀번호를 입력해주세요.", type="password", key="password_input")
        st.text_input("🔹 사번(5자리)을 입력해주세요.", key="employee_id_input")

        # ✅ on_click 제거하고 반환값 사용
        submitted = st.form_submit_button("확인")

    # ✅ 메인 흐름에서 스피너 표시
    if submitted:
        with st.spinner("접속 중입니다..."):
            time.sleep(1)
            attempt_login()

# --- 로그인 성공 후 처리 ---
if st.session_state["login_success"]:
    st.markdown(f"#### 👋 {st.session_state['name']}님, 안녕하세요!")
    st.info("왼쪽 사이드바의 메뉴에서 원하시는 작업을 선택해주세요.")
    
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

    # [변경] st.session_state["notices"]를 직접 사용. 비어있으면 load_notices_from_sheet가 빈 리스트를 반환.
    if not st.session_state["notices"]:
        st.info("현재 등록된 공지사항이 없습니다.")
    else:
        # 데이터프레임으로 변환하여 날짜순 정렬
        notices_df = pd.DataFrame(st.session_state["notices"]).sort_values(by="날짜", ascending=False)
        with st.expander("ℹ️ 공지사항 목록 보기", expanded=True):
            for idx, row in notices_df.iterrows():
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.markdown(f"- **{row['제목']}** ({row['날짜']})")
                    # 줄바꿈(\n)을 HTML <br> 태그로 변환하여 내용에 반영
                    content_with_br = row['내용'].replace('\\n', '<br>')
                    st.markdown(f'<div style="padding-left: 20px; padding-bottom: 10px;">{content_with_br}</div>', unsafe_allow_html=True)
                    st.write(" ")
                with col2:
                    if st.session_state["is_admin"] and st.session_state["admin_mode"]:
                        # [변경] 삭제 버튼 로직 수정
                        if st.button("삭제", key=f"delete_notice_{idx}"):
                            notice_to_delete = row.to_dict()
                            if delete_notice_from_sheet(notice_to_delete):
                                st.success("공지사항이 성공적으로 삭제되었습니다.")
                                # 세션 상태를 다시 로드하여 UI에 즉시 반영
                                st.session_state["notices"] = load_notices_from_sheet()
                            else:
                                st.error("공지사항 삭제에 실패했습니다.")
                            time.sleep(1)
                            st.rerun()

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
                    # [변경] 구글 시트에 직접 추가하는 함수 호출
                    add_notice_to_sheet(new_notice)
                    # 세션 상태를 다시 로드하여 UI에 즉시 반영
                    st.session_state["notices"] = load_notices_from_sheet()
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

                    st.success("입력 내용이 성공적으로 전송되었습니다! 감사합니다.")
                except smtplib.SMTPAuthenticationError:
                    st.error("이메일 인증에 실패했습니다. Streamlit secrets에 저장된 '앱 비밀번호'가 올바른지 확인해주세요.")
                except Exception as e:
                    st.error(f"오류 보고 전송 중 예상치 못한 문제가 발생했습니다: {e}")
            elif submit_button and not error_report:
                st.warning("오류 내용을 입력해주세요.")