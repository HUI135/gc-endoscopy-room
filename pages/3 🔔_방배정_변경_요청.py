import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
import re
import uuid
from zoneinfo import ZoneInfo
import menu
import os

# --- 페이지 설정 및 메뉴 호출 ---
st.set_page_config(page_title="방배정 변경 요청", page_icon="🔔", layout="wide")
st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# --- 로그인 체크 ---
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# --- 상수 및 기본 설정 ---
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0]
REQUEST_SHEET_NAME = f"{MONTH_STR} 방배정 변경요청"

if "pending_swap" not in st.session_state:
    st.session_state.pending_swap = None

# --- 함수 정의 ---
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"Google Sheets 인증 정보를 불러오는 데 실패했습니다: {e}")
        return None

@st.cache_data(ttl=300)
def load_room_data(month_str):
    try:
        gc = get_gspread_client()
        if not gc: return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 방배정")
        records = worksheet.get_all_records()
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        if '날짜' not in df.columns:
            st.error("오류: Google Sheets 시트에 '날짜' 열이 없습니다.")
            return pd.DataFrame()
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(YEAR_STR + '년 ' + df['날짜'].astype(str), format='%Y년 %m월 %d일', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{month_str} 방배정' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"방 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

# [수정] 요청 목록 관련 함수들
@st.cache_data(ttl=30)
def get_my_room_requests(month_str, employee_id):
    if not employee_id: return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        # [수정] 새로운 컬럼 이름으로 헤더 정의
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '요청 근무일', '요청자 방배정', '상대방', '상대방 방배정']
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=len(headers))
            worksheet.append_row(headers)
            return []
        all_requests = worksheet.get_all_records()
        # [수정] '요청자 사번'으로 필터링
        my_requests = [req for req in all_requests if str(req.get('요청자 사번')) == str(employee_id)]
        return my_requests
    except Exception as e:
        st.error(f"요청 목록을 불러오는 중 오류 발생: {e}")
        return []

def add_room_request_to_sheet(request_data, month_str):
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        # [수정] 새로운 컬럼 순서에 맞게 데이터 추가
        row_to_add = [
            request_data.get('RequestID'), request_data.get('요청일시'), request_data.get('요청자'),
            request_data.get('요청자 사번'), request_data.get('요청 근무일'), request_data.get('요청자 방배정'),
            request_data.get('상대방'), request_data.get('상대방 방배정')
        ]
        worksheet.append_row(row_to_add)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"교환 요청 저장 실패: {e}")
        return False

def delete_room_request_from_sheet(request_id, month_str):
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        cell = worksheet.find(request_id)
        if cell:
            worksheet.delete_rows(cell.row)
            st.cache_data.clear()
            return True
        st.error("삭제할 요청을 찾을 수 없습니다.")
        return False
    except Exception as e:
        st.error(f"요청 삭제 중 오류 발생: {e}")
        return False

def get_person_room_assignments(df, person_name):
    assignments = []
    sorted_df = df.sort_values(by='날짜_dt').reset_index(drop=True)
    def sort_key(col_name):
        match = re.match(r"(\d{1,2}:\d{2})", str(col_name))
        if match:
            time_str = match.group(1);
            return datetime.strptime(f"0{time_str}" if ':' in time_str and len(time_str.split(':')[0]) == 1 else time_str, "%H:%M").time()
        return datetime.max.time()
    time_cols = sorted([col for col in df.columns if re.match(r"(\d{1,2}:\d{2})", str(col))], key=sort_key)
    for _, row in sorted_df.iterrows():
        dt = row['날짜_dt']
        date_str = dt.strftime("%m월 %d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        for col in time_cols:
            if '온콜' in str(col) or '당직' in str(col): continue
            current_person = row.get(col)
            if person_name == "" or current_person == person_name:
                if current_person:
                    assignments.append({'date_obj': dt.date(), 'column_name': str(col), 'person_name': current_person, 'display_str': f"{date_str} - {col}"})
    return assignments

def get_shift_period(column_name):
    if re.compile(r"^(8:30|9:00|9:30|10:00|10:30|11:00|11:30)").match(str(column_name)): return "오전"
    if re.compile(r"^(13:30|14:00|14:30|15:00|15:30|16:00|16:30|17:00)").match(str(column_name)): return "오후"
    return "기타"

# --- 메인 로직 ---
user_name = st.session_state.get("name", "")
employee_id = st.session_state.get("employee_id", "")

st.header(f"📅 {user_name} 님의 {MONTH_STR} 방배정 변경 요청", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.rerun()

df_room = load_room_data(MONTH_STR)
if df_room.empty:
    st.warning("방 데이터를 불러올 수 없거나 데이터가 비어있습니다.")
else:
    st.dataframe(df_room.drop(columns=['날짜_dt'], errors='ignore'), use_container_width=True)
    st.divider()

    st.subheader("✨ 방 교환 요청하기")
    st.write("- 변경은 같은 일자 같은 시간대(오전/오후)끼리만 가능합니다.")

    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🟢 변경할 근무일자 선택</h6>", unsafe_allow_html=True)
    
    user_assignments = get_person_room_assignments(df_room, user_name)
    if not user_assignments:
        st.warning(f"'{user_name}'님의 교환 가능한 배정된 방이 없습니다. (온콜/당직 제외)")
    else:
        assignment_options = {a['display_str']: a for a in user_assignments}
        cols = st.columns([2, 2, 1])
        with cols[0]:
            my_selected_shift_str = st.selectbox("**요청 일자**", assignment_options.keys(), index=None, placeholder="변경을 원하는 나의 근무 선택")
        with cols[1]:
            compatible_assignments = []
            if my_selected_shift_str:
                my_shift = assignment_options[my_selected_shift_str]
                all_assignments = get_person_room_assignments(df_room, "")
                compatible_assignments = [
                    a for a in all_assignments 
                    if a['date_obj'] == my_shift['date_obj'] and 
                       get_shift_period(a['column_name']) == get_shift_period(my_shift['column_name']) and
                       a['person_name'] and a['person_name'] != user_name
                ]
            colleague_options_dict = {f"{p['person_name']} - {p['column_name']}": p for p in compatible_assignments}
            selected_colleague_str = st.selectbox("**교환할 인원**", colleague_options_dict.keys(), index=None, placeholder="교환할 인원을 선택하세요")
        with cols[2]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            if st.button("➕ 요청 추가", use_container_width=True, disabled=(not my_selected_shift_str or not selected_colleague_str)):
                my_shift_info = assignment_options[my_selected_shift_str]
                selected_colleague_info = colleague_options_dict[selected_colleague_str]
                
                # [수정] 새로운 데이터 형식으로 요청 생성
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "요청 근무일": my_shift_info['display_str'].split('-')[0].strip() + " " + get_shift_period(my_shift_info['column_name']),
                    "요청자 방배정": my_shift_info['column_name'],
                    "상대방": selected_colleague_info['person_name'],
                    "상대방 방배정": selected_colleague_info['column_name'],
                }
                with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                    if add_room_request_to_sheet(new_request, MONTH_STR):
                        st.success("교환 요청이 성공적으로 기록되었습니다.")
                        st.rerun()

    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 방배정 변경 요청 목록")
    my_requests = get_my_room_requests(MONTH_STR, employee_id)

    HTML_CARD_TEMPLATE = (
        '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
            '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
                '<thead><tr>'
                    '<th style="font-weight: bold; color: #555; width: 40%; padding-bottom: 8px; font-size: 1.0em;">일자</th>'
                    '<th style="font-weight: bold; color: #2E86C1; width: 25%; padding-bottom: 8px; font-size: 1.0em;">나의 방배정</th>'
                    '<th style="font-weight: bold; color: #28B463; width: 35%; padding-bottom: 8px; font-size: 1.0em;">교환 방배정</th>'
                '</tr></thead>'
                '<tbody><tr>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{date_header}</td>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{my_room}</td>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{their_room} (<strong style="color:#1E8449;">{their_name}</strong> 님)</td>'
                '</tr></tbody>'
            '</table>'
            '<hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 15px 0 10px 0;">'
            '<div style="text-align: right; font-size: 0.85em; color: #757575;">요청 시간: {timestamp}</div>'
        '</div>'
    )

    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        for req in my_requests:
            col1, col2 = st.columns([5, 1])
            with col1:
                # [수정] 새로운 데이터 키를 기존 HTML 템플릿에 매핑
                card_html = HTML_CARD_TEMPLATE.format(
                    date_header=req.get('요청 근무일', ''),
                    my_room=req.get('요청자 방배정', ''),
                    their_room=req.get('상대방 방배정', ''),
                    their_name=req.get('상대방', ''),
                    timestamp=req.get('요청일시', '')
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                st.markdown("<div style='height: 35px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️ 삭제", key=req.get('RequestID', str(uuid.uuid4())), use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        if delete_room_request_from_sheet(req.get('RequestID'), MONTH_STR):
                            st.rerun()