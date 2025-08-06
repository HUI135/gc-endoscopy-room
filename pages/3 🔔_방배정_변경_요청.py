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

def get_my_room_requests(month_str, employee_id):
    if not employee_id: return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '요청 근무일', '요청자 방배정', '상대방', '상대방 방배정']
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=len(headers))
            worksheet.append_row(headers)
            return []
        all_requests = worksheet.get_all_records()
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

def get_person_room_assignments(df, person_name=""):
    assignments = []
    sorted_df = df.sort_values(by='날짜_dt').reset_index(drop=True)
    def sort_key(col_name):
        match = re.search(r"(\d{1,2}:\d{2})", str(col_name))
        if match:
            time_str = match.group(1)
            return datetime.strptime(f"0{time_str}" if ':' in time_str and len(time_str.split(':')[0]) == 1 else time_str, "%H:%M").time()
        if '당직' in str(col_name) or '온콜' in str(col_name):
            return datetime.strptime("23:59", "%H:%M").time()
        return datetime.max.time()

    time_cols = sorted([col for col in df.columns if re.search(r"(\d{1,2}:\d{2})", str(col)) or '당직' in str(col) or '온콜' in str(col)], key=sort_key)
    
    for _, row in sorted_df.iterrows():
        dt = row['날짜_dt']
        date_str = dt.strftime("%m월 %d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        for col in time_cols:
            current_person = row.get(col)
            if (not person_name and current_person) or (person_name and current_person == person_name):
                assignments.append({'date_obj': dt.date(), 'column_name': str(col), 'person_name': current_person, 'display_str': f"{date_str} - {col}"})
    return assignments

def get_shift_period(column_name):
    match = re.search(r"(\d{1,2}:\d{2})", str(column_name))
    if match:
        hour = int(match.group(1).split(':')[0])
        if 8 <= hour <= 12:
            return "오전"
        elif 13 <= hour <= 18:
            return "오후"
    
    if '당직' in str(column_name) or '온콜' in str(column_name):
        return "기타"
        
    return "기타"

def is_person_assigned_at_time(df, person_name, date_obj, column_name):
    row_data = df[df['날짜_dt'].dt.date == date_obj]
    if row_data.empty:
        return False
    
    row_dict = row_data.iloc[0].to_dict()
    for col, assigned_person in row_dict.items():
        if get_shift_period(col) == get_shift_period(column_name) and assigned_person == person_name:
            return True
    return False

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
    st.stop()
else:
    st.dataframe(df_room.drop(columns=['날짜_dt'], errors='ignore'), use_container_width=True, hide_index=True)
    st.divider()

    st.subheader("✨ 방 변경 요청하기")

    # --- 나의 방배정을 상대방과 바꾸기 ---
    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🟢 나의 방배정을 상대방과 바꾸기</h6>", unsafe_allow_html=True)
    
    user_assignments_my = get_person_room_assignments(df_room, user_name)
    if not user_assignments_my:
        st.warning(f"'{user_name}'님의 배정된 방이 없습니다.")
    else:
        assignment_options_my = {a['display_str']: a for a in user_assignments_my}
        cols_my_to_them = st.columns([2, 2, 1])
        
        with cols_my_to_them[0]:
            my_selected_assignment_str_my = st.selectbox(
                "변경을 원하는 나의 방배정 선택",
                assignment_options_my.keys(),
                index=None,
                placeholder="나의 방배정을 선택하세요",
                key="my_to_them_my_select"
            )

        with cols_my_to_them[1]:
            # 모든 직원 목록 (나를 제외)
            if st.session_state.get('user_data', None):
                all_employee_names = set(st.session_state.get('user_data', {}).keys())
            else:
                time_cols_all = [col for col in df_room.columns if re.search(r"(\d{1,2}:\d{2})", str(col)) or '당직' in str(col) or '온콜' in str(col)]
                all_employee_names = set(df_room[time_cols_all].values.ravel()) - {''}
            
            compatible_colleague_names = sorted(list(all_employee_names - {user_name}))

            selected_colleague_name = st.selectbox(
                "교환할 상대방 선택",
                compatible_colleague_names,
                index=None,
                placeholder="상대방을 선택하세요",
                key="my_to_them_colleague_select"
            )
        
        request_disabled_my = True
        
        if my_selected_assignment_str_my and selected_colleague_name:
            my_selected_info = assignment_options_my[my_selected_assignment_str_my]
            
            # 내가 선택한 날짜/시간대에 상대방이 근무가 있는지 확인
            is_colleague_occupied = is_person_assigned_at_time(df_room, selected_colleague_name, my_selected_info['date_obj'], my_selected_info['column_name'])
            
            if is_colleague_occupied:
                st.warning(f"⚠️ **{selected_colleague_name}**님이 **{my_selected_info['display_str'].split('-')[0].strip()}** ({get_shift_period(my_selected_info['column_name'])})에 이미 근무가 있습니다. 중복 배치가 되지 않도록 **{selected_colleague_name}** 님의 방배정도 변경해 주십시오.")
            else:
                request_disabled_my = False

        with cols_my_to_them[2]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            if st.button("➕ 요청 추가", key="add_my_to_them_request_button", use_container_width=True, disabled=request_disabled_my or not(my_selected_assignment_str_my and selected_colleague_name)):
                my_assignment_info = assignment_options_my[my_selected_assignment_str_my]
                
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "요청 근무일": my_assignment_info['display_str'].split('-')[0].strip(),
                    "요청자 방배정": my_assignment_info['column_name'],
                    "상대방": selected_colleague_name,
                    "상대방 방배정": '근무 없음',
                }
                with st.spinner("요청을 기록하는 중입니다..."):
                    if add_room_request_to_sheet(new_request, MONTH_STR):
                        st.success("교환 요청이 성공적으로 기록되었습니다.")
                        st.rerun()

    # --- 상대방의 방배정을 나와 바꾸기 ---
    st.write(' ')
    st.markdown("<h6 style='font-weight:bold;'>🔵 상대방의 방배정을 나와 바꾸기</h6>", unsafe_allow_html=True)
    
    cols_them_to_my = st.columns([2, 2, 1])

    compatible_colleague_names_them = []

    if st.session_state.get('user_data', None):
        all_colleagues_set = set(st.session_state.get('user_data', {}).keys()) - {user_name, ''}
    else:
        time_cols_all = [col for col in df_room.columns if re.search(r"(\d{1,2}:\d{2})", str(col)) or '당직' in str(col) or '온콜' in str(col)]
        all_colleagues_set = set(df_room[time_cols_all].values.ravel()) - {user_name, ''}
    
    # 이 부분에서 내가 근무하지 않는 시간대라는 조건이 제거되었습니다.
    for colleague_name in sorted(list(all_colleagues_set)):
        compatible_colleague_names_them.append(colleague_name)
    
    with cols_them_to_my[0]:
        if not compatible_colleague_names_them:
            st.warning("교환 가능한 상대방이 없습니다.")
            index_to_use_them = None
        else:
            index_to_use_them = None

        selected_colleague_name_them = st.selectbox(
            "상대방 선택",
            compatible_colleague_names_them,
            index=index_to_use_them,
            placeholder="상대방을 선택하세요",
            key="them_to_my_colleague_select"
        )
    
    with cols_them_to_my[1]:
        colleague_assignment_options_them = {}
        selected_assignment_str_them = None
        
        if selected_colleague_name_them:
            colleague_assignments = get_person_room_assignments(df_room, selected_colleague_name_them)
            
            if not colleague_assignments:
                st.warning(f"'{selected_colleague_name_them}'님의 방배정이 없습니다.")
            else:
                colleague_assignment_options_them = {a['display_str']: a for a in colleague_assignments}
                
            selected_assignment_str_them = st.selectbox(
                f"'{selected_colleague_name_them}'의 방배정 선택",
                colleague_assignment_options_them.keys(),
                index=None,
                placeholder="상대방의 방배정을 선택하세요",
                key="them_to_my_assignment_select"
            )
        else:
            st.selectbox("상대방의 방배정 선택", [], placeholder="먼저 상대방을 선택하세요", key="them_to_my_assignment_select_disabled")

    with cols_them_to_my[2]:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        if st.button("➕ 요청 추가", key="add_them_to_my_request_button", use_container_width=True, disabled=not(selected_colleague_name_them and selected_assignment_str_them)):
            colleague_assignment_info = colleague_assignment_options_them[selected_assignment_str_them]
            
            new_request = {
                "RequestID": str(uuid.uuid4()),
                "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                "요청자": user_name,
                "요청자 사번": employee_id,
                "요청 근무일": "대체 근무",
                "요청자 방배정": "대체 근무",
                "상대방": colleague_assignment_info['person_name'],
                "상대방 방배정": colleague_assignment_info['column_name'],
            }
            with st.spinner("요청을 기록하는 중입니다..."):
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
        '<th style="font-weight: bold; color: #2E86C1; width: 30%; padding-bottom: 8px; font-size: 1.0em;">나의 방배정</th>'
        '<th style="font-weight: bold; color: #28B463; width: 30%; padding-bottom: 8px; font-size: 1.0em;">교환 방배정</th>'
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