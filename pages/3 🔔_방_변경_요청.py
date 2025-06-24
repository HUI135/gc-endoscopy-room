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

st.set_page_config(page_title="마스터 수정", page_icon="🔔", layout="wide")

menu.menu()

# --- 상수 정의 ---
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0] # "2025"
REQUEST_SHEET_NAME = f"{MONTH_STR} 방 변경요청"

# --- 세션 상태 초기화 (이 페이지에서는 사용하지 않지만, 다른 페이지와의 호환성을 위해 유지) ---
if "change_requests" not in st.session_state:
    st.session_state.change_requests = []

# --- Google Sheets 클라이언트 초기화 ---
def get_gspread_client():
    """Google Sheets API 클라이언트를 생성하고 반환합니다."""
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"Google Sheets 인증 정보를 불러오는 데 실패했습니다: {e}")
        return None

# --- 데이터 로딩 함수 (방) ---
@st.cache_data(ttl=300)
def load_room_data(month_str):
    """지정된 월의 방 데이터를 Google Sheets에서 불러옵니다."""
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

# --- [신규] 방 변경 요청 관련 함수 (조회, 추가, 삭제) ---
@st.cache_data(ttl=30)
def get_my_room_requests(month_str, employee_id):
    """현재 사용자의 모든 방 교환 요청을 Google Sheet에서 불러옵니다."""
    if not employee_id: return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=9)
            headers = ['RequestID', 'Timestamp', 'RequesterName', 'RequesterID', 'DateHeader', 'MyRoom', 'TheirRoom', 'TheirName', 'Status']
            worksheet.append_row(headers)
            return []
        all_requests = worksheet.get_all_records()
        my_requests = [req for req in all_requests if str(req.get('RequesterID')) == str(employee_id)]
        return my_requests
    except Exception as e:
        st.error(f"요청 목록을 불러오는 중 오류 발생: {e}")
        return []

def add_room_request_to_sheet(request_data, month_str):
    """단일 방 교환 요청을 Google Sheet에 추가합니다."""
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=9)
            headers = ['RequestID', 'Timestamp', 'RequesterName', 'RequesterID', 'DateHeader', 'MyRoom', 'TheirRoom', 'TheirName', 'Status']
            worksheet.append_row(headers)
        
        row_to_add = [
            request_data['RequestID'], request_data['Timestamp'], request_data['RequesterName'],
            request_data['RequesterID'], request_data['DateHeader'], request_data['MyRoom'],
            request_data['TheirRoom'], request_data['TheirName'], 'Pending'
        ]
        worksheet.append_row(row_to_add)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"교환 요청 저장 실패: {e}")
        return False

def delete_room_request_from_sheet(request_id, month_str):
    """RequestID를 기반으로 특정 방 교환 요청을 Google Sheet에서 삭제합니다."""
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
        st.error("삭제할 요청을 찾을 수 없습니다. 이미 삭제되었을 수 있습니다.")
        return False
    except Exception as e:
        st.error(f"요청 삭제 중 오류 발생: {e}")
        return False

# --- 헬퍼 함수 ---
def get_person_room_assignments(df, person_name):
    assignments = []
    sorted_df = df.sort_values(by='날짜_dt').reset_index(drop=True)
    
    def sort_key(col_name):
        match = re.match(r"(\d{1,2}:\d{2})", str(col_name))
        if match:
            time_str = match.group(1)
            if ':' in time_str and len(time_str.split(':')[0]) == 1:
                time_str = f"0{time_str}"
            return datetime.strptime(time_str, "%H:%M").time()
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
                    assignments.append({
                        'date_obj': dt.date(), 'column_name': str(col),
                        'person_name': current_person, 'display_str': f"{date_str} - {col}"
                    })
    return assignments

def get_shift_period(column_name):
    am_pattern = re.compile(r"^(8:30|9:00|9:30|10:00|10:30|11:00|11:30)")
    if am_pattern.match(str(column_name)): return "오전"
    pm_pattern = re.compile(r"^(13:30|14:00|14:30|15:00|15:30|16:00|16:30|17:00)")
    if pm_pattern.match(str(column_name)): return "오후"
    return "기타"

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.rerun()

# --- 메인 로직 ---
def main():
    if not st.session_state.get("login_success"):
        st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
        return

    user_name = st.session_state.get("name", "")
    employee_id = st.session_state.get("employee_id", "")

    # HTML 코드를 한 줄로 만들어 공백/줄바꿈 문제를 원천 차단합니다.
    HTML_CARD_TEMPLATE = (
        '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
            '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
                '<thead><tr>'
                    '<th style="font-weight: bold; color: #FF4F0F; width: 40%; padding-bottom: 8px; font-size: 1.1em;">일자</th>'
                    '<th style="font-weight: bold; color: #2E86C1; width: 25%; padding-bottom: 8px; font-size: 1.1em;">나의 방배정</th>'
                    '<th style="font-weight: bold; color: #28B463; width: 35%; padding-bottom: 8px; font-size: 1.1em;">교환 방배정</th>'
                '</tr></thead>'
                '<tbody><tr>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{date_header}</td>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{my_room}</td>'
                    '<td style="font-size: 1.1em; padding-top: 5px;">{their_room} (<strong style="color:#1E8449;">{their_name}</strong> 님)</td>'
                '</tr></tbody>'
            '</table>'
            '<hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 8px 0 5px 0;">'
            '<div style="text-align: right; font-size: 0.85em; color: #757575;">요청 시간: {timestamp}</div>'
        '</div>'
    )

    st.header(f"📅 {user_name} 님의 {MONTH_STR} 방 변경 요청", divider='rainbow')

    df_room = load_room_data(MONTH_STR)
    if df_room.empty:
        st.warning("방 데이터를 불러올 수 없거나 데이터가 비어있습니다.")
        return

    st.dataframe(df_room.drop(columns=['날짜_dt'], errors='ignore'))
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
                my_shift_date = my_shift['date_obj']
                my_shift_period = get_shift_period(my_shift['column_name'])
                all_assignments = get_person_room_assignments(df_room, "")
                for a in all_assignments:
                    if a['date_obj'] == my_shift_date and get_shift_period(a['column_name']) == my_shift_period:
                        if a['person_name'] and a['person_name'] != user_name:
                            compatible_assignments.append(a)
            colleague_options = {i: f"{p['person_name']} - {p['column_name']}" for i, p in enumerate(compatible_assignments)}
            selected_colleague_idx = st.selectbox("**교환할 인원**", colleague_options.keys(), format_func=lambda i: colleague_options[i], index=None, placeholder="교환할 인원을 선택하세요")

        with cols[2]:
            # --- [수정] 버튼을 아래로 내리기 위한 투명한 공간 추가 ---
            # selectbox의 라벨과 비슷한 높이의 공간을 만들어 버튼의 수직 위치를 맞춥니다.
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            
            if st.button("➕ 요청 추가", use_container_width=True, disabled=(not my_selected_shift_str or selected_colleague_idx is None)):
                my_shift_info = assignment_options[my_selected_shift_str]
                selected_colleague_info = compatible_assignments[selected_colleague_idx]
                
                # 요청 데이터를 생성
                date_header_str = my_shift_info['display_str'].split('-')[0].strip() + " " + get_shift_period(my_shift_info['column_name'])
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "Timestamp": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "RequesterName": user_name, "RequesterID": employee_id,
                    "DateHeader": date_header_str,
                    "MyRoom": my_shift_info['column_name'],
                    "TheirRoom": selected_colleague_info['column_name'],
                    "TheirName": selected_colleague_info['person_name'],
                }
                with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                    success = add_room_request_to_sheet(new_request, MONTH_STR)
                    if success:
                        st.success("교환 요청이 성공적으로 기록되었습니다.")
                        time.sleep(1)
                        st.rerun()

# ---------------------------------------------------------------------------------

    # --- 나의 방 교환 요청 목록 표시 ---
    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 방 변경 요청 목록")
    my_requests = get_my_room_requests(MONTH_STR, employee_id)

    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        for req in my_requests:
            req_id = req['RequestID']
            col1, col2 = st.columns([4, 1])
            with col1:
                card_html = HTML_CARD_TEMPLATE.format(
                    date_header=req['DateHeader'],
                    my_room=req['MyRoom'],
                    their_room=req['TheirRoom'],
                    their_name=req['TheirName'],
                    timestamp=req['Timestamp']
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                if st.button("🗑️ 삭제", key=f"del_{req_id}", use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        delete_success = delete_room_request_from_sheet(req_id, MONTH_STR)
                        if delete_success:
                            st.success(f"요청이 삭제되었습니다.")
                            time.sleep(1)
                            st.rerun()

if __name__ == "__main__":
    main()