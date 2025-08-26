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
today = date.today()
month_str = today.strftime("%Y년 %-m월")
YEAR_STR = month_str.split('년')[0]
REQUEST_SHEET_NAME = f"{month_str} 방배정 변경요청"

# --- 함수 정의 ---
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 인증 정보를 불러오는 데 실패했습니다: {str(e)}")
        st.stop()

@st.cache_data(ttl=300)
def load_room_data(month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            st.info(f"{month_str} 방배정이 아직 완료되지 않았습니다.")
            return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 방배정")
        records = worksheet.get_all_records()
        if not records:
            st.info(f"{month_str} 방배정이 아직 완료되지 않았습니다.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
        if '날짜' not in df.columns:
            st.info(f"{month_str} 방배정이 아직 완료되지 않았습니다.")
            return pd.DataFrame()
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(YEAR_STR + '년 ' + df['날짜'].astype(str), format='%Y년 %m월 %d일', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        return df
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (방배정 데이터 로드): {str(e)}")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.info(f"{month_str} 방배정이 아직 완료되지 않았습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.info(f"{month_str} 방배정이 아직 완료되지 않았습니다.")
        st.error(f"방배정 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

@st.cache_data(ttl=300)
def load_special_schedules(month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            st.info(f"{month_str} 토요/휴일 일자가 아직 설정되지 않았습니다.")
            return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 토요/휴일 일자")
        records = worksheet.get_all_records()
        if not records:
            st.info(f"{month_str} 토요/휴일 일자가 아직 설정되지 않았습니다.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
        if '날짜' not in df.columns or '근무 인원' not in df.columns:
            st.info(f"{month_str} 토요/휴일 일자가 아직 설정되지 않았습니다.")
            return pd.DataFrame()
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(df['날짜'], format='%Y-%m-%d', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.info(f"{month_str} 토요/휴일 일자가 아직 설정되지 않았습니다.")
        return pd.DataFrame()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (토요/휴일 데이터 로드): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.info(f"{month_str} 토요/휴일 일자가 아직 설정되지 않았습니다.")
        st.error(f"토요/휴일 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

def get_my_room_requests(month_str, employee_id):
    if not employee_id:
        return []
    try:
        gc = get_gspread_client()
        if not gc:
            return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return []
        all_requests = worksheet.get_all_records()
        my_requests = [req for req in all_requests if str(req.get('요청자 사번')) == str(employee_id)]
        return my_requests
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 목록 로드): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"요청 목록을 불러오는 중 오류 발생: {str(e)}")
        st.stop()

def add_room_request_to_sheet(request_data, month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '변경 요청', '변경 요청한 방배정']
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
            current_headers = worksheet.row_values(1)
            if not current_headers or current_headers != headers:
                try:
                    worksheet.update('A1:F1', [headers])
                    st.info(f"'{REQUEST_SHEET_NAME}' 시트의 헤더를 올바른 형식으로 업데이트했습니다.")
                except gspread.exceptions.APIError as e:
                    st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                    st.error(f"Google Sheets API 오류 (헤더 업데이트): {str(e)}")
                    st.stop()
        except gspread.exceptions.WorksheetNotFound:
            try:
                worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=len(headers))
                worksheet.append_row(headers)
                st.info(f"'{REQUEST_SHEET_NAME}' 시트를 새로 생성하고 헤더를 추가했습니다.")
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (시트 생성): {str(e)}")
                st.stop()
        row_to_add = [
            request_data.get('RequestID'),
            request_data.get('요청일시'),
            request_data.get('요청자'),
            request_data.get('요청자 사번'),
            request_data.get('변경 요청'),
            request_data.get('변경 요청한 방배정')
        ]
        try:
            worksheet.append_row(row_to_add)
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
            st.stop()
        st.cache_data.clear()
        return True
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"교환 요청 저장 실패: {str(e)}")
        st.stop()

def delete_room_request_from_sheet(request_id, month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        cell = worksheet.find(request_id)
        if cell:
            try:
                worksheet.delete_rows(cell.row)
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
                st.stop()
            st.cache_data.clear()
            return True
        st.error("삭제할 요청을 찾을 수 없습니다.")
        return False
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"요청 삭제 중 오류 발생: {str(e)}")
        st.stop()

def get_person_room_assignments(df, person_name="", special_schedules_df=None):
    assignments = []
    # 일반 스케줄 처리
    if not df.empty:
        sorted_df = df.sort_values(by='날짜_dt').reset_index(drop=True)
        def sort_key(col_name):
            match = re.search(r"(\d{1,2}:\d{2})", str(col_name))
            if match:
                time_str = match.group(1)
                return datetime.strptime(time_str.zfill(5), "%H:%M").time()
            if '당직' in str(col_name) or '온콜' in str(col_name):
                return datetime.strptime("23:59", "%H:%M").time()
            return datetime.max.time()
        time_cols = sorted([col for col in df.columns if re.search(r"(\d{1,2}:\d{2})", str(col)) or '당직' in str(col) or '온콜' in str(col)], key=sort_key)
        for _, row in sorted_df.iterrows():
            dt = row['날짜_dt']
            display_date_str = dt.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[dt.weekday()]})"
            sheet_date_str = dt.strftime("%Y-%m-%d")
            for col in time_cols:
                current_person = row.get(col)
                if (not person_name and current_person) or (person_name and current_person == person_name):
                    assignments.append({
                        'date_obj': dt.date(),
                        'column_name': str(col),
                        'person_name': current_person,
                        'display_str': f"{display_date_str} - {col}",
                        'sheet_str': f"{sheet_date_str} ({col})"
                    })

    # 토요/휴일 스케줄 처리
    if special_schedules_df is not None and not special_schedules_df.empty:
        for _, row in special_schedules_df.iterrows():
            dt = row['날짜_dt']
            display_date_str = dt.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[dt.weekday()]})"
            sheet_date_str = dt.strftime("%Y-%m-%d")
            workers = row['근무 인원'].split(', ') if row['근무 인원'] else []
            cleaned_workers = [re.sub(r'\[\d+\]', '', worker).strip() for worker in workers]
            if not person_name or person_name in cleaned_workers:
                regular_row = df[df['날짜_dt'].dt.date == dt.date()]
                time_slots = ['당직', '8:15', '8:30', '9:00', '9:30']
                if not regular_row.empty:
                    regular_row_dict = regular_row.iloc[0].to_dict()
                    current_time_idx = 0
                    for col in regular_row_dict:
                        if col in ['날짜', '요일', '날짜_dt']:
                            continue
                        if regular_row_dict[col] == '':
                            if current_time_idx < len(time_slots) - 1:
                                current_time_idx += 1
                            continue
                        match = re.search(r'\[(\d+)\]', str(regular_row_dict[col]))
                        if match:
                            room_number = match.group(1)
                            worker_name = re.sub(r'\[\d+\]', '', str(regular_row_dict[col])).strip()
                            if (not person_name and worker_name) or (person_name and worker_name == person_name):
                                time_slot = time_slots[current_time_idx]
                                display_str = f"{display_date_str} - {time_slot}({room_number})" if time_slot != '당직' else f"{display_date_str} - 당직"
                                sheet_str = f"{sheet_date_str} ({time_slot}({room_number}))" if time_slot != '당직' else f"{sheet_date_str} (당직)"
                                assignments.append({
                                    'date_obj': dt.date(),
                                    'column_name': f"{time_slot}({room_number})" if time_slot != '당직' else '당직',
                                    'person_name': worker_name,
                                    'display_str': display_str,
                                    'sheet_str': sheet_str
                                })
                else:
                    # df_room에 해당 날짜 데이터가 없어도 df_special의 근무 인원을 기반으로 배정 생성
                    for worker in cleaned_workers:
                        if (not person_name and worker) or (person_name and worker == person_name):
                            # 기본적으로 9:00 시간대와 가상의 방 번호(예: 0)를 사용
                            time_slot = '9:00'
                            room_number = '0'
                            display_str = f"{display_date_str} - {time_slot}({room_number})"
                            sheet_str = f"{sheet_date_str} ({time_slot}({room_number}))"
                            assignments.append({
                                'date_obj': dt.date(),
                                'column_name': f"{time_slot}({room_number})",
                                'person_name': worker,
                                'display_str': display_str,
                                'sheet_str': sheet_str
                            })

    return sorted(assignments, key=lambda x: (x['date_obj'], x['column_name']))
    
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

def is_person_assigned_at_time(df, person_name, date_obj, column_name, special_schedules_df=None):
    # 일반 스케줄 확인
    row_data = df[df['날짜_dt'].dt.date == date_obj]
    if not row_data.empty:
        row_dict = row_data.iloc[0].to_dict()
        for col, assigned_person in row_dict.items():
            if col in ['날짜', '요일', '날짜_dt']:
                continue
            if assigned_person == person_name:
                return True
    
    # 토요/휴일 스케줄 확인
    if special_schedules_df is not None and not special_schedules_df.empty:
        special_row = special_schedules_df[special_schedules_df['날짜_dt'].dt.date == date_obj]
        if not special_row.empty:
            workers = special_row.iloc[0]['근무 인원'].split(', ') if special_row.iloc[0]['근무 인원'] else []
            cleaned_workers = [re.sub(r'\[\d+\]', '', worker).strip() for worker in workers]
            if person_name in cleaned_workers:
                return True
    
    return False

# --- 메인 로직 ---
try:
    user_name = st.session_state.get("name", "")
    employee_id = st.session_state.get("employee_id", "")
    if not user_name or not employee_id:
        st.error("⚠️ 사용자 정보가 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
        st.stop()
except NameError as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()

st.header(f"📅 {user_name} 님의 {month_str} 방배정 변경 요청", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()
            st.rerun()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()

df_room = load_room_data(month_str)
df_special = load_special_schedules(month_str)

if df_room.empty:
    st.stop()
else:
    st.dataframe(df_room.drop(columns=['날짜_dt'], errors='ignore'), use_container_width=True, hide_index=True)
    st.divider()

    st.subheader("✨ 방 변경 요청하기")
    with st.expander("🔑 사용설명서"):
        st.markdown("""
        **🟢 나의 방배정을 상대방과 바꾸기**

        : 내가 맡은 방배정을 다른 사람에게 넘겨줄 때 사용합니다.
        - **[변경을 원하는 나의 방배정 선택]**: 내가 바꾸고 싶은 방배정을 선택하세요.
        - **[교환할 상대방 선택]**: 당월의 모든 근무자가 목록에 나타납니다.
        _※ 주의: 내가 선택한 방 배정의 날짜와 시간대에 이미 상대방이 근무한다면, 근무가 중복될 수 있습니다.
        상대방의 방배정도 함께 변경해야 합니다._

        **🔵 상대방의 방배정을 나와 바꾸기**

        : 내가 다른 사람의 방배정을 대신 맡아줄 때 사용합니다.
        - **[상대방 선택]**: 상대방을 선택하세요.
        - **[상대방의 근무 선택]**: 선택한 상대방의 방배정을 나로 대체합니다.
        """)

    # --- 나의 방배정을 상대방과 바꾸기 ---
    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🟢 나의 방배정을 상대방과 바꾸기</h6>", unsafe_allow_html=True)
    
    user_assignments_my = get_person_room_assignments(df_room, user_name, df_special)
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
            is_my_assignment_selected = my_selected_assignment_str_my is not None
            
            if st.session_state.get('user_data', None):
                all_employee_names = set(st.session_state.get('user_data', {}).keys())
            else:
                time_cols_all = [col for col in df_room.columns if re.search(r"(\d{1,2}:\d{2})", str(col)) or '당직' in str(col) or '온콜' in str(col)]
                all_employee_names = set()
                for col in time_cols_all:
                    for value in df_room[col].values:
                        if value:
                            # 이름에서 [방번호] 제거
                            cleaned_value = re.sub(r'\[\d+\]', '', str(value)).strip()
                            all_employee_names.add(cleaned_value)
            
            compatible_colleague_names = sorted(list(all_employee_names - {user_name}))
            
            selected_colleague_name = st.selectbox(
                "교환할 상대방 선택",
                options=compatible_colleague_names,
                index=None,
                placeholder="먼저 나의 방배정을 선택하세요" if not is_my_assignment_selected else "상대방을 선택하세요",
                disabled=not is_my_assignment_selected,
                key="my_to_them_colleague_select"
            )
        
        request_disabled_my = True
        
        if my_selected_assignment_str_my and selected_colleague_name:
            my_selected_info = assignment_options_my[my_selected_assignment_str_my]
            
            is_colleague_occupied = is_person_assigned_at_time(df_room, selected_colleague_name, my_selected_info['date_obj'], my_selected_info['column_name'], df_special)
            
            if is_colleague_occupied:
                st.warning(f"⚠️ **{selected_colleague_name}**님이 **{my_selected_info['display_str'].split('-')[0].strip()}** ({get_shift_period(my_selected_info['column_name'])})에 이미 근무가 있습니다. 중복 배치가 되지 않도록 **{selected_colleague_name}** 님의 방배정도 변경해 주십시오.")
            
            request_disabled_my = False

        with cols_my_to_them[2]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            if st.button("➕ 요청 추가", key="add_my_to_them_request_button", use_container_width=True, disabled=request_disabled_my):
                my_assignment_info = assignment_options_my[my_selected_assignment_str_my]
                
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "변경 요청": f"{user_name} ➡️ {selected_colleague_name}",
                    "변경 요청한 방배정": my_assignment_info['sheet_str'],
                }
                with st.spinner("요청을 기록하는 중입니다..."):
                    if add_room_request_to_sheet(new_request, month_str):
                        st.success("교환 요청이 성공적으로 기록되었습니다.")
                        time.sleep(1.5)
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
        all_colleagues_set = set()
        for col in time_cols_all:
            for value in df_room[col].values:
                if value:
                    cleaned_value = re.sub(r'\[\d+\]', '', str(value)).strip()
                    all_colleagues_set.add(cleaned_value)
        if not df_special.empty:
            for workers in df_special['근무 인원']:
                if workers:
                    cleaned_workers = [re.sub(r'\[\d+\]', '', worker).strip() for worker in workers.split(', ')]
                    all_colleagues_set.update(cleaned_workers)

    for colleague_name in sorted(list(all_colleagues_set)):
        compatible_colleague_names_them.append(colleague_name)

    with cols_them_to_my[0]:
        selected_colleague_name_them = st.selectbox(
            "상대방 선택",
            compatible_colleague_names_them,
            index=None,
            placeholder="상대방을 선택하세요",
            key="them_to_my_colleague_select"
        )

    with cols_them_to_my[1]:
        colleague_assignment_options_them = {}
        selected_assignment_str_them = None
        is_them_assignment_selected = selected_colleague_name_them is not None

        if selected_colleague_name_them:
            colleague_assignments = get_person_room_assignments(df_room, selected_colleague_name_them, df_special)

            user_occupied_slots = {(s['date_obj'], s['column_name']) for s in get_person_room_assignments(df_room, user_name, df_special)}
            compatible_assignments = [
                s for s in colleague_assignments if (s['date_obj'], s['column_name']) not in user_occupied_slots
            ]

            if not compatible_assignments:
                st.warning(f"'{selected_colleague_name_them}'님의 근무 중 교환 가능한 날짜/시간대가 없습니다.")
                st.selectbox(
                    f"'{selected_colleague_name_them}'의 방배정 선택",
                    [],
                    disabled=True,
                    placeholder="교환 가능한 근무 없음",
                    key="them_to_my_assignment_select_no_option"
                )
            else:
                colleague_assignment_options_them = {a['display_str']: a for a in compatible_assignments}
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
                "변경 요청": f"{colleague_assignment_info['person_name']} ➡️ {user_name}",
                "변경 요청한 방배정": colleague_assignment_info['sheet_str'],
            }
            with st.spinner("요청을 기록하는 중입니다..."):
                if add_room_request_to_sheet(new_request, month_str):
                    st.success("요청이 성공적으로 기록되었습니다.")
                    time.sleep(1.5)
                    st.rerun()

    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 방배정 변경 요청 목록")
    my_requests = get_my_room_requests(month_str, employee_id)

    HTML_CARD_TEMPLATE = (
        '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; background-color: #fcfcfc; margin-bottom: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
        '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
        '<thead><tr>'
        '<th style="font-weight: bold; color: #555; width: 40%; padding-bottom: 5px; font-size: 0.9em;">변경 요청</th>'
        '<th style="font-weight: bold; color: #2E86C1; width: 60%; padding-bottom: 5px; font-size: 0.9em;">변경 요청한 방배정</th>'
        '</tr></thead>'
        '<tbody><tr>'
        '<td style="font-size: 1.0em; color: #555; padding-top: 3px;">{request_type}</td>'
        '<td style="font-size: 1.0em; color: #555; padding-top: 3px;">{assignment_detail_display}</td>'
        '</tr></tbody>'
        '</table>'
        '<hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 8px 0 5px 0;">'
        '<div style="text-align: right; font-size: 0.75em; color: #757575;">요청 시간: {timestamp}</div>'
        '</div>'
    )

    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        for req in my_requests:
            col1, col2 = st.columns([5, 1])
            with col1:
                assignment_detail = req.get('변경 요청한 방배정', '')
                if re.match(r'\d{4}-\d{2}-\d{2} \(.+\)', assignment_detail):
                    date_part, time_part = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', assignment_detail).groups()
                    dt = datetime.strptime(date_part, '%Y-%m-%d')
                    display_date_str = dt.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[dt.weekday()]})"
                    assignment_detail_display = f"{display_date_str} - {time_part}"
                else:
                    assignment_detail_display = assignment_detail
                card_html = HTML_CARD_TEMPLATE.format(
                    request_type=req.get('변경 요청', ''),
                    assignment_detail_display=assignment_detail_display,
                    timestamp=req.get('요청일시', '')
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                st.markdown("<div style='height: 35px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️ 삭제", key=req.get('RequestID', str(uuid.uuid4())), use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        if delete_room_request_from_sheet(req.get('RequestID'), month_str):
                            st.success("요청이 성공적으로 삭제되었습니다.")
                            time.sleep(1.5)
                            st.rerun()