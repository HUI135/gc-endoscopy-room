import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import uuid
import re
from zoneinfo import ZoneInfo
import menu
import os

# --- 페이지 설정 및 메뉴 호출 ---
st.set_page_config(page_title="스케줄 변경 요청", page_icon="🔍", layout="wide")
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
from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.now(kst)
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
month_str = next_month_date.strftime("%Y년 %-m월")
YEAR_STR = month_str.split('년')[0]
AM_COLS = [str(i) for i in range(1, 13)] + ['온콜']
PM_COLS = [f'오후{i}' for i in range(1, 6)]
REQUEST_SHEET_NAME = f"{month_str} 스케줄 변경요청"

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

@st.cache_data(ttl=300, show_spinner=False)
def load_schedule_data(month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
            return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 스케줄")
        records = worksheet.get_all_records()
        if not records:
            st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
            return pd.DataFrame()
        df = pd.DataFrame(records)
        if '오전당직(온콜)' in df.columns:
            df.rename(columns={'오전당직(온콜)': '온콜'}, inplace=True)
        if '날짜' not in df.columns:
            st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
            return pd.DataFrame()
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(YEAR_STR + '년 ' + df['날짜'].astype(str), format='%Y년 %m월 %d일', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        return df
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (스케줄 데이터 로드): {str(e)}")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
        st.error(f"스케줄 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

@st.cache_data(ttl=30, show_spinner=False)
def get_my_requests(month_str, employee_id):
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

def add_request_to_sheet(request_data, month_str):
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '변경 요청', '변경 요청한 스케줄']
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
            current_headers = worksheet.row_values(1)
            if not current_headers or current_headers != headers:
                try:
                    worksheet.update('A1:F1', [headers])
                    # st.info(f"'{REQUEST_SHEET_NAME}' 시트의 헤더를 올바른 형식으로 업데이트했습니다.")
                except gspread.exceptions.APIError as e:
                    st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                    st.error(f"Google Sheets API 오류 (헤더 업데이트): {str(e)}")
                    st.stop()
        except gspread.exceptions.WorksheetNotFound:
            try:
                worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=len(headers))
                worksheet.append_row(headers)
                # st.info(f"'{REQUEST_SHEET_NAME}' 시트를 새로 생성하고 헤더를 추가했습니다.")
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
            request_data.get('변경 요청한 스케줄')
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

def delete_request_from_sheet(request_id, month_str):
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

def get_shift_type(col_name):
    if col_name in AM_COLS:
        return "오전"
    elif col_name in PM_COLS:
        return "오후"
    return "기타"

def get_person_shifts(df, person_name):
    shifts = []
    am_cols_in_df = [col for col in AM_COLS if col in df.columns]
    pm_cols_in_df = [col for col in PM_COLS if col in df.columns]
    for _, row in df.iterrows():
        dt = row['날짜_dt']
        date_str = dt.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        for col in am_cols_in_df:
            if row[col] == person_name:
                shifts.append({'date_obj': dt.date(), 'shift_type': '오전', 'col_name': col, 'display_str': f"{date_str} - 오전", 'person_name': person_name})
        for col in pm_cols_in_df:
            if row[col] == person_name:
                shifts.append({'date_obj': dt.date(), 'shift_type': '오후', 'col_name': col, 'display_str': f"{date_str} - 오후", 'person_name': person_name})
    return shifts

def get_all_employee_names(df):
    all_cols = [col for col in df.columns if col in AM_COLS + PM_COLS]
    return set(df[all_cols].values.ravel()) - {''}

def is_person_assigned_at_time(df, person_name, date_obj, shift_type):
    row_data = df[df['날짜_dt'].dt.date == date_obj]
    if row_data.empty:
        return False
    row_dict = row_data.iloc[0].to_dict()
    if shift_type == "오전":
        check_cols = [col for col in AM_COLS if col in row_dict]
    elif shift_type == "오후":
        check_cols = [col for col in PM_COLS if col in row_dict]
    else:
        return False
    for col in check_cols:
        if row_dict.get(col) == person_name:
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

st.header(f"📅 {user_name} 님의 {month_str} 스케줄 변경 요청", divider='rainbow')

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

df_schedule = load_schedule_data(month_str)

if df_schedule.empty:
    st.stop()
else:
    st.dataframe(df_schedule.drop(columns=['날짜_dt'], errors='ignore'), use_container_width=True, hide_index=True)
    st.divider()

    st.subheader("✨ 스케줄 변경 요청하기")
    with st.expander("🔑 사용설명서"):
        st.markdown("""
        **🟢 나의 스케줄을 상대방과 바꾸기**

        : 내가 맡은 근무를 다른 사람에게 넘겨줄 때 사용합니다.
        - **[변경을 원하는 나의 스케줄 선택]**: 내가 바꾸고 싶은 근무를 선택하세요.
        - **[교환할 상대방 선택]**: 그 날짜와 시간대에 **근무가 비어있는 사람**만 목록에 나타납니다.

        **🔵 상대방의 스케줄을 나와 바꾸기**

        : 내가 다른 사람의 근무를 대신 맡아줄 때 사용합니다.
        - **[상대방 선택]**: 상대방을 선택하세요.
        - **[상대방의 근무 선택]**: 선택한 상대방의 근무 중에서 **내가 이미 근무하고 있지 않은 날짜와 시간대**만 목록에 나타납니다.
        """)

    # (기존 "🟢 나의 스케줄~" 섹션 전체를 아래 코드로 교체)
    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🟢 나의 스케줄을 상대방과 바꾸기</h6>", unsafe_allow_html=True)
    user_shifts = get_person_shifts(df_schedule, user_name)

    if not user_shifts:
        st.warning(f"'{user_name}'님의 배정된 스케줄이 없습니다.")
    else:
        # 1. 날짜 선택 UI
        cols_my_to_them = st.columns([2, 2, 2, 1])
        
        # 사용자의 근무 날짜 목록 (중복 제거)
        user_shift_dates = sorted(list(set((s['date_obj'], s['display_str'].split(' - ')[0]) for s in user_shifts)), key=lambda x: x[0])
        user_date_options = {display: date_obj for date_obj, display in user_shift_dates}

        with cols_my_to_them[0]:
            my_selected_date_str = st.selectbox(
                "나의 근무일 선택",
                user_date_options.keys(),
                index=None,
                placeholder="날짜를 선택하세요",
                key="my_to_them_my_date_select"
            )

        # 2. 시간대 선택 UI
        with cols_my_to_them[1]:
            my_selected_shift_type = None
            if my_selected_date_str:
                my_selected_date_obj = user_date_options[my_selected_date_str]
                # 선택된 날짜에 가능한 시간대 (오전/오후) 목록 생성
                available_shifts_for_date = [s['shift_type'] for s in user_shifts if s['date_obj'] == my_selected_date_obj]
                
                my_selected_shift_type = st.selectbox(
                    "시간대 선택",
                    options=available_shifts_for_date,
                    index=None,
                    placeholder="시간대",
                    key="my_to_them_my_shift_select"
                )
            else:
                st.selectbox("시간대 선택", [], disabled=True, placeholder="시간대", key="my_to_them_my_shift_select_disabled")

        # 3. 교환 상대방 선택 UI
        with cols_my_to_them[2]:
            colleagues = sorted(list(get_all_employee_names(df_schedule) - {user_name}))
            compatible_colleagues = []
            selectbox_placeholder = "날짜와 시간대를 선택하세요"
            is_disabled = True
            
            if my_selected_date_str and my_selected_shift_type:
                is_disabled = False
                my_date = user_date_options[my_selected_date_str]
                
                compatible_colleagues = [
                    c for c in colleagues if not is_person_assigned_at_time(df_schedule, c, my_date, my_selected_shift_type)
                ]
                
                if not compatible_colleagues:
                    selectbox_placeholder = "교환 가능한 동료 없음"
                    is_disabled = True
            
            selected_colleague_name = st.selectbox(
                "교환할 상대방 선택",
                options=compatible_colleagues,
                index=None,
                placeholder=selectbox_placeholder,
                disabled=is_disabled,
                key="my_to_them_colleague_select"
            )

        # 4. 요청 추가 버튼
        with cols_my_to_them[3]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            request_disabled_my = not my_selected_date_str or not my_selected_shift_type or not selected_colleague_name
            if st.button("➕ 요청 추가", key="add_my_to_them_request_button", use_container_width=True, disabled=request_disabled_my):
                # 선택된 정보로 최종 요청 데이터 생성
                my_date = user_date_options[my_selected_date_str]
                
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "변경 요청": f"{user_name} ➡️ {selected_colleague_name}",
                    "변경 요청한 스케줄": f"{my_date.strftime('%Y-%m-%d')} ({my_selected_shift_type})",
                }
                with st.spinner("요청을 기록하는 중입니다..."):
                    if add_request_to_sheet(new_request, month_str):
                        st.success("요청이 성공적으로 기록되었습니다.")
                        time.sleep(1.5)
                        st.rerun()

        # --- 상대방의 스케줄을 나와 바꾸기 ---
        st.write(" ")

    st.markdown("<h6 style='font-weight:bold;'>🔵 상대방의 스케줄을 나와 바꾸기</h6>", unsafe_allow_html=True)
    cols_them_to_my = st.columns([2, 2, 2, 1])

    # 1. 상대방 선택 UI
    with cols_them_to_my[0]:
        colleagues = sorted(list(get_all_employee_names(df_schedule) - {user_name}))
        selected_colleague_name_them = st.selectbox(
            "상대방 선택",
            colleagues,
            index=None,
            placeholder="상대방을 선택하세요",
            key="them_to_my_colleague_select"
        )
        
    # 2. 상대방 근무일 선택 UI
    with cols_them_to_my[1]:
        colleague_date_options = {}
        if selected_colleague_name_them:
            colleague_shifts = get_person_shifts(df_schedule, selected_colleague_name_them)
            user_occupied_slots = {(s['date_obj'], s['shift_type']) for s in user_shifts}
            
            # 내가 근무가 비어있는, 교환 가능한 상대방의 근무만 필터링
            compatible_shifts = [s for s in colleague_shifts if (s['date_obj'], s['shift_type']) not in user_occupied_slots]
            
            if compatible_shifts:
                # 교환 가능한 근무 날짜 목록 (중복 제거)
                colleague_shift_dates = sorted(list(set((s['date_obj'], s['display_str'].split(' - ')[0]) for s in compatible_shifts)), key=lambda x: x[0])
                colleague_date_options = {display: {'date_obj': date_obj, 'shifts': compatible_shifts} for date_obj, display in colleague_shift_dates}
                
                selected_colleague_date_str = st.selectbox(
                    f"'{selected_colleague_name_them}'의 근무일 선택",
                    colleague_date_options.keys(),
                    index=None,
                    placeholder="날짜를 선택하세요",
                    key="them_to_my_date_select"
                )
            else:
                st.selectbox(f"'{selected_colleague_name_them}'의 근무일 선택", [], disabled=True, placeholder="교환 가능한 날짜 없음", key="them_to_my_date_select_disabled")
                selected_colleague_date_str = None
        else:
            st.selectbox("상대방의 근무일 선택", [], disabled=True, placeholder="먼저 상대방을 선택하세요", key="them_to_my_date_select_disabled")
            selected_colleague_date_str = None

    # 3. 시간대 선택 UI
    with cols_them_to_my[2]:
        selected_colleague_shift_type = None
        if selected_colleague_date_str:
            selected_date_info = colleague_date_options[selected_colleague_date_str]
            selected_date_obj = selected_date_info['date_obj']
            # 선택된 날짜에 교환 가능한 시간대 목록
            available_shifts_for_date = [s['shift_type'] for s in selected_date_info['shifts'] if s['date_obj'] == selected_date_obj]
            
            selected_colleague_shift_type = st.selectbox(
                "시간대 선택",
                options=available_shifts_for_date,
                index=None,
                placeholder="시간대",
                key="them_to_my_shift_select"
            )
        else:
            st.selectbox("시간대 선택", [], disabled=True, placeholder="시간대", key="them_to_my_shift_select_disabled")

    # 4. 요청 추가 버튼
    with cols_them_to_my[3]:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        request_disabled_them = not selected_colleague_name_them or not selected_colleague_date_str or not selected_colleague_shift_type
        if st.button("➕ 요청 추가", key="add_them_to_my_request_button", use_container_width=True, disabled=request_disabled_them):
            # 최종 요청 데이터 생성
            colleague_date_obj = colleague_date_options[selected_colleague_date_str]['date_obj']
            
            new_request = {
                "RequestID": str(uuid.uuid4()),
                "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                "요청자": user_name,
                "요청자 사번": employee_id,
                "변경 요청": f"{selected_colleague_name_them} ➡️ {user_name}",
                "변경 요청한 스케줄": f"{colleague_date_obj.strftime('%Y-%m-%d')} ({selected_colleague_shift_type})",
            }
            with st.spinner("요청을 기록하는 중입니다..."):
                if add_request_to_sheet(new_request, month_str):
                    st.success("요청이 성공적으로 기록되었습니다.")
                    time.sleep(1.5)
                    st.rerun()

    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 스케줄 변경 요청 목록")

    def format_schedule_for_display(schedule_str):
        """Google Sheets에 저장된 'YYYY-MM-DD (오전)' 형식을 'M월 D일 (요일) - 오전'으로 변환"""
        match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', schedule_str)
        if match:
            date_part, shift_part = match.groups()
            try:
                dt_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
                weekday_str = ['월', '화', '수', '목', '금', '토', '일'][dt_obj.weekday()]
                return f"{dt_obj.month}월 {dt_obj.day}일 ({weekday_str}) - {shift_part}"
            except ValueError:
                return schedule_str
        return schedule_str

    my_requests = get_my_requests(month_str, employee_id)
    
    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        HTML_CARD_TEMPLATE = (
            '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; background-color: #fcfcfc; margin-bottom: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
            '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
            '<thead><tr>'
            '<th style="font-weight: bold; color: #555; width: 40%; padding-bottom: 5px; font-size: 0.9em;">변경 요청</th>'
            '<th style="font-weight: bold; color: #D9534F; width: 60%; padding-bottom: 5px; font-size: 0.9em;">변경 요청한 스케줄</th>'
            '</tr></thead>'
            '<tbody><tr>'
            '<td style="font-size: 1.0em; color: #555; padding-top: 3px;">{request_type}</td>'
            '<td style="font-size: 1.0em; color: #555; padding-top: 3px;">{assignment_detail}</td>'
            '</tr></tbody>'
            '</table>'
            '<hr style="border: none; border-top: 1px dotted #555; margin: 8px 0 5px 0;">'
            '<div style="text-align: right; font-size: 0.75em; color: #757575;">요청 시간: {timestamp}</div>'
            '</div>'
        )

        for req in my_requests:
            req_id = req.get('RequestID')
            col1, col2 = st.columns([5, 1])
            with col1:
                display_schedule = format_schedule_for_display(req.get('변경 요청한 스케줄', ''))
                card_html = HTML_CARD_TEMPLATE.format(
                    request_type=req.get('변경 요청', ''),
                    assignment_detail=display_schedule,
                    timestamp=req.get('요청일시', '')
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                st.markdown("<div style='height: 35px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️ 삭제", key=f"del_{req_id}", use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        if delete_request_from_sheet(req_id, month_str):
                            st.success("요청이 성공적으로 삭제되었습니다.")
                            time.sleep(1.5)  # 2초 대기
                            st.rerun()