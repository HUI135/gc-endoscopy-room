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
kst = ZoneInfo("Asia/Seoul")
now = datetime.now(kst)
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
month_str = next_month_date.strftime("%Y년 %-m월")
YEAR_STR = month_str.split('년')[0]
# '온콜'을 AM_COLS에서 분리하여 명확하게 관리
AM_COLS = [str(i) for i in range(1, 13)]
ONCALL_COL = '오전당직(온콜)'
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

        existing_requests = worksheet.get_all_records()
        new_request_signature = (request_data.get('변경 요청'), request_data.get('변경 요청한 스케줄'))
        for req in existing_requests:
            existing_signature = (req.get('변경 요청'), req.get('변경 요청한 스케줄'))
            if new_request_signature == existing_signature:
                return "DUPLICATE"
                            
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

# ✅ 수정된 get_person_shifts 함수
def get_person_shifts(df, person_name):
    # 중복을 방지하기 위해 세트(set)를 사용
    shifts_set = set()

    am_cols_in_df = [col for col in AM_COLS if col in df.columns]
    oncall_col_in_df = ONCALL_COL if ONCALL_COL in df.columns else None
    pm_cols_in_df = [col for col in PM_COLS if col in df.columns]

    for _, row in df.iterrows():
        dt = row['날짜_dt']
        date_str_display = dt.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[dt.weekday()]})"

        # 1. 온콜 근무 확인
        if oncall_col_in_df and row[oncall_col_in_df] == person_name:
            shift_type = '오전당직(온콜)'
            display_str = f"{date_str_display} - {shift_type}"
            shifts_set.add((dt.date(), shift_type, display_str, person_name))

        # 2. 일반 오전 근무 확인
        is_in_am = any(row[col] == person_name for col in am_cols_in_df)
        if is_in_am:
            shift_type = '오전'
            display_str = f"{date_str_display} - {shift_type}"
            shifts_set.add((dt.date(), shift_type, display_str, person_name))

        # 3. 오후 근무 확인
        is_in_pm = any(row[col] == person_name for col in pm_cols_in_df)
        if is_in_pm:
            shift_type = '오후'
            display_str = f"{date_str_display} - {shift_type}"
            shifts_set.add((dt.date(), shift_type, display_str, person_name))

    # 세트를 정렬된 딕셔너리 리스트로 변환하여 반환
    sorted_shifts = sorted(list(shifts_set), key=lambda x: (x[0], x[1]))
    return [
        {'date_obj': date_obj, 'shift_type': stype, 'display_str': dstr, 'person_name': pname}
        for date_obj, stype, dstr, pname in sorted_shifts
    ]

def get_all_employee_names(df):
    all_cols = [col for col in df.columns if col in AM_COLS + PM_COLS]
    return set(df[all_cols].values.ravel()) - {''}

# ✅ 수정된 is_person_assigned_at_time 함수
def is_person_assigned_at_time(df, person_name, date_obj, shift_type):
    row_data = df[df['날짜_dt'].dt.date == date_obj]
    if row_data.empty:
        return False
    row_dict = row_data.iloc[0].to_dict()

    check_cols = []
    if shift_type == "오전":
        # '온콜'을 제외한 오전 근무 열만 확인
        check_cols = [col for col in AM_COLS if col in row_dict]
    elif shift_type == "오전당직(온콜)":
        # '온콜' 열만 확인
        if ONCALL_COL in row_dict:
            check_cols = [ONCALL_COL]
    elif shift_type == "오후":
        check_cols = [col for col in PM_COLS if col in row_dict]
    else:
        return False

    # 해당 열들에 이름이 있는지 확인
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
        - **[교환할 상대방 선택]**:
            - 그 날짜와 시간대에 **근무가 비어있는 사람**만 목록에 나타납니다.
            - 오전당직(온콜)이 있는 날 '오전' 혹은 '오후'를 선택하면 **모든 근무자 명단**이 나타납니다:
                - 이후 교환 상대를 '그 날의 근무자'로 선택 시: 당직만 상대방으로 변경합니다.
                - 이후 교환 상대를 '그 날의 미근무자'로 선택 시: 나의 모든 근무(오전+오후+당직)를 상대방으로 변경합니다.

        **🔵 상대방의 스케줄을 나와 바꾸기**

        : 내가 다른 사람의 근무를 대신 맡을 때 사용합니다.
        - **[상대방 선택]**: 상대방을 선택하세요.
        - **[상대방의 근무 선택]**: 
            - 선택한 상대방의 근무 중에서 **내가 이미 근무하고 있지 않은 날짜와 시간대**만 목록에 나타납니다.
            - 상대방의 **'오전당직(온콜)'** 근무를 선택할 때, 나의 근무에 따라 결과가 달라집니다.
                - 그날 나의 근무가 있으면: 당직만 나로 변경합니다.
                - 그날 나의 근무가 없으면: 상대방의 모든 근무(오전+오후+당직)를 나로 변경합니다.
        """)

    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🟢 나의 스케줄을 상대방과 바꾸기</h6>", unsafe_allow_html=True)
    user_shifts = get_person_shifts(df_schedule, user_name)

    if not user_shifts:
        st.warning(f"'{user_name}'님의 배정된 스케줄이 없습니다.")
    else:
        cols_my_to_them = st.columns([2, 2, 2, 1])
        
        user_shift_dates = sorted(list(set(s['date_obj'] for s in user_shifts)))
        user_date_options = {d.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[d.weekday()]})": d for d in user_shift_dates}
        
        with cols_my_to_them[0]:
            my_selected_date_str = st.selectbox("나의 근무일 선택", user_date_options.keys(), index=None, placeholder="날짜를 선택하세요", key="my_date")

        with cols_my_to_them[1]:
            my_selected_shift_type = None
            if my_selected_date_str:
                my_selected_date_obj = user_date_options[my_selected_date_str]
                shifts_on_date = sorted(list({s['shift_type'] for s in user_shifts if s['date_obj'] == my_selected_date_obj} - {'오전당직(온콜)'}))
                my_selected_shift_type = st.selectbox("시간대 선택", shifts_on_date, index=None, placeholder="시간대를 선택하세요", key="my_shift_type")
            else:
                st.selectbox("시간대 선택", [], disabled=True, placeholder="날짜를 먼저 선택하세요", key="my_shift_type_disabled")

        with cols_my_to_them[2]:
            compatible_colleagues = []
            selectbox_placeholder = "시간대를 선택하세요"
            is_disabled = True
            
            if my_selected_date_str and my_selected_shift_type:
                is_disabled = False
                my_date = user_date_options[my_selected_date_str]
                all_colleagues = get_all_employee_names(df_schedule) - {user_name}
                my_shifts_on_date = {s['shift_type'] for s in user_shifts if s['date_obj'] == my_date}
                
                if '오전당직(온콜)' in my_shifts_on_date:
                    row_data = df_schedule[df_schedule['날짜_dt'].dt.date == my_date].iloc[0]
                    am_workers = {row_data[col] for col in AM_COLS if col in row_data and row_data[col]} - {user_name, ''}
                    non_am_workers = {c for c in all_colleagues if not is_person_assigned_at_time(df_schedule, c, my_date, '오전')}
                    compatible_colleagues = sorted(list(am_workers | non_am_workers))
                else:
                    compatible_colleagues = sorted([c for c in all_colleagues if not is_person_assigned_at_time(df_schedule, c, my_date, my_selected_shift_type)])
                
                selectbox_placeholder = "상대방을 선택하세요"
                if not compatible_colleagues:
                    selectbox_placeholder = "교대 가능한 동료 없음"
                    is_disabled = True
            
            selected_colleague_name = st.selectbox("교환할 상대방 선택", compatible_colleagues, index=None, placeholder=selectbox_placeholder, disabled=is_disabled, key="my_colleague")

        with cols_my_to_them[3]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            is_request_disabled = not all([my_selected_date_str, my_selected_shift_type, selected_colleague_name])

            if st.button("➕ 요청 추가", key="add_my_to_them_request_button", use_container_width=True, disabled=is_request_disabled):
                my_date = user_date_options[my_selected_date_str]
                final_shift_type = my_selected_shift_type
                
                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "변경 요청": f"{user_name} ➡️ {selected_colleague_name}",
                    "변경 요청한 스케줄": f"{my_date.strftime('%Y-%m-%d')} ({final_shift_type})",
                }
                with st.spinner("요청을 기록하는 중입니다..."):
                    status = add_request_to_sheet(new_request, month_str)
                    if status == "SUCCESS":
                        st.success("요청이 성공적으로 기록되었습니다.")
                        time.sleep(1.5)
                        st.rerun()
                    elif status == "DUPLICATE":
                        st.error("이미 존재하는 변경 요청입니다.")
                        time.sleep(1.5)
                        st.rerun()
                        
        # --- 동적 경고 메시지 표시 ---
        if my_selected_date_str and my_selected_shift_type:
            my_date = user_date_options[my_selected_date_str]
            my_shifts_on_date = {s['shift_type'] for s in user_shifts if s['date_obj'] == my_date}
            
            # 💡 [핵심 수정] 내가 오전당직(온콜)인 날짜를 선택했다면 경고/안내 표시
            if '오전당직(온콜)' in my_shifts_on_date:
                row_data = df_schedule[df_schedule['날짜_dt'].dt.date == my_date].iloc[0]
                am_workers_list = sorted(list({row_data[col] for col in AM_COLS if col in row_data and row_data[col]} - {user_name, ''}))
                all_colleagues = get_all_employee_names(df_schedule) - {user_name}
                non_am_workers_list = sorted(list({c for c in all_colleagues if not is_person_assigned_at_time(df_schedule, c, my_date, '오전')} - set(am_workers_list)))

                st.warning(f"해당 날짜는 {user_name}님의 오전당직이 있는 날입니다. 근무자를 선택하시는 경우 당직이 변경되며, 미근무자를 선택하게 되면 오전,오후,오전당직이 모두 변경됩니다.")
                st.info(f"근무자: {', '.join(am_workers_list) if am_workers_list else '없음'}\n\n미근무자: {', '.join(non_am_workers_list) if non_am_workers_list else '없음'}")

    st.write(" ")
    st.markdown("<h6 style='font-weight:bold;'>🔵 상대방의 스케줄을 나와 바꾸기</h6>", unsafe_allow_html=True)
    cols_them_to_my = st.columns([2, 2, 2, 1])

    with cols_them_to_my[0]:
        colleagues = sorted(list(get_all_employee_names(df_schedule) - {user_name}))
        selected_colleague_name_them = st.selectbox("상대방 선택", colleagues, index=None, placeholder="상대방을 선택하세요", key="them_colleague")

    with cols_them_to_my[1]:
        colleague_shifts = get_person_shifts(df_schedule, selected_colleague_name_them) if selected_colleague_name_them else []
        colleague_shift_dates = sorted(list(set(s['date_obj'] for s in colleague_shifts)))
        colleague_date_options = {d.strftime("%-m월 %-d일") + f" ({'월화수목금토일'[d.weekday()]})": d for d in colleague_shift_dates}
        selected_colleague_date_str = st.selectbox("상대방 근무일 선택", colleague_date_options.keys(), index=None, placeholder="상대방을 선택하세요", key="them_date", disabled=not selected_colleague_name_them)

    with cols_them_to_my[2]:
        selected_colleague_shift_type = None
        selected_colleague_shift_type_display = None
        available_shifts_for_display = []

        if selected_colleague_date_str:
            selected_date_obj = colleague_date_options[selected_colleague_date_str]
            
            # 1. 동료의 해당 날짜 모든 근무 형태를 확인
            colleague_shifts_on_date = {s['shift_type'] for s in colleague_shifts if s['date_obj'] == selected_date_obj}
            
            # 💡 [핵심 수정] 나의 스케줄과 상관없이, 동료의 근무 형태만으로 선택지를 생성
            display_options = set()
            if '오전당직(온콜)' in colleague_shifts_on_date:
                # 동료가 당직이면, 무조건 '오전', '오후'를 선택지로 제공
                display_options.add('오전')
                display_options.add('오후')
            else:
                # 동료가 일반 근무이면, 해당 근무만 선택지로 제공
                if '오전' in colleague_shifts_on_date:
                    display_options.add('오전')
                if '오후' in colleague_shifts_on_date:
                    display_options.add('오후')

            available_shifts_for_display = sorted(list(display_options))
            selected_colleague_shift_type_display = st.selectbox("시간대 선택", available_shifts_for_display, index=None, placeholder="시간대를 선택하세요", key="them_shift_type", disabled=not available_shifts_for_display)
            
            selected_colleague_shift_type = selected_colleague_shift_type_display
        else:
            st.selectbox("시간대 선택", [], disabled=True, placeholder="날짜를 먼저 선택하세요", key="them_shift_type_disabled")

    # 동적 경고 메시지 표시
    if selected_colleague_date_str and selected_colleague_shift_type_display:
        selected_date_obj = colleague_date_options[selected_colleague_date_str]
        colleague_shifts_on_date = {s['shift_type'] for s in colleague_shifts if s['date_obj'] == selected_date_obj}

        # 💡 [핵심 수정] 동료가 당직이면, '오전' 또는 '오후' 무엇을 선택하든 경고 표시
        if '오전당직(온콜)' in colleague_shifts_on_date:
            my_shifts_on_date = {s['shift_type'] for s in user_shifts if s['date_obj'] == selected_date_obj}
            if '오전' in my_shifts_on_date or '오전당직(온콜)' in my_shifts_on_date:
                st.warning(f"해당 날짜는 {selected_colleague_name_them}님의 오전당직 날짜이며, {user_name}님도 근무가 있는 날입니다. 오전당직이 {user_name}님으로 변경됩니다.")
            else:
                st.warning(f"해당 날짜는 {selected_colleague_name_them}님의 오전당직 날짜입니다. 오전,오후,오전당직이 모두 {user_name}님으로 변경됩니다.")

    with cols_them_to_my[3]:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        request_disabled_them = not all([selected_colleague_name_them, selected_colleague_date_str, selected_colleague_shift_type])
        
        if st.button("➕ 요청 추가", key="add_them_to_my_request_button", use_container_width=True, disabled=request_disabled_them):
            colleague_date_obj = colleague_date_options[selected_colleague_date_str]
            final_shift_type = selected_colleague_shift_type
            
            new_request = {
                "RequestID": str(uuid.uuid4()),
                "요청일시": datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S'),
                "요청자": user_name,
                "요청자 사번": employee_id,
                "변경 요청": f"{selected_colleague_name_them} ➡️ {user_name}",
                "변경 요청한 스케줄": f"{colleague_date_obj.strftime('%Y-%m-%d')} ({final_shift_type})",
            }
            with st.spinner("요청을 기록하는 중입니다..."):
                status = add_request_to_sheet(new_request, month_str)
                if status == "SUCCESS":
                    st.success("요청이 성공적으로 기록되었습니다.")
                    time.sleep(1.5)
                    st.rerun()
                elif status == "DUPLICATE":
                    st.error("이미 존재하는 변경 요청입니다.")
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