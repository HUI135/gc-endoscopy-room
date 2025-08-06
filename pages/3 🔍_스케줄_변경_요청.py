import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
import uuid
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
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0]
AM_COLS = [str(i) for i in range(1, 13)] + ['온콜']
PM_COLS = [f'오후{i}' for i in range(1, 6)]
REQUEST_SHEET_NAME = f"{MONTH_STR} 스케줄 변경요청"

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
def load_schedule_data(month_str):
    try:
        gc = get_gspread_client()
        if not gc: return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 스케줄")
        records = worksheet.get_all_records()
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        if '오전당직(온콜)' in df.columns:
            df.rename(columns={'오전당직(온콜)': '온콜'}, inplace=True)
        if '날짜' not in df.columns:
            st.error("오류: Google Sheets 시트에 '날짜' 열이 없습니다.")
            return pd.DataFrame()
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(YEAR_STR + '년 ' + df['날짜'].astype(str), format='%Y년 %m월 %d일', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        return df
    except Exception as e:
        st.error(f"스케줄 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

# 요청 목록 가져오는 함수
@st.cache_data(ttl=30)
def get_my_requests(month_str, employee_id):
    if not employee_id: return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        # 새로운 컬럼 이름으로 헤더 생성
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '요청자 기존 근무', '상대방', '상대방 기존 근무', '시간대']
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=len(headers))
            worksheet.append_row(headers)
            return []
        
        all_requests = worksheet.get_all_records()
        # '요청자 사번' 컬럼으로 필터링
        my_requests = [req for req in all_requests if str(req.get('요청자 사번')) == str(employee_id)]
        return my_requests
    except Exception as e:
        st.error(f"요청 목록을 불러오는 중 오류 발생: {e}")
        return []

# 요청을 시트에 추가하는 함수
def add_request_to_sheet(request_data, month_str):
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        # 새로운 컬럼 순서에 맞게 데이터 추가
        row_to_add = [
            request_data['RequestID'], request_data['요청일시'], request_data['요청자'],
            request_data['요청자 사번'], request_data['요청자 기존 근무'], request_data['상대방'],
            request_data['상대방 기존 근무'], request_data['시간대']
        ]
        worksheet.append_row(row_to_add)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"교환 요청 저장 실패: {e}")
        return False

def delete_request_from_sheet(request_id, month_str):
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

def get_person_shifts(df, person_name):
    shifts = []
    am_cols_in_df = [col for col in AM_COLS if col in df.columns]
    pm_cols_in_df = [col for col in PM_COLS if col in df.columns]
    for _, row in df.iterrows():
        is_am = person_name in row[am_cols_in_df].values
        is_pm = person_name in row[pm_cols_in_df].values
        dt = row['날짜_dt']
        date_str = dt.strftime("%m월 %d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        if is_am: shifts.append({'date_obj': dt.date(), 'shift_type': '오전', 'display_str': f"{date_str} 오전"})
        if is_pm: shifts.append({'date_obj': dt.date(), 'shift_type': '오후', 'display_str': f"{date_str} 오후"})
    return shifts

# --- 메인 로직 ---
user_name = st.session_state.get("name", "")
employee_id = st.session_state.get("employee_id", "")

st.header(f"📅 {user_name} 님의 {MONTH_STR} 스케줄 변경 요청", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.rerun()

df_schedule = load_schedule_data(MONTH_STR)

if df_schedule.empty:
    st.warning("스케줄 데이터를 불러올 수 없습니다.")
else:
    all_names = set(df_schedule[AM_COLS + PM_COLS].values.ravel()) - {''}
    all_colleagues = sorted(list(all_names - {user_name}))
    st.dataframe(df_schedule.drop(columns=['날짜_dt'], errors='ignore'), use_container_width=True, hide_index=True)
    st.divider()

    st.markdown("#### ✨ 스케줄 변경 요청하기")
    st.write("- 오전 근무는 오전 근무끼리, 오후 근무는 오후 근무끼리만 교환 가능합니다.")
        
    st.write(" ")
        
    # --- [변경 시작] 요청 타입 선택 기능 추가 ---
    request_type = st.radio(
        "요청 방식 선택",
        ["근무 교환", "대체하여 근무"],
        key="request_type_radio",
        horizontal=True
    )
        
    # "대체하여 근무"를 선택했을 때의 안내 문구
    if request_type == "대체하여 근무":
        st.info("선택한 상대방의 근무를 나의 근무로 변경 요청합니다. 나의 기존 근무는 사라지지 않습니다.")

    # --- [변경 시작] 1단계 UI 로직 변경 ---
    is_step2_active = st.session_state.pending_swap is not None

    # '대체하여 근무'인 경우, 나의 근무 선택 없이 바로 상대방 근무 선택으로 넘어감
    if request_type == "대체하여 근무":
        if not is_step2_active:
            cols_takeover = st.columns([2, 2, 1])
            with cols_takeover[0]:
                selected_colleague = st.selectbox("상대방 선택", all_colleagues, index=None, placeholder="대체할 근무를 가진 인원 선택")
            with cols_takeover[1]:
                st.write("")
                st.session_state.pending_swap = {"request_type": "대체 근무", "colleague_name": selected_colleague}
                st.rerun()

    # '근무 교환'인 경우, 기존의 1단계 로직
    else: # request_type == "근무 교환"
        user_shifts = get_person_shifts(df_schedule, user_name)
        if not user_shifts and not is_step2_active:
            st.warning(f"'{user_name}'님의 배정된 근무일이 없습니다. 교환이 불가합니다.")
        else:
            cols_top = st.columns([2, 2, 1])
            if is_step2_active:
                # ... 2단계 상단 UI ...
                with cols_top[2]:
                    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                    if st.button("✏️ 수정", use_container_width=True):
                        st.session_state.pending_swap = None
                        st.rerun()
            else:
                my_shift_options = {s['display_str']: s for s in user_shifts}
                with cols_top[0]:
                    my_selected_shift_str = st.selectbox("**요청 일자**", my_shift_options.keys(), index=None, placeholder="변경을 원하는 나의 근무 선택")
                with cols_top[1]:
                    selected_colleague = st.selectbox("**변경 후 인원**", all_colleagues, index=None, placeholder="교환할 인원을 선택하세요")
                with cols_top[2]:
                    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                    if st.button("다음 단계 ➞", use_container_width=True, disabled=(not my_selected_shift_str or not selected_colleague)):
                        st.session_state.pending_swap = {"request_type": "근무 교환", "my_shift": my_shift_options[my_selected_shift_str], "colleague_name": selected_colleague}
                        st.rerun()
    # --- [변경 종료] 1단계 UI 로직 변경 ---

    # --- [변경 시작] 2단계 UI 및 요청 생성 로직 변경 ---
    if is_step2_active:
        req_type = st.session_state.pending_swap["request_type"]
        colleague_name = st.session_state.pending_swap["colleague_name"]
        
        st.write(" ")
        
        colleague_shifts = get_person_shifts(df_schedule, colleague_name)
        
        # 근무 교환 로직
        if req_type == "근무 교환":
            my_shift = st.session_state.pending_swap["my_shift"]
            st.markdown(f"<h6 style='font-weight:bold;'>🔴 {colleague_name} 님의 근무와 교환</h6>", unsafe_allow_html=True)
            st.info(f"'{my_shift['display_str']}' 근무를 **{colleague_name}** 님의 아래 근무와 교환합니다.")
            compatible_shifts = [s for s in colleague_shifts if s['shift_type'] == my_shift['shift_type']]
            if not compatible_shifts:
                st.error(f"**{colleague_name}** 님은 교환 가능한 {my_shift['shift_type']} 근무가 없습니다.")
                st.session_state.pending_swap = None
            else:
                colleague_shift_options = {s['display_str']: s for s in compatible_shifts}
                cols_bottom = st.columns([2, 1])
                with cols_bottom[0]:
                    colleague_selected_shift_str = st.selectbox(f"**{colleague_name}님의 교환할 근무 선택**", colleague_shift_options.keys(), index=None, placeholder="상대방 근무 선택")
                with cols_bottom[1]:
                    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                    if st.button("➕ 요청 추가", use_container_width=True, type="primary", disabled=(not colleague_selected_shift_str)):
                        colleague_shift = colleague_shift_options[colleague_selected_shift_str]
                        # 새로운 데이터 형식으로 요청 생성
                        new_request = {
                            "RequestID": str(uuid.uuid4()),
                            "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                            "요청자": user_name,
                            "요청자 사번": employee_id,
                            "요청자 기존 근무": my_shift['display_str'],
                            "상대방": colleague_name,
                            "상대방 기존 근무": colleague_shift['display_str'],
                            "시간대": my_shift['shift_type']
                        }
                        with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                            success = add_request_to_sheet(new_request, MONTH_STR)
                            if success:
                                st.success("변경 요청이 성공적으로 기록되었습니다.")
                                st.session_state.pending_swap = None
                                st.rerun()

        # 대체 근무 로직
        else: # req_type == "대체 근무"
            st.markdown(f"<h6 style='font-weight:bold;'>🔴 {colleague_name} 님의 근무 대체</h6>", unsafe_allow_html=True)
            st.info(f"**{colleague_name}** 님의 아래 근무를 **대체**하여 근무합니다.")
            
            if not colleague_shifts:
                st.error(f"**{colleague_name}** 님에게는 대체 가능한 근무가 없습니다.")
                st.session_state.pending_swap = None
            else:
                colleague_shift_options = {s['display_str']: s for s in colleague_shifts}
                cols_bottom = st.columns([2, 1])
                with cols_bottom[0]:
                    colleague_selected_shift_str = st.selectbox(f"**{colleague_name}님의 대체할 근무 선택**", colleague_shift_options.keys(), index=None, placeholder="상대방 근무 선택")
                with cols_bottom[1]:
                    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                    if st.button("➕ 요청 추가", use_container_width=True, type="primary", disabled=(not colleague_selected_shift_str)):
                        colleague_shift = colleague_shift_options[colleague_selected_shift_str]
                        # 대체 근무 요청 데이터 생성
                        new_request = {
                            "RequestID": str(uuid.uuid4()),
                            "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                            "요청자": user_name,
                            "요청자 사번": employee_id,
                            "요청자 기존 근무": "대체 근무", # '대체 근무'로 구분
                            "상대방": colleague_name,
                            "상대방 기존 근무": colleague_shift['display_str'],
                            "시간대": colleague_shift['shift_type']
                        }
                        with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                            success = add_request_to_sheet(new_request, MONTH_STR)
                            if success:
                                st.success("변경 요청이 성공적으로 기록되었습니다.")
                                st.session_state.pending_swap = None
                                st.rerun()
    # --- [변경 종료] 2단계 UI 및 요청 생성 로직 변경 ---

    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 스케줄 변경 요청 목록")
        
    my_requests = get_my_requests(MONTH_STR, employee_id)
        
    # --- [변경 시작] 요청 목록 표시 로직 변경 ---
    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        for req in my_requests:
            req_id = req['RequestID']
            col1, col2 = st.columns([5, 1])
            with col1:
                # '대체 근무'와 '근무 교환'을 구분하여 표시
                if req.get('요청자 기존 근무') == "대체 근무":
                    card_html = f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                        <table style="width: 100%; border-collapse: collapse; text-align: center;">
                            <thead><tr>
                                <th style="font-weight: bold; color: #E74C3C; width: 100%; padding-bottom: 5px; font-size: 1.0em;">대체하여 근무</th>
                            </tr></thead>
                            <tbody><tr>
                                <td style="font-size: 1.1em; padding-top: 5px; vertical-align: middle;">{req.get('상대방 기존 근무', '')} (<strong style="color:#1E8449;">{req.get('상대방', '')}</strong> 님)</td>
                            </tr></tbody>
                        </table>
                        <hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 8px 0 5px 0;">
                        <div style="text-align: right; font-size: 0.85em; color: #757575;">요청 시간: {req.get('요청일시', '')}</div>
                    </div>
                    """
                else: # 기존 근무 교환 로직
                    card_html = f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                        <table style="width: 100%; border-collapse: collapse; text-align: center;">
                            <thead><tr>
                                <th style="font-weight: bold; color: #2E86C1; width: 50%; padding-bottom: 5px; font-size: 1.0em;">나의 근무</th>
                                <th style="font-weight: bold; color: #28B463; width: 50%; padding-bottom: 5px; font-size: 1.0em;">교환 근무</th>
                            </tr></thead>
                            <tbody><tr>
                                <td style="font-size: 1.1em; padding-top: 5px; vertical-align: middle;">{req.get('요청자 기존 근무', '')}</td>
                                <td style="font-size: 1.1em; padding-top: 5px; vertical-align: middle;">{req.get('상대방 기존 근무', '')} (<strong style="color:#1E8449;">{req.get('상대방', '')}</strong> 님)</td>
                            </tr></tbody>
                        </table>
                        <hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 8px 0 5px 0;">
                        <div style="text-align: right; font-size: 0.85em; color: #757575;">요청 시간: {req.get('요청일시', '')}</div>
                    </div>
                    """
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️ 삭제", key=f"del_{req_id}", use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        delete_request_from_sheet(req_id, MONTH_STR)
                        st.rerun()
    # --- [변경 종료] 요청 목록 표시 로직 변경 ---