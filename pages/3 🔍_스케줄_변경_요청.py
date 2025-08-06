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

@st.cache_data(ttl=30)
def get_my_requests(month_str, employee_id):
    if not employee_id: return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        headers = ['RequestID', '요청일시', '요청자', '요청자 사번', '요청자 기존 근무', '상대방', '상대방 기존 근무', '시간대']
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

def add_request_to_sheet(request_data, month_str):
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
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

# ... (기존 코드 생략) ...

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
    st.write("- 교환/대체할 상대방의 근무 중, 내가 근무하지 않는 날짜와 시간만 옵션에 표시됩니다.")
    st.write("- 오전 근무는 오전 근무끼리, 오후 근무는 오후 근무끼리만 교환 가능합니다.")
    st.write(" ")

    is_step2_active = st.session_state.pending_swap is not None

    if not is_step2_active:
        cols_top = st.columns([2, 1, 2])
        with cols_top[0]:
            selected_colleague = st.selectbox(
                "**교환/대체 근무할 상대방 선택**",
                all_colleagues,
                index=None,
                placeholder="상대방 선택"
            )
        
        with cols_top[1]:
            st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
            if st.button("다음 단계 ➞", use_container_width=True, disabled=(not selected_colleague)):
                st.session_state.pending_swap = {"colleague_name": selected_colleague}
                st.rerun()

    if is_step2_active:
        colleague_name = st.session_state.pending_swap["colleague_name"]
        
        # ⚠️ 변경된 부분: 나의 근무 리스트를 미리 가져와서 상대방 근무를 필터링
        user_shifts = get_person_shifts(df_schedule, user_name)
        colleague_shifts_all = get_person_shifts(df_schedule, colleague_name)

        # 상대방의 근무 중에서 나의 근무와 겹치지 않는 것만 선택
        my_shift_dates = {(s['date_obj'], s['shift_type']) for s in user_shifts}
        colleague_shifts = [
            s for s in colleague_shifts_all
            if (s['date_obj'], s['shift_type']) not in my_shift_dates
        ]

        if not colleague_shifts:
            st.error(f"**{colleague_name}** 님의 근무 중 교환/대체 가능한 날짜/시간대가 없습니다. (해당 일자에 본인의 근무가 없는지 확인해 주세요.)")
            st.session_state.pending_swap = None
            if st.button("이전 단계로 돌아가기"):
                st.rerun()
            st.stop()

        cols_bottom = st.columns([2, 2, 1])
        with cols_bottom[0]:
            colleague_shift_options = {s['display_str']: s for s in colleague_shifts}
            colleague_selected_shift_str = st.selectbox(
                f"**{colleague_name} 님의 교환/대체할 근무 선택**",
                colleague_shift_options.keys(),
                help="내가 근무하지 않는 날짜와 시간만 옵션에 표시됩니다.",
                index=None,
                placeholder="상대방 근무 선택"
            )

        with cols_bottom[1]:
            if colleague_selected_shift_str:
                selected_shift_data = colleague_shift_options[colleague_selected_shift_str]
                selected_shift_type = selected_shift_data['shift_type']
                selected_shift_date_obj = selected_shift_data['date_obj']
                
                # '대체하여 근무' 옵션 추가
                my_shift_options = {"대체하여 근무": {"display_str": "대체 근무", "shift_type": selected_shift_type}}
                
                # 호환되는 나의 근무를 추가하되, 상대방의 근무와 겹치지 않는 경우만 추가
                for s in user_shifts:
                    # 나의 근무 날짜와 시간대가 상대방의 근무와 겹치지 않는 경우에만 옵션에 추가
                    if s['shift_type'] == selected_shift_type and s['date_obj'] != selected_shift_date_obj:
                        my_shift_options[s['display_str']] = s
                        
                # '대체하여 근무' 옵션을 가장 위로 정렬
                my_shift_keys = list(my_shift_options.keys())
                my_shift_keys.sort(key=lambda x: (x != "대체하여 근무", x)) # '대체하여 근무'를 가장 먼저 정렬

                my_selected_shift_str = st.selectbox(
                    f"**나의 근무 선택** ({selected_shift_type} 근무)",
                    my_shift_keys,
                    index=0,
                    placeholder="교환할 나의 근무 선택 또는 대체"
                )
            else:
                my_selected_shift_str = None
                st.write("")

        cols_buttons = st.columns([1, 1, 4])
        with cols_buttons[0]:
            if st.button("➕ 요청 추가", use_container_width=True, type="primary", disabled=(not my_selected_shift_str)):
                colleague_shift = colleague_shift_options[colleague_selected_shift_str]

                if my_selected_shift_str == "대체하여 근무":
                    my_shift_data = {"display_str": "대체 근무", "shift_type": selected_shift_type}
                else:
                    my_shift_data = my_shift_options[my_selected_shift_str]

                new_request = {
                    "RequestID": str(uuid.uuid4()),
                    "요청일시": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                    "요청자": user_name,
                    "요청자 사번": employee_id,
                    "요청자 기존 근무": my_shift_data['display_str'],
                    "상대방": colleague_name,
                    "상대방 기존 근무": colleague_shift['display_str'],
                    "시간대": my_shift_data['shift_type']
                }

                with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                    success = add_request_to_sheet(new_request, MONTH_STR)
                    if success:
                        st.success("변경 요청이 성공적으로 기록되었습니다.")
                        st.session_state.pending_swap = None
                        st.rerun()

        with cols_buttons[1]:
            if st.button("❌ 취소", use_container_width=True):
                st.session_state.pending_swap = None
                st.rerun()

    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 스케줄 변경 요청 목록")

    my_requests = get_my_requests(MONTH_STR, employee_id)
    
    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        # 기존 HTML 카드 템플릿
        HTML_CARD_TEMPLATE = (
            '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
            '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
            '<thead><tr>'
            '<th style="font-weight: bold; color: #2E86C1; width: 50%; padding-bottom: 5px; font-size: 1.0em;">나의 근무</th>'
            '<th style="font-weight: bold; color: #28B463; width: 50%; padding-bottom: 5px; font-size: 1.0em;">교환 근무</th>'
            '</tr></thead>'
            '<tbody><tr>'
            '<td style="font-size: 1.1em; padding-top: 5px; vertical-align: middle;">{from_date_str}</td>'
            '<td style="font-size: 1.1em; padding-top: 5px; vertical-align: middle;">{to_date_str} (<strong style="color:#1E8449;">{to_person_name}</strong> 님)</td>'
            '</tr></tbody>'
            '</table>'
            '<hr style="border: none; border-top: 1px dotted #bdbdbd; margin: 8px 0 5px 0;">'
            '<div style="text-align: right; font-size: 0.85em; color: #757575;">요청 시간: {timestamp}</div>'
            '</div>'
        )

        for req in my_requests:
            req_id = req['RequestID']
            col1, col2 = st.columns([5, 1])
            with col1:
                # '대체 근무' 요청일 경우 '나의 근무'를 '대체하여 근무'로 변경
                from_date_str = req.get('요청자 기존 근무', '')
                if from_date_str == "대체 근무":
                    from_date_str = "대체하여 근무"
                
                card_html = HTML_CARD_TEMPLATE.format(
                    from_date_str=from_date_str,
                    to_date_str=req.get('상대방 기존 근무', ''),
                    to_person_name=req.get('상대방', ''),
                    timestamp=req.get('요청일시', '')
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🗑️ 삭제", key=f"del_{req_id}", use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        delete_request_from_sheet(req_id, MONTH_STR)
                        st.rerun()