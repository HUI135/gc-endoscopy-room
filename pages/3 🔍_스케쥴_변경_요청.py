import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
import uuid
from zoneinfo import ZoneInfo # (수정) 한국 시간(KST)을 적용하기 위해 추가
import menu

st.set_page_config(page_title="마스터 수정", page_icon="🔍", layout="wide")

menu.menu()

# --- 상수 정의 ---
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0] # "2025"
AM_COLS = [str(i) for i in range(1, 13)] + ['온콜']
PM_COLS = [f'오후{i}' for i in range(1, 6)]
REQUEST_SHEET_NAME = f"{MONTH_STR} 스케쥴 교환요청"

# --- 세션 상태 초기화 ---
if "pending_swap" not in st.session_state:
    st.session_state.pending_swap = None

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

# --- 데이터 로딩 함수 (스케쥴) ---
@st.cache_data(ttl=300)
def load_schedule_data(month_str):
    """지정된 월의 스케쥴 데이터를 Google Sheets에서 불러옵니다."""
    try:
        gc = get_gspread_client()
        if not gc: return pd.DataFrame()
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 스케쥴")
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
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{month_str} 스케쥴' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"스케쥴 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

# --- 교환 요청 관련 함수 (추가, 조회, 삭제) ---
@st.cache_data(ttl=30) # 요청 목록은 자주 바뀔 수 있으므로 TTL을 짧게 설정
def get_my_requests(month_str, employee_id):
    """현재 사용자의 모든 교환 요청을 Google Sheet에서 불러옵니다."""
    if not employee_id:
        return []
    try:
        gc = get_gspread_client()
        if not gc: return []
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"'{REQUEST_SHEET_NAME}' 시트가 없어 새로 생성합니다.")
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=8)
            headers = ['RequestID', 'Timestamp', 'RequesterName', 'RequesterID', 'FromDateStr', 'ToPersonName', 'ToDateStr', 'ShiftType']
            worksheet.append_row(headers)
            return []

        all_requests = worksheet.get_all_records()
        # RequesterID를 문자열로 변환하여 비교
        my_requests = [req for req in all_requests if str(req.get('RequesterID')) == str(employee_id)]
        return my_requests
    except Exception as e:
        st.error(f"요청 목록을 불러오는 중 오류 발생: {e}")
        return []

def add_request_to_sheet(request_data, month_str):
    """단일 교환 요청을 Google Sheet에 추가합니다."""
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            # 시트가 없으면 get_my_requests에서 생성하므로 이 부분은 예방 차원
            worksheet = spreadsheet.add_worksheet(title=REQUEST_SHEET_NAME, rows=100, cols=8)
            headers = ['RequestID', 'Timestamp', 'RequesterName', 'RequesterID', 'FromDateStr', 'ToPersonName', 'ToDateStr', 'ShiftType']
            worksheet.append_row(headers)

        row_to_add = [
            request_data['RequestID'],
            request_data['Timestamp'],
            request_data['RequesterName'],
            request_data['RequesterID'],
            request_data['FromDateStr'],
            request_data['ToPersonName'],
            request_data['ToDateStr'],
            request_data['ShiftType']
        ]
        worksheet.append_row(row_to_add)
        st.cache_data.clear() # 데이터 변경 후 캐시 클리어
        return True
    except Exception as e:
        st.error(f"교환 요청 저장 실패: {e}")
        return False

def delete_request_from_sheet(request_id, month_str):
    """RequestID를 기반으로 특정 교환 요청을 Google Sheet에서 삭제합니다."""
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(REQUEST_SHEET_NAME)
        
        cell = worksheet.find(request_id)
        if cell:
            worksheet.delete_rows(cell.row)
            st.cache_data.clear() # 데이터 변경 후 캐시 클리어
            return True
        else:
            st.error("삭제할 요청을 찾을 수 없습니다. 이미 삭제되었을 수 있습니다.")
            return False
    except Exception as e:
        st.error(f"요청 삭제 중 오류 발생: {e}")
        return False


# --- 헬퍼 함수 ---
def get_person_shifts(df, person_name):
    """특정 인원의 모든 근무 시간(오전/오후) 목록을 반환합니다."""
    shifts = []
    am_cols_in_df = [col for col in AM_COLS if col in df.columns]
    pm_cols_in_df = [col for col in PM_COLS if col in df.columns]

    for _, row in df.iterrows():
        is_am = person_name in row[am_cols_in_df].values
        is_pm = person_name in row[pm_cols_in_df].values
        dt = row['날짜_dt']
        date_str = dt.strftime("%m월 %d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        if is_am:
            shifts.append({'date_obj': dt.date(), 'shift_type': '오전', 'display_str': f"{date_str} 오전"})
        if is_pm:
            shifts.append({'date_obj': dt.date(), 'shift_type': '오후', 'display_str': f"{date_str} 오후"})
    return shifts

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state.selected_shift_str = None
    st.rerun()

# --- 메인 로직 ---
def main():
    if not st.session_state.get("login_success"):
        st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
        return

    user_name = st.session_state.get("name", "")
    employee_id = st.session_state.get("employee_id", "")

    st.header(f"📅 {user_name} 님의 {MONTH_STR} 스케쥴 변경 요청", divider='rainbow')
    
    df_schedule = load_schedule_data(MONTH_STR)

    if df_schedule.empty:
        st.warning("스케쥴 데이터를 불러올 수 없거나 데이터가 비어있습니다.")
        with st.expander("오류 해결 가이드"):
            st.info(f"""
            1.  **Google Sheets 이름 확인**: `{MONTH_STR} 스케쥴` 시트가 정확한 이름으로 존재하는지 확인해주세요.
            2.  **'날짜' 열 확인**: 시트의 첫 행에 '날짜'라는 이름의 열이 있는지, 날짜들이 `4월 1일` 형식으로 잘 입력되어 있는지 확인해주세요.
            3.  **내용 확인**: 시트에 헤더만 있고 실제 데이터 행이 없는지 확인해주세요.
            """)
        return

    all_names = set(df_schedule[AM_COLS + PM_COLS].values.ravel()) - {''}
    all_colleagues = sorted(list(all_names - {user_name}))

    st.dataframe(df_schedule.drop(columns=['날짜_dt'], errors='ignore'))
    st.divider()

    st.markdown("#### ✨ 스케쥴 변경 요청하기")
    st.write("- 오전 근무는 오전 근무끼리, 오후 근무는 오후 근무끼리만 교환 가능합니다.")

    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>🟢 변경할 근무일자 선택</h6>", unsafe_allow_html=True)
    
    user_shifts = get_person_shifts(df_schedule, user_name)
    is_step2_active = st.session_state.pending_swap is not None
    
    if not user_shifts and not is_step2_active:
        st.warning(f"'{user_name}'님의 배정된 근무일이 없습니다.")
    else:
        # --- 1단계 또는 2단계 상단 UI ---
        cols_top = st.columns([2, 2, 1])
        if is_step2_active:
            my_shift_display = st.session_state.pending_swap['my_shift']['display_str']
            colleague_display = st.session_state.pending_swap['colleague_name']
            with cols_top[0]:
                st.text_input("**요청 일자**", value=my_shift_display, disabled=True)
            with cols_top[1]:
                st.text_input("**변경 후 인원**", value=colleague_display, disabled=True)
            
            with cols_top[2]:
                # [수정] '수정' 버튼 정렬을 위한 공백 추가
                st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                if st.button("✏️ 수정", use_container_width=True):
                    st.session_state.pending_swap = None
                    st.rerun()
        else:
            my_shift_options = {s['display_str']: s for s in user_shifts}
            with cols_top[0]:
                my_selected_shift_str = st.selectbox(
                    "**요청 일자**", my_shift_options.keys(),
                    index=None, placeholder="변경을 원하는 나의 근무 선택"
                )
            with cols_top[1]:
                selected_colleague = st.selectbox(
                    "**변경 후 인원**", all_colleagues,
                    index=None, placeholder="교환할 인원을 선택하세요"
                )
            with cols_top[2]:
                # [수정] '다음 단계' 버튼 정렬을 위한 공백 추가
                st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                if st.button("다음 단계 ➞", use_container_width=True, disabled=(not my_selected_shift_str or not selected_colleague)):
                    st.session_state.pending_swap = {
                        "my_shift": my_shift_options[my_selected_shift_str],
                        "colleague_name": selected_colleague
                    }
                    st.rerun()

    # --- 2단계 하단 UI ---
    if is_step2_active:
        my_shift = st.session_state.pending_swap["my_shift"]
        colleague_name = st.session_state.pending_swap["colleague_name"]
        
        st.write(" ")
        st.markdown(f"<h6 style='font-weight:bold;'>🔴 {colleague_name} 님의 근무와 교환</h6>", unsafe_allow_html=True)
        st.info(f"'{my_shift['display_str']}' 근무를 **{colleague_name}** 님의 아래 근무와 교환합니다.")

        colleague_shifts = get_person_shifts(df_schedule, colleague_name)
        compatible_shifts = [s for s in colleague_shifts if s['shift_type'] == my_shift['shift_type']]
        
        if not compatible_shifts:
            st.error(f"**{colleague_name}** 님은 교환 가능한 {my_shift['shift_type']} 근무가 없습니다.")
            if st.button("취소하고 돌아가기"):
                st.session_state.pending_swap = None
                st.rerun()
        else:
            colleague_shift_options = {s['display_str']: s for s in compatible_shifts}
            
            # [수정] '취소' 버튼 삭제로 컬럼을 2개로 변경
            cols_bottom = st.columns([2, 1])
            
            with cols_bottom[0]:
                colleague_selected_shift_str = st.selectbox(f"**{colleague_name}님의 교환할 근무 선택**", colleague_shift_options.keys(), index=None)
            
            with cols_bottom[1]:
                # [수정] '요청 추가' 버튼 정렬을 위한 공백 추가
                st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
                if st.button("➕ 요청 추가", use_container_width=True, type="primary", disabled=(not colleague_selected_shift_str)):
                    colleague_shift = colleague_shift_options[colleague_selected_shift_str]
                    new_request = {
                        "RequestID": str(uuid.uuid4()),
                        "Timestamp": datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S'),
                        "RequesterName": user_name,
                        "RequesterID": employee_id,
                        "FromDateStr": my_shift['display_str'],
                        "ToPersonName": colleague_name,
                        "ToDateStr": colleague_shift['display_str'],
                        "ShiftType": my_shift['shift_type']
                    }
                    with st.spinner("Google Sheet에 요청을 기록하는 중입니다..."):
                        success = add_request_to_sheet(new_request, MONTH_STR)
                        if success:
                            st.success("변경 요청이 성공적으로 기록되었습니다.")
                            st.session_state.pending_swap = None
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("요청 기록에 실패했습니다. 다시 시도해주세요.")
            
    st.divider()
    st.markdown(f"#### 📝 {user_name}님의 스케쥴 변경 요청 목록")
    
    my_requests = get_my_requests(MONTH_STR, employee_id)

    # HTML 코드를 2단 컬럼, 한 줄 문자열 방식으로 변경
    HTML_CARD_TEMPLATE = (
        '<div style="border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; background-color: #fcfcfc; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">'
            '<table style="width: 100%; border-collapse: collapse; text-align: center;">'
                '<thead><tr>'
                    '<th style="font-weight: bold; color: #2E86C1; width: 50%; padding-bottom: 8px; font-size: 1.1em;">나의 근무</th>'
                    '<th style="font-weight: bold; color: #28B463; width: 50%; padding-bottom: 8px; font-size: 1.1em;">교환 근무</th>'
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

    if not my_requests:
        st.info("현재 접수된 변경 요청이 없습니다.")
    else:
        for req in my_requests:
            req_id = req['RequestID']
            col1, col2 = st.columns([4, 1])
            with col1:
                # 위에서 만든 HTML '틀'에 실제 데이터를 채워서 보여줍니다.
                card_html = HTML_CARD_TEMPLATE.format(
                    from_date_str=req['FromDateStr'],
                    to_date_str=req['ToDateStr'],
                    to_person_name=req['ToPersonName'],
                    timestamp=req['Timestamp']
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with col2:
                # 삭제 버튼
                if st.button("🗑️ 삭제", key=f"del_{req_id}", use_container_width=True):
                    with st.spinner("요청을 삭제하는 중입니다..."):
                        delete_success = delete_request_from_sheet(req_id, MONTH_STR)
                        if delete_success:
                            st.success(f"요청이 삭제되었습니다.")
                            time.sleep(1)
                            st.rerun()

if __name__ == "__main__":
    main()