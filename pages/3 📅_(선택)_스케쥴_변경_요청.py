import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date

# --- 상수 정의 ---
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0] # "2025"
AM_COLS = [str(i) for i in range(1, 13)] + ['온콜']
PM_COLS = [f'오후{i}' for i in range(1, 6)]

# set_page_config()를 스크립트 최상단으로 이동
st.set_page_config(page_title=f"{MONTH_STR} 스케쥴 변경 요청", layout="wide")

# --- 세션 상태 초기화 ---
if "change_requests" not in st.session_state:
    st.session_state.change_requests = []
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

# --- Google Sheets 저장 함수 (스케쥴 및 로그) ---
def save_schedule_to_google_sheets(df, month_str):
    """데이터프레임을 Google Sheets에 저장합니다."""
    df_to_save = df.drop(columns=['날짜_dt'], errors='ignore')
    if '온콜' in df_to_save.columns:
        df_to_save.rename(columns={'온콜': '오전당직(온콜)'}, inplace=True)
    df_to_save.fillna('', inplace=True)
    data = [df_to_save.columns.tolist()] + df_to_save.values.tolist()
    
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 스케쥴")
        worksheet.clear()
        worksheet.update('A1', data)
        return True
    except Exception as e:
        st.error(f"Google Sheets 저장 실패: {e}")
        return False

def log_swap_requests(requests, month_str):
    """조정사항을 로그 시트에 기록합니다."""
    if not requests: return True
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(f"{month_str} 스케쥴 조정사항")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=f"{month_str} 스케쥴 조정사항", rows=100, cols=5)
            worksheet.append_row(['Timestamp', '요청 일자', '변경 전 (본인)', '변경 후 (인원)', '변경 날짜'])
        
        log_data = []
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for req in requests:
            log_data.append([
                ts,
                req.get('from_date_str', ''),
                req.get('from_person', ''),
                req.get('to_person', ''),
                req.get('to_date_str', '')
            ])
        worksheet.append_rows(log_data)
        return True
    except Exception as e:
        st.error(f"조정사항 로그 기록 실패: {e}")
        return False

def update_schedule_with_requests(month_str, requests, original_df):
    """변경 요청 목록(교환)을 기반으로 스케쥴을 업데이트합니다."""
    df_updated = original_df.copy()

    for req in requests:
        from_person, to_person = req['from_person'], req['to_person']
        from_date, to_date = req['from_date_obj'], req['to_date_obj']
        shift_type = req['shift_type']
        
        cols_to_search = AM_COLS if shift_type == '오전' else PM_COLS

        from_row_idx = df_updated.index[df_updated['날짜_dt'].dt.date == from_date].tolist()
        if from_row_idx:
            from_row_idx = from_row_idx[0]
            found = False
            for col in cols_to_search:
                if col in df_updated.columns and df_updated.loc[from_row_idx, col] == from_person:
                    df_updated.loc[from_row_idx, col] = to_person
                    found = True
                    break
            if not found: st.error(f"{req['from_date_str']}에 '{from_person}' 님의 근무를 찾지 못했습니다.")

        to_row_idx = df_updated.index[df_updated['날짜_dt'].dt.date == to_date].tolist()
        if to_row_idx:
            to_row_idx = to_row_idx[0]
            found = False
            for col in cols_to_search:
                 if col in df_updated.columns and df_updated.loc[to_row_idx, col] == to_person:
                    df_updated.loc[to_row_idx, col] = from_person
                    found = True
                    break
            if not found: st.error(f"{req['to_date_str']}에 '{to_person}' 님의 근무를 찾지 못했습니다.")

    return save_schedule_to_google_sheets(df_updated, month_str)


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

    st.header(f"📅 {st.session_state.get('name', '사용자')} 님의 {MONTH_STR} 스케쥴 변경 요청", divider='rainbow')
    
    # [수정] 사용자가 요청한 사이드바 코드로 변경 및 main 함수 안으로 이동
    st.sidebar.write(f"현재 사용자: {st.session_state.get('name', '')} ({str(st.session_state.get('employee_id', '')).zfill(5)})")
    if st.sidebar.button("로그아웃"):
        st.session_state.clear()
        st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
        time.sleep(2)
        st.rerun()

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

    user_name = st.session_state.get("name", "")
    user_shifts = get_person_shifts(df_schedule, user_name)
    all_names = set(df_schedule[AM_COLS + PM_COLS].values.ravel()) - {''}
    all_colleagues = sorted(list(all_names - {user_name}))

    st.dataframe(df_schedule.drop(columns=['날짜_dt'], errors='ignore'))
    st.divider()

    st.subheader("✨ 스케쥴 교환 요청하기")
    st.write("- 오전 근무는 오전 근무끼리, 오후 근무는 오후 근무끼리만 교환 가능합니다.")

    # [수정] 1단계와 2단계를 분리하여 1단계가 항상 보이도록 함
    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>🟢 변경할 근무일자 선택</h6>", unsafe_allow_html=True)
    
    is_step2_active = st.session_state.pending_swap is not None
    
    if not user_shifts and not is_step2_active:
        st.warning(f"'{user_name}'님의 배정된 근무일이 없습니다.")
    else:
        cols = st.columns([2, 2, 1])
        # 2단계가 활성화된 경우, 1단계는 선택된 값으로 고정하여 보여줌
        if is_step2_active:
            my_shift_display = st.session_state.pending_swap['my_shift']['display_str']
            colleague_display = st.session_state.pending_swap['colleague_name']
            with cols[0]:
                st.text_input("**요청 일자**", value=my_shift_display, disabled=True)
            with cols[1]:
                st.text_input("**변경 후 인원**", value=colleague_display, disabled=True)
            # 1단계 수정을 위한 버튼
            if cols[2].button("✏️ 수정"):
                st.session_state.pending_swap = None
                st.rerun()
        # 1단계가 활성화된 경우, 선택 위젯을 보여줌
        else:
            my_shift_options = {s['display_str']: s for s in user_shifts}
            with cols[0]:
                my_selected_shift_str = st.selectbox(
                    "**요청 일자**",
                    my_shift_options.keys(),
                    index=None,
                    placeholder="변경을 원하는 나의 근무 선택"
                )
            with cols[1]:
                selected_colleague = st.selectbox(
                    "**변경 후 인원**",
                    all_colleagues,
                    index=None,
                    placeholder="교환할 인원을 선택하세요"
                )
            # 다음 단계 버튼
            if cols[2].button("다음 단계 ➞", use_container_width=True, disabled=(not my_selected_shift_str or not selected_colleague)):
                st.session_state.pending_swap = {
                    "my_shift": my_shift_options[my_selected_shift_str],
                    "colleague_name": selected_colleague
                }
                st.rerun()

    # 2단계: 상대방의 근무일 선택 (조건부 표시)
    if is_step2_active:
        my_shift = st.session_state.pending_swap["my_shift"]
        colleague_name = st.session_state.pending_swap["colleague_name"]
        
        st.write(" ")
        st.markdown(f"<h6 style='font-weight:bold;'>🟢 {colleague_name} 님의 근무와 교환</h6>", unsafe_allow_html=True)
        info_str = f"'{my_shift['display_str']}' 근무를 **{colleague_name}** 님의 아래 근무와 교환합니다."
        st.info(info_str)

        colleague_shifts = get_person_shifts(df_schedule, colleague_name)
        compatible_shifts = [s for s in colleague_shifts if s['shift_type'] == my_shift['shift_type']]
        
        if not compatible_shifts:
            st.error(f"**{colleague_name}** 님은 교환 가능한 {my_shift['shift_type']} 근무가 없습니다.")
            if st.button("취소하고 돌아가기"):
                st.session_state.pending_swap = None
                st.rerun()
        else:
            colleague_shift_options = {s['display_str']: s for s in compatible_shifts}
            cols = st.columns([2, 1, 1])
            with cols[0]:
                colleague_selected_shift_str = st.selectbox(f"**{colleague_name}님의 교환할 근무 선택**", colleague_shift_options.keys(), index=None)
            
            if cols[1].button("➕ 요청 추가", use_container_width=True, type="primary", disabled=(not colleague_selected_shift_str)):
                colleague_shift = colleague_shift_options[colleague_selected_shift_str]
                new_request = {
                    "from_person": user_name,
                    "to_person": colleague_name,
                    "from_date_obj": my_shift['date_obj'],
                    "from_date_str": my_shift['display_str'],
                    "to_date_obj": colleague_shift['date_obj'],
                    "to_date_str": colleague_shift['display_str'],
                    "shift_type": my_shift['shift_type']
                }
                st.session_state.change_requests.append(new_request)
                st.session_state.pending_swap = None
                st.success("교환 요청이 아래 목록에 추가되었습니다.")
                st.rerun()

            if cols[2].button("취소", use_container_width=True):
                st.session_state.pending_swap = None
                st.rerun()

    st.divider()
    st.markdown("##### 📋 입력사항 확인")
    if not st.session_state.change_requests:
        st.info("추가된 변경 요청이 없습니다.")
    else:
        display_data = [{
            '요청 일자': r.get('from_date_str', '알 수 없음'),
            '변경 전 (본인)': r.get('from_person', '알 수 없음'),
            '변경 후 (교환 인원)': r.get('to_person', '알 수 없음'),
            '변경 날짜': r.get('to_date_str', '알 수 없음')
        } for r in st.session_state.change_requests]
        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
        
        col1, col2 = st.columns([1, 4])
        if col1.button("🗑️ 전체 삭제", use_container_width=True):
            st.session_state.change_requests = []
            st.rerun()
        if col2.button("✅ 최종 제출하기", type="primary", use_container_width=True):
            with st.spinner("스케쥴 변경사항을 적용하고 로그를 기록하는 중입니다..."):
                update_success = update_schedule_with_requests(MONTH_STR, st.session_state.change_requests, df_schedule)
                log_success = log_swap_requests(st.session_state.change_requests, MONTH_STR)

            if update_success and log_success:
                st.success("모든 변경 요청이 성공적으로 처리되었습니다!")
                st.balloons()
                st.session_state.change_requests = []
                st.cache_data.clear()
                time.sleep(2)
                st.rerun()
            else:
                st.error("업데이트 또는 로그 기록에 실패했습니다. 관리자에게 문의해주세요.")

if __name__ == "__main__":
    main()
