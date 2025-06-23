import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
import re

# --- 상수 정의 ---
MONTH_STR = "2025년 04월"
YEAR_STR = MONTH_STR.split('년')[0] # "2025"

# set_page_config()를 스크립트 최상단으로 이동
st.set_page_config(page_title=f"{MONTH_STR} 방 변경 요청", layout="wide")

# --- 세션 상태 초기화 ---
if "change_requests" not in st.session_state:
    st.session_state.change_requests = []
if "selected_shift_str" not in st.session_state:
    st.session_state.selected_shift_str = None

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

# --- Google Sheets 저장 함수 (방 및 로그) ---
def save_room_data_to_google_sheets(df, month_str):
    """데이터프레임을 Google Sheets에 저장합니다."""
    df_to_save = df.drop(columns=['날짜_dt'], errors='ignore')
    df_to_save.fillna('', inplace=True)
    data = [df_to_save.columns.tolist()] + df_to_save.values.tolist()
    
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet = spreadsheet.worksheet(f"{month_str} 방배정")
        worksheet.clear()
        worksheet.update('A1', data)
        return True
    except Exception as e:
        st.error(f"Google Sheets 저장 실패: {e}")
        return False

def log_room_change_requests(requests, month_str):
    """조정사항을 로그 시트에 기록합니다."""
    if not requests: return True
    try:
        gc = get_gspread_client()
        if not gc: return False
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        try:
            worksheet = spreadsheet.worksheet(f"{month_str} 방 변경 요청")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=f"{month_str} 방 변경 요청", rows=100, cols=5)
            # [수정] 맞교환에 맞게 헤더 변경
            worksheet.append_row(['Timestamp', '요청 일자', '변경 전 (본인)', '변경 후 (교환 인원)', '변경 날짜'])
        
        log_data = []
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for req in requests:
            log_data.append([
                ts,
                req.get('from_display_str', ''),
                req.get('from_person', ''),
                req.get('to_person', ''),
                req.get('to_display_str', '')
            ])
        worksheet.append_rows(log_data)
        return True
    except Exception as e:
        st.error(f"조정사항 로그 기록 실패: {e}")
        return False

# [수정] 맞교환 로직으로 업데이트
def update_room_with_requests(month_str, requests, original_df):
    """변경 요청 목록(맞교환)을 기반으로 방 배정을 업데이트합니다."""
    df_updated = original_df.copy()

    for req in requests:
        from_person, to_person = req['from_person'], req['to_person']
        # 맞교환에서는 날짜가 동일함
        change_date = req['from_date_obj']
        from_col, to_col = req['from_column_name'], req['to_column_name']

        row_idx_list = df_updated.index[df_updated['날짜_dt'].dt.date == change_date].tolist()
        if not row_idx_list:
            st.error(f"오류: 날짜 '{change_date}'를 찾을 수 없습니다.")
            continue
        
        row_idx = row_idx_list[0]
        
        current_from_person = df_updated.loc[row_idx, from_col]
        current_to_person = df_updated.loc[row_idx, to_col]

        if current_from_person == from_person and current_to_person == to_person:
            # 두 인원의 위치를 맞바꿈
            df_updated.loc[row_idx, from_col] = to_person
            df_updated.loc[row_idx, to_col] = from_person
        else:
            st.error(f"오류: {req['from_display_str']}의 교환 처리 중 문제가 발생했습니다. 데이터가 변경되었을 수 있습니다.")

    return save_room_data_to_google_sheets(df_updated, month_str)

# --- 헬퍼 함수 ---
# [수정] 요청 일자 목록 정렬 로직 수정
def get_person_room_assignments(df, person_name):
    """특정 인원의 모든 방 배정 목록을 반환하고 정렬합니다."""
    assignments = []
    # 데이터프레임을 날짜순으로 먼저 정렬
    sorted_df = df.sort_values(by='날짜_dt').reset_index(drop=True)
    
    # 열 이름에서 시간 정보를 추출하여 정렬하기 위한 함수
    def sort_key(col_name):
        match = re.match(r"(\d{1,2}:\d{2})", str(col_name))
        if match:
            time_str = match.group(1)
            if ':' in time_str and len(time_str.split(':')[0]) == 1:
                time_str = f"0{time_str}"
            return datetime.strptime(time_str, "%H:%M").time()
        return datetime.max.time() # 숫자로 시작 안 하면 뒤로

    # 데이터프레임의 시간 관련 열들을 시간순으로 정렬
    time_cols = sorted([col for col in df.columns if re.match(r"(\d{1,2}:\d{2})", str(col))], key=sort_key)
    
    # 정렬된 순서대로 순회
    for index, row in sorted_df.iterrows():
        dt = row['날짜_dt']
        date_str = dt.strftime("%m월 %d일") + f" ({'월화수목금토일'[dt.weekday()]})"
        for col in time_cols:
            # 교환 불가능한 열 제외
            if '온콜' in str(col) or '당직' in str(col):
                continue
            
            # person_name이 비어있으면 모든 근무를, 아니면 해당 인원의 근무만 찾음
            if person_name == "" or row[col] == person_name:
                assignments.append({
                    'date_obj': dt.date(),
                    'column_name': str(col),
                    'person_name': row[col],
                    'display_str': f"{date_str} - {col}"
                })
    return assignments

def get_shift_period(column_name):
    """주어진 열 이름이 오전인지 오후인지 판단합니다."""
    am_pattern = re.compile(r"^(8:30|9:00|9:30|10:00)")
    if am_pattern.match(str(column_name)):
        return "오전"
    pm_pattern = re.compile(r"^(13:30)")
    if pm_pattern.match(str(column_name)):
        return "오후"
    return None

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state.selected_shift_str = None
    st.rerun()

# --- 메인 로직 ---
def main():
    if not st.session_state.get("login_success"):
        st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
        return

    st.header(f"📅 {st.session_state.get('name', '사용자')} 님의 {MONTH_STR} 방 변경 요청", divider='rainbow')
    
    st.sidebar.write(f"현재 사용자: {st.session_state.get('name', '')} ({str(st.session_state.get('employee_id', '')).zfill(5)})")
    if st.sidebar.button("로그아웃"):
        st.session_state.clear()
        st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
        time.sleep(2)
        st.rerun()

    df_room = load_room_data(MONTH_STR)
    
    if df_room.empty:
        st.warning("방 데이터를 불러올 수 없거나 데이터가 비어있습니다.")
        return

    user_name = st.session_state.get("name", "")
    user_assignments = get_person_room_assignments(df_room, user_name)

    st.dataframe(df_room.drop(columns=['날짜_dt'], errors='ignore'))
    st.divider()

    st.subheader("✨ 방 교환 요청하기")
    
    st.markdown("##### 🟢 변경할 근무일자 선택")
    
    if not user_assignments:
        st.warning(f"'{user_name}'님의 교환 가능한 배정된 방이 없습니다. (온콜/당직 제외)")
    else:
        assignment_options = {a['display_str']: a for a in user_assignments}
        
        # [수정] UI를 원래대로 복원, 맞교환 로직은 내부적으로 처리
        cols = st.columns([2, 2, 1])
        with cols[0]:
            my_selected_shift_str = st.selectbox(
                "**요청 일자**",
                assignment_options.keys(),
                index=None,
                placeholder="변경을 원하는 나의 근무 선택"
            )
        
        with cols[1]:
            compatible_colleagues = []
            if my_selected_shift_str:
                my_shift = assignment_options[my_selected_shift_str]
                my_shift_date = my_shift['date_obj']
                my_shift_period = get_shift_period(my_shift['column_name'])
                
                all_assignments = get_person_room_assignments(df_room, "")
                
                for a in all_assignments:
                    if a['date_obj'] == my_shift_date and get_shift_period(a['column_name']) == my_shift_period:
                        if a['person_name'] and a['person_name'] != user_name:
                             compatible_colleagues.append({'name': a['person_name'], 'assignment': a})
            
            # 교환할 인원 목록 생성 (표시: 이름, 값: 인덱스)
            colleague_options = {i: p['name'] for i, p in enumerate(compatible_colleagues)}
            selected_colleague_idx = st.selectbox("**교환할 인원**", colleague_options.keys(), format_func=lambda i: colleague_options[i], index=None, placeholder="교환할 인원을 선택하세요")

        if cols[2].button("➕ 요청 추가", use_container_width=True, disabled=(not my_selected_shift_str or selected_colleague_idx is None)):
            selected_colleague_info = compatible_colleagues[selected_colleague_idx]
            
            new_request = {
                "from_person": user_name,
                "to_person": selected_colleague_info['name'],
                "from_date_obj": my_shift['date_obj'],
                "from_column_name": my_shift['column_name'],
                "from_display_str": my_shift['display_str'],
                "to_date_obj": selected_colleague_info['assignment']['date_obj'],
                "to_column_name": selected_colleague_info['assignment']['column_name'],
                "to_display_str": selected_colleague_info['assignment']['display_str'],
            }
            st.session_state.change_requests.append(new_request)
            st.success("교환 요청이 아래 목록에 추가되었습니다.")
            st.rerun()

    st.divider()
    st.markdown("##### 🟢 입력사항 확인")
    if not st.session_state.change_requests:
        st.info("추가된 변경 요청이 없습니다.")
    else:
        # [수정] 맞교환 정보 표시 (4열)
        display_data = [{
            '요청 일자': r.get('from_display_str', '알 수 없음'),
            '변경 전 (본인)': r.get('from_person', '알 수 없음'),
            '변경 후 (교환 인원)': r.get('to_person', '알 수 없음'),
            '변경 날짜': r.get('to_display_str', '알 수 없음')
        } for r in st.session_state.change_requests]
        st.dataframe(pd.DataFrame(display_data), use_container_width=True, hide_index=True)
        
        col1, col2 = st.columns([1, 6])
        if col1.button("🗑️ 전체 삭제", use_container_width=True):
            st.session_state.change_requests = []
            st.rerun()
        if col2.button("✅ 최종 제출하기", type="primary", use_container_width=True):
            with st.spinner("방 변경사항을 적용하고 로그를 기록하는 중입니다..."):
                update_success = update_room_with_requests(MONTH_STR, st.session_state.change_requests, df_room)
                log_success = log_room_change_requests(st.session_state.change_requests, MONTH_STR)

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
