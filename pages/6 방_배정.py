import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import random
import time
from datetime import datetime, date
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu

st.set_page_config(page_title="방 배정", page_icon="🚪", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# 세션 상태 초기화
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False
if "df_room_request" not in st.session_state:
    st.session_state["df_room_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
if "room_settings" not in st.session_state:
    st.session_state["room_settings"] = {
        "830_room_select": ['1', '2', '4', '7'],
        "900_room_select": ['10', '11', '12'],
        "930_room_select": ['5', '6', '8'],
        "1000_room_select": ['3', '9'],
        "1330_room_select": ['2', '3', '4', '9']
    }
if "swapped_assignments" not in st.session_state:
    st.session_state["swapped_assignments"] = set()

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='RAW')
            return
        except Exception as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.error(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# 데이터 로드 (캐싱 사용) - 캐시 문제 방지
def load_data_page6(month_str):
    # 캐시 강제 갱신
    st.cache_data.clear()
    
    # load_data_page6_no_cache 호출
    result = load_data_page6_no_cache(month_str)
    
    # 반환값 디버깅
    if len(result) != 3:
        st.error(f"Expected 3 return values, but got {len(result)}. Returned: {result}")
        st.stop()
    
    return result

# 데이터 로드 (캐싱 미사용)
def load_data_page6_no_cache(month_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    
    try:
        worksheet_schedule = sheet.worksheet(f"{month_str} 스케줄")
        df_schedule = pd.DataFrame(worksheet_schedule.get_all_records())
    except Exception as e:
        st.error(f"스케줄 시트를 불러오는 데 실패: {e}")
        st.stop()
    
    try:
        worksheet_room_request = sheet.worksheet(f"{month_str} 방배정 요청")
        df_room_request = pd.DataFrame(worksheet_room_request.get_all_records())
        if "우선순위" in df_room_request.columns:
            df_room_request = df_room_request.drop(columns=["우선순위"])
    except:
        worksheet_room_request = sheet.add_worksheet(f"{month_str} 방배정 요청", rows=100, cols=3)
        worksheet_room_request.append_row(["이름", "분류", "날짜정보"])
        df_room_request = pd.DataFrame(columns=["이름", "분류", "날짜정보"])

    # 누적 시트 로드 - 첫 번째 열을 이름으로 처리
    try:
        worksheet_cumulative = sheet.worksheet(f"{month_str} 누적")
        df_cumulative = pd.DataFrame(worksheet_cumulative.get_all_records())
        if df_cumulative.empty:
            st.warning(f"{month_str} 누적 시트가 비어 있습니다. 빈 DataFrame으로 초기화합니다.")
            df_cumulative = pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
        else:
            df_cumulative.rename(columns={f"{month_str}": "이름"}, inplace=True)
    except:
        st.warning(f"{month_str} 누적 시트가 없습니다. 빈 DataFrame으로 초기화합니다.")
        df_cumulative = pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])

    # [추가된 부분] 스케줄 교환 요청 시트 불러오기
    try:
        worksheet_swap_requests = sheet.worksheet(f"{month_str} 스케줄 변경요청")
        df_swap_requests = pd.DataFrame(worksheet_swap_requests.get_all_records())
        st.session_state["df_swap_requests"] = df_swap_requests
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"'{month_str} 스케줄 변경요청' 시트를 찾을 수 없습니다. 빈 테이블로 시작합니다.")
        st.session_state["df_swap_requests"] = pd.DataFrame(columns=[
            "RequestID", "요청일시", "요청자", "요청자 사번", "요청자 기존 근무",
            "상대방", "상대방 기존 근무", "시간대"
        ])

    st.session_state["df_schedule"] = df_schedule
    st.session_state["df_room_request"] = df_room_request
    st.session_state["worksheet_room_request"] = worksheet_room_request
    st.session_state["df_cumulative"] = df_cumulative
    st.session_state["data_loaded"] = True
    
    # 반환 값은 기존과 동일하게 유지
    result = (df_schedule, df_room_request, worksheet_room_request)
    return result

# 근무 가능 일자 계산
@st.cache_data
def get_user_available_dates(name, df_schedule, month_start, month_end):
    available_dates = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    
    personnel_columns = [str(i) for i in range(1, 13)] + [f'오후{i}' for i in range(1, 5)]
    all_personnel = set()
    for col in personnel_columns:
        for val in df_schedule[col].dropna():
            all_personnel.add(str(val).strip())
    if name not in all_personnel:
        st.warning(f"{name}이 df_schedule의 근무자 목록에 없습니다. 데이터 확인 필요: {sorted(all_personnel)}")
    
    for _, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            if "월" in date_str:
                date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            continue
        if month_start <= date_obj <= month_end and row['요일'] not in ['토요일', '일요일']:
            morning_personnel = [str(row[str(i)]).strip() for i in range(1, 13) if pd.notna(row[str(i)]) and row[str(i)]]
            afternoon_personnel = [str(row[f'오후{i}']).strip() for i in range(1, 5) if pd.notna(row[f'오후{i}']) and row[f'오후{i}']]
            display_date = f"{date_obj.month}월 {date_obj.day}일({weekday_map[date_obj.weekday()]})"
            save_date_am = f"{date_obj.strftime('%Y-%m-%d')} (오전)"
            save_date_pm = f"{date_obj.strftime('%Y-%m-%d')} (오후)"
            if name in morning_personnel:
                available_dates.append((date_obj, f"{display_date} 오전", save_date_am))
            if name in afternoon_personnel:
                available_dates.append((date_obj, f"{display_date} 오후", save_date_pm))
    
    available_dates.sort(key=lambda x: x[0])
    sorted_dates = [(display_str, save_str) for _, display_str, save_str in available_dates]
    if not sorted_dates:
        st.warning(f"{name}의 근무 가능 일자가 없습니다. df_schedule 데이터를 확인하세요.")
    return sorted_dates

# 요청 저장 (df_room_request용)
def save_to_gsheet(name, categories, dates, month_str, worksheet):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    worksheet = sheet.worksheet(f"{month_str} 방배정 요청")
    df = pd.DataFrame(worksheet.get_all_records())
    if "우선순위" in df.columns:
        df = df.drop(columns=["우선순위"])
    
    new_rows = []
    for date in dates:
        for cat in categories:
            new_rows.append({"이름": name, "분류": cat, "날짜정보": date})
    
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    update_sheet_with_retry(worksheet, [df.columns.tolist()] + df.values.tolist())
    return df

# df_schedule_md 생성
def create_df_schedule_md(df_schedule):
    df_schedule_md = df_schedule.copy().fillna('')
    for idx, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        oncall_worker = row['오전당직(온콜)']
        
        try:
            if isinstance(date_str, (float, int)):
                date_str = str(int(date_str))
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025) if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            st.error(f"날짜 파싱 오류: {date_str}, 오류: {str(e)}")
            continue
        
        afternoon_cols = ['오후1', '오후2', '오후3', '오후4', '오후5']
        if all(row[col] == '' for col in afternoon_cols):
            df_schedule_md.at[idx, '오전당직(온콜)'] = ''
            continue
        
        if pd.isna(oncall_worker) or oncall_worker == '':
            oncall_worker = ''
            df_schedule_md.at[idx, '오전당직(온콜)'] = ''
        
        if oncall_worker:
            morning_cols = [str(i) for i in range(1, 13)]
            for col in morning_cols + afternoon_cols:
                if row[col] == oncall_worker:
                    df_schedule_md.at[idx, col] = ''
        
        morning_cols = [str(i) for i in range(1, 13)]
        morning_workers = [row[col] for col in morning_cols if row[col]]
        if len(morning_workers) > 11:
            morning_workers = morning_workers[:11]
        morning_workers.extend([''] * (11 - len(morning_workers)))
        for i, col in enumerate([str(i) for i in range(1, 12)], 1):
            df_schedule_md.at[idx, col] = morning_workers[i-1]
        
        afternoon_workers = [row[col] for col in afternoon_cols if row[col]]
        if len(afternoon_workers) > 4:
            afternoon_workers = afternoon_workers[:4]
        afternoon_workers.extend([''] * (4 - len(afternoon_workers)))
        for i, col in enumerate(['오후1', '오후2', '오후3', '오후4'], 1):
            df_schedule_md.at[idx, col] = afternoon_workers[i-1]
    
    df_schedule_md = df_schedule_md.drop(columns=['12', '오후5'], errors='ignore')
    return df_schedule_md

# (기존 apply_schedule_swaps 함수 전체를 아래 코드로 교체)

def apply_schedule_swaps(original_schedule_df, swap_requests_df):
    """스케줄 교환 요청을 적용하고, 변경된 (날짜, 근무타입, 인원)을 기록합니다."""
    
    def parse_swap_date(date_str):
        match = re.search(r'(\d+)월 (\d+)일', date_str)
        return f"{int(match.group(1))}월 {int(match.group(2))}일" if match else None

    df = original_schedule_df.copy()
    applied_requests = 0

    for _, row in swap_requests_df.iterrows():
        from_date_str = parse_swap_date(row['요청자 기존 근무'])
        to_date_str = parse_swap_date(row['상대방 기존 근무'])
        shift_type = row['시간대'] # '오전' 또는 '오후'
        requester = str(row['요청자']).strip()
        to_person = str(row['상대방']).strip()

        if not all([from_date_str, to_date_str, shift_type, requester, to_person]):
            st.warning(f"정보가 부족하여 교환 요청을 건너뜁니다: RequestID {row.get('RequestID', 'N/A')}")
            continue

        cols_to_search = [str(i) for i in range(1, 12)] if shift_type == '오전' else [f'오후{i}' for i in range(1, 5)]
        from_row = df[df['날짜'] == from_date_str]
        to_row = df[df['날짜'] == to_date_str]

        if from_row.empty or to_row.empty:
            continue

        from_row_idx, to_row_idx = from_row.index[0], to_row.index[0]
        from_col = next((col for col in cols_to_search if df.at[from_row_idx, col] == requester), None)
        to_col = next((col for col in cols_to_search if df.at[to_row_idx, col] == to_person), None)
        
        if from_col and to_col:
            df.at[from_row_idx, from_col] = to_person
            df.at[to_row_idx, to_col] = requester
            
            # [수정] (날짜, 근무타입, 인원) 쌍으로 정확히 기록
            st.session_state["swapped_assignments"].add((from_date_str, shift_type, to_person))
            st.session_state["swapped_assignments"].add((to_date_str, shift_type, requester))

            applied_requests += 1
        else:
            st.error(f"적용 실패: {from_date_str}의 '{requester}' 또는 {to_date_str}의 '{to_person}'을 스케줄에서 찾을 수 없습니다.")

    if applied_requests > 0:
        st.success(f"총 {applied_requests}건의 스케줄 교환이 성공적으로 반영되었습니다.")
    else:
        st.info("새롭게 적용할 스케줄 교환이 없습니다.")
        
    return df

# 메인
month_str = "2025년 04월"
next_month_start = date(2025, 4, 1)
next_month_end = date(2025, 4, 30)

# 데이터 로드 호출
df_schedule, df_room_request, worksheet_room_request = load_data_page6(month_str)
st.session_state["df_room_request"] = df_room_request
st.session_state["worksheet_room_request"] = worksheet_room_request

# df_schedule_md 초기화
if "df_schedule_md" not in st.session_state:
    st.session_state["df_schedule_md"] = create_df_schedule_md(df_schedule)

st.header("🚪 방 배정", divider='rainbow')

# 새로고침 버튼
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    df_schedule, df_room_request, worksheet_room_request = load_data_page6_no_cache(month_str)
    st.session_state["df_schedule"] = df_schedule
    st.session_state["df_room_request"] = df_room_request
    st.session_state["worksheet_room_request"] = worksheet_room_request
    st.session_state["df_schedule_md"] = create_df_schedule_md(df_schedule)
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 근무자 명단 수정
st.write(" ")
st.subheader("📝 근무자 명단 수정")
st.write(" ")
st.write("**📋 스케줄 변경 요청 목록**")
st.write("- 아래 변경 요청 목록을 확인하고, 스케줄을 수정 후 저장하세요.")
df_swaps_raw = st.session_state.get("df_swap_requests", pd.DataFrame())
if not df_swaps_raw.empty:
    cols_to_display = {'요청일시': '요청일시', '요청자': '요청자', '요청자 기존 근무': '요청자 기존 근무', '상대방': '상대방', '상대방 기존 근무': '상대방 기존 근무'}
    existing_cols = [col for col in cols_to_display.keys() if col in df_swaps_raw.columns]
    df_swaps_display = df_swaps_raw[existing_cols].rename(columns=cols_to_display)
    st.dataframe(df_swaps_display, use_container_width=True, hide_index=True)
else:
    st.info("표시할 교환 요청 데이터가 없습니다.")

st.write(" ")
st.write("**✍️ 스케줄 수정**")
st.write("- 요청사항을 일괄 적용하거나, 셀을 더블클릭하여 직접 수정한 후 **최종 저장 버튼**을 누르세요.")
if st.button("🔄 요청사항 일괄 적용"):
    df_swaps = st.session_state.get("df_swap_requests", pd.DataFrame())
    if not df_swaps.empty:
        modified_schedule = apply_schedule_swaps(st.session_state["df_schedule"], df_swaps)
        st.session_state.update({"df_schedule": modified_schedule, "df_schedule_md": create_df_schedule_md(modified_schedule)})
        st.info("교환 요청이 적용되었습니다. 아래 표에서 결과를 확인하고 직접 수정할 수 있습니다.")
        time.sleep(1); st.rerun()
    else:
        st.info("처리할 교환 요청이 없습니다.")
edited_df_md = st.data_editor(st.session_state["df_schedule_md"], use_container_width=True, key="schedule_editor", disabled=['날짜', '요일'])
st.write(" ")
if st.button("✍️ 최종 변경사항 Google Sheets에 저장", type="primary", use_container_width=True):
    df_schedule_to_save = st.session_state["df_schedule"].copy()
    if not st.session_state["df_schedule_md"].equals(edited_df_md):
        st.info("수작업 변경사항을 최종본에 반영합니다...")
        for md_idx, edited_row in edited_df_md.iterrows():
            original_row = st.session_state["df_schedule_md"].loc[md_idx]
            if not original_row.equals(edited_row):
                date_str = edited_row['날짜']
                for col_name, new_value in edited_row.items():
                    if original_row[col_name] != new_value and new_value and isinstance(new_value, str) and new_value.strip():
                        # [핵심 수정] 수작업 시에도 근무타입을 판별하여 (날짜, 근무타입, 이름)의 3개짜리 데이터로 기록
                        shift_type = '오후' if '오후' in col_name or '13:30' in col_name else '오전'
                        st.session_state["swapped_assignments"].add((date_str, shift_type, new_value))
                target_row_indices = df_schedule_to_save[df_schedule_to_save['날짜'] == date_str].index
                if not target_row_indices.empty:
                    target_idx = target_row_indices[0]
                    for col_name in edited_df_md.columns:
                        if col_name in df_schedule_to_save.columns:
                            df_schedule_to_save.loc[target_idx, col_name] = edited_row[col_name]
    try:
        st.info("최종 스케줄을 Google Sheets에 저장합니다...")
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet_schedule = sheet.worksheet(f"{month_str} 스케줄")
        schedule_data = [df_schedule_to_save.columns.tolist()] + df_schedule_to_save.fillna('').values.tolist()
        update_sheet_with_retry(worksheet_schedule, schedule_data)
        st.session_state.update({"df_schedule": df_schedule_to_save, "df_schedule_md": create_df_schedule_md(df_schedule_to_save)})
        st.success("✅ 최종 스케줄이 Google Sheets에 성공적으로 저장되었습니다."); time.sleep(1); st.rerun()
    except Exception as e:
        st.error(f"Google Sheets 저장 중 오류 발생: {e}")

st.write("---")
st.caption("📝 현재까지 기록된 변경사항 로그")
change_log_set = st.session_state.get("swapped_assignments", set())
if change_log_set:
    # [핵심 수정] 이제 모든 데이터가 3개짜리이므로 에러 없이 표 생성 가능
    log_df = pd.DataFrame(list(change_log_set), columns=['날짜', '근무타입', '해당 날짜에 변경된 인원'])
    log_df = log_df.sort_values(by=['날짜', '근무타입', '해당 날짜에 변경된 인원']).reset_index(drop=True)
    st.dataframe(log_df, use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")

# 방 설정 UI
st.divider()
st.subheader("📋 방 설정")
st.write("시간대별 탭을 클릭하여 운영할 방의 개수와 번호를 설정하세요.")
room_options = [str(i) for i in range(1, 13)]

tab830, tab900, tab930, tab1000, tab1330 = st.tabs([
    "🕗 08:30", "🕘 09:00", "🕤 09:30", "🕙 10:00", "🕜 13:30 (오후)"
])
with tab830:
    # ... (기존 방 설정 UI 코드는 모두 동일) ...
    col1, col2 = st.columns([1, 2.5])
    with col1:
        st.markdown("###### **방 개수**")
        num_830 = st.number_input("830_rooms_count", min_value=0, max_value=12, value=4, key="830_rooms", label_visibility="collapsed")
        st.markdown("###### **오전 당직방**")
        duty_830_options = st.session_state["room_settings"]["830_room_select"]
        try:
            duty_index_830 = duty_830_options.index(st.session_state["room_settings"].get("830_duty"))
        except ValueError:
            duty_index_830 = 0
        duty_830 = st.selectbox("830_duty_room", duty_830_options, index=duty_index_830, key="830_duty", label_visibility="collapsed", help="8:30 시간대의 당직 방을 선택합니다.")
        st.session_state["room_settings"]["830_duty"] = duty_830
    with col2:
        st.markdown("###### **방 번호 선택**")
        if len(st.session_state["room_settings"]["830_room_select"]) > num_830:
            st.session_state["room_settings"]["830_room_select"] = st.session_state["room_settings"]["830_room_select"][:num_830]
        rooms_830 = st.multiselect("830_room_select_numbers", room_options, default=st.session_state["room_settings"]["830_room_select"], max_selections=num_830, key="830_room_select", label_visibility="collapsed")
        if len(rooms_830) < num_830:
            st.warning(f"방 번호를 {num_830}개 선택해주세요.")
        st.session_state["room_settings"]["830_room_select"] = rooms_830
# ... (다른 시간대 탭 UI도 모두 동일하게 유지) ...
with tab900:
    col1, col2 = st.columns([1, 2.5])
    with col1:
        st.markdown("###### **방 개수**")
        num_900 = st.number_input("900_rooms_count", min_value=0, max_value=12, value=3, key="900_rooms", label_visibility="collapsed")
    with col2:
        st.markdown("###### **방 번호 선택**")
        if len(st.session_state["room_settings"]["900_room_select"]) > num_900:
            st.session_state["room_settings"]["900_room_select"] = st.session_state["room_settings"]["900_room_select"][:num_900]
        rooms_900 = st.multiselect("900_room_select_numbers", room_options, default=st.session_state["room_settings"]["900_room_select"], max_selections=num_900, key="900_room_select", label_visibility="collapsed")
        if len(rooms_900) < num_900:
            st.warning(f"방 번호를 {num_900}개 선택해주세요.")
        st.session_state["room_settings"]["900_room_select"] = rooms_900
with tab930:
    col1, col2 = st.columns([1, 2.5])
    with col1:
        st.markdown("###### **방 개수**")
        num_930 = st.number_input("930_rooms_count", min_value=0, max_value=12, value=3, key="930_rooms", label_visibility="collapsed")
    with col2:
        st.markdown("###### **방 번호 선택**")
        if len(st.session_state["room_settings"]["930_room_select"]) > num_930:
            st.session_state["room_settings"]["930_room_select"] = st.session_state["room_settings"]["930_room_select"][:num_930]
        rooms_930 = st.multiselect("930_room_select_numbers", room_options, default=st.session_state["room_settings"]["930_room_select"], max_selections=num_930, key="930_room_select", label_visibility="collapsed")
        if len(rooms_930) < num_930:
            st.warning(f"방 번호를 {num_930}개 선택해주세요.")
        st.session_state["room_settings"]["930_room_select"] = rooms_930
with tab1000:
    col1, col2 = st.columns([1, 2.5])
    with col1:
        st.markdown("###### **방 개수**")
        num_1000 = st.number_input("1000_rooms_count", min_value=0, max_value=12, value=2, key="1000_rooms", label_visibility="collapsed")
    with col2:
        st.markdown("###### **방 번호 선택**")
        if len(st.session_state["room_settings"]["1000_room_select"]) > num_1000:
            st.session_state["room_settings"]["1000_room_select"] = st.session_state["room_settings"]["1000_room_select"][:num_1000]
        rooms_1000 = st.multiselect("1000_room_select_numbers", room_options, default=st.session_state["room_settings"]["1000_room_select"], max_selections=num_1000, key="1000_room_select", label_visibility="collapsed")
        if len(rooms_1000) < num_1000:
            st.warning(f"방 번호를 {num_1000}개 선택해주세요.")
        st.session_state["room_settings"]["1000_room_select"] = rooms_1000
with tab1330:
    col1, col2 = st.columns([1, 2.5])
    with col1:
        st.markdown("###### **방 개수**")
        st.info("4개 고정")
        num_1330 = 4
        st.markdown("###### **오후 당직방**")
        duty_1330_options = st.session_state["room_settings"]["1330_room_select"]
        try:
            duty_index_1330 = duty_1330_options.index(st.session_state["room_settings"].get("1330_duty"))
        except ValueError:
            duty_index_1330 = 0
        duty_1330 = st.selectbox("1330_duty_room", duty_1330_options, index=duty_index_1330, key="1330_duty", label_visibility="collapsed", help="13:30 시간대의 당직 방을 선택합니다.")
        st.session_state["room_settings"]["1330_duty"] = duty_1330
    with col2:
        st.markdown("###### **방 번호 선택**")
        if len(st.session_state["room_settings"]["1330_room_select"]) > num_1330:
            st.session_state["room_settings"]["1330_room_select"] = st.session_state["room_settings"]["1330_room_select"][:num_1330]
        rooms_1330 = st.multiselect("1330_room_select_numbers", room_options, default=st.session_state["room_settings"]["1330_room_select"], max_selections=num_1330, key="1330_room_select", label_visibility="collapsed")
        if len(rooms_1330) < num_1330:
            st.warning(f"방 번호를 {num_1330}개 선택해주세요.")
        st.session_state["room_settings"]["1330_room_select"] = rooms_1330
all_selected_rooms = (st.session_state["room_settings"]["830_room_select"] + st.session_state["room_settings"]["900_room_select"] + st.session_state["room_settings"]["930_room_select"] + st.session_state["room_settings"]["1000_room_select"] + st.session_state["room_settings"]["1330_room_select"])

# 배정 요청 입력 UI
st.divider()
st.subheader("📋 배정 요청 관리")
# ... (배정 요청 UI 코드는 모두 동일하게 유지) ...
st.write("- 모든 인원의 배정 요청(고정 및 우선)을 추가 및 수정할 수 있습니다.")
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방", "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]
st.write(" ")
st.markdown("**🟢 방 배정 요청 추가**")
col1, col2, col3, col_button_add = st.columns([2.5, 2.5, 3.5, 1])
with col1:
    names = sorted([str(name).strip() for name in df_schedule.iloc[:, 2:].stack().dropna().unique() if str(name).strip()])
    name = st.selectbox("근무자", names, key="request_employee_select", index=None, placeholder="근무자 선택")
with col2:
    categories = st.multiselect("요청 분류", 요청분류, key="request_category_select")
with col3:
    selected_save_dates = []
    if name:
        st.cache_data.clear()
        available_dates = get_user_available_dates(name, df_schedule, next_month_start, next_month_end)
        date_options = [display_str for display_str, _ in available_dates]
        dates = st.multiselect("요청 일자", date_options, key="request_date_select")
        selected_save_dates = [save_str for display_str, save_str in available_dates if display_str in dates]
    else:
        dates = st.multiselect("요청 일자", [], key="request_date_select", disabled=True)
with col_button_add:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    add_button_clicked = st.button("📅 추가", key="request_add_button")
if add_button_clicked:
    if not name:
        st.error("근무자를 먼저 선택해주세요.")
    elif not categories or not selected_save_dates:
        st.error("요청 분류와 날짜를 선택해주세요.")
    else:
        new_rows = []
        for date in selected_save_dates:
            for cat in categories:
                new_rows.append({"이름": name, "분류": cat, "날짜정보": date})
        df_room_request = pd.concat([st.session_state["df_room_request"], pd.DataFrame(new_rows)], ignore_index=True)
        st.session_state["df_room_request"] = df_room_request
        try:
            update_sheet_with_retry(st.session_state["worksheet_room_request"], [df_room_request.columns.tolist()] + df_room_request.values.tolist())
            st.cache_data.clear()
            st.success("방 배정 요청 저장 완료!")
        except Exception as e:
            st.error(f"Google Sheets 업데이트 실패: {str(e)}")
st.write(" ")
st.markdown("**🔴 방 배정 요청 삭제**")
if not st.session_state["df_room_request"].empty:
    col0, col1, col_button_del = st.columns([2.5, 4.5, 1])
    with col0:
        unique_names = st.session_state["df_room_request"]["이름"].unique()
        selected_employee = st.selectbox("근무자 선택", unique_names, key="delete_request_employee_select", index=None, placeholder="근무자 선택")
    with col1:
        selected_items = []
        if selected_employee:
            df_request_filtered = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == selected_employee]
            if not df_request_filtered.empty:
                options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_request_filtered.iterrows()]
                selected_items = st.multiselect("삭제할 항목", options, key="delete_request_select")
            else:
                st.multiselect("삭제할 항목", [], disabled=True, key="delete_request_select", help="해당 근무자의 요청이 없습니다.")
        else:
            st.multiselect("삭제할 항목", [], key="delete_request_select", disabled=True)
    with col_button_del:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        delete_button_clicked = st.button("📅 삭제", key="request_delete_button")
    if delete_button_clicked:
        if not selected_employee or not selected_items:
            st.error("삭제할 근무자와 항목을 선택해주세요.")
        else:
            indices = []
            for item in selected_items:
                for idx, row in st.session_state["df_room_request"].iterrows():
                    if row['이름'] == selected_employee and f"{row['분류']} - {row['날짜정보']}" == item:
                        indices.append(idx)
            df_room_request = st.session_state["df_room_request"].drop(indices).reset_index(drop=True)
            st.session_state["df_room_request"] = df_room_request
            try:
                update_sheet_with_retry(st.session_state["worksheet_room_request"], [df_room_request.columns.tolist()] + df_room_request.values.tolist())
                st.cache_data.clear()
                st.success("선택한 방 배정 요청 삭제 완료!")
                st.rerun()
            except Exception as e:
                st.error(f"Google Sheets 업데이트 실패: {str(e)}")
else:
    st.info("📍 방 배정 요청이 없습니다.")
st.write(" ")
st.markdown("**🙋‍♂️ 현재 방 배정 요청 목록**")
if st.session_state["df_room_request"].empty:
    st.info("☑️ 현재 방 배정 요청이 없습니다.")
else:
    st.dataframe(st.session_state["df_room_request"], use_container_width=True, hide_index=True)
    
# 날짜정보 파싱 함수
def parse_date_info(date_info):
    try:
        date_part = date_info.split('(')[0].strip()
        date_obj = datetime.strptime(date_part, '%Y-%m-%d')
        is_morning = '오전' in date_info
        parsed_date = date_obj.strftime('%Y-%m-%d')
        return parsed_date, is_morning
    except ValueError as e:
        st.warning(f"Failed to parse date_info: {date_info}, error: {str(e)}")
        return None, False

# random_assign 함수 - 오전/오후 당직 분리
def random_assign(personnel, slots, request_assignments, time_groups, total_stats, morning_personnel, afternoon_personnel, afternoon_duty_counts):
    assignment = [None] * len(slots)
    assigned_personnel_morning = set()  # 오전 시간대 배정된 인원 추적
    assigned_personnel_afternoon = set()  # 오후 시간대 배정된 인원 추적
    daily_stats = {
        'early': Counter(),
        'late': Counter(),
        'morning_duty': Counter(),  # 오전 당직 (8:30)
        'afternoon_duty': Counter(),  # 오후 당직 (13:30)
        'rooms': {str(i): Counter() for i in range(1, 13)}
    }

    # 슬롯 분류
    morning_slots = [s for s in slots if s.startswith(('8:30', '9:00', '9:30', '10:00')) and '_당직' not in s]
    afternoon_slots = [s for s in slots if s.startswith('13:30')]
    afternoon_duty_slot = '13:30(2)_당직'  # 오후당직 슬롯

    # 1. 배정 요청 먼저 처리 (중복 배정 방지, 균등 배정 고려)
    for slot, person in request_assignments.items():
        if person in personnel and slot in slots:
            slot_idx = slots.index(slot)
            if assignment[slot_idx] is None:
                # 시간대 제약 확인
                if (slot in morning_slots and person in morning_personnel) or \
                   (slot in afternoon_slots and person in afternoon_personnel):
                    # 오전/오후 중복 체크
                    if slot in morning_slots and person in assigned_personnel_morning:
                        st.warning(f"중복 배정 방지: {person}은 이미 오전 시간대({slot})에 배정됨")
                        continue
                    if slot in afternoon_slots and person in assigned_personnel_afternoon:
                        st.warning(f"중복 배정 방지: {person}은 이미 오후 시간대({slot})에 배정됨")
                        continue

                    assignment[slot_idx] = person
                    if slot in morning_slots:
                        assigned_personnel_morning.add(person)
                    else:
                        assigned_personnel_afternoon.add(person)
                    room_num = slot.split('(')[1].split(')')[0]
                    daily_stats['rooms'][room_num][person] += 1
                    if slot.startswith('8:30') and '_당직' not in slot:
                        daily_stats['early'][person] += 1
                    elif slot.startswith('10:00'):
                        daily_stats['late'][person] += 1
                    if slot.startswith('8:30') and slot.endswith('_당직'):
                        daily_stats['morning_duty'][person] += 1
                    elif slot.startswith('13:30') and slot.endswith('_당직'):
                        daily_stats['afternoon_duty'][person] += 1
                else:
                    st.warning(f"배정 요청 무시: {person}은 {slot} 시간대({'오전' if slot in morning_slots else '오후'})에 근무 불가")
            else:
                st.warning(f"배정 요청 충돌: {person}을 {slot}에 배정할 수 없음. 이미 배정됨: {assignment[slot_idx]}")

    # 2. 오후당직 우선 배정 (누적 시트 기반, 당직 균등 배정)
    afternoon_duty_slot_idx = slots.index(afternoon_duty_slot) if afternoon_duty_slot in slots else None
    if afternoon_duty_slot_idx is not None and assignment[afternoon_duty_slot_idx] is None:
        # 오후당직 배정 가능한 인원: afternoon_personnel 중 아직 오후에 배정되지 않은 인원
        available_personnel = [p for p in afternoon_personnel if p not in assigned_personnel_afternoon]
        # 오후당직 횟수가 있는 인원만 대상으로
        candidates = [p for p in available_personnel if p in afternoon_duty_counts and afternoon_duty_counts[p] > 0]
        
        if candidates:
            # 오후 당직 횟수 기준 균등 배정
            best_person = None
            min_duty_count = float('inf')
            for person in candidates:
                duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person]
                if duty_count < min_duty_count:
                    min_duty_count = duty_count
                    best_person = person
            if best_person:
                assignment[afternoon_duty_slot_idx] = best_person
                assigned_personnel_afternoon.add(best_person)
                room_num = afternoon_duty_slot.split('(')[1].split(')')[0]
                daily_stats['rooms'][room_num][best_person] += 1
                daily_stats['afternoon_duty'][best_person] += 1
                # 오후당직 횟수 감소
                afternoon_duty_counts[best_person] -= 1
                if afternoon_duty_counts[best_person] <= 0:
                    del afternoon_duty_counts[best_person]

    # 3. 남은 인원 배정 (오전/오후 구분, 공란 방지, 독립적 균등 배정)
    morning_remaining = [p for p in morning_personnel if p not in assigned_personnel_morning]
    afternoon_remaining = [p for p in afternoon_personnel if p not in assigned_personnel_afternoon]
    remaining_slots = [i for i, a in enumerate(assignment) if a is None]
    
    # 오전 슬롯 배정
    morning_slot_indices = [i for i in remaining_slots if slots[i] in morning_slots] 
    while morning_remaining and morning_slot_indices: 
        best_person = None 
        best_slot_idx = None 
        min_score = float('inf')

        # ### 수정된 부분 1: shuffle 위치를 while 루프 안으로 이동 ###
        # 매번 새로운 최적의 조합을 찾기 전에 순서를 섞어 공정성을 높입니다.
        random.shuffle(morning_remaining)
        
        for slot_idx in morning_slot_indices: 
            if assignment[slot_idx] is not None: 
                continue 
            slot = slots[slot_idx] 
            room_num = slot.split('(')[1].split(')')[0] 
            
            for person in morning_remaining:
                
                # ### 수정된 부분 2: 슬롯 중요도에 따른 가중치 점수 체계 도입 ###
                if slot.startswith('8:30') and '_당직' not in slot: 
                    early_count = total_stats['early'][person] + daily_stats['early'][person]
                    score = early_count  # 기준 점수 (가장 낮음)
                
                elif slot.startswith('10:00'): 
                    late_count = total_stats['late'][person] + daily_stats['late'][person]
                    score = 10000 + late_count # 늦은방은 10000점대
                
                else: # 9:00, 9:30 등 일반방
                    room_count = total_stats['rooms'][room_num][person] + daily_stats['rooms'][room_num][person]
                    score = 20000 + room_count # 일반방은 20000점대
                
                if score < min_score: 
                    min_score = score 
                    best_person = person 
                    best_slot_idx = slot_idx 
        
        if best_slot_idx is None or best_person is None: 
            st.warning(f"오전 슬롯 배정 불가: 더 이상 배정 가능한 인원 없음") 
            break 
        
        # 이하 배정 로직은 기존과 동일
        slot = slots[best_slot_idx] 
        assignment[best_slot_idx] = best_person 
        assigned_personnel_morning.add(best_person) 
        morning_remaining.remove(best_person) 
        morning_slot_indices.remove(best_slot_idx) 
        remaining_slots.remove(best_slot_idx) 
        room_num = slot.split('(')[1].split(')')[0] 
        daily_stats['rooms'][room_num][best_person] += 1 
        if slot.startswith('8:30') and '_당직' not in slot: 
            daily_stats['early'][best_person] += 1 
        elif slot.startswith('10:00'): 
            daily_stats['late'][best_person] += 1 
        if slot.startswith('8:30') and slot.endswith('_당직'): 
            daily_stats['morning_duty'][best_person] += 1
            
    # 오후 슬롯 배정
    afternoon_slot_indices = [i for i in remaining_slots if slots[i] in afternoon_slots]
    while afternoon_remaining and afternoon_slot_indices:
        best_person = None
        best_slot_idx = None
        min_score = float('inf')
        
        for slot_idx in afternoon_slot_indices:
            if assignment[slot_idx] is not None:
                continue
            slot = slots[slot_idx]
            room_num = slot.split('(')[1].split(')')[0]
            
            for person in afternoon_remaining:
                # 방별 배정 균등성
                room_count = total_stats['rooms'][room_num][person] + daily_stats['rooms'][room_num][person]
                # 당직 배정 균등성
                duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person] if slot.endswith('_당직') else float('inf')
                early_count = total_stats['early'][person]
                late_count = total_stats['late'][person]

                # 해당 슬롯 유형에 따라 스코어 선택
                if slot.endswith('_당직'):
                    score = duty_count  # 당직 슬롯은 오후 당직 횟수만 고려
                elif slot.startswith('8:30') and '_당직' not in slot:
                    score = (early_count, room_count)
                elif slot.startswith('10:00'):
                    score = (late_count, room_count)
                else:
                    score = room_count  # 나머지 슬롯은 방별 횟수만 고려
                
                if score < min_score:
                    min_score = score
                    best_person = person
                    best_slot_idx = slot_idx
        
        if best_slot_idx is None or best_person is None:
            st.warning(f"오후 슬롯 배정 불가: 더 이상 배정 가능한 인원 없음")
            break
        
        slot = slots[best_slot_idx]
        assignment[best_slot_idx] = best_person
        assigned_personnel_afternoon.add(best_person)
        afternoon_remaining.remove(best_person)
        afternoon_slot_indices.remove(best_slot_idx)
        room_num = slot.split('(')[1].split(')')[0]
        daily_stats['rooms'][room_num][best_person] += 1
        if slot.endswith('_당직'):
            daily_stats['afternoon_duty'][best_person] += 1

    # 모든 슬롯 채우기 (공란 방지, 독립적 균등 배정 고려)
    for slot_idx in range(len(slots)):
        if assignment[slot_idx] is None:
            slot = slots[slot_idx]
            # 오전/오후 인원 중 가능한 인원 선택
            available_personnel = morning_personnel if slot in morning_slots else afternoon_personnel
            assigned_set = assigned_personnel_morning if slot in morning_slots else assigned_personnel_afternoon
            candidates = [p for p in available_personnel if p not in assigned_set]
            
            if candidates:
                room_num = slot.split('(')[1].split(')')[0]
                best_person = None
                min_score = float('inf')
                for person in candidates:
                    early_count = total_stats['early'][person] + daily_stats['early'][person] if slot.startswith('8:30') and '_당직' not in slot else float('inf')
                    late_count = total_stats['late'][person] + daily_stats['late'][person] if slot.startswith('10:00') else float('inf')
                    morning_duty_count = total_stats['morning_duty'][person] + daily_stats['morning_duty'][person] if slot.startswith('8:30') and slot.endswith('_당직') else float('inf')
                    afternoon_duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person] if slot.startswith('13:30') and slot.endswith('_당직') else float('inf')
                    room_count = total_stats['rooms'][room_num][person] + daily_stats['rooms'][room_num][person]
                    
                    if slot.startswith('8:30') and '_당직' not in slot:
                        score = early_count
                    elif slot.startswith('10:00'):
                        score = late_count
                    elif slot.startswith('8:30') and slot.endswith('_당직'):
                        score = morning_duty_count
                    elif slot.startswith('13:30') and slot.endswith('_당직'):
                        score = afternoon_duty_count
                    else:
                        score = room_count
                    
                    if score < min_score:
                        min_score = score
                        best_person = person
                
                person = best_person
                if slot in morning_slots:
                    assigned_personnel_morning.add(person)
                else:
                    assigned_personnel_afternoon.add(person)
                st.warning(f"슬롯 {slot} 공란 방지: {person} 배정 (스코어: {min_score})")
            else:
                # 이미 배정된 인원 중에서 스코어 최소인 인원 선택
                available_personnel = morning_personnel if slot in morning_slots else afternoon_personnel
                if available_personnel:
                    room_num = slot.split('(')[1].split(')')[0]
                    best_person = None
                    min_score = float('inf')
                    for person in available_personnel:
                        early_count = total_stats['early'][person] + daily_stats['early'][person] if slot.startswith('8:30') and '_당직' not in slot else float('inf')
                        late_count = total_stats['late'][person] + daily_stats['late'][person] if slot.startswith('10:00') else float('inf')
                        morning_duty_count = total_stats['morning_duty'][person] + daily_stats['morning_duty'][person] if slot.startswith('8:30') and slot.endswith('_당직') else float('inf')
                        afternoon_duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person] if slot.startswith('13:30') and slot.endswith('_당직') else float('inf')
                        room_count = total_stats['rooms'][room_num][person] + daily_stats['rooms'][room_num][person]
                        
                        if slot.startswith('8:30') and '_당직' not in slot:
                            score = early_count
                        elif slot.startswith('10:00'):
                            score = late_count
                        elif slot.startswith('8:30') and slot.endswith('_당직'):
                            score = morning_duty_count
                        elif slot.startswith('13:30') and slot.endswith('_당직'):
                            score = afternoon_duty_count
                        else:
                            score = room_count
                        
                        if score < min_score:
                            min_score = score
                            best_person = person
                    
                    person = best_person
                    st.warning(f"슬롯 {slot} 공란 방지: 이미 배정된 {person} 재배정 (스코어: {min_score})")
                else:
                    st.warning(f"슬롯 {slot} 공란 방지 불가: 배정 가능한 인원 없음")
                    continue
            
            assignment[slot_idx] = person
            daily_stats['rooms'][room_num][person] += 1
            if slot.startswith('8:30') and '_당직' not in slot:
                daily_stats['early'][person] += 1
            elif slot.startswith('10:00'):
                daily_stats['late'][person] += 1
            if slot.startswith('8:30') and slot.endswith('_당직'):
                daily_stats['morning_duty'][person] += 1
            elif slot.startswith('13:30') and slot.endswith('_당직'):
                daily_stats['afternoon_duty'][person] += 1

    # 통계 업데이트
    for key in ['early', 'late', 'morning_duty', 'afternoon_duty']:
        total_stats[key].update(daily_stats[key])
    for room in daily_stats['rooms']:
        total_stats['rooms'][room].update(daily_stats['rooms'][room])

    return assignment, daily_stats

if st.button("🚀 방배정 수행", type="primary", use_container_width=True):
    st.write(" ")
    st.subheader(f"💡 {month_str} 방배정 결과", divider='rainbow')
    
    # --- 방 설정 검증 및 슬롯 정보 생성 (기존과 동일) ---
    time_slots, time_groups, memo_rules = {}, {}, {}
    if num_830 + num_900 + num_930 + num_1000 != 12:
        st.error(f"오전 방 개수 합계는 12개여야 합니다. (온콜 제외) 현재: {num_830 + num_900 + num_930 + num_1000}개")
        st.stop()
    elif len(rooms_830) != num_830 or len(rooms_900) != num_900 or len(rooms_930) != num_930 or len(rooms_1000) != num_1000 or len(rooms_1330) != num_1330:
        st.error("각 시간대의 방 번호 선택을 완료해주세요.")
        st.stop()
    else:
        for room in rooms_830:
            slot = f"8:30({room})_당직" if room == duty_830 else f"8:30({room})"
            time_slots[slot] = len(time_slots)
            time_groups.setdefault('8:30', []).append(slot)
        for room in rooms_900:
            slot = f"9:00({room})"
            time_slots[slot] = len(time_slots)
            time_groups.setdefault('9:00', []).append(slot)
        for room in rooms_930:
            slot = f"9:30({room})"
            time_slots[slot] = len(time_slots)
            time_groups.setdefault('9:30', []).append(slot)
        for room in rooms_1000:
            slot = f"10:00({room})"
            time_slots[slot] = len(time_slots)
            time_groups.setdefault('10:00', []).append(slot)
        for room in rooms_1330:
            slot = f"13:30({room})_당직" if room == duty_1330 else f"13:30({room})"
            time_slots[slot] = len(time_slots)
            time_groups.setdefault('13:30', []).append(slot)
        
        memo_rules = {
            **{f'{i}번방': [s for s in time_slots if f'({i})' in s and '_당직' not in s] for i in range(1, 13)},
            '당직 아닌 이른방': [s for s in time_slots if s.startswith('8:30') and '_당직' not in s],
            '이른방 제외': [s for s in time_slots if s.startswith(('9:00', '9:30', '10:00'))],
            '늦은방 제외': [s for s in time_slots if s.startswith(('8:30', '9:00', '9:30'))],
            '8:30': [s for s in time_slots if s.startswith('8:30') and '_당직' not in s],
            '9:00': [s for s in time_slots if s.startswith('9:00')],
            '9:30': [s for s in time_slots if s.startswith('9:30')],
            '10:00': [s for s in time_slots if s.startswith('10:00')],
            '오후 당직 제외': [s for s in time_slots if s.startswith('13:30') and '_당직' not in s]
        }
        
        st.session_state.update({"time_slots": time_slots, "time_groups": time_groups, "memo_rules": memo_rules})
    
    morning_duty_slot = f"8:30({duty_830})_당직"
    all_slots = [morning_duty_slot] + sorted([s for s in time_slots if s.startswith('8:30') and not s.endswith('_당직')]) + sorted([s for s in time_slots if s.startswith('9:00')]) + sorted([s for s in time_slots if s.startswith('9:30')]) + sorted([s for s in time_slots if s.startswith('10:00')]) + ['온콜'] + sorted([s for s in time_slots if s.startswith('13:30') and s.endswith('_당직')]) + sorted([s for s in time_slots if s.startswith('13:30') and not s.endswith('_당직')])
    columns = ['날짜', '요일'] + all_slots
    
    # --- 배정 로직 (기존과 동일) ---
    # random.seed(time.time())
    total_stats = {'early': Counter(),'late': Counter(),'morning_duty': Counter(),'afternoon_duty': Counter(),'rooms': {str(i): Counter() for i in range(1, 13)}}
    df_cumulative = st.session_state["df_cumulative"]
    afternoon_duty_counts = {row['이름']: int(row['오후당직']) for _, row in df_cumulative.iterrows() if pd.notna(row.get('오후당직')) and int(row['오후당직']) > 0}
    
    assignments, date_cache, request_cells, result_data = {}, {}, {}, []
    assignable_slots = [s for s in st.session_state["time_slots"].keys() if not (s.startswith('8:30') and s.endswith('_당직'))]
    weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
    
    for _, row in st.session_state["df_schedule_md"].iterrows():
        date_str = row['날짜']
        try:
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025) if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%Y-%m-%d').strip()
            date_cache[date_str] = formatted_date
            day_of_week = weekday_map[date_obj.weekday()]
        except (ValueError, TypeError):
            continue
        
        result_row = [date_str, day_of_week]
        has_person = any(val for val in row.iloc[2:-1] if pd.notna(val) and val)

        personnel_for_the_day = [p for p in row.iloc[2:].dropna() if p]

        # 2. '소수 인원 근무'로 판단할 기준 인원수를 설정합니다. (이 값을 조절하여 기준 변경 가능)
        SMALL_TEAM_THRESHOLD = 15

        # 3. 근무 인원수가 설정된 기준보다 적으면, 방 배정 없이 순서대로 나열합니다.
        if len(personnel_for_the_day) < SMALL_TEAM_THRESHOLD and has_person:
            
            result_row.append(None)
            
            result_row.extend(personnel_for_the_day)

            num_slots_to_fill = len(all_slots)
            slots_filled_count = len(personnel_for_the_day) + 1 # 근무자 수 + 비워둔 1칸
            padding_needed = num_slots_to_fill - slots_filled_count
            if padding_needed > 0:
                result_row.extend([None] * padding_needed)

            result_data.append(result_row)
            continue
        
        morning_personnel = [row[str(i)] for i in range(1, 12) if pd.notna(row[str(i)]) and row[str(i)]]
        afternoon_personnel = [row[f'오후{i}'] for i in range(1, 5) if pd.notna(row[f'오후{i}']) and row[f'오후{i}']]
        
        if not (morning_personnel or afternoon_personnel):
            result_row.extend([None] * len(all_slots))
            result_data.append(result_row)
            continue
        
        request_assignments = {}
        if not st.session_state["df_room_request"].empty:
            for _, req in st.session_state["df_room_request"].iterrows():
                req_date, is_morning = parse_date_info(req['날짜정보'])
                if req_date and req_date == formatted_date:
                    slots_for_category = st.session_state["memo_rules"].get(req['분류'], [])
                    if slots_for_category:
                        valid_slots = [s for s in slots_for_category if (is_morning and not s.startswith('13:30')) or (not is_morning and s.startswith('13:30'))]
                        if valid_slots:
                            selected_slot = random.choice(valid_slots)
                            request_assignments[selected_slot] = req['이름']
                            request_cells[(formatted_date, selected_slot)] = {'이름': req['이름'], '분류': req['분류']}

        assignment, _ = random_assign(list(set(morning_personnel+afternoon_personnel)), assignable_slots, request_assignments, st.session_state["time_groups"], total_stats, morning_personnel, afternoon_personnel, afternoon_duty_counts)
        
        for slot in all_slots:
            person = row['오전당직(온콜)'] if slot == morning_duty_slot or slot == '온콜' else (assignment[assignable_slots.index(slot)] if slot in assignable_slots and assignment else None)
            result_row.append(person if has_person else None)
        
        # [추가] 중복 배정 검증 로직
        assignments_for_day = dict(zip(all_slots, result_row[2:]))
        morning_slots_check = [s for s in all_slots if s.startswith(('8:30', '9:00', '9:30', '10:00'))]
        afternoon_slots_check = [s for s in all_slots if s.startswith('13:30') or s == '온콜']

        morning_counts = Counter(p for s, p in assignments_for_day.items() if s in morning_slots_check and p)
        for person, count in morning_counts.items():
            if count > 1:
                duplicated_slots = [s for s, p in assignments_for_day.items() if p == person and s in morning_slots_check]
                st.error(f"⚠️ {date_str}: '{person}'님이 오전에 중복 배정되었습니다 (슬롯: {', '.join(duplicated_slots)}).")
        
        afternoon_counts = Counter(p for s, p in assignments_for_day.items() if s in afternoon_slots_check and p)
        for person, count in afternoon_counts.items():
            if count > 1:
                duplicated_slots = [s for s, p in assignments_for_day.items() if p == person and s in afternoon_slots_check]
                st.error(f"⚠️ {date_str}: '{person}'님이 오후/온콜에 중복 배정되었습니다 (슬롯: {', '.join(duplicated_slots)}).")

        result_data.append(result_row)
    
    df_room = pd.DataFrame(result_data, columns=columns)
    st.write(" ")
    st.markdown("**✅ 통합 배치 결과**")
    st.dataframe(df_room, hide_index=True)
    
    # --- 통계 계산 (기존과 동일) ---
    for row_data in result_data:
        person_on_call = row_data[columns.index('온콜')]
        if person_on_call:
            total_stats['morning_duty'][person_on_call] += 1
    
    # --- 통계 DataFrame 생성 (기존과 동일) ---
    stats_data, all_personnel_stats = [], set(p for _, r in st.session_state["df_schedule_md"].iterrows() for p in r[2:-1].dropna() if p)
    for person in sorted(all_personnel_stats):
        stats_data.append({'인원': person, '이른방 합계': total_stats['early'][person], '늦은방 합계': total_stats['late'][person], '오전 당직 합계': total_stats['morning_duty'][person], '오후 당직 합계': total_stats['afternoon_duty'][person], **{f'{r}번방 합계': total_stats['rooms'][r][person] for r in total_stats['rooms']}})
    stats_df = pd.DataFrame(stats_data)
    st.divider(); st.markdown("**☑️ 인원별 통계**"); st.dataframe(stats_df, hide_index=True)
    
    # --- [수정] Excel 생성 및 다운로드 로직 ---
    wb = openpyxl.Workbook()
    sheet = wb.active
    sheet.title = "Schedule"
    sky_blue_fill = PatternFill(start_color="CCEEFF", end_color="CCEEFF", fill_type="solid")
    duty_font = Font(name="맑은 고딕", size=9, bold=True, color="FF00FF")
    default_font = Font(name="맑은 고딕", size=9)
    swapped_set = st.session_state.get("swapped_assignments", set())

    special_day_fill = PatternFill(start_color="BFBFBF", end_color="BFBFBF", fill_type="solid") # 소수 근무일 '요일' 색상
    no_person_day_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid") # 근무자 없는 날 색상
    default_yoil_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid") # 기본 '요일' 색상

    # 헤더 렌더링
    for col_idx, header in enumerate(columns, 1):
        cell = sheet.cell(1, col_idx, header)
        cell.font = Font(bold=True, name="맑은 고딕", size=9)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        if header.startswith('8:30') or header == '온콜': cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
        elif header.startswith('9:00'): cell.fill = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
        elif header.startswith('9:30'): cell.fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
        elif header.startswith('10:00'): cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
        elif header.startswith('13:30'): cell.fill = PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")

    # 데이터 렌더링
    for row_idx, row_data in enumerate(result_data, 2):
        has_person = any(val for val in row_data[2:] if val)

        current_date_str = row_data[0]
        assignment_cells = row_data[2:]
        personnel_in_row = [p for p in assignment_cells if p]
        is_no_person_day = not any(personnel_in_row)
        SMALL_TEAM_THRESHOLD_FORMAT = 15
        is_small_team_day = (0 < len(personnel_in_row) < SMALL_TEAM_THRESHOLD_FORMAT)

        current_date_str = row_data[0]
        for col_idx, value in enumerate(row_data, 1):
            cell = sheet.cell(row_idx, col_idx, value)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            
            if col_idx == 1:
                cell.fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
            elif col_idx == 2: # '요일' 열
                if is_no_person_day:
                    cell.fill = no_person_day_fill   # 1순위: 근무자 없는 날
                elif is_small_team_day:
                    cell.fill = special_day_fill     # 2순위: 소수 인원 근무일
                else:
                    cell.fill = default_yoil_fill    # 3순위: 일반 근무일
            elif is_no_person_day and col_idx >= 3: # 근무자 없는 날의 배정 슬롯
                cell.fill = no_person_day_fill

            slot_name = columns[col_idx-1]
            
            # [핵심 수정] 셀의 근무타입을 판별
            cell_shift_type = '오후' if '13:30' in slot_name or '온콜' in slot_name else '오전'
            
            # 서식 적용 (배경색 -> 폰트 -> 메모 순)
            if (current_date_str, cell_shift_type, value) in swapped_set:
                cell.fill = sky_blue_fill
            
            if (slot_name.endswith('_당직') or slot_name == '온콜') and value:
                cell.font = duty_font
            else:
                cell.font = default_font
            
            if col_idx > 2 and value and date_cache.get(current_date_str):
                formatted_date_for_comment = date_cache[current_date_str]
                if (formatted_date_for_comment, slot_name) in request_cells and value == request_cells[(formatted_date_for_comment, slot_name)]['이름']:
                    cell.comment = Comment(f"배정 요청: {request_cells[(formatted_date_for_comment, slot_name)]['분류']}", "System")
    
    # --- Stats 시트 생성 및 최종 파일 저장 (기존과 동일) ---
    stats_sheet = wb.create_sheet("Stats")
    stats_columns = stats_df.columns.tolist()
    for col_idx, header in enumerate(stats_columns, 1):
            stats_sheet.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = 12
            cell = stats_sheet.cell(1, col_idx, header)
            cell.font = Font(bold=True, name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            if header == '인원': cell.fill = PatternFill(start_color="D0CECE", end_color="D0CECE", fill_type="solid")
            elif header == '이른방 합계': cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
            elif header == '늦은방 합계': cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
            elif '당직' in header: cell.fill = PatternFill(start_color="FFC0CB", end_color="FFC0CB", fill_type="solid")
            elif '번방' in header: cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    
    for row_idx, row in enumerate(stats_df.values, 2):
        for col_idx, value in enumerate(row, 1):
            cell = stats_sheet.cell(row_idx, col_idx, value)
            cell.font = Font(name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    st.divider()
    st.download_button(
        label="📥 최종 방배정 다운로드",
        data=output,
        file_name=f"{month_str} 방배정.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )

    # Google Sheets에 방배정 시트 저장
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    try:
        worksheet_result = sheet.worksheet(f"{month_str} 방배정")
    except gspread.exceptions.WorksheetNotFound:
        worksheet_result = sheet.add_worksheet(f"{month_str} 방배정", rows=100, cols=len(df_room.columns))
    update_sheet_with_retry(worksheet_result, [df_room.columns.tolist()] + df_room.fillna('').values.tolist())
    st.success(f"✅ {month_str} 방배정 테이블이 Google Sheets에 저장되었습니다.")