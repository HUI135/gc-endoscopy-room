import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import time
from datetime import datetime, date
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu

st.set_page_config(page_title="방 배정 변경", page_icon="🔄", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

import time

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# --- 상수 정의 ---
MONTH_STR = "2025년 04월"

# --- 세션 상태 초기화 ---
# 이 페이지에서 발생한 변경사항만 기록하도록 초기화
def initialize_session_state():
    st.session_state.setdefault("data_loaded", False)
    st.session_state.setdefault("df_room_original", pd.DataFrame())
    st.session_state.setdefault("df_room_edited", pd.DataFrame())
    st.session_state.setdefault("df_room_swap_requests", pd.DataFrame())
    # {날짜: {사람1, 사람2}} 형식으로 기록하여 특정 셀만 정확히 타겟
    st.session_state.setdefault("schedule_changed_cells", {}) 
    st.session_state.setdefault("room_changed_cells", {})

# --- 데이터 통신 함수 ---
@st.cache_data(ttl=600)
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='USER_ENTERED')
            return
        except Exception as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.error(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# --- 데이터 로딩 ---
def load_data(month_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    
    # 1. 최종 방배정 결과 불러오기
    try:
        worksheet_room = sheet.worksheet(f"{month_str} 방배정")
        df_room = pd.DataFrame(worksheet_room.get_all_records())
        st.session_state["df_room_original"] = df_room.copy()
        # 수정용 데이터프레임이 없으면 원본으로 초기화
        if st.session_state.df_room_edited.empty:
            st.session_state["df_room_edited"] = df_room.copy()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{month_str} 방배정' 시트를 찾을 수 없습니다. 이전 단계 먼저 수행해주세요.")
        st.stop()
        
    # 2. 방배정 변경 요청 불러오기
    try:
        ws_room_swap = sheet.worksheet(f"{month_str} 방배정 변경요청")
        st.session_state["df_room_swap_requests"] = pd.DataFrame(ws_room_swap.get_all_records())
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"'{month_str} 방배정 변경요청' 시트가 없습니다.")
        st.session_state["df_room_swap_requests"] = pd.DataFrame()

    # 3. 스케줄 변경 이력 불러오기 (하늘색 하이라이트용)
    try:
        ws_schedule_swap = sheet.worksheet(f"{month_str} 스케줄 교환요청")
        df_schedule_swaps = pd.DataFrame(ws_schedule_swap.get_all_records())
        
        def parse_swap_date(date_str):
            match = re.search(r'(\d+)월 (\d+)일', date_str)
            return f"{int(match.group(1))}월 {int(match.group(2))}일" if match else None

        for _, row in df_schedule_swaps.iterrows():
            from_date = parse_swap_date(row['FromDateStr'])
            to_date = parse_swap_date(row['ToDateStr'])
            if from_date:
                st.session_state.schedule_changed_cells.setdefault(from_date, set()).add(row['ToPersonName'])
            if to_date:
                st.session_state.schedule_changed_cells.setdefault(to_date, set()).add(row['RequesterName'])
    except gspread.exceptions.WorksheetNotFound:
        pass # 이 시트는 없어도 오류 아님

    st.session_state["data_loaded"] = True

# --- 로직 함수 ---
def apply_room_swaps(df_current, df_requests):
    df_modified = df_current.copy()
    applied_count = 0
    for _, row in df_requests.iterrows():
        date_str = row['Date']
        requester = row['RequesterName']
        target_person = row['TheirName']
        my_room_col = row['MyRoom']
        their_room_col = row['TheirRoom']
        
        target_row_idx = df_modified[df_modified['날짜'] == date_str].index
        if target_row_idx.empty:
            st.warning(f"적용 실패: 날짜 '{date_str}'를 방배정 표에서 찾을 수 없습니다.")
            continue

        idx = target_row_idx[0]
        # 현재 셀의 값이 요청자와 일치하는지 확인 후 교환
        if df_modified.at[idx, my_room_col] == requester and df_modified.at[idx, their_room_col] == target_person:
            df_modified.at[idx, my_room_col] = target_person
            df_modified.at[idx, their_room_col] = requester
            
            # 변경된 인원 기록 (연두색 하이라이트용)
            st.session_state.room_changed_cells.setdefault(date_str, set()).update([requester, target_person])
            applied_count += 1
        else:
            st.warning(f"적용 실패: {date_str}의 {my_room_col} 또는 {their_room_col}의 근무자가 요청과 다릅니다.")
            
    if applied_count > 0:
        st.success(f"{applied_count}건의 방배정 변경 요청이 적용되었습니다.")
    else:
        st.info("새롭게 적용할 방배정 변경 요청이 없습니다.")
        
    return df_modified

def check_duplicates(df):
    errors = []
    morning_slots = [c for c in df.columns if re.match(r'^(8:30|9:00|9:30|10:00)', c)]
    afternoon_slots = [c for c in df.columns if c.startswith('13:30') or c == '온콜']
    
    for idx, row in df.iterrows():
        date = row['날짜']
        morning_workers = [p for p in row[morning_slots].values if pd.notna(p) and p]
        afternoon_workers = [p for p in row[afternoon_slots].values if pd.notna(p) and p]

        for person, count in Counter(morning_workers).items():
            if count > 1:
                errors.append(f"{date}: '{person}'님이 오전에 중복 배정되었습니다.")
        for person, count in Counter(afternoon_workers).items():
            if count > 1:
                errors.append(f"{date}: '{person}'님이 오후/온콜에 중복 배정되었습니다.")
    return errors

def create_final_excel(df, stats_df):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "방배정 최종"
    
    # 스타일 정의
    sky_blue_fill = PatternFill(start_color="CCEEFF", end_color="CCEEFF", fill_type="solid")
    light_green_fill = PatternFill(start_color="D8E4BC", end_color="D8E4BC", fill_type="solid")
    duty_font = Font(color="FF00FF", bold=True)
    
    # 헤더 쓰기
    for c, col_name in enumerate(df.columns, 1):
        ws.cell(row=1, column=c, value=col_name).font = Font(bold=True)
    
    # 데이터 쓰기 및 서식 적용
    for r, row in enumerate(df.itertuples(), 2):
        date_str = row.날짜
        for c, value in enumerate(row[1:], 1):
            cell = ws.cell(row=r, column=c, value=value)
            
            # 배경색 적용 (우선순위: 연두색 > 하늘색)
            is_room_changed = date_str in st.session_state.room_changed_cells and value in st.session_state.room_changed_cells[date_str]
            is_schedule_changed = date_str in st.session_state.schedule_changed_cells and value in st.session_state.schedule_changed_cells[date_str]

            if is_room_changed:
                cell.fill = light_green_fill
            elif is_schedule_changed:
                cell.fill = sky_blue_fill

            # 당직자 폰트
            if df.columns[c-1].endswith('_당직') or df.columns[c-1] == '온콜':
                cell.font = duty_font
    
    # 통계 시트 추가 (옵션)
    if not stats_df.empty:
        ws_stats = wb.create_sheet("통계")
        for r, row in enumerate(pd.DataFrame(stats_df).itertuples(index=False), 1):
            for c, value in enumerate(row, 1):
                ws_stats.cell(row=r, column=c, value=value)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output

# --- 메인 UI ---
st.set_page_config(layout="wide")
initialize_session_state()

st.title(f"✨ {MONTH_STR} 방배정 최종 조정")
if st.button("🔄 데이터 새로고침"):
    st.cache_data.clear()
    st.rerun()

load_data(MONTH_STR)

# --- 1. 방배정 변경 요청 확인 및 일괄 적용 ---
st.header("Step 1. 변경 요청 확인 및 적용")
st.write("방배정 결과를 확인하고, 아래 요청에 따라 스케줄을 조정합니다.")

st.subheader("📋 방배정 변경 요청 목록")
df_swaps = st.session_state.df_room_swap_requests
if not df_swaps.empty:
    st.dataframe(df_swaps, use_container_width=True, hide_index=True)
    if st.button("🔄 요청 일괄 적용하기"):
        st.session_state.df_room_edited = apply_room_swaps(st.session_state.df_room_edited, df_swaps)
        st.rerun()
else:
    st.info("접수된 방배정 변경 요청이 없습니다.")

st.divider()

# --- 2. 수작업 수정 및 최종 확인 ---
st.header("Step 2. 최종 수정 및 저장")
st.write("일괄 적용 결과를 확인하거나, 셀을 더블클릭하여 직접 수정한 후 저장하세요.")

edited_df = st.data_editor(
    st.session_state.df_room_edited,
    use_container_width=True,
    key="room_editor"
)

# --- 3. 저장 및 내보내기 ---
st.write(" ")
if st.button("💾 최종 저장 및 내보내기", type="primary", use_container_width=True):
    final_df = edited_df.copy()
    
    # 3-1. 중복 배정 검증
    st.info("중복 배정 여부를 확인합니다...")
    errors = check_duplicates(final_df)
    if errors:
        for error in errors:
            st.error(error)
        st.warning("오류를 수정한 후 다시 저장해주세요.")
        st.stop()
    else:
        st.success("중복 배정 오류가 없습니다.")

    # 3-2. 수작업 변경사항 기록
    diff = final_df.compare(st.session_state.df_room_edited)
    if not diff.empty:
        st.info("수작업 변경사항을 기록합니다...")
        for idx, row in diff.iterrows():
            date_str = final_df.loc[idx, '날짜']
            # 변경된 셀의 값(사람 이름)을 기록
            changed_values = set(val for val in row.values if pd.notna(val))
            st.session_state.room_changed_cells.setdefault(date_str, set()).update(changed_values)

    # 3-3. Google Sheets 저장
    st.info("Google Sheets에 최종 결과를 저장합니다...")
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    try:
        worksheet_final = sheet.worksheet(f"{MONTH_STR} 방배정 최종")
    except gspread.exceptions.WorksheetNotFound:
        worksheet_final = sheet.add_worksheet(title=f"{MONTH_STR} 방배정 최종", rows=100, cols=50)
    
    update_sheet_with_retry(worksheet_final, [final_df.columns.tolist()] + final_df.fillna('').values.tolist())
    st.success("✅ Google Sheets 저장이 완료되었습니다.")
    
    # 3-4. Excel 파일 생성 및 다운로드
    st.info("다운로드할 Excel 파일을 생성합니다...")
    stats_df = calculate_stats(final_df) # 통계는 최종본으로 계산
    excel_file = create_final_excel(final_df, stats_df)
    
    st.download_button(
        label="📥 변경사항 포함된 Excel 다운로드",
        data=excel_file,
        file_name=f"{MONTH_STR} 방배정_최종본.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
    
    # 3-5. 변경 로그 표시
    st.subheader("📝 최종 변경사항 요약")
    if st.session_state.room_changed_cells:
        log_data = []
        for date_val, names in st.session_state.room_changed_cells.items():
            log_data.append(f"**{date_val}:** {', '.join(names)}")
        st.markdown("\n".join(f"- {item}" for item in log_data))
    else:
        st.info("이번 세션에서 방 배정 변경사항이 없습니다.")