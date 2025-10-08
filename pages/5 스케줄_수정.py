import streamlit as st
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from collections import Counter

# Google Sheets 관련 라이브러리
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError

# 엑셀 생성을 위한 라이브러리
import io
import openpyxl
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
from openpyxl.comments import Comment

# 사용자 정의 메뉴 모듈
import menu

# --- 페이지 설정 및 초기화 ---
st.set_page_config(page_title="스케줄 수정", page_icon="✍️", layout="wide")
menu.menu()

# --- 로그인 확인 ---
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# --- Google Sheets API 연동 함수 ---

@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"⚠️ Google Sheets 클라이언트 초기화 또는 인증에 실패했습니다: {e}"); st.stop()

def update_sheet_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.clear(); worksheet.update(data, "A1"); return True
        except APIError as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ API 요청 지연... {delay}초 후 재시도 ({attempt+1}/{retries})"); time.sleep(delay * (attempt + 1))
            else:
                st.error(f"Google Sheets API 오류: {e}"); st.stop()
    return False

# --- 데이터 로딩 및 처리 함수 ---

def find_schedule_versions(sheet, month_str):
    versions = {}; pattern = re.compile(f"^{re.escape(month_str)} 스케줄( ver(\d+\.\d+))?$")
    for ws in sheet.worksheets():
        match = pattern.match(ws.title)
        if match:
            version_str = match.group(2); version_num = float(version_str) if version_str else 1.0
            versions[ws.title] = version_num
    return dict(sorted(versions.items(), key=lambda item: item[1], reverse=True))

@st.cache_data(ttl=600, show_spinner="최신 데이터를 구글 시트에서 불러오는 중...")
def load_data(month_str, schedule_sheet_name, version_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    target_year = month_str.split('년')[0]
    
    current_month_dt = datetime.strptime(month_str, "%Y년 %m월")
    next_month_str = (current_month_dt + relativedelta(months=1)).strftime("%Y년 %-m월")

    # 스케줄 시트 로드
    try:
        ws_schedule = sheet.worksheet(schedule_sheet_name)
        df_schedule = pd.DataFrame(ws_schedule.get_all_records())
    except WorksheetNotFound:
        st.error(f"'{schedule_sheet_name}' 시트를 찾을 수 없습니다."); st.stop()
    
    # 익월 누적 시트 로드
    display_cum_sheet_name = f"{next_month_str} 누적{version_str}"
    try:
        ws_display_cum = sheet.worksheet(display_cum_sheet_name)
        df_display_cum = pd.DataFrame(ws_display_cum.get_all_records())
    except WorksheetNotFound:
        df_display_cum = pd.DataFrame()

    # 토요/휴일 스케줄 로드
    try:
        ws_special = sheet.worksheet(f"{target_year}년 토요/휴일 스케줄")
        df_yearly = pd.DataFrame(ws_special.get_all_records())
        df_yearly['날짜_dt'] = pd.to_datetime(df_yearly['날짜'])
        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        df_special = df_yearly[(df_yearly['날짜_dt'].dt.year == target_month_dt.year) & (df_yearly['날짜_dt'].dt.month == target_month_dt.month)].copy()
    except WorksheetNotFound: df_special = pd.DataFrame()

    # 휴관일 데이터 로드
    try:
        ws_closing = sheet.worksheet(f"{target_year}년 휴관일")
        df_closing = pd.DataFrame(ws_closing.get_all_records())
        if '날짜' in df_closing.columns and not df_closing.empty:
            closing_dates = pd.to_datetime(df_closing['날짜']).dt.strftime('%Y-%m-%d').tolist()
        else:
            closing_dates = []
    except WorksheetNotFound:
        closing_dates = []

    return {
        "schedule": df_schedule, 
        "cumulative_display": df_display_cum, 
        "swaps": pd.DataFrame(), # 이 페이지에서는 직접 사용하지 않으므로 빈DF
        "special": df_special,
        "requests": pd.DataFrame(), # 이 페이지에서는 직접 사용하지 않으므로 빈DF
        "closing_dates": closing_dates
    }

def apply_schedule_swaps(original_schedule_df, swap_requests_df):
    df_modified = original_schedule_df.copy(); change_log = []; messages = []; applied_count = 0
    for _, request_row in swap_requests_df.iterrows():
        try:
            change_request_str = str(request_row.get('변경 요청', '')).strip(); schedule_info_str = str(request_row.get('변경 요청한 스케줄', '')).strip()
            if '➡️' not in change_request_str: continue
            person_before, person_after = [p.strip() for p in change_request_str.split('➡️')]; date_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', schedule_info_str)
            if not date_match: continue
            date_part, time_period = date_match.groups(); date_obj = datetime.strptime(date_part, '%Y-%m-%d').date(); formatted_date_in_df = f"{date_obj.month}월 {date_obj.day}일"
            target_rows = df_modified[df_modified['날짜'] == formatted_date_in_df]
            if target_rows.empty: continue
            target_row_idx = target_rows.index[0]; on_call_person = str(df_modified.at[target_row_idx, '오전당직(온콜)']).strip()
            if time_period == '오전당직(온콜)' or person_before == on_call_person:
                cols_with_person_before = [c for c in df_modified.columns if str(df_modified.at[target_row_idx, c]).strip() == person_before]
                if not cols_with_person_before: messages.append(('error', f"❌ {schedule_info_str} - '{person_before}' 당직 근무가 없습니다.")); continue
                cols_with_person_after = [c for c in df_modified.columns if str(df_modified.at[target_row_idx, c]).strip() == person_after]
                for col in cols_with_person_before: df_modified.at[target_row_idx, col] = person_after
                for col in cols_with_person_after: df_modified.at[target_row_idx, col] = person_before
                change_log.append({'날짜': f"{formatted_date_in_df} (당직 맞교환)", '변경 전': person_before, '변경 후': person_after})
            else:
                target_cols = [str(i) for i in range(1, 18)] if time_period == '오전' else [f'오후{i}' for i in range(1, 10)]; personnel_in_period = {str(df_modified.at[target_row_idx, c]).strip() for c in target_cols if c in df_modified.columns}
                if person_after in personnel_in_period: messages.append(('warning', f"🟡 {schedule_info_str} - '{person_after}'님은 이미 해당 시간 근무자입니다.")); continue
                found_and_replaced = False
                for col in target_cols:
                    if col in df_modified.columns and str(df_modified.at[target_row_idx, col]).strip() == person_before:
                        df_modified.at[target_row_idx, col] = person_after; change_log.append({'날짜': f"{schedule_info_str}", '변경 전': person_before, '변경 후': person_after}); found_and_replaced = True; break
                if not found_and_replaced: messages.append(('error', f"❌ {schedule_info_str} - '{person_before}' 근무자를 찾을 수 없습니다.")); continue
            applied_count += 1
        except Exception as e: messages.append(('error', f"요청 처리 중 오류: {e}"))
    if applied_count > 0: messages.insert(0, ('success', f"✅ 총 {applied_count}건의 스케줄 변경 요청이 반영되었습니다."))
    elif not messages: messages.append(('info', "새롭게 적용할 스케줄 변경 요청이 없습니다."))
    st.session_state["change_log"] = change_log; return df_modified, messages

def format_sheet_date_for_display(date_string):
    match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', date_string)
    if match:
        date_part, shift_part = match.groups()
        try:
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d').date(); weekday_str = ['월', '화', '수', '목', '금', '토', '일'][dt_obj.weekday()]; return f"{dt_obj.month}월 {dt_obj.day}일 ({weekday_str}) - {shift_part}"
        except ValueError: pass
    return date_string

# --- 1. 기존 엑셀 생성 함수 전체를 이 코드로 교체하세요 ---

def create_formatted_schedule_excel(initial_df, edited_df, edited_cumulative_df, df_special, df_requests, closing_dates, month_str):
    output = io.BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "수정된 스케줄"

    # --- 1. 스타일 및 전체 색상 맵 정의 ---
    font_name = "맑은 고딕"
    default_font = Font(name=font_name, size=9)
    bold_font = Font(name=font_name, size=9, bold=True)
    duty_font = Font(name=font_name, size=9, bold=True, color="FF69B4")
    header_font = Font(name=font_name, size=9, color='FFFFFF', bold=True)

    color_map = {
        '휴가': 'DA9694', '학회': 'DA9694',
        '꼭 근무': 'FABF8F',
        '추가보충': 'FFF28F', '보충': 'FFF28F',
        '대체 보충': 'A9D08E',
        '추가제외': 'B1A0C7', '제외': 'B1A0C7',
        '대체 휴근': '95B3D7',
        '특수근무': 'D0E0E3',
        '기본': 'FFFFFF'
    }
    
    header_fill = PatternFill(start_color='000000', fill_type='solid')
    date_col_fill = PatternFill(start_color='808080', fill_type='solid')
    weekday_fill = PatternFill(start_color='FFF2CC', fill_type='solid')
    special_day_fill = PatternFill(start_color='95B3D7', fill_type='solid')
    changed_fill = PatternFill(start_color='FFFF00', fill_type='solid')
    empty_day_fill = PatternFill(start_color='808080', fill_type='solid')

    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    center_align = Alignment(horizontal='center', vertical='center')

    requests_map = {}
    if not df_requests.empty:
        def parse_date_range(d_str):
            if pd.isna(d_str) or not isinstance(d_str, str) or d_str.strip() == '': return []
            d_str = d_str.strip()
            if '~' in d_str:
                try:
                    start, end = [datetime.strptime(d.strip(), '%Y-%m-%d').date() for d in d_str.split('~')]
                    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end - start).days + 1)]
                except: return []
            else:
                try:
                    return [datetime.strptime(d.strip(), '%Y-%m-%d').date().strftime('%Y-%m-%d') for d in d_str.split(',')]
                except: return []
        
        for _, row in df_requests.iterrows():
            worker = row['이름']
            status = row['분류']
            if status in ['휴가', '학회'] or '꼭 근무' in status:
                clean_status = '꼭 근무' if '꼭 근무' in status else status
                for date_iso in parse_date_range(row['날짜정보']):
                    requests_map[(worker, date_iso)] = clean_status

    # --- 2. 헤더 생성 ---
    for c, col_name in enumerate(edited_df.columns, 1):
        cell = ws.cell(row=1, column=c, value=col_name)
        cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align; cell.border = border

    # --- 3. 데이터 행 생성 및 서식 적용 ---
    for r, (idx, edited_row) in enumerate(edited_df.iterrows(), 2):
        initial_row = initial_df.loc[idx]
        
        try:
            current_date = datetime.strptime(f"{month_str.split('년')[0]}-{edited_row['날짜']}", "%Y-%m월 %d일").date()
            current_date_iso = current_date.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            current_date = None; current_date_iso = None

        is_row_empty = all(pd.isna(v) or str(v).strip() == '' for k, v in edited_row.items() if k not in ['날짜', '요일'])
        is_special_day = current_date in pd.to_datetime(df_special['날짜']).dt.date.values if current_date and not df_special.empty else False
        is_empty_day = (is_row_empty and not is_special_day) or (current_date_iso in closing_dates)
        
        weekend_oncall_worker = None
        if is_special_day:
            special_day_info = df_special[pd.to_datetime(df_special['날짜']).dt.date == current_date]
            if not special_day_info.empty and '당직' in special_day_info.columns:
                oncall_val = special_day_info['당직'].iloc[0]
                if pd.notna(oncall_val) and oncall_val != "당직 없음":
                    weekend_oncall_worker = str(oncall_val).strip()

        for c, col_name in enumerate(edited_df.columns, 1):
            cell = ws.cell(row=r, column=c, value=edited_row[col_name])
            cell.font = default_font; cell.alignment = center_align; cell.border = border

            if is_empty_day:
                cell.fill = empty_day_fill; continue

            if col_name == '날짜':
                cell.fill = date_col_fill; continue
            if col_name == '요일':
                cell.fill = special_day_fill if is_special_day else weekday_fill; continue
            
            raw_value = str(edited_row.get(col_name, '')).strip()
            
            if is_special_day:
                if str(col_name).isdigit() and raw_value:
                    cell.fill = PatternFill(start_color=color_map['특수근무'], end_color=color_map['특수근무'], fill_type='solid')
                    if raw_value == weekend_oncall_worker:
                        cell.font = duty_font
                elif '오후' in str(col_name):
                    cell.value = ""
                continue
            
            worker_name = raw_value
            status = '기본'
            
            match = re.match(r'(.+?)\((.+)\)', raw_value)
            if match:
                worker_name = match.group(1).strip(); status = match.group(2).strip()
            elif current_date_iso and worker_name:
                status = requests_map.get((worker_name, current_date_iso), '기본')

            cell.value = worker_name
            if not worker_name: continue

            fill_color_hex = color_map.get(status)
            if fill_color_hex:
                cell.fill = PatternFill(start_color=fill_color_hex, end_color=fill_color_hex, fill_type='solid')

            if col_name == '오전당직(온콜)' and worker_name:
                cell.font = duty_font
            
            initial_raw_value = str(initial_row.get(col_name, '')).strip()
            if raw_value != initial_raw_value:
                cell.fill = changed_fill
                cell.comment = Comment(f"변경 전: {initial_raw_value or '빈 값'}", "Edit Tracker")

    # --- 4. 익월 누적 현황 추가 ---
    if not edited_cumulative_df.empty:
        start_row = ws.max_row + 3
        ws.cell(row=start_row - 1, column=1, value="익월 누적 현황 (수정본)").font = bold_font
        
        cum_header = edited_cumulative_df.columns.tolist()
        for c, col_name in enumerate(cum_header, 1):
            cell = ws.cell(row=start_row, column=c, value=col_name)
            cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align; cell.border = border
            
        for r_cum, (_, cum_row) in enumerate(edited_cumulative_df.iterrows(), start_row + 1):
            for c_cum, col_name in enumerate(cum_header, 1):
                cell = ws.cell(row=r_cum, column=c_cum, value=cum_row[col_name])
                cell.alignment = center_align; cell.border = border

    # --- 5. 열 너비 설정 ---
    ws.column_dimensions['A'].width = 11
    for col in ws.columns:
        if col[0].column_letter != 'A':
            ws.column_dimensions[col[0].column_letter].width = 9

    wb.save(output)
    return output.getvalue()

# --- 메인 UI ---
st.header("✍️ 스케줄 수정", divider='rainbow')
kst = ZoneInfo("Asia/Seoul")
month_dt_now = datetime.now(kst).replace(day=1) + relativedelta(months=1)
month_str = month_dt_now.strftime("%Y년 %-m월")
month_str = "2025년 10월" # 테스트용 고정

gc = get_gspread_client()
sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
versions = find_schedule_versions(sheet, month_str)

def on_version_change():
    st.session_state.data_loaded = False

if not versions:
    st.warning(f"'{month_str}'에 해당하는 스케줄 시트가 없습니다. 먼저 스케줄을 생성해주세요."); st.stop()

version_list = list(versions.keys())
selected_sheet_name = st.selectbox("🗓️ 불러올 스케줄 버전을 선택하세요", options=version_list, index=0, key="selected_sheet_name", on_change=on_version_change)
version_str = " " + selected_sheet_name.split(" 스케줄 ")[1] if " ver" in selected_sheet_name else ""

# --- 1. 새로고침 버튼 부분을 이 코드로 교체하세요 ---

if st.button("🔄 현재 버전 데이터 새로고침"):
    st.cache_data.clear()
    # ▼▼▼ "closing_dates" 키를 삭제 목록에 추가 ▼▼▼
    for key in ["data_loaded", "df_display_modified", "change_log", "apply_messages", "df_cumulative_next_display", "cumulative_editor", "closing_dates"]:
        if key in st.session_state: del st.session_state[key]
    st.rerun()

if not st.session_state.get("data_loaded", False):
    data = load_data(month_str, selected_sheet_name, version_str)
    st.session_state["df_schedule_original"] = data["schedule"]
    st.session_state["df_cumulative_next_display"] = data["cumulative_display"]
    st.session_state["df_display_initial"] = data["schedule"].copy()
    st.session_state["df_swaps"] = data["swaps"]
    st.session_state["df_special"] = data["special"]
    st.session_state["df_requests"] = data["requests"]
    st.session_state["closing_dates"] = data["closing_dates"]
    st.session_state.data_loaded = True

if st.session_state["df_schedule_original"].empty:
    st.info(f"'{selected_sheet_name}' 시트에 데이터가 없습니다."); st.stop()

st.divider(); st.subheader("📋 스케줄 변경 요청 목록")
df_swaps_raw = st.session_state.get("df_swaps", pd.DataFrame())
if not df_swaps_raw.empty:
    df_swaps_display = df_swaps_raw[['요청일시', '요청자', '변경 요청', '변경 요청한 스케줄']].copy()
    df_swaps_display['변경 요청한 스케줄'] = df_swaps_display['변경 요청한 스케줄'].apply(format_sheet_date_for_display)
    st.dataframe(df_swaps_display, use_container_width=True, hide_index=True)
else:
    st.info("접수된 스케줄 변경 요청이 없습니다.")

st.divider(); st.subheader("✍️ 스케줄 수정 테이블")
df_to_display = st.session_state.get("df_display_modified", st.session_state.get("df_display_initial"))
col1, col2 = st.columns(2)
with col1:
    if st.button("🔄 요청사항 일괄 적용"):
        if not st.session_state.df_swaps.empty:
            base_df = st.session_state.get("df_display_modified", st.session_state["df_display_initial"]); modified_df, messages = apply_schedule_swaps(base_df, st.session_state.df_swaps)
            st.session_state["df_display_modified"] = modified_df; st.session_state["apply_messages"] = messages; st.rerun()
        else: st.info("처리할 교환 요청이 없습니다.")
with col2:
    if st.button("⏪ 적용 취소", disabled="df_display_modified" not in st.session_state):
        if "df_display_modified" in st.session_state: del st.session_state["df_display_modified"]
        if "change_log" in st.session_state: del st.session_state["change_log"]
        st.session_state["apply_messages"] = [('info', "변경사항이 취소되고 원본 스케줄로 돌아갑니다.")]; st.rerun()
if "apply_messages" in st.session_state:
    for msg_type, msg_text in st.session_state["apply_messages"]:
        if msg_type == 'success': st.success(msg_text)
        elif msg_type == 'warning': st.warning(msg_text)
        elif msg_type == 'error': st.error(msg_text)
        else: st.info(msg_text)
    del st.session_state["apply_messages"]

edited_df = st.data_editor(df_to_display, use_container_width=True, key="schedule_editor", disabled=['날짜', '요일'])

st.divider()
st.subheader("📊 익월 누적 현황 수정")

if "df_cumulative_next_display" in st.session_state and not st.session_state.df_cumulative_next_display.empty:
    edited_cumulative_df = st.data_editor(
        st.session_state.df_cumulative_next_display,
        hide_index=True,
        key="cumulative_editor"
    )
else:
    st.info("표시할 익월 누적 데이터가 없습니다. 해당 버전의 누적 시트가 존재하는지 확인해주세요.")
    edited_cumulative_df = pd.DataFrame()

st.write("---")
st.caption("📝 변경사항 미리보기")
manual_change_log = []
if not edited_df.equals(df_to_display):
    diff_indices = np.where(edited_df.ne(df_to_display))
    for row_idx, col_idx in zip(diff_indices[0], diff_indices[1]):
        date_str = edited_df.iloc[row_idx, 0]
        weekday = edited_df.iloc[row_idx, 1]
        old_val = df_to_display.iloc[row_idx, col_idx]
        new_val = edited_df.iloc[row_idx, col_idx]
        manual_change_log.append({'날짜': f"{date_str} ({weekday})", '변경 전': str(old_val), '변경 후': str(new_val)})
combined_log = st.session_state.get("change_log", []) + manual_change_log
if combined_log:
    st.dataframe(pd.DataFrame(combined_log), use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")


# --- 2. 다운로드 버튼 부분을 이 코드로 교체하세요 ---

st.divider()
st.download_button(
    label="📥 상세 엑셀 다운로드 (변경사항 확인용)",
    data=create_formatted_schedule_excel(
        st.session_state.df_display_initial,
        edited_df,
        edited_cumulative_df,
        st.session_state.df_special,
        st.session_state.df_requests,
        st.session_state.get("closing_dates", []),
        month_str
    ),
    file_name=f"{selected_sheet_name}_수정본.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

st.divider()
has_schedule_changes = not edited_df.equals(st.session_state.df_display_initial)
has_cumulative_changes = not edited_cumulative_df.equals(st.session_state.df_cumulative_next_display) if not edited_cumulative_df.empty else False

if not has_schedule_changes and not has_cumulative_changes:
    st.warning("저장할 변경사항이 없습니다.")
else:
    df_to_save = edited_df.copy()
    
    def save_schedule(sheet_name, df_to_save, df_cum_to_save):
        with st.spinner(f"'{sheet_name}' 시트에 저장 중입니다..."):
            try:
                gc = get_gspread_client()
                sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                
                # 1. 스케줄 시트 저장
                try: 
                    worksheet = sheet.worksheet(sheet_name)
                except WorksheetNotFound: 
                    worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=50)
                
                columns_to_save = st.session_state["df_schedule_original"].columns.tolist()
                df_to_save_final = df_to_save[columns_to_save] # 저장 시 열 순서 보장
                final_data = [columns_to_save] + df_to_save_final.fillna('').values.tolist()
                update_sheet_with_retry(worksheet, final_data)

                # 2. 익월 누적 시트 저장
                if not df_cum_to_save.empty:
                    current_month_dt_save = datetime.strptime(month_str, "%Y년 %m월")
                    next_month_str_save = (current_month_dt_save + relativedelta(months=1)).strftime("%Y년 %-m월")
                    version_s_save = " " + sheet_name.split(" 스케줄 ")[1] if " ver" in sheet_name else ""
                    cum_sheet_name = f"{next_month_str_save} 누적{version_s_save}"

                    try: 
                        ws_cum = sheet.worksheet(cum_sheet_name)
                    except WorksheetNotFound: 
                        ws_cum = sheet.add_worksheet(title=cum_sheet_name, rows=100, cols=50)
                    
                    cum_data = [df_cum_to_save.columns.tolist()] + df_cum_to_save.astype(str).values.tolist()
                    update_sheet_with_retry(ws_cum, cum_data)

                st.success(f"🎉 스케줄과 익월 누적 데이터가 '{sheet_name}' 버전에 맞게 저장되었습니다.")
                time.sleep(1)
                st.cache_data.clear()
                keys_to_delete_after_save = [
                    "data_loaded", "df_display_modified", "change_log", "apply_messages", 
                    "selected_sheet_name", "df_cumulative_next_display", "cumulative_editor"
                ]
                for key in keys_to_delete_after_save:
                     if key in st.session_state: 
                         del st.session_state[key]
                st.rerun()
            except Exception as e: 
                st.error(f"Google Sheets 저장 중 오류 발생: {e}")

    save_col1, save_col2 = st.columns(2)
    with save_col1:
        if st.button(f"💾 '{selected_sheet_name}' **덮어쓰기**"):
            save_schedule(selected_sheet_name, df_to_save, edited_cumulative_df)
    with save_col2:
        latest_version_name = list(versions.keys())[0]
        latest_version_num = versions[latest_version_name]
        new_version_num = float(int(latest_version_num) + 1)
        new_sheet_name = f"{month_str} 스케줄 ver{new_version_num:.1f}"
        if st.button(f"✨ '{new_sheet_name}'(으)로 **새로 저장**", type="primary"):
            save_schedule(new_sheet_name, df_to_save, edited_cumulative_df)