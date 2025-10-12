import streamlit as st
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from collections import Counter
import platform

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
import os
st.session_state.current_page = os.path.basename(__file__)

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
    
    # ✨ [핵심 수정] 익월 누적 시트 로드 (Transpose 로직 완전 제거)
    display_cum_sheet_name = f"{next_month_str} 누적{version_str}"
    try:
        ws_display_cum = sheet.worksheet(display_cum_sheet_name)
        all_values = ws_display_cum.get_all_values()
        
        if not all_values or len(all_values) < 2:
            st.warning(f"'{display_cum_sheet_name}' 시트가 비어있거나 데이터가 없습니다.")
            df_display_cum = pd.DataFrame()
        else:
            # 시트 모양 그대로 DataFrame 생성
            headers = all_values[0]
            data = all_values[1:]
            df_display_cum = pd.DataFrame(data, columns=headers)
            
            # 숫자 형식으로 변환
            for col in df_display_cum.columns[1:]:
                df_display_cum[col] = pd.to_numeric(df_display_cum[col], errors='coerce').fillna(0)

    except WorksheetNotFound:
        df_display_cum = pd.DataFrame()

    # (이하 토요/휴일, 휴관일 로드 로직은 동일)
    try:
        ws_special = sheet.worksheet(f"{target_year}년 토요/휴일 스케줄")
        df_yearly = pd.DataFrame(ws_special.get_all_records())
        df_yearly['날짜_dt'] = pd.to_datetime(df_yearly['날짜'])
        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        df_special = df_yearly[(df_yearly['날짜_dt'].dt.year == target_month_dt.year) & (df_yearly['날짜_dt'].dt.month == target_month_dt.month)].copy()
    except WorksheetNotFound: df_special = pd.DataFrame()

    try:
        ws_closing = sheet.worksheet(f"{target_year}년 휴관일")
        df_closing = pd.DataFrame(ws_closing.get_all_records())
        closing_dates = pd.to_datetime(df_closing['날짜']).dt.strftime('%Y-%m-%d').tolist() if '날짜' in df_closing.columns and not df_closing.empty else []
    except WorksheetNotFound:
        closing_dates = []

    return {
        "schedule": df_schedule, "cumulative_display": df_display_cum, "swaps": pd.DataFrame(),
        "special": df_special, "requests": pd.DataFrame(), "closing_dates": closing_dates
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

def delete_schedule_version(month_str, sheet_to_delete):
    """선택된 스케줄 버전과 해당 누적 시트를 Google Sheets에서 삭제합니다."""
    try:
        with st.spinner(f"'{sheet_to_delete}' 버전 삭제 중..."):
            gc = get_gspread_client()
            sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])

            # 1. 스케줄 시트 삭제
            try:
                worksheet_to_delete = sheet.worksheet(sheet_to_delete)
                sheet.del_worksheet(worksheet_to_delete)
                st.info(f"'{sheet_to_delete}' 시트를 삭제했습니다.")
            except WorksheetNotFound:
                st.warning(f"'{sheet_to_delete}' 시트를 찾을 수 없어 삭제를 건너뜁니다.")

            # 2. 해당 버전의 누적 시트 이름 생성 및 삭제
            version_str = " " + sheet_to_delete.split(" 스케줄 ")[1] if " ver" in sheet_to_delete else ""
            current_month_dt = datetime.strptime(month_str, "%Y년 %m월")
            next_month_str = (current_month_dt + relativedelta(months=1)).strftime("%Y년 %-m월")
            cum_sheet_name = f"{next_month_str} 누적{version_str}"
            
            try:
                worksheet_cum_to_delete = sheet.worksheet(cum_sheet_name)
                sheet.del_worksheet(worksheet_cum_to_delete)
                st.info(f"'{cum_sheet_name}' 시트를 삭제했습니다.")
            except WorksheetNotFound:
                st.warning(f"'{cum_sheet_name}' 시트를 찾을 수 없어 삭제를 건너뜁니다.")
        
        st.success("선택한 버전이 성공적으로 삭제되었습니다.")
        time.sleep(2)
        st.cache_data.clear()
        st.rerun()

    except Exception as e:
        st.error(f"버전 삭제 중 오류가 발생했습니다: {e}")

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
        style_args = {'font': default_font, 'bold_font': bold_font, 'border': border}
        # 요청하신 함수에 편집된 데이터프레임을 그대로 전달
        append_summary_table_to_excel(ws, edited_cumulative_df, style_args)

    # --- 5. 열 너비 설정 ---
    ws.column_dimensions['A'].width = 11
    for col in ws.columns:
        if col[0].column_letter != 'A':
            ws.column_dimensions[col[0].column_letter].width = 9

    wb.save(output)
    return output.getvalue()

def apply_outer_border(worksheet, start_row, end_row, start_col, end_col):
    medium_side = Side(style='medium') 
    for r in range(start_row, end_row + 1):
        for c in range(start_col, end_col + 1):
            cell = worksheet.cell(row=r, column=c)
            top, left, bottom, right = cell.border.top, cell.border.left, cell.border.bottom, cell.border.right
            if r == start_row: top = medium_side
            if r == end_row: bottom = medium_side
            if c == start_col: left = medium_side
            if c == end_col: right = medium_side
            cell.border = Border(top=top, left=left, bottom=bottom, right=right)

def append_summary_table_to_excel(worksheet, summary_df, style_args):
    if summary_df.empty:
        return

    fills = {
        'header': PatternFill(start_color='E7E6E6', fill_type='solid'), 'yellow': PatternFill(start_color='FFF296', fill_type='solid'),
        'pink': PatternFill(start_color='FFC8CD', fill_type='solid'), 'green': PatternFill(start_color='C6E0B4', fill_type='solid'),
        'dark_green': PatternFill(start_color='82C4B5', fill_type='solid'), 'blue': PatternFill(start_color='B8CCE4', fill_type='solid'),
        'orange': PatternFill(start_color='FCE4D6', fill_type='solid')
    }
    
    start_row = worksheet.max_row + 3
    thin_border = style_args['border'] 

    # 헤더 쓰기
    for c_idx, value in enumerate(summary_df.columns.tolist(), 1):
        cell = worksheet.cell(row=start_row, column=c_idx, value=value)
        cell.fill = fills['header']; cell.font = style_args['bold_font']; cell.border = thin_border
        cell.alignment = Alignment(horizontal='center', vertical='center')

    # 데이터 행 쓰기
    for r_idx, row_data in enumerate(summary_df.itertuples(index=False), start_row + 1):
        label = row_data[0]
        for c_idx, value in enumerate(row_data, 1):
            cell = worksheet.cell(row=r_idx, column=c_idx, value=value)
            cell.font = style_args['bold_font'] if c_idx == 1 else style_args['font']
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', vertical='center')

            fill_color = None
            if label in ["오전누적", "오후누적"]: fill_color = fills['pink']
            elif label in ["오전합계", "오후합계"]: fill_color = fills['blue']
            elif label == "오전당직 (목표)": fill_color = fills['green']
            elif label == "오전당직 (배정)": fill_color = fills['dark_green']
            elif label == "오후당직 (목표)": fill_color = fills['orange']
            if c_idx == 1 and label in ["오전보충", "임시보충", "오후보충", "온콜검사"]: fill_color = fills['yellow']
            if fill_color: cell.fill = fill_color

    start_col, end_col = 1, len(summary_df.columns)
    labels = summary_df.iloc[:, 0].tolist()

    apply_outer_border(worksheet, start_row, start_row, start_col, end_col)
    apply_outer_border(worksheet, start_row, start_row + len(labels), start_col, start_col)
    if "오전보충" in labels and "오전누적" in labels: apply_outer_border(worksheet, start_row + 1 + labels.index("오전보충"), start_row + 1 + labels.index("오전누적"), start_col, end_col)
    if "오후보충" in labels and "오후누적" in labels: apply_outer_border(worksheet, start_row + 1 + labels.index("오후보충"), start_row + 1 + labels.index("오후누적"), start_col, end_col)
    if "오전당직 (목표)" in labels and "오후당직 (목표)" in labels: apply_outer_border(worksheet, start_row + 1 + labels.index("오전당직 (목표)"), start_row + 1 + labels.index("오후당직 (목표)"), start_col, end_col)

    legend_start_row = worksheet.max_row + 3 
    legend_data = [('A9D08E', '대체 보충'), ('FFF28F', '보충'), ('95B3D7', '대체 휴근'), ('B1A0C7', '휴근'), ('DA9694', '휴가/학회')]

    for i, (hex_color, description) in enumerate(legend_data):
        current_row = legend_start_row + i
        
        # ✨ [오류 수정] 'ws'를 'worksheet'로 변경
        color_cell = worksheet.cell(row=current_row, column=1)
        color_cell.fill = PatternFill(start_color=hex_color, fill_type='solid')
        color_cell.border = thin_border

        # ✨ [오류 수정] 'ws'를 'worksheet'로 변경
        desc_cell = worksheet.cell(row=current_row, column=2, value=description)
        desc_cell.font = style_args['font']
        desc_cell.border = thin_border
        desc_cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)

    # ✨ [오류 수정] 'ws'를 'worksheet'로 변경
    worksheet.column_dimensions[openpyxl.utils.get_column_letter(1)].width = 15
    for i in range(2, len(summary_df.columns) + 1):
        worksheet.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 9

# --- 1. 최종본(공유용) 엑셀 생성 함수 ---
def create_final_schedule_excel(initial_df, edited_df, edited_cumulative_df, df_special, df_requests, closing_dates, month_str):
    """
    [공유용 최종본]
    - 열 개수가 고정되며, 셀에는 근무자 이름만 표시됩니다. (상태는 색상으로 표현)
    """
    output = io.BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "스케줄"

    # --- 스타일 정의 ---
    font_name = "맑은 고딕" if platform.system() == "Windows" else "Arial"
    default_font = Font(name=font_name, size=9)
    bold_font = Font(name=font_name, size=9, bold=True)
    duty_font = Font(name=font_name, size=9, bold=True, color="FF69B4")
    header_font = Font(name=font_name, size=9, color='FFFFFF', bold=True)
    color_map = {'휴가': 'DA9694', '학회': 'DA9694', '꼭 근무': 'FABF8F', '추가보충': 'FFF28F', '보충': 'FFF28F', '대체 보충': 'A9D08E', '추가제외': 'B1A0C7', '제외': 'B1A0C7', '대체 휴근': '95B3D7', '특수근무': 'D0E0E3', '기본': 'FFFFFF'}
    header_fill = PatternFill(start_color='000000', fill_type='solid')
    date_col_fill = PatternFill(start_color='808080', fill_type='solid')
    weekday_fill = PatternFill(start_color='FFF2CC', fill_type='solid')
    special_day_fill = PatternFill(start_color='95B3D7', fill_type='solid')
    changed_fill = PatternFill(start_color='FFFF00', fill_type='solid')
    empty_day_fill = PatternFill(start_color='808080', fill_type='solid')
    holiday_blue_fill = PatternFill(start_color="DDEBF7", fill_type='solid')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    center_align = Alignment(horizontal='center', vertical='center')

    # --- 고정된 열 정의 ---
    final_columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + [''] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 5)]

    # --- 헤더 생성 ---
    for c, col_name in enumerate(final_columns, 1):
        cell = ws.cell(row=1, column=c, value=col_name); cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align; cell.border = border

    # --- 데이터 행 생성 및 서식 적용 ---
    for r, (idx, edited_row) in enumerate(edited_df.iterrows(), 2):
        initial_row = initial_df.loc[idx]
        try:
            current_date = datetime.strptime(f"{month_str.split('년')[0]}-{edited_row['날짜']}", "%Y-%m월 %d일").date()
            current_date_iso = current_date.strftime('%Y-%m-%d')
        except: current_date, current_date_iso = None, None
        is_row_empty = all(pd.isna(v) or str(v).strip() == '' for k, v in edited_row.items() if k not in ['날짜', '요일'])
        is_special_day = current_date in pd.to_datetime(df_special['날짜']).dt.date.values if current_date and not df_special.empty else False
        is_empty_day = (is_row_empty and not is_special_day) or (current_date_iso in closing_dates)
        weekend_oncall_worker = None
        if is_special_day:
            special_day_info = df_special[pd.to_datetime(df_special['날짜']).dt.date == current_date]
            if not special_day_info.empty and '당직' in special_day_info.columns:
                oncall_val = special_day_info['당직'].iloc[0]
                if pd.notna(oncall_val) and oncall_val != "당직 없음": weekend_oncall_worker = str(oncall_val).strip()
        for c, col_name in enumerate(final_columns, 1):
            cell = ws.cell(row=r, column=c, value=edited_row.get(col_name, ''))
            cell.font = default_font; cell.alignment = center_align; cell.border = border
            if is_empty_day: cell.fill = empty_day_fill; continue
            if col_name == '날짜': cell.fill = date_col_fill; continue
            if col_name == '요일': cell.fill = special_day_fill if is_special_day else weekday_fill; continue
            raw_value = str(edited_row.get(col_name, '')).strip()
            worker_name = re.sub(r'\(.+\)', '', raw_value).strip()
            status = '기본'
            match = re.match(r'.+?\((.+)\)', raw_value)
            if match: status = match.group(1).strip()
            cell.value = worker_name
            if not worker_name: continue
            if is_special_day:
                if str(col_name).isdigit():
                    cell.fill = holiday_blue_fill
                    if worker_name == weekend_oncall_worker: cell.font = duty_font
                elif '오후' in str(col_name): cell.value = ""
                continue
            fill_hex = color_map.get(status)
            if fill_hex: cell.fill = PatternFill(start_color=fill_hex, fill_type='solid')
            if col_name == '오전당직(온콜)': cell.font = duty_font
            initial_raw_value = str(initial_row.get(col_name, '')).strip()
            if raw_value != initial_raw_value:
                cell.fill = changed_fill
                cell.comment = Comment(f"변경 전: {initial_raw_value or '빈 값'}", "Edit Tracker")

    # --- ✨ [핵심 수정] 익월 누적 현황을 올바른 형식으로 추가 ---
    if not edited_cumulative_df.empty:
        style_args = {'font': default_font, 'bold_font': bold_font, 'border': border}
        # 요청하신 함수에 편집된 데이터프레임을 그대로 전달
        append_summary_table_to_excel(ws, edited_cumulative_df, style_args)

    # --- 열 너비 설정 ---
    ws.column_dimensions['A'].width = 11
    for i in range(2, len(final_columns) + 1): ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 9

    wb.save(output)
    return output.getvalue()


# --- 2. 배정 확인용 엑셀 생성 함수 ---
def create_checking_schedule_excel(initial_df, edited_df, edited_cumulative_df, df_special, df_requests, closing_dates, month_str):
    """
    [관리자 확인용]
    - 열 개수가 동적으로 변하며, 셀에는 이름만 표시되고 상태는 색상으로 표현됩니다.
    """
    output = io.BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "스케줄 (확인용)"

    # --- 스타일 정의 ---
    font_name = "맑은 고딕" if platform.system() == "Windows" else "Arial"
    default_font = Font(name=font_name, size=9)
    bold_font = Font(name=font_name, size=9, bold=True)
    duty_font = Font(name=font_name, size=9, bold=True, color="FF69B4")
    header_font = Font(name=font_name, size=9, color='FFFFFF', bold=True)
    color_map = {'휴가': 'DA9694', '학회': 'DA9694', '꼭 근무': 'FABF8F', '추가보충': 'FFF28F', '보충': 'FFF28F', '대체 보충': 'A9D08E', '추가제외': 'B1A0C7', '제외': 'B1A0C7', '대체 휴근': '95B3D7', '특수근무': 'D0E0E3', '기본': 'FFFFFF'}
    header_fill = PatternFill(start_color='000000', fill_type='solid')
    date_col_fill = PatternFill(start_color='808080', fill_type='solid')
    weekday_fill = PatternFill(start_color='FFF2CC', fill_type='solid')
    special_day_fill = PatternFill(start_color='95B3D7', fill_type='solid')
    changed_fill = PatternFill(start_color='FFFF00', fill_type='solid')
    empty_day_fill = PatternFill(start_color='808080', fill_type='solid')
    holiday_blue_fill = PatternFill(start_color="DDEBF7", fill_type='solid')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    center_align = Alignment(horizontal='center', vertical='center')

    # --- 동적 열 정의 ---
    checking_columns = edited_df.columns.tolist()

    # --- 헤더 생성 ---
    for c, col_name in enumerate(checking_columns, 1):
        cell = ws.cell(row=1, column=c, value=col_name); cell.font = header_font; cell.fill = header_fill; cell.alignment = center_align; cell.border = border

    # --- 데이터 행 생성 및 서식 적용 ---
    for r, (idx, edited_row) in enumerate(edited_df.iterrows(), 2):
        initial_row = initial_df.loc[idx]
        try:
            current_date = datetime.strptime(f"{month_str.split('년')[0]}-{edited_row['날짜']}", "%Y-%m월 %d일").date()
            current_date_iso = current_date.strftime('%Y-%m-%d')
        except: current_date, current_date_iso = None, None
        
        is_row_empty = all(pd.isna(v) or str(v).strip() == '' for k, v in edited_row.items() if k not in ['날짜', '요일'])
        is_special_day = current_date in pd.to_datetime(df_special['날짜']).dt.date.values if current_date and not df_special.empty else False
        is_empty_day = (is_row_empty and not is_special_day) or (current_date_iso in closing_dates)
        
        weekend_oncall_worker = None
        if is_special_day:
            special_day_info = df_special[pd.to_datetime(df_special['날짜']).dt.date == current_date]
            if not special_day_info.empty and '당직' in special_day_info.columns:
                oncall_val = special_day_info['당직'].iloc[0]
                if pd.notna(oncall_val) and oncall_val != "당직 없음": weekend_oncall_worker = str(oncall_val).strip()

        for c, col_name in enumerate(checking_columns, 1):
            raw_value = str(edited_row.get(col_name, '')).strip()
            worker_name = re.sub(r'\(.+\)', '', raw_value).strip()
            status = '기본'
            match = re.match(r'.+?\((.+)\)', raw_value)
            if match: status = match.group(1).strip()
            
            cell = ws.cell(row=r, column=c, value=worker_name)
            cell.font = default_font; cell.alignment = center_align; cell.border = border

            if is_empty_day: cell.fill = empty_day_fill; continue
            if col_name == '날짜': cell.fill = date_col_fill; continue
            if col_name == '요일': cell.fill = special_day_fill if is_special_day else weekday_fill; continue
            
            if not worker_name: continue
            
            if is_special_day:
                if str(col_name).isdigit():
                    cell.fill = holiday_blue_fill
                    if worker_name == weekend_oncall_worker: cell.font = duty_font
                elif '오후' in str(col_name): cell.value = ""
                continue
            
            fill_hex = color_map.get(status)
            if fill_hex: cell.fill = PatternFill(start_color=fill_hex, fill_type='solid')
            if col_name == '오전당직(온콜)': cell.font = duty_font
            initial_raw_value = str(initial_row.get(col_name, '')).strip()
            if raw_value != initial_raw_value:
                cell.fill = changed_fill
                cell.comment = Comment(f"변경 전: {initial_raw_value or '빈 값'}", "Edit Tracker")
    
    # --- ✨ [핵심 수정] 익월 누적 현황을 올바른 형식으로 추가 ---
    if not edited_cumulative_df.empty:
        style_args = {'font': default_font, 'bold_font': bold_font, 'border': border}
        # 요청하신 함수에 편집된 데이터프레임을 그대로 전달
        append_summary_table_to_excel(ws, edited_cumulative_df, style_args)

    # --- 열 너비 설정 ---
    ws.column_dimensions['A'].width = 11
    for i in range(2, len(checking_columns) + 1): ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 9

    wb.save(output)
    return output.getvalue()

def save_schedule(sheet_name, df_to_save, df_cum_to_save):
    with st.spinner(f"'{sheet_name}' 시트에 저장 중입니다..."):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
            
            # 1. 스케줄 시트 저장 (기존과 동일)
            try: 
                worksheet = sheet.worksheet(sheet_name)
            except WorksheetNotFound: 
                worksheet = sheet.add_worksheet(title=sheet_name, rows=100, cols=50)
            
            columns_to_save = st.session_state["df_schedule_original"].columns.tolist()
            df_to_save_final = pd.DataFrame(columns=columns_to_save)
            for col in columns_to_save:
                if col in df_to_save.columns:
                    df_to_save_final[col] = df_to_save[col]
                else:
                    df_to_save_final[col] = ''
            final_data = [columns_to_save] + df_to_save_final.fillna('').values.tolist()
            update_sheet_with_retry(worksheet, final_data)

            # 2. 익월 누적 시트 저장 (기존과 동일)
            if not df_cum_to_save.empty:
                current_month_dt_save = datetime.strptime(month_str, "%Y년 %m월")
                next_month_str_save = (current_month_dt_save + relativedelta(months=1)).strftime("%Y년 %-m월")
                version_s_save = " " + sheet_name.split(" 스케줄 ")[1] if " ver" in sheet_name else ""
                cum_sheet_name = f"{next_month_str_save} 누적{version_s_save}"

                try: 
                    ws_cum = sheet.worksheet(cum_sheet_name)
                except WorksheetNotFound: 
                    ws_cum = sheet.add_worksheet(title=cum_sheet_name, rows=100, cols=50)
                
                df_to_save_int = df_cum_to_save.copy()
                for col in df_to_save_int.columns[1:]:
                    df_to_save_int[col] = pd.to_numeric(df_to_save_int[col], errors='coerce').fillna(0).astype(int)

                cum_data = [df_to_save_int.columns.tolist()] + df_to_save_int.astype(str).values.tolist()
                update_sheet_with_retry(ws_cum, cum_data)

            # --- ▼▼▼ 여기가 핵심 수정 사항입니다 ▼▼▼ ---
            # 저장이 성공했으므로, 현재 앱의 '기준 데이터'를 방금 저장한 데이터로 업데이트합니다.
            # 이렇게 해야 다음번 rerun에서 변경사항이 없다고 올바르게 판단합니다.
            st.session_state.df_display_initial = df_to_save.copy()
            st.session_state.df_cumulative_next_display = df_cum_to_save.copy()
            # --- ▲▲▲ 여기까지 수정 ---

            st.session_state.save_successful = True
            st.session_state.last_saved_sheet_name = sheet_name
            
            st.success(f"🎉 스케줄과 익월 누적 데이터가 '{sheet_name}' 버전에 맞게 저장되었습니다.")
            time.sleep(1)
            st.cache_data.clear()
            st.rerun()

        except Exception as e: 
            st.error(f"Google Sheets 저장 중 오류 발생: {e}")

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
st.write(" ")
selected_sheet_name = st.selectbox("- 불러올 스케줄 버전을 선택하세요:", options=version_list, index=0, key="selected_sheet_name", on_change=on_version_change)
version_str = " " + selected_sheet_name.split(" 스케줄 ")[1] if " ver" in selected_sheet_name else ""

# --- 1. 새로고침 버튼 부분을 이 코드로 교체하세요 ---

# --- 새로고침 및 삭제 버튼 UI ---
col_refresh, col_delete, none = st.columns([2, 2, 2])

with col_refresh:
    if st.button("🔄 현재 버전 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        for key in ["data_loaded", "df_display_modified", "change_log", "apply_messages", "df_cumulative_next_display", "cumulative_editor", "closing_dates"]:
            if key in st.session_state: del st.session_state[key]
        st.rerun()

with col_delete:
    # 삭제는 위험한 작업이므로 확인 절차를 거칩니다.
    with st.expander("🗑️ 현재 버전 데이터 완전 삭제"):
        st.error("이 작업은 되돌릴 수 없습니다! Google Sheets에서 해당 버전의 스케줄과 누적 시트가 영구적으로 삭제됩니다.")
        
        # 최종 삭제 확인 버튼
        if st.button("네, 선택한 버전을 영구적으로 삭제합니다.", type="primary", use_container_width=True):
            delete_schedule_version(month_str, selected_sheet_name)

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

# 2. 선택된 버전을 바로 다운로드하는 버튼 생성
st.write(" ") # 버튼 위에 약간의 여백 추가

# 선택된 시트 이름에서 버전 정보 추출 (예: "ver2.0")
version_part = ""
schedule_keyword = "스케줄 "
if schedule_keyword in selected_sheet_name:
    version_part = selected_sheet_name.split(schedule_keyword, 1)[1]

display_version = f" {version_part}" if version_part else ""

# 데이터가 로드되었는지 확인 후 다운로드 버튼 표시
if "df_display_initial" in st.session_state:
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label=f"📥 스케줄{display_version} 다운로드",
            # create 함수에는 원본과 수정본 자리에 모두 원본 데이터를 넣어 변경사항 없음으로 처리
            data=create_final_schedule_excel(
                st.session_state.df_display_initial, st.session_state.df_display_initial, 
                st.session_state.df_cumulative_next_display, st.session_state.df_special, 
                st.session_state.df_requests, st.session_state.get("closing_dates", []), month_str
            ),
            file_name=f"{month_str} 스케줄{display_version}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )
    with col2:
        st.download_button(
            label=f"📥 스케줄{display_version} 다운로드 (배정 확인용)",
            data=create_checking_schedule_excel(
                st.session_state.df_display_initial, st.session_state.df_display_initial,
                st.session_state.df_cumulative_next_display, st.session_state.df_special, 
                st.session_state.df_requests, st.session_state.get("closing_dates", []), month_str
            ),
            file_name=f"{month_str} 스케줄{display_version} (배정 확인용).xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="secondary"
        )

# # 근무자 명단 수정
# st.divider()
# st.subheader("📋 스케줄 변경 요청 목록")
# if "df_schedule" not in st.session_state or st.session_state["df_schedule"].empty:
#     st.warning("⚠️ 스케줄 데이터가 로드되지 않았습니다. 새로고침 버튼을 눌러 데이터를 다시 로드해주세요.")
#     st.stop()

# # --- 표시할 데이터프레임 결정 ---
# # data_editor에 들어갈 데이터를 먼저 결정합니다. 이것이 현재 화면의 기준이 됩니다.
# df_to_display = st.session_state.get("df_schedule_md_modified", st.session_state.get("df_schedule_md_initial", pd.DataFrame()))

# # --- '스케줄 변경 요청 목록' 섹션 ---
# df_swaps_raw = st.session_state.get("df_swap_requests", pd.DataFrame())
# if not df_swaps_raw.empty:
#     cols_to_display = {'요청일시': '요청일시', '요청자': '요청자', '변경 요청': '변경 요청', '변경 요청한 스케줄': '변경 요청한 스케줄'}
#     existing_cols = [col for col in cols_to_display.keys() if col in df_swaps_raw.columns]
#     df_swaps_display = df_swaps_raw[existing_cols].rename(columns=cols_to_display)
#     if '변경 요청한 스케줄' in df_swaps_display.columns:
#         df_swaps_display['변경 요청한 스케줄'] = df_swaps_display['변경 요청한 스케줄'].apply(format_sheet_date_for_display)
#     st.dataframe(df_swaps_display, use_container_width=True, hide_index=True)

#     # >>>>>>>>> [핵심 수정] '일괄 적용' 전 상태일 때만 아래의 충돌 검사를 실행 <<<<<<<<<
#     if "df_schedule_md_modified" not in st.session_state:
#         # --- 충돌 경고 로직 ---
#         request_sources = []
#         request_destinations = []

#         schedule_df_to_check = df_to_display
#         target_year = int(month_str.split('년')[0])

#         for index, row in df_swaps_raw.iterrows():
#             change_request_str = str(row.get('변경 요청', '')).strip()
#             schedule_info_str = str(row.get('변경 요청한 스케줄', '')).strip()
            
#             if '➡️' in change_request_str and schedule_info_str:
#                 person_before, person_after = [p.strip() for p in change_request_str.split('➡️')]
                
#                 is_on_call_request = False
#                 date_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', schedule_info_str)
#                 if date_match:
#                     date_part, time_period = date_match.groups()
#                     if time_period == '오전':
#                         try:
#                             date_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
#                             formatted_date_in_df = f"{date_obj.month}월 {date_obj.day}일"
                            
#                             target_row = schedule_df_to_check[schedule_df_to_check['날짜'] == formatted_date_in_df]
                            
#                             if not target_row.empty:
#                                 on_call_person_of_the_day = str(target_row.iloc[0].get('오전당직(온콜)', '')).strip()
#                                 if person_before == on_call_person_of_the_day:
#                                     is_on_call_request = True
#                         except Exception:
#                             pass 
                
#                 if not is_on_call_request:
#                     request_sources.append(f"{person_before} - {schedule_info_str}")
                
#                 if date_match:
#                     date_part, time_period = date_match.groups()
#                     request_destinations.append((date_part, time_period, person_after))

#         # [검사 1: 출처 충돌]
#         source_counts = Counter(request_sources)
#         source_conflicts = [item for item, count in source_counts.items() if count > 1]
#         if source_conflicts:
#             st.warning(
#                 "⚠️ **요청 출처 충돌**: 동일한 근무에 대한 변경 요청이 2개 이상 있습니다. "
#                 "목록의 가장 위에 있는 요청이 먼저 반영되며, 이후 요청은 무시될 수 있습니다."
#             )
#             for conflict_item in source_conflicts:
#                 person, schedule = conflict_item.split(' - ', 1)
#                 formatted_schedule = format_sheet_date_for_display(schedule)
#                 st.info(f"- **'{person}'** 님의 **{formatted_schedule}** 근무 요청이 중복되었습니다.")

#         # [검사 2: 도착지 중복]
#         dest_counts = Counter(request_destinations)
#         dest_conflicts = [item for item, count in dest_counts.items() if count > 1]
#         if dest_conflicts:
#             st.warning(
#                 "⚠️ **요청 도착지 중복**: 한 사람이 같은 날, 같은 시간대에 여러 근무를 받게 되는 요청이 있습니다. "
#                 "이 경우, 먼저 처리되는 요청만 반영됩니다."
#             )
#             for date, period, person in dest_conflicts:
#                 formatted_date = format_sheet_date_for_display(f"{date} ({period})")
#                 st.info(f"- **'{person}'** 님이 **{formatted_date}** 근무에 중복으로 배정될 가능성이 있습니다.")
# else:
#     st.info("표시할 교환 요청 데이터가 없습니다.")

st.divider(); st.subheader("📅 스케줄표 수정")
df_to_display = st.session_state.get("df_display_modified", st.session_state.get("df_display_initial"))
# col1, col2 = st.columns(2)
# with col1:
#     if st.button("🔄 요청사항 일괄 적용"):
#         if not st.session_state.df_swaps.empty:
#             base_df = st.session_state.get("df_display_modified", st.session_state["df_display_initial"]); modified_df, messages = apply_schedule_swaps(base_df, st.session_state.df_swaps)
#             st.session_state["df_display_modified"] = modified_df; st.session_state["apply_messages"] = messages; st.rerun()
#         else: st.info("처리할 교환 요청이 없습니다.")
# with col2:
#     if st.button("⏪ 적용 취소", disabled="df_display_modified" not in st.session_state):
#         if "df_display_modified" in st.session_state: del st.session_state["df_display_modified"]
#         if "change_log" in st.session_state: del st.session_state["change_log"]
#         st.session_state["apply_messages"] = [('info', "변경사항이 취소되고 원본 스케줄로 돌아갑니다.")]; st.rerun()
# if "apply_messages" in st.session_state:
#     for msg_type, msg_text in st.session_state["apply_messages"]:
#         if msg_type == 'success': st.success(msg_text)
#         elif msg_type == 'warning': st.warning(msg_text)
#         elif msg_type == 'error': st.error(msg_text)
#         else: st.info(msg_text)
#     del st.session_state["apply_messages"]

edited_df = st.data_editor(df_to_display, use_container_width=True, key="schedule_editor", disabled=['날짜', '요일'])

st.caption("📝 스케줄표 변경사항 미리보기")
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

st.divider()
st.subheader("📊 익월 누적표 수정")

if "df_cumulative_next_display" in st.session_state and not st.session_state.df_cumulative_next_display.empty:
    df_cum = st.session_state.df_cumulative_next_display
    
    column_config = {
        # 첫 번째 열(이름)은 편집 불가
        df_cum.columns[0]: st.column_config.Column(disabled=True)
    }
    # 나머지 모든 열에 대해 음수를 허용하는 숫자 형식으로 지정
    for col in df_cum.columns[1:]:
        column_config[col] = st.column_config.NumberColumn()
    
    edited_cumulative_df = st.data_editor(
        df_cum,  # 변수로 받아서 사용
        hide_index=True,
        key="cumulative_editor",
        use_container_width=True,
        column_config=column_config # 수정된 설정 적용
    )
else:
    st.info("표시할 익월 누적 데이터가 없습니다. 해당 버전의 누적 시트가 존재하는지 확인해주세요.")
    edited_cumulative_df = pd.DataFrame()

# --- 누적표 변경사항 미리보기 (수정됨) ---
st.caption("📝 누적표 변경사항 미리보기")

base_cumulative_df = st.session_state.df_cumulative_next_display
cumulative_change_log = []

try:
    # 비교를 위한 임시 데이터프레임 생성
    base_numeric = base_cumulative_df.copy()
    edited_numeric = edited_cumulative_df.copy()

    # ✨ [핵심 수정] 첫 번째 열을 제외한 모든 열을 숫자(정수) 형식으로 통일합니다.
    cols_to_convert = base_numeric.columns[1:]
    for col in cols_to_convert:
        base_numeric[col] = pd.to_numeric(base_numeric[col], errors='coerce').fillna(0).astype(int)
        edited_numeric[col] = pd.to_numeric(edited_numeric[col], errors='coerce').fillna(0).astype(int)

    # 이제 숫자 형식으로 변환된 데이터프레임을 비교합니다.
    if not edited_numeric.equals(base_numeric):
        # numpy를 사용하여 차이가 나는 셀의 인덱스를 찾습니다.
        diff_indices = np.where(edited_numeric.ne(base_numeric))
        
        # 변경된 각 셀에 대한 로그를 생성합니다.
        for row_idx, col_idx in zip(diff_indices[0], diff_indices[1]):
            person_name = edited_numeric.iloc[row_idx, 0]
            item_name = edited_numeric.columns[col_idx]
            # 변환된 데이터프레임에서 값을 가져와 로그를 기록합니다.
            old_val = base_numeric.iloc[row_idx, col_idx]
            new_val = edited_numeric.iloc[row_idx, col_idx]

            cumulative_change_log.append({
                '이름': person_name,
                '항목': item_name,
                '변경 전': old_val,
                '변경 후': new_val
            })
except Exception as e:
    st.error(f"변경사항 비교 중 오류가 발생했습니다: {e}")

# 변경사항이 있으면 데이터프레임으로 표시하고, 없으면 메시지를 표시합니다.
if cumulative_change_log:
    st.dataframe(pd.DataFrame(cumulative_change_log), use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")

st.divider()

# --- 변경사항 유무 확인 ---
has_schedule_changes = not edited_df.equals(st.session_state.df_display_initial)
has_cumulative_changes = not edited_cumulative_df.equals(st.session_state.df_cumulative_next_display) if not edited_cumulative_df.empty else False
has_unsaved_changes = has_schedule_changes or has_cumulative_changes

# --- 변경사항 유무 확인 ---
has_schedule_changes = not edited_df.equals(st.session_state.df_display_initial)
has_cumulative_changes = not edited_cumulative_df.equals(st.session_state.df_cumulative_next_display) if not edited_cumulative_df.empty else False
has_unsaved_changes = has_schedule_changes or has_cumulative_changes

# --- UI 표시 로직 (수정됨) ---

# 1. [가장 먼저] '저장 완료' 상태를 확인하여 다운로드 UI를 표시합니다.
#    저장 직후이고, 새로운 변경사항이 없을 때만 이 부분이 나타납니다.
if st.session_state.get("save_successful", False) and not has_unsaved_changes:
    st.subheader("✅ 저장 완료! 엑셀 파일 다운로드")
    st.write("- 수정된 스케줄을 아래 버튼으로 다운로드하세요.")

    last_saved_sheet = st.session_state.get("last_saved_sheet_name", "스케줄")
    
    version_part = ""
    schedule_keyword = "스케줄 "
    if schedule_keyword in last_saved_sheet:
        version_part = last_saved_sheet.split(schedule_keyword, 1)[1]

    display_version = f" {version_part}" if version_part else ""

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label=f"📥 스케줄{display_version} 다운로드",
            data=create_final_schedule_excel(
                st.session_state.df_display_initial, edited_df, edited_cumulative_df,
                st.session_state.df_special, st.session_state.df_requests,
                st.session_state.get("closing_dates", []), month_str
            ),
            file_name=f"{month_str} 스케줄{display_version}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary"
        )
    with col2:
        st.download_button(
            label=f"📥 스케줄{display_version} 다운로드 (확인용)",
            data=create_checking_schedule_excel(
                st.session_state.df_display_initial, edited_df, edited_cumulative_df,
                st.session_state.df_special, st.session_state.df_requests,
                st.session_state.get("closing_dates", []), month_str
            ),
            file_name=f"{month_str} 스케줄{display_version} (확인용).xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="secondary"
        )

# 2. 그 외 모든 경우 (변경사항이 있거나, 아직 아무 작업도 하지 않은 초기 상태)
else:
    # '변경사항 저장' 헤더를 항상 표시
    st.subheader("💾 변경사항 저장")

    # 변경사항이 있을 때만 저장 옵션을 보여줍니다.
    if has_unsaved_changes:
        # 만약 '저장 완료' 상태였다면, 새로운 수정이 발생했으므로 해당 상태를 제거합니다.
        if "save_successful" in st.session_state:
            del st.session_state["save_successful"]

        st.write("수정한 스케줄표와 누적표를 저장하시려면 아래 옵션 중 선택해주세요.")
        st.warning("현재 버전 덮어쓰기를 선택하시면 이전 버전으로 돌아갈 수 없습니다.")

        latest_version_name = list(versions.keys())[0]
        latest_version_num = versions[latest_version_name]
        new_version_num = float(int(latest_version_num) + 1)
        new_sheet_name = f"{month_str} 스케줄 ver{new_version_num:.1f}"

        save_option = st.radio(
            "저장 옵션 선택",
            (f"현재 버전 - '{selected_sheet_name}' 덮어쓰기", f"다음 버전 - '{new_sheet_name}'(으)로 새로 저장하기"),
            key="save_option",
            label_visibility="collapsed"
        )

        if st.button("저장하기", use_container_width=True, type="primary"):
            df_to_save = edited_df.copy()
            sheet_name_to_save = selected_sheet_name if "덮어쓰기" in save_option else new_sheet_name
            save_schedule(sheet_name_to_save, df_to_save, edited_cumulative_df)
    
    # 변경사항이 없을 때 (초기 상태) 안내 메시지를 표시합니다.
    else:
        st.info("ℹ️ 저장할 변경사항이 없습니다.")