import streamlit as st
import pandas as pd
import calendar
from io import BytesIO
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import time
import io
import xlsxwriter
import platform
import openpyxl
import random
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
from openpyxl.comments import Comment
from datetime import datetime, timedelta
from collections import Counter
import menu
import re

st.set_page_config(page_title="스케줄 배정", page_icon="🗓️", layout="wide")

st.error("test 시트로 저장되며 실제 스케줄로 저장되지 않습니다.")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

random.seed(42)

def initialize_schedule_session_state():
    """스케줄 배정 페이지에서 사용할 모든 세션 상태 키를 초기화합니다."""
    keys_to_init = {
        "assigned": False,
        "output": None,
        "df_cumulative_next": pd.DataFrame(),
        "request_logs": [],
        # ▼▼▼ 아래 줄을 추가하세요 (이미 있다면 OK) ▼▼▼
        "swap_logs": [],
        "adjustment_logs": [],
        "oncall_logs": [],
        "assignment_results": None,
        "show_confirmation_warning": False,
        "latest_existing_version": None
    }
    for key, value in keys_to_init.items():
        if key not in st.session_state:
            st.session_state[key] = value

def get_sort_key(log_string):
    # '10월 1일'과 같은 패턴을 찾습니다.
    match = re.search(r'(\d{1,2}월 \d{1,2}일)', log_string)
    if match:
        date_str = match.group(1)
        try:
            # month_dt 변수에서 연도를 가져와 완전한 날짜 객체로 만듭니다.
            return datetime.strptime(f"{month_dt.year}년 {date_str}", "%Y년 %m월 %d일")
        except ValueError:
            # 날짜 변환에 실패하면 정렬 순서에 영향을 주지 않도록 맨 뒤로 보냅니다.
            return datetime.max
    # 로그에서 날짜를 찾지 못하면 맨 뒤로 보냅니다.
    return datetime.max

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]

from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.now(kst)
today = now.date()
month_dt = today.replace(day=1) + relativedelta(months=1)
month_str = month_dt.strftime("%Y년 %-m월")
month_str = "2025년 10월"
_, last_day = calendar.monthrange(month_dt.year, month_dt.month)
month_start = month_dt
month_end = month_dt.replace(day=last_day)

# Google Sheets 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        gc = gspread.authorize(credentials)
        return gc
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {e.response.status_code} - {e.response.text}")
        st.stop()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 인증 정보 로드 중 오류: {type(e).__name__} - {e}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 클라이언트 초기화 또는 인증 실패: {type(e).__name__} - {e}")
        st.stop()

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.clear()  # 시트를 완전히 비우고 새 데이터로 덮어씌움
            worksheet.update(data, "A1")
            return True
        except gspread.exceptions.APIError as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ API 요청이 지연되고 있습니다. {delay}초 후 재시도합니다... ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2  # 지수 백오프
            else:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (시트 업데이트): {str(e)}")
                st.stop()
        except Exception as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ 업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
                delay *= 2
            else:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"Google Sheets 업데이트 실패: {str(e)}")
                st.stop()
    return False

def find_latest_schedule_version(sheet, month_str):
    """주어진 월에 해당하는 스케줄 시트 중 가장 최신 버전을 찾습니다."""
    versions = {}
    # 'ver 1.0', 'ver1.0' 등 다양한 형식을 모두 찾도록 정규식 수정
    pattern = re.compile(f"^{re.escape(month_str)} 스케줄(?: ver\s*(\d+\.\d+))?$")
    
    for ws in sheet.worksheets():
        match = pattern.match(ws.title)
        if match:
            version_num_str = match.group(1) # ver 뒤의 숫자 부분 (예: '1.0')
            # 버전 넘버가 있으면 float으로 변환, 없으면 (기본 시트면) 1.0으로 처리
            version_num = float(version_num_str) if version_num_str else 1.0
            versions[ws.title] = version_num
    
    if not versions:
        return None

    # 가장 높은 버전 번호를 가진 시트의 이름을 반환
    return max(versions, key=versions.get)

def find_latest_cumulative_version(sheet, month_str):
    """주어진 월의 '다음 달'에 해당하는 누적 시트 중 가장 최신 버전을 찾습니다."""
    versions = {}
    pattern = re.compile(f"^{re.escape(month_str)} 누적(?: ver\s*(\d+\.\d+))?$")
    
    for ws in sheet.worksheets():
        match = pattern.match(ws.title)
        if match:
            version_num_str = match.group(1)
            version_num = float(version_num_str) if version_num_str else 1.0
            versions[ws.title] = version_num
            
    if not versions:
        return None # 최신 버전을 찾지 못하면 None 반환
        
    return max(versions, key=versions.get)

@st.cache_data(ttl=600, show_spinner="최신 데이터를 구글 시트에서 불러오는 중...")
def load_data_page5():
    url = st.secrets["google_sheet"]["url"]
    try:
        gc = get_gspread_client()
        if gc is None: st.stop()
        sheet = gc.open_by_url(url)
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

    # --- 마스터 시트 로드 ---
    try:
        ws1 = sheet.worksheet("마스터")
        df_master = pd.DataFrame(ws1.get_all_records())
        master_names_list = df_master["이름"].unique().tolist()
    except WorksheetNotFound:
        st.error("❌ '마스터' 시트를 찾을 수 없습니다."); st.stop()
    except Exception as e:
        st.error(f"'마스터' 시트 로드 실패: {e}"); st.stop()

    # --- 요청사항 시트 로드 ---
    try:
        ws2 = sheet.worksheet(f"{month_str} 요청")
        df_request = pd.DataFrame(ws2.get_all_records())
    except WorksheetNotFound:
        st.warning(f"⚠️ '{month_str} 요청' 시트를 찾을 수 없어 새로 생성합니다.")
        ws2 = sheet.add_worksheet(title=f"{month_str} 요청", rows=100, cols=3)
        ws2.append_row(["이름", "분류", "날짜정보"])
        df_request = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
    except Exception as e:
        st.error(f"'요청' 시트 로드 실패: {e}"); st.stop()

    # --- [핵심 수정] 최신 버전 누적 시트 로드 (원본 형태 그대로) ---
    df_cumulative = pd.DataFrame()
    # 다음 달 기준 최신 누적 시트 이름 찾기
    latest_cum_version_name = find_latest_cumulative_version(sheet, month_str)
    
    worksheet_to_load = None
    if latest_cum_version_name:
        try:
            worksheet_to_load = sheet.worksheet(latest_cum_version_name)
        except WorksheetNotFound:
            st.warning(f"'{latest_cum_version_name}' 시트를 찾지 못했습니다.")
    
    # 최신 버전이 없으면 이전 달의 최종 누적 시트(현재 월 기준)를 찾음
    if worksheet_to_load is None:
        try:
            prev_month_cum_sheet_name = f"{month_str} 누적"
            worksheet_to_load = sheet.worksheet(prev_month_cum_sheet_name)
        except WorksheetNotFound:
            st.warning(f"⚠️ '{prev_month_cum_sheet_name}' 시트도 찾을 수 없습니다. 빈 누적 테이블로 시작합니다.")

    if worksheet_to_load:
        all_values = worksheet_to_load.get_all_values()
        if all_values and len(all_values) > 1:
            headers = all_values[0]
            data = [row for row in all_values[1:] if any(cell.strip() for cell in row)]
            df_cumulative = pd.DataFrame(data, columns=headers)
        else:
            st.warning(f"'{worksheet_to_load.title}' 시트가 비어있습니다.")

    # 누적 시트가 비었거나 '항목' 열이 없으면 기본값으로 생성
    if df_cumulative.empty or '항목' not in df_cumulative.columns:
        default_cols = ["항목"] + master_names_list
        default_data = [
            ["오전누적"] + [0] * len(master_names_list), ["오후누적"] + [0] * len(master_names_list),
            ["오전당직 (목표)"] + [0] * len(master_names_list), ["오후당직 (목표)"] + [0] * len(master_names_list)
        ]
        df_cumulative = pd.DataFrame(default_data, columns=default_cols)

    # 숫자 열 변환
    for col in df_cumulative.columns:
        if col != '항목': # '항목' 열은 문자열이므로 제외
            df_cumulative[col] = pd.to_numeric(df_cumulative[col], errors='coerce').fillna(0).astype(int)

    # --- 근무/보충 테이블 생성 ---
    df_shift = generate_shift_table(df_master)
    df_supplement = generate_supplement_table(df_shift, master_names_list)
    
    return df_master, df_request, df_cumulative, df_shift, df_supplement

def generate_shift_table(df_master):
    def split_shift(row):
        shifts = []
        if row["근무여부"] == "오전 & 오후":
            shifts.extend([(row["이름"], row["주차"], row["요일"], "오전"), (row["이름"], row["주차"], row["요일"], "오후")])
        elif row["근무여부"] in ["오전", "오후"]:
            shifts.append((row["이름"], row["주차"], row["요일"], row["근무여부"]))
        return shifts

    shift_list = [shift for _, row in df_master.iterrows() for shift in split_shift(row)]
    df_split = pd.DataFrame(shift_list, columns=["이름", "주차", "요일", "시간대"])

    weekday_order = ["월", "화", "수", "목", "금"]
    time_slots = ["오전", "오후"]
    result = {}
    for day in weekday_order:
        for time in time_slots:
            key = f"{day} {time}"
            df_filtered = df_split[(df_split["요일"] == day) & (df_split["시간대"] == time)]
            every_week = df_filtered[df_filtered["주차"] == "매주"]["이름"].unique()
            specific_weeks = df_filtered[df_filtered["주차"] != "매주"]
            specific_week_dict = {name: sorted(specific_weeks[specific_weeks["이름"] == name]["주차"].tolist(), 
                                               key=lambda x: int(x.replace("주", ""))) 
                                  for name in specific_weeks["이름"].unique() if specific_weeks[specific_weeks["이름"] == name]["주차"].tolist()}
            employees = list(every_week) + [f"{name}({','.join(weeks)})" for name, weeks in specific_week_dict.items()]
            result[key] = ", ".join(employees) if employees else ""
    
    return pd.DataFrame(list(result.items()), columns=["시간대", "근무"])

def generate_supplement_table(df_result, names_in_master):
    supplement = []
    weekday_order = ["월", "화", "수", "목", "금"]
    shift_list = ["오전", "오후"]
    names_in_master = set(names_in_master)

    for day in weekday_order:
        for shift in shift_list:
            time_slot = f"{day} {shift}"
            row = df_result[df_result["시간대"] == time_slot].iloc[0]
            employees = set(emp.split("(")[0].strip() for emp in row["근무"].split(", ") if emp)
            supplement_employees = names_in_master - employees

            if shift == "오후":
                morning_slot = f"{day} 오전"
                morning_employees = set(df_result[df_result["시간대"] == morning_slot].iloc[0]["근무"].split(", ") 
                                        if morning_slot in df_result["시간대"].values else [])
                supplement_employees = {emp if emp in morning_employees else f"{emp}🔺" for emp in supplement_employees}

            supplement.append({"시간대": time_slot, "보충": ", ".join(sorted(supplement_employees)) if supplement_employees else ""})

    return pd.DataFrame(supplement)

def split_column_to_multiple(df, column_name, prefix):
    if column_name not in df.columns:
        st.warning(f"⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.stop()
        return df
    
    split_data = df[column_name].str.split(", ", expand=True)
    
    max_cols = split_data.shape[1]
    
    new_columns = [f"{prefix}{i+1}" for i in range(max_cols)]
    split_data.columns = new_columns
    
    df = df.drop(columns=[column_name])
    
    df = pd.concat([df, split_data], axis=1)

    return df

def append_transposed_cumulative(worksheet, df_cumulative, style_args):
    if df_cumulative.empty:
        return

    start_row = worksheet.max_row + 3

    df_transposed = df_cumulative.set_index(df_cumulative.columns[0]).T
    df_transposed.reset_index(inplace=True)
    df_transposed.rename(columns={'index': '항목'}, inplace=True)

    header_row = df_transposed.columns.tolist()
    for c_idx, value in enumerate(header_row, 1):
        cell = worksheet.cell(row=start_row, column=c_idx, value=value)
        cell.font = style_args['font']
        cell.fill = PatternFill(start_color='808080', end_color='808080', fill_type='solid') 
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = style_args['border']

    for r_idx, row_data in enumerate(df_transposed.itertuples(index=False), start_row + 1):
        for c_idx, value in enumerate(row_data, 1):
            cell = worksheet.cell(row=r_idx, column=c_idx, value=value)
            cell.font = style_args['bold_font'] if c_idx == 1 else style_args['font']
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = style_args['border']

    worksheet.column_dimensions[openpyxl.utils.get_column_letter(1)].width = 11
    for i in range(2, len(header_row) + 1):
        worksheet.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 9

def append_summary_table_to_excel(worksheet, summary_df, style_args):
    if summary_df.empty:
        return

    fills = {
        'header': PatternFill(start_color='E7E6E6', end_color='E7E6E6', fill_type='solid'),
        'yellow': PatternFill(start_color='FFF296', end_color='FFF296', fill_type='solid'),
        'pink': PatternFill(start_color='FFC8CD', end_color='FFC8CD', fill_type='solid'),
        'green': PatternFill(start_color='C6E0B4', end_color='C6E0B4', fill_type='solid'),
        'dark_green': PatternFill(start_color='82C4B5', end_color='82C4B5', fill_type='solid'),
        'blue': PatternFill(start_color='B8CCE4', end_color='B8CCE4', fill_type='solid'),
        'orange': PatternFill(start_color='FCE4D6', end_color='FCE4D6', fill_type='solid')
    }
    
    start_row = worksheet.max_row + 3
    thin_border = style_args['border'] 

    # 헤더 쓰기
    for c_idx, value in enumerate(summary_df.columns.tolist(), 1):
        cell = worksheet.cell(row=start_row, column=c_idx, value=value)
        cell.fill = fills['header']
        cell.font = style_args['bold_font']
        cell.border = thin_border
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
            
            if c_idx == 1 and label in ["오전보충", "임시보충", "오후보충", "온콜검사"]:
                fill_color = fills['yellow']
            
            if fill_color:
                cell.fill = fill_color

    start_col = 1
    end_col = len(summary_df.columns)
    labels = summary_df.iloc[:, 0].tolist()

    apply_outer_border(worksheet, start_row, start_row, start_col, end_col)
    
    apply_outer_border(worksheet, start_row, start_row + len(labels), start_col, start_col)

    block1_start = start_row + 1 + labels.index("오전보충")
    block1_end = start_row + 1 + labels.index("오전누적")
    apply_outer_border(worksheet, block1_start, block1_end, start_col, end_col)

    block2_start = start_row + 1 + labels.index("오후보충")
    block2_end = start_row + 1 + labels.index("오후누적")
    apply_outer_border(worksheet, block2_start, block2_end, start_col, end_col)
    
    block3_start = start_row + 1 + labels.index("오전당직 (목표)")
    block3_end = start_row + 1 + labels.index("오후당직 (목표)")
    apply_outer_border(worksheet, block3_start, block3_end, start_col, end_col)

    legend_start_row = worksheet.max_row + 3 

    legend_data = [
        ('A9D08E', '대체 보충'),
        ('FFF28F', '보충'),
        ('95B3D7', '대체 휴근'),
        ('B1A0C7', '휴근'),
        ('DA9694', '휴가/학회')
    ]

    for i, (hex_color, description) in enumerate(legend_data):
        current_row = legend_start_row + i
        
        color_cell = worksheet.cell(row=current_row, column=1)
        color_cell.fill = PatternFill(start_color=hex_color, end_color=hex_color, fill_type='solid')
        color_cell.border = thin_border

        desc_cell = worksheet.cell(row=current_row, column=2, value=description)
        desc_cell.font = style_args['font']
        desc_cell.border = thin_border
        desc_cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)

    worksheet.column_dimensions[openpyxl.utils.get_column_letter(1)].width = 11
    for i in range(2, len(summary_df.columns) + 1):
        worksheet.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 9

def apply_outer_border(worksheet, start_row, end_row, start_col, end_col):
    medium_side = Side(style='medium') 

    for r in range(start_row, end_row + 1):
        for c in range(start_col, end_col + 1):
            cell = worksheet.cell(row=r, column=c)
            
            top = cell.border.top
            left = cell.border.left
            bottom = cell.border.bottom
            right = cell.border.right

            if r == start_row: top = medium_side
            if r == end_row: bottom = medium_side
            if c == start_col: left = medium_side
            if c == end_col: right = medium_side
            
            cell.border = Border(top=top, left=left, bottom=bottom, right=right)

def append_final_summary_to_excel(worksheet, df_final_summary, style_args):
    if df_final_summary.empty: return
    start_row = worksheet.max_row + 3
    
    worksheet.append(df_final_summary.columns.tolist())
    for cell in worksheet[start_row]:
        cell.font = style_args['bold_font']
        cell.border = style_args['border']
        cell.alignment = Alignment(horizontal='center', vertical='center')

    for _, row in df_final_summary.iterrows():
        worksheet.append(row.tolist())
    
    for row in worksheet.iter_rows(min_row=start_row + 1, max_row=worksheet.max_row):
        for cell in row:
            cell.font = style_args['font']
            cell.border = style_args['border']
            cell.alignment = Alignment(horizontal='center', vertical='center')

def replace_adjustments(df):
    """
    [수정됨] 동일 인물 + 동일 주차에서 추가보충/추가제외 -> 대체보충/대체제외로 변경합니다.
    추가보충/추가제외가 1:N 또는 N:1일 경우, 날짜가 빠른 순서대로 1:1 매칭합니다.
    """
    color_priority = {'🟠 주황색': 0, '🟢 초록색': 1, '🟡 노란색': 2, '기본': 3, '🔴 빨간색': 4, '🔵 파란색': 5, '🟣 보라색': 6, '특수근무색': -1}

    # 1. '추가보충' 또는 '추가제외'인 행만 필터링 (주차 정보 포함 필수)
    adjustments_df = df[df['상태'].isin(['추가보충', '추가제외'])].copy()
    
    # 2. 그룹별로 순차 매칭을 위해 날짜순으로 정렬
    adjustments_df.sort_values(by='날짜', inplace=True)

    # 3. 그룹별로 순차 매칭 수행
    for (worker, week, shift), group in adjustments_df.groupby(['근무자', '주차', '시간대']):
        
        # 날짜 순으로 정렬된 추가보충 및 추가제외 레코드 리스트를 얻습니다.
        bochung_records = group[group['상태'] == '추가보충'].to_dict('records')
        jeoe_records = group[group['상태'] == '추가제외'].to_dict('records')

        # 대체 가능 횟수 (min(추가보충 수, 추가제외 수))
        num_swaps = min(len(bochung_records), len(jeoe_records))

        # 4. 최대 가능 횟수만큼 순차적으로 짝짓기
        for i in range(num_swaps):
            bochung = bochung_records[i]
            jeoe = jeoe_records[i]
            
            # 매칭 날짜를 YYYY-MM-DD 형식으로 가져옵니다.
            bochung_date_str = bochung['날짜']
            jeoe_date_str = jeoe['날짜']
            
            # 5. 원본 df에 상태 업데이트 (매칭된 두 레코드에 대해)
            
            # 대체보충으로 변경 (추가보충이었던 레코드)
            bochung_mask = (df['날짜'] == bochung_date_str) & \
                           (df['시간대'] == shift) & \
                           (df['근무자'] == worker) & \
                           (df['상태'] == '추가보충')
            
            df.loc[bochung_mask, '상태'] = '대체보충'
            df.loc[bochung_mask, '색상'] = '🟢 초록색'
            df.loc[bochung_mask, '메모'] = f"{pd.to_datetime(jeoe_date_str).strftime('%-m월 %-d일')}일과 대체"

            # 대체제외로 변경 (추가제외였던 레코드)
            jeoe_mask = (df['날짜'] == jeoe_date_str) & \
                        (df['시간대'] == shift) & \
                        (df['근무자'] == worker) & \
                        (df['상태'] == '추가제외')
            
            df.loc[jeoe_mask, '상태'] = '대체제외'
            df.loc[jeoe_mask, '색상'] = '🔵 파란색'
            df.loc[jeoe_mask, '메모'] = f"{pd.to_datetime(bochung_date_str).strftime('%-m월 %-d일')}일과 대체"
            
    # 6. 최종 결과를 반환합니다. (호출한 곳에서 최종 중복 제거 필요)
    return df

st.header("🗓️ 스케줄 배정", divider='rainbow')
st.write("- 먼저 새로고침 버튼으로 최신 데이터를 불러온 뒤, 배정을 진행해주세요.")

if st.button("🔄 새로고침 (R)"):
    try:
        st.cache_data.clear()
        st.cache_resource.clear()

        # ▼▼▼ [핵심 수정] 페이지에 필요한 데이터만 선택적으로 삭제합니다 ▼▼▼
        keys_to_clear = [
            "assigned", "output", "df_cumulative_next", "request_logs", 
            "swap_logs", "adjustment_logs", "oncall_logs", "assignment_results",
            "show_confirmation_warning", "latest_existing_version",
            "data_loaded", "df_master", "df_request", "df_cumulative", 
            "df_shift", "df_supplement", "edited_df_cumulative"
        ]
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        # --- 수정 끝 ---
        
        st.success("데이터가 새로고침되었습니다. 페이지를 다시 로드합니다.")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"새로고침 중 오류 발생: {type(e).__name__} - {e}")
        st.stop()

# get_adjustment 함수 정의 (이전 수정사항 유지)
def get_adjustment(name, time_slot, df_final_unique=None):
    """근무자의 시간대별 보충/제외 횟수 차이를 계산합니다."""
    if df_final_unique is None:
        return 0
    adjustments = df_final_unique[
        (df_final_unique['근무자'] == name) &
        (df_final_unique['시간대'] == time_slot) &
        (df_final_unique['상태'].isin(['추가보충', '추가제외']))
    ]
    if adjustments.empty:
        return 0
    count = (
        len(adjustments[adjustments['상태'] == '추가보충']) -
        len(adjustments[adjustments['상태'] == '추가제외'])
    )
    return count

def display_cumulative_table(df_cumulative):
    if df_cumulative.empty:
        st.warning("⚠️ 누적 테이블 데이터가 비어 있습니다.")
        return
    if '항목' not in df_cumulative.columns:
        st.error(f"누적 테이블에 '항목' 열이 없습니다. 열: {df_cumulative.columns.tolist()}")
        st.stop()

def display_pivoted_summary_table(df_summary):
    if df_summary.empty:
        st.warning("⚠️ 요약 테이블 데이터가 비어 있습니다.")
        return
    st.dataframe(df_summary, use_container_width=True, hide_index=True)

# 기존 build_summary_table 함수를 아래 코드로 전체 교체하세요.

def build_summary_table(df_cumulative, all_names, next_month_str, df_final_unique=None):
    """
    [수정됨] 최종 요약 테이블을 생성합니다.
    누적 값을 직접 계산하여 합계가 항상 일치하도록 보장합니다.
    """
    summary_data = {name: [""] * 11 for name in all_names}
    df_summary = pd.DataFrame(summary_data)

    row_labels = [
        "오전보충", "임시보충", "오전합계", "오전누적",
        "오후보충", "온콜검사", "오후합계", "오후누적",
        "오전당직 (목표)", "오전당직 (배정)", "오후당직 (목표)"
    ]
    df_summary.index = row_labels

    df_cum_indexed = df_cumulative.set_index('항목')
    
    # 실제 배정된 당직 횟수 계산
    actual_oncall_counts = Counter(df_final_unique[df_final_unique['시간대'] == '오전당직']['근무자']) if df_final_unique is not None else Counter()

    for name in all_names:
        if name not in df_cum_indexed.columns:
            # 누적 테이블에 없는 신규 인원이면 모든 값을 0으로 초기화
            df_cum_indexed[name] = 0

        # --- 합계 및 변동 값 가져오기 ---
        am_hapgye = int(df_cum_indexed.loc['오전누적', name])
        pm_hapgye = int(df_cum_indexed.loc['오후누적', name])
        am_bochung = get_adjustment(name, '오전', df_final_unique)
        pm_bochung = get_adjustment(name, '오후', df_final_unique)
        
        oncall_target = int(df_cum_indexed.loc['오전당직 (목표)', name])
        pm_oncall_target = int(df_cum_indexed.loc['오후당직 (목표)', name])

        # --- 테이블에 값 채우기 및 누적 값 직접 계산 ---
        df_summary.at["오전보충", name] = am_bochung
        df_summary.at["오전합계", name] = am_hapgye
        df_summary.at["오전누적", name] = am_hapgye + am_bochung  # [핵심] 직접 계산

        df_summary.at["오후보충", name] = pm_bochung
        df_summary.at["오후합계", name] = pm_hapgye
        df_summary.at["오후누적", name] = pm_hapgye + pm_bochung  # [핵심] 직접 계산
        
        df_summary.at["오전당직 (목표)", name] = oncall_target
        df_summary.at["오전당직 (배정)", name] = actual_oncall_counts.get(name, 0)
        df_summary.at["오후당직 (목표)", name] = pm_oncall_target

    df_summary.reset_index(inplace=True)
    df_summary.rename(columns={'index': next_month_str}, inplace=True)
    return df_summary

def build_final_summary_table(df_cumulative, df_final_unique, all_names):
    summary_data = []
    
    adjustments = df_final_unique[df_final_unique['상태'].isin(['추가보충', '추가제외'])]
    am_adjust = adjustments[adjustments['시간대'] == '오전'].groupby('근무자')['상태'].apply(lambda x: (x == '추가보충').sum() - (x == '추가제외').sum()).to_dict()
    pm_adjust = adjustments[adjustments['시간대'] == '오후'].groupby('근무자')['상태'].apply(lambda x: (x == '추가보충').sum() - (x == '추가제외').sum()).to_dict()
    
    oncall_counts = df_final_unique_sorted[df_final_unique_sorted['시간대'] == '오전당직']['근무자'].value_counts().to_dict() # 여기도 _sorted로 변경

    before_dict = df_cumulative.set_index('항목').T.to_dict()

    for name in all_names:
        b = before_dict.get(name, {})
        am_change = am_adjust.get(name, 0)
        pm_change = pm_adjust.get(name, 0)
        
        summary_data.append({
            '이름': name,
            '오전누적 (시작)': b.get('오전누적', 0),
            '오전누적 (변동)': am_change,
            '오전누적 (최종)': b.get('오전누적', 0) + am_change,
            '오후누적 (시작)': b.get('오후누적', 0),
            '오후누적 (변동)': pm_change,
            '오후누적 (최종)': b.get('오후누적', 0) + pm_change,
            '오전당직 (목표)': b.get('오전당직 (목표)', 0),
            '오전당직 (최종)': oncall_counts.get(name, 0),
            '오후당직 (목표)': b.get('오후당직 (목표)', 0),
        })
        
    return pd.DataFrame(summary_data)

df_master, df_request, df_cumulative, df_shift, df_supplement = load_data_page5()

# 세션 상태에 데이터 저장
st.session_state["df_master"] = df_master
st.session_state["df_request"] = df_request
if "df_cumulative" not in st.session_state or st.session_state["df_cumulative"].empty:
    st.session_state["df_cumulative"] = df_cumulative
st.session_state["df_shift"] = df_shift
st.session_state["df_supplement"] = df_supplement

# 'edited_df_cumulative'가 없거나 비어있을 경우에만 초기화
if "edited_df_cumulative" not in st.session_state or st.session_state["edited_df_cumulative"].empty:
    st.session_state["edited_df_cumulative"] = df_cumulative.copy()

if '근무' not in df_shift.columns or '보충' not in df_supplement.columns:
    st.warning("⚠️ 데이터를 불러오는 데 문제가 발생했습니다. 새로고침 버튼을 눌러 다시 시도해주세요.")
    st.stop()

st.divider()
st.subheader(f"✨ {month_str} 테이블 종합")
st.write("- 당월 근무자와 보충 가능 인원을 확인하거나, 누적 테이블을 수정할 수 있습니다.\n- 보충 테이블에서 '🔺' 표시가 있는 인원은 해당일 오전 근무가 없으므로, 보충 시 오전·오후 모두 보충되어야 함을 의미합니다.")
with st.expander("📁 테이블 펼쳐보기"):

    df_shift_processed = split_column_to_multiple(df_shift, "근무", "근무")
    df_supplement_processed = split_column_to_multiple(df_supplement, "보충", "보충")

    def excel_download(name, sheet1, name1, sheet2, name2, sheet3, name3, sheet4, name4):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            sheet1.to_excel(writer, sheet_name=name1, index=False)
            sheet2.to_excel(writer, sheet_name=name2, index=False)
            sheet3.to_excel(writer, sheet_name=name3, index=False)
            sheet4.to_excel(writer, sheet_name=name4, index=False)
        
        excel_data = output.getvalue()
        return excel_data

    st.write(" ")
    st.markdown("**✅ 근무 테이블**")
    st.dataframe(df_shift, use_container_width=True, hide_index=True)

    st.markdown("**☑️ 보충 테이블**")
    st.dataframe(df_supplement, use_container_width=True, hide_index=True)

    st.markdown("**➕ 누적 테이블**")
    st.write("- 변동이 있는 경우, 수정 가능합니다.")
    # 1. 표시할 행 이름 정의 및 원본 데이터에서 필터링
    rows_to_display = ["오전누적", "오후누적", "오전당직 (목표)", "오후당직 (목표)"]
    df_cumulative_full = st.session_state["df_cumulative"]
    df_to_edit = df_cumulative_full[df_cumulative_full['항목'].isin(rows_to_display)]

    # 2. 필터링된 데이터를 data_editor에 표시 (display_cumulative_table 호출 제거)
    edited_partial_df = st.data_editor(
        df_to_edit,
        use_container_width=True,
        hide_index=True,
        column_config={"항목": {"editable": False}},
        key="cumulative_editor" # 고유 키 부여
    )

    # 3. 저장 버튼 로직
    if st.button("💾 누적 테이블 수정사항 저장"):
        try:
            # 원본 전체 데이터의 복사본 생성
            df_updated_full = st.session_state["df_cumulative"].copy()

            # '항목'을 인덱스로 설정하여 정확한 위치에 업데이트 준비
            df_updated_full.set_index('항목', inplace=True)
            edited_partial_df.set_index('항목', inplace=True)

            # 수정된 내용으로 원본 업데이트
            df_updated_full.update(edited_partial_df)
            df_updated_full.reset_index(inplace=True) # 인덱스를 다시 열로 복원

            # 세션 상태 및 Google Sheet 업데이트 (이제 df_updated_full이 최신 전체 데이터임)
            st.session_state["df_cumulative"] = df_updated_full.copy()
            st.session_state["edited_df_cumulative"] = df_updated_full.copy()
            
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet4 = sheet.worksheet(f"{month_str} 누적") # 주의: 이 로직은 최신 버전을 찾지 않음
            update_data = [df_updated_full.columns.tolist()] + df_updated_full.values.tolist()
            
            if update_sheet_with_retry(worksheet4, update_data):
                st.success(f"{month_str} 누적 테이블이 성공적으로 저장되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("누적 테이블 저장 실패")
                st.stop()
        except Exception as e:
            st.error(f"누적 테이블 저장 중 오류 발생: {str(e)}")

    # 4. 다운로드 버튼 로직
    with st.container():
        excel_data = excel_download(
            name=f"{month_str} 테이블 종합",
            sheet1=df_shift_processed, name1="근무 테이블",
            sheet2=df_supplement_processed, name2="보충 테이블",
            sheet3=df_request, name3="요청사항 테이블",
            # 수정된 전체 데이터를 다운로드에 사용
            sheet4=st.session_state["edited_df_cumulative"], name4="누적 테이블"
        )
        st.download_button(
            label="📥 상단 테이블 다운로드",
            data=excel_data,
            file_name=f"{month_str} 테이블 종합.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.divider()
st.subheader("📋 요청사항 관리")
st.write("- 명단 및 마스터에 등록되지 않은 인원 중 스케줄 배정이 필요한 경우, 관리자가 이름을 수기로 입력하여 요청사항을 추가해야 합니다.\n- '꼭 근무'로 요청된 사항은 해당 인원이 마스터가 없거나 모두 '근무없음' 상태더라도 반드시 배정됩니다.")

if df_request["분류"].nunique() == 1 and df_request["분류"].iloc[0] == '요청 없음':
    st.warning(f"⚠️ 아직까지 {month_str}에 작성된 요청사항이 없습니다.")

요청분류 = ["휴가", "학회", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
st.dataframe(df_request.reset_index(drop=True), use_container_width=True, hide_index=True, height=300)

def add_request_callback():
    날짜정보 = ""
    분류 = st.session_state.request_category_select
    
    if 분류 != "요청 없음":
        방식 = st.session_state.method_select
        if 방식 == "일자 선택":
            날짜 = st.session_state.get("date_multiselect", [])
            if 날짜: 날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜])
        elif 방식 == "기간 선택":
            날짜범위 = st.session_state.get("date_range", ())
            if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                시작, 종료 = 날짜범위
                날짜정보 = f"{시작.strftime('%Y-%m-%d')} ~ {종료.strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.session_state.get("week_select", [])
            선택요일 = st.session_state.get("day_select", [])
            if 선택주차 or 선택요일:
                c = calendar.Calendar(firstweekday=6)
                month_calendar = c.monthdatescalendar(month_dt.year, month_dt.month)
                요일_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
                선택된_요일_인덱스 = [요일_map[요일] for 요일 in 선택요일] if 선택요일 else list(요일_map.values())
                날짜목록 = []
                for i, week in enumerate(month_calendar):
                    주차_이름 = ""
                    if i == 0: 주차_이름 = "첫째주"
                    elif i == 1: 주차_이름 = "둘째주"
                    elif i == 2: 주차_이름 = "셋째주"
                    elif i == 3: 주차_이름 = "넷째주"
                    elif i == 4: 주차_이름 = "다섯째주"
                    if not 선택주차 or "매주" in 선택주차 or 주차_이름 in 선택주차:
                        for date_obj in week:
                            if date_obj.month == month_dt.month and date_obj.weekday() in 선택된_요일_인덱스:
                                날짜목록.append(date_obj.strftime("%Y-%m-%d"))
                if 날짜목록:
                    날짜정보 = ", ".join(sorted(list(set(날짜목록))))
                else:
                    add_placeholder.warning(f"⚠️ {month_str}에는 해당 주차/요일의 날짜가 없습니다. 다른 조합을 선택해주세요.")
                    return

    이름 = st.session_state.get("add_employee_select", "")
    이름_수기 = st.session_state.get("new_employee_input", "")
    최종_이름 = 이름 if 이름 else 이름_수기

    if not 최종_이름 or (분류 != "요청 없음" and not 날짜정보):
        add_placeholder.warning("⚠️ 이름과 날짜를 올바르게 선택/입력해주세요.")
        return

    with add_placeholder.container():
        with st.spinner("요청사항 확인 및 저장 중..."):
            try:
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet2 = sheet.worksheet(f"{month_str} 요청")
                all_requests = worksheet2.get_all_records()
                df_request_live = pd.DataFrame(all_requests)

                is_duplicate = not df_request_live[
                    (df_request_live["이름"] == 최종_이름) &
                    (df_request_live["분류"] == 분류) &
                    (df_request_live["날짜정보"] == 날짜정보)
                ].empty

                if is_duplicate:
                    st.error("⚠️ 이미 존재하는 요청사항입니다.")
                    time.sleep(1.5)
                    st.rerun()
                    return

                rows_to_delete = []
                for i, req in enumerate(all_requests):
                    if req.get("이름") == 최종_이름:
                        if 분류 == "요청 없음" or req.get("분류") == "요청 없음":
                            rows_to_delete.append(i + 2)
                
                if rows_to_delete:
                    for row_idx in sorted(rows_to_delete, reverse=True):
                        worksheet2.delete_rows(row_idx)

                worksheet2.append_row([최종_이름, 분류, 날짜정보 if 분류 != "요청 없음" else ""])
                
                st.success("요청사항이 저장되었습니다.")
                time.sleep(1.5)
                
                st.session_state.add_employee_select = None
                st.session_state.new_employee_input = ""
                st.session_state.request_category_select = "휴가"
                st.session_state.method_select = "일자 선택"
                st.session_state.date_multiselect = []
                st.session_state.date_range = (month_start, month_start + timedelta(days=1))
                st.session_state.week_select = []
                st.session_state.day_select = []
                
                st.rerun()

            except Exception as e:
                st.error(f"요청사항 추가 중 오류 발생: {e}")

입력_모드 = st.selectbox("입력 모드", ["이름 선택", "이름 수기 입력"], key="input_mode_select")
col1, col2, col3, col4 = st.columns([1, 1, 1, 1.5])
with col1:
    if 입력_모드 == "이름 선택":
        sorted_names = sorted(df_master["이름"].unique()) if not df_master.empty and "이름" in df_master.columns else []
        st.selectbox("이름 선택", sorted_names, key="add_employee_select")
    else:
        이름_수기 = st.text_input("이름 입력", help="명단에 없는 새로운 인원에 대한 요청을 추가하려면 입력", key="new_employee_input")
        if 이름_수기 and 이름_수기 not in st.session_state.get("df_map", pd.DataFrame()).get("이름", pd.Series()).values:
            st.warning(f"{이름_수기}은(는) 매핑 시트에 존재하지 않습니다. 먼저 명단 관리 페이지에서 추가해주세요.")
            st.stop()
with col2:
    st.selectbox("요청 분류", 요청분류, key="request_category_select")
if st.session_state.get("request_category_select") != "요청 없음":
    with col3:
        st.selectbox("날짜 선택 방식", ["일자 선택", "기간 선택", "주/요일 선택"], key="method_select")
    with col4:
        if st.session_state.method_select == "일자 선택":
            weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
            def format_date(date_obj):
                return f"{date_obj.strftime('%-m월 %-d일')} ({weekday_map[date_obj.weekday()]})"
            날짜_목록 = [month_start + timedelta(days=i) for i in range((month_end - month_start).days + 1)]
            st.multiselect("요청 일자", 날짜_목록, format_func=format_date, key="date_multiselect")
        elif st.session_state.method_select == "기간 선택":
            st.date_input("요청 기간", value=(month_start, month_start + timedelta(days=1)), min_value=month_start, max_value=month_end, key="date_range")
        elif st.session_state.method_select == "주/요일 선택":
            st.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"], key="week_select")
            st.multiselect("요일 선택", ["월", "화", "수", "목", "금"], key="day_select")

if st.session_state.get("request_category_select") == "요청 없음":
    st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

st.button("📅 추가", on_click=add_request_callback)

add_placeholder = st.empty()

st.write(" ")
st.markdown("**🔴 요청사항 삭제**")
if not df_request.empty:
    col0, col1 = st.columns([1, 2])
    with col0:
        sorted_names = sorted(df_request["이름"].unique()) if not df_request.empty else []
        selected_employee_id2 = st.selectbox("이름 선택", sorted_names, key="delete_request_employee_select")
    with col1:
        df_employee2 = df_request[df_request["이름"] == selected_employee_id2]
        df_employee2_filtered = df_employee2[df_employee2["분류"] != "요청 없음"]
        if not df_employee2_filtered.empty:
            selected_rows = st.multiselect(
                "요청사항 선택",
                df_employee2_filtered.index,
                format_func=lambda x: f"{df_employee2_filtered.loc[x, '분류']} - {df_employee2_filtered.loc[x, '날짜정보']}",
                key="delete_request_select"
            )
        else:
            st.info("📍 선택한 이름에 대한 요청사항이 없습니다.")
            selected_rows = []
else:
    st.info("📍 당월 요청사항 없음")
    selected_rows = []

if st.button("📅 삭제"):
    with st.spinner("요청을 삭제하는 중입니다..."):
        try:
            if selected_rows:
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet2 = sheet.worksheet(f"{month_str} 요청")
                all_requests = worksheet2.get_all_records()
                
                items_to_delete_set = set()
                df_request_original = st.session_state["df_request"]
                for index in selected_rows:
                    row = df_request_original.loc[index]
                    items_to_delete_set.add((row['이름'], row['분류'], row['날짜정보']))

                rows_to_delete_indices = []
                for i, record in enumerate(all_requests):
                    record_tuple = (record.get('이름'), record.get('분류'), record.get('날짜정보'))
                    if record_tuple in items_to_delete_set:
                        rows_to_delete_indices.append(i + 2)
                
                if rows_to_delete_indices:
                    for row_idx in sorted(rows_to_delete_indices, reverse=True):
                        worksheet2.delete_rows(row_idx)

                remaining_requests = worksheet2.findall(selected_employee_id2)
                if not remaining_requests:
                    worksheet2.append_row([selected_employee_id2, "요청 없음", ""])
                
                st.success("요청사항이 삭제되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.warning("삭제할 요청사항을 선택해주세요.")
        except Exception as e:
            st.error(f"요청사항 삭제 중 오류 발생: {e}")

# 근무 배정 로직
current_cumulative = {'오전': {}, '오후': {}}

_, last_day = calendar.monthrange(today.year, today.month)
next_month = today.replace(day=1) + relativedelta(months=1)
dates = pd.date_range(start=next_month, end=next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1]))
weekdays = [d for d in dates if d.weekday() < 5]
week_numbers = {d.to_pydatetime().date(): (d.day - 1) // 7 + 1 for d in dates}
day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금'}
df_final = pd.DataFrame(columns=['날짜', '요일', '주차', '시간대', '근무자', '상태', '메모', '색상'])

st.divider()
st.subheader(f"✨ {month_str} 스케줄 배정 수행")

def parse_date_range(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str) or date_str.strip() == '':
        return []
    date_str = date_str.strip()
    result = []
    if ',' in date_str:
        for single_date in date_str.split(','):
            single_date = single_date.strip()
            try:
                parsed_date = datetime.strptime(single_date, '%Y-%m-%d')
                if parsed_date.weekday() < 5:
                    result.append(single_date)
            except ValueError:
                pass
        return result
    if '~' in date_str:
        try:
            start_date, end_date = date_str.split('~')
            start_date = start_date.strip()
            end_date = end_date.strip()
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            date_list = pd.date_range(start=start, end=end)
            return [d.strftime('%Y-%m-%d') for d in date_list if d.weekday() < 5]
        except ValueError as e:
            pass
            return []
    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        if parsed_date.weekday() < 5:
            return [date_str]
        return []
    except ValueError:
        pass
        return []

def update_worker_status(df, date_str, time_slot, worker, status, memo, color, day_map, week_numbers):
    date_obj = pd.to_datetime(date_str)
    worker_stripped = worker.strip()
    
    existing_indices = df.index[
        (df['날짜'] == date_str) &
        (df['시간대'] == time_slot) &
        (df['근무자'] == worker_stripped)
    ].tolist()

    if existing_indices:
        df.loc[existing_indices, ['상태', '메모', '색상']] = [status, memo, color]
    else:
        new_row = pd.DataFrame([{
            '날짜': date_str,
            '요일': day_map.get(date_obj.weekday(), ''),
            '주차': week_numbers.get(date_obj.date(), 0),
            '시간대': time_slot,
            '근무자': worker_stripped,
            '상태': status,
            '메모': memo,
            '색상': color
        }])
        df = pd.concat([df, new_row], ignore_index=True)
    return df

# 아래 코드로 함수 전체를 교체하세요.
def sync_am_to_pm_exclusions(df_final, active_weekdays, day_map, week_numbers, initial_master_assignments):
    """
    오전 근무에서 제외된 근무자를 오후 근무에서도 제외 처리하여 동기화합니다.
    - 이미 오후 근무자로 등록된 경우 상태를 '추가제외'로 업데이트합니다.
    - 오후 근무자로 등록되지 않았지만 마스터에는 있는 경우, '추가제외' 상태로 새로 추가합니다.
    """
    changed = False
    for date in active_weekdays:
        date_str = date.strftime('%Y-%m-%d')
        # 오전 근무에서 제외된 근무자 찾기
        excluded_am_workers = df_final[
            (df_final['날짜'] == date_str) &
            (df_final['시간대'] == '오전') &
            (df_final['상태'].isin(['제외', '추가제외']))
        ]['근무자'].unique()

        for worker in excluded_am_workers:
            # 해당 날짜, 오후 시간대에 해당 근무자의 기록이 있는지 확인
            pm_record = df_final[
                (df_final['날짜'] == date_str) &
                (df_final['시간대'] == '오후') &
                (df_final['근무자'] == worker)
            ]

            # [수정] 로직 시작
            # CASE 1: 기록이 이미 있는 경우
            if not pm_record.empty:
                # 상태가 '근무', '보충', '추가보충'인 경우에만 '추가제외'로 변경
                if pm_record.iloc[0]['상태'] in ['근무', '보충', '추가보충']:
                    df_final = update_worker_status(
                        df_final, date_str, '오후', worker,
                        '추가제외', '오전 제외로 인한 오후 제외',
                        '🟣 보라색', day_map, week_numbers
                    )
                    changed = True
            # CASE 2: 기록이 없는 경우
            else:
                # 마스터 스케줄에 오후 근무자로 지정되었는지 확인
                pm_master_workers = initial_master_assignments.get((date_str, '오후'), set())
                if worker in pm_master_workers:
                    # 마스터에는 있었으므로 '추가제외' 상태로 새로 추가
                    df_final = update_worker_status(
                        df_final, date_str, '오후', worker,
                        '추가제외', '오전 제외로 인한 오후 제외',
                        '🟣 보라색', day_map, week_numbers
                    )
                    changed = True
            # [수정] 로직 끝

    return df_final, changed

def is_worker_already_excluded_with_memo(df_data, date_s, time_s, worker_s):
    worker_records = df_data[
        (df_data['날짜'] == date_s) &
        (df_data['시간대'] == time_s) &
        (df_data['근무자'] == worker_s)
    ]
    if worker_records.empty:
        return False 

    excluded_records = worker_records[worker_records['상태'].isin(['제외', '추가제외'])]
    if excluded_records.empty:
        return False 

    return excluded_records['메모'].str.contains('보충 위해 제외됨|인원 초과로 인한 제외|오전 추가제외로 인한 오후 제외', na=False).any()

@st.cache_data(ttl=600, show_spinner=False)
def load_monthly_special_schedules(month_str):
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_url(st.secrets["google_sheet"]["url"])
        
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"

        yearly_schedule_sheet = spreadsheet.worksheet(sheet_name)
        yearly_schedule_data = yearly_schedule_sheet.get_all_records()
        df_yearly_schedule = pd.DataFrame(yearly_schedule_data)

        if df_yearly_schedule.empty:
            return pd.DataFrame(), pd.DataFrame()

        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        target_month = target_month_dt.month

        df_yearly_schedule['날짜'] = pd.to_datetime(df_yearly_schedule['날짜'])

        df_monthly_schedule = df_yearly_schedule[
            (df_yearly_schedule['날짜'].dt.year == int(target_year)) &
            (df_yearly_schedule['날짜'].dt.month == target_month)
        ].copy()

        df_display = df_monthly_schedule.copy()
        weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
        df_display['날짜'] = df_display['날짜'].apply(
            lambda x: f"{x.month}월 {x.day}일 ({weekday_map[x.weekday()]})"
        )

        return df_monthly_schedule, df_display  

    except gspread.exceptions.WorksheetNotFound:
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"
        st.error(f"❌ '{sheet_name}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"토요/휴일 스케줄을 불러오는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame(), pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def load_closing_days(month_str):
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_url(st.secrets["google_sheet"]["url"])
        
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 휴관일"
        
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        df_closing = pd.DataFrame(data)

        if df_closing.empty or "날짜" not in df_closing.columns:
            return [], pd.DataFrame(columns=["날짜"]) 

        df_closing['날짜'] = pd.to_datetime(df_closing['날짜'])
        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        
        df_monthly_closing = df_closing[
            df_closing['날짜'].dt.month == target_month_dt.month
        ].copy()

        df_display = df_monthly_closing.copy()
        weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
        df_display['날짜'] = df_display['날짜'].apply(
            lambda x: f"{x.month}월 {x.day}일 ({weekday_map[x.weekday()]})"
        )

        closing_dates_list = df_monthly_closing['날짜'].dt.strftime('%Y-%m-%d').tolist()
        
        return closing_dates_list, df_display

    except gspread.exceptions.WorksheetNotFound:
        st.info(f"ℹ️ '{sheet_name}' 시트를 찾을 수 없어 휴관일을 불러오지 않았습니다.")
        return [], pd.DataFrame(columns=["날짜"])
    except Exception as e:
        st.error(f"휴관일 정보를 불러오는 중 오류가 발생했습니다: {e}")
        return [], pd.DataFrame(columns=["날짜"])

def transform_schedule_for_checking(df_final_unique, df_excel, month_start, month_end):
    """
    [수정] 배정 확인용 스케줄 데이터를 생성합니다.
    휴가/제외 인원을 포함한 모든 인원이 출력되도록 열 개수를 동적으로 계산합니다.
    """
    # [핵심 수정 1] 월 전체에서 일별 최대 인원수 계산
    daily_counts = df_final_unique.groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
    max_am_workers = int(daily_counts.get('오전', pd.Series([0])).max())
    max_pm_workers = int(daily_counts.get('오후', pd.Series([0])).max())

    # 토요/휴일 스케줄의 최대 인원수도 고려
    if not df_excel.empty:
        weekend_am_counts = df_excel[[str(i) for i in range(1, 13)]].apply(lambda row: row.str.strip().ne('').sum(), axis=1)
        max_am_workers = max(max_am_workers, weekend_am_counts.max())

    # 최종 열 개수 확정 (최소 12, 4개는 유지)
    max_am_workers = max(max_am_workers, 12)
    max_pm_workers = max(max_pm_workers, 4)

    date_range = pd.date_range(start=month_start, end=month_end)
    date_list = [f"{d.month}월 {d.day}일" for d in date_range]
    weekday_map = {'Mon': '월', 'Tue': '화', 'Wed': '수', 'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'}
    weekdays = [weekday_map[d.strftime('%a')] for d in date_range]
    target_year = month_start.year

    # [핵심 수정 2] 동적으로 계산된 열 개수로 컬럼 정의
    columns = ['날짜', '요일'] + \
              [str(i) for i in range(1, max_am_workers + 1)] + \
              ['오전당직(온콜)'] + \
              [f'오후{i}' for i in range(1, max_pm_workers + 1)]
    result_df = pd.DataFrame(columns=columns)

    for date, weekday in zip(date_list, weekdays):
        date_key = datetime.strptime(date, '%m월 %d일').replace(year=target_year).strftime('%Y-%m-%d')
        
        row_data = {'날짜': date, '요일': weekday}

        # 오전/오후 근무자 정보 처리
        for time_slot, max_workers, col_prefix in [('오전', max_am_workers, ''), ('오후', max_pm_workers, '오후')]:
            # 모든 상태의 근무자 정보를 가져옴
            workers_info = df_final_unique[
                (df_final_unique['날짜'] == date_key) &
                (df_final_unique['시간대'] == time_slot)
            ].sort_values(by=['색상_우선순위', '근무자']).to_dict('records')

            for i in range(max_workers):
                col_name = f"{col_prefix}{i+1}" if col_prefix else str(i+1)
                if i < len(workers_info):
                    info = workers_info[i]
                    worker_name = info['근무자']
                    status = info['상태']
                    if status not in ['근무', '당직', '기본']:
                        row_data[col_name] = f"{worker_name}({status})"
                    else:
                        row_data[col_name] = worker_name
                else:
                    row_data[col_name] = ''

        # 당직 및 주말 정보 처리
        excel_row = df_excel[df_excel['날짜'] == date]
        if not excel_row.empty:
            row_data['오전당직(온콜)'] = excel_row['오전당직(온콜)'].iloc[0] if '오전당직(온콜)' in excel_row.columns else ''
            if weekday in ['토', '일']:
                for i in range(1, max_am_workers + 1):
                    row_data[str(i)] = excel_row[str(i)].iloc[0] if str(i) in excel_row.columns and pd.notna(excel_row[str(i)].iloc[0]) else ''
                for i in range(1, max_pm_workers + 1):
                    row_data[f'오후{i}'] = ''

        result_df = pd.concat([result_df, pd.DataFrame([row_data])], ignore_index=True)

    return result_df

def transform_schedule_data(df, df_excel, month_start, month_end):
    # 모든 상태 포함 (제외, 추가제외 포함)
    df = df[['날짜', '시간대', '근무자', '요일', '상태', '색상', '메모']].copy()
    
    date_range = pd.date_range(start=month_start, end=month_end)
    date_list = [f"{d.month}월 {d.day}일" for d in date_range]
    weekday_list = [d.strftime('%a') for d in date_range]
    weekday_map = {'Mon': '월', 'Tue': '화', 'Wed': '수', 'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'}
    weekdays = [weekday_map[w] for w in weekday_list]
    
    target_year = month_start.year

    columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 5)]
    result_df = pd.DataFrame(columns=columns)
    
    for date, weekday in zip(date_list, weekdays):
        date_key = datetime.strptime(date, '%m월 %d일').replace(year=target_year).strftime('%Y-%m-%d')
        date_df = df[df['날짜'] == date_key]
        
        # 오전 근무자 (모든 상태 포함)
        morning_workers = date_df[date_df['시간대'] == '오전'][['근무자', '상태', '색상', '메모']].to_dict('records')
        morning_data = [''] * 12
        for i, worker_info in enumerate(morning_workers[:12]):
            morning_data[i] = worker_info['근무자']
        
        # 오후 근무자 (모든 상태 포함)
        afternoon_workers = date_df[date_df['시간대'] == '오후'][['근무자', '상태', '색상', '메모']].to_dict('records')
        afternoon_data = [''] * 4
        for i, worker_info in enumerate(afternoon_workers[:4]):
            afternoon_data[i] = worker_info['근무자']
        
        if weekday in ['토', '일']: 
            excel_row = df_excel[df_excel['날짜'] == date]
            if not excel_row.empty:
                morning_data = [excel_row[str(i)].iloc[0] if str(i) in excel_row.columns and pd.notna(excel_row[str(i)].iloc[0]) else '' for i in range(1, 13)]
        
        oncall_worker = ''
        excel_row = df_excel[df_excel['날짜'] == date]
        if not excel_row.empty:
            oncall_worker = excel_row['오전당직(온콜)'].iloc[0] if '오전당직(온콜)' in excel_row.columns else ''
        
        row_data = [date, weekday] + morning_data + [oncall_worker] + afternoon_data
        result_df = pd.concat([result_df, pd.DataFrame([row_data], columns=columns)], ignore_index=True)
    
    return result_df

df_cumulative_next = df_cumulative.copy()

initialize_schedule_session_state()

st.write("")
st.markdown(f"**📅 {month_str} 토요/휴일 스케줄**")

df_monthly_schedule, df_display = load_monthly_special_schedules(month_str)

if not df_monthly_schedule.empty:
    st.dataframe(df_display[['날짜', '근무', '당직']], use_container_width=True, hide_index=True)
else:
    st.info(f"ℹ️ '{month_str}'에 해당하는 토요/휴일 스케줄이 없습니다.")

st.write(" ")
st.markdown(f"**📅 {month_str} 휴관일 정보**")

holiday_dates, df_closing_display = load_closing_days(month_str)

if holiday_dates:
    st.write("- 아래 날짜는 근무 배정에서 제외됩니다.")
    
    formatted_dates_list = df_closing_display['날짜'].tolist()
    
    display_string = ", ".join(formatted_dates_list)
    
    st.info(f"➡️ {display_string}")
else:
    st.info(f"ℹ️ {month_str}에는 휴관일이 없습니다.")

names_in_master = set(df_master["이름"].unique().tolist())
names_in_request = set(df_request["이름"].unique().tolist())
all_names = sorted(list(names_in_master.union(names_in_request)))  

def find_afternoon_swap_possibility(worker_to_check, original_date_str, df_final, active_weekdays, target_count_pm, df_supplement_processed, df_request, initial_master_assignments, day_map, week_numbers):
    shortage_dates = []
    original_date = pd.to_datetime(original_date_str).date()

    for date in active_weekdays:
        date_str = date.strftime('%Y-%m-%d')
        if date_str == original_date_str: continue
        
        if week_numbers.get(original_date) != week_numbers.get(date.date()):
            continue

        workers_on_date = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오후') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'].unique()
        if len(workers_on_date) < target_count_pm:
            shortage_dates.append(date_str)

    if not shortage_dates:
        return None

    for shortage_date in shortage_dates:
        morning_workers_on_shortage_date = set(df_final[(df_final['날짜'] == shortage_date) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
        if worker_to_check not in morning_workers_on_shortage_date:
            continue

        shortage_day_name = day_map.get(pd.to_datetime(shortage_date).weekday())
        supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == f"{shortage_day_name} 오후"]
        if supplement_row.empty: continue
        
        supplement_pool = set()
        for col in supplement_row.columns:
            if col.startswith('보충'):
                for val in supplement_row[col].dropna():
                    supplement_pool.add(val.replace('🔺','').strip())

        if worker_to_check not in supplement_pool:
            continue
        
        if worker_to_check in initial_master_assignments.get((shortage_date, '오후'), set()):
            continue

        no_supplement_req = {r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == '보충 불가(오후)'}
        if worker_to_check in no_supplement_req:
            continue

        return shortage_date
    return None

# 기존 execute_adjustment_pass 함수의 내용을 아래 코드로 전체 교체하세요.

def execute_adjustment_pass(df_final, active_weekdays, time_slot, target_count, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative, df_cumulative, all_names):
    from collections import defaultdict

    active_weekdays = [pd.to_datetime(date) if isinstance(date, str) else date for date in active_weekdays]
    df_cum_indexed = df_cumulative.set_index('항목').T
    
    # --- scores를 루프 시작 전 '한 번만' 정확히 계산 ---
    scores = {w: (df_cum_indexed.loc[w, f'{time_slot}누적'] + current_cumulative[time_slot].get(w, 0)) for w in all_names if w in df_cum_indexed.index}

    # 추가 제외 / 보충 로직
    for date in active_weekdays:
        date_str = date.strftime('%Y-%m-%d')
        current_workers_df = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충', '추가보충']))]
        current_workers = current_workers_df['근무자'].unique()
        count_diff = len(current_workers) - target_count
        
        # [인원 부족 시 보충]
        if count_diff < 0:
            needed = -count_diff
            day_name = day_map.get(date.weekday())
            supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == f"{day_name} {time_slot}"]
            candidates = []
            if not supplement_row.empty:
                for col in supplement_row.columns:
                    if col.startswith('보충'):
                        candidates.extend(val.replace('🔺', '').strip() for val in supplement_row[col].dropna())
            
            unavailable = set(current_workers)
            no_supp = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
            difficult_supp = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 어려움({time_slot})'}
            candidates = [w for w in candidates if w not in unavailable and w not in no_supp]
            
            if time_slot == '오후':
                am_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                candidates = [w for w in candidates if w in am_workers]
            
            if not candidates: continue

            candidates.sort(key=lambda w: (1 if w in difficult_supp else 0, scores.get(w, 0)))

            for worker_to_add in candidates[:needed]:
                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_add, '추가보충', '인원 부족 (균형 조정)', '🟡 노란색', day_map, week_numbers)
                current_cumulative[time_slot][worker_to_add] = current_cumulative[time_slot].get(worker_to_add, 0) + 1
                scores[worker_to_add] = scores.get(worker_to_add, 0) + 1 # scores 실시간 업데이트

        # [인원 초과 시 제외]
        elif count_diff > 0:
            over_count = count_diff
            must_work = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}

            # ✨ [핵심 변경 1] 루프가 돌 때마다 제외 가능한 최신 근무자 목록을 가져옵니다.
            for _ in range(over_count):
                # 현재 근무 중인 인원 목록을 다시 계산
                current_workers_df = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충', '추가보충']))]
                potential_removals = [w for w in current_workers_df['근무자'].unique() if w not in must_work]

                if not potential_removals:
                    break # 제외할 후보가 없으면 중단

                # ✨ [핵심 변경 2] '바로 이 순간'의 실시간 점수를 기준으로 정렬합니다.
                # scores 딕셔너리는 외부에서 계속 업데이트되고 있으므로 항상 최신 상태입니다.
                potential_removals.sort(key=lambda w: scores.get(w, 0), reverse=True) # 점수가 높은 순으로 정렬

                # 가장 점수가 높은 한 명을 선택하여 제외
                worker_to_remove = potential_removals[0]

                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_remove, '추가제외', '인원 초과 (실시간 균형 조정)', '🟣 보라색', day_map, week_numbers)

                # ✨ [핵심 변경 3] 점수를 즉시 업데이트하여 다음 루프에 반영합니다.
                current_cumulative[time_slot][worker_to_remove] = current_cumulative[time_slot].get(worker_to_remove, 0) - 1
                scores[worker_to_remove] = scores.get(worker_to_remove, 0) - 1

    return df_final, current_cumulative

from collections import defaultdict

def calculate_weekly_counts(df_final, all_names, week_numbers):
    """지정된 주차 정보에 따라 모든 인원의 주간 오전/오후 근무 횟수를 계산합니다."""
    weekly_counts = {worker: {'오전': defaultdict(int), '오후': defaultdict(int)} for worker in all_names}
    
    for _, row in df_final.iterrows():
        if row['상태'] in ['근무', '보충', '추가보충']:
            try:
                date_obj = pd.to_datetime(row['날짜']).date()
                week = week_numbers.get(date_obj) # .get()으로 안전하게 접근
                if week and row['근무자'] in weekly_counts:
                    weekly_counts[row['근무자']][row['시간대']][week] += 1
            except (KeyError, ValueError):
                continue
    return weekly_counts

def balance_weekly_and_cumulative(df_final, active_weekdays, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative, all_names, df_cumulative):
    df_cum_indexed = df_cumulative.set_index('항목').T
    
    for time_slot in ['오전', '오후']:
        for i in range(50):
            scores = {w: (df_cum_indexed.loc[w, f'{time_slot}누적'] + current_cumulative[time_slot].get(w, 0)) for w in all_names if w in df_cum_indexed.index}
            if not scores: break

            min_s, max_s = min(scores.values()), max(scores.values())
            
            worker_scores = sorted(scores.items(), key=lambda item: item[1])
            w_l, s_l = worker_scores[0]
            w_h, s_h = worker_scores[-1]
            
            swap_found_in_iteration = False
            
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                
                must_work = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}
                if w_h in must_work: continue

                is_h_working = not df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['근무자'] == w_h) & (df_final['상태'].isin(['근무', '보충', '추가보충']))].empty
                if not is_h_working: continue

                s_row = df_supplement_processed[df_supplement_processed['시간대'] == f"{day_map.get(date.weekday())} {time_slot}"]
                can_supp = any(w_l in s_row[col].dropna().str.replace('🔺', '').str.strip().tolist() for col in s_row.columns if col.startswith('보충'))
                if not can_supp: continue
                
                no_supp = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
                if w_l in no_supp: continue

                if time_slot == '오후':
                    am_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                    if w_l not in am_workers: continue
                
                is_master = w_l in initial_master_assignments.get((date_str, time_slot), set())
                status, color, memo = ('근무', '기본', '마스터 복귀') if is_master else ('추가보충', '🟡 노란색', '최종 균형 조정')
                
                df_final = update_worker_status(df_final, date_str, time_slot, w_h, '추가제외', '최종 균형 조정', '🟣 보라색', day_map, week_numbers)
                current_cumulative[time_slot][w_h] = current_cumulative[time_slot].get(w_h, 0) - 1
                df_final = update_worker_status(df_final, date_str, time_slot, w_l, status, memo, color, day_map, week_numbers)
                current_cumulative[time_slot][w_l] = current_cumulative[time_slot].get(w_l, 0) + 1
                
                swap_found_in_iteration = True
                break

            if swap_found_in_iteration:
                continue
            
            else:
                max_workers = ", ".join([worker for worker, score in scores.items() if score == max_s])
                min_workers = ", ".join([worker for worker, score in scores.items() if score == min_s])
                break
        
        else:
            st.warning(f"⚠️ {time_slot} 균형 조정이 최대 반복 횟수({i+1}회)에 도달했습니다.")

    return df_final, current_cumulative

def balance_final_cumulative_with_weekly_check(df_final, active_weekdays, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative, all_names, df_cumulative):
    """
    [완성본] 주간 최소 근무 횟수를 보장하면서 월간 누적 편차를 2 이하로 맞추는 최종 균형 조정 함수
    """

    # 규칙 설정: 주간 최소 오전 근무 3회, 오후 근무 1회
    MIN_AM_PER_WEEK = 3
    MIN_PM_PER_WEEK = 1

    # 오전, 오후 각각에 대해 조정 실행
    for time_slot in ['오전', '오후']:
        # 최대 50번까지 반복하며 편차를 줄임
        for i in range(50):
            # 1. '바로 지금' 시점의 실시간 누적 점수와 주간 근무 횟수를 계산
            df_cum_indexed = df_cumulative.set_index('항목').T
            scores = {w: (df_cum_indexed.loc[w, f'{time_slot}누적'] + current_cumulative[time_slot].get(w, 0)) for w in all_names if w in df_cum_indexed.index}
            if not scores: break

            weekly_counts = calculate_weekly_counts(df_final, all_names, week_numbers)
            
            min_s, max_s = min(scores.values()), max(scores.values())
            
            # 2. 목표 달성: 편차가 2 이하이면 해당 시간대 조정 완료
            if max_s - min_s <= 2:
                st.success(f"✅ [{time_slot}] 최종 누적 편차 2 이하 달성! (편차: {max_s - min_s})")
                break

            # 3. 최고점자(w_h)와 최저점자(w_l) 선정
            worker_scores = sorted(scores.items(), key=lambda item: item[1])
            w_l, s_l = worker_scores[0]    # 가장 근무 적게 한 사람
            w_h, s_h = worker_scores[-1]   # 가장 근무 많이 한 사람
            
            swap_found = False
            # 4. 최고점자의 근무일 중 하나를 최저점자에게 넘길 날짜 탐색
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                
                # 조건 1: 최고점자(w_h)가 해당일에 실제로 근무 중인가?
                is_working_df = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['근무자'] == w_h) & (df_final['상태'].isin(['근무', '보충', '추가보충']))]
                if is_working_df.empty:
                    continue # 근무 중이 아니면 다른 날짜 탐색

                # [핵심 안전장치] 조건 2: 이 근무를 빼도 w_h의 주간 최소 근무 횟수를 만족하는가?
                week_of_date = week_numbers.get(date.date())
                min_shifts = MIN_AM_PER_WEEK if time_slot == '오전' else MIN_PM_PER_WEEK
                if weekly_counts.get(w_h, {}).get(time_slot, {}).get(week_of_date, 0) - 1 < min_shifts:
                    continue # 만족하지 못하면 다른 날짜 탐색

                # 조건 3: 최저점자(w_l)가 이 날, 이 시간대에 보충 근무가 가능한가?
                # (이미 근무 중이거나, 휴가/보충불가 요청이 있거나, 보충 테이블에 없으면 불가능)
                is_already_working = not df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['근무자'] == w_l)].empty
                if is_already_working: continue
                
                no_supp_req = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
                if w_l in no_supp_req: continue
                
                # 보충 테이블에서 보충 가능한지 최종 확인
                day_name = day_map.get(date.weekday())
                supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == f"{day_name} {time_slot}"]
                can_supplement = any(w_l in supplement_row[col].dropna().str.replace('🔺', '').str.strip().tolist() for col in supplement_row.columns if col.startswith('보충'))
                if not can_supplement: continue

                # 5. 모든 조건을 통과했다면, 근무 교체 실행!
                st.warning(f"🔄 [{i+1}차/{time_slot}] 최종 균형 조정: {date.strftime('%-m/%d')} {w_h}({s_h:.0f}회) ➔ {w_l}({s_l:.0f}회)")
                
                # 최고점자는 제외 처리
                df_final = update_worker_status(df_final, date_str, time_slot, w_h, '추가제외', '최종 누적 균형 조정', '🟣 보라색', day_map, week_numbers)
                current_cumulative[time_slot][w_h] = current_cumulative[time_slot].get(w_h, 0) - 1
                
                # 최저점자는 보충 처리
                df_final = update_worker_status(df_final, date_str, time_slot, w_l, '추가보충', '최종 누적 균형 조정', '🟡 노란색', day_map, week_numbers)
                current_cumulative[time_slot][w_l] = current_cumulative[time_slot].get(w_l, 0) + 1
                
                swap_found = True
                break # 교체에 성공했으므로, 다시 처음부터 점수 계산을 위해 루프 탈출
            
            # 만약 모든 날짜를 다 찾아봤는데 교체할 대상을 못 찾았다면, 조정 중단
            if not swap_found:
                st.error(f"⚠️ [{time_slot}] 최종 균형 조정 중단: 주간 최소 근무 규칙을 위반하지 않는 교체 대상을 더 이상 찾지 못했습니다. (현재 편차: {max_s - min_s})")
                break
        else: # for문이 break 없이 50회를 모두 돌았다면
            st.warning(f"⚠️ [{time_slot}] 최종 균형 조정이 최대 반복 횟수({i+1}회)에 도달했습니다.")
            
    return df_final, current_cumulative

df_cumulative_next = df_cumulative.copy()

initialize_schedule_session_state()

st.divider()
# 1단계: 메인 배정 실행 버튼
if st.button("🚀 스케줄 배정 수행", type="primary", use_container_width=True, disabled=st.session_state.get("show_confirmation_warning", False)):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    latest_version = find_latest_schedule_version(sheet, month_str)

    # 이미 버전이 존재하면 확인 단계로 넘어감
    if latest_version:
        st.session_state.show_confirmation_warning = True
        st.session_state.latest_existing_version = latest_version
        st.rerun()
    # 버전이 없으면 바로 배정 실행
    else:
        st.session_state.assigned = True
        st.session_state.assignment_results = None
        st.session_state.request_logs, st.session_state.swap_logs, st.session_state.adjustment_logs, st.session_state.oncall_logs = [], [], [], []
        st.rerun()

# 2단계: 확인 경고 및 최종 실행 UI
if st.session_state.get("show_confirmation_warning", False):
    latest_version = st.session_state.get("latest_existing_version", "알 수 없는 버전")
    
    # 정규식을 사용하여 'verX.X' 부분만 추출
    version_match = re.search(r'(ver\s*\d+\.\d+)', latest_version)
    version_str = version_match.group(1) if version_match else latest_version
    
    st.warning(f"⚠️ **이미 '{version_str}' 버전이 존재합니다.**\n\n새로운 'ver1.0' 스케줄을 생성하시더라도 {version_str}은 계속 남아있습니다. 계속하시겠습니까?")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ 네, 새로운 ver1.0으로 배정을 실행합니다.", use_container_width=True, type="primary"):
            st.session_state.assigned = True
            st.session_state.show_confirmation_warning = False
            st.session_state.assignment_results = None
            st.session_state.request_logs, st.session_state.swap_logs, st.session_state.adjustment_logs, st.session_state.oncall_logs = [], [], [], []
            st.rerun()
    with col2:
        if st.button("❌ 아니요, 취소합니다.", use_container_width=True):
            st.session_state.show_confirmation_warning = False
            st.rerun()

if st.session_state.get('assigned', False):

    if st.session_state.get('assignment_results') is None:
        with st.spinner("근무 배정 중..."):
            st.session_state.request_logs = []
            st.session_state.swap_logs = []
            st.session_state.adjustment_logs = []
            st.session_state.oncall_logs = []
                    
            time.sleep(1)
            
            df_monthly_schedule, df_display = load_monthly_special_schedules(month_str)

            special_schedules = []
            if not df_monthly_schedule.empty:
                for index, row in df_monthly_schedule.iterrows():
                    date_str = row['날짜'].strftime('%Y-%m-%d')
                    oncall_person = row['당직']
                    workers_str = row.get('근무', '')
                    
                    if workers_str and isinstance(workers_str, str):
                        workers_list = [name.strip() for name in workers_str.split(',')]
                    else:
                        workers_list = []
                    
                    special_schedules.append((date_str, workers_list, oncall_person))

            df_final = pd.DataFrame(columns=['날짜', '요일', '주차', '시간대', '근무자', '상태', '메모', '색상'])
            month_dt = datetime.strptime(month_str, "%Y년 %m월")
            _, last_day = calendar.monthrange(month_dt.year, month_dt.month) 
            all_month_dates = pd.date_range(start=month_dt, end=month_dt.replace(day=last_day))
            weekdays = [d for d in all_month_dates if d.weekday() < 5]
            active_weekdays = [d for d in weekdays if d.strftime('%Y-%m-%d') not in holiday_dates]
            day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

            # --- ✨ 주차 계산 로직 변경 ---
            # 1. 월 내 모든 날짜의 ISO 주차 번호(연간 기준, 월요일 시작)를 중복 없이 구합니다.
            iso_weeks_in_month = sorted(list(set(d.isocalendar()[1] for d in all_month_dates)))
            
            # 2. ISO 주차 번호를 해당 월의 1, 2, 3... 주차로 매핑하는 사전을 만듭니다.
            # 예: {35주차: 1, 36주차: 2, 37주차: 3, ...}
            iso_to_monthly_week_map = {iso_week: i + 1 for i, iso_week in enumerate(iso_weeks_in_month)}
            
            # 3. 최종적으로 모든 날짜에 대해 '월 기준 주차'를 할당합니다.
            week_numbers = {d.to_pydatetime().date(): iso_to_monthly_week_map[d.isocalendar()[1]] for d in all_month_dates}
            # --- 로직 변경 끝 ---

            initial_master_assignments = {}
            for date in active_weekdays:
                date_str, day_name, week_num = date.strftime('%Y-%m-%d'), day_map[date.weekday()], week_numbers[date.date()]
                for ts in ['오전', '오후']:
                    shift_key, base_workers = f"{day_name} {ts}", set()
                    shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                    if not shift_row.empty:
                        for col in shift_row.columns[1:]:
                            worker_info = shift_row[col].values[0]
                            if pd.notna(worker_info):
                                worker_name = str(worker_info).split('(')[0].strip()
                                if '(' in str(worker_info) and f'{week_num}주' in str(worker_info):
                                    base_workers.add(worker_name)
                                elif '(' not in str(worker_info):
                                    base_workers.add(worker_name)
                    initial_master_assignments[(date_str, ts)] = base_workers
            
            current_cumulative = {'오전': {}, '오후': {}}

            time_slot_am = '오전'
            target_count_am = 12
            
            # 오전 초기 배정
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                requests_on_date = df_request[df_request['날짜정보'].apply(lambda x: date_str in parse_date_range(str(x)))]
                vacationers = set(requests_on_date[requests_on_date['분류'].isin(['휴가', '학회'])]['이름'].tolist())
                base_workers = initial_master_assignments.get((date_str, time_slot_am), set())
                must_work = set(requests_on_date[requests_on_date['분류'] == f'꼭 근무({time_slot_am})']['이름'].tolist())
                final_workers = (base_workers - vacationers) | (must_work - vacationers)
                for worker in final_workers:
                    df_final = update_worker_status(df_final, date_str, time_slot_am, worker, '근무', '' if worker in must_work else '', '🟠 주황색' if worker in must_work else '기본', day_map, week_numbers)
                
                weekday_map_korean = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

                # [수정 1] 오전 휴가자 상태를 '제외'가 아닌 '휴가' 또는 '학회'로 설정
                for vac in (vacationers & base_workers):
                    korean_day = weekday_map_korean[date.weekday()]
                    log_date = f"{date.strftime('%-m월 %-d일')} ({korean_day})"
                    reason = requests_on_date[requests_on_date['이름'] == vac]['분류'].iloc[0]
                    
                    st.session_state.request_logs.append(f"• {log_date} {vac} - {reason}로 인한 제외")
                    
                    # '제외' 대신 실제 사유(reason)를 상태(status)로 전달
                    df_final = update_worker_status(df_final, date_str, time_slot_am, vac, reason, f'{reason}로 인한 제외', '🔴 빨간색', day_map, week_numbers)
            
            # 오전 배정 후 동기화
            df_final, changed = sync_am_to_pm_exclusions(df_final, active_weekdays, day_map, week_numbers, initial_master_assignments)

            # 오전 균형 맞추기 (execute_adjustment_pass)
            df_before_pass = df_final.copy()
            df_final, current_cumulative = execute_adjustment_pass(
                df_final, active_weekdays, time_slot_am, target_count_am, initial_master_assignments, 
                df_supplement_processed, df_request, day_map, week_numbers, current_cumulative, df_cumulative, all_names
            )
            # 오전 조정 후 동기화
            df_final, changed = sync_am_to_pm_exclusions(df_final, active_weekdays, day_map, week_numbers, initial_master_assignments)

            time_slot_pm = '오후'
            target_count_pm = 4
            
            # 오후 초기 배정
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                morning_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                requests_on_date = df_request[df_request['날짜정보'].apply(lambda x: date_str in parse_date_range(str(x)))]
                vacationers = set(requests_on_date[requests_on_date['분류'].isin(['휴가', '학회'])]['이름'].tolist())
                base_workers = initial_master_assignments.get((date_str, time_slot_pm), set())
                must_work = set(requests_on_date[requests_on_date['분류'] == f'꼭 근무({time_slot_pm})']['이름'].tolist())
                
                eligible_workers = morning_workers | must_work
                final_workers = (base_workers & eligible_workers) - vacationers | must_work
                
                for worker in final_workers:
                    df_final = update_worker_status(df_final, date_str, time_slot_pm, worker, '근무', '' if worker in must_work else '', '🟠 주황색' if worker in must_work else '기본', day_map, week_numbers)
                
                # [수정 2] 오후 휴가자 상태도 '제외'가 아닌 실제 사유로 설정
                for vac in (vacationers & base_workers):
                    if not df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot_pm) & (df_final['근무자'] == vac) & (df_final['상태'] == '근무')].empty:
                        continue
                    
                    reason_series = requests_on_date[requests_on_date['이름'] == vac]['분류']
                    reason = reason_series.iloc[0] if not reason_series.empty else "휴가"
                    
                    # '제외' 대신 실제 사유(reason)를 상태(status)로 전달
                    df_final = update_worker_status(df_final, date_str, time_slot_pm, vac, reason, f'{reason}로 제외', '🔴 빨간색', day_map, week_numbers)

            # 오후 배정 후 동기화
            df_final, changed = sync_am_to_pm_exclusions(df_final, active_weekdays, day_map, week_numbers, initial_master_assignments)

            # 오후 조정 패스
            df_final, current_cumulative = execute_adjustment_pass(
                df_final, active_weekdays, time_slot_pm, target_count_pm, initial_master_assignments,
                df_supplement_processed, df_request, day_map, week_numbers, current_cumulative, df_cumulative, all_names
            )

            df_final, current_cumulative = balance_weekly_and_cumulative(
                df_final, active_weekdays, initial_master_assignments, df_supplement_processed,
                df_request, day_map, week_numbers, current_cumulative, all_names,
                df_cumulative
            )

            # ✨✨✨✨✨✨✨✨✨ [새 코드 추가 위치] ✨✨✨✨✨✨✨✨✨
            # 바로 이어서, 새로 만든 최종 누적 균형 조정 함수를 호출합니다.
            df_final, current_cumulative = balance_final_cumulative_with_weekly_check(
                df_final, active_weekdays, df_supplement_processed, df_request, 
                day_map, week_numbers, current_cumulative, all_names, df_cumulative
            )

            # ✨✨✨ [핵심 수정 1] 상태 변경은 이 함수가 유일하게 담당합니다. ✨✨✨
            df_final = replace_adjustments(df_final)
            # ✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨✨

            # df_final_unique_sorted 생성 후 로그 생성 부분 수정
            df_final_unique_sorted = df_final.sort_values(by=['날짜', '시간대', '근무자']).drop_duplicates(
                subset=['날짜', '시간대', '근무자'], keep='last'
            ).copy()

            # 대체 로그 생성
            df_replacements = df_final_unique_sorted[
                df_final_unique_sorted['상태'].isin(['대체보충', '대체제외'])
            ].copy()
            df_replacements['주차'] = df_replacements['날짜'].apply(
                lambda x: week_numbers.get(pd.to_datetime(x).date())
            )

            weekly_swap_dates = {}
            for (week, worker, time_slot), group in df_replacements.groupby(['주차', '근무자', '시간대']):
                dates_excluded = sorted(group[group['상태'] == '대체제외']['날짜'].tolist())
                dates_supplemented = sorted(group[group['상태'] == '대체보충']['날짜'].tolist())

                if dates_excluded and dates_supplemented:
                    key = (week, worker, time_slot)
                    weekly_swap_dates[key] = {
                        '제외일': dates_excluded,
                        '보충일': dates_supplemented
                    }
                    
                    # 메모 업데이트
                    memo_for_exclusion = f"{', '.join([pd.to_datetime(d).strftime('%-m월 %-d일') for d in dates_supplemented])}일과 대체"
                    memo_for_supplement = f"{', '.join([pd.to_datetime(d).strftime('%-m월 %-d일') for d in dates_excluded])}일과 대체"

                    df_final_unique_sorted.loc[
                        (df_final_unique_sorted['근무자'] == worker) &
                        (df_final_unique_sorted['시간대'] == time_slot) &
                        (df_final_unique_sorted['날짜'].isin(dates_excluded)), '메모'
                    ] = memo_for_exclusion

                    df_final_unique_sorted.loc[
                        (df_final_unique_sorted['근무자'] == worker) &
                        (df_final_unique_sorted['시간대'] == time_slot) &
                        (df_final_unique_sorted['날짜'].isin(dates_supplemented)), '메모'
                    ] = memo_for_supplement

            # 로그 생성
            st.session_state.swap_logs, st.session_state.adjustment_logs = [], []
            weekday_map_korean = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

            # 대체 로그
            for (week, worker, time_slot), swap_info in weekly_swap_dates.items():
                excluded_dates_str = [pd.to_datetime(d).strftime('%-m월 %-d일') for d in sorted(swap_info['제외일'])]
                supplemented_dates_str = [pd.to_datetime(d).strftime('%-m월 %-d일') for d in sorted(swap_info['보충일'])]
                log_message = f"• {worker} ({time_slot}): {', '.join(excluded_dates_str)}(대체 제외) ➔ {', '.join(supplemented_dates_str)}(대체 보충)"
                if log_message not in st.session_state.swap_logs:
                    st.session_state.swap_logs.append(log_message)

            # 추가 보충/제외 로그
            for _, row in df_final_unique_sorted.iterrows():
                if row['상태'] in ['추가보충', '추가제외']:
                    date_obj = pd.to_datetime(row['날짜'])
                    log_date_info = f"{date_obj.strftime('%-m월 %-d일')} ({weekday_map_korean[date_obj.weekday()]}) {row['시간대']}"
                    if row['상태'] == '추가제외':
                        st.session_state.adjustment_logs.append(f"• {log_date_info} {row['근무자']} - {row['메모'] or '인원 초과'}로 추가 제외")
                    elif row['상태'] == '추가보충':
                        st.session_state.adjustment_logs.append(f"• {log_date_info} {row['근무자']} - {row['메모'] or '인원 부족'}으로 추가 보충")
                        
            # 모든 로그를 날짜 기준으로 정렬합니다.
            st.session_state.request_logs.sort(key=get_sort_key)
            st.session_state.swap_logs.sort(key=get_sort_key)
            st.session_state.adjustment_logs.sort(key=get_sort_key)          
            st.session_state.request_logs.sort(key=get_sort_key)
            st.session_state.swap_logs.sort(key=get_sort_key)
            st.session_state.adjustment_logs.sort(key=get_sort_key)

            df_cumulative_next = df_cumulative.copy()  # 인덱스 설정 제거
            for worker, count in current_cumulative.get('오전', {}).items():
                if worker not in df_cumulative_next.columns:
                    df_cumulative_next[worker] = 0  # 새로운 근무자 열 추가
                if '오전누적' not in df_cumulative_next['항목'].values:
                    new_row = pd.DataFrame([[0] * len(df_cumulative_next.columns)], columns=df_cumulative_next.columns)
                    new_row['항목'] = '오전누적'
                    df_cumulative_next = pd.concat([df_cumulative_next, new_row], ignore_index=True)
                df_cumulative_next.loc[df_cumulative_next['항목'] == '오전누적', worker] += count

            for worker, count in current_cumulative.get('오후', {}).items():
                if worker not in df_cumulative_next.columns:
                    df_cumulative_next[worker] = 0  # 새로운 근무자 열 추가
                if '오후누적' not in df_cumulative_next['항목'].values:
                    new_row = pd.DataFrame([[0] * len(df_cumulative_next.columns)], columns=df_cumulative_next.columns)
                    new_row['항목'] = '오후누적'
                    df_cumulative_next = pd.concat([df_cumulative_next, new_row], ignore_index=True)
                df_cumulative_next.loc[df_cumulative_next['항목'] == '오후누적', worker] += count

            if special_schedules:
                for date_str, workers, oncall in special_schedules:
                    if not df_final.empty: df_final = df_final[df_final['날짜'] != date_str].copy()
                    for worker in workers:
                        df_final = update_worker_status(df_final, date_str, '오전', worker, '근무', '', '특수근무색', day_map, week_numbers)

            color_priority = {'🟠 주황색': 0, '🟢 초록색': 1, '🟡 노란색': 2, '기본': 3, '🔴 빨간색': 4, '🔵 파란색': 5, '🟣 보라색': 6, '특수근무색': -1}
            df_final['색상_우선순위'] = df_final['색상'].map(color_priority)
            df_final_unique = df_final.sort_values(by=['날짜', '시간대', '근무자', '색상_우선순위']).drop_duplicates(subset=['날짜', '시간대', '근무자'], keep='last')

            all_month_dates = pd.date_range(start=month_dt, end=month_dt.replace(day=last_day))
            weekdays = [d for d in all_month_dates if d.weekday() < 5]
            active_weekdays = [d for d in weekdays if d.strftime('%Y-%m-%d') not in holiday_dates]
            day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
            week_numbers = {d.to_pydatetime().date(): (d.day - 1) // 7 + 1 for d in all_month_dates}

            df_schedule = pd.DataFrame({'날짜': [d.strftime('%Y-%m-%d') for d in all_month_dates], '요일': [day_map.get(d.weekday()) for d in all_month_dates]})
            worker_counts_all = df_final_unique.groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
            max_morning_workers = int(worker_counts_all.get('오전', pd.Series(data=0)).max())
            max_afternoon_workers = int(worker_counts_all.get('오후', pd.Series(data=0)).max())
            columns = ['날짜', '요일'] + [str(i) for i in range(1, max_morning_workers + 1)] + [''] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, max_afternoon_workers + 1)]
            df_excel = pd.DataFrame(index=df_schedule.index, columns=columns)

            for idx, row in df_schedule.iterrows():
                date = row['날짜']
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                df_excel.at[idx, '날짜'] = f"{date_obj.month}월 {date_obj.day}일"
                df_excel.at[idx, '요일'] = row['요일']
                df_excel.fillna("", inplace=True)
                
                morning_workers_for_excel = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오전')]
                morning_workers_for_excel_sorted = morning_workers_for_excel.sort_values(by=['색상_우선순위', '근무자'])['근무자'].tolist()
                for i, worker_name in enumerate(morning_workers_for_excel_sorted, 1):
                    if i <= max_morning_workers: df_excel.at[idx, str(i)] = worker_name
                
                afternoon_workers_for_excel = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후')]
                afternoon_workers_for_excel_sorted = afternoon_workers_for_excel.sort_values(by=['색상_우선순위', '근무자'])['근무자'].tolist()
                for i, worker_name in enumerate(afternoon_workers_for_excel_sorted, 1):
                    if i <= max_afternoon_workers: df_excel.at[idx, f'오후{i}'] = worker_name
                
                for special_date, workers, oncall in special_schedules:
                    if date == special_date:
                        workers_padded = workers[:10] + [''] * (10 - len(workers[:10]))
                        for i in range(1, 11): df_excel.at[idx, str(i)] = workers_padded[i-1]
                        df_excel.at[idx, '오전당직(온콜)'] = oncall if oncall != "당직 없음" else ''

            ### 시작: 오전당직 배정 로직 ###
            df_cum_indexed = df_cumulative.set_index('항목')
            oncall_counts = df_cum_indexed.loc['오전당직 (목표)'].to_dict() if '오전당직 (목표)' in df_cum_indexed.index else {name: 0 for name in df_cumulative.columns if name != '항목'}
            oncall_assignments = {worker: int(count) for worker, count in oncall_counts.items() if pd.notna(count) and int(count) > 0}

            assignable_dates = sorted([d for d in df_final_unique['날짜'].unique() if d not in {s[0] for s in special_schedules}])
            oncall = {}

            # --- 1단계: (준비) 날짜별 후보자 목록 생성 ---
            daily_candidates = {}
            for date in assignable_dates:
                morning_workers = set(df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오전') & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                afternoon_workers = set(df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후') & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                candidates = list(morning_workers - afternoon_workers)
                daily_candidates[date] = candidates

            # --- 2단계: (목표 배정) ---
            assignments_needed = []
            for worker, count in oncall_assignments.items():
                assignments_needed.extend([worker] * count)
            random.shuffle(assignments_needed)

            worker_possible_dates = {worker: [d for d in assignable_dates if worker in daily_candidates.get(d, [])] for worker in set(assignments_needed)}

            for worker_to_assign in assignments_needed:
                possible = [d for d in worker_possible_dates[worker_to_assign] if d not in oncall]
                random.shuffle(possible)
                assigned = False
                for date in possible:
                    date_index = assignable_dates.index(date)
                    if date_index > 0 and oncall.get(assignable_dates[date_index - 1]) == worker_to_assign:
                        continue
                    oncall[date] = worker_to_assign
                    assigned = True
                    break
                # ✨ [수정] 중간 과정의 '배정 실패' 로그는 생성하지 않음

            # --- 3단계: (나머지 랜덤 배정) ---
            remaining_dates = [d for d in assignable_dates if d not in oncall]
            for date in sorted(remaining_dates):
                current_counts = Counter(oncall.values())
                date_index = assignable_dates.index(date)
                previous_oncall_person = oncall.get(assignable_dates[date_index - 1]) if date_index > 0 else None

                candidates_on_date = [p for p in daily_candidates.get(date, []) if p != previous_oncall_person]
                if not candidates_on_date:
                    candidates_on_date = daily_candidates.get(date, [])

                if not candidates_on_date:
                    continue

                candidates_on_date.sort(key=lambda p: (
                    current_counts.get(p, 0) < oncall_assignments.get(p, 0),
                    oncall_assignments.get(p, 0) == 0
                ), reverse=True)

                oncall[date] = candidates_on_date[0]

            # --- ✨ [핵심 수정] 최종 배정 결과 로그 생성 ---
            st.session_state.oncall_logs = [] # 기존 로그를 모두 지우고 시작
            actual_oncall_counts = Counter(oncall.values())
            all_relevant_workers = sorted(list(set(oncall_assignments.keys()) | set(actual_oncall_counts.keys())))

            for worker in all_relevant_workers:
                required_count = oncall_assignments.get(worker, 0)
                actual_count = actual_oncall_counts.get(worker, 0)

                if required_count != actual_count:
                    if actual_count > required_count:
                        comparison_text = f"많은 {actual_count}회 배정"
                    else:
                        comparison_text = f"적은 {actual_count}회 배정"

                    log_message = f"• {worker}: 오전당직 목표치 '{required_count}회'보다 {comparison_text}"
                    st.session_state.oncall_logs.append(log_message)
            # --- 배정 종료 ---

            # 엑셀 시트에 배정 결과 업데이트
            for idx, row in df_schedule.iterrows():
                date = row['날짜']
                df_excel.at[idx, '오전당직(온콜)'] = oncall.get(date, '')
            ### 끝: 오전당직 배정 로직 ###

            # ✨ [핵심 수정 1] 배정된 oncall 결과를 df_final에 '오전당직' 시간대로 추가
            oncall_df = pd.DataFrame([
                {
                    '날짜': date, '요일': day_map.get(pd.to_datetime(date).weekday(), ''),
                    '주차': week_numbers.get(pd.to_datetime(date).date(), 0),
                    '시간대': '오전당직', '근무자': worker, '상태': '당직',
                    '메모': '', '색상': '기본'
                } for date, worker in oncall.items()
            ])
            if not oncall_df.empty:
                df_final = pd.concat([df_final, oncall_df], ignore_index=True)

            # ✨ [핵심 수정 2] 모든 배정이 끝난 후, 최종 데이터를 정리
            color_priority = {'🟠 주황색': 0, '🟢 초록색': 1, '🟡 노란색': 2, '기본': 3, '🔴 빨간색': 4, '🔵 파란색': 5, '🟣 보라색': 6, '특수근무색': -1}
            df_final['색상_우선순위'] = df_final['색상'].map(color_priority)
            df_final_unique_sorted = df_final.sort_values(by=['날짜', '시간대', '근무자', '색상_우선순위']).drop_duplicates(
                subset=['날짜', '시간대', '근무자'], keep='last'
            )
            # create_final_schedule_excel 함수에 전달할 df_final_unique 변수도 여기서 최종본으로 다시 정의
            df_final_unique = df_final_unique_sorted 

            # ✨ [핵심 수정 3] 요약 테이블 생성에 필요한 변수들을 정의
            month_dt = datetime.strptime(month_str, "%Y년 %m월")
            next_month_dt = (month_dt + relativedelta(months=1)).replace(day=1)
            next_month_str = next_month_dt.strftime("%Y년 %-m월")

            # ✨ [핵심 수정 4] 올바른 최종 데이터로 요약 테이블 생성
            summary_df = build_summary_table(
                df_cumulative, all_names, next_month_str,
                df_final_unique=df_final_unique_sorted
            )

            if platform.system() == "Windows":
                font_name = "맑은 고딕"  
            else:
                font_name = "Arial"  

            duty_font = Font(name=font_name, size=9, bold=True, color="FF69B4")  
            default_font = Font(name=font_name, size=9)  

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "스케줄"

            color_map = {
                '🔴 빨간색': 'DA9694',  
                '🟠 주황색': 'FABF8F',  
                '🟢 초록색': 'A9D08E',  
                '🟡 노란색': 'FFF28F',  
                '🔵 파란색': '95B3D7',  
                '🟣 보라색': 'B1A0C7',  
                '기본': 'FFFFFF',        
                '특수근무색': 'D0E0E3'   
            }
            special_day_fill = PatternFill(start_color='95B3D7', end_color='95B3D7', fill_type='solid')
            empty_day_fill = PatternFill(start_color='808080', end_color='808080', fill_type='solid')
            default_day_fill = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')

            for col_idx, col_name in enumerate(df_excel.columns, 1):
                cell = ws.cell(row=1, column=col_idx, value=col_name)
                cell.fill = PatternFill(start_color='000000', end_color='000000', fill_type='solid')
                cell.font = Font(name=font_name, size=9, color='FFFFFF', bold=True)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin', color='000000'),
                                    right=Side(style='thin', color='000000'),
                                    top=Side(style='thin', color='000000'),
                                    bottom=Side(style='thin', color='000000'))

            border = Border(left=Side(style='thin', color='000000'),
                            right=Side(style='thin', color='000000'),
                            top=Side(style='thin', color='000000'),
                            bottom=Side(style='thin', color='000000'))

            for row_idx, (idx, row) in enumerate(df_excel.iterrows(), 2):
                date_str_lookup = df_schedule.at[idx, '날짜']
                special_schedule_dates_set = {s[0] for s in special_schedules}
                is_special_day = date_str_lookup in special_schedule_dates_set
                is_empty_day = df_final_unique[df_final_unique['날짜'] == date_str_lookup].empty and not is_special_day

                oncall_person_for_row = str(row['오전당직(온콜)']).strip() if pd.notna(row['오전당직(온콜)']) else ""

                weekend_oncall_worker = None
                if is_special_day:
                    for s in special_schedules:
                        if s[0] == date_str_lookup and s[2] != "당직 없음":
                            weekend_oncall_worker = s[2]
                            break

                for col_idx, col_name in enumerate(df_excel.columns, 1):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.value = row[col_name]
                    cell.font = default_font  
                    cell.border = border
                    cell.alignment = Alignment(horizontal='center', vertical='center')

                    if is_empty_day:
                        cell.fill = empty_day_fill
                        continue

                    if col_name == '날짜':
                        cell.fill = empty_day_fill
                    elif col_name == '요일':
                        cell.fill = special_day_fill if is_special_day else default_day_fill
                    elif str(col_name).isdigit():  
                        worker = str(row[col_name]).strip()
                        if worker and pd.notna(worker):
                            if is_special_day and worker == weekend_oncall_worker:
                                cell.font = duty_font
                            
                            worker_data = df_final_unique[(df_final_unique['날짜'] == date_str_lookup) & (df_final_unique['시간대'] == '오전') & (df_final_unique['근무자'] == worker)]
                            if not worker_data.empty:
                                color_name = worker_data.iloc[0]['색상']
                                cell.fill = PatternFill(start_color=color_map.get(color_name, 'FFFFFF'), end_color=color_map.get(color_name, 'FFFFFF'), fill_type='solid')
                                memo_text = worker_data.iloc[0]['메모']
                                if memo_text and ('보충' in memo_text or '이동' in memo_text):
                                    cell.comment = Comment(memo_text, "Schedule Bot")
                    
                    elif '오후' in str(col_name):  
                        worker = str(row[col_name]).strip()
                        if worker and pd.notna(worker):
                            worker_data = df_final_unique[(df_final_unique['날짜'] == date_str_lookup) & (df_final_unique['시간대'] == '오후') & (df_final_unique['근무자'] == worker)]
                            if not worker_data.empty:
                                color_name = worker_data.iloc[0]['색상']
                                cell.fill = PatternFill(start_color=color_map.get(color_name, 'FFFFFF'), end_color=color_map.get(color_name, 'FFFFFF'), fill_type='solid')
                                memo_text = worker_data.iloc[0]['메모']
                                if memo_text and ('보충' in memo_text or '이동' in memo_text):
                                    cell.comment = Comment(memo_text, "Schedule Bot")
                    
                    elif col_name == '오전당직(온콜)':
                        if oncall_person_for_row:
                            cell.font = duty_font

            ws.column_dimensions['A'].width = 11
            for col in ws.columns:
                 if col[0].column_letter != 'A':
                     ws.column_dimensions[col[0].column_letter].width = 9

            month_dt = datetime.strptime(month_str, "%Y년 %m월")
            next_month_dt = (month_dt + relativedelta(months=1)).replace(day=1)
            next_month_str = next_month_dt.strftime("%Y년 %-m월")
            month_start = month_dt.replace(day=1)
            month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

            summary_df = build_summary_table(
                df_cumulative,
                all_names,
                next_month_str,
                df_final_unique=df_final_unique_sorted
            )
            style_args = {
                'font': default_font,
                'bold_font': Font(name=font_name, size=9, bold=True),
                'border': border,
            }
            append_summary_table_to_excel(ws, summary_df, style_args)

            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            st.session_state.output = output
            
            def create_final_schedule_excel(df_excel_original, df_schedule, df_final_unique, special_schedules, **style_args):
                wb_final = openpyxl.Workbook()
                ws_final = wb_final.active
                ws_final.title = "스케줄"
                final_columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + [''] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 5)]

                # 헤더 작성
                for col_idx, col_name in enumerate(final_columns, 1):
                    cell = ws_final.cell(row=1, column=col_idx, value=col_name)
                    cell.fill = PatternFill(start_color='000000', end_color='000000', fill_type='solid')
                    cell.font = Font(name=style_args['font_name'], size=9, color='FFFFFF', bold=True)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = style_args['border']

                # 데이터 작성
                for row_idx, (idx, row_original) in enumerate(df_excel_original.iterrows(), 2):
                    date_str_lookup = df_schedule.at[idx, '날짜']
                    is_special_day = date_str_lookup in {s[0] for s in special_schedules}
                    is_empty_day = df_final_unique[df_final_unique['날짜'] == date_str_lookup].empty and not is_special_day
                    oncall_person = str(row_original['오전당직(온콜)']).strip() if pd.notna(row_original['오전당직(온콜)']) else ""

                    weekend_oncall_worker = None
                    if is_special_day:
                        weekend_oncall_worker = next((s[2] for s in special_schedules if s[0] == date_str_lookup and s[2] != "당직 없음"), None)

                    # 오후 근무자 처리 (모든 상태 포함, 최대 4명)
                    afternoon_workers_original = df_final_unique[
                        (df_final_unique['날짜'] == date_str_lookup) & 
                        (df_final_unique['시간대'] == '오후') &
                        (df_final_unique['상태'].isin(['근무', '보충', '추가보충', '제외', '추가제외', '휴가']))
                    ][['근무자', '상태', '색상', '메모', '색상_우선순위']].sort_values(by=['색상_우선순위', '근무자']).to_dict('records')
                    
                    afternoon_workers_final = afternoon_workers_original[:4]  # 최대 4명, 상태 정보 포함

                    # 행 데이터 구성
                    final_row_data = {col: row_original.get(col, '') for col in ['날짜', '요일'] + [str(i) for i in range(1, 13)]}
                    final_row_data[''] = ''
                    final_row_data['오전당직(온콜)'] = oncall_person
                    for i, worker_info in enumerate(afternoon_workers_final, 1):
                        if i <= 4:  # 오후 근무자 최대 4명
                            final_row_data[f'오후{i}'] = worker_info['근무자']

                    # 셀 작성
                    for col_idx, col_name in enumerate(final_columns, 1):
                        cell_value = final_row_data.get(col_name, "")
                        cell = ws_final.cell(row=row_idx, column=col_idx, value=cell_value)
                        cell.font = style_args['font']
                        cell.border = style_args['border']
                        cell.alignment = Alignment(horizontal='center', vertical='center')

                        if is_empty_day:
                            cell.fill = style_args['empty_day_fill']
                            continue
                        
                        if col_name == '날짜':
                            cell.fill = style_args['empty_day_fill']
                        elif col_name == '요일':
                            cell.fill = style_args['special_day_fill'] if is_special_day else style_args['default_day_fill']
                        else:
                            worker_name = str(cell.value).strip()
                            if worker_name:
                                time_slot = '오전' if str(col_name).isdigit() else ('오후' if '오후' in str(col_name) else None)
                                
                                if ((time_slot == '오전' and is_special_day and worker_name == weekend_oncall_worker) or
                                    (time_slot == '오후' and worker_name == oncall_person) or
                                    (col_name == '오전당직(온콜)')):
                                    cell.font = style_args['duty_font']
                                
                                if time_slot:
                                    # 상태 우선순위에 따라 worker_data 조회
                                    worker_data = df_final_unique[
                                        (df_final_unique['날짜'] == date_str_lookup) & 
                                        (df_final_unique['시간대'] == time_slot) & 
                                        (df_final_unique['근무자'] == worker_name) &
                                        (df_final_unique['상태'].isin(['근무', '보충', '추가보충', '제외', '추가제외', '휴가']))
                                    ].sort_values(by='색상_우선순위', ascending=False)  # 높은 우선순위 선택
                                    
                                    if not worker_data.empty:
                                        worker_info = worker_data.iloc[0]
                                        color = worker_info['색상']
                                        status = worker_info['상태']
                                        cell.fill = PatternFill(start_color=style_args['color_map'].get(color, 'FFFFFF'), fill_type='solid')
                                        memo = worker_info['메모']
                                        if memo and any(keyword in memo for keyword in ['보충', '이동', '제외', '휴가']):
                                            cell.comment = Comment(f"{status}: {memo}", "Schedule Bot")

                # 요약 테이블 추가
                append_summary_table_to_excel(ws_final, style_args['summary_df'], style_args)

                # 열 너비 설정
                ws_final.column_dimensions['A'].width = 11
                for col in ws_final.columns:
                    if col[0].column_letter != 'A':
                        ws_final.column_dimensions[col[0].column_letter].width = 9
                
                return wb_final
            
            summary_df = build_summary_table(
                df_cumulative,
                all_names,
                next_month_str,
                df_final_unique=df_final_unique_sorted
            )

            wb_final = create_final_schedule_excel(
                df_excel_original=df_excel,
                df_schedule=df_schedule,
                df_final_unique=df_final_unique,
                special_schedules=special_schedules,
                color_map=color_map,
                font_name=font_name,
                duty_font=duty_font,
                font=default_font,
                bold_font=Font(name=font_name, size=9, bold=True),
                border=border,
                special_day_fill=special_day_fill,
                empty_day_fill=empty_day_fill,
                default_day_fill=default_day_fill,
                summary_df=summary_df  # 추가
            )
            output_final = io.BytesIO()
            wb_final.save(output_final)
            output_final.seek(0)
            
            month_dt = datetime.strptime(month_str, "%Y년 %m월")
            next_month_dt = (month_dt + relativedelta(months=1)).replace(day=1)
            next_month_str = next_month_dt.strftime("%Y년 %-m월")
            month_start = month_dt.replace(day=1)
            month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

            try:
                gc = get_gspread_client()
                sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                
                # 이 함수가 이제 동적으로 열이 생성된 데이터프레임을 반환합니다.
                df_schedule_to_save = transform_schedule_for_checking(df_final_unique, df_excel, month_start, month_end)
                
                try:
                    worksheet_schedule = sheet.worksheet(f"{month_str} 스케줄 ver1.0")
                except gspread.exceptions.WorksheetNotFound:
                    worksheet_schedule = sheet.add_worksheet(title=f"{month_str} 스케줄 ver1.0", rows=1000, cols=50) # cols는 여유있게
                
                update_sheet_with_retry(worksheet_schedule, [df_schedule_to_save.columns.tolist()] + df_schedule_to_save.astype(str).values.tolist())
                
                try:
                    # 시트 이름을 "누적 요약"으로 변경하여 기존 시트와 구분하는 것을 권장합니다.
                    worksheet_summary = sheet.worksheet(f"{next_month_str} 누적 ver1.0")
                except gspread.exceptions.WorksheetNotFound:
                    worksheet_summary = sheet.add_worksheet(title=f"{next_month_str} 누적 ver1.0", rows=100, cols=50)
                
                # [핵심] df_cumulative_next 대신 summary_df 변수를 사용하여 시트를 업데이트합니다.
                summary_df_to_save = build_summary_table(
                    df_cumulative, all_names, next_month_str,
                    df_final_unique=df_final_unique_sorted
                )

                update_sheet_with_retry(worksheet_summary, [summary_df_to_save.columns.tolist()] + summary_df_to_save.values.tolist())

            except Exception as e:
                st.error(f"Google Sheets 저장 중 오류 발생: {e}")
                st.stop()
            
            st.session_state.assignment_results = {
                "output_checking": output,
                "output_final": output_final,
                "df_excel": df_excel,
                "df_cumulative_next": df_cumulative_next,
                "summary_df": summary_df,  # summary_df 추가
                "request_logs": st.session_state.request_logs,
                "swap_logs": st.session_state.swap_logs,
                "adjustment_logs": st.session_state.adjustment_logs,
                "oncall_logs": st.session_state.oncall_logs,
                "df_final_unique_sorted": df_final_unique_sorted,
                "df_schedule": df_schedule,       
            }

    month_dt = datetime.strptime(month_str, "%Y년 %m월")
    next_month_dt = (month_dt + relativedelta(months=1)).replace(day=1)
    next_month_str = next_month_dt.strftime("%Y년 %-m월")
    month_start = month_dt.replace(day=1)
    month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

    if st.session_state.get('assigned', False):
        results = st.session_state.get('assignment_results', {})
        if results:
            with st.expander("🔍 배정 과정 상세 로그 보기", expanded=True):
                st.markdown("**📋 요청사항 반영 로그**"); st.code("\n".join(results.get("request_logs", [])) if results.get("request_logs") else "반영된 요청사항(휴가/학회)이 없습니다.", language='text')
                st.markdown("---"); st.markdown("**🔄 대체 보충/휴근 로그 (1:1 이동)**"); st.code("\n".join(results.get("swap_logs", [])) if results.get("swap_logs") else "일반 제외/보충이 발생하지 않았습니다.", language='text')
                st.markdown("---"); st.markdown("**📞 오전당직(온콜) 배정 조정 로그**"); st.code("\n".join(results.get("oncall_logs", [])) if results.get("oncall_logs") else "모든 오전당직(온콜)이 누적 횟수에 맞게 정상 배정되었습니다.", language='text')
            

            if results.get("df_excel") is not None and not results["df_excel"].empty:
                # 1. 엑셀 원본 데이터는 보존하고, 화면 표시용 복사본을 생성합니다.
                df_for_display = results.get("df_excel").copy()
                
                # 2. 상태 정보를 담고 있는 데이터프레임들을 불러옵니다.
                df_final_unique = results.get("df_final_unique_sorted")
                df_schedule = results.get("df_schedule")
                
                if df_final_unique is not None and df_schedule is not None:
                    # 3. 빠른 조회를 위해 (날짜, 시간대, 근무자)를 키로 하는 상태 정보 딕셔너리를 만듭니다.
                    status_lookup = {}
                    for _, row in df_final_unique.iterrows():
                        key = (row['날짜'], row['시간대'], row['근무자'])
                        status_lookup[key] = row['상태']

                    # 4. 화면에 표시할 복사본 데이터프레임의 내용을 업데이트합니다.
                    for idx, row in df_for_display.iterrows():
                        date_str = df_schedule.at[idx, '날짜'] # YYYY-MM-DD 형식의 날짜

                        for col_name in df_for_display.columns:
                            worker_name = row[col_name]
                            if worker_name and pd.notna(worker_name):
                                time_slot = '오전' if str(col_name).isdigit() else ('오후' if '오후' in str(col_name) else None)
                                
                                if time_slot:
                                    key = (date_str, time_slot, worker_name)
                                    status = status_lookup.get(key)
                                    
                                    # '근무', '당직' 등 기본 상태가 아닐 경우에만 상태를 괄호로 추가합니다.
                                    if status and status not in ['근무', '당직', '기본']:
                                        df_for_display.at[idx, col_name] = f"{worker_name}({status})"
                
                st.write(" ")
                st.markdown(f"**➕ {next_month_str} 배정 스케줄**")
                # 5. 상태 정보가 추가된 복사본을 화면에 출력합니다.
                st.dataframe(df_for_display, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ 배정 테이블 데이터를 불러올 수 없습니다. 새로고침 후 다시 시도해주세요.")

            if results.get("summary_df") is not None and not results["summary_df"].empty:
                st.write(" ")
                st.markdown(f"**➕ {next_month_str} 누적 테이블**")
                display_pivoted_summary_table(results["summary_df"])
            else:
                st.warning("⚠️ 요약 테이블 데이터를 불러올 수 없습니다. 새로고침 후 다시 시도해주세요.")

            st.divider()
            st.success(f"✅ {month_str} 스케줄 및 {next_month_str} 누적 테이블 ver1.0이 Google Sheets에 저장되었습니다.")
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 스케줄 ver1.0 다운로드",
                    data=results.get("output_final"),
                    file_name=f"{month_str} 스케줄 ver1.0.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_final_schedule_button",
                    use_container_width=True,
                    type="primary",
                )
            with col2:
                st.download_button(
                    label="📥 스케줄 ver1.0 다운로드 (배정 확인용)",
                    data=results.get("output_checking"),
                    file_name=f"{month_str} 스케줄 ver1.0 (배정 확인용).xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_checking_schedule_button",
                    use_container_width=True,
                    type="secondary",
                )