import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import random
import time
from datetime import datetime, date, timedelta
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu
import numpy as np
from dateutil.relativedelta import relativedelta
import platform

st.set_page_config(page_title="방배정", page_icon="", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# 세션 상태 초기화
def initialize_session_state():
    if "data_loaded" not in st.session_state:
        st.session_state["data_loaded"] = False
    if "df_room_request" not in st.session_state:
        st.session_state["df_room_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
    if "room_settings" not in st.session_state:
        st.session_state["room_settings"] = {
            "830_room_select": ['1', '8', '4', '7'],
            "900_room_select": ['10', '11', '12'],
            "930_room_select": ['2', '5', '6'],
            "1000_room_select": ['9', '3'],
            "1330_room_select": ['3', '4', '9', '2']
        }
    if "weekend_room_settings" not in st.session_state:
        st.session_state["weekend_room_settings"] = {}
    if "swapped_assignments" not in st.session_state:
        st.session_state["swapped_assignments"] = set()
    if "df_schedule_original" not in st.session_state:
        st.session_state["df_schedule_original"] = pd.DataFrame()
    if "manual_change_log" not in st.session_state:
        st.session_state["manual_change_log"] = []
    if "final_change_log" not in st.session_state:
        st.session_state["final_change_log"] = []
    if "saved_changes_log" not in st.session_state:
        st.session_state["saved_changes_log"] = []
    if "df_schedule_md_initial" not in st.session_state:
        st.session_state["df_schedule_md_initial"] = pd.DataFrame()
    if "swapped_assignments_log" not in st.session_state:
        st.session_state["swapped_assignments_log"] = []
    if "df_schedule" not in st.session_state:
        st.session_state["df_schedule"] = pd.DataFrame()
    if "df_swap_requests" not in st.session_state:
        st.session_state["df_swap_requests"] = pd.DataFrame(columns=[
            "RequestID", "요청일시", "요청자", "변경 요청", "변경 요청한 스케줄"
        ])
    if "worksheet_room_request" not in st.session_state:
        st.session_state["worksheet_room_request"] = None
    if "batch_apply_messages" not in st.session_state:
        st.session_state["batch_apply_messages"] = []
    if "assignment_results" not in st.session_state:
        st.session_state["assignment_results"] = None
    if "show_assignment_results" not in st.session_state:
        st.session_state["show_assignment_results"] = False
    if "df_cumulative_original" not in st.session_state:
        st.session_state["df_cumulative_original"] = pd.DataFrame()
    if "latest_cumulative_name" not in st.session_state:
        st.session_state["latest_cumulative_name"] = None


def clean_name(name):
    """이름 뒤에 붙는 (상태) 문자열을 제거합니다."""
    if not isinstance(name, str):
        return ""
    return re.sub(r'\s*\(.*\)', '', name).strip()

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"Google Sheets 인증 정보 로드 중 오류: {type(e).__name__} - {e}")
        return None

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='RAW')
            return True
        except Exception as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.error(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")
    return False

# --- find_latest_version 함수를 아래 코드로 완전히 교체하세요 ---

def find_latest_version(sheet, month_str, sheet_type):
    """
    주어진 월과 시트 타입에 해당하는 시트 중 가장 최신 버전을 찾습니다.
    '최종' 버전을 최우선으로 합니다.
    """
    versions = {}
    # 1. '최종' 버전 시트 이름을 먼저 정의합니다.
    final_version_name = f"{month_str} {sheet_type} 최종"
    
    all_worksheet_titles = [ws.title for ws in sheet.worksheets()]
    
    # 2. '최종' 버전이 있는지 확인하고, 있으면 즉시 반환합니다.
    if final_version_name in all_worksheet_titles:
        return final_version_name
            
    # 3. '최종' 버전이 없으면, 다른 버전(ver X.X 또는 기본)들을 찾습니다.
    pattern = re.compile(f"^{re.escape(month_str)} {re.escape(sheet_type)}(?: ver\s*(\d+\.\d+))?$")
    for title in all_worksheet_titles:
        match = pattern.match(title)
        if match:
            version_num_str = match.group(1)
            version_num = float(version_num_str) if version_num_str else 1.0
            versions[title] = version_num
    
    if not versions:
        return None
    
    # 4. 찾은 버전들 중 가장 최신 버전을 반환합니다.
    return max(versions, key=versions.get)

# 데이터 로드 함수
def load_data_page6_no_cache(month_str):
    try:
        gc = get_gspread_client()
        if gc is None:
            raise Exception("Failed to initialize gspread client")
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])

        # --- 스케줄 시트 로드 ---
        latest_schedule_name = find_latest_version(sheet, month_str, "스케줄") 
        if not latest_schedule_name:
            return pd.DataFrame(), pd.DataFrame(), None, pd.DataFrame(), pd.DataFrame(), None
        worksheet_schedule = sheet.worksheet(latest_schedule_name)
        df_schedule = pd.DataFrame(worksheet_schedule.get_all_records())
        if df_schedule.empty:
            return pd.DataFrame(), pd.DataFrame(), None, pd.DataFrame(), pd.DataFrame(), latest_schedule_name

        # --- 방배정 요청 시트 로드 ---
        try:
            worksheet_room_request = sheet.worksheet(f"{month_str} 방배정 요청")
        except gspread.exceptions.WorksheetNotFound:
            worksheet_room_request = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows=100, cols=10)
            worksheet_room_request.update('A1', [["이름", "분류", "날짜정보"]])
        df_room_request = pd.DataFrame(worksheet_room_request.get_all_records())

        # --- [수정] 누적 시트 로드 로직 변경 ---
        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        next_month_dt = target_month_dt + relativedelta(months=1)
        next_month_str = next_month_dt.strftime("%Y년 %-m월")

        latest_cumulative_name = find_latest_version(sheet, next_month_str, "누적")

        # [핵심 변경] 다음 달 누적 시트가 없으면 즉시 중단
        if not latest_cumulative_name:
            st.error(f"🚨 '{next_month_str} 누적' 시트를 찾을 수 없습니다. 방배정을 진행할 수 없습니다.")
            st.stop() # 여기서 스크립트 실행을 중단합니다.

        # [추가] 찾은 시트 이름을 세션에 저장하여 '저장' 버튼에서 사용
        st.session_state["latest_cumulative_name"] = latest_cumulative_name

        # 다음 달 누적 시트를 성공적으로 찾은 경우에만 아래 로직 실행
        st.info(f"'{latest_cumulative_name}' 시트에서 누적 데이터를 불러옵니다.")
        worksheet_cumulative = sheet.worksheet(latest_cumulative_name)
        all_values = worksheet_cumulative.get_all_values()
        if not all_values or len(all_values) < 2 or all_values[0][0] != '항목':
            st.error(f"🚨 '{latest_cumulative_name}' 시트의 형식이 올바르지 않습니다. A1셀에 '항목'이 있는지 확인해주세요.")
            st.stop()
        else:
            headers, data = all_values[0], all_values[1:]
            
            # [추가] 원본 데이터(transpose 전)를 DataFrame으로 만들어 세션 상태에 저장
            df_cumulative_original = pd.DataFrame(data, columns=headers)
            st.session_state["df_cumulative_original"] = df_cumulative_original
            
            # 기존 로직은 다른 기능에서 사용하므로 유지
            df_transposed = df_cumulative_original.set_index('항목')
            df_cumulative = df_transposed.transpose().reset_index().rename(columns={'index': '이름'})
            for col in ['오전누적', '오후누적', '오전당직 (목표)', '오후당직 (목표)']:
                if col in df_cumulative.columns:
                    df_cumulative[col] = pd.to_numeric(df_cumulative[col], errors='coerce').fillna(0).astype(int)
                    
        # --- 스케줄 변경요청 시트 로드 ---
        try:
            worksheet_swap_requests = sheet.worksheet(f"{month_str} 스케줄 변경요청")
        except gspread.exceptions.WorksheetNotFound:
            worksheet_swap_requests = sheet.add_worksheet(title=f"{month_str} 스케줄 변경요청", rows=100, cols=10)
            worksheet_swap_requests.update('A1', [["RequestID", "요청일시", "요청자", "변경 요청", "변경 요청한 스케줄"]])
        df_swap_requests = pd.DataFrame(worksheet_swap_requests.get_all_records())

        return df_schedule, df_room_request, worksheet_room_request, df_cumulative, df_swap_requests, latest_schedule_name

    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {type(e).__name__} - {e}")
        return pd.DataFrame(), pd.DataFrame(), None, pd.DataFrame(), pd.DataFrame(), None
    
def find_latest_schedule_version(sheet, month_str):
    """주어진 월에 해당하는 스케줄 시트 중 가장 최신 버전을 찾습니다. '최종'이 최우선입니다."""
    versions = {}
    
    # 1. '최종' 시트 존재 여부 확인 (가장 높은 우선순위)
    final_version_name = f"{month_str} 스케줄 최종"
    for ws in sheet.worksheets():
        if ws.title == final_version_name:
            return final_version_name
    
    # 2. 'ver X.X' 및 기본 버전 찾기 (기존 로직 유지)
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

@st.cache_data(ttl=300, show_spinner=False)
def load_schedule_data(month_str):
    """가장 최신 버전의 스케줄 데이터를 불러온 후, 필요한 열만 남도록 필터링합니다."""
    try:
        gc = get_gspread_client()
        if not gc:
            return pd.DataFrame(), None

        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        latest_version_name = find_latest_schedule_version(spreadsheet, month_str)
        
        if not latest_version_name:
            st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
            return pd.DataFrame(), None

        worksheet = spreadsheet.worksheet(latest_version_name)
        records = worksheet.get_all_records()
        
        if not records:
            st.info(f"'{latest_version_name}' 시트에 데이터가 없습니다.")
            return pd.DataFrame(), latest_version_name
            
        df = pd.DataFrame(records)
        if '날짜' not in df.columns:
            st.info(f"'{latest_version_name}' 시트의 형식이 올바르지 않습니다.")
            return pd.DataFrame(), latest_version_name

        # ▼▼▼ [핵심 수정] 데이터프레임 자체를 필요한 열만 남도록 필터링합니다. ▼▼▼
        essential_columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 5)]
        columns_to_keep = [col for col in essential_columns if col in df.columns]
        df = df[columns_to_keep]
        # ▲▲▲ [핵심 수정] ▲▲▲
            
        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(YEAR_STR + '년 ' + df['날짜'].astype(str), format='%Y년 %m월 %d일', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)
        
        return df, latest_version_name

    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (스케줄 데이터 로드): {str(e)}")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.info(f"{month_str} 스케줄이 아직 배정되지 않았습니다.")
        return pd.DataFrame(), None
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"스케줄 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

# 근무 가능 일자 계산
@st.cache_data(show_spinner=False)
def get_user_available_dates(name, df_schedule, month_start, month_end, month_str):
    available_dates = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    
    # [수정] month_str에서 연도를 동적으로 추출
    target_year = int(month_str.split('년')[0])

    personnel_columns = [str(i) for i in range(1, 13)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 6)]
    all_personnel = set(p.strip() for col in personnel_columns if col in df_schedule.columns for p in df_schedule[col].dropna().astype(str))

    if name not in all_personnel:
        st.warning(f"'{name}'님은 이번 달 근무자로 등록되어 있지 않습니다.")
        return []

    for _, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            if "월" in date_str:
                # [수정] 하드코딩된 연도 대신 target_year 사용
                date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=target_year).date()
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue

        if month_start <= date_obj <= month_end: # ◀ 이렇게 수정
            oncall_person = str(row.get('오전당직(온콜)', '')).strip()
            
            morning_personnel = set(str(row.get(str(i), '')).strip() for i in range(1, 13)) - {''}
            afternoon_personnel = set(str(row.get(f'오후{i}', '')).strip() for i in range(1, 6)) - {''}
            
            display_date = f"{date_obj.month}월 {date_obj.day}일 ({weekday_map[date_obj.weekday()]})"
            save_date_am = f"{date_obj.strftime('%Y-%m-%d')} (오전)"
            save_date_pm = f"{date_obj.strftime('%Y-%m-%d')} (오후)"
            
            if name in morning_personnel or name == oncall_person:
                available_dates.append((date_obj, f"{display_date} 오전", save_date_am))
            if name in afternoon_personnel:
                available_dates.append((date_obj, f"{display_date} 오후", save_date_pm))
    
    unique_dates = sorted(list(set(available_dates)), key=lambda x: x[0])
    return [(display_str, save_str) for _, display_str, save_str in unique_dates]

# df_schedule_md 생성 함수
def create_df_schedule_md(df_schedule):
    """화면 표시에 적합한 형태로 스케줄을 가공하고, 이름에서 (상태)를 제거합니다."""
    display_cols = ['날짜', '요일', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '오전당직(온콜)', '오후1', '오후2', '오후3', '오후4']
    df_schedule_md = pd.DataFrame(columns=display_cols)
    if df_schedule.empty:
        return df_schedule_md

    # 날짜, 요일, 온콜 정보는 미리 복사 (온콜 이름도 clean_name 적용)
    df_schedule_md['날짜'] = df_schedule['날짜']
    df_schedule_md['요일'] = df_schedule['요일']
    if '오전당직(온콜)' in df_schedule.columns:
        df_schedule_md['오전당직(온콜)'] = df_schedule['오전당직(온콜)'].apply(clean_name)

    # 행별로 근무자 재배치
    for idx, row in df_schedule.iterrows():
        oncall_person = clean_name(str(row.get('오전당직(온콜)', '')))
        
        # 오전 근무자 (이름 정리 및 중복 제거)
        am_original_cols = [str(i) for i in range(1, 13)]
        am_personnel_list = [
            clean_name(row[col]) for col in am_original_cols
            if col in df_schedule.columns and clean_name(row[col]) and clean_name(row[col]) != oncall_person
        ]
        am_personnel_unique = list(dict.fromkeys(am_personnel_list))
        am_display_cols = [str(i) for i in range(1, 12)]
        for i, col in enumerate(am_display_cols):
            df_schedule_md.at[idx, col] = am_personnel_unique[i] if i < len(am_personnel_unique) else ''
        
        # 오후 근무자 (이름 정리 및 중복 제거)
        pm_original_cols = [f'오후{i}' for i in range(1, 6)]
        pm_personnel_list = [
            clean_name(row[col]) for col in pm_original_cols
            if col in df_schedule.columns and clean_name(row[col]) and clean_name(row[col]) != oncall_person
        ]
        pm_personnel_unique = list(dict.fromkeys(pm_personnel_list))
        pm_display_cols = [f'오후{i}' for i in range(1, 5)]
        for i, col in enumerate(pm_display_cols):
            df_schedule_md.at[idx, col] = pm_personnel_unique[i] if i < len(pm_personnel_unique) else ''
            
    return df_schedule_md

# ✂️ 복사 & 붙여넣기용 최종 apply_schedule_swaps 함수
def apply_schedule_swaps(original_schedule_df, swap_requests_df, special_df):
    df_modified = original_schedule_df.copy()
    applied_count = 0
    total_requests = len(swap_requests_df) # --- 이 줄 추가 ---
    swapped_assignments = st.session_state.get("swapped_assignments", set())
    batch_change_log = []
    messages = []

    for _, request_row in swap_requests_df.iterrows():
        try:
            change_request_str = str(request_row.get('변경 요청', '')).strip()
            schedule_info_str = str(request_row.get('변경 요청한 스케줄', '')).strip()
            formatted_schedule_info = format_sheet_date_for_display(schedule_info_str)

            if '➡️' not in change_request_str: continue

            person_before, person_after = [p.strip() for p in change_request_str.split('➡️')]
            date_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', schedule_info_str)
            if not date_match: continue

            date_part, time_period_from_request = date_match.groups()
            date_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
            formatted_date_in_df = f"{date_obj.month}월 {date_obj.day}일"
            target_row_indices = df_modified[df_modified['날짜'] == formatted_date_in_df].index
            if target_row_indices.empty: continue
            target_row_idx = target_row_indices[0]

            all_cols = [str(i) for i in range(1, 18)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 10)]
            available_cols = [col for col in all_cols if col in df_modified.columns]
            
            on_call_person = clean_name(df_modified.at[target_row_idx, '오전당직(온콜)'])
            is_on_call_swap = (person_before == on_call_person)

            if is_on_call_swap:
                cols_with_person_before = [c for c in available_cols if clean_name(df_modified.at[target_row_idx, c]) == person_before]
                cols_with_person_after = [c for c in available_cols if clean_name(df_modified.at[target_row_idx, c]) == person_after]

                if not cols_with_person_before:
                    error_msg = f"❌ {formatted_schedule_info} - {change_request_str} 적용 실패: {formatted_date_in_df}에 '{person_before}' 당직 근무가 배정되어 있지 않습니다."
                    messages.append(('error', error_msg))
                    continue

                for col in cols_with_person_before: df_modified.at[target_row_idx, col] = person_after
                for col in cols_with_person_after: df_modified.at[target_row_idx, col] = person_before

                swapped_assignments.add((formatted_date_in_df, '오전', person_after))
                swapped_assignments.add((formatted_date_in_df, '오후', person_after))
                swapped_assignments.add((formatted_date_in_df, '오전당직(온콜)', person_after))
                batch_change_log.append({
                    '날짜': f"{formatted_date_in_df} ({'월화수목금토일'[date_obj.weekday()]}) - 당직 변경",
                    '변경 전 인원': person_before, '변경 후 인원': person_after,
                })
                applied_count += 1
                continue

            target_cols = [str(i) for i in range(1, 18)] if time_period_from_request == '오전' else [f'오후{i}' for i in range(1, 10)]
            available_target_cols = [col for col in target_cols if col in df_modified.columns]
            
            matched_cols = [col for col in available_target_cols if clean_name(df_modified.loc[target_row_idx, col]) == person_before]
            if not matched_cols:
                error_msg = f"❌ {formatted_schedule_info} - {change_request_str} 적용 실패: {formatted_date_in_df} '{time_period_from_request}'에 '{person_before}'이(가) 배정되어 있지 않습니다."
                messages.append(('error', error_msg))
                continue
            
            personnel_in_target_period = {clean_name(df_modified.loc[target_row_idx, col]) for col in available_target_cols}
            if person_after in personnel_in_target_period:
                warning_msg = f"🟡 {formatted_schedule_info} - {change_request_str} 적용 건너뜀: '{person_after}'님은 이미 {formatted_date_in_df} '{time_period_from_request}' 근무에 배정되어 있습니다."
                messages.append(('warning', warning_msg))
                continue
            
            for col in matched_cols:
                df_modified.at[target_row_idx, col] = person_after
            swapped_assignments.add((formatted_date_in_df, time_period_from_request, person_after))
            batch_change_log.append({
                '날짜': f"{formatted_schedule_info}", '변경 전 인원': person_before, '변경 후 인원': person_after,
            })
            applied_count += 1

        except Exception as e:
            messages.append(('error', f"요청 처리 중 오류 발생: {type(e).__name__} - {str(e)}"))
            continue
    
    # --- 이 부분 수정 ---
    if applied_count > 0 or messages: # 성공, 경고, 오류 중 하나라도 있으면 요약 메시지 생성
        summary = f"✅ 총 {total_requests}건 중 {applied_count}건의 스케줄 변경 요청이 성공적으로 반영되었습니다."
        messages.insert(0, ('success', summary))
    elif not messages:
        messages.append(('info', "새롭게 적용할 스케줄 변경 요청이 없습니다."))

    st.session_state["swapped_assignments_log"] = batch_change_log
    st.session_state["swapped_assignments"] = swapped_assignments

    return create_df_schedule_md(df_modified), messages

def format_sheet_date_for_display(date_string):
    match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', date_string)
    if match:
        date_part, shift_part = match.groups()
        try:
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
            weekday_str = ['월', '화', '수', '목', '금', '토', '일'][dt_obj.weekday()]
            return f"{dt_obj.month}월 {dt_obj.day}일 ({weekday_str}) - {shift_part}"
        except ValueError:
            return date_string
    return date_string

def format_date_str_to_display(date_str, weekday, time_period):
    if '요일' in weekday:
        weekday = weekday.replace('요일', '')
    return f"{date_str} ({weekday}) - {time_period}"

@st.cache_data(ttl=600, show_spinner=False)
def load_special_schedules(month_str):
    """
    'YYYY년 토요/휴일 스케줄' 시트에서 데이터를 로드하는 함수입니다.
    """
    try:
        gc = get_gspread_client()
        if not gc: return pd.DataFrame()

        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        
        # month_str에서 연도를 동적으로 추출하여 시트 이름을 생성합니다.
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"
        
        worksheet = spreadsheet.worksheet(sheet_name)
        records = worksheet.get_all_records()
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df.fillna('', inplace=True)
        return df
        
    except gspread.exceptions.WorksheetNotFound:
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"
        st.info(f"'{sheet_name}' 시트를 찾을 수 없습니다. 토요/휴일 일정 없이 진행합니다.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"토요/휴일 데이터 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame()

# 메인
from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.now(kst)
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
month_str = next_month_date.strftime("%Y년 %-m월")
month_str = '2025년 10월'
YEAR_STR = month_str.split('년')[0]
this_month_start = next_month_date.replace(day=1)

# 다음 달의 마지막 날 계산
if this_month_start.month == 12:
    this_month_end = date(this_month_start.year, 12, 31)
else:
    this_month_end = (date(this_month_start.year, this_month_start.month + 1, 1) - timedelta(days=1))

# 다음 달 계산 (기존 코드 유지, 필요 시 사용)
if today.month == 12:
    next_month_start = date(today.year + 1, 1, 1)
else:
    next_month_start = date(today.year, today.month + 1, 1)
next_month_end = (next_month_start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

# 세션 상태 초기화
initialize_session_state()

st.header("🚪 방배정", divider='rainbow')

# 새로고침 버튼
st.write("- 먼저 새로고침 버튼으로 최신 데이터를 불러온 뒤, 배정을 진행해주세요.")
if st.button("🔄 새로고침 (R)"):
    st.session_state["data_loaded"] = False
    st.cache_data.clear()

    # 모든 로그 및 메시지 초기화
    if "final_change_log" in st.session_state:
        st.session_state["final_change_log"] = []
    if "swapped_assignments_log" in st.session_state:
        st.session_state["swapped_assignments_log"] = []
    if "batch_apply_messages" in st.session_state:
        st.session_state["batch_apply_messages"] = []
    
    # 수정된 스케줄 및 결과 초기화
    if "df_schedule_md_modified" in st.session_state:
        del st.session_state["df_schedule_md_modified"]
        
    # >>>>>>>>> [핵심 수정] 이 두 줄을 추가/수정하세요 <<<<<<<<<
    if "assignment_results" in st.session_state:
        del st.session_state["assignment_results"]
    st.session_state.show_assignment_results = False # 결과 보기 스위치 끄기
    
    st.rerun()

df_schedule, loaded_version = load_schedule_data(month_str)

if df_schedule.empty:
    st.stop()
else:
    # 불러온 스케줄의 버전 정보를 화면에 표시합니다.
    if loaded_version:
        # ' 스케줄 '을 기준으로 시트 이름을 분리하여 마지막 부분을 버전으로 인식합니다.
        # 예: "2025년 10월 스케줄 ver1.0" -> "ver1.0"
        # 예: "2025년 10월 스케줄 최종" -> "최종"
        version_str = loaded_version.split(' 스케줄 ')[-1]

        # version_str이 비어있다면 "2025년 10월 스케줄"과 같은 기본 시트입니다.
        if version_str:
            st.info(f"현재 표시되는 스케줄 버전은 '**{version_str}**' 입니다.")
        else:
            st.info(f"현재 표시되는 스케줄 버전은 **기본 버전**입니다.")

# 데이터 로드 (페이지 첫 로드 시에만 실행)
if not st.session_state.get("data_loaded", False):
    with st.spinner("데이터를 로드하고 있습니다..."):
        # 반환값에 loaded_version 추가
        df_schedule, df_room_request, worksheet_room_request, df_cumulative, df_swap_requests, loaded_version = load_data_page6_no_cache(month_str)

        # 로드된 데이터를 세션 상태에 저장
        st.session_state["df_schedule"] = df_schedule if df_schedule is not None else pd.DataFrame()
        st.session_state["df_schedule_original"] = st.session_state["df_schedule"].copy()
        st.session_state["df_room_request"] = df_room_request if df_room_request is not None else pd.DataFrame()
        st.session_state["worksheet_room_request"] = worksheet_room_request
        st.session_state["df_cumulative"] = df_cumulative if df_cumulative is not None else pd.DataFrame()
        st.session_state["df_swap_requests"] = df_swap_requests if df_swap_requests is not None else pd.DataFrame()
        st.session_state["df_schedule_md"] = create_df_schedule_md(st.session_state["df_schedule"])
        st.session_state["df_schedule_md_initial"] = st.session_state["df_schedule_md"].copy()
        st.session_state["loaded_version"] = loaded_version # 버전 정보 세션에 저장

        special_schedules_data = []
        special_dates_data = set()
        special_df_data = pd.DataFrame() # 기본 빈 데이터프레임

        try:
            gc = get_gspread_client()
            spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
            target_year = month_str.split('년')[0]
            special_sheet_name = f"{target_year}년 토요/휴일 스케줄"
            worksheet = spreadsheet.worksheet(special_sheet_name)
            schedule_records = worksheet.get_all_records()

            if schedule_records:
                df_yearly = pd.DataFrame(schedule_records)
                df_yearly['날짜_dt'] = pd.to_datetime(df_yearly['날짜'])
                
                target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
                special_df_data = df_yearly[
                    (df_yearly['날짜_dt'].dt.year == target_month_dt.year) &
                    (df_yearly['날짜_dt'].dt.month == target_month_dt.month)
                ].copy()

                for _, row in special_df_data.iterrows():
                    date_obj = row['날짜_dt'].date()
                    formatted_date_str = f"{date_obj.month}월 {date_obj.day}일"
                    personnel_str = row.get('근무', '')
                    personnel = [p.strip() for p in personnel_str.split(',')] if personnel_str else []
                    special_schedules_data.append((date_obj, formatted_date_str, personnel))
                    special_dates_data.add(formatted_date_str)
        
        except gspread.exceptions.WorksheetNotFound:
            st.info(f"'{special_sheet_name}' 시트를 찾을 수 없어 토요/휴일 정보 없이 진행합니다.")
        except Exception as e:
            st.error(f"토요/휴일 데이터 로드 중 오류: {e}")

        st.session_state["special_schedules"] = special_schedules_data
        st.session_state["special_dates"] = special_dates_data
        st.session_state["special_df"] = special_df_data

        st.session_state["data_loaded"] = True

# 근무자 명단 수정
st.divider()
st.subheader("📋 스케줄 변경 요청 목록")
if "df_schedule" not in st.session_state or st.session_state["df_schedule"].empty:
    st.warning("⚠️ 스케줄 데이터가 로드되지 않았습니다. 새로고침 버튼을 눌러 데이터를 다시 로드해주세요.")
    st.stop()

# --- 표시할 데이터프레임 결정 ---
# data_editor에 들어갈 데이터를 먼저 결정합니다. 이것이 현재 화면의 기준이 됩니다.
df_to_display = st.session_state.get("df_schedule_md_modified", st.session_state.get("df_schedule_md_initial", pd.DataFrame()))

# --- '스케줄 변경 요청 목록' 섹션 ---
df_swaps_raw = st.session_state.get("df_swap_requests", pd.DataFrame())
if not df_swaps_raw.empty:
    cols_to_display = {'요청일시': '요청일시', '요청자': '요청자', '변경 요청': '변경 요청', '변경 요청한 스케줄': '변경 요청한 스케줄'}
    existing_cols = [col for col in cols_to_display.keys() if col in df_swaps_raw.columns]
    df_swaps_display = df_swaps_raw[existing_cols].rename(columns=cols_to_display)
    if '변경 요청한 스케줄' in df_swaps_display.columns:
        df_swaps_display['변경 요청한 스케줄'] = df_swaps_display['변경 요청한 스케줄'].apply(format_sheet_date_for_display)
    st.dataframe(df_swaps_display, use_container_width=True, hide_index=True)

    # >>>>>>>>> [핵심 수정] '일괄 적용' 전 상태일 때만 아래의 충돌 검사를 실행 <<<<<<<<<
    if "df_schedule_md_modified" not in st.session_state:
        # --- 충돌 경고 로직 (수정됨) ---
        request_sources = []
        request_destinations = []

        schedule_df_to_check = st.session_state.get("df_schedule_original", pd.DataFrame()) # 원본 스케줄로 검사
        target_year = int(month_str.split('년')[0])

        for index, row in df_swaps_raw.iterrows():
            change_request_str = str(row.get('변경 요청', '')).strip()
            schedule_info_str = str(row.get('변경 요청한 스케줄', '')).strip()
            
            if '➡️' in change_request_str and schedule_info_str:
                person_before, person_after = [p.strip() for p in change_request_str.split('➡️')]
                
                is_on_call_request = False
                date_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', schedule_info_str)
                if date_match:
                    date_part, time_period = date_match.groups()
                    try:
                        date_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
                        formatted_date_in_df = f"{date_obj.month}월 {date_obj.day}일"
                        
                        target_row = schedule_df_to_check[schedule_df_to_check['날짜'] == formatted_date_in_df]
                        
                        if not target_row.empty:
                            on_call_person_of_the_day = clean_name(target_row.iloc[0].get('오전당직(온콜)', ''))
                            if person_before == on_call_person_of_the_day:
                                is_on_call_request = True
                    except Exception:
                        pass 
                
                if not is_on_call_request:
                    request_sources.append(f"{person_before} - {schedule_info_str}")
                
                if date_match:
                    date_part, time_period = date_match.groups()
                    request_destinations.append((date_part, time_period, person_after))

        # [검사 1: 출처 충돌]
        source_counts = Counter(request_sources)
        source_conflicts = [item for item, count in source_counts.items() if count > 1]
        if source_conflicts:
            # 1. 기본 경고 메시지 생성
            warning_message = (
                "⚠️ **요청 출처 충돌**: 동일한 근무에 대한 변경 요청이 2개 이상 있습니다. "
                "목록의 가장 위에 있는 요청이 먼저 반영되며, 이후 요청은 무시될 수 있습니다."
            )
            # 2. 충돌 항목들을 리스트에 저장
            conflict_details = []
            for conflict_item in source_conflicts:
                person, schedule = conflict_item.split(' - ', 1)
                formatted_schedule = format_sheet_date_for_display(schedule)
                conflict_details.append(f"- '{person}' 님의 {formatted_schedule} 근무 요청이 중복되었습니다.")
            
            # 3. 모든 메시지를 합쳐서 st.warning으로 한 번에 출력
            warning_message += "\n" + "\n".join(conflict_details)
            st.warning(warning_message)

        # [검사 2: 도착지 중복]
        dest_counts = Counter(request_destinations)
        dest_conflicts = [item for item, count in dest_counts.items() if count > 1]
        if dest_conflicts:
            # 1. 기본 경고 메시지 생성
            warning_message = (
                "⚠️ **요청 도착지 중복**: 한 사람이 같은 날, 같은 시간대에 여러 근무를 받게 되는 요청이 있습니다. "
                "이 경우, 먼저 처리되는 요청만 반영됩니다."
            )
            # 2. 충돌 항목들을 리스트에 저장
            conflict_details = []
            for date, period, person in dest_conflicts:
                formatted_date = format_sheet_date_for_display(f"{date} ({period})")
                conflict_details.append(f"- '{person}' 님이 {formatted_date} 근무에 중복으로 배정될 가능성이 있습니다.")

            # 3. 모든 메시지를 합쳐서 st.warning으로 한 번에 출력
            warning_message += "\n" + "\n".join(conflict_details)
            st.warning(warning_message)
else:
    st.info("표시할 교환 요청 데이터가 없습니다.")

st.divider()
st.subheader("✍️ 스케줄 수정")
st.write("- 요청사항을 **일괄 적용/취소**하거나, 셀을 더블클릭하여 직접 수정한 후 **최종 저장 버튼**을 누르세요.")

# 표시할 데이터프레임 결정
df_to_display = st.session_state.get("df_schedule_md_modified", st.session_state.get("df_schedule_md_initial", pd.DataFrame()))

col1, col2 = st.columns(2)
with col1:
    if st.button("🔄 요청사항 일괄 적용"):
        df_swaps = st.session_state.get("df_swap_requests", pd.DataFrame())
        if not df_swaps.empty:
            modified_schedule, messages = apply_schedule_swaps(
                st.session_state.get("df_schedule_md_modified", st.session_state["df_schedule_original"]), # 수정된 스케줄 기반으로 계속 수정
                df_swaps,
                st.session_state.get("special_df", pd.DataFrame())
            )
            st.session_state["df_schedule_md_modified"] = modified_schedule
            
            # 💡 [로그 수정] 기존 로그에 새로운 일괄 적용 로그를 추가
            existing_log = st.session_state.get("final_change_log", [])
            new_batch_log = st.session_state.get("swapped_assignments_log", [])
            st.session_state["final_change_log"] = existing_log + new_batch_log
            
            st.session_state["batch_apply_messages"] = messages
            st.rerun()
        else:
            st.session_state["batch_apply_messages"] = [('info', "ℹ️ 처리할 교환 요청이 없습니다.")]
            st.rerun()
            
with col2:
    if st.button("⏪ 적용 취소", disabled="df_schedule_md_modified" not in st.session_state):
        if "df_schedule_md_modified" in st.session_state:
            del st.session_state["df_schedule_md_modified"]
        
        st.session_state["swapped_assignments_log"] = []
        st.session_state["final_change_log"] = []
        st.session_state["batch_apply_messages"] = [('info', "변경사항이 취소되고 원본 스케줄로 돌아갑니다.")]
        st.rerun()

# 세션에 저장된 메시지를 항상 표시하는 로직 추가 (수정됨)
if "batch_apply_messages" in st.session_state and st.session_state["batch_apply_messages"]:
    messages = st.session_state["batch_apply_messages"]
    
    # 메시지를 종류별로 분리
    summary_msg = ""
    error_details = []
    warning_details = []
    info_msgs = []

    for msg_type, msg_text in messages:
        if msg_type == 'success':
            summary_msg = msg_text
        elif msg_type == 'error':
            error_details.append(f"• {msg_text[2:]}") # 이모티콘 제거 후 리스트 아이템으로 추가
        elif msg_type == 'warning':
            warning_details.append(f"• {msg_text[2:]}")
        elif msg_type == 'info':
            info_msgs.append(msg_text)

    # 'info' 타입의 메시지는 그대로 표시
    for msg in info_msgs:
        st.info(msg)

    # 요약 및 상세 로그 표시
    if summary_msg or error_details or warning_details:
        if summary_msg:
            st.success(summary_msg)
        
        # '적용됨' 로그는 batch_change_log에서 가져옴
        success_log = st.session_state.get("swapped_assignments_log", [])
        success_details = [
            f"• {log['날짜']}: {log['변경 전 인원']} ➡️ {log['변경 후 인원']}으로 변경되었습니다."
            for log in success_log
        ]

        with st.expander("🔍 스케줄 변경 상세 로그 보기", expanded=True):
            st.write("**⛔️ 요청사항 적용 불가**")
            error_text = "\n".join(sorted(error_details)) if error_details else "해당 없음"
            st.code(error_text, language='text')
            
            st.divider()
            
            st.write("**⚠️ 요청사항 적용 건너뜀**")
            warning_text = "\n".join(sorted(warning_details)) if warning_details else "해당 없음"
            st.code(warning_text, language='text')
            
            st.divider()
            
            st.write("**✅ 요청사항 적용됨**")
            success_text = "\n".join(sorted(success_details)) if success_details else "해당 없음"
            st.code(success_text, language='text')

    # 메시지를 표시한 후 세션 상태에서 제거
    del st.session_state["batch_apply_messages"]

# 데이터 에디터 UI
edited_df_md = st.data_editor(df_to_display, use_container_width=True, key="schedule_editor", disabled=['날짜', '요일'])

# --- 실시간 변경사항 로그 ---
st.caption("📝 변경사항 미리보기")

# 1. 수동 변경사항 계산
base_df_for_manual_diff = st.session_state.get("df_schedule_md_modified", st.session_state.get("df_schedule_md_initial"))
manual_change_log = []
if not edited_df_md.equals(base_df_for_manual_diff):
    diff_indices = np.where(edited_df_md.ne(base_df_for_manual_diff))
    for row_idx, col_idx in zip(diff_indices[0], diff_indices[1]):
        date_str_raw = edited_df_md.iloc[row_idx, 0]
        col_name = edited_df_md.columns[col_idx]
        old_value = base_df_for_manual_diff.iloc[row_idx, col_idx]
        new_value = edited_df_md.iloc[row_idx, col_idx]
        try:
            original_row = st.session_state["df_schedule_original"][st.session_state["df_schedule_original"]['날짜'] == date_str_raw].iloc[0]
            weekday = original_row['요일']
        except IndexError:
            weekday = ''
        time_period = '오후' if col_name.startswith('오후') else '오전'
        formatted_date_str = f"{date_str_raw} ({weekday.replace('요일', '')}) - {time_period}"
        manual_change_log.append({
            '날짜': formatted_date_str, 
            '변경 전 인원': str(old_value), 
            '변경 후 인원': str(new_value)
        })

# 2. 일괄 적용 로그와 수동 변경 로그를 합쳐서 표시
batch_log = st.session_state.get("swapped_assignments_log", [])
st.session_state["final_change_log"] = batch_log + manual_change_log

if st.session_state["final_change_log"]:
    log_df = pd.DataFrame(st.session_state["final_change_log"])
    st.dataframe(log_df, use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")

# --- 기존 '변경사항 저장' 버튼 관련 코드를 아래 블록으로 교체하세요 ---

st.write(" ") # 여백

# --- 1. [수정] 버튼을 표시하기 전에 다음 버전 번호를 미리 계산 ---
loaded_version = st.session_state.get("loaded_version", "")
version_match = re.search(r'ver\s*(\d+)\.\d+', loaded_version)
if version_match:
    current_major_version = int(version_match.group(1))
    next_major_version = current_major_version + 1
else:
    # 기본 시트에서 작업한 경우, 다음 버전은 2.0
    next_major_version = 2

# --- 2. [수정] 동적 버튼 레이블 생성 ---
button_label = f"✍️ 스케줄 최종 버전으로 저장"

if st.button(button_label, type="primary", use_container_width=True):
    is_manually_edited = not edited_df_md.equals(st.session_state["df_schedule_md_initial"])
    if not is_manually_edited:
        st.info("ℹ️ 변경사항이 없어 저장할 내용이 없습니다.")
        st.stop()

    # (로그 기록 및 swapped_set 구성 로직은 동일)
    manual_change_log = []
    swapped_set = st.session_state.get("swapped_assignments", set())
    diff_indices = np.where(edited_df_md.ne(st.session_state["df_schedule_md_initial"]))
    for row_idx, col_idx in zip(diff_indices[0], diff_indices[1]):
        date_str_raw = edited_df_md.iloc[row_idx, 0]
        col_name = edited_df_md.columns[col_idx]
        old_value = st.session_state["df_schedule_md_initial"].iloc[row_idx, col_idx]
        new_value = edited_df_md.iloc[row_idx, col_idx]
        try:
            original_row = st.session_state["df_schedule_original"][st.session_state["df_schedule_original"]['날짜'] == date_str_raw].iloc[0]
            weekday = original_row['요일']
        except IndexError:
            weekday = ''
        time_period = '오후' if col_name.startswith('오후') else '오전'
        formatted_date_str = f"{date_str_raw} ({weekday.replace('요일', '')}) - {time_period}"
        manual_change_log.append({'날짜': formatted_date_str, '변경 전 인원': str(old_value), '변경 후 인원': str(new_value)})
        if str(new_value).strip():
            swapped_set.add((date_str_raw.strip(), time_period, str(new_value).strip()))
    st.session_state["final_change_log"] = manual_change_log
    st.session_state["swapped_assignments"] = swapped_set

    # --- [핵심 수정] 저장할 DataFrame 재구성 로직 변경 ---
    df_schedule_to_save = st.session_state["df_schedule_original"].copy()
    target_year = int(month_str.split('년')[0])
    def robust_parse_date(date_str, year=target_year):
        try:
            if "월" in str(date_str): return datetime.strptime(str(date_str), '%m월 %d일').replace(year=year).date()
            else: return pd.to_datetime(date_str).date()
        except: return None
    df_schedule_to_save['parsed_date'] = df_schedule_to_save['날짜'].apply(robust_parse_date)

    for _, edited_row in edited_df_md.iterrows():
        edited_date_obj = robust_parse_date(edited_row['날짜'])
        if edited_date_obj is None: continue
        target_indices = df_schedule_to_save[df_schedule_to_save['parsed_date'] == edited_date_obj].index
        if target_indices.empty: continue
        original_row_idx = target_indices[0]

        # 1. 수정된 내용 가져오기
        oncall_person = str(edited_row.get('오전당직(온콜)', '')).strip()
        
        am_editor_cols = [str(i) for i in range(1, 12)]
        am_personnel = [str(edited_row[col]).strip() for col in am_editor_cols if col in edited_row and pd.notna(edited_row[col]) and str(edited_row[col]).strip()]
        
        pm_editor_cols = [f'오후{i}' for i in range(1, 5)]
        pm_personnel = [str(edited_row[col]).strip() for col in pm_editor_cols if col in edited_row and pd.notna(edited_row[col]) and str(edited_row[col]).strip()]

        # 2. 저장할 DataFrame의 특정 열만 초기화 ('오후5' 이후는 건드리지 않음)
        cols_to_clear_am = [str(i) for i in range(1, 13)]
        for col in cols_to_clear_am:
            if col in df_schedule_to_save.columns: df_schedule_to_save.at[original_row_idx, col] = ''
        
        cols_to_clear_pm = [f'오후{i}' for i in range(1, 5)]
        for col in cols_to_clear_pm:
            if col in df_schedule_to_save.columns: df_schedule_to_save.at[original_row_idx, col] = ''

        # 3. 수정된 내용으로 다시 채워넣기
        df_schedule_to_save.at[original_row_idx, '오전당직(온콜)'] = oncall_person
        
        am_save_list = list(dict.fromkeys(am_personnel + ([oncall_person] if oncall_person else [])))
        for i, person in enumerate(am_save_list, 1):
            df_schedule_to_save.at[original_row_idx, str(i)] = person
            
        # [핵심] 오후는 pm_personnel (오후1~4) 만 순서대로 저장
        pm_save_list = list(dict.fromkeys(pm_personnel))
        for i, person in enumerate(pm_save_list, 1):
            if f'오후{i}' in df_schedule_to_save.columns: # 오후4까지만 채우도록 방어
                df_schedule_to_save.at[original_row_idx, f'오후{i}'] = person

    # --- Google Sheets에 저장 ---
    try:
        st.info("ℹ️ 수정된 스케줄을 Google Sheets에 저장합니다...")
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        
        df_schedule_to_save.drop(columns=['parsed_date'], inplace=True)
        
        sheet_name = f"{month_str} 스케줄 최종"

        try:
            worksheet_schedule = sheet.worksheet(sheet_name)
            st.warning(f"⚠️ '{sheet_name}' 시트가 이미 존재하여 덮어씁니다.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet_schedule = sheet.add_worksheet(title=sheet_name, rows=100, cols=30)
        
        columns_to_save = st.session_state["df_schedule_original"].columns.tolist()
        schedule_data = [columns_to_save] + df_schedule_to_save[columns_to_save].fillna('').values.tolist()
        
        if update_sheet_with_retry(worksheet_schedule, schedule_data):
            st.success(f"🎉 수정된 스케줄이 '{sheet_name}' 시트에 성공적으로 저장되었습니다.")
            st.session_state["schedule_sheet_name"] = sheet_name
            time.sleep(1.5)
            st.session_state['data_loaded'] = False 
            st.rerun()

    except Exception as e:
        st.error(f"Google Sheets 저장 중 오류 발생: {type(e).__name__} - {e}")
        st.stop()

# 방 설정 UI
st.divider()
st.subheader("⚙️ 방 설정")

special_schedules = []
special_dates = set()
special_df = pd.DataFrame(columns=["날짜", "근무", "당직"])

tab_weekday, tab_weekend = st.tabs(["평일 방 설정", "토요/휴일 방 설정"])

with tab_weekday:
    room_options = [str(i) for i in range(1, 13)]

    tab830, tab900, tab930, tab1000, tab1330 = st.tabs([
        "🕘 08:30", "🕘 09:00", "🕤 09:30", "🕙 10:00", "🕜 13:30 (오후)"
    ])
    with tab830:
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

with tab_weekend:
    # 페이지 로드 시 session_state에 저장된 데이터를 사용합니다.
    special_schedules = st.session_state.get("special_schedules", [])
    special_df = st.session_state.get("special_df", pd.DataFrame())
    
    # --- 토요/휴일 UI 렌더링 (이하 로직은 기존과 거의 동일) ---
    if special_schedules:
        for date_obj, date_str, personnel_for_day in sorted(special_schedules):
            weekday_map = {5: "토", 6: "일"}
            weekday_str = weekday_map.get(date_obj.weekday(), '휴')
            
            duty_person_for_date = ""
            if not special_df.empty:
                duty_row = special_df[special_df['날짜_dt'].dt.date == date_obj]
                if not duty_row.empty: 
                    duty_person_for_date = str(duty_row['당직'].iloc[0]).strip()

            expander_title = (f"🗓️ {date_str} ({weekday_str}) | "
                              f"근무: {len(personnel_for_day)}명 | "
                              f"당직: {duty_person_for_date or '없음'}")

            with st.expander(expander_title):
                col1, col2 = st.columns([1, 1])
                duty_room = None
                with col1:
                    st.markdown("###### **당직 방**")
                    if duty_person_for_date and duty_person_for_date != "당직 없음":
                        duty_room_options = ["선택 안 함"] + [str(i) for i in range(1, 13)]
                        default_duty_room = st.session_state.weekend_room_settings.get(date_str, {}).get("duty_room", "1")
                        duty_room = st.selectbox("당직 방 선택", duty_room_options, key=f"duty_room_{date_str}", 
                                                 index=duty_room_options.index(default_duty_room) if default_duty_room in duty_room_options else 0, label_visibility="collapsed")
                    else: 
                        st.info("당직 인원 없음")
                
                with col2:
                    st.markdown("###### **총 방 개수**")
                    default_room_count = st.session_state.weekend_room_settings.get(date_str, {}).get("total_room_count", len(personnel_for_day))
                    total_room_count = st.number_input("총 방 개수", min_value=0, max_value=12, value=default_room_count, 
                                                       key=f"total_rooms_{date_str}", label_visibility="collapsed")
                
                st.markdown("###### **방 번호 선택**")
                room_options = [str(i) for i in range(1, 13)]
                default_rooms = st.session_state.weekend_room_settings.get(date_str, {}).get("selected_rooms", room_options[:total_room_count])
                selected_rooms = st.multiselect("방 번호 선택", room_options, default=default_rooms, max_selections=total_room_count, 
                                                key=f"rooms_{date_str}", label_visibility="collapsed")

                st.session_state.weekend_room_settings[date_str] = {
                    "duty_room": duty_room if duty_room and duty_room != "선택 안 함" else None,
                    "total_room_count": total_room_count, "selected_rooms": selected_rooms
                }
    else: 
        st.info("이번 달은 토요/휴일 근무가 없습니다.")
        
all_selected_rooms = (st.session_state["room_settings"]["830_room_select"] + 
                     st.session_state["room_settings"]["900_room_select"] + 
                     st.session_state["room_settings"]["930_room_select"] + 
                     st.session_state["room_settings"]["1000_room_select"] + 
                     st.session_state["room_settings"]["1330_room_select"])

# 배정 요청 입력 UI
st.divider()
st.subheader("📋 배정 요청 관리")
st.write("- 모든 인원의 배정 요청을 추가 및 수정할 수 있습니다.")
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방", "12번방", 
            "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]

st.write(" ")
st.markdown("**🙋‍♂️ 현재 방배정 요청 목록**")
if st.session_state["df_room_request"].empty:
    st.info("☑️ 현재 방배정 요청이 없습니다.")
else:
    st.dataframe(st.session_state["df_room_request"], use_container_width=True, hide_index=True)


st.write(" ")

# 기존 save_to_gsheet 함수를 이 코드로 교체하세요.
def save_to_gsheet(name, categories, selected_save_dates, month_str, worksheet):
    try:
        if not name or not categories or not selected_save_dates:
            return None, "input_error"

        # [수정] 시트에서 직접 최신 데이터를 읽어 중복 검사
        all_requests = worksheet.get_all_records()
        df_live_requests = pd.DataFrame(all_requests)
        
        new_requests_to_append = []
        is_duplicate = False

        for category in categories:
            for date in selected_save_dates:
                date = date.strip()
                # 라이브 데이터로 중복 확인
                if not df_live_requests[(df_live_requests['이름'] == name) &
                                        (df_live_requests['날짜정보'] == date) &
                                        (df_live_requests['분류'] == category)].empty:
                    is_duplicate = True
                    continue # 중복이면 추가 목록에 넣지 않음
                
                new_requests_to_append.append([name, category, date])

        if not new_requests_to_append:
            # 추가할 요청은 없는데 중복이 발견된 경우
            return st.session_state["df_room_request"], "duplicate"

        # [수정] append_rows로 안전하게 새 요청만 추가
        worksheet.append_rows(new_requests_to_append, value_input_option='USER_ENTERED')
        
        # 성공 후 최신 데이터 다시 로드하여 반환
        updated_df = pd.DataFrame(worksheet.get_all_records())
        return updated_df, "success"

    except Exception as e:
        st.error(f"요청 추가 중 오류 발생: {type(e).__name__} - {str(e)}")
        return None, "error"

st.markdown("**🟢 방배정 요청 추가**")

# Reset flag to control form clearing
if "reset_form" not in st.session_state:
    st.session_state.reset_form = False

# Clear widget states on reset
if st.session_state.reset_form:
    for key in ["add_name", "add_categories", "add_dates", "add_time"]:
        if key in st.session_state:
            del st.session_state[key]
    st.session_state.reset_form = False  # Reset the flag after clearing

# --- UI 위젯 정의 ---
col1, col2, col3, col_button_add = st.columns([2, 2, 4, 1])

with col1:
    names = sorted([str(name).strip() for name in st.session_state["df_schedule"].iloc[:, 2:].stack().dropna().unique() if str(name).strip()])
    selected_name = st.selectbox(
        "근무자 선택",  # ✅ label을 직접 사용
        names,
        key="add_name",
        index=None,
        placeholder="근무자 선택",
    )

with col2:
    selected_categories = st.multiselect(
        "요청 분류", # ✅ label을 직접 사용
        요청분류,
        key="add_categories",
        default=[],
    )

with col3:
    processed_dates = {}
    date_to_obj_map = {}
    if st.session_state.get("add_name"):
        st.cache_data.clear()
        available_dates = get_user_available_dates(st.session_state.add_name, st.session_state["df_schedule"], this_month_start, this_month_end, month_str)
        for display_str, save_str in available_dates:
            parts = display_str.split(' ')
            date_part, time_part = ' '.join(parts[:-1]), parts[-1]
            if date_part not in processed_dates:
                processed_dates[date_part] = {}
                date_obj_str = save_str.split(' ')[0]
                date_to_obj_map[date_part] = datetime.strptime(date_obj_str, '%Y-%m-%d')
            processed_dates[date_part][time_part] = save_str
    
    date_options = sorted(processed_dates.keys(), key=lambda k: date_to_obj_map.get(k, datetime.max))
    
    sub_col_date, sub_col_time = st.columns([3, 1.5])
    with sub_col_date:
        selected_dates = st.multiselect(
            "요청 일자", # ✅ label을 직접 사용
            date_options,
            key="add_dates",
            default=[],
        )

    with sub_col_time:
        time_options = ["오전", "오후"]
        selected_time = st.selectbox(
            "시간대", # ✅ label을 직접 사용
            time_options,
            key="add_time",
            index=None,
        )

with col_button_add:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    add_button_clicked = st.button("📅 추가")

# 메시지 출력을 버튼 아래로
st.write(" ")  # 버튼과 메시지 사이 공백

# 버튼 클릭 처리
if add_button_clicked:
    name_to_add = st.session_state.get("add_name")
    categories_to_add = st.session_state.get("add_categories", [])
    dates_to_add = st.session_state.get("add_dates", [])
    time_to_add = st.session_state.get("add_time")

    if not name_to_add or not categories_to_add or not dates_to_add:
        st.session_state.add_request_status = "input_error"
    else:
        selected_save_dates = []
        if name_to_add:
            for date_display in dates_to_add:
                if date_display in processed_dates and time_to_add in processed_dates[date_display]:
                    selected_save_dates.append(processed_dates[date_display][time_to_add])
        
        if not selected_save_dates:
            st.session_state.add_request_status = "no_slot_error"
        else:
            with st.spinner("요청을 기록중입니다..."):
                df_room_request, status = save_to_gsheet(name_to_add, categories_to_add, selected_save_dates, month_str, st.session_state["worksheet_room_request"])
                st.session_state.add_request_status = status
                if df_room_request is not None:
                    st.session_state["df_room_request"] = df_room_request

# 메시지 출력
if "add_request_status" in st.session_state:
    status = st.session_state.add_request_status
    if status == "success":
        st.success("요청이 성공적으로 추가되었습니다.")
        st.session_state.reset_form = True
        time.sleep(2)  # 메시지 표시를 위해 2초 대기
    elif status == "duplicate":
        st.warning("이미 존재하는 요청사항입니다.")
    elif status == "input_error":
        st.error("근무자, 요청 분류, 요청 일자를 모두 선택해주세요.")
    elif status == "no_slot_error":
        st.warning("선택하신 날짜에 해당하는 근무 시간대가 없습니다.")
    
    # 상태 초기화 및 성공 시 새로고침
    del st.session_state.add_request_status
    if status == "success":
        st.rerun()

st.write(" ")
st.markdown("**🔴 방배정 요청 삭제**")
if not st.session_state["df_room_request"].empty:
    col0, col1, col_button_del = st.columns([2, 6, 1])
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
    # '방배정 요청 삭제'의 if delete_button_clicked: 블록 전체를 이 코드로 교체하세요.
    if delete_button_clicked:
        if not selected_employee or not selected_items:
            st.error("삭제할 근무자와 항목을 선택해주세요.")
        else:
            with st.spinner("요청을 삭제하는 중입니다..."):
                try:
                    worksheet = st.session_state["worksheet_room_request"]
                    all_requests = worksheet.get_all_records()
                    
                    # 삭제할 항목 정보를 set으로 만들어 빠른 조회 가능
                    items_to_delete_set = set(selected_items)
                    
                    # 삭제할 행의 인덱스를 뒤에서부터 찾아서 기록 (삭제 시 인덱스 밀림 방지)
                    rows_to_delete_indices = []
                    for i, record in reversed(list(enumerate(all_requests))):
                        record_str = f"{record.get('분류')} - {record.get('날짜정보')}"
                        if record.get('이름') == selected_employee and record_str in items_to_delete_set:
                            rows_to_delete_indices.append(i + 2) # gspread는 1-based, 헤더 포함
                    
                    # 찾은 행들을 삭제
                    if rows_to_delete_indices:
                        for row_idx in rows_to_delete_indices:
                            worksheet.delete_rows(row_idx)

                    st.cache_data.clear()
                    st.success("요청사항이 삭제되었습니다.")
                    time.sleep(1.5)
                    st.rerun()

                except Exception as e:
                    st.error(f"요청 삭제 중 오류 발생: {type(e).__name__} - {e}")
else:
    st.info("📍 방배정 요청이 없습니다.")

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

# 🔼 기존 assign_special_date 함수를 지우고 아래 코드로 교체하세요.

def assign_special_date(personnel_for_day, date_str, formatted_date, settings, special_df_for_month, df_room_request):
    """
    [수정된 함수]
    토요/휴일의 방배정을 수행합니다.
    - 1순위: 당직자 배정
    - 2순위: '방 지정 요청'이 있는 인원 배정
    - 3순위: 나머지 인원 랜덤 배정
    """
    assignment_dict = {}
    assigned_personnel = set()
    
    duty_room = settings.get("duty_room", None)
    selected_rooms = settings.get("selected_rooms", [])
    
    # 선호도 순서에 따라 선택된 방을 정렬
    preferred_room_order = ['1', '8', '4', '7', '10', '2', '5', '6', '9', '3']
    sorted_rooms = [room for room in preferred_room_order if room in selected_rooms]
    
    duty_person = None
    if not special_df_for_month.empty:
        try:
            target_year = int(month_str.split('년')[0])
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=target_year).date()
            duty_person_row = special_df_for_month[special_df_for_month['날짜_dt'].dt.date == date_obj]
            if not duty_person_row.empty:
                duty_person = duty_person_row['당직'].iloc[0]
        except Exception as e:
            st.warning(f"당직자 정보 조회 중 오류: {e}")

    # 1. 당직 인원 우선 배정
    if duty_person and duty_person in personnel_for_day and duty_room and duty_room != "선택 안 함":
        if duty_person not in assigned_personnel:
            assignment_dict[f"방({duty_room})"] = duty_person
            assigned_personnel.add(duty_person)

    # 2. 방 지정 요청 처리 (새로 추가된 로직)
    if not df_room_request.empty:
        # 현재 날짜(오전)에 해당하는 요청만 필터링 (토요/휴일 근무는 모두 '오전'으로 간주)
        requests_for_day = df_room_request[
            df_room_request['날짜정보'].str.startswith(formatted_date)
        ]

        for _, req in requests_for_day.iterrows():
            person = req['이름']
            category = req['분류']  # 예: "1번방"

            # 요청자가 오늘 근무자이고 아직 배정되지 않았는지 확인
            if person in personnel_for_day and person not in assigned_personnel:
                room_match = re.match(r'(\d+)번방', category)
                if room_match:
                    req_room_num = room_match.group(1)
                    slot_key = f"방({req_room_num})"

                    # 요청한 방이 오늘 운영되는 방이고, 아직 비어있는지 확인
                    if req_room_num in selected_rooms and slot_key not in assignment_dict:
                        assignment_dict[slot_key] = person
                        assigned_personnel.add(person)

    # 3. 나머지 인원을 랜덤 배정
    remaining_personnel = [p for p in personnel_for_day if p not in assigned_personnel]
    random.shuffle(remaining_personnel)
    
    # 배정되지 않은 방 목록
    unassigned_rooms = [r for r in sorted_rooms if f"방({r})" not in assignment_dict]

    for room in unassigned_rooms:
        if remaining_personnel:
            person = remaining_personnel.pop(0)
            assignment_dict[f"방({room})"] = person
            assigned_personnel.add(person)
    
    return assignment_dict, sorted_rooms

from collections import Counter
import random
import streamlit as st

def random_assign(personnel, slots, request_assignments, time_groups, total_stats, morning_personnel, afternoon_personnel, afternoon_duty_counts):
    assignment = [None] * len(slots)
    assigned_personnel_morning = set()
    assigned_personnel_afternoon = set()
    daily_stats = {
        'early': Counter(),
        'late': Counter(),
        'morning_duty': Counter(),
        'afternoon_duty': Counter(),
        'rooms': {str(i): Counter() for i in range(1, 13)},
        'time_room_slots': {}  # 시간대-방 쌍에 대한 Counter 객체를 딕셔너리로 관리
    }

    # time_room_slots 초기화
    for slot in slots:
        daily_stats['time_room_slots'][slot] = Counter()

    # total_stats['time_room_slots'] 초기화 (외부 코드 수정 없이 함수 내에서 처리)
    if 'time_room_slots' not in total_stats:
        total_stats['time_room_slots'] = {}
    for slot in slots:
        total_stats['time_room_slots'].setdefault(slot, Counter())

    morning_slots = [s for s in slots if s.startswith(('8:30', '9:00', '9:30', '10:00')) and '_당직' not in s]
    afternoon_slots = [s for s in slots if s.startswith('13:30')]
    afternoon_duty_slot = [s for s in slots if s.startswith('13:30') and s.endswith('_당직')]

    # 요청된 배정 처리
    for slot, person in request_assignments.items():
        if person in personnel and slot in slots:
            slot_idx = slots.index(slot)
            if assignment[slot_idx] is None:
                if (slot in morning_slots and person in morning_personnel) or \
                   (slot in afternoon_slots and person in afternoon_personnel):
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
                    daily_stats['time_room_slots'][slot][person] += 1
                    if slot.startswith('8:30') and '_당직' not in slot:
                        daily_stats['early'][person] += 1
                    elif slot.startswith('10:00'):
                        daily_stats['late'][person] += 1
                    if slot.startswith('8:30') and slot.endswith('_당직'):
                        daily_stats['morning_duty'][person] += 1
                    elif slot.startswith('13:30') and slot.endswith('_당직'):
                        daily_stats['afternoon_duty'][person] += 1
                else:
                    st.warning(f"{date_str}({slot}): {person}님의 방배정 요청 무시됨: 해당 시간대({'오전' if slot in morning_slots else '오후'})에 근무하지 않습니다.")
            else:
                st.warning(f"배정 요청 충돌: {person}을 {date_str}({slot})에 배정할 수 없음. 이미 배정됨: {assignment[slot_idx]}")

    # 오후 당직 배정
    afternoon_duty_slot_idx = slots.index(afternoon_duty_slot[0]) if afternoon_duty_slot else None
    if afternoon_duty_slot_idx is not None and assignment[afternoon_duty_slot_idx] is None:
        available_personnel = [p for p in afternoon_personnel if p not in assigned_personnel_afternoon]
        candidates = [p for p in available_personnel if p in afternoon_duty_counts and afternoon_duty_counts[p] > 0]
        
        if candidates:
            best_person = None
            min_duty_count = float('inf')
            for person in candidates:
                duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person]
                time_room_count = total_stats['time_room_slots'][afternoon_duty_slot[0]][person] + \
                                 daily_stats['time_room_slots'][afternoon_duty_slot[0]][person]
                score = duty_count * 100 + time_room_count
                if score < min_duty_count:
                    min_duty_count = score
                    best_person = person
            if best_person:
                assignment[afternoon_duty_slot_idx] = best_person
                assigned_personnel_afternoon.add(best_person)
                room_num = afternoon_duty_slot[0].split('(')[1].split(')')[0]
                daily_stats['rooms'][room_num][best_person] += 1
                daily_stats['time_room_slots'][afternoon_duty_slot[0]][best_person] += 1
                daily_stats['afternoon_duty'][best_person] += 1
                afternoon_duty_counts[best_person] -= 1
                if afternoon_duty_counts[best_person] <= 0:
                    del afternoon_duty_counts[best_person]

    # 오전 슬롯 배정
    morning_remaining = [p for p in morning_personnel if p not in assigned_personnel_morning]
    afternoon_remaining = [p for p in afternoon_personnel if p not in assigned_personnel_afternoon]
    remaining_slots = [i for i, a in enumerate(assignment) if a is None]
    
    morning_slot_indices = [i for i in remaining_slots if slots[i] in morning_slots]
    random.shuffle(morning_remaining)
    while morning_remaining and morning_slot_indices:
        best_person = None
        best_slot_idx = None
        min_score = float('inf')
        
        for slot_idx in morning_slot_indices:
            if assignment[slot_idx] is not None:
                continue
            slot = slots[slot_idx]
            
            for person in morning_remaining:
                time_room_count = total_stats['time_room_slots'][slot][person] + \
                                  daily_stats['time_room_slots'][slot][person]
                if slot.startswith('8:30') and '_당직' not in slot:
                    early_count = total_stats['early'][person] + daily_stats['early'][person]
                    score = early_count * 100 + time_room_count
                elif slot.startswith('10:00'):
                    late_count = total_stats['late'][person] + daily_stats['late'][person]
                    score = 10000 + late_count * 100 + time_room_count
                else:
                    score = 20000 + time_room_count
                
                if score < min_score:
                    min_score = score
                    best_person = person
                    best_slot_idx = slot_idx
        
        if best_slot_idx is None or best_person is None:
            st.warning(f"오전 슬롯 배정 불가: 더 이상 배정 가능한 인원 없음")
            break
        
        slot = slots[best_slot_idx]
        assignment[best_slot_idx] = best_person
        assigned_personnel_morning.add(best_person)
        morning_remaining.remove(best_person)
        morning_slot_indices.remove(best_slot_idx)
        remaining_slots.remove(best_slot_idx)
        room_num = slot.split('(')[1].split(')')[0]
        daily_stats['rooms'][room_num][best_person] += 1
        daily_stats['time_room_slots'][slot][best_person] += 1
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
            
            for person in afternoon_remaining:
                time_room_count = total_stats['time_room_slots'][slot][person] + \
                                  daily_stats['time_room_slots'][slot][person]
                duty_count = total_stats['afternoon_duty'][person] + daily_stats['afternoon_duty'][person] if slot.endswith('_당직') else float('inf')
                if slot.endswith('_당직'):
                    score = duty_count * 100 + time_room_count
                else:
                    score = time_room_count
                
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
        daily_stats['time_room_slots'][slot][best_person] += 1
        if slot.endswith('_당직'):
            daily_stats['afternoon_duty'][best_person] += 1

    # 남은 빈 슬롯 처리
    for slot_idx in range(len(slots)):
        if assignment[slot_idx] is None:
            slot = slots[slot_idx]
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
                    time_room_count = total_stats['time_room_slots'][slot][person] + \
                                      daily_stats['time_room_slots'][slot][person]
                    
                    if slot.startswith('8:30') and '_당직' not in slot:
                        score = early_count * 100 + time_room_count
                    elif slot.startswith('10:00'):
                        score = late_count * 100 + time_room_count
                    elif slot.startswith('8:30') and slot.endswith('_당직'):
                        score = morning_duty_count * 100 + time_room_count
                    elif slot.startswith('13:30') and slot.endswith('_당직'):
                        score = afternoon_duty_count * 100 + time_room_count
                    else:
                        score = time_room_count
                    
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
                        time_room_count = total_stats['time_room_slots'][slot][person] + \
                                          daily_stats['time_room_slots'][slot][person]
                        
                        if slot.startswith('8:30') and '_당직' not in slot:
                            score = early_count * 100 + time_room_count
                        elif slot.startswith('10:00'):
                            score = late_count * 100 + time_room_count
                        elif slot.startswith('8:30') and slot.endswith('_당직'):
                            score = morning_duty_count * 100 + time_room_count
                        elif slot.startswith('13:30') and slot.endswith('_당직'):
                            score = afternoon_duty_count * 100 + time_room_count
                        else:
                            score = time_room_count
                        
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
            daily_stats['time_room_slots'][slot][person] += 1
            if slot.startswith('8:30') and '_당직' not in slot:
                daily_stats['early'][person] += 1
            elif slot.startswith('10:00'):
                daily_stats['late'][person] += 1
            if slot.startswith('8:30') and slot.endswith('_당직'):
                daily_stats['morning_duty'][person] += 1
            elif slot.startswith('13:30') and slot.endswith('_당직'):
                daily_stats['afternoon_duty'][person] += 1

    # total_stats 업데이트
    for key in ['early', 'late', 'morning_duty', 'afternoon_duty']:
        total_stats[key].update(daily_stats[key])
    for room in daily_stats['rooms']:
        total_stats['rooms'][room].update(daily_stats['rooms'][room])
    for slot in daily_stats['time_room_slots']:
        total_stats['time_room_slots'][slot].update(daily_stats['time_room_slots'][slot])

    return assignment, daily_stats

st.divider()
st.write('➕ **누적 테이블 (오후당직 목표 편집)**')

# 세션 상태에 저장된 원본 df_cumulative_original 데이터를 가져옵니다.
df_cumulative_original = st.session_state.get("df_cumulative_original", pd.DataFrame())

if not df_cumulative_original.empty:
    # 1. 표시할 데이터 필터링: '오후당직 (목표)' 행만 선택
    df_to_display = df_cumulative_original[df_cumulative_original['항목'] == '오후당직 (목표)'].copy()

    # 2. data_editor를 사용하여 편집 가능한 테이블을 만듭니다.
    edited_df_display = st.data_editor(
        df_to_display,
        use_container_width=True,
        hide_index=True,
        key="cumulative_editor",
        disabled=["항목"]  # '항목' 열은 편집 불가
    )

    if st.button("💾 누적 데이터 저장", type="primary"):
        with st.spinner("수정된 누적 데이터를 Google Sheets에 저장 중입니다..."):
            try:
                # 3. 저장할 전체 데이터 준비
                # 원본 데이터의 복사본을 만듭니다.
                df_to_save = df_cumulative_original.copy()
                
                # 원본에서 '오후당직 (목표)' 행의 인덱스를 찾습니다.
                idx_to_update = df_to_save.index[df_to_save['항목'] == '오후당직 (목표)'][0]
                
                # 해당 인덱스의 행을 편집된 내용으로 교체합니다.
                df_to_save.iloc[idx_to_update] = edited_df_display.iloc[0]

                # 4. Google Sheets에 저장
                gc = get_gspread_client()
                sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                cumulative_sheet_name = st.session_state.get("latest_cumulative_name")

                if cumulative_sheet_name:
                    worksheet_to_update = sheet.worksheet(cumulative_sheet_name)
                    
                    data_to_save_list = [df_to_save.columns.tolist()] + df_to_save.fillna('').values.tolist()
                    
                    if update_sheet_with_retry(worksheet_to_update, data_to_save_list):
                        st.success(f"🎉 '{cumulative_sheet_name}' 시트가 성공적으로 업데이트되었습니다.")
                        # 변경사항 즉시 반영을 위해 세션 데이터도 업데이트
                        st.session_state["df_cumulative_original"] = df_to_save
                        df_transposed = df_to_save.set_index('항목')
                        st.session_state["df_cumulative"] = df_transposed.transpose().reset_index().rename(columns={'index': '이름'})
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("Google Sheets 업데이트에 실패했습니다.")
                else:
                    st.error("업데이트할 누적 시트의 이름을 찾을 수 없습니다. 페이지를 새로고침 해주세요.")

            except Exception as e:
                st.error(f"저장 중 오류 발생: {e}")

# afternoon_duty_counts는 기존 로직(transpose된 df_cumulative)을 그대로 사용해야 하므로 이 부분은 수정하지 않습니다.
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame())
if df_cumulative.empty:
    st.error("❌ 방배정 실패: 누적 데이터를 불러올 수 없습니다.")
    st.stop()
    
afternoon_duty_counts = {row['이름']: int(row['오후당직 (목표)']) for _, row in df_cumulative.iterrows() if pd.notna(row.get('오후당직 (목표)')) and int(row['오후당직 (목표)']) > 0}

st.divider()
if st.button("🚀 방배정 수행", type="primary", use_container_width=True):
    # 버튼을 누를 때마다 항상 새로 계산하도록 이전 결과를 삭제합니다.
    if "assignment_results" in st.session_state:
        del st.session_state["assignment_results"]

    if "assignment_results" not in st.session_state or st.session_state.assignment_results is None:
        with st.spinner("방배정 중..."):
            # --- 요청사항 처리 결과 추적을 위한 초기화 ---
            applied_messages = []
            unapplied_messages = []
            weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
            
            # --- [수정] 방배정 전 요청사항 유효성 검사 ---
            st.info("ℹ️ 방배정 요청사항 유효성을 검사합니다...")
            
            # 날짜 파싱 성능을 위해 근무일 정보 미리 생성
            work_days_map = {}
            target_year = int(month_str.split('년')[0])
            df_schedule_for_check = st.session_state.get("df_schedule_md_modified", st.session_state.get("df_schedule_md_initial"))

            for _, schedule_row in df_schedule_for_check.iterrows():
                date_str = schedule_row['날짜']
                try:
                    date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=target_year)
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                    
                    morning_workers = set(p.strip() for p in schedule_row.iloc[2:13].dropna() if p and p.strip())
                    afternoon_workers = set(p.strip() for p in schedule_row.iloc[13:].dropna() if p and p.strip())
                    on_call_worker = str(schedule_row.get('오전당직(온콜)', '')).strip()

                    if on_call_worker:
                        morning_workers.add(on_call_worker)
                        afternoon_workers.add(on_call_worker)
                    
                    work_days_map[formatted_date] = {
                        "morning": morning_workers,
                        "afternoon": afternoon_workers,
                        "on_call": on_call_worker
                    }
                except (ValueError, TypeError):
                    continue

            # 유효성 검사를 통과한 요청만 실제 배정에 사용
            valid_requests_indices = []
            for index, req in st.session_state["df_room_request"].iterrows():
                req_date, is_morning = parse_date_info(req['날짜정보'])
                person = req['이름']
                category = req['분류']
                
                is_valid = True

                # 날짜 포맷팅 ('MM월 DD일(요일) (오전/오후)')
                date_obj = datetime.strptime(req_date, '%Y-%m-%d')
                day_of_week = weekday_map[date_obj.weekday()]
                date_str_display = f"{date_obj.strftime('%m월 %d일')}({day_of_week})"
                time_str_display = '오전' if is_morning else '오후'
                
                # 1. 근무일이 아닌 경우 검사
                time_period_key = "morning" if is_morning else "afternoon"
                if req_date not in work_days_map or person not in work_days_map[req_date][time_period_key]:
                    msg = f"⚠️ {person}: {date_str_display} ({time_str_display})이 근무일이 아니므로 '{category}' 요청을 처리할 수 없습니다."
                    unapplied_messages.append(msg)
                    is_valid = False

                # 2. 평일 오전 당직방 관련 요청 검사
                is_special_day = req_date in [d.strftime('%Y-%m-%d') for d, _, _ in st.session_state.get("special_schedules", [])]
                if is_valid and not is_special_day and is_morning:
                    room_match = re.match(r'(\d+)번방', category)
                    if room_match:
                        req_room_num = room_match.group(1)
                        morning_duty_room = st.session_state["room_settings"].get("830_duty")
                        if req_room_num == morning_duty_room:
                            msg = f"⛔️ {person}: {date_str_display} ({time_str_display})의 '{req_room_num}번방' 요청은 오전 당직방입니다. 수기로 수정해 주십시오."
                            unapplied_messages.append(msg)
                            is_valid = False

                if is_valid:
                    valid_requests_indices.append(index)
            
            # 유효한 요청들만 필터링하여 DataFrame 생성
            valid_requests_df = st.session_state["df_room_request"].loc[valid_requests_indices].copy()
            time.sleep(1)

            try:
                st.info("ℹ️ 토요/휴일 스케줄의 변경된 근무 정보를 동기화합니다...")
                target_year = int(month_str.split('년')[0])
                
                # 토요/휴일 날짜 목록 ('m월 d일' 형식)
                special_dates_str_set = {s[1] for s in st.session_state.get("special_schedules", [])}
                
                # edited_df_md에서 토요/휴일 데이터만 필터링
                final_special_df_md = edited_df_md[edited_df_md['날짜'].isin(special_dates_str_set)].copy()

                date_to_personnel_map = {}
                if not final_special_df_md.empty:
                    # 날짜 형식 변환 및 근무자 목록 생성
                    for _, row in final_special_df_md.iterrows():
                        try:
                            # 'm월 d일' -> 'YYYY-MM-DD'
                            date_obj = datetime.strptime(row['날짜'], '%m월 %d일').replace(year=target_year)
                            date_key = date_obj.strftime('%Y-%m-%d')
                            
                            # 해당 날짜의 모든 근무자 추출 (중복 제거 및 정렬)
                            personnel_cols = [str(i) for i in range(1, 12)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 5)]
                            personnel_list = [str(row[col]).strip() for col in personnel_cols if col in row and pd.notna(row[col]) and str(row[col]).strip()]
                            unique_personnel = sorted(list(dict.fromkeys(personnel_list)))
                            
                            date_to_personnel_map[date_key] = ", ".join(unique_personnel)
                        except (ValueError, TypeError):
                            continue

                # Google Sheets 업데이트
                if date_to_personnel_map:
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                    special_sheet_name = f"{target_year}년 토요/휴일 스케줄"
                    worksheet_special = sheet.worksheet(special_sheet_name)
                    df_yearly = pd.DataFrame(worksheet_special.get_all_records())
                    
                    if not df_yearly.empty:
                        # '날짜' 열을 기준으로 '근무' 열 업데이트
                        df_yearly['근무'] = df_yearly.apply(lambda r: date_to_personnel_map.get(str(r['날짜']), r['근무']), axis=1)
                        
                        if update_sheet_with_retry(worksheet_special, [df_yearly.columns.tolist()] + df_yearly.fillna('').values.tolist()):
                            st.success(f"✅ '{special_sheet_name}' 시트의 근무 정보가 성공적으로 동기화되었습니다.")
                        else:
                            st.error(f"❌ '{special_sheet_name}' 시트 동기화에 실패했습니다.")
            
            except gspread.exceptions.WorksheetNotFound:
                st.warning(f"'{special_sheet_name}' 시트를 찾을 수 없어 토요/휴일 스케줄을 동기화할 수 없습니다.")
            except Exception as e:
                st.error(f"토요/휴일 스케줄 동기화 중 오류 발생: {type(e).__name__} - {e}")

            # --- 최종 당직 정보 입력 검증 ---
            # 이 부분은 이전에 생성된 special_schedules와 special_df를 사용합니다.
            for date_obj, date_str, _ in special_schedules:
                settings = st.session_state.get("weekend_room_settings", {}).get(date_str, {})
                total_room_count = settings.get("total_room_count", 0)
                duty_room_selected = settings.get("duty_room")
                
                duty_person_val = ""
                if not special_df.empty:
                    duty_row = special_df[special_df['날짜_dt'].dt.date == date_obj]
                    if not duty_row.empty: 
                        duty_person_val = str(duty_row['당직'].iloc[0]).strip()

                if total_room_count > 0 and duty_person_val and duty_person_val != "당직 없음" and not duty_room_selected:
                    st.error(f"⚠️ {date_str}: 당직 인원({duty_person_val})이 지정되어 있으므로, '토요/휴일 방 설정'에서 당직 방을 선택해야 합니다.")
                    st.stop()

            # --- 평일 방 설정 검증 및 슬롯 정보 생성 (기존과 동일) ---
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

            # --- 배정 로직 ---
            total_stats = {'early': Counter(), 'late': Counter(), 'morning_duty': Counter(), 'afternoon_duty': Counter(), 'rooms': {str(i): Counter() for i in range(1, 13)}, 'time_room_slots': {s: Counter() for s in time_slots}}
            df_cumulative = st.session_state["df_cumulative"]
            afternoon_duty_counts = {row['이름']: int(row['오후당직 (목표)']) for _, row in df_cumulative.iterrows() if pd.notna(row.get('오후당직 (목표)')) and int(row['오후당직 (목표)']) > 0}

            assignments, date_cache, request_cells, result_data = {}, {}, {}, []
            assignable_slots = [s for s in st.session_state["time_slots"].keys() if not (s.startswith('8:30') and s.endswith('_당직'))]
            weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

            special_dates = [date_str for _, date_str, _ in special_schedules]

            target_year = int(month_str.split('년')[0])
            
            # [수정] for 루프 이전에 special_df 변수를 명확히 정의
            special_df_for_assignment = special_df 

            for _, row in edited_df_md.iterrows():
                date_str = row['날짜']
                try:
                    date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=target_year) if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d')
                    formatted_date = date_obj.strftime('%Y-%m-%d').strip()
                    date_cache[date_str] = formatted_date
                    day_of_week = weekday_map[date_obj.weekday()]
                except (ValueError, TypeError):
                    continue

                result_row = [date_str, day_of_week]

                # --- 토요/휴일 배정 로직 ---
                if date_str in special_dates:
                    personnel = [p for p in row.iloc[2:].dropna() if p]
                    settings = st.session_state["weekend_room_settings"].get(date_str, {})
                    
                    assignment_dict, sorted_rooms = assign_special_date(personnel, date_str, formatted_date, settings, special_df_for_assignment, valid_requests_df)

                    # (이하 로직은 기존 코드를 그대로 따르되, 하드코딩된 부분만 제거)
                    room_to_first_slot_idx = {}
                    for slot_idx, slot_name in enumerate(columns[2:]):
                        room_match = re.search(r'\((\d+)\)', str(slot_name))
                        if room_match:
                            room_num = room_match.group(1)
                            if room_num not in room_to_first_slot_idx:
                                room_to_first_slot_idx[room_num] = slot_idx
                    
                    # ▼▼▼ [새로 추가할 부분] 토요/휴일 요청사항도 request_cells에 기록하여 메모 기능 활성화 ▼▼▼
                    if not st.session_state["df_room_request"].empty:
                        requests_for_day = st.session_state["df_room_request"][
                            st.session_state["df_room_request"]['날짜정보'].str.startswith(formatted_date)
                        ]
                        for _, req in requests_for_day.iterrows():
                            person_req = req['이름']
                            category_req = req['분류'] # 예: "7번방"
                            room_match_req = re.match(r'(\d+)번방', category_req)

                            if room_match_req:
                                room_num_req = room_match_req.group(1)
                                # 이 요청이 실제로 배정에 반영되었는지 확인
                                if f"방({room_num_req})" in assignment_dict and assignment_dict[f"방({room_num_req})"] == person_req:
                                    # 해당 방 번호에 해당하는 슬롯 이름을 찾음
                                    if room_num_req in room_to_first_slot_idx:
                                        slot_idx = room_to_first_slot_idx[room_num_req]
                                        slot_name = columns[slot_idx + 2] # +2 for '날짜', '요일' columns
                                        request_cells[(formatted_date, slot_name)] = {'이름': person_req, '분류': category_req}
        
                    mapped_assignment = [None] * (len(columns) - 2)
                    
                    # sorted_rooms를 기준으로 배정하여 순서 보장
                    for room_num in sorted_rooms:
                        slot_key = f"방({room_num})"
                        if slot_key in assignment_dict:
                            person = assignment_dict[slot_key]
                            if room_num in room_to_first_slot_idx:
                                slot_idx = room_to_first_slot_idx[room_num]
                                mapped_assignment[slot_idx] = person
                            
                    result_data.append(result_row + mapped_assignment)
                    continue # 평일 로직 건너뛰기
                    
                has_person = any(val for val in row.iloc[2:-1] if pd.notna(val) and val)
                personnel_for_the_day = [p for p in row.iloc[2:].dropna() if p]
                        
                # 이 코드는 사용자의 기존 `if date_str in special_dates:` 블록을 대체합니다.
                if date_str in special_dates:
                    found_special_schedule = False
                    # 해당 날짜의 특별 근무 일정을 찾습니다.
                    for date_obj, special_date_str, personnel in special_schedules:
                        if special_date_str == date_str:
                            # Streamlit 세션 상태에서 해당 날짜의 주말/공휴일 설정을 가져옵니다.
                            settings = st.session_state["weekend_room_settings"].get(date_str, {})
                            duty_person = settings.get("duty_person", None)
                            duty_room = settings.get("duty_room", None)

                            # 설정된 인원과 방 정보를 바탕으로 배정 계획을 생성합니다.
                            # assignment_dict는 {"방번호": "담당자"} 형태의 딕셔너리입니다.
                            assignment_dict, sorted_rooms = assign_special_date(personnel, date_str, formatted_date, settings, special_df_for_assignment, valid_requests_df)
                            
                            # 배정된 인원 수가 방 수보다 적을 경우 경고 메시지를 표시합니다.
                            if len(assignment_dict) < len(sorted_rooms):
                                st.warning(f"{date_str}: 인원 수({len(personnel)}) 부족으로 {len(sorted_rooms) - len(assignment_dict)}개 방배정 안 됨.")
                            
                            # 1. 각 방 번호와 매칭되는 첫 번째 오전 슬롯의 인덱스를 찾습니다.
                            room_to_first_slot_idx = {}
                            # DataFrame의 최종 컬럼을 기준으로 슬롯을 순회하여 길이 불일치 문제를 해결합니다.
                            for slot_idx, slot in enumerate(columns[2:]):
                                # 오후(13:30) 슬롯이나 '온콜' 등 배정 대상이 아닌 슬롯은 건너뜁니다.
                                slot_str = str(slot)
                                if '13:30' in slot_str or '온콜' in slot_str:
                                    continue
                                
                                # 정규식을 사용해 슬롯 이름에서 방 번호를 추출합니다. 예: "8:30(1)_당직" -> "1"
                                room_match = re.search(r'\((\d+)\)', slot_str)
                                if room_match:
                                    room_num = room_match.group(1)
                                    # 아직 맵에 없는 방 번호일 경우에만 추가하여, 각 방의 '첫 번째' 슬롯만 매핑되도록 합니다.
                                    if room_num not in room_to_first_slot_idx:
                                        room_to_first_slot_idx[room_num] = slot_idx
                            
                            # 2. 배정 결과를 최종 슬롯 리스트에 매핑합니다.
                            # 최종 결과(엑셀의 한 행)를 담을 리스트를 'columns' 길이에 맞춰 초기화합니다.
                            mapped_assignment = [None] * (len(columns) - 2)
                            # 중복 배정을 방지하기 위해 이미 배정된 인원을 기록하는 세트입니다.
                            assigned_personnel = set()
                            
                            # `assignment_dict`의 모든 항목(방-사람)을 순회하며 배정합니다.
                            for room_num, person_with_room in assignment_dict.items():
                                # 담당자 이름만 추출합니다. (예: "강승주[3]" -> "강승주")
                                person = person_with_room.split('[')[0].strip()

                                # 해당 방 번호가 배정 대상인 오전 슬롯에 포함되어 있는지 확인합니다.
                                if room_num in room_to_first_slot_idx:
                                    # 이미 다른 방에 배정된 인원인지 확인하여 중복을 방지합니다.
                                    if person in assigned_personnel:
                                        st.warning(f"{date_str}: {person}님이 중복 배정되었습니다. 확인이 필요합니다.")
                                        continue

                                    # 배정할 슬롯의 인덱스를 가져옵니다.
                                    slot_idx = room_to_first_slot_idx[room_num]
                                    
                                    # 최종 배정 리스트의 해당 위치에 담당자 이름을 할당합니다.
                                    mapped_assignment[slot_idx] = person
                                    # 이 담당자를 '배정 완료' 세트에 추가합니다.
                                    assigned_personnel.add(person)
                            
                            # 완성된 배정 결과를 전체 결과 데이터에 추가합니다.
                            full_row = result_row + mapped_assignment
                            result_data.append(full_row)
                            found_special_schedule = True
                            break  # 해당 날짜의 처리가 끝났으므로 내부 루프를 종료합니다.

                    # 특별 근무 일정이 없는 경우 (예: 공휴일이지만 근무자가 없는 날) 빈 행을 추가합니다.
                    if not found_special_schedule:
                        result_data.append(result_row + [None] * (len(columns) - 2))

                    # special_date 처리가 끝났으므로, 평일 배정 로직을 건너뛰고 다음 날짜로 넘어갑니다.
                    continue

                # 기존 평일 처리
                # 2. '소수 인원 근무'로 판단할 기준 인원수를 설정합니다.
                SMALL_TEAM_THRESHOLD = 15

                # 3. 근무 인원수가 설정된 기준보다 적으면, 방배정 없이 순서대로 나열합니다.
                if len(personnel_for_the_day) < SMALL_TEAM_THRESHOLD and has_person:
                    result_row.append(None)
                    result_row.extend(personnel_for_the_day)
                    num_slots_to_fill = len(all_slots)
                    slots_filled_count = len(personnel_for_the_day) + 1  # 근무자 수 + 비워둔 1칸
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
                
                # ✨ --- 여기가 수정된 요청사항 처리 로직입니다 --- ✨
                request_assignments = {}
                # 그날에 해당하는 유효한 요청만 필터링
                requests_for_day = valid_requests_df[valid_requests_df['날짜정보'].str.startswith(formatted_date)]
                
                if not requests_for_day.empty:
                    # 1단계: '특정 방' 요청 먼저 처리 (충돌 가능성 높음)
                    room_reqs = requests_for_day[requests_for_day['분류'].str.contains('번방')].sort_index()
                    for _, req in room_reqs.iterrows():
                        person, category = req['이름'], req['분류']
                        # 이 사람/시간대에 대한 요청이 이미 처리되었는지 확인
                        if any(p == person for p in request_assignments.values()): continue

                        slots_for_category = st.session_state["memo_rules"].get(category, [])
                        if slots_for_category:
                            # '1번방' 요청은 슬롯이 하나뿐이므로, 그 슬롯이 비어있으면 배정
                            target_slot = slots_for_category[0]
                            if target_slot not in request_assignments:
                                request_assignments[target_slot] = person
                                request_cells[(formatted_date, target_slot)] = {'이름': person, '분류': category}

                    # 2단계: '특정 시간대' 및 기타 요청 처리
                    other_reqs = requests_for_day[~requests_for_day['분류'].str.contains('번방')].sort_index()
                    for _, req in other_reqs.iterrows():
                        person, category, date_info = req['이름'], req['분류'], req['날짜정보']
                        is_morning = '(오전)' in date_info
                        if any(p == person for p in request_assignments.values()): continue

                        # 요청을 만족하는 '아직 비어있는' 슬롯 찾기
                        possible_slots = [s for s in st.session_state["memo_rules"].get(category, []) if s not in request_assignments]
                        if possible_slots:
                            selected_slot = random.choice(possible_slots)
                            request_assignments[selected_slot] = person
                            request_cells[(formatted_date, selected_slot)] = {'이름': person, '분류': category}

                # `random_assign` 호출은 기존과 동일합니다.
                assignment, _ = random_assign(list(set(morning_personnel)|set(afternoon_personnel)), assignable_slots, request_assignments, st.session_state["time_groups"], total_stats, list(morning_personnel), list(afternoon_personnel), afternoon_duty_counts)

                # ... (이후 결과 처리) ... 
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

            # [수정] 온콜 근무를 '오후 당직' 합계에 포함하여 집계
            for row_data in result_data:
                current_date_str = row_data[0]
                if current_date_str in special_dates:
                    continue
                
                duty_830_person = row_data[columns.index(morning_duty_slot)] if morning_duty_slot in columns else None
                if duty_830_person:
                    total_stats['morning_duty'][duty_830_person] += 1
                
                person_on_call = row_data[columns.index('온콜')] if '온콜' in columns else None
                if person_on_call:
                    total_stats['afternoon_duty'][person_on_call] += 1
            
            time_order = ['8:30', '9:00', '9:30', '10:00', '13:30']

            # --- [수정 2] 통계 DataFrame 생성 (목표치 열 추가) ---
            stats_data, all_personnel_stats = [], set(p for _, r in st.session_state["df_schedule_md"].iterrows() for p in r[2:].dropna() if p)
            
            df_cumulative_stats = st.session_state.get("df_cumulative", pd.DataFrame())
            duty_target_map = {}
            if not df_cumulative_stats.empty and '이름' in df_cumulative_stats.columns and '오후당직 (목표)' in df_cumulative_stats.columns:
                 duty_target_map = df_cumulative_stats.set_index('이름')['오후당직 (목표)'].to_dict()

            for person in sorted(all_personnel_stats):
                stats_entry = {
                    '인원': person,
                    '이른방 합계': total_stats['early'][person],
                    '늦은방 합계': total_stats['late'][person],
                    '오전 당직 합계': total_stats['morning_duty'][person],
                    '오후 당직 합계': total_stats['afternoon_duty'][person],
                    '오후 당직 (목표)': duty_target_map.get(person, 0)
                }
                for slot in st.session_state["time_slots"].keys():
                    if not slot.endswith('_당직'):
                        stats_entry[f'{slot} 합계'] = total_stats['time_room_slots'].get(slot, Counter())[person]
                stats_data.append(stats_entry)

            sorted_columns = ['인원', '이른방 합계', '늦은방 합계', '오전 당직 합계', '오후 당직 합계', '오후 당직 (목표)']
            time_slots = sorted(
                [slot for slot in st.session_state["time_slots"].keys() if not slot.endswith('_당직')],
                key=lambda x: (time_order.index(x.split('(')[0]), int(x.split('(')[1].split(')')[0]))
            )
            sorted_columns.extend([f'{slot} 합계' for slot in time_slots])
            stats_df = pd.DataFrame(stats_data)[sorted_columns]

            # --- [수정 3] 배정 완료 후, 모든 로그 생성 ---
            
            # 3-1. 방배정 요청 로그 생성
            applied_request_keys = set((key[0], value['이름'], value['분류']) for key, value in request_cells.items())
            for _, req in valid_requests_df.iterrows():
                req_date, is_morning = parse_date_info(req['날짜정보'])
                if not req_date: continue
                person, category = req['이름'], req['분류']
                
                date_obj = datetime.strptime(req_date, '%Y-%m-%d')
                date_str_display = f"{date_obj.strftime('%m월 %d일')}({weekday_map[date_obj.weekday()]})"
                time_str_display = '오전' if is_morning else '오후'
                
                if (req_date, person, category) in applied_request_keys:
                    msg = f"✅ {person}: {date_str_display} ({time_str_display})의 '{category}' 요청이 적용되었습니다."
                    applied_messages.append(msg)
                else:
                    msg = f"ℹ️ {person}: {date_str_display} ({time_str_display})의 '{category}' 요청이 배정 균형을 위해 반영되지 않았습니다."
                    unapplied_messages.append(msg)

            # 3-2. 오후당직 배정 로그 생성
            oncall_logs = []
            actual_duty_counts = total_stats.get('afternoon_duty', Counter())
            if duty_target_map:
                for worker in all_personnel_stats:
                    required_count = int(duty_target_map.get(worker, 0))
                    actual_count = actual_duty_counts.get(worker, 0)
                    if required_count != actual_count:
                        comparison_text = f"많은 {actual_count}회 배정" if actual_count > required_count else f"적은 {actual_count}회 배정"
                        log_message = f"• {worker}: 오후당직 목표 '{required_count}회'보다 {comparison_text}"
                        oncall_logs.append(log_message)

            # --- [수정] Google Sheets 연결 및 시트 저장 로직 (누락된 부분 복원) ---
            try:
                gc = get_gspread_client()
                sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])

                # '방배정' 시트 저장
                try:
                    worksheet_result = sheet.worksheet(f"{month_str} 방배정")
                except gspread.exceptions.WorksheetNotFound:
                    worksheet_result = sheet.add_worksheet(f"{month_str} 방배정", rows=100, cols=len(df_room.columns))
                
                update_sheet_with_retry(worksheet_result, [df_room.columns.tolist()] + df_room.fillna('').values.tolist())
                st.success(f"✅ {month_str} 방배정 테이블이 Google Sheets에 저장되었습니다.")

                # '다음달 누적 최종' 시트 업데이트
                target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
                next_month_dt = target_month_dt + relativedelta(months=1)
                next_month_str = next_month_dt.strftime("%Y년 %-m월")
                latest_cumulative_name_next = find_latest_version(sheet, next_month_str, "누적")
                
                if not latest_cumulative_name_next:
                    st.warning(f"⚠️ '{next_month_str} 누적' 시트를 찾을 수 없어 '오후당직 (배정)' 횟수를 업데이트할 수 없습니다.")
                else:
                    worksheet_cumulative_next = sheet.worksheet(latest_cumulative_name_next)
                    all_data = worksheet_cumulative_next.get_all_values()
                    headers, rows = all_data[0], all_data[1:]

                    for r_idx, row_data in enumerate(rows):
                        for c_idx, cell_value in enumerate(row_data):
                            if c_idx == 0: continue
                            try:
                                rows[r_idx][c_idx] = int(cell_value)
                            except (ValueError, TypeError):
                                pass
                    
                    target_row_found = False
                    for i, row_data in enumerate(rows):
                        if row_data[0] == '오후당직 (배정)':
                            target_row_found = True
                            for col_idx, name in enumerate(headers):
                                if col_idx == 0: continue
                                assigned_count = actual_duty_counts.get(name.strip(), 0)
                                rows[i][col_idx] = assigned_count
                            break
                    
                    if target_row_found:
                        final_sheet_name = f"{next_month_str} 누적 최종"
                        try:
                            worksheet_final = sheet.worksheet(final_sheet_name)
                            st.warning(f"⚠️ 기존 '{final_sheet_name}' 시트를 덮어씁니다.")
                        except gspread.exceptions.WorksheetNotFound:
                            worksheet_final = sheet.add_worksheet(title=final_sheet_name, rows=len(all_data), cols=len(headers))
                        
                        if update_sheet_with_retry(worksheet_final, [headers] + rows):
                            st.success(f"✅ '{final_sheet_name}' 시트에 오후 당직 배정 횟수가 성공적으로 업데이트되었습니다.")
                        else:
                            st.error(f"❌ '{final_sheet_name}' 시트 업데이트에 실패했습니다.")
                    else:
                        st.error(f"❌ '{latest_cumulative_name_next}' 시트에서 '오후당직 (배정)' 항목을 찾을 수 없습니다.")
            except Exception as e:
                st.error(f"Google Sheets 저장 중 오류 발생: {type(e).__name__} - {e}")

            # --- Excel 파일 생성 ---
            wb = openpyxl.Workbook()
            sheet = wb.active
            sheet.title = "Schedule"

            import platform

            # 플랫폼에 따라 폰트 선택
            if platform.system() == "Windows":
                font_name = "맑은 고딕"  # Windows에서 기본 제공
            else:
                font_name = "Arial"  # Mac에서 기본 제공, Windows에서도 사용 가능

            # 색상 및 스타일 정의
            highlight_fill = PatternFill(start_color="F2DCDB", end_color="F2DCDB", fill_type="solid")
            sky_blue_fill = PatternFill(start_color="CCEEFF", end_color="CCEEFF", fill_type="solid")
            duty_font = Font(name=font_name, size=9, bold=True, color="FF00FF")  # 폰트 크기 9로 명시
            default_font = Font(name=font_name, size=9)  # 폰트 크기 9로 명시
            special_day_fill = PatternFill(start_color="BFBFBF", end_color="BFBFBF", fill_type="solid")
            no_person_day_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
            default_yoil_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
            special_person_fill = PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid")  # special_schedules 근무자 셀 배경색

            # 세션에서 변경된 셀 위치를 가져옴
            swapped_assignments = st.session_state.get("swapped_assignments", set())

            # 헤더 렌더링
            for col_idx, header in enumerate(columns, 1):
                cell = sheet.cell(1, col_idx, header)
                cell.font = Font(bold=True, name=font_name, size=9)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                if header.startswith('8:30') or header == '온콜':
                    cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
                elif header.startswith('9:00'):
                    cell.fill = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
                elif header.startswith('9:30'):
                    cell.fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
                elif header.startswith('10:00'):
                    cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
                elif header.startswith('13:30'):
                    cell.fill = PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")

            # 데이터 렌더링
            for row_idx, row_data in enumerate(result_data, 2):
                current_date_str = row_data[0]
                
                duty_person_for_the_day = None
                if current_date_str in special_dates:
                    try:
                        date_obj_lookup = datetime.strptime(current_date_str, '%m월 %d일').replace(year=datetime.now().year)
                        formatted_date_lookup = date_obj_lookup.strftime('%Y-%m-%d')
                        duty_person_row = special_df[special_df['날짜'] == formatted_date_lookup]
                        if not duty_person_row.empty:
                            duty_person_raw = duty_person_row['당직'].iloc[0]
                            if pd.notna(duty_person_raw) and str(duty_person_raw).strip() and str(duty_person_raw).strip() != '당직 없음':
                                duty_person_for_the_day = str(duty_person_raw).strip()
                    except Exception as e:
                        st.warning(f"Excel 스타일링 중 당직 인원 조회 오류: {e}")

                assignment_cells = row_data[2:]
                personnel_in_row = [p for p in assignment_cells if p]
                is_no_person_day = not any(personnel_in_row)
                SMALL_TEAM_THRESHOLD_FORMAT = 15
                is_small_team_day = (0 < len(personnel_in_row) < SMALL_TEAM_THRESHOLD_FORMAT) or (current_date_str in special_dates)

                # --- 데이터 렌더링 ---
                for row_idx, row_data in enumerate(result_data, 2):
                    # --- 1. 현재 행(날짜)의 상태를 먼저 모두 정의합니다 ---
                    current_date_str = row_data[0]
                    
                    # [핵심 수정 1] 휴일 여부를 명확한 변수로 먼저 정의합니다.
                    is_special_day = current_date_str in special_dates
                    
                    duty_person_for_the_day = None
                    # 휴일인 경우에만 당직자 정보를 조회합니다. (효율성 증가)
                    if is_special_day:
                        try:
                            date_obj_lookup = datetime.strptime(current_date_str, '%m월 %d일').replace(year=datetime.now().year)
                            formatted_date_lookup = date_obj_lookup.strftime('%Y-%m-%d')
                            duty_person_row = special_df[special_df['날짜'] == formatted_date_lookup]
                            if not duty_person_row.empty:
                                duty_person_raw = duty_person_row['당직'].iloc[0]
                                if pd.notna(duty_person_raw) and str(duty_person_raw).strip():
                                    duty_person_for_the_day = str(duty_person_raw).strip()
                        except Exception as e:
                            st.warning(f"Excel 스타일링 중 당직 인원 조회 오류: {e}")

                    # 행의 다른 상태들도 여기서 정의합니다.
                    assignment_cells = row_data[2:]
                    personnel_in_row = [p for p in assignment_cells if p]
                    is_no_person_day = not any(personnel_in_row)
                    SMALL_TEAM_THRESHOLD_FORMAT = 15
                    is_small_team_day_for_bg = (0 < len(personnel_in_row) < SMALL_TEAM_THRESHOLD_FORMAT) or is_special_day


                    # --- 2. 열을 순회하며 각 셀의 스타일을 순서대로 적용합니다 ---
                    for col_idx, value in enumerate(row_data, 1):
                        cell = sheet.cell(row_idx, col_idx, value)
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                        
                        # --- 배경색 적용 ---
                        if col_idx == 1:  # 날짜
                            cell.fill = no_person_day_fill
                        elif col_idx == 2:  # 요일
                            if is_no_person_day:
                                cell.fill = no_person_day_fill
                            elif is_small_team_day_for_bg:
                                cell.fill = special_day_fill
                            else:
                                cell.fill = default_yoil_fill
                        elif is_no_person_day and col_idx >= 3:
                            cell.fill = no_person_day_fill
                        elif is_special_day and col_idx > 2 and value:
                            cell.fill = special_person_fill
                        
                        # --- 변경사항 하이라이트 적용 (배경색 덮어쓰기) ---
                        slot_name = columns[col_idx-1]
                        cell_shift_type = ''
                        if any(time_str in str(slot_name) for time_str in ['8:30', '9:00', '9:30', '10:00']):
                            cell_shift_type = '오전'
                        elif any(time_str in str(slot_name) for time_str in ['13:30', '온콜']):
                            cell_shift_type = '오후'
                        
                        if (current_date_str.strip(), cell_shift_type, str(value).strip()) in swapped_assignments:
                            cell.fill = highlight_fill

                        # --- 폰트 적용 (가장 중요) ---
                        # [핵심 수정 2] 폰트 로직을 is_special_day 변수로 명확하게 분리합니다.
                        cell.font = default_font  # 1. 먼저 기본 폰트를 적용하고,
                        
                        if value:  # 2. 셀에 값이 있을 때만 아래 조건에 따라 폰트를 덮어씌웁니다.
                            if is_special_day:
                                # [휴일 로직] '조회된 당직자'와 이름이 일치할 때만 핑크색 볼드체 적용
                                if duty_person_for_the_day and value == duty_person_for_the_day:
                                    cell.font = duty_font
                            else:
                                # [평일 로직] 열 이름에 '_당직'이나 '온콜'이 포함될 때 핑크색 볼드체 적용
                                if slot_name.endswith('_당직') or slot_name == '온콜':
                                    cell.font = duty_font
                                    
                        # --- 코멘트 추가 ---
                        if col_idx > 2 and value and date_cache.get(current_date_str):
                            formatted_date_for_comment = date_cache[current_date_str]
                            if (formatted_date_for_comment, slot_name) in request_cells and value == request_cells[(formatted_date_for_comment, slot_name)]['이름']:
                                cell.comment = Comment(f"{request_cells[(formatted_date_for_comment, slot_name)]['분류']}", "System")
                
                    slot_name = columns[col_idx-1]
                    cell_shift_type = ''
                    if '8:30' in slot_name or '9:00' in slot_name or '9:30' in slot_name or '10:00' in slot_name:
                        cell_shift_type = '오전'
                    elif '13:30' in slot_name or '온콜' in slot_name:
                        cell_shift_type = '오후'
                    
                    # 셀의 배경색 적용 (변경 요청 하이라이트)
                    formatted_current_date = current_date_str.strip()
                    if (formatted_current_date, cell_shift_type, str(value).strip()) in swapped_assignments:
                        cell.fill = highlight_fill

                    # special_dates의 경우 폰트 설정
                    if current_date_str in special_dates:
                        settings = st.session_state["weekend_room_settings"].get(current_date_str, {})
                        duty_room = settings.get("duty_room", None)
                        duty_person = settings.get("duty_person", None)
                        room_match = re.search(r'\((\d+)\)', slot_name)
                        if room_match:
                            room_num = room_match.group(1)
                            if room_num == duty_room and value and duty_person and duty_person != "선택 안 함" and value == duty_person:
                                cell.font = Font(name=font_name, size=9, bold=True, color="FF00FF")  # 당직 인원: 크기 9, 굵은 글씨, 보라색
                            else:
                                cell.font = Font(name=font_name, size=9)  # 일반 인원: 크기 9, 기본 스타일
                    else:
                        # 평일 당직 강조 로직
                        if slot_name.startswith('8:30') and slot_name.endswith('_당직') and value:
                            cell.font = Font(name=font_name, size=9, bold=True, color="FF00FF")  # 크기 9, 굵은 글씨, 보라색
                        elif (slot_name.startswith('13:30') and slot_name.endswith('_당직') or slot_name == '온콜') and value:
                            cell.font = Font(name=font_name, size=9, bold=True, color="FF00FF")  # 크기 9, 굵은 글씨, 보라색
                        else:
                            cell.font = Font(name=font_name, size=9)  # 크기 9, 기본 스타일

                    # special_dates의 경우 value를 그대로 셀에 기록
                    if current_date_str in special_dates and col_idx > 2 and value:
                        cell.value = value
                    elif col_idx > 2 and value and date_cache.get(current_date_str):
                        formatted_date_for_comment = date_cache[current_date_str]
                        if (formatted_date_for_comment, slot_name) in request_cells and value == request_cells[(formatted_date_for_comment, slot_name)]['이름']:
                            cell.comment = Comment(f"{request_cells[(formatted_date_for_comment, slot_name)]['분류']}", "System")

            sheet.column_dimensions['A'].width = 11
            for col in sheet.columns:
                if col[0].column_letter != 'A':
                    sheet.column_dimensions[col[0].column_letter].width = 9

            # --- Stats 시트 생성 ---
            stats_sheet = wb.create_sheet("Stats")
            stats_columns = stats_df.columns.tolist()
            for col_idx, header in enumerate(stats_columns, 1):
                stats_sheet.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = 12
                cell = stats_sheet.cell(1, col_idx, header)
                cell.font = Font(bold=True, name=font_name, size=9)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                if header == '인원':
                    cell.fill = PatternFill(start_color="D0CECE", end_color="D0CECE", fill_type="solid")
                elif header == '이른방 합계':
                    cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
                elif header == '늦은방 합계':
                    cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
                elif '당직' in header: # '오전/오후 당직 합계' 및 '오후 당직 (목표)' 모두 핑크색으로
                    cell.fill = PatternFill(start_color="FFC0CB", end_color="FFC0CB", fill_type="solid")
                else:
                    cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

            for row_idx, row in enumerate(stats_df.values, 2):
                for col_idx, value in enumerate(row, 1):
                    cell = stats_sheet.cell(row_idx, col_idx, value)
                    cell.font = Font(name=font_name, size=9)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

            output = BytesIO()
            wb.save(output)
            output.seek(0)
            
            st.session_state["assignment_results"] = {
                "df_room": df_room,
                "stats_df": stats_df,
                "excel_output": output,
                "applied_messages": applied_messages,
                "unapplied_messages": unapplied_messages,
                "oncall_logs": oncall_logs
            }
        
        # 계산이 끝나면 스크립트를 재실행하여 아래의 '결과 표시' 로직을 타게 합니다.
        st.rerun()

if "assignment_results" in st.session_state and st.session_state["assignment_results"] is not None:
    results = st.session_state["assignment_results"]

    df_room = results["df_room"]
    stats_df = results["stats_df"]
    output = results["excel_output"]
    applied_messages = results["applied_messages"]
    unapplied_messages = results["unapplied_messages"]
    
    # 1. 생성된 메시지를 심각도에 따라 세 그룹으로 분류합니다.
    critical_unapplied = [msg for msg in unapplied_messages if msg.strip().startswith('⛔️')]
    warning_unapplied = [msg for msg in unapplied_messages if not msg.strip().startswith('⛔️')]
    sorted_applied = sorted(applied_messages)
    
    # ▼▼▼ [핵심 수정] 이 블록 전체를 교체합니다. ▼▼▼
    st.write("---")
    with st.expander("🔍 방배정 상세 로그 보기", expanded=True):
        
        st.write(" ")
        st.write("**⛔️ 방배정 요청사항 적용 안 됨 (수기 수정 필요)**")
        critical_log_text = "\n".join(f"• {msg[2:]}" for msg in sorted(critical_unapplied)) if critical_unapplied else "해당 없음"
        st.code(critical_log_text, language='text')
        
        st.divider()

        st.write("**⚠️ 방배정 요청사항 적용 안 됨**")
        warning_log_text = "\n".join(f"• {msg[2:]}" for msg in sorted(warning_unapplied)) if warning_unapplied else "해당 없음"
        st.code(warning_log_text, language='text')

        st.divider()

        st.write("**✅ 방배정 요청사항 적용됨**")
        applied_log_text = "\n".join(f"• {msg[2:]}" for msg in sorted_applied) if sorted_applied else "해당 없음"
        st.code(applied_log_text, language='text')

        # --- [추가] 오후당직 배정 로그 표시 ---
        st.divider()
        st.write("**📞 오후당직 배정 로그**")
        oncall_logs = results.get("oncall_logs", [])
        oncall_log_text = "\n".join(sorted(oncall_logs)) if oncall_logs else "모든 오후당직이 목표치에 맞게 정상 배정되었습니다."
        st.code(oncall_log_text, language='text')

    st.divider()
    st.markdown("**✅ 방배정 결과**") # 기존 헤더와 연결
    st.dataframe(df_room, hide_index=True)
    
    st.divider()
    st.markdown("**☑️ 인원별 통계**")
    st.dataframe(stats_df, hide_index=True)

    st.divider()
    st.download_button(
        label="📥 최종 방배정 다운로드",
        data=output,
        file_name=f"{month_str} 방배정.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )