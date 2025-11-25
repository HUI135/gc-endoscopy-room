import re
import streamlit as st
import pandas as pd
import numpy as np
import gspread
from collections import Counter
from google.oauth2.service_account import Credentials
import time
from datetime import datetime, date
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu
import os
from dateutil.relativedelta import relativedelta

# --- 페이지 기본 설정 ---
st.set_page_config(page_title="방배정 변경", page_icon="🔄", layout="wide")
st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# --- 로그인 확인 ---
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# --- 세션 상태 초기화 ---
if "change_data_loaded" not in st.session_state:
    st.session_state["change_data_loaded"] = False
if "saved_changes_log" not in st.session_state:
    st.session_state["saved_changes_log"] = []
if "df_final_assignment" not in st.session_state:
    st.session_state["df_final_assignment"] = pd.DataFrame()
if "df_change_requests" not in st.session_state:
    st.session_state["df_change_requests"] = pd.DataFrame()
if "changed_cells_log" not in st.session_state:
    st.session_state["changed_cells_log"] = []
if "df_before_apply" not in st.session_state:
    st.session_state["df_before_apply"] = pd.DataFrame()
if "has_changes_to_revert" not in st.session_state:
    st.session_state["has_changes_to_revert"] = False
if 'download_file' not in st.session_state:
    st.session_state.download_file = None
if 'download_filename' not in st.session_state:
    st.session_state.download_filename = None
if 'page7_messages' not in st.session_state:
    st.session_state['page7_messages'] = []
if "editor_key" not in st.session_state:
    st.session_state["editor_key"] = 0
    
# --- Google Sheets 연동 함수 ---
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
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

@st.cache_data(ttl=600, show_spinner=False)
def load_data_for_change_page(month_str):
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        return "STOP", None, None

    # 1. 방배정 시트
    try:
        worksheet_final = sheet.worksheet(f"{month_str} 방배정")
        df_final = pd.DataFrame(worksheet_final.get_all_records()).fillna('')
    except:
        st.info("방배정이 아직 수행되지 않았습니다.")
        return "STOP", None, None

    # 2. 변경요청 시트
    try:
        worksheet_req = sheet.worksheet(f"{month_str} 방배정 변경요청")
        df_req = pd.DataFrame(worksheet_req.get_all_records())
    except:
        df_req = pd.DataFrame(columns=['RequestID', '요청일시', '요청자', '변경 요청', '변경 요청한 방배정'])

    # 3. [수정] 누적 데이터 시트 (모양 그대로 가져오기)
    df_cumulative = pd.DataFrame()
    try:
        # (예: 2025년 10월 -> 2025년 11월 누적 최종)
        target_dt = datetime.strptime(month_str, "%Y년 %m월")
        next_dt = target_dt + relativedelta(months=1)
        next_month_str = next_dt.strftime("%Y년 %-m월")

        cum_name = f"{next_month_str} 누적 최종"
        all_titles = [ws.title for ws in sheet.worksheets()]
        if cum_name not in all_titles:
            cum_name = f"{next_month_str} 누적"

        if cum_name in all_titles:
            ws = sheet.worksheet(cum_name)
            vals = ws.get_all_values()
            if len(vals) > 1:
                # [핵심] 시트 그대로 DataFrame 생성 (Transpose 안 함)
                # 첫 행 = 이름들(헤더), A열 = 항목
                headers = vals[0]
                data = vals[1:]
                df_cumulative = pd.DataFrame(data, columns=headers)
                
                # '항목'을 인덱스로 설정하여 (Index=항목, Columns=이름) 구조 확정
                if '항목' in df_cumulative.columns:
                    df_cumulative.set_index('항목', inplace=True)

                # 숫자 변환 (계산 가능한 상태로 만들기)
                df_cumulative = df_cumulative.apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
                
    except Exception as e:
        print(f"누적 로드 실패: {e}")

    return df_final, df_req, df_cumulative

@st.cache_data(ttl=600, show_spinner=False)
def load_special_schedules(month_str):
    """
    'YYYY년 토요/휴일 스케줄' 시트에서 특정 월의 데이터를 로드합니다.
    연도는 month_str에서 동적으로 추출합니다.
    """
    try:
        gc = get_gspread_client()
        if not gc: return pd.DataFrame()
        
        spreadsheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        
        # 1. month_str에서 연도를 동적으로 추출하여 시트 이름을 생성합니다.
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"
        
        worksheet = spreadsheet.worksheet(sheet_name)
        records = worksheet.get_all_records()
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)

        # 2. '날짜'와 '근무' 열이 있는지 확인합니다.
        if '날짜' not in df.columns or '근무' not in df.columns:
            st.error(f"'{sheet_name}' 시트에 '날짜' 또는 '근무' 열이 없습니다.")
            return pd.DataFrame()

        df.fillna('', inplace=True)
        df['날짜_dt'] = pd.to_datetime(df['날짜'], format='%Y-%m-%d', errors='coerce')
        df.dropna(subset=['날짜_dt'], inplace=True)

        # 3. 'month_str'에 해당하는 월의 데이터만 필터링합니다.
        target_month_dt = datetime.strptime(month_str, "%Y년 %m월")
        df_filtered = df[
            (df['날짜_dt'].dt.year == target_month_dt.year) &
            (df['날짜_dt'].dt.month == target_month_dt.month)
        ].copy()

        return df_filtered
        
    except gspread.exceptions.WorksheetNotFound:
        target_year = month_str.split('년')[0]
        sheet_name = f"{target_year}년 토요/휴일 스케줄"
        st.info(f"'{sheet_name}' 시트를 찾을 수 없습니다.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"토요/휴일 데이터 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame()

def apply_assignment_swaps(df_assignment, df_requests, df_special):
    df_modified = df_assignment.copy()
    df_special_modified = df_special.copy() if df_special is not None else pd.DataFrame()
    changed_log = []
    applied_count = 0
    # [수정] 메시지를 담을 리스트 생성
    messages = []

    for _, req in df_requests.iterrows():
        try:
            swap_request_str = str(req.get('변경 요청', '')).strip()
            raw_slot_info = str(req.get('변경 요청한 방배정', '')).strip()

            if '➡️' not in swap_request_str: continue
            old_person, new_person = [p.strip() for p in swap_request_str.split('➡️')]
            
            slot_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', raw_slot_info)
            if not slot_match: continue
            
            date_str, target_slot = slot_match.groups()
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            target_date_str = f"{date_obj.month}월 {date_obj.day}일"
            
            row_indices = df_modified.index[df_modified['날짜'] == target_date_str].tolist()
            if not row_indices:
                # [수정] 메시지 리스트에 추가
                messages.append(('warning', f"⚠️ 요청 처리 불가: 방배정표에서 날짜 '{target_date_str}'를 찾을 수 없습니다."))
                continue
            target_row_idx = row_indices[0]

            target_col_found = None
            for col in df_modified.columns[2:]: 
                person_in_cell = str(df_modified.at[target_row_idx, col]).strip()
                if person_in_cell == old_person and col == target_slot:
                    target_col_found = col
                    break
            
            if target_col_found:
                df_modified.at[target_row_idx, target_col_found] = new_person
                applied_count += 1
                
                is_special_date = False
                if df_special is not None and not df_special.empty and '날짜_dt' in df_special.columns:
                    is_special_date = not df_special[df_special['날짜_dt'].dt.date == date_obj.date()].empty
                
                if is_special_date and not df_special_modified.empty:
                    duty_row = df_special_modified[df_special_modified['날짜_dt'].dt.date == date_obj.date()]
                    if not duty_row.empty:
                        current_duty_person = str(duty_row['당직'].iloc[0]).strip()
                        if current_duty_person == old_person:
                            df_special_modified.loc[duty_row.index, '당직'] = new_person
                            # [수정] 메시지 리스트에 추가
                            messages.append(('info', f"ℹ️ {target_date_str}의 토요/휴일 당직자가 '{new_person}' (으)로 함께 변경됩니다."))

                changed_log.append({
                    '날짜': f"{target_date_str} ({'월화수목금토일'[date_obj.weekday()]})",
                    '방배정': target_slot,
                    '변경 전 인원': old_person,
                    '변경 후 인원': new_person,
                })
            else:
                # [수정] 메시지 리스트에 추가
                messages.append(('error', f"❌ 적용 실패: {target_date_str}의 '{target_slot}'에 '{old_person}'이(가) 배정되어 있지 않습니다."))
                
        except Exception as e:
            # [수정] 메시지 리스트에 추가
            messages.append(('error', f"⚠️ 요청 처리 중 시스템 오류 발생: {e}"))

    if applied_count > 0:
        # [수정] 메시지 리스트에 추가 (가장 위로)
        messages.insert(0, ('success', f"🎉 총 {applied_count}건의 변경 요청이 반영되었습니다."))
    elif not df_requests.empty and not messages:
        messages.append(('info', "ℹ️ 새롭게 반영할 유효한 변경 요청이 없습니다."))

    # [수정] df_modified, 로그, 그리고 '메시지 리스트'를 함께 반환
    return df_modified, changed_log, df_special_modified, messages
    
# --- 시간대 순서 정의 ---
time_order = ['8:30', '9:00', '9:30', '10:00', '13:30']

def calculate_statistics(result_df: pd.DataFrame, df_special: pd.DataFrame, df_cumulative: pd.DataFrame) -> pd.DataFrame:
    # 1. 화면의 스케줄표 카운팅 (이름별 카운트)
    # (행렬 연산을 위해 이름을 키로 하는 딕셔너리 생성)
    total_stats = {
        'early': Counter(), 'late': Counter(),
        'morning_duty': Counter(), 'afternoon_duty': Counter(),
        'time_room_slots': {} 
    }
    
    # 날짜/인원 처리
    special_dates = []
    if df_special is not None and not df_special.empty and '날짜_dt' in df_special.columns:
        special_dates = df_special['날짜_dt'].dt.strftime('%#m월 %#d일').tolist() if os.name != 'nt' else df_special['날짜_dt'].dt.strftime('%m월 %d일').apply(lambda x: x.lstrip("0").replace(" 0", " "))
    
    all_personnel_raw = pd.unique(result_df.iloc[:, 2:].values.ravel('K'))
    all_personnel = sorted(list({re.sub(r'\[\d+\]', '', str(p)).strip() for p in all_personnel_raw if pd.notna(p) and str(p).strip()}))
    SMALL_TEAM_THRESHOLD = 13
    
    # 슬롯 초기화
    for col in result_df.columns[2:]:
        if col != '온콜': total_stats['time_room_slots'].setdefault(col, Counter())

    # 카운팅
    for _, row in result_df.iterrows():
        if str(row['날짜']).strip() in special_dates: continue
        personnel = [p for p in row.iloc[2:].dropna() if p]
        if 0 < len(personnel) < 13: continue

        for col in result_df.columns[2:]:
            person = row.get(col)
            if not person: continue
            p = re.sub(r'\[\d+\]', '', str(person)).strip()
            
            if col != '온콜':
                total_stats['time_room_slots'][col][p] += 1
            if col.startswith('8:30') and '_당직' not in col:
                total_stats['early'][p] += 1
            elif col.startswith('10:00'):
                total_stats['late'][p] += 1
            
            # 당직 카운팅 (화면 기준 실시간)
            if col == '온콜' or (col.startswith('8:30') and '_당직' in col):
                total_stats['morning_duty'][p] += 1
            elif col.startswith('13:30') and '_당직' in col:
                total_stats['afternoon_duty'][p] += 1

    # 2. 결과 데이터프레임 생성 (Index=항목, Columns=이름)
    # 시트 형식을 따름
    rows_list = [
        '이른방 합계', '늦은방 합계', 
        '오전당직', '오전당직 누적', 
        '오후당직', '오후당직 누적'
    ]
    
    # 시간대별 합계 행 추가
    time_order = ['8:30', '9:00', '9:30', '10:00', '13:30']
    sorted_slots = sorted([s for s in total_stats['time_room_slots'].keys() if '_당직' not in s],
                          key=lambda x: (time_order.index(x.split('(')[0]), x))
    for s in sorted_slots:
        rows_list.append(f"{s} 합계")
    
    # 빈 DataFrame 생성 (시트와 같은 모양)
    stats_df = pd.DataFrame(index=rows_list, columns=all_personnel)
    stats_df = stats_df.fillna(0) # 기본값 0

    # 3. 데이터 채우기
    # df_cumulative는 이미 (Index=항목, Columns=이름) 상태임.
    
    for p in all_personnel:
        # (1) 기본 카운트 채우기
        stats_df.at['이른방 합계', p] = total_stats['early'][p]
        stats_df.at['늦은방 합계', p] = total_stats['late'][p]
        stats_df.at['오전당직', p] = total_stats['morning_duty'][p] # 화면 값
        stats_df.at['오후당직', p] = total_stats['afternoon_duty'][p] # 화면 값

        for s in sorted_slots:
             stats_df.at[f"{s} 합계", p] = total_stats['time_room_slots'][s][p]

        # (2) 누적 계산 (시트 값 참조)
        # df_cumulative에서 해당 사람(p)의 데이터를 가져옴
        old_am_cum = 0
        old_am_sum = 0
        old_pm_cum = 0
        old_pm_sum = 0

        if not df_cumulative.empty and p in df_cumulative.columns:
            try:
                # df_cumulative는 Index가 '항목'임
                if '오전당직누적' in df_cumulative.index: old_am_cum = int(df_cumulative.at['오전당직누적', p])
                if '오전당직' in df_cumulative.index: old_am_sum = int(df_cumulative.at['오전당직', p])
                
                if '오후당직누적' in df_cumulative.index: old_pm_cum = int(df_cumulative.at['오후당직누적', p])
                if '오후당직' in df_cumulative.index: old_pm_sum = int(df_cumulative.at['오후당직', p])
            except: pass

        # 계산: (시트누적 - 시트합계) + 화면합계
        stats_df.at['오전당직 누적', p] = (old_am_cum - old_am_sum) + total_stats['morning_duty'][p]
        stats_df.at['오후당직 누적', p] = (old_pm_cum - old_pm_sum) + total_stats['afternoon_duty'][p]

    # 최종: '항목'을 컬럼으로 꺼내서 반환 (Streamlit 표시용)
    return stats_df.reset_index().rename(columns={'index': '항목'})

@st.cache_data(ttl=300, show_spinner=False)
def check_final_sheets_exist(month_str, next_month_str):
    """
    지정된 월의 '방배정 최종' 시트와 다음 달의 '누적 최종' 시트가 
    이미 존재하는지 확인하여 True/False를 반환합니다.
    """
    try:
        # 1. 구글 시트 연결
        gc = get_gspread_client()
        if not gc:
            return False
            
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        
        # 2. 현재 존재하는 모든 시트 이름 가져오기
        all_titles = [ws.title for ws in sheet.worksheets()]
        
        # 3. 확인할 시트 이름 정의
        # (1) 현재 달의 최종 방배정 결과
        schedule_sheet_name = f"{month_str} 방배정 최종"
        # (2) 다음 달의 최종 누적 데이터
        cumulative_sheet_name = f"{next_month_str} 누적 최종"
        
        # 4. 존재 여부 확인 (둘 중 하나라도 있으면 True 반환)
        if schedule_sheet_name in all_titles or cumulative_sheet_name in all_titles:
            return True
            
        return False

    except Exception as e:
        # 에러 발생 시 (연결 실패 등) False 반환하여 진행 막지 않음
        # 필요 시 st.error(f"확인 중 오류: {e}") 추가 가능
        return False

# --- UI 및 데이터 핸들링 ---
from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.now(kst)
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
month_str = next_month_date.strftime("%Y년 %-m월")
month_str = "2025년 10월"
st.header(f"🔄 {month_str} 방배정 변경", divider='rainbow')

def load_and_initialize_data():
    with st.spinner("데이터를 로드하고 있습니다..."):
        # [수정] 3개 반환
        df_final, df_req, df_cumulative = load_data_for_change_page(month_str)
    
    if isinstance(df_final, str) and df_final == "STOP":
        st.stop()
        
    df_special = load_special_schedules(month_str)
    
    st.session_state.df_final_assignment = df_final
    st.session_state.df_change_requests = df_req
    # [추가] 누적 데이터 세션 저장
    st.session_state.df_cumulative_stats = df_cumulative
    st.session_state.df_special_schedules = df_special
    st.session_state.changed_cells_log = []
    st.session_state.df_before_apply = df_final.copy()
    st.session_state.has_changes_to_revert = False
    st.session_state.change_data_loaded = True

# 새로고침 버튼
st.write("- 먼저 새로고침 버튼으로 최신 데이터를 불러온 뒤, 배정을 진행해주세요.")
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state.change_data_loaded = False
    
    # 페이지 메시지를 초기화합니다.
    if 'page7_messages' in st.session_state:
        st.session_state['page7_messages'] = []
        
    # [핵심 수정] '결과 보기' 상태를 초기화하여 수정 화면으로 돌아가도록 합니다.
    if 'show_final_results' in st.session_state:
        st.session_state['show_final_results'] = False
        
    st.rerun()
# 초기 데이터 로드
if not st.session_state.change_data_loaded:
    load_and_initialize_data()

st.divider()

st.subheader("📋 방배정 변경 요청 목록")
# --- st.subheader("📋 방배정 변경 요청 목록") 섹션 내부 ---

if not st.session_state.df_change_requests.empty:
    df_display = st.session_state.df_change_requests.copy()
    
    # 날짜 포맷을 보기 좋게 변경하는 함수
    def convert_date_format(x):
        x = str(x).strip()
        match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', x)
        if match:
            date_str, slot = match.groups()
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                weekday_str = '월화수목금토일'[date_obj.weekday()]
                return f"{date_obj.month}월 {date_obj.day}일 ({weekday_str}) - {slot}"
            except ValueError:
                return x
        return x

    df_display['변경 요청한 방배정'] = df_display['변경 요청한 방배정'].apply(convert_date_format)
    if 'RequestID' in df_display.columns:
        df_display = df_display.drop(columns=['RequestID'])
    if '요청자 사번' in df_display.columns:
        df_display = df_display.drop(columns=['요청자 사번'])
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # --- 💡 [추가] 충돌 감지 경고 메시지 로직 ---
    request_sources = []
    request_destinations = []

    for index, row in st.session_state.df_change_requests.iterrows():
        change_request_str = str(row.get('변경 요청', '')).strip()
        slot_info_str = str(row.get('변경 요청한 방배정', '')).strip()
        
        if '➡️' in change_request_str and slot_info_str:
            person_before, person_after = [p.strip() for p in change_request_str.split('➡️')]
            
            # 1. 출처 충돌 검사 리스트 추가
            # 동일한 슬롯에 대한 요청이 여러 개 있는지 확인
            request_sources.append(slot_info_str)
            
            # 2. 도착지 중복 검사 리스트 추가
            date_match = re.match(r'(\d{4}-\d{2}-\d{2}) \((.+)\)', slot_info_str)
            if date_match:
                date_part, slot_name = date_match.groups()
                # 시간대만 추출 (예: "8:30(1)_당직" -> "8:30")
                time_part_match = re.match(r'(\d{1,2}:\d{2})', slot_name)
                if time_part_match:
                    time_part = time_part_match.group(1)
                    # (날짜, 시간대, 변경 후 사람)을 기준으로 중복 확인
                    request_destinations.append((date_part, time_part, person_after))

    # [검사 1: 출처 충돌]
    source_counts = Counter(request_sources)
    source_conflicts = [item for item, count in source_counts.items() if count > 1]
    if source_conflicts:
        st.warning(
            "⚠️ **요청 출처 충돌**: 동일한 방(시간대)에 대한 변경 요청이 2개 이상 있습니다. "
            "목록의 가장 위에 있는 요청이 먼저 반영되며, 이후 요청은 무시될 수 있습니다."
        )
        for conflict_item in source_conflicts:
            formatted_slot = convert_date_format(conflict_item)
            st.info(f"- **{formatted_slot}** 에 대한 요청이 중복되었습니다.")

    # [검사 2: 도착지 중복]
    dest_counts = Counter(request_destinations)
    dest_conflicts = [item for item, count in dest_counts.items() if count > 1]
    if dest_conflicts:
        st.warning(
            "⚠️ **요청 도착지 중복**: 한 사람이 같은 날, 같은 시간대에 여러 방에 배정될 가능성이 있는 요청이 있습니다. "
            "이 경우, 먼저 처리되는 요청만 반영됩니다."
        )
        for date, period, person in dest_conflicts:
            # 날짜 포맷팅을 위해 임시 문자열 생성
            temp_slot_info = f"{date} ({period})"
            formatted_date = convert_date_format(temp_slot_info)
            # 시간대만 표시하도록 재조정 (예: "10월 23일 (목) - 8:30")
            display_text = formatted_date.split(' - ')[0] + f" - {period} 시간대"
            st.info(f"- **'{person}'** 님이 **{display_text}** 에 중복으로 배정될 가능성이 있습니다.")

else:
    st.info("접수된 변경 요청이 없습니다.")
st.divider()

# --- UI 및 데이터 핸들링 (수정된 부분) ---
st.subheader("✍️ 방배정 최종 수정")
st.write("- 요청사항을 **일괄 적용/취소**하거나, 셀을 더블클릭하여 직접 수정한 후 **최종 저장 버튼**을 누르세요.\n- 하단에서 방배정 수행 버튼을 누르면 위 변경사항이 반영된 '**스케줄 최종**' 버전이 저장됩니다.")
col1, col2 = st.columns(2)
# [추가] 세션에 저장된 메시지를 항상 표시하는 로직
if "page7_messages" in st.session_state and st.session_state["page7_messages"]:
    for msg_type, msg_text in st.session_state["page7_messages"]:
        if msg_type == 'success':
            st.success(msg_text)
        elif msg_type == 'warning':
            st.warning(msg_text)
        elif msg_type == 'error':
            st.error(msg_text)
        elif msg_type == 'info':
            st.info(msg_text)

with col1:
    if st.button("🔄 요청사항 일괄 적용"):
        # 메시지 리스트를 먼저 비워줍니다.
        st.session_state['page7_messages'] = []
        if not st.session_state.df_change_requests.empty:
            current_df = st.session_state.df_final_assignment
            requests_df = st.session_state.df_change_requests
            special_df = st.session_state.df_special_schedules
            st.session_state.df_before_apply = current_df.copy()
            
            # [수정] 4개의 반환값을 모두 받음
            modified_df, new_changes, modified_special_df, messages = apply_assignment_swaps(current_df, requests_df, special_df)
            
            # [수정] 반환된 메시지를 세션에 저장
            st.session_state['page7_messages'] = messages
            
            st.session_state.df_final_assignment = modified_df
            st.session_state.df_special_schedules = modified_special_df
            if not isinstance(st.session_state.changed_cells_log, list):
                st.session_state.changed_cells_log = []
            existing_keys = {(log['날짜'], log['방배정']) for log in st.session_state.changed_cells_log}
            for change in new_changes:
                if (change['날짜'], change['방배정']) not in existing_keys:
                    st.session_state.changed_cells_log.append(change)
                    existing_keys.add((change['날짜'], change['방배정']))
            st.session_state.has_changes_to_revert = True
            st.rerun()
        else:
            # [수정] 직접 메시지를 표시하는 대신 세션에 저장
            st.session_state['page7_messages'] = [('info', "ℹ️ 처리할 변경 요청이 없습니다.")]
            st.rerun()
with col2:
    if st.button("⏪ 적용 취소", disabled=not st.session_state.has_changes_to_revert):
        st.session_state.df_final_assignment = st.session_state.df_before_apply.copy()
        st.session_state.changed_cells_log = []
        st.session_state.has_changes_to_revert = False
        # [수정] 직접 메시지를 표시하는 대신 세션에 저장
        st.session_state['page7_messages'] = [('info', "변경사항이 취소되고 원본 스케줄로 돌아갑니다.")]
        st.rerun()

# 실시간 차이 비교 및 로그 생성 준비
batch_log = st.session_state.get("changed_cells_log", [])
manual_change_log = []
oncall_warnings = []

base_df = st.session_state.df_final_assignment 

edited_df = st.data_editor(
    st.session_state.df_final_assignment,
    use_container_width=True,
    # [수정] 키를 변수로 설정하여 버튼 누를 때마다 강제 리셋
    key=f"assignment_editor_top_{st.session_state['editor_key']}", 
    disabled=['날짜', '요일'],
    hide_index=True
)

# 변경 사항 감지 및 로그 생성 (통합 로직)
if not edited_df.equals(base_df):
    diff_mask = (edited_df != base_df) & (edited_df.notna() | base_df.notna())
    
    for col in diff_mask.columns:
        if diff_mask[col].any():
            for idx in diff_mask.index[diff_mask[col]]:
                date_val = edited_df.at[idx, '날짜']
                day_val = edited_df.at[idx, '요일']
                
                new_val = str(edited_df.at[idx, col]).strip() if pd.notna(edited_df.at[idx, col]) else ""
                old_val = str(base_df.at[idx, col]).strip() if pd.notna(base_df.at[idx, col]) else ""

                if new_val != old_val:
                    # 일반 로그 추가
                    manual_change_log.append({
                        '날짜': f"{date_val} ({day_val})",
                        '방배정': col,
                        '변경 전 인원': old_val,
                        '변경 후 인원': new_val
                    })
                    
                    # [수정 2] 당직/온콜 경고 메시지 통합
                    if '온콜' in col or '당직' in col:
                        # A -> B
                        if old_val and new_val:
                             oncall_warnings.append(f"• {date_val}: '{old_val}' 오전당직 누적 -1, '{new_val}' 누적 +1")
                        # A -> 빈 값
                        elif old_val:
                             oncall_warnings.append(f"• {date_val}: '{old_val}' 오전당직 누적 -1")
                        # 빈 값 -> B
                        elif new_val:
                             oncall_warnings.append(f"• {date_val}: '{new_val}' 오전당직 누적 +1")

# 로그 표시
final_log_to_display = batch_log + manual_change_log

st.write(" ")
st.caption("📝 변경사항 미리보기")

# 2. 일괄 적용 로그와 수동 변경 로그를 합쳐서 표시
batch_log = st.session_state.get("swapped_assignments_log", [])
st.session_state["final_change_log"] = batch_log + manual_change_log

if st.session_state["final_change_log"]:
    log_df = pd.DataFrame(st.session_state["final_change_log"])
    st.dataframe(log_df, use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")

# --- ▼▼▼ [경고 메시지 표시 로직 추가] (L1448 다음 줄) ▼▼▼ ---
if oncall_warnings:
    # 리스트의 중복을 제거하고 날짜순으로 정렬
    sorted_warnings = sorted(list(set(oncall_warnings)))
    
    # [수정] 경고 메시지에 안내 문구 추가
    warning_text = (
        "🔔 **오전당직 누적 수치 변경 알림**\n\n" +
        "\n".join(sorted_warnings) +
        "\n\n(하단 '방배정 수행' 버튼을 누르면 이 누적 수치가 최종 저장됩니다.)"
    )
    st.warning(warning_text)
# --- ▲▲▲ [추가 완료] ▲▲▲

st.divider()

# --- 2. 방배정 수행 버튼 (저장 및 결과 보기) ---
# [핵심 변경] '변경사항 저장' 버튼 삭제하고, '수행' 버튼 하나로 통합

# 2. 캐시된 함수를 호출하여 3개 시트의 존재 여부 확인
curr_dt = datetime.strptime(month_str, "%Y년 %m월")
next_dt = curr_dt + relativedelta(months=1)
next_month_str = next_dt.strftime("%Y년 %-m월")
final_sheets_exist = check_final_sheets_exist(month_str, next_month_str)

if final_sheets_exist:
    st.warning(
        "⚠️ **덮어쓰기 경고**\n\n"
        "이미 Google Sheets에 다음달의 방배정 최종 결과 시트가 존재합니다.\n\n"
        "배정을 다시 수행하면 '이어서 작업'되지 않으며, 현재 화면의 설정을 기준으로 **처음부터 다시 계산하여 기존 시트들을 덮어쓰기**합니다."
    )

if st.button("🚀 최종 방배정 수행", type="primary", use_container_width=True):
    with st.spinner("수기 수정사항을 초기화하고, 원본 상태로 '방배정 최종' 시트에 저장합니다..."):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
            
            final_sheet_name = f"{month_str} 방배정 최종"

            try:
                worksheet_final = sheet.worksheet(final_sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet_final = sheet.add_worksheet(title=final_sheet_name, rows=100, cols=30)
            
            original_df = st.session_state.df_final_assignment
            
            final_data_list = [original_df.columns.tolist()] + original_df.fillna('').values.tolist()
            update_sheet_with_retry(worksheet_final, final_data_list)
            
            st.session_state['show_final_results'] = True
            
            st.session_state["editor_key"] += 1 

            st.session_state.changed_cells_log = []
            st.session_state.has_changes_to_revert = False
            
            # 6. 하단 에디터의 기준점도 원본으로 확실하게 재설정
            st.session_state.df_final_assignment_base = original_df.copy()

            st.success(f"✅ '{final_sheet_name}' 시트가 원본 상태로 저장/초기화 되었습니다.")
            time.sleep(1)
            st.rerun()

        except Exception as e:
            st.error(f"저장 및 수행 중 오류 발생: {e}")
# ---------------------------------------------------------------------------
# [하단 섹션] 방배정 결과 검토 및 수정 (덮어쓰기 모드)
# ---------------------------------------------------------------------------
if st.session_state.get('show_final_results', False):
    st.divider()
    
    # 1. 기준 데이터 로드
    if 'df_final_assignment_base' not in st.session_state:
        st.session_state.df_final_assignment_base = st.session_state.df_final_assignment.copy()
    current_schedule = st.session_state.df_final_assignment_base
    
    # 2. 방배정 스케줄 에디터
    st.markdown("**✅ 방배정 스케줄 (수정 가능)**") 
    edited_final_schedule = st.data_editor(
        current_schedule,
        use_container_width=True,
        hide_index=True,
        disabled=['날짜', '요일'],
        # [핵심 수정] 이 key가 바뀌어야 에디터가 백창희를 잊어버리고 배정호로 돌아옵니다.
        key=f"final_schedule_editor_{st.session_state['editor_key']}"
    )
    
    with st.spinner("통계 재계산 중..."):
        # 3-1. 집계용 변수 초기화
        # time_slots 키를 미리 확보하기 위해 세션 등에서 가져오거나, 기본값 설정
        # (Page 7 문맥상 time_slots 변수가 없다면 하드코딩된 순서 사용)
        time_order = ['8:30', '9:00', '9:30', '10:00', '13:30']
        
        # 현재 스케줄에 있는 모든 슬롯 이름을 수집하여 카운터 초기화
        all_active_slots = set()
        for col in edited_final_schedule.columns:
            if col not in ['날짜', '요일']:
                all_active_slots.add(col)

        total_stats = {
            'early': Counter(), 
            'late': Counter(), 
            'morning_duty': Counter(), 
            'afternoon_duty': Counter(),
            'time_room_slots': {s: Counter() for s in all_active_slots}
        }

        # 휴일 날짜 목록 (Page 7 방식에 맞게 추출)
        special_dates_set = set()
        if "df_special_schedules" in st.session_state and not st.session_state.df_special_schedules.empty:
             try:
                 special_dates_set = set(st.session_state.df_special_schedules['날짜'].tolist()) # YYYY-MM-DD 형식 가정
                 # 만약 형식이 '10월 1일' 방식이라면 변환 필요. 여기서는 단순 비교.
             except: pass

        # 3-2. 수정된 스케줄(edited_final_schedule) 순회하며 카운트
        for _, row in edited_final_schedule.iterrows():
            date_val = row['날짜']
            # 날짜 형식이 '10월 1일'이고 special_dates가 '2025-10-01'이라면 매칭 안될 수 있음.
            # Page 7의 특성상 이미 걸러졌다고 가정하거나, 단순히 당직 로직만 계산.
            
            # (휴일 여부는 데이터 특성에 따라 체크, 여기서는 모든 행 계산하되 필요시 제외)
            # if date_val in special_dates_set: continue 

            # 각 슬롯(컬럼)별 인원 확인
            for slot_name, person in row.items():
                if slot_name in ['날짜', '요일'] or not person: continue
                person = str(person).strip()
                if not person: continue
                
                # A. 오전 당직 판별 (슬롯 이름 기준)
                if slot_name == '오전당직(온콜)' or (slot_name.startswith('8:30') and '_당직' in slot_name):
                    total_stats['morning_duty'][person] += 1
                
                # B. 오후 당직 판별
                elif slot_name.startswith('13:30') and '_당직' in slot_name:
                    total_stats['afternoon_duty'][person] += 1
                
                # C. 이른방 (8:30, 당직 제외)
                elif slot_name.startswith('8:30') and '_당직' not in slot_name:
                    total_stats['early'][person] += 1
                    
                # D. 늦은방 (10:00)
                elif slot_name.startswith('10:00'):
                    total_stats['late'][person] += 1
                
                # E. 시간대별 상세
                if slot_name in total_stats['time_room_slots']:
                    total_stats['time_room_slots'][slot_name][person] += 1

        # 3-3. 누적 데이터(True Base)와 결합
        stats_data = []
        # 세션에 저장된 원본 누적 데이터 사용 (Page 6와 동일한 소스)
        df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame())
        
        # 누적 맵 로드 (없으면 0 처리)
        old_pm_cumul = df_cumulative.set_index('이름')['오후당직누적'].to_dict() if not df_cumulative.empty and '오후당직누적' in df_cumulative.columns else {}
        old_pm_source = df_cumulative.set_index('이름')['오후당직'].to_dict() if not df_cumulative.empty and '오후당직' in df_cumulative.columns else {}
        old_am_cumul = df_cumulative.set_index('이름')['오전당직누적'].to_dict() if not df_cumulative.empty and '오전당직누적' in df_cumulative.columns else {}
        old_am_source = df_cumulative.set_index('이름')['오전당직'].to_dict() if not df_cumulative.empty and '오전당직' in df_cumulative.columns else {}

        # 통계에 표시할 모든 인원 추출
        # 1. 현재 스케줄에 배정된 모든 사람
        active_people = set(total_stats['morning_duty'].keys()) | set(total_stats['afternoon_duty'].keys()) | \
                        set(total_stats['early'].keys()) | set(total_stats['late'].keys())
        # 2. 누적 데이터에 있는 사람
        cumulative_people = set(old_pm_cumul.keys())
        
        all_personnel = sorted(list(active_people | cumulative_people))

        for person in all_personnel:
            # [핵심 로직] (과거 누적 - 과거 이번달) + 현재 집계된 이번달
            pm_base = int(old_pm_cumul.get(person, 0)) - int(old_pm_source.get(person, 0))
            pm_final_cum = pm_base + total_stats['afternoon_duty'][person]
            
            am_base = int(old_am_cumul.get(person, 0)) - int(old_am_source.get(person, 0))
            am_final_cum = am_base + total_stats['morning_duty'][person]

            entry = {
                '인원': person,
                '이른방 합계': total_stats['early'][person],
                '늦은방 합계': total_stats['late'][person],
                '오전당직': total_stats['morning_duty'][person],
                '오전당직 누적': am_final_cum,
                '오후당직': total_stats['afternoon_duty'][person],
                '오후당직 누적': pm_final_cum
            }
            
            # 시간대별 합계 추가 (정렬을 위해)
            for slot in all_active_slots:
                 if not slot.endswith('_당직') and not slot == '오전당직(온콜)':
                     entry[f'{slot} 합계'] = total_stats['time_room_slots'].get(slot, Counter())[person]
            
            stats_data.append(entry)

        # 3-4. DataFrame 생성 및 포맷팅
        if stats_data:
            # 컬럼 순서 정의
            base_cols = ['인원', '이른방 합계', '늦은방 합계', '오전당직', '오전당직 누적', '오후당직', '오후당직 누적']
            # 시간대 컬럼 정렬 (8:30 -> 9:00 -> ... 순서)
            sorted_slot_cols = sorted(
                [col for col in stats_data[0].keys() if col not in base_cols],
                key=lambda x: (
                    time_order.index(x.split('(')[0]) if x.split('(')[0] in time_order else 99, 
                    x
                )
            )
            final_cols = base_cols + sorted_slot_cols
            
            df_temp = pd.DataFrame(stats_data)
            # 없는 컬럼 0 채우기
            for c in final_cols:
                if c not in df_temp.columns: df_temp[c] = 0
            
            # 최종 형태: 행=항목, 열=이름 (Transpose)
            recalculated_stats = df_temp[final_cols].set_index('인원').transpose().reset_index().rename(columns={'index': '항목'})
        else:
            recalculated_stats = pd.DataFrame(columns=['항목'])

    # ---------------------------------------------------------------------------
    # [끝] 통계 자동 재계산 완료
    # ---------------------------------------------------------------------------

    # 4. 스케줄 변경 로그
    st.markdown("📝 **방배정 스케줄 수정사항**")
    schedule_logs = []
    original_room_df = st.session_state.df_final_assignment_base # 저장 시점의 원본
    if not edited_final_schedule.equals(original_room_df):
        try:
            diff_indices = np.where(edited_final_schedule.astype(str).ne(original_room_df.astype(str)))
            changed_cells = set(zip(diff_indices[0], diff_indices[1]))
            for row_idx, col_idx in changed_cells:
                date_str = edited_final_schedule.iloc[row_idx, 0]  # 변경
                slot_name = edited_final_schedule.columns[col_idx] # 변경
                old_value = original_room_df.iloc[row_idx, col_idx]
                new_value = edited_final_schedule.iloc[row_idx, col_idx] # 변경
                log_msg = f"{date_str} '{slot_name}' 변경: '{old_value}' → '{new_value}'"
                schedule_logs.append(log_msg)
        except Exception as e:
            schedule_logs.append(f"[로그 오류] 방배정 변경사항을 비교하는 중 오류: {e}")
    if  schedule_logs:
        st.code("\n".join(f"• {msg}" for msg in sorted(schedule_logs)), language='text')
    else:
        st.info("수정된 사항이 없습니다.")
    # --- ▲▲▲ 방배정 로그 끝 ---

    st.divider()

    # =============================================================================
    # ▼▼▼ [통계 재계산 로직] 함수 정의 및 실행 ▼▼▼
    # =============================================================================
    
    def calculate_stats_from_schedule(schedule_df):
        """스케줄 DataFrame을 입력받아 통계 DataFrame을 반환하는 함수"""
        if schedule_df is None or schedule_df.empty:
            return pd.DataFrame(columns=['항목'])

        # 1. 집계 카운터 초기화
        temp = {
            'early': Counter(), 'late': Counter(), 
            'morning_duty': Counter(), 'afternoon_duty': Counter(),
            'time_slots': Counter()
        }
        
        # 휴일 날짜 처리 (문자열 집합으로 변환)
        special_dates_s = set()
        if "df_special_schedules" in st.session_state and not st.session_state.df_special_schedules.empty:
            try: special_dates_s = set(st.session_state.df_special_schedules['날짜'].astype(str).tolist())
            except: pass

        # 2. 스케줄 순회 및 카운트
        for _, row in schedule_df.iterrows():
            if str(row.iloc[0]) in special_dates_s: continue # 휴일 제외
            
            for col_name, val in row.items():
                if col_name in ['날짜', '요일'] or not val: continue
                person = str(val).replace(u'\xa0', ' ').strip()
                if not person: continue
                
                # [중요] 당직 여부 확인
                is_duty_slot = '_당직' in col_name or col_name == '오전당직(온콜)'
                
                # (1) 시간대별 합계 카운트 (당직 방은 제외!)
                if not is_duty_slot:
                    time_prefix = col_name.split('(')[0]
                    temp['time_slots'][(time_prefix, person)] += 1
                
                # (2) 주요 지표 카운트
                if col_name == '오전당직(온콜)' or (col_name.startswith('8:30') and '_당직' in col_name):
                    temp['morning_duty'][person] += 1
                elif col_name.startswith('13:30') and '_당직' in col_name:
                    temp['afternoon_duty'][person] += 1
                elif col_name.startswith('8:30') and not is_duty_slot:
                    temp['early'][person] += 1
                elif col_name.startswith('10:00'):
                    temp['late'][person] += 1

        # 3. 누적 데이터와 결합
        df_cum_base = st.session_state.get("df_cumulative", pd.DataFrame())
        # 누적값 로드 (없으면 0)
        map_am_cum = df_cum_base.set_index('이름')['오전당직누적'].to_dict() if not df_cum_base.empty and '오전당직누적' in df_cum_base.columns else {}
        map_am_src = df_cum_base.set_index('이름')['오전당직'].to_dict() if not df_cum_base.empty and '오전당직' in df_cum_base.columns else {}
        map_pm_cum = df_cum_base.set_index('이름')['오후당직누적'].to_dict() if not df_cum_base.empty and '오후당직누적' in df_cum_base.columns else {}
        map_pm_src = df_cum_base.set_index('이름')['오후당직'].to_dict() if not df_cum_base.empty and '오후당직' in df_cum_base.columns else {}
        
        # 인원 목록 추출 (스케줄에 있는 사람 + 누적 데이터에 있는 사람)
        active_p = set(temp['morning_duty'].keys()) | set(temp['afternoon_duty'].keys()) | \
                   set(temp['early'].keys()) | set(temp['late'].keys()) | {p for t, p in temp['time_slots'].keys()}
        all_p = sorted(list(active_p | set(map_am_cum.keys())))
        
        rows_list = []
        t_headers = ['8:30', '9:00', '9:30', '10:00', '13:30']
        
        for p in all_p:
            # 진짜 누적 = (DB누적 - DB이번달) + 실시간 카운트
            am_fin = (int(map_am_cum.get(p, 0)) - int(map_am_src.get(p, 0))) + temp['morning_duty'][p]
            pm_fin = (int(map_pm_cum.get(p, 0)) - int(map_pm_src.get(p, 0))) + temp['afternoon_duty'][p]
            
            r = {
                '인원': p,
                '이른방 합계': temp['early'][p], '늦은방 합계': temp['late'][p],
                '오전당직': temp['morning_duty'][p], '오전당직 누적': am_fin,
                '오후당직': temp['afternoon_duty'][p], '오후당직 누적': pm_fin
            }
            for t in t_headers: r[f'{t} 합계'] = temp['time_slots'][(t, p)]
            rows_list.append(r)
            
        if not rows_list: return pd.DataFrame(columns=['항목'])
        
        # DataFrame 생성 및 Transpose
        fixed_cols = ['인원', '이른방 합계', '늦은방 합계', '오전당직', '오전당직 누적', '오후당직', '오후당직 누적'] + [f'{t} 합계' for t in t_headers]
        res_df = pd.DataFrame(rows_list)
        for c in fixed_cols: 
            if c not in res_df.columns: res_df[c] = 0
        return res_df[fixed_cols].set_index('인원').transpose().reset_index().rename(columns={'index': '항목'})

    # ---------------------------------------------------------------------------
    
    # 1. [현재 통계 계산] 사용자가 수정한 스케줄 기준 (Data Editor에 표시될 값)
    recalculated_stats = calculate_stats_from_schedule(edited_final_schedule)
    
    # 2. [원본 통계 계산] 수정 전 원본 스케줄 기준 (비교 대상)
    original_stats_df = calculate_stats_from_schedule(original_room_df)

    # 5. 통계 테이블 에디터
    st.markdown("**☑️ 통계 테이블 (수정 가능)**")
    st.write("- 통계 테이블은 '방배정 스케줄' 편집기에 반영된 내용을 바탕으로 자동 재계산됩니다.")
    
    # [중요] key 값을 유니크하게 유지하고, 중복 호출을 제거함
    edited_final_stats = st.data_editor(
        recalculated_stats,
        use_container_width=True,
        hide_index=True,
        disabled=['항목'],
        key="final_stats_editor_unique"  # Key 충돌 방지를 위해 이름 변경
    )

    # 6. 통계 변경 로그 생성
    st.markdown("📝 **통계 테이블 수정사항**")
    stats_change_log = []
    
    # 정렬 순서 정의
    desired_order = ["이른방 합계", "늦은방 합계", "오전당직", "오전당직 누적", "오후당직", "오후당직 누적"]
    order_map = {name: i for i, name in enumerate(desired_order)}

    # [비교 로직] 원본(original_stats_df) vs 현재(edited_final_stats)
    if not edited_final_stats.equals(original_stats_df):
        try:
            # 값 비교를 위해 문자열로 변환
            s_orig = original_stats_df.astype(str)
            s_edit = edited_final_stats.astype(str)
            
            diffs = np.where(s_edit.ne(s_orig))
            changed_indices = set(zip(diffs[0], diffs[1]))
            
            for r_idx, c_idx in changed_indices:
                stat_name = edited_final_stats.iloc[r_idx, 0] # 항목명
                person_name = edited_final_stats.columns[c_idx] # 인원명
                
                old_val = original_stats_df.iloc[r_idx, c_idx]
                new_val = edited_final_stats.iloc[r_idx, c_idx]
                
                # 0 -> 0 변경 (형식 차이 등)은 무시
                if str(old_val) == str(new_val): continue

                log_msg = f"{person_name} '{stat_name}' 변경: {old_val} → {new_val}"
                sort_k = order_map.get(stat_name, 99)
                stats_change_log.append((person_name, sort_k, log_msg))
                
        except Exception as e:
            stats_change_log.append(("Error", 999, f"[오류] 통계 비교 중: {e}"))

    if stats_change_log:
        # 이름순 -> 항목순 정렬
        stats_change_log.sort(key=lambda x: (x[0], x[1]))
        log_text = "\n".join(f"• {item[2]}" for item in stats_change_log)
        st.code(log_text, language='text')
    else:
        st.info("수정된 사항이 없습니다.")

    st.divider()
    
    # =============================================================================
    # ▲▲▲ [수정 완료] ▲▲▲
    # =============================================================================

    # 6. 저장 및 다운로드
    # [핵심] 수정사항이 있든 없든 저장을 눌러서 덮어쓰기 및 파일 생성 가능
    
    c1, c2 = st.columns(2)
    
    with c1:
        if st.button("💾 수정사항 Google Sheet에 저장", type="primary", use_container_width=True):
            with st.spinner("데이터를 덮어쓰고 엑셀 파일을 생성합니다..."):
                try:
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                    
                    # A. 방배정 시트 (덮어쓰기)
                    ws_final_name = f"{month_str} 방배정 최종"
                    try:
                        ws_final = sheet.worksheet(ws_final_name)
                    except:
                        ws_final = sheet.add_worksheet(ws_final_name, 100, 30)
                    
                    room_data = [edited_final_schedule.columns.tolist()] + edited_final_schedule.fillna('').values.tolist()
                    update_sheet_with_retry(ws_final, room_data)
                    
                    # B. 누적 통계 시트 (덮어쓰기)
                    curr_dt = datetime.strptime(month_str, "%Y년 %m월")
                    next_dt = curr_dt + relativedelta(months=1)
                    next_month_str = next_dt.strftime("%Y년 %-m월")
                    cum_name = f"{next_month_str} 누적 최종"
                    
                    try:
                        ws_cum = sheet.worksheet(cum_name)
                    except:
                        ws_cum = sheet.add_worksheet(cum_name, 100, 30)
                        
                    stats_data = [edited_final_stats.columns.tolist()] + edited_final_stats.fillna('').values.tolist()
                    update_sheet_with_retry(ws_cum, stats_data)

                    with st.spinner("Excel 파일을 생성 중입니다..."):
                        # 안전장치
                        final_df_to_save = st.session_state.get("df_final_assignment", pd.DataFrame())
                        df_before_compare = st.session_state.get("df_before_apply", pd.DataFrame())
                        
                        if final_df_to_save.empty: st.stop()

                        import openpyxl
                        from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
                        from openpyxl.utils import get_column_letter
                        from openpyxl.comments import Comment

                        wb = openpyxl.Workbook()
                        sheet = wb.active
                        sheet.title = "방배정 변경"

                        import platform
                        font_name = "맑은 고딕" if platform.system() == "Windows" else "Arial"
                        
                        # 스타일
                        font_bold = Font(name=font_name, size=9, bold=True)
                        font_default = Font(name=font_name, size=9)
                        font_duty = Font(name=font_name, size=9, bold=True, color="FF00FF")

                        thin_side = Side(style='thin')
                        thick_side = Side(style='medium')
                        border_thin = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

                        # 색상
                        fill_header_830 = PatternFill(start_color="FFE699", fill_type="solid")
                        fill_header_900 = PatternFill(start_color="F8CBAD", fill_type="solid")
                        fill_header_930 = PatternFill(start_color="B4C6E7", fill_type="solid")
                        fill_header_1000 = PatternFill(start_color="C6E0B4", fill_type="solid")
                        fill_header_1330 = PatternFill(start_color="CC99FF", fill_type="solid")
                        fill_gray = PatternFill(start_color="808080", fill_type="solid")
                        fill_light_gray = PatternFill(start_color="BFBFBF", fill_type="solid")
                        fill_yoil = PatternFill(start_color="FFF2CC", fill_type="solid")
                        fill_holiday = PatternFill(start_color="DDEBF7", fill_type="solid")
                        fill_change = PatternFill(start_color="F2DCDB", fill_type="solid")
                        
                        fill_stats_header = PatternFill(start_color="E7E6E6", fill_type="solid")
                        fill_stats_label = PatternFill(start_color="D0CECE", fill_type="solid")
                        fill_row_early = PatternFill(start_color="FFE699", fill_type="solid")
                        fill_row_late = PatternFill(start_color="C6E0B4", fill_type="solid")
                        fill_row_am = PatternFill(start_color="B8CCE4", fill_type="solid")
                        fill_row_cum = PatternFill(start_color="FFC8CD", fill_type="solid")

                        # ==========================================
                        # 1. 스케줄 테이블
                        # ==========================================
                        cols = final_df_to_save.columns.tolist()
                        
                        # 헤더
                        for i, col in enumerate(cols, 1):
                            cell = sheet.cell(1, i, col)
                            cell.font = font_bold
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.border = border_thin
                            
                            if '8:30' in col or '온콜' in col: cell.fill = fill_header_830
                            elif '9:00' in col: cell.fill = fill_header_900
                            elif '9:30' in col: cell.fill = fill_header_930
                            elif '10:00' in col: cell.fill = fill_header_1000
                            elif '13:30' in col: cell.fill = fill_header_1330
                            else: cell.fill = fill_gray

                        # 데이터
                        special_dates = []
                        if st.session_state.df_special_schedules is not None:
                            try: special_dates = [d.strftime('%-m월 %-d일').lstrip('0').replace(' 0', ' ') for d in st.session_state.df_special_schedules['날짜_dt']]
                            except: pass

                        last_row = 1
                        for r, row in enumerate(final_df_to_save.itertuples(index=False), 2):
                            date_str = row[0]
                            is_special = date_str in special_dates
                            
                            duty_name = None
                            if is_special:
                                # 휴일 당직자 찾기 (간소화)
                                try:
                                    dt = datetime.strptime(date_str, '%m월 %d일').replace(year=int(month_str[:4]))
                                    d_str = dt.strftime('%Y-%m-%d')
                                    res = st.session_state.df_special_schedules[st.session_state.df_special_schedules['날짜']==d_str]
                                    if not res.empty: duty_name = str(res.iloc[0]['당직']).strip()
                                except: pass

                            personnel = [x for x in row[2:] if x]
                            is_no_person = not any(personnel)
                            is_small = 0 < len(personnel) < 15

                            for c, val in enumerate(row, 1):
                                cell = sheet.cell(r, c, val)
                                cell.alignment = Alignment(horizontal='center', vertical='center')
                                cell.border = border_thin
                                cell.font = font_default

                                # 배경색
                                if c == 1: cell.fill = fill_gray
                                elif c == 2:
                                    if is_no_person: cell.fill = fill_gray
                                    elif is_small or is_special: cell.fill = fill_light_gray
                                    else: cell.fill = fill_yoil
                                elif is_no_person and c > 2: cell.fill = fill_gray
                                
                                if is_special and val and c > 2: cell.fill = fill_holiday

                                # 변경사항
                                val_str = str(val).strip() if pd.notna(val) else ""
                                old_str = ""
                                try:
                                    if r-2 < len(df_before_compare):
                                        old_str = str(df_before_compare.iat[r-2, c-1]).strip()
                                except: pass
                                
                                if val_str != old_str:
                                    cell.fill = fill_change
                                    cell.comment = Comment(f"변경 전: {old_str if old_str else '빈 값'}", "Edit Tracker")

                                # 폰트 (당직)
                                if val:
                                    head = cols[c-1]
                                    if is_special:
                                        if duty_name and val == duty_name: cell.font = font_duty
                                    else:
                                        if '_당직' in head or '온콜' in head: cell.font = font_duty
                            
                            last_row = r

                        # ==========================================
                        # 2. 통계 테이블 (시트 모양대로 작성)
                        # ==========================================
                        stats_start = last_row + 4
                        stats_cols = stats_df.columns.tolist()
                        
                        # 헤더 (인원 이름들)
                        for i, col in enumerate(stats_cols, 1):
                            cell = sheet.cell(stats_start, i, col)
                            cell.font = font_bold
                            cell.alignment = Alignment(horizontal='center', vertical='center')
                            cell.fill = fill_stats_header
                            
                            # [테두리] 아래쪽 굵게
                            cell.border = Border(
                                left=thick_side if i==1 else thin_side,
                                right=thick_side if i==len(stats_cols) else thin_side,
                                top=thick_side,
                                bottom=thick_side
                            )

                        # 데이터 (항목들)
                        # 구분선 항목 정의
                        sep_items = ["늦은방 합계", "오전당직 누적", "오후당직 누적"]
                        item_list = stats_df['항목'].tolist()
                        prefixes = ["8:30(", "9:00(", "9:30(", "10:00("]
                        for pf in prefixes:
                            matches = [x for x in item_list if str(x).startswith(pf)]
                            if matches: sep_items.append(matches[-1])

                        for r, row in enumerate(stats_df.itertuples(index=False), stats_start + 1):
                            item_name = str(row[0])
                            is_last = (r == stats_start + len(stats_df))
                            is_sep = (item_name in sep_items)

                            row_fill = None
                            if '이른방' in item_name: row_fill = fill_row_early
                            elif '늦은방' in item_name: row_fill = fill_row_late
                            elif item_name in ['오전당직', '오후당직']: row_fill = fill_row_am
                            elif '누적' in item_name: row_fill = fill_row_cum

                            for c, val in enumerate(row, 1):
                                cell = sheet.cell(r, c, val)
                                cell.alignment = Alignment(horizontal='center', vertical='center')
                                
                                # [테두리] 양옆 굵게, 구분선 아래 굵게
                                cell.border = Border(
                                    left=thick_side if c==1 else thin_side,
                                    right=thick_side if c==len(stats_cols) else thin_side,
                                    top=thin_side,
                                    bottom=thick_side if is_last or is_sep else thin_side
                                )

                                if c == 1: # 항목명 열
                                    cell.font = font_bold
                                    cell.fill = fill_stats_label
                                else:
                                    cell.font = font_default
                                    if row_fill: cell.fill = row_fill

                        # 열 너비
                        sheet.column_dimensions['A'].width = 11
                        for i in range(2, 50):
                            sheet.column_dimensions[get_column_letter(i)].width = 10

                        output = BytesIO()
                        wb.save(output)
                        output.seek(0)
                        st.session_state.download_file = output
                        st.session_state.download_filename = f"{month_str} 방배정_최종확정.xlsx"
                        
                    # 기준 데이터 업데이트 (덮어썼으므로 현재가 기준이 됨)
                    st.session_state.df_final_assignment = edited_final_schedule.copy()
                    st.session_state.df_cumulative_stats = edited_final_stats.copy()
                    
                    st.success("✅ 저장 및 엑셀 생성이 완료되었습니다!")
                    time.sleep(1)
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"저장 오류: {e}")

    with c2:
        # 현재 상태가 저장된 상태와 다른지 체크 (저장 유도용)
        is_modified_now = not (edited_final_schedule.equals(st.session_state.df_final_assignment) and 
                               edited_final_stats.equals(st.session_state.get("df_cumulative_stats", pd.DataFrame())))
        
        if is_modified_now:
            st.error("⚠️ 수정사항이 감지되었습니다. 먼저 '수정사항 Google Sheet에 저장' 버튼을 눌러주세요.")
            st.button("📥 방배정 최종 다운로드", disabled=True, key="dl_btn_disabled", use_container_width=True)
        
        elif st.session_state.get('download_file'):
            st.download_button(
                label="📥 방배정 최종 다운로드",
                data=st.session_state.download_file,
                file_name=st.session_state.download_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary",
                use_container_width=True
            )
        else:
            st.info("⬅️ 왼쪽 저장 버튼을 누르면 파일이 생성됩니다.")