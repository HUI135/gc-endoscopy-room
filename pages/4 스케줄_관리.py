import numpy as np
import json
import streamlit as st
import pandas as pd
import calendar
import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import uuid
import menu
import io
from collections import Counter
import re # 정규표현식을 사용하기 위해 import 추가

st.set_page_config(page_title="스케줄 관리", page_icon="⚙️", layout="wide")

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
    week_labels = [f"{i}주" for i in range(1, 6)]  # 최대 5주 가정
    result = {}

    for day in weekday_order:
        for time in time_slots:
            key = f"{day} {time}"
            employees = {}
            for name in df_split["이름"].unique():
                every_week = df_split[
                    (df_split["이름"] == name) & 
                    (df_split["요일"] == day) & 
                    (df_split["시간대"] == time) & 
                    (df_split["주차"] == "매주")
                ]
                specific_weeks = sorted(
                    df_split[
                        (df_split["이름"] == name) & 
                        (df_split["요일"] == day) & 
                        (df_split["시간대"] == time) & 
                        (df_split["주차"].isin(week_labels))
                    ]["주차"].tolist(),
                    key=lambda x: int(x.replace("주", ""))
                )
                if not every_week.empty:
                    employees[name] = None
                elif specific_weeks:
                    if set(specific_weeks) == set(week_labels):
                        employees[name] = None
                    else:
                        employees[name] = specific_weeks

            employee_list = []
            for name, weeks in employees.items():
                if weeks:
                    employee_list.append(f"{name}({','.join(weeks)})")
                else:
                    employee_list.append(name)
            
            result[key] = ", ".join(sorted(employee_list)) if employee_list else ""
    
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
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.stop()
        return df
    
    split_data = df[column_name].str.split(", ", expand=True)
    max_cols = split_data.shape[1]
    new_columns = [f"{prefix}{i+1}" for i in range(max_cols)]
    split_data.columns = new_columns
    df = df.drop(columns=[column_name])
    df = pd.concat([df, split_data], axis=1)
    return df

# Google Sheets 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 인증 정보를 불러오는 데 실패했습니다: {str(e)}")
        st.stop()

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update(data, "A1")
            return True
        except gspread.exceptions.APIError as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ API 요청이 지연되고 있습니다. {delay}초 후 재시도합니다... ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
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

def find_latest_cumulative_version_page4(sheet, month_str):
    """주어진 월의 누적 시트 중 '최종' 또는 가장 최신 버전을 찾습니다."""
    versions = {}
    prefix = f"{month_str} 누적"
    
    # 1. '최종' 시트 존재 여부 확인 (최우선)
    final_version_name = f"{prefix} 최종"
    try:
        sheet.worksheet(final_version_name)
        return final_version_name
    except WorksheetNotFound:
        pass 

    # 2. 'ver X.X' 및 기본 버전 찾기
    pattern = re.compile(f"^{re.escape(prefix)}(?: ver\s*(\d+\.\d+))?$")
    for ws in sheet.worksheets():
        match = pattern.match(ws.title)
        if match:
            version_num_str = match.group(1)
            version_num = float(version_num_str) if version_num_str else 1.0
            versions[ws.title] = version_num
            
    if not versions:
        return None
            
    return max(versions, key=versions.get)


def load_request_data_page4():
    """
    [완성본] 모든 데이터를 로드하고 '매핑' 시트를 기준으로 모든 시트의 명단을 동기화합니다.
    항상 '누적 최종' 또는 최신 버전의 누적 시트를 찾아 작업합니다.
    """
    try:
        st.cache_resource.clear()
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)

        # --- 1. 필수 시트(매핑, 마스터) 및 날짜 변수 설정 ---
        df_map = pd.DataFrame(sheet.worksheet("매핑").get_all_records())
        if df_map.empty:
            st.error("🚨 '매핑' 시트에 데이터가 없습니다. 실행을 중단합니다.")
            st.stop()
        df_master = pd.DataFrame(sheet.worksheet("마스터").get_all_records())
        
        # --- 2. 최신 '누적' 시트 찾기 및 데이터 로드/파싱 ---
        df_cumulative_temp = pd.DataFrame()
        worksheet4 = None
        
        latest_cum_sheet_name = find_latest_cumulative_version_page4(sheet, month_str)
        sheet_names = set()

        if latest_cum_sheet_name:
            try:
                worksheet4 = sheet.worksheet(latest_cum_sheet_name)
                all_values = worksheet4.get_all_values()
                if not all_values or len(all_values) < 2 or all_values[0][0] != '항목':
                    raise WorksheetNotFound # 형식이 잘못되면 새로 생성하도록 유도
                
                headers = all_values[0]
                data = all_values[1:]
                df_transposed = pd.DataFrame(data, columns=headers).set_index('항목')
                df_cumulative_temp = df_transposed.transpose().reset_index().rename(columns={'index': '이름'})
                sheet_names = set(headers[1:])

            except WorksheetNotFound:
                latest_cum_sheet_name = None # 못 찾았다고 명시
        
        if latest_cum_sheet_name is None:
            st.warning(f"'{month_str} 누적' 최신 버전을 찾을 수 없어 기본 시트를 새로 생성/동기화합니다.")
            base_sheet_name = f"{month_str} 누적"
            try: sheet.del_worksheet(sheet.worksheet(base_sheet_name))
            except: pass
            worksheet4 = sheet.add_worksheet(title=base_sheet_name, rows=100, cols=50)
            df_cumulative_temp = pd.DataFrame(columns=['이름']) 
            sheet_names = set()

        # --- 3. 명단 동기화 ('매핑' 기준) ---
        final_names = set(df_map["이름"].unique())
        if sheet_names != final_names:
            st.info(f"🔄 '매핑' 시트를 기준으로 '{worksheet4.title}' 시트의 명단을 동기화합니다.")
            
            if '이름' in df_cumulative_temp.columns and not df_cumulative_temp.empty:
                df = df_cumulative_temp.set_index('이름').T
            else: 
                df = pd.DataFrame(index=["오전누적", "오후누적", "오전당직 (목표)", "오후당직 (목표)"])

            df = df.reindex(columns=sorted(list(final_names)), fill_value='0')
            update_data = [["항목"] + df.columns.tolist()] + df.reset_index().values.tolist()
            update_sheet_with_retry(worksheet4, update_data)

            all_values = worksheet4.get_all_values()
            df_transposed = pd.DataFrame(all_values[1:], columns=all_values[0]).set_index('항목')
            df_cumulative = df_transposed.transpose().reset_index().rename(columns={'index': '이름'})
        else:
            df_cumulative = df_cumulative_temp

        # --- 4. 최종 후처리 및 세션 상태 저장 ---
        for col in ["오전누적", "오후누적", "오전당직 (목표)", "오후당직 (목표)"]:
            if col in df_cumulative.columns:
                df_cumulative[col] = pd.to_numeric(df_cumulative[col], errors='coerce').fillna(0).astype(int)

        st.session_state["df_cumulative"] = df_cumulative.sort_values(by="이름")
        st.session_state["worksheet4"] = worksheet4

        # (이하 나머지 시트 로드 및 세션 상태 저장 코드는 기존과 동일하게 유지)
        df_request = pd.DataFrame(sheet.worksheet(f"{month_str} 요청").get_all_records()) # 간소화
        st.session_state.update({
            "df_map": df_map.sort_values(by="이름"),
            "df_master": df_master,
            "df_request": df_request,
            "df_shift": generate_shift_table(df_master),
            "df_supplement": generate_supplement_table(generate_shift_table(df_master), df_master["이름"].unique())
        })
        load_holiday_schedule()
        load_closing_days_schedule()

        return True

    except Exception as e:
        st.error(f"데이터 로드 및 동기화 중 심각한 오류 발생: {e}")
        import traceback
        st.code(traceback.format_exc())
        return False

def load_holiday_schedule():
    """'YYYY년 토요/휴일 스케줄' 시트에서 데이터를 다시 로드하여 세션 상태를 업데이트합니다. (안정성 강화 v2)"""
    try:
        now = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
        today = now.date()
        next_month = today.replace(day=1) + relativedelta(months=1)

        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        worksheet_name = f"{next_month.year}년 토요/휴일 스케줄"
        
        try:
            worksheet_holiday = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{worksheet_name}' 시트를 찾을 수 없습니다. 새로 생성합니다.")
            worksheet_holiday = sheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
            worksheet_holiday.append_row(["날짜", "근무", "당직"])
        
        all_values = worksheet_holiday.get_all_values()
        
        # 시트가 비어있거나 헤더만 있는 경우 처리
        if len(all_values) < 2:
            df_holiday = pd.DataFrame(columns=["날짜", "근무", "당직"])
        else:
            headers = all_values[0]
            data = all_values[1:]
            df_holiday = pd.DataFrame(data, columns=headers)

            # --- 👇 여기가 새로운 핵심 로직입니다 ---
            # 1. '날짜' 열이 비어있거나 공백인 '유령 행'을 모두 제거합니다.
            if '날짜' in df_holiday.columns:
                # .astype(str)을 추가하여 모든 데이터를 문자열로 취급 후 공백 제거
                df_holiday = df_holiday[df_holiday['날짜'].astype(str).str.strip() != '']
            # --- 👆 여기까지 ---

        # 실제 데이터가 있는 경우에만 날짜 형식 변환 및 후처리 실행
        if not df_holiday.empty:
            # 2. 날짜 형식으로 변환 시도 (오류 발생 시 해당 값은 NaT로 변경)
            df_holiday["날짜"] = pd.to_datetime(df_holiday["날짜"], errors='coerce').dt.date
            
            # 3. 날짜 변환에 실패한 행(NaT)이 있다면 최종적으로 제거 (2차 안정성)
            df_holiday.dropna(subset=['날짜'], inplace=True)
            
            df_holiday = df_holiday.sort_values(by="날짜").reset_index(drop=True)
        
        st.session_state["df_holiday"] = df_holiday
        st.session_state["worksheet_holiday"] = worksheet_holiday
        return True

    except Exception as e:
        st.error(f"토요/휴일 데이터 리로드 중 오류 발생: {str(e)}")
        return False

def load_closing_days_schedule():
    """'YYYY년 휴관일' 시트에서 데이터를 다시 로드하여 세션 상태를 업데이트합니다."""
    try:
        # 오류 수정을 위해 날짜 변수 정의를 함수 내부에 추가
        now = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
        today = now.date()
        next_month = today.replace(day=1) + relativedelta(months=1)

        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        worksheet_name = f"{next_month.year}년 휴관일"
        try:
            worksheet_closing = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{worksheet_name}' 시트를 찾을 수 없습니다. 새로 생성합니다.")
            worksheet_closing = sheet.add_worksheet(title=worksheet_name, rows="100", cols="1")
            worksheet_closing.append_row(["날짜"])
        
        closing_data = worksheet_closing.get_all_records()
        df_closing = pd.DataFrame(closing_data) if closing_data else pd.DataFrame(columns=["날짜"])

        if not df_closing.empty and '날짜' in df_closing.columns:
            df_closing["날짜"] = pd.to_datetime(df_closing["날짜"], errors='coerce').dt.date
            df_closing = df_closing.sort_values(by="날짜").reset_index(drop=True)
        
        st.session_state["df_closing"] = df_closing
        st.session_state["worksheet_closing"] = worksheet_closing
        return True
    except Exception as e:
        st.error(f"휴관일 데이터 리로드 중 오류 발생: {str(e)}")
        return False
    
# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]
from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.datetime.now(kst)
today = now.date()
month_str = (today.replace(day=1) + relativedelta(months=1)).strftime("%Y년 %-m월")

st.header("⚙️ 스케줄 관리", divider='rainbow')

col_text, col_btn = st.columns([3, 1], vertical_alignment="center")

with col_text:
    st.caption("ℹ️ 먼저 새로고침 버튼으로 최신 데이터를 불러온 뒤 진행해주세요.")

with col_btn:
    # use_container_width=True를 쓰면 버튼이 컬럼 너비에 맞춰 깔끔하게 찹니다.
    if st.button("🔄 새로고침 (R)", use_container_width=True):
        success = False
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            try:
                success = load_request_data_page4()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"새로고침 중 예측하지 못한 오류 발생: {str(e)}")
                success = False
        if success:
                st.session_state["data_loaded"] = True
                st.success("데이터가 성공적으로 새로고침되었습니다!")
                
                # ✅ 새로 불러온 원본 데이터로 편집용 화면 데이터를 덮어씌웁니다.
                st.session_state.edited_df_holiday = st.session_state.get("df_holiday", pd.DataFrame()).copy()
                
                time.sleep(1)
                st.rerun()

# ✅✅✅ 위에서 삭제한 자리에 이 코드로 '대체' 하세요 ✅✅✅
# 앱이 처음 켜졌을 때('data_loaded'가 없을 때) 실행됩니다.
if "data_loaded" not in st.session_state:
    with st.spinner("최초 데이터를 동기화하고 불러오는 중입니다..."):
        # '새로고침' 버튼과 동일한, 이미 검증된 로딩 함수를 호출합니다.
        # 이 함수 안에는 transpose 로직이 올바르게 포함되어 있습니다.
        success = load_request_data_page4()

    if success:
        st.session_state["data_loaded"] = True
        st.success("데이터 로드 및 동기화 완료!")
        time.sleep(1)
        st.rerun()
    else:
        # 만약 최초 로딩에 실패하면, 오류 메시지를 보여주고 멈춥니다.
        st.error("데이터를 불러오는 데 실패했습니다. 새로고침 버튼을 눌러 다시 시도해주세요.")
        st.stop()

def delete_old_sheets():
    """세 달 전 및 그 이전의 모든 월별 시트를 찾아 삭제하는 함수"""
    try:
        # 1. gspread 클라이언트 및 스프레드시트 가져오기
        gc = get_gspread_client()
        url = st.secrets["google_sheet"]["url"]
        spreadsheet = gc.open_by_url(url)

        # 2. 삭제 기준이 될 '경계 날짜'를 계산합니다.
        # 오늘이 8월이면, '두 달 전 1일'은 6월 1일이 됩니다.
        # 이 날짜보다 빠른 모든 시트(5월, 4월...)가 삭제 대상입니다.
        today = now.date()
        cutoff_date = (today - relativedelta(months=2)).replace(day=1)
        
        st.warning(f"**{cutoff_date.strftime('%Y년 %m월 %d일')}** 이전의 모든 월별 시트를 삭제합니다.")
        time.sleep(1.5)

        # 3. 전체 시트 목록에서 삭제할 시트들을 찾습니다.
        all_worksheets = spreadsheet.worksheets()
        sheets_to_delete = []

        for ws in all_worksheets:
            # 시트 이름에서 'YYYY년 M월' 패턴을 찾습니다.
            match = re.match(r"(\d{4})년 (\d{1,2})월", ws.title)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
                sheet_date = datetime.date(year, month, 1)

                # 4. 시트의 날짜가 경계 날짜보다 이전인지 확인합니다.
                if sheet_date < cutoff_date:
                    sheets_to_delete.append(ws)

        if not sheets_to_delete:
            st.success("✅ 삭제할 오래된 시트가 없습니다.")
            time.sleep(1.5)
            return

        # 5. 찾은 시트들을 삭제합니다.
        deleted_count = 0
        for worksheet in sheets_to_delete:
            try:
                spreadsheet.del_worksheet(worksheet)
                deleted_count += 1
            except Exception as e:
                st.error(f"❌ '{worksheet.title}' 시트 삭제 중 오류 발생: {e}")
                time.sleep(1.5)
        
        st.success(f"✅ 총 {deleted_count}개의 오래된 시트를 성공적으로 삭제했습니다.")
        time.sleep(1.5)

    except Exception as e:
        st.error(f"전체 프로세스 중 오류 발생: {e}")
        time.sleep(1.5)

# 세션 상태에서 데이터 가져오기
df_map = st.session_state.get("df_map", pd.DataFrame(columns=["이름", "사번"]))
mapping = st.session_state.get("mapping")
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
worksheet1 = st.session_state.get("worksheet1")
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (목표)", "오후당직 (목표)"]))
worksheet4 = st.session_state.get("worksheet4")
names_in_master = df_master["이름"].unique() if not df_master.empty else []

today = now.date()
next_month = today.replace(day=1) + relativedelta(months=1)
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_start = next_month
next_month_end = next_month.replace(day=last_day)

st.divider()
st.subheader("📁 스케줄 시트 관리")
st.markdown("https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?usp=sharing")

# 세션 상태를 사용하여 확인 창 표시 여부를 제어합니다.
if 'confirm_delete' not in st.session_state:
    st.session_state.confirm_delete = False

# 상단 레이아웃: 왼쪽(버튼/expander), 오른쪽(빈공간 또는 설명)
col_delete, none = st.columns([2, 4])

with col_delete:

    # 2) expander: confirm_delete 상태에 따라 자동으로 열리게
    cutoff_date = (datetime.date.today() - relativedelta(months=2)).replace(day=1)
    exp_title = f"🗑️ 오래된 시트 정리하기"

    with st.expander(exp_title, expanded=st.session_state.confirm_delete):
        st.error("Google Sheets에서 해당 버전의 스케줄과 누적 시트가 영구적으로 삭제됩니다.")
        st.warning(f"**{cutoff_date.strftime('%Y년 %m월 %d일')}** 이전(세 달 전)의 모든 월별 시트를 삭제하시겠습니까?")

        # 긴 기본(주요) 버튼 — 컨테이너 너비 가득 채움
        if st.button("네, 삭제합니다.", type="primary",
                     use_container_width=True, key="delete_old_confirm"):
            delete_old_sheets()
            st.session_state.confirm_delete = False
            st.experimental_rerun()

        # 취소 버튼도 동일한 너비로
        if st.button("아니요, 취소합니다.", use_container_width=True, key="delete_old_cancel"):
            st.info("오래된 시트 삭제 작업을 취소하였습니다.")
            st.session_state.confirm_delete = False
            time.sleep(1)
            st.experimental_rerun()

st.divider()
st.subheader("📋 명단 관리")
st.write(" - 매핑 시트, 마스터 시트, 요청사항 시트, 누적 시트에서 인원을 추가/삭제합니다.\n- 아래 명단에 존재하는 인원만 해당 사번으로 시스템 로그인이 가능합니다.")

if "df_master" not in st.session_state or st.session_state["df_master"].empty:
    st.session_state["df_master"] = df_master.copy() if not df_master.empty else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])

df_master = st.session_state["df_master"]

if not df_map.empty:
    df_map["사번"] = df_map["사번"].astype(str).str.zfill(5)

st.dataframe(df_map.reset_index(drop=True), height=200, width=500, use_container_width=True, hide_index=True)

if "add_transaction_id" not in st.session_state:
    st.session_state["add_transaction_id"] = None

with st.form("fixed_form_namelist"):
    col_add, col_delete = st.columns([1.8, 1.2])

    with col_add:
        st.markdown("**🟢 명단 추가**")
        col_name, col_id = st.columns(2)
        with col_name:
            new_employee_name = st.text_input("이름 입력", key="new_employee_name_input")
        with col_id:
            new_employee_id = st.number_input("5자리 사번 입력", min_value=0, max_value=99999, step=1, format="%05d")

        submit_add = st.form_submit_button("✔️ 추가")
        if submit_add:
            if not new_employee_name:
                st.error("이름을 입력하세요.")
            elif new_employee_name in df_map["이름"].values:
                st.error(f"이미 존재하는 이름입니다: {new_employee_name}님은 이미 목록에 있습니다.")
            else:
                try:
                    with st.spinner("모든 시트에 새 인원을 추가하는 중입니다..."):
                        gc = get_gspread_client()
                        sheet = gc.open_by_url(url)
                        formatted_id = f"{new_employee_id:05d}"

                        # [수정] 1. 매핑 시트: append_row로 안전하게 추가
                        mapping_worksheet = sheet.worksheet("매핑")
                        mapping_worksheet.append_row([new_employee_name, formatted_id])

                        # [수정] 2. 마스터 시트: append_rows로 안전하게 추가
                        worksheet1 = sheet.worksheet("마스터")
                        new_master_rows = [[new_employee_name, "매주", day, "근무없음"] for day in ["월", "화", "수", "목", "금"]]
                        worksheet1.append_rows(new_master_rows)

                        # [수정] 3. 요청사항 시트: append_row로 안전하게 추가
                        try:
                            worksheet2 = sheet.worksheet(f"{month_str} 요청")
                            worksheet2.append_row([new_employee_name, "요청 없음", ""])
                        except WorksheetNotFound:
                             pass # 없으면 그냥 통과

                        latest_cum_sheet_name = find_latest_cumulative_version_page4(sheet, month_str)
                        if latest_cum_sheet_name:
                            worksheet4 = sheet.worksheet(latest_cum_sheet_name)
                            # gspread는 insert_cols가 없으므로, 열 전체를 다시 쓰는 방식으로 업데이트
                            all_data = worksheet4.get_all_values()
                            all_data[0].append(new_employee_name)
                            for i in range(1, len(all_data)):
                                all_data[i].append('0')
                            update_sheet_with_retry(worksheet4, all_data)

                    st.success(f"{new_employee_name}님을 모든 관련 시트에 추가했습니다.")
                    time.sleep(1.5) # 사용자가 메시지를 읽을 시간을 줍니다.

                    # ✅ 데이터를 다시 로드하여 변경사항을 즉시 반영합니다.
                    with st.spinner("최신 명단을 다시 불러오는 중..."):
                        load_request_data_page4()
                    
                    # ✅ 화면을 완전히 새로고침합니다.
                    st.rerun()
                except Exception as e:
                    st.error(f"명단 추가 중 오류 발생: {e}")

    with col_delete:
        st.markdown("**🔴 명단 삭제**")
        sorted_names = sorted(df_map["이름"].unique()) if not df_map.empty else []
        selected_employee_name = st.selectbox("이름 선택", sorted_names, key="delete_employee_select")
        
        submit_delete = st.form_submit_button("🗑️ 삭제")
        if submit_delete:
            try:
                with st.spinner("모든 시트에서 인원을 삭제하는 중입니다..."):
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(url)
                    
                    # [수정] 1. 매핑 시트: find -> delete_rows로 안전하게 삭제
                    mapping_worksheet = sheet.worksheet("매핑")
                    cell_to_delete = mapping_worksheet.find(selected_employee_name)
                    if cell_to_delete:
                        mapping_worksheet.delete_rows(cell_to_delete.row)

                    # [수정] 2. 마스터 시트: findall -> delete_rows로 안전하게 삭제
                    worksheet1 = sheet.worksheet("마스터")
                    cells_to_delete = worksheet1.findall(selected_employee_name)
                    if cells_to_delete:
                        # 역순으로 정렬하여 삭제 시 인덱스 밀림 방지
                        for cell in sorted(cells_to_delete, key=lambda x: x.row, reverse=True):
                            worksheet1.delete_rows(cell.row)
                    
                    # [수정] 3. 요청사항, 누적 시트: find -> delete_rows로 안전하게 삭제
                    try:
                        ws_req = sheet.worksheet(f"{month_str} 요청")
                        cell_req = ws_req.find(selected_employee_name)
                        if cell_req:
                            ws_req.delete_rows(cell_req.row)
                    except WorksheetNotFound:
                        pass
                    
                    latest_cum_sheet_name = find_latest_cumulative_version_page4(sheet, month_str)
                    if latest_cum_sheet_name:
                        ws_cum = sheet.worksheet(latest_cum_sheet_name)
                        cell_cum = ws_cum.find(selected_employee_name, in_row=1)
                        if cell_cum:
                            ws_cum.delete_columns(cell_cum.col)

                st.success(f"{selected_employee_name}님을 모든 관련 시트에서 삭제했습니다.")
                time.sleep(1.5)

                # ✅ 데이터를 다시 로드하여 변경사항을 즉시 반영합니다.
                with st.spinner("최신 명단을 다시 불러오는 중..."):
                    load_request_data_page4()
                
                # ✅ 화면을 완전히 새로고침합니다.
                st.rerun()
            except Exception as e:
                st.error(f"명단 삭제 중 오류 발생: {e}")

st.divider()
st.subheader("📋 마스터 관리")
st.write("- 월 단위 또는 주 단위로 선택한 인원의 마스터 스케줄을 수정할 수 있습니다.")

sorted_names = sorted(df_master["이름"].unique()) if not df_master.empty else []
selected_employee_name = st.selectbox("이름 선택", sorted_names, key="master_employee_select")
df_user_master = df_master[df_master["이름"] == selected_employee_name].copy()

근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
요일리스트 = ["월", "화", "수", "목", "금"]

today = now.date()
next_month = today.replace(day=1) + relativedelta(months=1)
_, last_day = calendar.monthrange(next_month.year, next_month.month)
c = calendar.Calendar(firstweekday=6)
month_calendar = c.monthdatescalendar(next_month.year, next_month.month)
week_nums = [i + 1 for i, _ in enumerate(month_calendar) if any(date.month == next_month.month for date in month_calendar[i])]

# 월 단위로 일괄 설정
with st.expander("📅 월 단위로 일괄 설정"):
    has_weekly = "매주" in df_user_master["주차"].values
    has_weekly_specific = any(w in df_user_master["주차"].values for w in [f"{i}주" for i in week_nums])
    
    # 기본값 설정: df_user_master에서 최신 데이터 가져오기
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    default_bulk = {}
    if has_weekly_specific:
        week_labels = [f"{i}주" for i in week_nums]
        for day in 요일리스트:
            day_values = []
            for week in week_labels:
                week_df = df_user_master[df_user_master["주차"] == week]
                day_specific = week_df[week_df["요일"] == day]
                if not day_specific.empty:
                    day_values.append(day_specific.iloc[0]["근무여부"])
                elif not every_week_df.empty:
                    day_every = every_week_df[every_week_df["요일"] == day]
                    if not day_every.empty:
                        day_values.append(day_every.iloc[0]["근무여부"])
                    else:
                        day_values.append("근무없음")
                else:
                    day_values.append("근무없음")
            if day_values:
                if all(v == day_values[0] for v in day_values):
                    default_bulk[day] = day_values[0]
                else:
                    most_common = Counter(day_values).most_common(1)[0][0]
                    default_bulk[day] = most_common
            else:
                default_bulk[day] = "근무없음"
    elif has_weekly:
        weekly_df = df_user_master[df_user_master["주차"] == "매주"]
        default_bulk = weekly_df.set_index("요일")["근무여부"].to_dict()
    # For missing days, set to "근무없음"
    for day in 요일리스트:
        if day not in default_bulk:
            default_bulk[day] = "근무없음"

    if has_weekly and all(df_user_master[df_user_master["주차"] == "매주"]["근무여부"] == "근무없음"):
        st.info("마스터 입력이 필요합니다.")
    elif has_weekly_specific:
        st.warning("현재 주차별 근무 일정이 다릅니다. 주 단위 스케줄을 확인하신 후, 월 단위로 초기화하려면 내용을 입력하세요.")

    col1, col2, col3, col4, col5 = st.columns(5)
    월값 = col1.selectbox("월", 근무옵션, index=근무옵션.index(default_bulk.get("월", "근무없음")), key=f"월_bulk_{selected_employee_name}")
    화값 = col2.selectbox("화", 근무옵션, index=근무옵션.index(default_bulk.get("화", "근무없음")), key=f"화_bulk_{selected_employee_name}")
    수값 = col3.selectbox("수", 근무옵션, index=근무옵션.index(default_bulk.get("수", "근무없음")), key=f"수_bulk_{selected_employee_name}")
    목값 = col4.selectbox("목", 근무옵션, index=근무옵션.index(default_bulk.get("목", "근무없음")), key=f"목_bulk_{selected_employee_name}")
    금값 = col5.selectbox("금", 근무옵션, index=근무옵션.index(default_bulk.get("금", "근무없음")), key=f"금_bulk_{selected_employee_name}")

    # '월 단위 저장' 버튼의 if 블록을 아래 코드로 교체
    if st.button("💾 월 단위 저장", key="save_monthly"):
        try:
            with st.spinner("월 단위 마스터 스케줄을 저장하는 중입니다..."):
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet1 = sheet.worksheet("마스터")

                # [수정] 1. 해당 직원의 기존 데이터를 모두 찾아서 삭제
                cells_to_delete = worksheet1.findall(selected_employee_name)
                if cells_to_delete:
                    for cell in sorted(cells_to_delete, key=lambda x: x.row, reverse=True):
                        worksheet1.delete_rows(cell.row)
                
                # [수정] 2. 새로운 데이터를 append_rows로 추가
                new_rows_data = [
                    [selected_employee_name, "매주", 요일, {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}[요일]]
                    for 요일 in 요일리스트
                ]
                worksheet1.append_rows(new_rows_data)

            st.success("월 단위 수정사항이 저장되었습니다.")
            time.sleep(1)
            st.info("새로고침 버튼을 눌러 변경사항을 완전히 적용해주세요.")
            time.sleep(1.5)
            st.rerun()
        except Exception as e:
            st.error(f"월 단위 저장 중 오류 발생: {e}")

with st.expander("📅 주 단위로 설정"):
    st.markdown("**주 단위로 근무 여부가 다른 경우 아래 내용들을 입력해주세요.**")
    week_labels = [f"{i}주" for i in week_nums]
    
    # 최신 df_user_master 가져오기
    df_user_master = df_master[df_master["이름"] == selected_employee_name].copy()
    st.session_state["df_user_master"] = df_user_master
    
    # master_data 초기화: 요일별로 체크
    master_data = {}
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    for week in week_labels:
        master_data[week] = {}
        week_df = df_user_master[df_user_master["주차"] == week]
        for day in 요일리스트:
            # 해당 주의 해당 요일 확인
            day_specific = week_df[week_df["요일"] == day]
            if not day_specific.empty:
                master_data[week][day] = day_specific.iloc[0]["근무여부"]
            # 없으면 매주에서 가져옴
            elif not every_week_df.empty:
                day_every = every_week_df[every_week_df["요일"] == day]
                if not day_every.empty:
                    master_data[week][day] = day_every.iloc[0]["근무여부"]
                else:
                    master_data[week][day] = "근무없음"
            else:
                master_data[week][day] = "근무없음"

    # UI: selectbox에 최신 데이터 반영
    for week in week_labels:
        st.markdown(f"**🗓 {week}**")
        col1, col2, col3, col4, col5 = st.columns(5)
        master_data[week]["월"] = col1.selectbox(f"월", 근무옵션, index=근무옵션.index(master_data[week]["월"]), key=f"{week}_월_{selected_employee_name}")
        master_data[week]["화"] = col2.selectbox(f"화", 근무옵션, index=근무옵션.index(master_data[week]["화"]), key=f"{week}_화_{selected_employee_name}")
        master_data[week]["수"] = col3.selectbox(f"수", 근무옵션, index=근무옵션.index(master_data[week]["수"]), key=f"{week}_수_{selected_employee_name}")
        master_data[week]["목"] = col4.selectbox(f"목", 근무옵션, index=근무옵션.index(master_data[week]["목"]), key=f"{week}_목_{selected_employee_name}")
        master_data[week]["금"] = col5.selectbox(f"금", 근무옵션, index=근무옵션.index(master_data[week]["금"]), key=f"{week}_금_{selected_employee_name}")

    # 나머지 저장 버튼 로직은 그대로
    if st.button("💾 주 단위 저장", key="save_weekly"):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            
            rows = []
            for 요일 in 요일리스트:
                week_shifts = [master_data[week][요일] for week in week_labels]
                if all(shift == week_shifts[0] for shift in week_shifts):
                    rows.append({"이름": selected_employee_name, "주차": "매주", "요일": 요일, "근무여부": week_shifts[0]})
                else:
                    for week in week_labels:
                        rows.append({"이름": selected_employee_name, "주차": week, "요일": 요일, "근무여부": master_data[week][요일]})
            
            df_master = df_master[df_master["이름"] != selected_employee_name]
            updated_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            
            if update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist()):
                st.session_state["df_master"] = df_result
                st.session_state["worksheet1"] = worksheet1
                st.session_state["df_user_master"] = df_result[df_result["이름"] == selected_employee_name].copy()
                
                with st.spinner("근무 및 보충 테이블 갱신 중..."):
                    st.session_state["df_shift"] = generate_shift_table(df_result)
                    st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], df_result["이름"].unique())
                
                st.success("주 단위 수정사항이 저장되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("마스터 시트 저장 실패")
                st.stop()
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (주 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"주 단위 저장 중 오류 발생: {str(e)}")
            st.stop()

st.divider()
st.subheader(f"📅 {next_month.year}년 토요/휴일 스케줄 관리")
st.write("- 아래 테이블에서 직접 스케줄을 추가, 수정, 삭제한 후 **'저장'** 버튼을 누르세요.\n - 또는 '빠른 추가/삭제' 메뉴를 이용할 수도 있습니다.")

st.write(" ")
# --- 1. 테이블 직접 수정 UI (st.data_editor) ---
st.markdown("**📋 토요/휴일 스케줄 테이블**")

# 세션 상태에 편집용 데이터가 없으면 원본 데이터로 초기화
if "edited_df_holiday" not in st.session_state:
    st.session_state.edited_df_holiday = st.session_state.get("df_holiday", pd.DataFrame()).copy()

edited_holiday_df = st.data_editor(
    st.session_state.edited_df_holiday,
    num_rows="dynamic",
    use_container_width=True,
    hide_index=True,
    key="holiday_editor", # 이 key가 페이지 내에서 유일해야 합니다.
    column_config={
        "날짜": st.column_config.DateColumn(
            "날짜 (필수)",
            format="YYYY-MM-DD",
            required=True,
        ),
        "근무": st.column_config.TextColumn("근무 (쉼표+공백으로 구분)", required=True),
        "당직": st.column_config.TextColumn("당직"),
    }
)

# --- 테이블 수정사항 저장 버튼 ---
if st.button("💾 테이블 수정사항 저장"):
    try:
        is_valid = True
        error_messages = []
        df_to_save = edited_holiday_df.copy()

        if df_to_save['날짜'].isnull().any():
            error_messages.append("모든 행에 날짜를 입력해야 합니다.")
            is_valid = False
        if df_to_save['날짜'].duplicated().any():
            error_messages.append("중복된 날짜가 있습니다. 각 날짜는 한 번만 입력할 수 있습니다.")
            is_valid = False
        for index, row in df_to_save.iterrows():
            duty_person = str(row.get('당직', '')).strip()
            work_list = [name.strip() for name in str(row.get('근무', '')).split(',')]
            if duty_person and duty_person != "당직 없음" and duty_person not in work_list:
                error_messages.append(f"{row['날짜']}의 당직자 '{duty_person}'님은 근무자 목록에 포함되어야 합니다.")
                is_valid = False

        if is_valid:
            with st.spinner("테이블 수정사항을 저장하는 중입니다..."):
                worksheet_holiday = st.session_state.get("worksheet_holiday")
                if worksheet_holiday:
                    df_to_save["날짜"] = pd.to_datetime(df_to_save["날짜"]).dt.date
                    df_to_save = df_to_save.sort_values(by="날짜").reset_index(drop=True)
                    df_to_save['날짜'] = df_to_save['날짜'].astype(str)
                    
                    update_data = [df_to_save.columns.tolist()] + df_to_save.values.tolist()
                    if update_sheet_with_retry(worksheet_holiday, update_data):
                        st.success("✅ 테이블 수정사항이 성공적으로 저장되었습니다.")
                        time.sleep(1.5)
                        load_holiday_schedule() 
                        st.session_state.edited_df_holiday = st.session_state.get("df_holiday", pd.DataFrame()).copy()
                        st.rerun()
                else:
                    st.error("❌ 시트 정보를 찾을 수 없습니다. 새로고침 후 다시 시도해주세요.")
        else:
            for msg in error_messages:
                st.error(msg)
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")

# --- 2. 빠른 추가/삭제 UI (st.expander + st.form) ---
with st.expander("➕ 빠른 추가 / 삭제"):
    st.markdown("**🟢 토요/휴일 스케줄 추가**")
    with st.form("add_holiday_schedule_form_expander"):
        col_date, col_workers, col_duty = st.columns([1, 2, 1])
        with col_date:
            new_date = st.date_input("날짜 선택", value=next_month_start, min_value=next_month_start, max_value=next_month_end, key="new_holiday_date_expander")
        with col_workers:
            available_names = sorted(df_map["이름"].unique()) if not df_map.empty else []
            new_workers = st.multiselect("근무자 선택", available_names, key="new_holiday_workers_expander")
        with col_duty:
            new_duty = st.selectbox("당직자 선택", ["당직 없음"] + available_names, key="new_holiday_duty_expander")
        
        submit_add_expander = st.form_submit_button("✔️ 추가")
        if submit_add_expander:
            df_holiday_check = st.session_state.get("df_holiday", pd.DataFrame())
            if not new_date: st.error("날짜를 선택하세요.")
            elif not new_workers: st.error("근무자를 선택하세요.")
            elif new_duty != "당직 없음" and new_duty not in new_workers: st.error("당직자는 근무자 목록에 포함되어야 합니다.")
            elif not df_holiday_check[df_holiday_check['날짜'] == new_date].empty: st.error(f"{new_date}는 이미 스케줄에 존재합니다.")
            else:
                try:
                    with st.spinner("스케줄을 추가하는 중입니다..."):
                        worksheet_holiday = st.session_state.get("worksheet_holiday")
                        new_row_data = [new_date.strftime("%Y-%m-%d"), ", ".join(new_workers), new_duty]
                        worksheet_holiday.append_row(new_row_data)
                        st.success(f"{new_date} 스케줄이 추가되었습니다.")
                        time.sleep(1.5)
                        load_holiday_schedule()
                        st.session_state.edited_df_holiday = st.session_state.get("df_holiday", pd.DataFrame()).copy()
                        st.rerun()
                except Exception as e:
                    st.error(f"스케줄 추가 중 오류 발생: {str(e)}")

    st.write("---")

    st.markdown("**🔴 토요/휴일 스케줄 삭제**")
    df_holiday_current = st.session_state.get("df_holiday", pd.DataFrame())
    if not df_holiday_current.empty:
        with st.form("delete_holiday_schedule_form_expander"):
            sorted_dates = sorted(df_holiday_current["날짜"].astype(str).unique())
            selected_date_to_delete = st.selectbox("삭제할 날짜 선택", sorted_dates, key="delete_holiday_date_expander")
            
            submit_delete_expander = st.form_submit_button("🗑️ 삭제")
            if submit_delete_expander:
                try:
                    with st.spinner("스케줄을 삭제하는 중입니다..."):
                        worksheet_holiday = st.session_state.get("worksheet_holiday")
                        cell_to_delete = worksheet_holiday.find(selected_date_to_delete)
                        if cell_to_delete:
                            worksheet_holiday.delete_rows(cell_to_delete.row)
                            st.success(f"{selected_date_to_delete} 스케줄이 삭제되었습니다.")
                            time.sleep(1.5)
                            load_holiday_schedule()
                            st.session_state.edited_df_holiday = st.session_state.get("df_holiday", pd.DataFrame()).copy()
                            st.rerun()
                        else:
                            st.warning("삭제할 날짜를 시트에서 찾을 수 없습니다.")
                except Exception as e:
                    st.error(f"스케줄 삭제 중 오류 발생: {str(e)}")
    else:
        st.info("삭제할 스케줄이 없습니다.")

# ✂️✂️✂️ st.divider() 부터 load_data_page4() 전까지 이 코드로 모두 교체하세요. ✂️✂️✂️

st.divider()
st.subheader(f"📅 {next_month.year}년 휴관일 관리")
st.write("- 아래 테이블에서 직접 휴관일을 월별로 수정하거나, '빠른 추가/삭제' 메뉴를 이용할 수 있습니다.\n - 휴관일 목록은 'YYYY-MM-DD, YYYY-MM-DD' 형식으로 입력해야 합니다.")

# --- 데이터 준비: 월별 그룹화 ---
df_closing_raw = st.session_state.get("df_closing", pd.DataFrame(columns=["날짜"]))
df_closing_monthly = pd.DataFrame(columns=['월', '휴관일 목록'])
if not df_closing_raw.empty:
    # 날짜 타입 확인 및 변환
    if not pd.api.types.is_datetime64_any_dtype(df_closing_raw['날짜']):
        df_closing_raw['날짜'] = pd.to_datetime(df_closing_raw['날짜'], errors='coerce')
        df_closing_raw.dropna(subset=['날짜'], inplace=True)
    
    df_closing_raw['월'] = df_closing_raw['날짜'].dt.strftime('%Y-%m')
    df_closing_monthly = df_closing_raw.groupby('월')['날짜'].apply(
        lambda x: ', '.join(sorted(x.dt.strftime('%Y-%m-%d')))
    ).reset_index(name='휴관일 목록')

st.write(" ")
# --- 1. 테이블 직접 수정 UI (st.data_editor) ---
st.markdown("**📋 휴관일 테이블**")
edited_closing_df = st.data_editor(
    df_closing_monthly,
    num_rows="dynamic",
    use_container_width=True,
    hide_index=True,
    key="closing_editor",
    column_config={
        "월": st.column_config.TextColumn("월 (YYYY-MM)", required=True),
        "휴관일 목록": st.column_config.TextColumn("휴관일 목록 (쉼표+공백으로 구분)"),
    }
)

# --- 테이블 수정사항 저장 버튼 ---
if st.button("💾 휴관일 테이블 저장"):
    try:
        is_valid = True
        error_messages = []
        all_dates = []

        # 유효성 검사
        for index, row in edited_closing_df.iterrows():
            month_str = str(row['월']).strip()
            dates_str = str(row.get('휴관일 목록', '')).strip()

            if not re.match(r'^\d{4}-\d{2}$', month_str):
                error_messages.append(f"{index+1}행: '월'은 'YYYY-MM' 형식이어야 합니다 (예: {month_str}).")
                is_valid = False
                continue

            if not dates_str: continue

            date_parts = [d.strip() for d in dates_str.split(',')]
            for date_part in date_parts:
                if not date_part: continue # 빈 문자열은 건너뛰기
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
                    error_messages.append(f"{index+1}행: '{date_part}'는 'YYYY-MM-DD' 형식이 아닙니다.")
                    is_valid = False
                    continue
                try:
                    date_obj = datetime.datetime.strptime(date_part, '%Y-%m-%d').date()
                    if date_obj.strftime('%Y-%m') != month_str:
                        error_messages.append(f"{index+1}행: 날짜 '{date_part}'는 해당 월 '{month_str}'에 속하지 않습니다.")
                        is_valid = False
                    all_dates.append(date_obj)
                except ValueError:
                    error_messages.append(f"{index+1}행: '{date_part}'는 유효한 날짜가 아닙니다.")
                    is_valid = False
        
        if is_valid:
            with st.spinner("휴관일 정보를 저장하는 중입니다..."):
                worksheet_closing = st.session_state.get("worksheet_closing")
                if worksheet_closing:
                    df_to_save = pd.DataFrame({'날짜': sorted(list(set(all_dates)))})
                    df_to_save['날짜'] = df_to_save['날짜'].astype(str)
                    
                    update_data = [df_to_save.columns.tolist()] + df_to_save.values.tolist()
                    if update_sheet_with_retry(worksheet_closing, update_data):
                        st.success("✅ 휴관일 정보가 성공적으로 저장되었습니다.")
                        time.sleep(1.5)
                        load_closing_days_schedule()
                        st.rerun()
                else:
                    st.error("❌ 시트 정보를 찾을 수 없습니다. 새로고침 후 다시 시도해주세요.")
        else:
            for msg in error_messages:
                st.error(msg)
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")

# --- 2. 빠른 추가/삭제 UI (st.expander + st.form) ---
with st.expander("➕ 빠른 추가 / 삭제"):
    # --- 휴관일 추가 ---
    st.markdown("**🟢 휴관일 추가**")
    st.write("- 하루만 추가하려면 시작일과 종료일을 같은 날짜로 선택하세요.")
    
    current_year = next_month.year
    year_start = datetime.date(current_year, 1, 1)
    year_end = datetime.date(current_year, 12, 31)

    with st.form("add_closing_day_form_expander"):
        selected_period = st.date_input(
            "날짜 또는 기간 선택",
            value=(next_month_start, next_month_start),
            min_value=year_start,
            max_value=year_end,
            key="new_closing_period_expander"
        )
        
        submit_add_closing = st.form_submit_button("✔️ 추가")
        if submit_add_closing:
            if not selected_period or len(selected_period) != 2:
                st.error("휴관일로 추가할 날짜 또는 기간을 선택해주세요.")
            else:
                start_date, end_date = selected_period
                if start_date > end_date:
                    st.error("시작일은 종료일보다 이전이거나 같아야 합니다.")
                else:
                    try:
                        all_dates_in_period = pd.date_range(start=start_date, end=end_date)
                        new_dates_to_add = []
                        df_closing_check = st.session_state.get("df_closing", pd.DataFrame())

                        for date in all_dates_in_period:
                            if df_closing_check[df_closing_check['날짜'] == date.date()].empty:
                                new_dates_to_add.append(date.date())

                        if not new_dates_to_add:
                            st.warning("선택하신 날짜(기간)는 모두 이미 휴관일로 등록되어 있습니다.")
                        else:
                            with st.spinner(f"{len(new_dates_to_add)}개의 휴관일을 추가하는 중입니다..."):
                                worksheet_closing = st.session_state.get("worksheet_closing")
                                rows_to_append = [[d.strftime("%Y-%m-%d")] for d in new_dates_to_add]
                                worksheet_closing.append_rows(rows_to_append)
                                st.success(f"총 {len(new_dates_to_add)}개의 휴관일이 성공적으로 추가되었습니다.")
                                time.sleep(1.5)
                                load_closing_days_schedule()
                                st.rerun()
                    except Exception as e:
                        st.error(f"휴관일 추가 중 오류가 발생했습니다: {str(e)}")

    st.write("---")
    
    # --- 휴관일 삭제 ---
    st.markdown("**🔴 휴관일 삭제**")
    df_closing_current = st.session_state.get("df_closing", pd.DataFrame())
    if not df_closing_current.empty:
        with st.form("delete_closing_day_form_expander"):
            sorted_dates = sorted(df_closing_current["날짜"].astype(str).unique())
            selected_date_to_delete = st.selectbox("삭제할 날짜 선택", sorted_dates, key="delete_closing_date_expander")
            
            submit_delete_closing = st.form_submit_button("🗑️ 삭제")
            if submit_delete_closing:
                try:
                    with st.spinner(f"{selected_date_to_delete} 휴관일을 삭제하는 중입니다..."):
                        worksheet_closing = st.session_state.get("worksheet_closing")
                        cell_to_delete = worksheet_closing.find(selected_date_to_delete)
                        if cell_to_delete:
                            worksheet_closing.delete_rows(cell_to_delete.row)
                            st.success(f"{selected_date_to_delete} 휴관일이 삭제되었습니다.")
                            time.sleep(1.5)
                            load_closing_days_schedule()
                            st.rerun()
                        else:
                            st.warning("삭제할 날짜를 시트에서 찾을 수 없습니다.")
                except Exception as e:
                    st.error(f"휴관일 삭제 중 오류 발생: {str(e)}")
    else:
        st.info("삭제할 휴관일이 없습니다.")

# --- 페이지 하단 원본 코드 ---
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (목표)", "오후당직 (목표)"]))
df_shift = st.session_state.get("df_shift", pd.DataFrame())
df_supplement = st.session_state.get("df_supplement", pd.DataFrame())

def excel_download(name, sheet1, name1, sheet2, name2, sheet3, name3, sheet4, name4):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        sheet1.to_excel(writer, sheet_name=name1, index=False)
        sheet2.to_excel(writer, sheet_name=name2, index=False)
        sheet3.to_excel(writer, sheet_name=name3, index=False)
        sheet4.to_excel(writer, sheet_name=name4, index=False)
    
    excel_data = output.getvalue()
    return excel_data
