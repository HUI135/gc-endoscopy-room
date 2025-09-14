import numpy as np
import streamlit as st
import pandas as pd
import calendar
import datetime
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import time
from collections import Counter
import menu
import streamlit as st

st.set_page_config(page_title="마스터 수정", page_icon="📅", layout="wide")

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

# Google Sheets 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except APIError as e:
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
        except APIError as e:
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

# 데이터 로드 함수
@st.cache_data(show_spinner=False)
def load_master_data_page1(_gc, url):
    try:
        sheet = _gc.open_by_url(url)
        worksheet_master = sheet.worksheet("마스터")
        data = worksheet_master.get_all_records()
        df = pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        df["요일"] = pd.Categorical(df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df = df.sort_values(by=["이름", "주차", "요일"])
        return df
    except APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (마스터 데이터): {str(e)}")
        st.stop()
    except WorksheetNotFound:
        st.warning("⚠️ '마스터' 시트를 찾을 수 없습니다.")
        st.error("필수 시트를 찾을 수 없습니다. '마스터' 시트가 있는지 확인해주세요.")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"마스터 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

def initialize_page_data(gc, url, name, week_labels):
    """페이지에 필요한 데이터를 한 번에 로드하고, 필요 시 초기화 및 업데이트합니다."""
    try:
        # --- 데이터 로딩 ---
        sheet = gc.open_by_url(url)
        month_str = (datetime.date.today().replace(day=1) + relativedelta(months=1)).strftime("%Y년 %-m월")
        
        df_master = pd.DataFrame(sheet.worksheet("마스터").get_all_records())
        try:
            df_request = pd.DataFrame(sheet.worksheet(f"{month_str} 요청").get_all_records())
        except WorksheetNotFound:
            df_request = pd.DataFrame()
        try:
            df_room_request = pd.DataFrame(sheet.worksheet(f"{month_str} 방배정 요청").get_all_records())
        except WorksheetNotFound:
            df_room_request = pd.DataFrame()

        # --- 기존 로직 (신규 유저, 매주 데이터 통합) ---
        df_user_master = df_master[df_master["이름"] == name].copy()
        sheet_needs_update = False

        # 경우 1: 신규 유저일 때
        if df_user_master.empty:
            st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
            initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
            initial_df = pd.DataFrame(initial_rows)
            df_master = pd.concat([df_master, initial_df], ignore_index=True)
            sheet_needs_update = True

        # 경우 2: '매주'로 데이터를 통합할 수 있을 때
        has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
        if not df_user_master.empty and not has_weekly:
            pivot_df = df_user_master.pivot(index="요일", columns="주차", values="근무여부")
            if set(pivot_df.columns) == set(week_labels) and pivot_df.apply(lambda x: x.nunique() == 1, axis=1).all():
                temp_user_df = df_user_master.drop_duplicates(subset=["이름", "요일"]).copy()
                temp_user_df["주차"] = "매주"
                df_master = df_master[df_master["이름"] != name]
                df_master = pd.concat([df_master, temp_user_df], ignore_index=True)
                sheet_needs_update = True

        # 위 두 경우 중 하나라도 해당되면 시트에 단 한 번만 업데이트합니다.
        if sheet_needs_update:
            df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_master = df_master.sort_values(by=["이름", "주차", "요일"])
            worksheet1 = sheet.worksheet("마스터")
            if not update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist()):
                st.error("마스터 시트 초기 데이터 업데이트 실패")
                st.stop()
        
        # --- 최종 데이터를 세션 상태에 저장합니다. ---
        st.session_state["df_master"] = df_master
        st.session_state["df_request"] = df_request
        st.session_state["df_room_request"] = df_room_request
        st.session_state["df_user_master"] = df_master[df_master["이름"] == name].copy()
        st.session_state["master_page_initialized"] = True

    except (APIError, Exception) as e:
        st.error(f"데이터 초기화 중 오류가 발생했습니다: {e}")
        st.stop()

# 데이터 로드 함수
@st.cache_data(show_spinner=False)
def load_saturday_schedule(_gc, url, year):
    """지정된 연도의 토요/휴일 스케줄 데이터를 로드하는 함수"""
    try:
        sheet = _gc.open_by_url(url)
        worksheet_name = f"{year}년 토요/휴일 스케줄"
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            st.warning(f"⚠️ '{worksheet_name}' 시트에 데이터가 없습니다.")
            return pd.DataFrame(columns=["날짜", "근무", "당직"])
        
        df = pd.DataFrame(data)
        # '날짜' 열이 비어있거나 잘못된 형식의 데이터를 제외하고 datetime으로 변환
        df = df[df['날짜'] != '']
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True) # 날짜 변환 실패한 행 제거
        return df
    except WorksheetNotFound:
        st.info(f"'{year}년 토요/휴일 스케줄' 시트를 찾을 수 없습니다. 토요일 근무가 표시되지 않을 수 있습니다.")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])
    except APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (토요/휴일 스케줄): {str(e)}")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"토요/휴일 스케줄 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])

def generate_master_events(df_user_master, year, month, week_labels, closing_dates_set):
    """마스터 스케줄(평일)에서 이벤트를 생성하는 함수 (휴관일 제외)"""
    # ... (함수 앞부분의 master_data 생성 로직은 기존과 동일) ...
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금"]
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    for week in week_labels:
        master_data[week] = {}
        week_df = df_user_master[df_user_master["주차"] == week]
        for day in 요일리스트:
            day_specific = week_df[week_df["요일"] == day]
            if not day_specific.empty:
                master_data[week][day] = day_specific.iloc[0]["근무여부"]
            elif not every_week_df.empty:
                day_every = every_week_df[every_week_df["요일"] == day]
                master_data[week][day] = day_every.iloc[0]["근무여부"] if not day_every.empty else "근무없음"
            else:
                master_data[week][day] = "근무없음"
    
    events = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
    _, last_day = calendar.monthrange(year, month)
    status_colors = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}
    first_sunday = next((day for day in range(1, 8) if datetime.date(year, month, day).weekday() == 6), None)
    
    for day_num in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day_num)
        
        # ▼▼▼ [수정된 부분] 휴관일인 경우 마스터 일정을 표시하지 않고 건너뜁니다. ▼▼▼
        if date_obj in closing_dates_set:
            continue
        # ▲▲▲ [수정된 부분] ▲▲▲

        if date_obj.weekday() in weekday_map:
            day_name = weekday_map[date_obj.weekday()]
            if first_sunday is None: week_num = (date_obj.day + datetime.date(year, month, 1).weekday()) // 7
            else: week_num = (day_num - first_sunday) // 7 + 1 if day_num >= first_sunday else 0
            if week_num >= len(week_labels): continue
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            if status and status != "근무없음":
                events.append({"title": f"{status}", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get(status, "#E0E0E0")})
    return events

def generate_closing_day_events(df_closing_days):
    """휴관일 DataFrame에서 이벤트를 생성하는 함수"""
    events = []
    if not df_closing_days.empty:
        for date_obj in df_closing_days['날짜']:
            events.append({
                "title": "휴관일", 
                "start": date_obj.strftime("%Y-%m-%d"), 
                "color": "#DC143C"  # 붉은색 계열 (Crimson)
            })
    return events
def generate_saturday_events(df_saturday_schedule, current_user_name, year, month):
    """토요/휴일 스케줄에서 이벤트를 생성하는 함수"""
    events = []
    status_colors = {"토요근무": "#6A5ACD", "당직": "#FF6347"}
    if not df_saturday_schedule.empty:
        month_schedule = df_saturday_schedule[(df_saturday_schedule['날짜'].dt.year == year) & (df_saturday_schedule['날짜'].dt.month == month)]
        for _, row in month_schedule.iterrows():
            date_obj = row['날짜'].date()
            if isinstance(row.get('근무', ''), str) and current_user_name in row.get('근무', ''):
                events.append({"title": "토요근무", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get("토요근무")})
            if isinstance(row.get('당직', ''), str) and current_user_name == row.get('당직', '').strip():
                events.append({"title": "당직", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get("당직")})
    return events

def generate_request_events(df_user_request):
    """일반 요청사항에서 이벤트를 생성하는 함수"""
    events = []
    if df_user_request.empty: return events
    status_colors = {"휴가": "#A1C1D3", "학회": "#B4ABE4", "보충 어려움(오전)": "#FFD3B5", "보충 불가(오전)": "#FFB6C1", "꼭 근무(오전)": "#C3E6CB"}
    label_map = {"휴가": "휴가🎉", "학회": "학회📚", "보충 어려움(오전)": "보충 어려움(오전)", "보충 불가(오전)": "보충 불가(오전)", "꼭 근무(오전)": "꼭근무(오전)"}
    for _, row in df_user_request.iterrows():
        분류, 날짜정보 = row["분류"], row["날짜정보"]
        if not 날짜정보 or 분류 == "요청 없음": continue
        # (오후)가 포함된 분류 처리
        base_분류 = 분류.replace("(오후)", "(오전)").replace("(오후)", "").strip()
        title = label_map.get(분류, 분류)
        color = status_colors.get(base_분류, "#E0E0E0")
        if "~" in 날짜정보:
            시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
            시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
            종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
            events.append({"title": title, "start": 시작.strftime("%Y-%m-%d"), "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "color": color})
        else:
            for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                try:
                    dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                    events.append({"title": title, "start": dt.strftime("%Y-%m-%d"), "color": color})
                except: continue
    return events

def generate_room_request_events(df_user_room_request):
    """방배정 요청사항에서 이벤트를 생성하는 함수"""
    events = []
    if df_user_room_request.empty: return events
    for _, row in df_user_room_request.iterrows():
        분류, 날짜정보 = row["분류"], row["날짜정보"]
        if not 날짜정보 or pd.isna(날짜정보): continue
        for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
            try:
                date_part = 날짜.split(" (")[0]
                dt = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
                events.append({"title": f"{분류}", "start": dt.strftime("%Y-%m-%d"), "color": "#7C8EC7"})
            except: continue
    return events

# 캘린더 이벤트 생성 함수
def generate_calendar_events(df_user_master, df_saturday_schedule, current_user_name, year, month, week_labels):
    # --- 1. 평일 스케줄 데이터 가공 (기존 로직) ---
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금"]
    
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    
    for week in week_labels:
        master_data[week] = {}
        week_df = df_user_master[df_user_master["주차"] == week]
        for day in 요일리스트:
            day_specific = week_df[week_df["요일"] == day]
            if not day_specific.empty:
                master_data[week][day] = day_specific.iloc[0]["근무여부"]
            elif not every_week_df.empty:
                day_every = every_week_df[every_week_df["요일"] == day]
                master_data[week][day] = day_every.iloc[0]["근무여부"] if not day_every.empty else "근무없음"
            else:
                master_data[week][day] = "근무없음"

    # --- 2. 캘린더 이벤트 생성 ---
    events = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토"}
    _, last_day = calendar.monthrange(year, month)
    status_colors = {
        "오전": "#48A6A7", 
        "오후": "#FCB454", 
        "오전 & 오후": "#F38C79",
        "토요근무": "#6A5ACD",  # 토요근무 색상
        "당직": "#FF6347"    # 당직 색상
    }

    first_sunday = next((day for day in range(1, last_day + 1) if datetime.date(year, month, day).weekday() == 6), None)
    
    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()

        # 평일(월~금) 처리
        if weekday <= 4:
            day_name = weekday_map[weekday]
            week_num = 0 if first_sunday and day < first_sunday else (day - first_sunday) // 7 + 1 if first_sunday else (day - 1) // 7
            if week_num >= len(week_labels):
                continue
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            if status != "근무없음":
                events.append({
                    "title": f"{status}",
                    "start": date_obj.strftime("%Y-%m-%d"),
                    "end": date_obj.strftime("%Y-%m-%d"),
                    "color": status_colors.get(status, "#E0E0E0")
                })
        
        # 토요일 처리
        elif weekday == 5:
            saturday_row = df_saturday_schedule[df_saturday_schedule['날짜'].dt.date == date_obj]
            if not saturday_row.empty:
                # '근무' 인원 목록에 현재 사용자가 있는지 확인
                work_staff = saturday_row.iloc[0].get('근무', '')
                if isinstance(work_staff, str) and current_user_name in work_staff:
                    events.append({
                        "title": "토요근무",
                        "start": date_obj.strftime("%Y-%m-%d"),
                        "end": date_obj.strftime("%Y-%m-%d"),
                        "color": status_colors.get("토요근무")
                    })
                
                # '당직' 인원에 현재 사용자가 있는지 확인
                on_call_staff = saturday_row.iloc[0].get('당직', '')
                if isinstance(on_call_staff, str) and current_user_name == on_call_staff.strip():
                     events.append({
                        "title": "당직",
                        "start": date_obj.strftime("%Y-%m-%d"),
                        "end": date_obj.strftime("%Y-%m-%d"),
                        "color": status_colors.get("당직")
                    })
    return events

@st.cache_data(show_spinner=False)
def load_closing_days(_gc, url, year):
    """지정된 연도의 휴관일 데이터를 로드하는 함수"""
    try:
        sheet = _gc.open_by_url(url)
        worksheet_name = f"{year}년 휴관일"
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame(columns=["날짜"])
        
        df = pd.DataFrame(data)
        df = df[df['날짜'] != '']
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True)
        return df
    except WorksheetNotFound:
        st.info(f"'{worksheet_name}' 시트를 찾을 수 없습니다. 휴관일이 표시되지 않습니다.")
        return pd.DataFrame(columns=["날짜"])
    except Exception as e:
        st.error(f"휴관일 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame(columns=["날짜"])

# 기본 변수 설정
url = st.secrets["google_sheet"]["url"]
gc = get_gspread_client()
name = st.session_state.get("name")
if name is None:
    st.error("⚠️ 사용자 이름이 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
    st.stop()

# 월 정보 및 주차 리스트 (초기화 함수에 필요하므로 먼저 정의)
from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.datetime.now(kst)
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
year, month = next_month_date.year, next_month_date.month # <-- 이 줄 추가
_, last_day = calendar.monthrange(year, month) # <-- 이 줄 추가
dates = pd.date_range(start=next_month_date.replace(day=1), end=next_month_date.replace(day=last_day))
week_nums = sorted(set(d.isocalendar()[1] for d in dates))
week_labels = [f"{i+1}주" for i in range(len(week_nums))]

# 페이지 최초 로드 시에만 데이터 초기화 함수를 실행합니다.
if "master_page_initialized" not in st.session_state:
    initialize_page_data(gc, url, name, week_labels)

# 세션 상태에서 최종 데이터를 가져옵니다.
df_master = st.session_state["df_master"]
df_user_master = st.session_state["df_user_master"]

# 월 정보 및 주차 리스트
근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
요일리스트 = ["월", "화", "수", "목", "금"]
today = now.date()
next_month_date = today.replace(day=1) + relativedelta(months=1)
year, month = next_month_date.year, next_month_date.month
_, last_day = calendar.monthrange(year, month)
month_str = next_month_date.strftime("%Y년 %-m월")
dates = pd.date_range(start=next_month_date.replace(day=1), end=next_month_date.replace(day=last_day))
week_nums = sorted(set(d.isocalendar()[1] for d in dates))
week_labels = [f"{i+1}주" for i in range(len(week_nums))]
has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False

# --- 모든 종류의 데이터 로드 ---
df_saturday = load_saturday_schedule(gc, url, year)
df_closing_days = load_closing_days(gc, url, year) # <-- [추가] 휴관일 데이터 로드
df_request = st.session_state.get("df_request", pd.DataFrame())
df_room_request = st.session_state.get("df_room_request", pd.DataFrame())

# 현재 사용자에 해당하는 데이터 필터링
df_user_request = df_request[df_request["이름"] == name].copy() if not df_request.empty else pd.DataFrame()
df_user_room_request = df_room_request[df_room_request["이름"] == name].copy() if not df_room_request.empty else pd.DataFrame()

# [추가] 빠른 조회를 위해 휴관일 날짜 세트 생성
closing_dates_set = set(df_closing_days['날짜'].dt.date) if not df_closing_days.empty else set()

# --- 각 종류별 이벤트 생성 ---
# [수정] generate_master_events에 closing_dates_set 전달
master_events = generate_master_events(df_user_master, year, month, week_labels, closing_dates_set)
saturday_events = generate_saturday_events(df_saturday, name, year, month)
request_events = generate_request_events(df_user_request)
room_request_events = generate_room_request_events(df_user_room_request)
closing_day_events = generate_closing_day_events(df_closing_days) # <-- [추가] 휴관일 이벤트 생성

# --- 모든 이벤트를 하나로 합치기 ---
# [수정] closing_day_events를 합산 목록에 추가
events = master_events + saturday_events + request_events + room_request_events + closing_day_events

calendar_options = {
    "initialView": "dayGridMonth",
    "initialDate": next_month_date.strftime("%Y-%m-%d"),
    "editable": False,
    "selectable": False,
    "eventDisplay": "block",
    "dayHeaderFormat": {"weekday": "short"},
    "themeSystem": "bootstrap",
    "height": 600,
    "headerToolbar": {
        "left": "",
        "center": "title",  # 'title'을 추가
        "right": ""
    },
    "showNonCurrentDates": True,
    "fixedWeekCount": False
}

st.header(f"📅 {name} 님의 마스터 스케줄", divider='rainbow')

# st.error("📅 [마스터 수정] 기능은 반드시 강승주 팀장님의 확인 후에 수정해 주시기 바랍니다.")

# 새로고침 버튼
if st.button("🔄 새로고침 (R)"):
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()
            st.session_state["df_master"] = load_master_data_page1(gc, url)
            st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
        st.success("데이터가 새로고침되었습니다.")
        time.sleep(1)
        st.rerun()
    except APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()

# st.html 부터 시작하는 부분을 아래 코드로 전부 교체하세요.
st.html("""
<style>
    /* --- 1. 공통 스타일 --- */
    .calendar-title { text-align: center; font-size: 24px; font-weight: bold; margin-bottom: 20px; }
    .schedule-container {
        background-color: var(--secondary-background-color);
        color: var(--text-color);
        border: 1px solid rgba(128, 128, 128, 0.4);
        padding: 10px;
        border-radius: 10px;
        margin-bottom: 15px;
    }

    /* --- 2. HTML 캘린더 스타일 (안정 버전) --- */
    .calendar-table-container {
        overflow: hidden;
    }
    .html-calendar {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
    }
    .html-calendar th, .html-calendar td {
        border: 1px solid rgba(128, 128, 120, 0.6);
        vertical-align: top;
        padding: 0;
        transition: background-color 0.2s ease-in-out;
    }
    .html-calendar th {
        font-weight: bold;
        text-align: center;
        padding: 10px 0;
        background-color: var(--secondary-background-color);
        color: var(--text-color);
        border-bottom: 2px solid rgba(128, 128, 120, 0.6);
    }
    .day-cell-content-wrapper {
        min-height: 120px;
        padding: 6px;
    }
    .html-calendar .day-number {
        font-weight: bold; font-size: 14px; margin-bottom: 5px;
        display: flex; align-items: center; justify-content: center;
        width: 1.8em; height: 1.8em;
    }
    .html-calendar .other-month { opacity: 0.5; }
    .html-calendar .saturday { color: #4169E1 !important; }
    .html-calendar .sunday { color: #DC143C !important; }
    .event-item {
        font-size: 13px; padding: 1px 5px; border-radius: 3px;
        margin-bottom: 3px; color: white; overflow: hidden;
        text-overflow: ellipsis; white-space: nowrap;
    }
    .html-calendar .today-cell .day-number {
        background-color: #007bff;
        color: white;
        border-radius: 50%;
    }
    .html-calendar td:hover {
        background-color: var(--secondary-background-color);
    }

    /* --- 3. 모바일 화면 대응 (안정 버전) --- */
    @media (max-width: 768px) {
        .calendar-table-container {
            overflow-x: auto; /* 테이블이 넘칠 경우 가로 스크롤 생성 */
        }
        .html-calendar {
            min-width: 600px; /* 테이블의 최소 너비를 지정해 스크롤 유도 */
        }
        .day-cell-content-wrapper { min-height: 90px; }
        .day-number, .html-calendar th { font-size: 11px !important; }
        .event-item {
            font-size: 11px !important; padding: 1px !important;
            white-space: normal !important; word-break: break-all !important;
            line-height: 1.1 !important;
        }
    }
</style>
""")

if df_user_request.empty:
    with st.container(border=True):
        st.write(f"🔔 {month_str}에 등록하신 '요청사항'이 없습니다.")
st.write(" ")

if st.session_state.get("df_user_room_request", pd.DataFrame()).empty:
    with st.container(border=True):
        st.write(f"🔔 {month_str}에 등록하신 '방배정 요청'이 없습니다.")
    st.write("")

# 2. 캘린더 UI 렌더링 (HTML Table 방식으로 완전 교체)

st.markdown(f'<div class="calendar-title">{month_str} 마스터 스케줄</div>', unsafe_allow_html=True)

events_by_date = {}
for event in events:
    start_date = datetime.datetime.strptime(event['start'], "%Y-%m-%d").date()
    if 'end' in event and event['start'] != event['end']:
        end_date = datetime.datetime.strptime(event['end'], "%Y-%m-%d").date()
        for i in range((end_date - start_date).days):
            current_date = start_date + datetime.timedelta(days=i)
            if current_date not in events_by_date: events_by_date[current_date] = []
            events_by_date[current_date].append(event)
    else:
        if start_date not in events_by_date: events_by_date[start_date] = []
        events_by_date[start_date].append(event)

cal = calendar.Calendar(firstweekday=6)
month_days = cal.monthdatescalendar(year, month)
days_of_week = ["일", "월", "화", "수", "목", "금", "토"] 

from zoneinfo import ZoneInfo
kst = ZoneInfo("Asia/Seoul")
now = datetime.datetime.now(kst)
today = now.date()

html_string = "<div class='calendar-table-container'>"
html_string += "<table class='html-calendar'>"
html_string += "<thead><tr>"
for day in days_of_week:
    day_class = ""
    if day == "토": day_class = "saturday"
    elif day == "일": day_class = "sunday"
    html_string += f"<th class='{day_class}'>{day}</th>"
html_string += "</tr></thead>"

html_string += "<tbody>"
for week in month_days:
    html_string += "<tr>"
    for day_date in week:
        event_html = ""
        if day_date in events_by_date:
            sorted_events = sorted(events_by_date[day_date], key=lambda x: x.get('source', 'z'))
            for event in sorted_events:
                color = event.get('color', '#6c757d')
                title = event['title']
                event_html += f"<div class='event-item' style='background-color:{color};' title='{title}'>{title}</div>"
        
        cell_class = ""
        if day_date.month != month: cell_class += " other-month"
        if day_date.weekday() == 6: cell_class += " sunday"
        if day_date.weekday() == 5: cell_class += " saturday"
        if day_date == today: cell_class += " today-cell"

        html_string += f"<td class='{cell_class}'>"
        html_string += "<div class='day-cell-content-wrapper'>"
        html_string += f"<div class='day-number'>{day_date.day}</div>"
        html_string += f"<div class='events-container'>{event_html}</div>"
        html_string += "</div></td>"
    html_string += "</tr>"
html_string += "</tbody></table></div>"

st.markdown(html_string, unsafe_allow_html=True)

# 이번 달 토요/휴일 스케줄 필터링 및 스타일 적용하여 출력
st.write("") # 캘린더와 간격을 주기 위해 빈 줄 추가
current_month_schedule_df = df_saturday[
    (df_saturday['날짜'].dt.year == year) & 
    (df_saturday['날짜'].dt.month == month)
].sort_values(by='날짜')

if not current_month_schedule_df.empty:
    # 요일 한글 변환 맵
    weekday_map_ko = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    
    # 날짜를 "월 일(요일)" 형식의 리스트로 변환
    schedule_list = [
        date.strftime(f"%-m월 %-d일({weekday_map_ko[date.weekday()]})") 
        for date in current_month_schedule_df['날짜']
    ]
    
    # 최종 문자열 생성
    schedule_str = ", ".join(schedule_list)
    
    styled_text = f"""
    <div class="schedule-container">
        📅 <strong>이번 달 토요/휴일 스케줄:</strong> {schedule_str}
    </div>
    """
    st.markdown(styled_text, unsafe_allow_html=True)

else:
    # 스케줄이 없을 경우에도 동일한 스타일 적용
    styled_text = """
    <div class="schedule-container">
        📅 이번 달에는 예정된 토요/휴일 근무가 없습니다.
    </div>
    """
    st.markdown(styled_text, unsafe_allow_html=True)