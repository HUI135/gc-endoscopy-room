from googleapiclient.discovery import build
import time
import numpy as np
import streamlit as st
import pandas as pd
import calendar
import datetime
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import menu
import re

# 페이지 설정
st.set_page_config(page_title="방배정 요청 입력", page_icon="🏠", layout="wide")

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

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 클라이언트 초기화 중 오류 발생: {str(e)}")
        st.stop()

# 데이터 로드 함수 (st.cache_data 적용)
def load_master_data_page3(sheet):
    try:
        # sheet = _gc.open_by_url(url)
        worksheet_master = sheet.worksheet("마스터")
        return pd.DataFrame(worksheet_master.get_all_records())
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (마스터 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"마스터 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

def load_request_data_page3(sheet, sheet_name):
    try:
        # sheet = _gc.open_by_url(url)
        try:
            worksheet = sheet.worksheet(sheet_name)
        except WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            worksheet.append_row(["이름", "분류", "날짜정보"])
        data = worksheet.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"요청 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

def load_room_request_data_page3(sheet, sheet_name):
    try:
        # sheet = _gc.open_by_url(url)
        try:
            worksheet = sheet.worksheet(sheet_name)
        except WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            worksheet.append_row(["이름", "분류", "날짜정보"])
        data = worksheet.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (방배정 요청 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"방배정 요청 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

def generate_saturday_events(df_saturday_schedule, current_user_name, year, month):
    """토요/휴일 스케줄에서 현재 사용자의 이벤트를 생성하는 함수"""
    events = []
    status_colors = {"토요근무": "#6A5ACD", "당직": "#FF6347"}

    if not df_saturday_schedule.empty:
        # 해당 월의 데이터만 필터링
        month_schedule = df_saturday_schedule[
            (df_saturday_schedule['날짜'].dt.year == year) &
            (df_saturday_schedule['날짜'].dt.month == month)
        ]
        
        for _, row in month_schedule.iterrows():
            date_obj = row['날짜'].date()
            # 근무자 확인
            work_staff = row.get('근무', '')
            if isinstance(work_staff, str) and current_user_name in work_staff:
                events.append({"title": "토요근무", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get("토요근무"), "source": "saturday"})
            # 당직자 확인
            on_call_staff = row.get('당직', '')
            if isinstance(on_call_staff, str) and current_user_name == on_call_staff.strip():
                events.append({"title": "당직", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get("당직"), "source": "saturday"})
    return events

def generate_master_events(df_user_master, year, month, week_labels, closing_dates_set):
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

    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        
        # ▼▼▼ [수정됨] 휴관일인 경우 마스터 일정을 표시하지 않고 건너뜁니다. ▼▼▼
        if date_obj in closing_dates_set:
            continue
        
        weekday = date_obj.weekday()
        if weekday in weekday_map:
            day_name = weekday_map[weekday]
            if first_sunday is None: week_num = (date_obj.day + datetime.date(year, month, 1).weekday()) // 7
            else: week_num = (day - first_sunday) // 7 + 1 if day >= first_sunday else 0
            if week_num >= len(week_labels): continue
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            
            if status and status != "근무없음":
                events.append({"title": f"{status}", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors.get(status, "#E0E0E0"), "source": "master"})
    return events

def generate_request_events(df_user_request, today):
    status_colors_request = {
        "휴가": "#A1C1D3", "학회": "#B4ABE4", "보충 어려움(오전)": "#FFD3B5", "보충 어려움(오후)": "#FFD3B5",
        "보충 불가(오전)": "#FFB6C1", "보충 불가(오후)": "#FFB6C1", "꼭 근무(오전)": "#C3E6CB",
        "꼭 근무(오후)": "#C3E6CB",
    }
    label_map = {
        "휴가": "휴가🎉", "학회": "학회📚", "보충 어려움(오전)": "보충 어려움(오전)", "보충 어려움(오후)": "보충 어려움(오후)",
        "보충 불가(오전)": "보충 불가(오전)", "보충 불가(오후)": "보충 불가(오후)", "꼭 근무(오전)": "꼭근무(오전)",
        "꼭 근무(오후)": "꼭근무(오후)"
    }
    
    events = []
    for _, row in df_user_request.iterrows():
        분류 = row["분류"]
        날짜정보 = row["날짜정보"]
        if not 날짜정보:
            continue
        if "~" in 날짜정보:
            시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
            시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
            종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
            events.append({"title": label_map.get(분류, 분류), "start": 시작.strftime("%Y-%m-%d"),
                           "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                           "color": status_colors_request.get(분류, "#E0E0E0"), "source": "request"})
        else:
            for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                try:
                    dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                    events.append({"title": label_map.get(분류, 분류), "start": dt.strftime("%Y-%m-%d"),
                                   "end": dt.strftime("%Y-%m-%d"),
                                   "color": status_colors_request.get(분류, "#E0E0E0"), "source": "request"})
                except:
                    continue
    return events

def load_saturday_schedule(sheet, year):
    """지정된 연도의 토요/휴일 스케줄 데이터를 로드하는 함수"""
    try:
        worksheet_name = f"{year}년 토요/휴일 스케줄"
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            st.warning(f"⚠️ '{worksheet_name}' 시트에 데이터가 없습니다.")
            return pd.DataFrame(columns=["날짜", "근무", "당직"])
        
        df = pd.DataFrame(data)
        df = df[df['날짜'] != '']
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True)
        return df
    except WorksheetNotFound:
        st.info(f"'{worksheet_name}' 시트를 찾을 수 없습니다. 토요일 근무가 표시되지 않을 수 있습니다.")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])
    except Exception as e:
        st.error(f"토요/휴일 스케줄 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])

def load_closing_days(sheet, year):
    """지정된 연도의 휴관일 데이터를 로드하는 함수"""
    try:
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

def generate_room_request_events(df_user_room_request, today):
    label_map = {
        "1번방": "1번방", "2번방": "2번방", "3번방": "3번방", "4번방": "4번방", "5번방": "5번방",
        "6번방": "6번방", "7번방": "7번방", "8번방": "8번방", "9번방": "9번방", "10번방": "10번방",
        "11번방": "11번방", "당직 안됨": "당직🚫", "오전 당직 안됨": "오전당직🚫", "오후 당직 안됨": "오후당직🚫",
        "당직 아닌 이른방": "당직아닌이른방", "이른방 제외": "이른방 제외", "늦은방 제외": "늦은방 제외",
        "8:30": "8:30", "9:00": "9:00", "9:30": "9:30", "10:00": "10:00", "오전 당직": "오전당직",
        "오후 당직": "오후당직",
    }
    events = []
    for _, row in df_user_room_request.iterrows():
        분류 = row["분류"]
        날짜정보 = row["날짜정보"]
        if not 날짜정보 or pd.isna(날짜정보):
            continue
        for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
            try:
                date_part = 날짜.split(" (")[0]
                dt = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
                events.append({"title": label_map.get(분류, 분류), "start": dt.strftime("%Y-%m-%d"),
                               "end": dt.strftime("%Y-%m-%d"), "color": "#7C8EC7",
                               "source": "room_request", "allDay": True})
            except Exception as e:
                continue
    return events

def initialize_and_sync_data(gc, url, name, month_start, month_end):
    """페이지에 필요한 모든 데이터를 로드하고, 동기화하며, 세션 상태에 저장합니다."""
    try:
        sheet = gc.open_by_url(url)
        st.session_state["sheet"] = sheet

        # 1. 데이터 로드
        df_master = load_master_data_page3(sheet)
        df_request = load_request_data_page3(sheet, f"{month_str} 요청")
        df_room_request = load_room_request_data_page3(sheet, f"{month_str} 방배정 요청")
        df_saturday_schedule = load_saturday_schedule(sheet, year)
        df_closing_days = load_closing_days(sheet, year) # <-- [추가] 휴관일 데이터 로드

        # 2. 신규 유저 마스터 데이터 동기화
        if not df_master.empty and name not in df_master["이름"].values:
            st.info(f"{name} 님의 마스터 데이터가 없어 새로 추가합니다.")
            initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
            initial_df = pd.DataFrame(initial_rows)
            df_master = pd.concat([df_master, initial_df], ignore_index=True).sort_values(by=["이름", "주차", "요일"])
            
            worksheet1 = sheet.worksheet("마스터")
            worksheet1.clear()
            worksheet1.update([df_master.columns.tolist()] + df_master.values.tolist())

        # 3. '매주' 데이터 동기화
        df_user_master_temp = df_master[df_master["이름"] == name]
        has_weekly = "매주" in df_user_master_temp["주차"].values if not df_user_master_temp.empty else False
        if not df_user_master_temp.empty and not has_weekly:
            week_nums_count = len(sorted(set(d.isocalendar()[1] for d in pd.date_range(start=month_start, end=month_end))))
            week_labels = [f"{i+1}주" for i in range(week_nums_count)]
            try:
                pivot_df = df_user_master_temp.pivot(index="요일", columns="주차", values="근무여부")
                if set(pivot_df.columns) == set(week_labels) and pivot_df.apply(lambda x: x.nunique() == 1, axis=1).all():
                    temp_user_df = df_user_master_temp.drop_duplicates(subset=["이름", "요일"]).copy()
                    temp_user_df["주차"] = "매주"
                    df_master = df_master[df_master["이름"] != name]
                    df_master = pd.concat([df_master, temp_user_df], ignore_index=True).sort_values(by=["이름", "주차", "요일"])

                    worksheet1 = sheet.worksheet("마스터")
                    worksheet1.clear()
                    worksheet1.update([df_master.columns.tolist()] + df_master.values.tolist())
            except KeyError:
                pass
        
        # 4. 최종 데이터를 세션 상태에 저장
        st.session_state["df_master"] = df_master
        st.session_state["df_request"] = df_request
        st.session_state["df_room_request"] = df_room_request
        st.session_state["df_saturday_schedule"] = df_saturday_schedule
        st.session_state["df_closing_days"] = df_closing_days # <-- [추가] 휴관일 데이터 세션에 저장

    except (gspread.exceptions.APIError, Exception) as e:
        st.error(f"데이터 초기화 및 동기화 중 오류가 발생했습니다: {e}")
        st.stop()

# 전역 변수 설정
try:
    gc = get_gspread_client()
    url = st.secrets["google_sheet"]["url"]
    if "name" not in st.session_state:
        st.error("⚠️ 사용자 이름이 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
        st.stop()
    name = st.session_state["name"]

    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    now = datetime.datetime.now(kst)
    today = now.date()
    next_month_date = today.replace(day=1) + relativedelta(months=1)

    month_str = next_month_date.strftime("%Y년 %-m월")
    month_start = next_month_date
    year, month = next_month_date.year, next_month_date.month
    _, last_day = calendar.monthrange(year, month)
    month_end = next_month_date.replace(day=last_day)
    week_nums = sorted(set(d.isocalendar()[1] for d in pd.date_range(start=month_start, end=month_end)))
    week_labels = [f"{i+1}주" for i in range(len(week_nums))]
except NameError as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()
except Exception as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()

# 페이지 로드 시 단 한 번만 데이터 로드
if "initial_load_done" not in st.session_state:
    try:
        with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
            initialize_and_sync_data(gc, url, name, month_start, month_end)
            st.session_state["initial_load_done"] = True
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (초기 데이터 로드): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

df_master = st.session_state.get("df_master", pd.DataFrame())
df_request = st.session_state.get("df_request", pd.DataFrame())
df_room_request = st.session_state.get("df_room_request", pd.DataFrame())
name = st.session_state.get("name")

# 각 데이터프레임에 '이름' 컬럼이 있는지 확인 후 필터링
if not df_master.empty and "이름" in df_master.columns:
    st.session_state["df_user_master"] = df_master[df_master["이름"] == name].copy()
else:
    st.session_state["df_user_master"] = pd.DataFrame()

if not df_request.empty and "이름" in df_request.columns:
    st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy()
else:
    st.session_state["df_user_request"] = pd.DataFrame()

if not df_room_request.empty and "이름" in df_room_request.columns:
    st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy()
else:
    st.session_state["df_user_room_request"] = pd.DataFrame()

# UI 렌더링 시작
df_saturday = st.session_state.get("df_saturday_schedule", pd.DataFrame())
df_closing_days = st.session_state.get("df_closing_days", pd.DataFrame()) # <-- [추가] 세션에서 휴관일 데이터 가져오기

# 빠른 조회를 위해 휴관일 날짜 세트 생성
closing_dates_set = set(df_closing_days['날짜'].dt.date) if not df_closing_days.empty else set()

# generate_master_events에 closing_dates_set 전달
master_events = generate_master_events(st.session_state["df_user_master"], year, month, week_labels, closing_dates_set)
request_events = generate_request_events(st.session_state["df_user_request"], next_month_date)
room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month_date)
saturday_events = generate_saturday_events(df_saturday, name, year, month)

# [추가] 휴관일 이벤트 생성
closing_day_events = []
if not df_closing_days.empty:
    for date_obj in df_closing_days['날짜']:
        closing_day_events.append({
            "title": "휴관일", 
            "start": date_obj.strftime("%Y-%m-%d"), 
            "color": "#DC143C", # 붉은색 계열
            "source": "closing_day"
        })

# 모든 이벤트를 하나로 합치기
all_events = master_events + room_request_events + saturday_events + closing_day_events

st.header(f"📅 {name} 님의 {month_str} 방배정 요청", divider='rainbow')

# 새로고침 버튼 로직
if st.button("🔄 새로고침 (R)"):
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()
            # 아래 함수 호출 부분에 month_start, month_end 추가
            initialize_and_sync_data(gc, url, name, month_start, month_end)
        st.success("데이터가 새로고침되었습니다.")
        st.rerun()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()

if not all_events:
    st.info("☑️ 당월에 입력하신 요청사항 또는 마스터 스케줄이 없습니다.")
    calendar_options = {"initialView": "dayGridMonth", "initialDate": month_start.strftime("%Y-%m-%d"), "height": 700, "headerToolbar": {"left": "", "center": "title", "right": ""}}

# st.html 부터 시작하는 부분을 교체하세요.
st.html("""
<style>
    /* CSS Version: Original Structure - Forceful PC Dark Mode Fix */

    /* --- 1. 기본 스타일 (라이트 모드) --- */
    .calendar-title {
        text-align: center; font-size: 24px; font-weight: bold;
        margin-bottom: 20px; color: #495057;
    }
    .schedule-container {
        background-color: #f8f9fa !important;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 15px;
        border: 1px solid #e1e4e8;
        color: black;
    }
    .calendar-header {
        text-align: center; font-weight: bold; padding: 10px 0;
        border: 1px solid #e1e4e8; border-radius: 5px;
        background-color: #e9ecef; color: black;
    }
    .saturday { color: #4169E1 !important; }
    .sunday { color: #DC143C !important; }
    .calendar-day-cell {
        border: 1px solid #e1e4e8; border-radius: 5px; padding: 6px;
        min-height: 120px; background-color: #f8f9fa;
        display: flex; flex-direction: column;
    }
    .day-number {
        font-weight: bold; font-size: 14px; margin-bottom: 5px; color: black;
    }
    .day-number.other-month { color: #ccc; }
    .event-item {
        font-size: 13px; padding: 1px 5px; border-radius: 3px;
        margin-bottom: 3px; color: white; overflow: hidden;
        text-overflow: ellipsis; white-space: nowrap;
    }

    /* --- 3. 모바일 화면 대응 (레이아웃 변경) --- */
    /* 이 부분은 원래대로 잘 작동했으므로 그대로 유지합니다. */
    @media (max-width: 768px) {
        div[data-testid="stHorizontalBlock"] {
            display: grid !important;
            grid-template-columns: repeat(7, minmax(80px, 1fr)) !important;
            gap: 0 !important; padding: 0 !important; margin: 0 !important;
            border-top: 1px solid #e0e0e0 !important;
            border-left: 1px solid #e0e0e0 !important;
        }
        .calendar-header {
            border: none !important;
            border-left: 1px solid #e0e0e0 !important;
            border-right: 1px solid #e0e0e0 !important;
            border-bottom: 1px solid #e0e0e0 !important;
            border-radius: 0 !important;
            background-color: #f8f9fa !important;
        }
        .calendar-day-cell { min-height: 75px !important; padding: 1px !important; }
        .event-item {
            font-size: 9px !important; padding: 1px !important;
            white-space: normal !important; word-break: break-all !important;
            line-height: 1.1 !important;
        }
        .day-number, .calendar-header { font-size: 11px !important; }
    }
</style>
""")

if st.session_state.get("df_user_room_request", pd.DataFrame()).empty:
    with st.container(border=True):
        st.write(f"🔔 {month_str}에 등록하신 '방배정 요청'이 없습니다.")
    st.write("")

# 2. 캘린더 UI 렌더링
# 제목 표시
st.markdown(f'<div class="calendar-title">{month_str} 방배정 요청</div>', unsafe_allow_html=True)

# 캘린더 격자 생성
with st.container():
    # 요일 헤더
    cols = st.columns(7, gap="small")
    days_of_week = ["일", "월", "화", "수", "목", "금", "토"]
    for col, day in zip(cols, days_of_week):
        header_class = "calendar-header"
        if day == "토":
            header_class += " saturday"
        elif day == "일":
            header_class += " sunday"
        col.markdown(f'<div class="{header_class}">{day}</div>', unsafe_allow_html=True)

    # 날짜 데이터 준비
    cal = calendar.Calendar(firstweekday=6) # 일요일 시작
    month_days = cal.monthdatescalendar(year, month)
    
    # 날짜별 이벤트 가공 (빠른 조회를 위해 딕셔너리로 변환)
    events_by_date = {}
    # ❗️ 기존 코드의 `all_events` 변수를 그대로 사용합니다.
    for event in all_events:
        start_date = datetime.datetime.strptime(event['start'], "%Y-%m-%d").date()
        if 'end' in event and event['start'] != event['end']:
            end_date = datetime.datetime.strptime(event['end'], "%Y-%m-%d").date()
            for i in range((end_date - start_date).days):
                current_date = start_date + datetime.timedelta(days=i)
                if current_date not in events_by_date:
                    events_by_date[current_date] = []
                events_by_date[current_date].append(event)
        else:
            if start_date not in events_by_date:
                events_by_date[start_date] = []
            events_by_date[start_date].append(event)

    # 날짜 셀 생성
    for week in month_days:
        cols = st.columns(7)
        for i, day_date in enumerate(week):
            is_other_month = "other-month" if day_date.month != month else ""
            
            with cols[i]:
                event_html = ""
                if day_date in events_by_date:
                    # 이벤트 정렬 (소스 우선)
                    sorted_events = sorted(events_by_date[day_date], key=lambda x: x.get('source', 'z'))
                    for event in sorted_events:
                        color = event.get('color', '#6c757d')
                        title = event['title']
                        event_html += f"<div class='event-item' style='background-color:{color};' title='{title}'>{title}</div>"

                # 각 날짜 칸(셀)을 HTML로 그림
                cell_html = f"""
                <div class="calendar-day-cell">
                    <div class="day-number {is_other_month}">{day_date.day}</div>
                    {event_html}
                </div>
                """
                st.markdown(cell_html, unsafe_allow_html=True)

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

st.divider()

# 근무 가능 일자와 시간대 계산
def get_user_available_dates(name, df_master, month_start, month_end):
    try:
        user_master = df_master[df_master['이름'] == name]
        available_dates = []
        요일_index = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
        weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
        
        for _, row in user_master.iterrows():
            주차 = row['주차']
            요일 = row['요일']
            근무여부 = row['근무여부']
            if 근무여부 == "근무없음":
                continue

            if 주차 == "매주":
                weeks = range(5)
            else:
                week_num = int(주차[0]) - 1
                weeks = [week_num]

            for day in pd.date_range(month_start, month_end):
                week_of_month = (day.day - 1) // 7
                if week_of_month in weeks and day.weekday() == 요일_index.get(요일):
                    weekday_name = weekday_map[day.weekday()]
                    month_num = day.month
                    day_num = day.day
                    display_date = f"{month_num}월 {day_num}일({weekday_name})"
                    save_date = day.strftime("%Y-%m-%d")
                    if 근무여부 == "오전 & 오후":
                        available_dates.append((f"{display_date} 오전", save_date, "오전"))
                        available_dates.append((f"{display_date} 오후", save_date, "오후"))
                    elif 근무여부 == "오전":
                        available_dates.append((f"{display_date} 오전", save_date, "오전"))
                    elif 근무여부 == "오후":
                        available_dates.append((f"{display_date} 오후", save_date, "오후"))

        available_dates = sorted(available_dates, key=lambda x: (datetime.datetime.strptime(x[1], "%Y-%m-%d"), x[2]))
        return available_dates
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"근무 가능 일자 계산 중 오류 발생: {str(e)}")
        st.stop()

# 날짜정보를 요청사항 삭제 UI 형식으로 변환
def format_date_for_display(date_info):
    try:
        formatted_dates = []
        weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
        for date in date_info.split(","):
            date = date.strip()
            
            # (오전) 또는 (오후) 파트를 분리
            match = re.match(r'(\d{4}-\d{2}-\d{2})\s*\((.+)\)', date)
            if match:
                date_part, time_part = match.groups()
                time_slot = f"({time_part})"
            else:
                date_part = date
                time_slot = ""
            
            dt = datetime.datetime.strptime(date_part, "%Y-%m-%d")
            month_num = dt.month
            day = dt.day
            weekday_name = weekday_map[dt.weekday()]
            
            # 분리된 파트를 올바르게 조합
            formatted_date = f"{month_num}월 {day}일({weekday_name}) {time_slot}".strip()
            formatted_dates.append(formatted_date)
        return ", ".join(formatted_dates)
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"날짜 형식 변환 중 오류 발생: {str(e)}")
        return date_info

# '추가' 및 '삭제' 섹션
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방", "12번방",
           "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]

# '추가' 및 '삭제' 섹션
# 페이지가 새로고침될 때 입력창을 초기화해야 하는지 확인하는 '신호'
if "clear_inputs" not in st.session_state:
    st.session_state.clear_inputs = False

# 'clear_inputs' 신호가 True이면, 위젯들의 상태를 초기화하고 신호를 다시 False로 변경
if st.session_state.clear_inputs:
    st.session_state.category_select = []
    st.session_state.date_multiselect_new = []
    st.session_state.timeslot_multiselect = []
    st.session_state.clear_inputs = False # 신호 초기화

요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방", "12번방",
           "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]

# '추가' 및 '삭제' 섹션
st.markdown("**🟢 방배정 요청사항 입력**")
add_col1, add_col2, add_col3, add_col4 = st.columns([2, 3, 1.5, 1])

with add_col1:
    분류 = st.multiselect("요청 분류", 요청분류, key="category_select")

# 날짜 선택 옵션을 준비하는 로직
date_options_map = {}
weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토"}

# 1. 모든 평일(월-금)을 옵션에 추가
for day in pd.date_range(month_start, month_end):
    if day.weekday() < 5:
        # ▼▼▼ [수정됨] 휴관일인 평일은 선택지에서 제외 ▼▼▼
        if day.date() in closing_dates_set:
            continue
        weekday_name = weekday_map[day.weekday()]
        display_date = f"{day.month}월 {day.day}일({weekday_name})"
        save_date = day.strftime("%Y-%m-%d")
        date_options_map[display_date] = {'save_date': save_date, 'is_saturday': False}

# 2. 근무가 있는 토요일을 옵션에 추가
if not df_saturday.empty:
    user_saturdays = df_saturday[
        (df_saturday['날짜'].dt.year == year) &
        (df_saturday['날짜'].dt.month == month) &
        (df_saturday.apply(lambda row: name in str(row.get('근무', '')) or name == str(row.get('당직', '')).strip(), axis=1))
    ]
    for _, row in user_saturdays.iterrows():
        day = row['날짜']
        # ▼▼▼ [수정됨] 휴관일인 토요일은 선택지에서 제외 ▼▼▼
        if day.date() in closing_dates_set:
            continue
        weekday_name = weekday_map[day.weekday()]
        display_date = f"{day.month}월 {day.day}일({weekday_name})"
        save_date = day.strftime("%Y-%m-%d")
        date_options_map[display_date] = {'save_date': save_date, 'is_saturday': True}

# 날짜순으로 정렬된 옵션 리스트 생성
sorted_date_options = sorted(date_options_map.keys(), key=lambda d: datetime.datetime.strptime(date_options_map[d]['save_date'], "%Y-%m-%d"))

with add_col2:
    선택된_날짜들 = st.multiselect("요청 일자", sorted_date_options, key="date_multiselect_new")

# 선택된 날짜 중 평일 또는 토요일이 있는지 확인
has_weekday = any(not date_options_map.get(d, {}).get('is_saturday', True) for d in 선택된_날짜들)
has_saturday = any(date_options_map.get(d, {}).get('is_saturday', True) for d in 선택된_날짜들)

with add_col3:
    # 평일을 선택했을 때만 시간대 선택이 활성화됨
    선택된_시간대들 = st.multiselect("시간대 선택", ["오전", "오후"], key="timeslot_multiselect", disabled=not has_weekday)

if has_saturday and "오후" in 선택된_시간대들:
    st.warning("⚠️ **토요일은 오전 근무만 가능합니다.** 선택하신 '오후' 시간대는 평일에만 적용됩니다.")

with add_col4:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    submit_add = st.button("📅 추가", use_container_width=True)

# (기존 if submit_add: 블록 전체를 아래 코드로 교체)

if submit_add:
    # 1. 저장될 '날짜정보' 문자열 생성
    날짜정보 = ""
    if 선택된_날짜들:
        final_date_list = []
        for display_date in 선택된_날짜들:
            info = date_options_map[display_date]
            save_date = info['save_date']
            if info['is_saturday']:
                final_date_list.append(f"{save_date} (오전)")
            elif has_weekday and 선택된_시간대들:
                for timeslot in 선택된_시간대들:
                    final_date_list.append(f"{save_date} ({timeslot})")
        final_date_list.sort()
        날짜정보 = ", ".join(final_date_list)
    
    # 2. 저장 로직 실행
    try:
        if 날짜정보 and 분류:
            sheet = st.session_state["sheet"]
            try:
                worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
            except WorksheetNotFound:
                worksheet2 = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
                worksheet2.append_row(["이름", "분류", "날짜정보"])
            
            df_room_request_temp = st.session_state["df_room_request"].copy()
            new_requests = []
            for category in 분류:
                for date in 날짜정보.split(","):
                    date = date.strip()
                    if not df_room_request_temp[(df_room_request_temp['이름'] == name) & (df_room_request_temp['날짜정보'] == date) & (df_room_request_temp['분류'] == category)].empty:
                        continue
                    new_requests.append({"이름": name, "분류": category, "날짜정보": date})

            if new_requests:
                with st.spinner("요청사항을 추가 중입니다..."):
                    new_request_df = pd.DataFrame(new_requests)
                    df_room_request_temp = pd.concat([df_room_request_temp, new_request_df], ignore_index=True).sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                    
                    worksheet2.clear()
                    worksheet2.update([df_room_request_temp.columns.tolist()] + df_room_request_temp.astype(str).values.tolist())
                    
                    st.session_state["df_room_request"] = df_room_request_temp
                    st.session_state["df_user_room_request"] = df_room_request_temp[df_room_request_temp["이름"] == name].copy()
                    
                    # --- 스피너가 보이도록 1초 강제 대기 ---
                    time.sleep(1)
                
                st.success("요청이 성공적으로 기록되었습니다.")
                st.session_state.clear_inputs = True
                time.sleep(1.5)
                st.rerun()
            else:
                st.info("ℹ️ 이미 존재하는 요청사항입니다.")
        else:
            st.warning("요청 분류와 날짜 정보를 올바르게 입력해주세요.")
    except Exception as e:
        st.error(f"요청 추가 중 오류 발생: {str(e)}")

st.write(" ")

st.markdown(f"<h6 style='font-weight:bold;'>🔴 방배정 요청사항 삭제</h6>", unsafe_allow_html=True)
if not st.session_state.get("df_user_room_request", pd.DataFrame()).empty:
    del_col1, del_col2 = st.columns([4, 0.5])
    
    with del_col1:
        options = [f"{row['분류']} - {format_date_for_display(row['날짜정보'])}" for _, row in st.session_state["df_user_room_request"].iterrows()]
        selected_items = st.multiselect("삭제할 요청사항 선택", options, key="delete_select")
    
    with del_col2:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        submit_delete = st.button("🗑️ 삭제", use_container_width=True)

    if submit_delete and selected_items:
        try:
            with st.spinner("요청사항을 삭제 중입니다..."):
                sheet = st.session_state["sheet"] # <-- 이렇게 수정하세요
                try:
                    worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
                except WorksheetNotFound:
                    st.error("요청사항이 저장된 시트를 찾을 수 없습니다.")
                    st.stop()
                
                df_room_request_temp = st.session_state["df_room_request"].copy()
                selected_indices = []
                for item in selected_items:
                    for idx, row in df_room_request_temp.iterrows():
                        if row['이름'] == name and f"{row['분류']} - {format_date_for_display(row['날짜정보'])}" == item:
                            selected_indices.append(idx)
                
                if selected_indices:
                    df_room_request_temp = df_room_request_temp.drop(index=selected_indices)
                    df_room_request_temp = df_room_request_temp.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                    try:
                        worksheet2.clear()
                        worksheet2.update([df_room_request_temp.columns.tolist()] + df_room_request_temp.astype(str).values.tolist())
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
                        st.stop()
                    
                    st.session_state["df_room_request"] = df_room_request_temp
                    st.session_state["df_user_room_request"] = df_room_request_temp[df_room_request_temp["이름"] == name].copy()
                    st.success("요청이 성공적으로 삭제되었습니다.")
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.info("ℹ️ 삭제할 항목을 찾을 수 없습니다.")
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"요청 삭제 중 오류 발생: {str(e)}")
            st.stop()
    elif submit_delete and not selected_items:
        st.warning("삭제할 항목을 선택해주세요.")
else:
    st.info("📍 삭제할 요청사항이 없습니다.")
