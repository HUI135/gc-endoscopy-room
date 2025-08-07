import numpy as np
import streamlit as st
import pandas as pd
import calendar
import datetime
import time
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import menu

# 💡 디버깅을 위한 출력문 추가: 스크립트가 실행될 때마다 콘솔에 표시됩니다.
print("--- Streamlit Script is running ---")

st.set_page_config(page_title="방배정 요청", page_icon="🏠", layout="wide")

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

# ✅ 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# ✅ 데이터 로드 함수 (st.cache_data 적용)
@st.cache_data(show_spinner=False)
def load_master_data_page3(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

@st.cache_data(show_spinner=False)
def load_request_data_page3(_gc, url, sheet_name):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

@st.cache_data(show_spinner=False)
def load_room_request_data_page3(_gc, url, sheet_name):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

def generate_master_events(df_user_master, year, month, week_labels):
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금"]
    
    has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
    if has_weekly:
        weekly_df = df_user_master[df_user_master["주차"] == "매주"]
        weekly_schedule = weekly_df.set_index("요일")["근무여부"].to_dict()
        for 요일 in 요일리스트:
            if 요일 not in weekly_schedule:
                weekly_schedule[요일] = "근무없음"
        for week in week_labels:
            master_data[week] = weekly_schedule
    else:
        for week in week_labels:
            week_df = df_user_master[df_user_master["주차"] == week]
            if not week_df.empty:
                master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()
            else:
                master_data[week] = {요일: "근무없음" for 요일 in 요일리스트}

    events = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
    _, last_day = calendar.monthrange(year, month)
    status_colors = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}

    first_sunday = None
    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        if date_obj.weekday() == 6:
            first_sunday = day
            break

    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()
        if weekday in weekday_map:
            day_name = weekday_map[weekday]
            if first_sunday and day < first_sunday:
                week_num = 0
            elif first_sunday:
                week_num = (day - first_sunday) // 7 + 1
            else:
                week_num = (day - 1) // 7
            
            if week_num >= len(week_labels):
                continue
            
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            if status != "근무없음":
                events.append({
                    "title": f"{status}",
                    "start": date_obj.strftime("%Y-%m-%d"),
                    "end": date_obj.strftime("%Y-%m-%d"),
                    "color": status_colors.get(status, "#E0E0E0"),
                    "source": "master"
                })
    return events

def generate_request_events(df_user_request, next_month):
    status_colors_request = {
        "휴가": "#A1C1D3", "보충 어려움(오전)": "#FFD3B5", "보충 어려움(오후)": "#FFD3B5",
        "보충 불가(오전)": "#FFB6C1", "보충 불가(오후)": "#FFB6C1", "꼭 근무(오전)": "#C3E6CB",
        "꼭 근무(오후)": "#C3E6CB",
    }
    label_map = {
        "휴가": "휴가🎉", "보충 어려움(오전)": "보충⚠️(오전)", "보충 어려움(오후)": "보충⚠️(오후)",
        "보충 불가(오전)": "보충🚫(오전)", "보충 불가(오후)": "보충🚫(오후)", "꼭 근무(오전)": "꼭근무(오전)",
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

def generate_room_request_events(df_user_room_request, next_month):
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

# ✅ 데이터 초기화 로직을 분리
def initialize_and_sync_data(gc, url, name):
    """페이지에 필요한 모든 데이터를 로드하고 세션 상태에 저장합니다."""
    # st.cache_data.clear() # 재실행 시 캐시를 지우지 않습니다.
    df_master = load_master_data_page3(gc, url)
    df_request = load_request_data_page3(gc, url, f"{month_str} 요청")
    df_room_request = load_room_request_data_page3(gc, url, f"{month_str} 방배정 요청")
    
    st.session_state["df_master"] = df_master
    st.session_state["df_request"] = df_request
    st.session_state["df_room_request"] = df_room_request
    st.session_state["df_user_master"] = df_master[df_master["이름"] == name].copy()
    st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy()
    st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy() if "이름" in df_room_request.columns and not df_room_request.empty else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

    # 마스터 데이터가 없으면 초기 데이터 추가
    if st.session_state["df_user_master"].empty:
        st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
        initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
        initial_df = pd.DataFrame(initial_rows)
        
        sheet = gc.open_by_url(url)
        worksheet1 = sheet.worksheet("마스터")
        df_master_all = pd.DataFrame(worksheet1.get_all_records())
        df_master_all = pd.concat([df_master_all, initial_df], ignore_index=True)
        worksheet1.clear()
        worksheet1.update([df_master_all.columns.tolist()] + df_master_all.values.tolist())
        st.session_state["df_user_master"] = initial_df

    # 주차별 근무 일정이 모두 같으면 "매주"로 변환
    has_weekly = "매주" in st.session_state["df_user_master"]["주차"].values if not st.session_state["df_user_master"].empty else False
    if not st.session_state["df_user_master"].empty and not has_weekly:
        week_nums_count = len(sorted(set(d.isocalendar()[1] for d in pd.date_range(start=next_month, end=next_month.replace(day=last_day)))))
        week_labels = [f"{i+1}주" for i in range(week_nums_count)]
        
        try:
            pivot_df = st.session_state["df_user_master"].pivot(index="요일", columns="주차", values="근무여부")
            expected_weeks = set(week_labels)
            actual_weeks = set(pivot_df.columns)
            
            if actual_weeks == expected_weeks and pivot_df.apply(lambda x: x.nunique() == 1, axis=1).all():
                st.session_state["df_user_master"]["주차"] = "매주"
                st.session_state["df_user_master"] = st.session_state["df_user_master"].drop_duplicates(subset=["이름", "주차", "요일"])
                df_master_all = st.session_state["df_master"][st.session_state["df_master"]["이름"] != name]
                df_master_all = pd.concat([df_master_all, st.session_state["df_user_master"]], ignore_index=True)
                sheet = gc.open_by_url(url)
                worksheet1 = sheet.worksheet("마스터")
                worksheet1.clear()
                worksheet1.update([df_master_all.columns.tolist()] + df_master_all.values.tolist())
                st.session_state["df_master"] = df_master_all
        except KeyError as e:
            pass

# ✅ 전역 변수 설정
gc = get_gspread_client()
url = st.secrets["google_sheet"]["url"]
name = st.session_state["name"]
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()
next_month = today.replace(day=1) + relativedelta(months=1)
month_str = next_month.strftime("%Y년 %m월")
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)
week_nums = sorted(set(d.isocalendar()[1] for d in pd.date_range(start=next_month, end=next_month.replace(day=last_day))))
week_labels = [f"{i+1}주" for i in range(len(week_nums))]

# ✅ 페이지 로드 시 단 한 번만 데이터 로드
if "initial_load_done" not in st.session_state:
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        initialize_and_sync_data(gc, url, name)
        st.session_state["initial_load_done"] = True
    # ⚠️ st.rerun()을 제거하여 불필요한 재실행을 막습니다.
    # 이 경우, 초기 로딩 후 자연스럽게 UI가 그려지도록 Streamlit의 정상적인 흐름에 맡깁니다.

# --- UI 렌더링 시작 ---
# st.session_state["df_master"]와 같은 세션 변수들이 이제 채워졌으므로
# UI 컴포넌트들은 정상적으로 동작하게 됩니다.

# 캘린더 이벤트 생성 (세션 상태에 저장된 최신 데이터를 사용)
master_events = generate_master_events(st.session_state["df_user_master"], next_month.year, next_month.month, week_labels)
request_events = generate_request_events(st.session_state["df_user_request"], next_month)
room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month)
all_events = master_events + request_events + room_request_events

st.header(f"📅 {name} 님의 {month_str} 방배정 요청", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    with st.spinner("데이터를 다시 불러오는 중입니다..."):
        st.cache_data.clear()
        initialize_and_sync_data(gc, url, name)
    st.success("데이터가 새로고침되었습니다.")
    # ✅ 새로고침 버튼 클릭 시에만 명시적으로 재실행
    st.rerun()

if not all_events:
    st.info("☑️ 표시할 스케줄 또는 요청사항이 없습니다.")
else:
    calendar_options = {
        "initialView": "dayGridMonth",
        "initialDate": next_month.strftime("%Y-%m-%d"),
        "editable": False,
        "selectable": False,
        "eventDisplay": "block",
        "dayHeaderFormat": {"weekday": "short"},
        "themeSystem": "bootstrap",
        "height": 700,
        "headerToolbar": {"left": "", "center": "", "right": ""},
        "showNonCurrentDates": False,
        "fixedWeekCount": False,
        "eventOrder": "source"
    }
    st_calendar(events=all_events, options=calendar_options, key="calendar_view")

st.divider()

# 근무 가능 일자와 시간대 계산
def get_user_available_dates(name, df_master, month_start, month_end):
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
                month_num = str(day.month).lstrip("0")
                day_num = f"{day.day:02d}"
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

# 날짜정보를 요청사항 삭제 UI 형식으로 변환
def format_date_for_display(date_info):
    try:
        formatted_dates = []
        weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
        for date in date_info.split(","):
            date = date.strip()
            date_part = date.split(" (")[0]
            time_slot_match = date.split(" (")
            time_slot = f"({time_slot_match[1]})" if len(time_slot_match) > 1 else ""
            
            dt = datetime.datetime.strptime(date_part, "%Y-%m-%d")
            month_num = str(dt.month).lstrip("0")
            day = f"{dt.day:02d}"
            weekday_name = weekday_map[dt.weekday()]
            formatted_date = f"{month_num}월 {day}일({weekday_name}) {time_slot}".strip()
            formatted_dates.append(formatted_date)
        return ", ".join(formatted_dates)
    except:
        return date_info

# ---------------- '추가' 및 '삭제' 섹션 ----------------
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방",
           "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]

st.markdown("**🟢 방배정 요청사항 입력**")
add_col1, add_col2, add_col3 = st.columns([2, 3, 1])

with add_col1:
    분류 = st.multiselect("요청 분류", 요청분류, key="category_select")
with add_col2:
    available_dates = get_user_available_dates(name, st.session_state["df_master"], next_month_start, next_month_end)
    date_options = [date_str for date_str, _, _ in available_dates]
    date_values = [(save_date, time_slot) for _, save_date, time_slot in available_dates]
    날짜 = st.multiselect("요청 일자", date_options, key="date_multiselect")

def format_date_to_korean(date_str, period):
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return f"{date_obj.strftime('%Y-%m-%d')} ({period})"

날짜정보 = ""
if 날짜:
    date_indices = [(i, date_values[i]) for i, opt in enumerate(date_options) if opt in 날짜]
    sorted_dates = sorted(date_indices, key=lambda x: (x[1][0], x[1][1]))
    날짜정보 = ", ".join([
        format_date_to_korean(date_values[idx][0], date_values[idx][1])
        for idx, _ in sorted_dates
    ])

with add_col3:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    submit_add = st.button("📅 추가", use_container_width=True)

if submit_add:
    if 날짜정보 and 분류:
        with st.spinner("요청사항을 추가 중입니다..."):
            sheet = get_gspread_client().open_by_url(url)
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
                    existing_request = df_room_request_temp[(df_room_request_temp['이름'] == name) & (df_room_request_temp['날짜정보'] == date) & (df_room_request_temp['분류'] == category)]
                    if existing_request.empty:
                        new_requests.append({"이름": name, "분류": category, "날짜정보": date})

            if new_requests:
                new_request_df = pd.DataFrame(new_requests)
                df_room_request_temp = pd.concat([df_room_request_temp, new_request_df], ignore_index=True)
                df_room_request_temp = df_room_request_temp.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                
                worksheet2.clear()
                worksheet2.update([df_room_request_temp.columns.tolist()] + df_room_request_temp.astype(str).values.tolist())
                
                st.session_state["df_room_request"] = df_room_request_temp
                st.session_state["df_user_room_request"] = df_room_request_temp[df_room_request_temp["이름"] == name].copy()
                st.success("요청사항이 추가되었습니다!", icon="📅")
                time.sleep(1)
                st.rerun() # 추가 후 재실행
            else:
                st.info("ℹ️ 이미 존재하는 요청사항입니다.")
    else:
        st.warning("요청 분류와 날짜 정보를 올바르게 입력해주세요.")

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
        with st.spinner("요청사항을 삭제 중입니다..."):
            sheet = get_gspread_client().open_by_url(url)
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
                worksheet2.clear()
                worksheet2.update([df_room_request_temp.columns.tolist()] + df_room_request_temp.astype(str).values.tolist())
                
                st.session_state["df_room_request"] = df_room_request_temp
                st.session_state["df_user_room_request"] = df_room_request_temp[df_room_request_temp["이름"] == name].copy()
                st.success("요청사항이 삭제되었습니다!", icon="🗑️")
                time.sleep(1)
                st.rerun() # 삭제 후 재실행
            else:
                st.info("ℹ️ 삭제할 항목을 찾을 수 없습니다.")
    elif submit_delete and not selected_items:
        st.warning("삭제할 항목을 선택해주세요.")
else:
    st.info("📍 삭제할 요청사항이 없습니다.")
