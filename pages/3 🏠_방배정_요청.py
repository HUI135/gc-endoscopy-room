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

st.set_page_config(page_title="마스터 수정", page_icon="🏠", layout="wide")

menu.menu()

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수
def load_master_data_page3(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

def load_request_data_page3(_gc, url, sheet_name):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

def load_room_request_data_page3(_gc, url, sheet_name):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

# 캘린더 이벤트 생성 함수 (df_master)
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
            if day < first_sunday:
                week_num = 0
            else:
                week_num = (day - first_sunday) // 7 + 1
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

# 캘린더 이벤트 생성 함수 (df_request)
def generate_request_events(df_user_request, next_month):
    status_colors_request = {
        "휴가": "#FE7743",
        "보충 어려움(오전)": "#FFB347",
        "보충 어려움(오후)": "#FFA07A",
        "보충 불가(오전)": "#FFB347",
        "보충 불가(오후)": "#FFA07A",
        "꼭 근무(오전)": "#4CAF50",
        "꼭 근무(오후)": "#2E8B57",
    }
    label_map = {
        "휴가": "휴가",
        "보충 어려움(오전)": "보충⚠️(오전)",
        "보충 어려움(오후)": "보충⚠️(오후)",
        "보충 불가(오전)": "보충🚫(오전)",
        "보충 불가(오후)": "보충🚫(오후)",
        "꼭 근무(오전)": "꼭근무(오전)",
        "꼭 근무(오후)": "꼭근무(오후)",
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
            events.append({
                "title": label_map.get(분류, 분류),
                "start": 시작.strftime("%Y-%m-%d"),
                "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                "color": status_colors_request.get(분류, "#E0E0E0"),
                "source": "request"
            })
        else:
            for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                try:
                    dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                    events.append({
                        "title": label_map.get(분류, 분류),
                        "start": dt.strftime("%Y-%m-%d"),
                        "end": dt.strftime("%Y-%m-%d"),
                        "color": status_colors_request.get(분류, "#E0E0E0"),
                        "source": "request"
                    })
                except:
                    continue
    return events

# 캘린더 이벤트 생성 함수 (df_room_request)
# 캘린더 이벤트 생성 함수 (df_room_request)
def generate_room_request_events(df_user_room_request, next_month):
    label_map = {
        "1번방": "1번방",
        "2번방": "2번방",
        "3번방": "3번방",
        "4번방": "4번방",
        "5번방": "5번방",
        "6번방": "6번방",
        "7번방": "7번방",
        "8번방": "8번방",
        "9번방": "9번방",
        "10번방": "10번방",
        "11번방": "11번방",
        "당직 안됨": "당직🚫",
        "오전 당직 안됨": "오전당직🚫",
        "오후 당직 안됨": "오후당직🚫",
        "당직 아닌 이른방": "당직아닌이른방",
        "이른방 제외": "이른방 제외",
        "늦은방 제외": "늦은방 제외",
        "8:30": "8:30",
        "9:00": "9:00",
        "9:30": "9:30",
        "10:00": "10:00",
        "오전 당직": "오전당직",
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
                # 날짜 형식: "2025-04-03 (오전)"
                date_part, time_slot = 날짜.split(" (")
                time_slot = time_slot.rstrip(")")
                dt = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
                events.append({
                    "title": label_map.get(분류, 분류),
                    "start": dt.strftime("%Y-%m-%d"),
                    "end": dt.strftime("%Y-%m-%d"),
                    "color": "#273F4F",
                    "source": "room_request",
                    "allDay": True
                })
            except Exception as e:
                st.warning(f"날짜 파싱 실패: {날짜}, 오류: {str(e)}")
                continue
    return events


# 로그인 체크
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# 기본 설정
gc = get_gspread_client()
url = st.secrets["google_sheet"]["url"]
name = st.session_state["name"]
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()
next_month = today.replace(day=1) + relativedelta(months=1)
month_str = next_month.strftime("%Y년 %m월")
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.cache_resource.clear()
    gc = get_gspread_client()
    st.session_state["df_master"] = load_master_data_page3(gc, url)
    st.session_state["df_request"] = load_request_data_page3(gc, url, f"{month_str} 요청")
    st.session_state["df_room_request"] = load_room_request_data_page3(gc, url, f"{month_str} 방배정 요청")
    st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
    if not st.session_state["df_room_request"].empty and "이름" in st.session_state["df_room_request"].columns:
        st.session_state["df_user_room_request"] = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == name].copy()
    else:
        st.session_state["df_user_room_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])

    week_nums = sorted(set(d.isocalendar()[1] for d in pd.date_range(start=next_month, end=next_month.replace(day=last_day))))
    week_labels = [f"{i+1}주" for i in range(len(week_nums))]

    master_events = generate_master_events(st.session_state["df_user_master"], next_month.year, next_month.month, week_labels)
    request_events = generate_request_events(st.session_state["df_user_request"], next_month)
    room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month)
    st.session_state["all_events"] = master_events + request_events + room_request_events
    
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 초기 데이터 로드 및 세션 상태 설정
if "df_master" not in st.session_state:
    st.session_state["df_master"] = load_master_data_page3(gc, url)
if "df_request" not in st.session_state:
    st.session_state["df_request"] = load_request_data_page3(gc, url, f"{month_str} 요청")
if "df_room_request" not in st.session_state:
    st.session_state["df_room_request"] = load_room_request_data_page3(gc, url, f"{month_str} 방배정 요청")
if "df_user_master" not in st.session_state:
    st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
if "df_user_request" not in st.session_state:
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
if "df_user_room_request" not in st.session_state:
    if not st.session_state["df_room_request"].empty and "이름" in st.session_state["df_room_request"].columns:
        st.session_state["df_user_room_request"] = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == name].copy()
    else:
        st.session_state["df_user_room_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])

# 항상 최신 세션 상태를 참조
df_master = st.session_state["df_master"]
df_request = st.session_state["df_request"]
df_room_request = st.session_state["df_room_request"]
df_user_master = st.session_state["df_user_master"]
df_user_request = st.session_state["df_user_request"]
df_user_room_request = st.session_state["df_user_room_request"]

# 마스터 데이터 초기화
if df_user_master.empty:
    st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
    initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
    initial_df = pd.DataFrame(initial_rows)
    initial_df["요일"] = pd.Categorical(initial_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
    initial_df = initial_df.sort_values(by=["이름", "주차", "요일"])
    df_master = pd.concat([df_master, initial_df], ignore_index=True)
    df_user_master = initial_df
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터")
    worksheet1.clear()
    worksheet1.update([df_master.columns.values.tolist()] + df_master.values.tolist())
    st.session_state["df_master"] = df_master
    st.session_state["df_user_master"] = df_user_master

# 주차 리스트
week_nums = sorted(set(d.isocalendar()[1] for d in pd.date_range(start=next_month, end=next_month.replace(day=last_day))))
week_labels = [f"{i+1}주" for i in range(len(week_nums))]

# 매주 변환 로직
has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
if not df_user_master.empty and not has_weekly:
    updated = False
    pivot_df = df_user_master.pivot(index="요일", columns="주차", values="근무여부")
    expected_weeks = set([f"{i+1}주" for i in range(len(week_nums))])
    actual_weeks = set(pivot_df.columns)
    if actual_weeks == expected_weeks and pivot_df.apply(lambda x: x.nunique() == 1, axis=1).all():
        df_user_master["주차"] = "매주"
        df_user_master = df_user_master.drop_duplicates(subset=["이름", "주차", "요일"])
        updated = True
    if updated:
        df_user_master["요일"] = pd.Categorical(df_user_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df_user_master = df_user_master.sort_values(by=["이름", "주차", "요일"])
        df_master = df_master[df_master["이름"] != name]
        df_master = pd.concat([df_master, df_user_master], ignore_index=True)
        df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df_master = df_master.sort_values(by=["이름", "주차", "요일"])
        sheet = gc.open_by_url(url)
        worksheet1 = sheet.worksheet("마스터")
        worksheet1.clear()
        worksheet1.update([df_master.columns.values.tolist()] + df_master.values.tolist())
        st.session_state["df_master"] = df_master
        st.session_state["df_user_master"] = df_user_master

# 초기 캘린더 이벤트 생성
master_events = generate_master_events(df_user_master, next_month.year, next_month.month, week_labels)
request_events = generate_request_events(df_user_request, next_month)
room_request_events = generate_room_request_events(df_user_room_request, next_month)
if "all_events" not in st.session_state:
    st.session_state["all_events"] = master_events + request_events + room_request_events

# 캘린더 표시
st.header(f"📅 {name} 님의 {month_str} 방배정 요청", divider='rainbow')
st.write("- 일자별 내시경실(방) 및 시간대 요청사항이 있으신 경우 입력해 주세요.")
if not st.session_state["all_events"]:
    st.info("☑️ 표시할 스케줄 또는 요청사항이 없습니다.")
elif df_room_request.empty or df_user_room_request.empty:
    st.info("☑️ 표시할 방배정 요청사항이 없습니다.")
    calendar_options = {
        "initialView": "dayGridMonth",
        "initialDate": next_month.strftime("%Y-%m-%d"),
        "editable": False,
        "selectable": False,
        "eventDisplay": "block",
        "dayHeaderFormat": {"weekday": "short"},
        "themeSystem": "bootstrap",
        "height": 500,
        "headerToolbar": {"left": "", "center": "", "right": ""},
        "showNonCurrentDates": False,
        "fixedWeekCount": False,
        "eventOrder": "source"
    }
    st_calendar(events=st.session_state["all_events"], options=calendar_options)
else:
    calendar_options = {
        "initialView": "dayGridMonth",
        "initialDate": next_month.strftime("%Y-%m-%d"),
        "editable": False,
        "selectable": False,
        "eventDisplay": "block",
        "dayHeaderFormat": {"weekday": "short"},
        "themeSystem": "bootstrap",
        "height": 500,
        "headerToolbar": {"left": "", "center": "", "right": ""},
        "showNonCurrentDates": False,
        "fixedWeekCount": False,
        "eventOrder": "source"
    }
    st_calendar(events=st.session_state["all_events"], options=calendar_options)

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
                # UI에 표시될 형식: "4월 02일(수) 오전"
                month = str(day.month).lstrip("0")
                day_num = f"{day.day:02d}"  # Zero-padded day
                display_date = f"{month}월 {day_num}일({weekday_name})"
                # Google Sheets 저장 형식: "2025-04-04"
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

# 사용자별 근무 가능 일자와 시간대
available_dates = get_user_available_dates(name, df_master, next_month_start, next_month_end)
date_options = [date_str for date_str, _, _ in available_dates]
date_values = [(save_date, time_slot) for _, save_date, time_slot in available_dates]

# 날짜정보를 요청사항 삭제 UI 형식으로 변환
def format_date_for_display(date_info):
    try:
        formatted_dates = []
        weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
        for date in date_info.split(","):
            date = date.strip()
            # 날짜 형식: "2025-04-03 (오전)"
            date_part, time_slot = date.split(" (")
            time_slot = time_slot.rstrip(")")
            dt = datetime.datetime.strptime(date_part, "%Y-%m-%d")
            month = str(dt.month).lstrip("0")
            day = f"{dt.day:02d}"  # Zero-padded day
            weekday_name = weekday_map[dt.weekday()]
            formatted_date = f"{month}월 {day}일({weekday_name}) {time_slot}"
            formatted_dates.append(formatted_date)
        return ", ".join(formatted_dates)
    except:
        return date_info

# 방배정 요청사항 입력 및 삭제 UI
st.write(" ")
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방",
           "8:30", "9:00", "9:30", "10:00", "당직 아닌 이른방", "이른방 제외", "늦은방 제외", "오후 당직 제외"]

# ---------------- '추가' 섹션 교체용 코드 ----------------
st.markdown("**🟢 방배정 요청사항 입력**")
# [수정] 버튼을 위한 세 번째 컬럼 추가
add_col1, add_col2, add_col3 = st.columns([2, 3, 1])

with add_col1:
    분류 = st.multiselect("요청 분류", 요청분류, key="category_select")
with add_col2:
    날짜 = st.multiselect("요청 일자", date_options, key="date_multiselect")

def format_date_to_korean(date_str, period):
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return f"{date_obj.strftime('%Y-%m-%d')} ({period})"

if 날짜:
    date_indices = [(i, date_values[i]) for i, opt in enumerate(date_options) if opt in 날짜]
    sorted_dates = sorted(date_indices, key=lambda x: (x[1][0], x[1][1]))
    날짜정보 = ", ".join([
        format_date_to_korean(date_values[idx][0], date_values[idx][1])
        for idx, _ in sorted_dates
    ])
else:
    날짜정보 = ""

with add_col3:
    # [수정] 버튼 정렬을 위한 공백 추가
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    submit_add = st.button("📅 추가", use_container_width=True)

# 저장 로직
if submit_add:
    sheet = gc.open_by_url(url)
    try:
        worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
    except WorksheetNotFound:
        worksheet2 = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
        worksheet2.append_row(["이름", "분류", "날짜정보"])
    
    if 날짜정보 and 분류:
        new_requests = []
        for date in 날짜정보.split(","):
            date = date.strip()
            for category in 분류:
                existing_request = df_room_request[(df_room_request['이름'] == name) & (df_room_request['날짜정보'] == date) & (df_room_request['분류'] == category)]
                if existing_request.empty:
                    new_requests.append({"이름": name, "분류": category, "날짜정보": date})

        if new_requests:
            existing_dates = set(date.strip() for date in 날짜정보.split(","))
            df_room_request = df_room_request[~((df_room_request['이름'] == name) & (df_room_request['날짜정보'].isin(existing_dates)))]
            new_request_df = pd.DataFrame(new_requests)
            df_room_request = pd.concat([df_room_request, new_request_df], ignore_index=True)

        df_room_request = df_room_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_room_request.columns.tolist()] + df_room_request.astype(str).values.tolist())
        st.session_state["df_room_request"] = df_room_request
        st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy()

        room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month)
        st.session_state["all_events"] = master_events + request_events + room_request_events

        st.success("✅ 요청사항이 저장되었습니다!")
        st.rerun()
    else:
        st.warning("요청 분류와 날짜 정보를 올바르게 입력해주세요.")

# ---------------- '삭제' 섹션 교체용 코드 ----------------
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 방배정 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_room_request.empty:
    # [수정] 컬럼을 사용하여 multiselect와 버튼을 나란히 배치
    del_col1, del_col2 = st.columns([4, 1])
    
    with del_col1:
        options = [f"{row['분류']} - {format_date_for_display(row['날짜정보'])}" for _, row in df_user_room_request.iterrows()]
        selected_items = st.multiselect("삭제할 요청사항 선택", options, key="delete_select")
    
    with del_col2:
        # [수정] 버튼 정렬을 위한 공백 추가
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        submit_delete = st.button("🗑️ 삭제", use_container_width=True)

    if submit_delete and selected_items:
        sheet = gc.open_by_url(url)
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
        except WorksheetNotFound:
            worksheet2 = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
        
        selected_indices = []
        # df_room_request에서 인덱스를 찾아 삭제해야 합니다.
        for item in selected_items:
            for idx, row in df_room_request.iterrows():
                if row['이름'] == name and f"{row['분류']} - {format_date_for_display(row['날짜정보'])}" == item:
                    selected_indices.append(idx)
        
        if selected_indices:
            df_room_request = df_room_request.drop(index=selected_indices)
        
        df_room_request = df_room_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_room_request.columns.tolist()] + df_room_request.astype(str).values.tolist())
        st.session_state["df_room_request"] = df_room_request
        st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy()

        room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month)
        st.session_state["all_events"] = master_events + request_events + room_request_events

        st.success("✅ 선택한 요청사항이 삭제되었습니다!")
        st.rerun()
    elif submit_delete and not selected_items:
        st.warning("삭제할 항목을 선택해주세요.")
else:
    st.info("📍 삭제할 요청사항이 없습니다.")