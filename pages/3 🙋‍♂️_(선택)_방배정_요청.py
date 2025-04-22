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

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수
def load_master_data(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

def load_request_data(_gc, url, sheet_name):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

def load_room_request_data(_gc, url, sheet_name):
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
                    "source": "master"  # 소스 추가
                })
    return events

# 캘린더 이벤트 생성 함수 (df_request)
def generate_request_events(df_user_request, next_month):
    status_colors_request = {
        "휴가": "#FE7743",
        "학회": "#5F99AE",
        "보충 어려움(오전)": "#FFB347",
        "보충 어려움(오후)": "#FFA07A",
        "보충 불가(오전)": "#FFB347",
        "보충 불가(오후)": "#FFA07A",
        "꼭 근무(오전)": "#4CAF50",
        "꼭 근무(오후)": "#2E8B57",
    }
    label_map = {
        "휴가": "휴가",
        "학회": "학회",
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
                "source": "request"  # 소스 추가
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
                        "source": "request"  # 소스 추가
                    })
                except:
                    continue
    return events

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
        "8:30": "8:30",
        "9:00": "9:00",
        "9:30": "9:30",
        "10:00": "10:00",
        "이른방": "이른방",
        "오전 당직": "오전당직",
        "오후 당직": "오후당직",
    }
    
    events = []
    for _, row in df_user_room_request.iterrows():
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
                "color": "#273F4F",  # 빨간색 고정
                "source": "room_request"  # 소스 추가
            })
        else:
            for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                try:
                    dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                    events.append({
                        "title": label_map.get(분류, 분류),
                        "start": dt.strftime("%Y-%m-%d"),
                        "end": dt.strftime("%Y-%m-%d"),
                        "color": "#273F4F",  # 빨간색 고정
                        "source": "room_request"  # 소스 추가
                    })
                except:
                    continue
    return events

# 로그인 체크
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# 사이드바
if st.session_state.get("login_success", False):
    st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
    if st.sidebar.button("로그아웃"):
        st.session_state.clear()
        st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
        time.sleep(2)
        st.rerun()

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

# 초기 데이터 로드 및 세션 상태 설정
if "df_master" not in st.session_state:
    st.session_state["df_master"] = load_master_data(gc, url)
if "df_request" not in st.session_state:
    st.session_state["df_request"] = load_request_data(gc, url, f"{month_str} 요청")
if "df_room_request" not in st.session_state:
    st.session_state["df_room_request"] = load_room_request_data(gc, url, f"{month_str} 방배정 요청")
if "df_user_master" not in st.session_state:
    st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
if "df_user_request" not in st.session_state:
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
if "df_user_room_request" not in st.session_state:
    st.session_state["df_user_room_request"] = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == name].copy()

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

# 캘린더 이벤트 생성 및 통합
master_events = generate_master_events(df_user_master, next_month.year, next_month.month, week_labels)
request_events = generate_request_events(df_user_request, next_month)
room_request_events = generate_room_request_events(df_user_room_request, next_month)
all_events = master_events + request_events + room_request_events

# 캘린더 표시
st.header(f"📅 {name} 님의 {month_str} 스케줄 및 요청사항", divider='rainbow')
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
        "height": 500,
        "headerToolbar": {"left": "", "center": "", "right": ""},
        "showNonCurrentDates": False,
        "fixedWeekCount": False,
        "eventOrder": "source"  # 소스 속성으로 이벤트 순서 정렬
    }
    st_calendar(events=all_events, options=calendar_options)

st.divider()

# 방배정 요청사항 입력 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🟢 방배정 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방", 
           "당직 안됨", "오전 당직 안됨", "오후 당직 안됨", "당직 아닌 이른방", "8:30", "9:00", "9:30", "10:00", "이른방", 
           "오전 당직", "오후 당직"]
날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

col1, col2, col3 = st.columns([1,1,2])
분류 = col1.multiselect("요청 분류", 요청분류, key="category_select")
방식 = col2.selectbox("날짜 선택 방식", 날짜선택방식, key="method_select")

# 날짜 입력 로직
날짜정보 = ""
if 방식 == "일자 선택":
    날짜 = col3.multiselect("요청 일자", [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)], format_func=lambda x: x.strftime("%Y-%m-%d"), key="date_multiselect")
    날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜]) if 날짜 else ""
elif 방식 == "기간 선택":
    날짜범위 = col3.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end, key="date_range")
    if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
        날짜정보 = f"{날짜범위[0].strftime('%Y-%m-%d')} ~ {날짜범위[1].strftime('%Y-%m-%d')}"
elif 방식 == "주/요일 선택":
    선택주차 = col3.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"], key="week_select")
    선택요일 = col3.multiselect("요일 선택", ["월", "화", "수", "목", "금"], key="day_select")
    주차_index = {"첫째주": 0, "둘째주": 1, "셋째주": 2, "넷째주": 3, "다섯째주": 4}
    요일_index = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
    날짜목록 = []

    first_sunday = None
    for i in range(1, last_day + 1):
        date_obj = datetime.date(next_month.year, next_month.month, i)
        if date_obj.weekday() == 6:
            first_sunday = i
            break

    for i in range(1, last_day + 1):
        날짜 = datetime.date(next_month.year, next_month.month, i)
        weekday = 날짜.weekday()
        if i < first_sunday:
            week_of_month = 0
        else:
            week_of_month = (i - first_sunday) // 7 + 1
        if weekday in 요일_index.values() and any(주차 == "매주" or 주차_index.get(주차) == week_of_month for 주차 in 선택주차):
            if weekday in [요일_index[요일] for 요일 in 선택요일]:
                날짜목록.append(날짜.strftime("%Y-%m-%d"))
    날짜정보 = ", ".join(날짜목록) if 날짜목록 else ""

# 저장 로직
if st.button("📅 추가"):
    sheet = gc.open_by_url(url)
    # 시트가 없으면 생성
    try:
        worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
    except WorksheetNotFound:
        worksheet2 = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
        worksheet2.append_row(["이름", "분류", "날짜정보"])
    
    if 날짜정보:
        df_room_request = pd.concat([df_room_request, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": 날짜정보}])], ignore_index=True)
    else:
        st.warning("날짜 정보를 올바르게 입력해주세요.")
        st.stop()
    
    df_room_request = df_room_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
    worksheet2.clear()
    worksheet2.update([df_room_request.columns.tolist()] + df_room_request.astype(str).values.tolist())
    st.session_state["df_room_request"] = df_room_request
    st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy()
    st.success("✅ 요청사항이 저장되었습니다!")
    st.cache_data.clear()
    st.session_state["df_room_request"] = load_room_request_data(gc, url, f"{month_str} 방배정 요청")
    st.session_state["df_user_room_request"] = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == name].copy()
    st.rerun()

# 삭제 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 방배정 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_room_request.empty:
    options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_user_room_request.iterrows()]
    selected_items = st.multiselect("요청사항 선택", options, key="delete_select")
    if st.button("🗑️ 삭제") and selected_items:
        sheet = gc.open_by_url(url)
        # 시트가 없으면 생성
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 방배정 요청")
        except WorksheetNotFound:
            worksheet2 = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
        
        selected_indices = []
        for item in selected_items:
            for idx, row in df_user_room_request.iterrows():
                if f"{row['분류']} - {row['날짜정보']}" == item:
                    selected_indices.append(idx)
        df_room_request = df_room_request.drop(index=selected_indices)
        df_room_request = df_room_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_room_request.columns.tolist()] + df_room_request.astype(str).values.tolist())
        st.session_state["df_room_request"] = df_room_request
        st.session_state["df_user_room_request"] = df_room_request[df_room_request["이름"] == name].copy()
        st.success("✅ 선택한 요청사항이 삭제되었습니다!")
        st.cache_data.clear()
        st.session_state["df_room_request"] = load_room_request_data(gc, url, f"{month_str} 방배정 요청")
        st.session_state["df_user_room_request"] = st.session_state["df_room_request"][st.session_state["df_room_request"]["이름"] == name].copy()
        st.rerun()
else:
    st.info("📍 요청사항 없음")