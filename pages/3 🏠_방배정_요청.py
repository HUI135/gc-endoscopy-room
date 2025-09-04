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

# 페이지 설정
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

def generate_master_events(df_user_master, year, month, week_labels):
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금", "토", "일"] 
    
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
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    _, last_day = calendar.monthrange(year, month)
    status_colors = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}

    # 해당 월의 첫 번째 일요일 찾기 (주차 계산의 기준)
    first_sunday = next((day for day in range(1, 8) if datetime.date(year, month, day).weekday() == 6), None)

    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()
        if weekday in weekday_map:
            day_name = weekday_map[weekday]
            
            # 날짜에 해당하는 주차 계산
            if first_sunday is None: # 만약 첫 주에 일요일이 없다면
                week_num = (date_obj.day + datetime.date(year, month, 1).weekday()) // 7
            else:
                week_num = (day - first_sunday) // 7 + 1 if day >= first_sunday else 0

            if week_num >= len(week_labels):
                continue
            
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            
            if status and status != "근무없음":
                events.append({
                    "title": f"{status}",
                    "start": date_obj.strftime("%Y-%m-%d"),
                    "end": date_obj.strftime("%Y-%m-%d"),
                    "color": status_colors.get(status, "#E0E0E0"),
                    "source": "master"
                })
    return events

def generate_request_events(df_user_request, today):
    status_colors_request = {
        "휴가": "#A1C1D3", "학회": "#B4ABE4", "보충 어려움(오전)": "#FFD3B5", "보충 어려움(오후)": "#FFD3B5",
        "보충 불가(오전)": "#FFB6C1", "보충 불가(오후)": "#FFB6C1", "꼭 근무(오전)": "#C3E6CB",
        "꼭 근무(오후)": "#C3E6CB",
    }
    label_map = {
        "휴가": "휴가🎉", "학회": "학회📚", "보충 어려움(오전)": "보충⚠️(오전)", "보충 어려움(오후)": "보충⚠️(오후)",
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

# 데이터 초기화 로직
# 데이터 초기화 로직
def initialize_and_sync_data(gc, url, name, month_start, month_end):
    """페이지에 필요한 모든 데이터를 로드하고, 동기화하며, 세션 상태에 저장합니다."""
    try:
        sheet = gc.open_by_url(url)
        st.session_state["sheet"] = sheet

        # 1. 데이터 로드
        df_master = load_master_data_page3(sheet)
        df_request = load_request_data_page3(sheet, f"{month_str} 요청")
        df_room_request = load_room_request_data_page3(sheet, f"{month_str} 방배정 요청")

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
master_events = generate_master_events(st.session_state["df_user_master"], year, month, week_labels)
request_events = generate_request_events(st.session_state["df_user_request"], next_month_date)
room_request_events = generate_room_request_events(st.session_state["df_user_room_request"], next_month_date)
all_events = master_events + request_events + room_request_events

st.header(f"📅 {name} 님의 {month_str} 방배정 요청", divider='rainbow')

# 새로고침 버튼 로직
if st.button("🔄 새로고침 (R)"):
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()
            initialize_and_sync_data(gc, url, name)
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

# 1. CSS 스타일 정의
st.markdown("""
<style>
/* 월(Month) 표시 타이틀 */
.calendar-title {
    text-align: center;
    font-size: 24px;
    font-weight: bold;
    margin-bottom: 20px;
}
div[data-testid="stHorizontalBlock"] {
    gap: 0.5rem;
}
/* 요일 헤더 */
.calendar-header {
    text-align: center;
    font-weight: bold;
    padding: 10px 0;
    border: 1px solid #e1e4e8;
    border-radius: 5px;
    background-color: #f6f8fa;
}
/* 토요일, 일요일 색상 */
.saturday { color: blue; }
.sunday { color: red; }

/* 날짜 하나하나를 의미하는 셀 */
.calendar-day-cell {
    border: 1px solid #e1e4e8;
    border-radius: 5px;
    padding: 6px;
    min-height: 120px; /* 칸 높이 조절 */
    background-color: white;
    display: flex;
    flex-direction: column;
}
/* 날짜 숫자 스타일 */
.day-number {
    font-weight: bold;
    font-size: 14px;
    margin-bottom: 5px;
}
/* 다른 달의 날짜는 회색으로 */
.day-number.other-month {
    color: #ccc;
}
/* 이벤트 아이템 스타일 */
.event-item {
    font-size: 13px;
    padding: 1px 5px;
    border-radius: 3px;
    margin-bottom: 3px;
    color: white;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
</style>
""", unsafe_allow_html=True)

# 2. 캘린더 UI 렌더링
# 제목 표시
st.markdown(f'<div class="calendar-title">{month_str} 방배정</div>', unsafe_allow_html=True)

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
            date_part = date.split(" (")[0]
            time_slot_match = date.split(" (")
            time_slot = f"({time_slot_match[1]})" if len(time_slot_match) > 1 else ""
            
            dt = datetime.datetime.strptime(date_part, "%Y-%m-%d")
            month_num = dt.month
            day = dt.day
            weekday_name = weekday_map[dt.weekday()]
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

st.markdown("**🟢 방배정 요청사항 입력**")
add_col1, add_col2, add_col3 = st.columns([2, 3, 1])

with add_col1:
    분류 = st.multiselect("요청 분류", 요청분류, key="category_select")
with add_col2:
    available_dates = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"} # 월요일=0, 금요일=4
    
    # 월의 시작일부터 마지막 날까지 모든 날짜를 순회
    for day in pd.date_range(month_start, month_end):
        # 날짜가 평일(월~금)인 경우에만 목록에 추가
        if day.weekday() in weekday_map:
            weekday_name = weekday_map[day.weekday()]
            display_date = f"{day.month}월 {day.day}일({weekday_name})"
            save_date = day.strftime("%Y-%m-%d")
            
            # 오전과 오후 선택지를 모두 추가
            available_dates.append((f"{display_date} 오전", save_date, "오전"))
            available_dates.append((f"{display_date} 오후", save_date, "오후"))

    date_options = [date_str for date_str, _, _ in available_dates]
    date_values = [(save_date, time_slot) for _, save_date, time_slot in available_dates]
    날짜 = st.multiselect("요청 일자", date_options, key="date_multiselect")

def format_date_to_korean(date_str, period):
    try:
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return f"{date_obj.strftime('%Y-%m-%d')} ({period})"
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"날짜 형식 변환 중 오류 발생: {str(e)}")
        return date_str

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
    try:
        if 날짜정보 and 분류:
            with st.spinner("요청사항을 추가 중입니다..."):
                sheet = st.session_state["sheet"] # <-- 이렇게 수정하세요
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
                    
                    try:
                        worksheet2.clear()
                        worksheet2.update([df_room_request_temp.columns.tolist()] + df_room_request_temp.astype(str).values.tolist())
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
                        st.stop()
                    
                    st.session_state["df_room_request"] = df_room_request_temp
                    st.session_state["df_user_room_request"] = df_room_request_temp[df_room_request_temp["이름"] == name].copy()
                    st.success("요청이 성공적으로 기록되었습니다.")
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.info("ℹ️ 이미 존재하는 요청사항입니다.")
        else:
            st.warning("요청 분류와 날짜 정보를 올바르게 입력해주세요.")
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"요청 추가 중 오류 발생: {str(e)}")
        st.stop()

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
