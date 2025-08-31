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
        df_master = load_master_data_page1(gc, url)
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
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            if not update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist()):
                st.error("마스터 시트 초기 데이터 업데이트 실패")
                st.stop()
        
        # 최종 데이터를 세션 상태에 저장합니다.
        st.session_state["df_master"] = df_master
        st.session_state["df_user_master"] = df_master[df_master["이름"] == name].copy()
        st.session_state["master_page_initialized"] = True

    except (APIError, Exception) as e:
        st.error(f"데이터 초기화 중 오류가 발생했습니다: {e}")
        st.stop()

# 캘린더 이벤트 생성 함수
def generate_calendar_events(df_user_master, year, month, week_labels):
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금"]
    
    has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
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

    first_sunday = next((day for day in range(1, last_day + 1) if datetime.date(year, month, day).weekday() == 6), None)
    
    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()
        if weekday in weekday_map:
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
    return events

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

# 캘린더 이벤트 생성
events = generate_calendar_events(df_user_master, year, month, week_labels)

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

st.error("📅 [마스터 수정] 기능은 반드시 강승주 팀장님의 확인 후에 수정해 주시기 바랍니다.")

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

# 캘린더 표시
st_calendar(events=events, options=calendar_options)

# 마스터 스케줄 편집
st.divider()
st.subheader("📅 마스터 스케줄 편집")
st.write("- 월 단위 또는 주 단위로 본인의 마스터 스케줄을 수정할 수 있습니다.")

# 월 단위 일괄 설정
with st.expander("📅 월 단위로 일괄 설정"):
    has_weekly_specific = any(w in df_user_master["주차"].values for w in week_labels)
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    default_bulk = {}
    
    if has_weekly_specific:
        for day in 요일리스트:
            day_values = []
            for week in week_labels:
                week_df = df_user_master[df_user_master["주차"] == week]
                day_specific = week_df[week_df["요일"] == day]
                if not day_specific.empty:
                    day_values.append(day_specific.iloc[0]["근무여부"])
                elif not every_week_df.empty:
                    day_every = every_week_df[every_week_df["요일"] == day]
                    day_values.append(day_every.iloc[0]["근무여부"] if not day_every.empty else "근무없음")
                else:
                    day_values.append("근무없음")
            if day_values and all(v == day_values[0] for v in day_values):
                default_bulk[day] = day_values[0]
            else:
                most_common = Counter(day_values).most_common(1)[0][0]
                default_bulk[day] = most_common
    elif has_weekly:
        default_bulk = every_week_df.set_index("요일")["근무여부"].to_dict()
    for day in 요일리스트:
        if day not in default_bulk:
            default_bulk[day] = "근무없음"

    if has_weekly and all(df_user_master["근무여부"] == "근무없음"):
        st.info("마스터 입력이 필요합니다.")
    elif has_weekly_specific:
        st.warning("현재 주차별 근무 일정이 다릅니다. 월 단위로 초기화하려면 내용을 입력하세요.")

    col1, col2, col3, col4, col5 = st.columns(5)
    월값 = col1.selectbox("월", 근무옵션, index=근무옵션.index(default_bulk.get("월", "근무없음")), key=f"월_bulk_{name}")
    화값 = col2.selectbox("화", 근무옵션, index=근무옵션.index(default_bulk.get("화", "근무없음")), key=f"화_bulk_{name}")
    수값 = col3.selectbox("수", 근무옵션, index=근무옵션.index(default_bulk.get("수", "근무없음")), key=f"수_bulk_{name}")
    목값 = col4.selectbox("목", 근무옵션, index=근무옵션.index(default_bulk.get("목", "근무없음")), key=f"목_bulk_{name}")
    금값 = col5.selectbox("금", 근무옵션, index=근무옵션.index(default_bulk.get("금", "근무없음")), key=f"금_bulk_{name}")

    if st.button("💾 월 단위 저장", key="save_monthly"):
        try:
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}[요일]} for 요일 in 요일리스트]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_master = df_master[df_master["이름"] != name]
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            if update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist()):
                st.session_state["df_master"] = df_result
                st.session_state["df_user_master"] = df_result[df_result["이름"] == name].copy()
                st.success("월 단위 수정사항이 저장되었습니다.")
                time.sleep(1.5)
                # st.cache_data.clear()
                st.rerun()
            else:
                st.error("마스터 시트 저장 실패")
                st.stop()
        except APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (월 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"월 단위 저장 중 오류 발생: {str(e)}")
            st.stop()

# 주 단위 설정
with st.expander("📅 주 단위로 설정"):
    st.markdown("**주 단위로 근무 여부가 다른 경우 아래 내용들을 입력해주세요.**")
    master_data = {}
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

    for week in week_labels:
        st.markdown(f"**🗓 {week}**")
        col1, col2, col3, col4, col5 = st.columns(5)
        master_data[week]["월"] = col1.selectbox(f"월", 근무옵션, index=근무옵션.index(master_data[week]["월"]), key=f"{week}_월_{name}")
        master_data[week]["화"] = col2.selectbox(f"화", 근무옵션, index=근무옵션.index(master_data[week]["화"]), key=f"{week}_화_{name}")
        master_data[week]["수"] = col3.selectbox(f"수", 근무옵션, index=근무옵션.index(master_data[week]["수"]), key=f"{week}_수_{name}")
        master_data[week]["목"] = col4.selectbox(f"목", 근무옵션, index=근무옵션.index(master_data[week]["목"]), key=f"{week}_목_{name}")
        master_data[week]["금"] = col5.selectbox(f"금", 근무옵션, index=근무옵션.index(master_data[week]["금"]), key=f"{week}_금_{name}")

    if st.button("💾 주 단위 저장", key="save_weekly"):
        try:
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            rows = []
            for 요일 in 요일리스트:
                week_shifts = [master_data[week][요일] for week in week_labels]
                if all(shift == week_shifts[0] for shift in week_shifts):
                    rows.append({"이름": name, "주차": "매주", "요일": 요일, "근무여부": week_shifts[0]})
                else:
                    for week in week_labels:
                        rows.append({"이름": name, "주차": week, "요일": 요일, "근무여부": master_data[week][요일]})
            updated_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_master = df_master[df_master["이름"] != name]
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            if update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist()):
                st.session_state["df_master"] = df_result
                st.session_state["df_user_master"] = df_result[df_result["이름"] == name].copy()
                st.success("주 단위 수정사항이 저장되었습니다.")
                time.sleep(1.5)
                # st.cache_data.clear()
                st.rerun()
            else:
                st.error("마스터 시트 저장 실패")
                st.stop()
        except APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (주 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"주 단위 저장 중 오류 발생: {str(e)}")
            st.stop()