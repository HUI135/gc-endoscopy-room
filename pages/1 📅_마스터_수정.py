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

st.set_page_config(page_title="마스터 수정", page_icon="📅", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# ✅ Gspread 클라이언트
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
        st.error(f"Google Sheets 클라이언트 초기화 중 오류 발생: {str(e)}")
        st.stop()

# ✅ 스프레드시트 ID 추출
def extract_spreadsheet_id(url):
    try:
        return url.split("/d/")[1].split("/")[0]
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"스프레드시트 ID 추출 중 오류 발생: {str(e)}")
        st.stop()

# ✅ 데이터 로드 함수 (캐싱 적용)
@st.cache_data(show_spinner=False)
def load_master_data_page1(_gc, url):
    try:
        sheet = _gc.open_by_url(url)
        worksheet_master = sheet.worksheet("마스터")
        return pd.DataFrame(worksheet_master.get_all_records())
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (마스터 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"마스터 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

# ✅ 데이터 새로고침 함수
def refresh_data():
    try:
        sheet = gc.open_by_url(url)
        worksheet1 = sheet.worksheet("마스터")
        data = worksheet1.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (데이터 새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"데이터 새로고침 중 오류 발생: {str(e)}")
        st.stop()

# ✅ 캘린더 이벤트 생성 함수
def generate_calendar_events(df_user_master, year, month, week_labels):
    master_data = {}
    요일리스트 = ["월", "화", "수", "목", "금"]
    
    # "매주" 설정이 있는지 확인
    has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
    if has_weekly:
        weekly_df = df_user_master[df_user_master["주차"] == "매주"]
        # 요일별 근무여부 딕셔너리 생성
        weekly_schedule = weekly_df.set_index("요일")["근무여부"].to_dict()
        # 누락된 요일이 있다면 "근무없음"으로 채우기
        for 요일 in 요일리스트:
            if 요일 not in weekly_schedule:
                weekly_schedule[요일] = "근무없음"
        # 모든 주에 대해 동일한 "매주" 스케줄 적용
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

    # 첫 번째 일요일 찾기
    first_sunday = None
    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        if date_obj.weekday() == 6:  # 일요일
            first_sunday = day
            break

    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()
        if weekday in weekday_map:
            day_name = weekday_map[weekday]
            # 주차 계산: 첫 번째 일요일 기준
            if first_sunday and day < first_sunday:
                week_num = 0  # 첫 번째 일요일 이전은 1주차
            elif first_sunday:
                week_num = (day - first_sunday) // 7 + 1  # 첫 번째 일요일 이후 주차 계산
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
                    "color": status_colors.get(status, "#E0E0E0")
                })
    return events

# ✅ 데이터 로드 및 세션 상태 초기화
try:
    url = st.secrets["google_sheet"]["url"]
    gc = get_gspread_client()
    name = st.session_state.get("name", None)
    if name is None:
        st.error("⚠️ 사용자 이름이 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
        st.stop()

    if "df_master" not in st.session_state:
        st.session_state["df_master"] = refresh_data()
    df_master = st.session_state["df_master"]
    df_user_master = df_master[df_master["이름"] == name]
except NameError as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()
except gspread.exceptions.APIError as e:
    st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
    st.error(f"Google Sheets API 오류 (초기 설정): {str(e)}")
    st.stop()
except Exception as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()

# ✅ 이름이 마스터 시트에 없으면 초기 데이터 추가
if df_user_master.empty:
    try:
        st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
        initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
        initial_df = pd.DataFrame(initial_rows)
        initial_df["요일"] = pd.Categorical(initial_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        initial_df = initial_df.sort_values(by=["이름", "주차", "요일"])
        df_master = pd.concat([df_master, initial_df], ignore_index=True)
        df_user_master = initial_df
        sheet = gc.open_by_url(url)
        worksheet1 = sheet.worksheet("마스터")
        try:
            worksheet1.clear()
            worksheet1.update([df_master.columns.values.tolist()] + df_master.values.tolist())
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (초기 데이터 업데이트): {str(e)}")
            st.stop()
        st.session_state["df_master"] = df_master
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (초기 데이터 추가): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 추가 중 오류 발생: {str(e)}")
        st.stop()

# ✅ 월 정보 및 주차 리스트
근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
요일리스트 = ["월", "화", "수", "목", "금"]
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()
year, month = today.year, today.month
_, last_day = calendar.monthrange(today.year, today.month)
dates = pd.date_range(start=today.replace(day=1), end=today.replace(day=last_day))
week_nums = sorted(set(d.isocalendar()[1] for d in dates))
month_str = today.strftime("%Y년 %-m월")
has_weekly = "매주" in df_user_master["주차"].values if not df_user_master.empty else False
week_labels = [f"{i+1}주" for i in range(len(week_nums))]  # 항상 주차 수에 맞게 설정

# 캘린더 이벤트 생성 (당월 반영)
events = generate_calendar_events(df_user_master, year, month, week_labels)

calendar_options = {
    "initialView": "dayGridMonth",
    "initialDate": today.strftime("%Y-%m-%d"),
    "editable": False,
    "selectable": False,
    "eventDisplay": "block",
    "dayHeaderFormat": {"weekday": "short"},
    "themeSystem": "bootstrap",
    "height": 600,
    "headerToolbar": {"left": "", "center": "", "right": ""},
    "showNonCurrentDates": True,
    "fixedWeekCount": False
}

st.header(f"📅 {name} 님의 마스터 스케줄", divider='rainbow')

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()
            st.session_state["df_master"] = load_master_data_page1(gc, url)
            st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
        st.success("데이터가 새로고침되었습니다.")
        time.sleep(1)
        st.rerun()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()

# ✅ "매주" & "근무없음" 여부 확인
all_no_work = False
if has_weekly and not df_user_master.empty:
    all_no_work = df_user_master["근무여부"].eq("근무없음").all()

# ✅ "매주"로 변환 로직
if not df_user_master.empty and not has_weekly:
    try:
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
            try:
                worksheet1.clear()
                worksheet1.update([df_master.columns.values.tolist()] + df_master.values.tolist())
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (매주 변환): {str(e)}")
                st.stop()
            st.session_state["df_master"] = df_master
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (매주 변환): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"매주 변환 중 오류 발생: {str(e)}")
        st.stop()

# 캘린더 이벤트 생성 (당월 반영)
events = generate_calendar_events(df_user_master, year, month, week_labels)

st_calendar(events=events, options=calendar_options)

# ✅ 캘린더 섹션
st.divider()
st.markdown(f"<h6 style='font-weight:bold;'>📅 마스터 스케줄 편집</h6>", unsafe_allow_html=True)

# 🌙 월 단위 일괄 설정
with st.expander("📅 월 단위로 일괄 설정"):
    default_bulk = {요일: "근무없음" for 요일 in 요일리스트}
    if has_weekly and all_no_work:
        st.info("마스터 입력이 필요합니다.")
    elif has_weekly and not all_no_work:
        weekly_df = df_user_master[df_user_master["주차"] == "매주"]
        default_bulk = weekly_df.set_index("요일")["근무여부"].to_dict()
    else:
        st.warning("현재 주차별 근무 일정이 다릅니다. 월 단위로 초기화하려면 내용을 입력하세요.")

    col1, col2, col3, col4, col5 = st.columns(5)
    월값 = col1.selectbox("월", 근무옵션, index=근무옵션.index(default_bulk.get("월", "근무없음")), key="월_bulk")
    화값 = col2.selectbox("화", 근무옵션, index=근무옵션.index(default_bulk.get("화", "근무없음")), key="화_bulk")
    수값 = col3.selectbox("수", 근무옵션, index=근무옵션.index(default_bulk.get("수", "근무없음")), key="수_bulk")
    목값 = col4.selectbox("목", 근무옵션, index=근무옵션.index(default_bulk.get("목", "근무없음")), key="목_bulk")
    금값 = col5.selectbox("금", 근무옵션, index=근무옵션.index(default_bulk.get("금", "근무없음")), key="금_bulk")

    if st.button("💾 월 단위 저장", key="save_monthly"):
        try:
            rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}[요일]} for 요일 in 요일리스트]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_master = df_master[df_master["이름"] != name]
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            try:
                worksheet1.clear()
                worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (월 단위 저장): {str(e)}")
                st.stop()
            
            st.session_state["df_master"] = df_result
            df_user_master = df_result[df_result["이름"] == name]  # df_user_master 즉시 업데이트
            st.success("편집하신 내용을 저장하였습니다.")
            time.sleep(1.5)
            st.cache_data.clear()  # 캐시 무효화
            st.session_state["df_master"] = load_master_data_page1(gc, url)
            st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
            st.rerun()  # 페이지 새로고침
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (월 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"월 단위 저장 중 오류 발생: {str(e)}")
            st.stop()

# 📅 주 단위로 설정
with st.expander("📅 주 단위로 설정"):
    st.markdown("**주 단위로 근무 여부가 다른 경우 아래 내용들을 입력해주세요.**")
    week_labels = [f"{i+1}주" for i in range(len(week_nums))]
    
    master_data = {}
    for week in week_labels:
        week_df = df_user_master[df_user_master["주차"] == week]
        if not week_df.empty:
            master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()
        else:
            if "매주" in df_user_master["주차"].values:
                weekly_df = df_user_master[df_user_master["주차"] == "매주"]
                master_data[week] = weekly_df.set_index("요일")["근무여부"].to_dict()
            else:
                master_data[week] = {요일: "근무없음" for 요일 in 요일리스트}

    for week in week_labels:
        st.markdown(f"**🗓 {week}**")
        col1, col2, col3, col4, col5 = st.columns(5)
        master_data[week]["월"] = col1.selectbox(f"월", 근무옵션, index=근무옵션.index(master_data[week]["월"]), key=f"{week}_월")
        master_data[week]["화"] = col2.selectbox(f"화", 근무옵션, index=근무옵션.index(master_data[week]["화"]), key=f"{week}_화")
        master_data[week]["수"] = col3.selectbox(f"수", 근무옵션, index=근무옵션.index(master_data[week]["수"]), key=f"{week}_수")
        master_data[week]["목"] = col4.selectbox(f"목", 근무옵션, index=근무옵션.index(master_data[week]["목"]), key=f"{week}_목")
        master_data[week]["금"] = col5.selectbox(f"금", 근무옵션, index=근무옵션.index(master_data[week]["금"]), key=f"{week}_금")

    if st.button("💾 주 단위 저장", key="save_weekly"):
        try:
            rows = [{"이름": name, "주차": week, "요일": 요일, "근무여부": 근무} for week, days in master_data.items() for 요일, 근무 in days.items()]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_master = df_master[df_master["이름"] != name]
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            try:
                worksheet1.clear()
                worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (주 단위 저장): {str(e)}")
                st.stop()
            
            st.session_state["df_master"] = df_result
            df_user_master = df_result[df_result["이름"] == name]  # df_user_master 즉시 업데이트
            st.success("편집하신 내용을 저장하였습니다.")
            time.sleep(1.5)
            st.session_state["df_master"] = load_master_data_page1(gc, url)
            st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()
            st.rerun()  # 페이지 새로고침
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (주 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"주 단위 저장 중 오류 발생: {str(e)}")
            st.stop()