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

# [수정] set_page_config를 menu()보다 먼저 호출
st.set_page_config(page_title="마스터 스케줄 수정", page_icon="📋", layout="wide")
menu.menu()

# 로그인 체크
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# --- 이하는 페이지의 메인 로직 ---

name = st.session_state.get("name", None)

# ✅ Gspread 클라이언트
@st.cache_resource
def get_gspread_client_page1():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# ✅ 데이터 로드 함수
@st.cache_data(ttl=300)
def load_master_data_page1(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    df = pd.DataFrame(worksheet_master.get_all_records())
    # 데이터 로드 시 공백 제거
    if "근무여부" in df.columns:
        df["근무여부"] = df["근무여부"].astype(str).str.strip()
    return df

gc = get_gspread_client_page1()
url = st.secrets["google_sheet"]["url"]

# ✅ 데이터 로드 및 세션 상태 초기화
if "df_master" not in st.session_state:
    st.session_state["df_master"] = load_master_data_page1(gc, url)
df_master = st.session_state["df_master"]
df_user_master = df_master[df_master["이름"] == name].copy()

# ✅ 월 정보
근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
요일리스트 = ["월", "화", "수", "목", "금"]
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()
next_month = today.replace(day=1) + relativedelta(months=1)
year, month = next_month.year, next_month.month
_, last_day = calendar.monthrange(year, month)
dates = pd.date_range(start=next_month, end=next_month.replace(day=last_day))
week_nums = sorted(set(d.isocalendar()[1] for d in dates))
week_labels = [f"{i+1}주" for i in range(len(week_nums))]

# ✅ 이름이 마스터 시트에 없으면 초기 데이터 추가
if df_user_master.empty and name:
    st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
    initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in 요일리스트]
    initial_df = pd.DataFrame(initial_rows)
    
    df_master = pd.concat([df_master, initial_df], ignore_index=True)
    df_master["요일"] = pd.Categorical(df_master["요일"], categories=요일리스트, ordered=True)
    df_master = df_master.sort_values(by=["이름", "주차", "요일"])
    
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터")
    worksheet1.clear()
    worksheet1.update([df_master.columns.values.tolist()] + df_master.values.tolist())
    
    st.session_state["df_master"] = df_master
    df_user_master = initial_df.copy()

# --- UI 시작 ---
st.header(f"📅 {name} 님의 마스터 스케줄", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.rerun()

# 캘린더 이벤트 생성 함수
def generate_calendar_events(df, year, month):
    events = []
    status_colors = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}
    first_day = datetime.date(year, month, 1)
    first_sunday_offset = (6 - first_day.weekday()) % 7

    for _, row in df.iterrows():
        주차, 요일, 근무여부 = row["주차"], row["요일"], str(row["근무여부"]).strip()
        if 근무여부 == "근무없음": continue
        day_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
        if 요일 not in day_map: continue
        target_weekday = day_map[요일]
        
        for day_num in range(1, last_day + 1):
            current_date = datetime.date(year, month, day_num)
            week_of_month = (current_date.day + first_sunday_offset - 1) // 7
            is_correct_week = (주차 == "매주") or (주차 == f"{week_of_month+1}주")
            if current_date.weekday() == target_weekday and is_correct_week:
                events.append({"title": 근무여부, "start": current_date.strftime("%Y-%m-%d"), "color": status_colors.get(근무여부, "#E0E0E0")})
    return events

# 캘린더 표시
events = generate_calendar_events(df_user_master, year, month)
st_calendar(events=events, options={"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "height": 500, "headerToolbar": {"left": "", "center": "", "right": ""}})

st.divider()
st.markdown(f"<h6 style='font-weight:bold;'>📅 마스터 스케쥴 편집</h6>", unsafe_allow_html=True)

def save_data(df_to_save):
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터")
    df_master_others = st.session_state["df_master"][st.session_state["df_master"]["이름"] != name]
    df_result = pd.concat([df_master_others, df_to_save], ignore_index=True)
    df_result["요일"] = pd.Categorical(df_result["요일"], categories=요일리스트, ordered=True)
    df_result = df_result.sort_values(by=["이름", "주차", "요일"])
    worksheet1.clear()
    worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
    st.cache_data.clear()
    st.success("편집하신 내용을 저장하였습니다 ✅")
    st.rerun()

# [수정] 모든 selectbox를 radio로 변경
with st.expander("📅 월 단위로 일괄 설정"):
    has_weekly = "매주" in df_user_master["주차"].values
    default_bulk = {day: "근무없음" for day in 요일리스트}
    if has_weekly:
        default_bulk.update(df_user_master[df_user_master["주차"] == "매주"].set_index("요일")["근무여부"].to_dict())

    cols = st.columns(5)
    new_values = {}
    for i, day in enumerate(요일리스트):
        with cols[i]:
            st.markdown(f"**{day}**")
            default_val = default_bulk.get(day, "근무없음")
            default_idx = 근무옵션.index(default_val) if default_val in 근무옵션 else 3
            new_values[day] = st.radio(f"bulk_{day}_val", 근무옵션, index=default_idx, key=f"bulk_{day}", horizontal=True, label_visibility="hidden")

    if st.button("💾 월 단위 저장", key="save_monthly"):
        rows = [{"이름": name, "주차": "매주", "요일": day, "근무여부": new_values[day]} for day in 요일리스트]
        save_data(pd.DataFrame(rows))

with st.expander("📅 주 단위로 설정", expanded=not has_weekly):
    st.markdown("**주 단위로 근무 여부가 다른 경우 아래 내용들을 입력해주세요.**")
    
    master_data = {}
    for week in week_labels:
        master_data[week] = {day: "근무없음" for day in 요일리스트}
        if has_weekly:
            master_data[week] = df_user_master[df_user_master["주차"] == "매주"].set_index("요일")["근무여부"].to_dict()
        else:
            week_df = df_user_master[df_user_master["주차"] == week]
            if not week_df.empty:
                master_data[week].update(week_df.set_index("요일")["근무여부"].to_dict())

    for week in week_labels:
        st.markdown(f"**🗓 {week}**")
        cols = st.columns(5)
        for i, day in enumerate(요일리스트):
            with cols[i]:
                st.markdown(f"**{day}**")
                default_val = master_data[week].get(day, "근무없음")
                default_idx = 근무옵션.index(default_val) if default_val in 근무옵션 else 3
                master_data[week][day] = st.radio(f"{week}_{day}_val", 근무옵션, index=default_idx, key=f"{week}_{day}", horizontal=True, label_visibility="hidden")

    if st.button("💾 주 단위 저장", key="save_weekly"):
        rows = [{"이름": name, "주차": week, "요일": day, "근무여부": status} for week, days in master_data.items() for day, status in days.items()]
        save_data(pd.DataFrame(rows))