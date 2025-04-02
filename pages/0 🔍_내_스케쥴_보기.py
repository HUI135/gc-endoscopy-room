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

# 🔒 로그인 체크
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

if st.session_state.get("login_success", False):
    st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")

    if st.sidebar.button("로그아웃"):
        st.session_state["login_success"] = False
        st.session_state["is_admin"] = False
        st.session_state["is_admin_authenticated"] = False
        st.session_state["employee_id"] = None
        st.session_state["name"] = None
        st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
        time.sleep(5)
        st.rerun()

    # ✅ 사용자 정보
    name = st.session_state["name"]

    # ✅ Gspread 클라이언트
    def get_gspread_client():
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)

    url = st.secrets["google_sheet"]["url"]
    gc = get_gspread_client()
    sheet = gc.open_by_url(url)

    # ✅ 데이터 로드 함수 (캐싱 적용)
    @st.cache_data
    def refresh_data(sheet_name, _timestamp):
        try:
            worksheet = sheet.worksheet(sheet_name)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except WorksheetNotFound:
            return pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]) if sheet_name == "마스터" else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        except Exception as e:
            st.error(f"{sheet_name} 시트 로드 중 오류: {e}")
            return pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]) if sheet_name == "마스터" else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

    # ✅ 초기 데이터 로드 (세션 상태 활용)
    today = datetime.date.today()
    next_month = today.replace(day=1) + relativedelta(months=1)
    month_str = next_month.strftime("%Y년 %m월")
    if "master_df" not in st.session_state or "request_df" not in st.session_state:
        st.session_state["master_df"] = refresh_data("마스터", time.time())
        request_sheet_name = f"{month_str} 요청"
        st.session_state["request_df"] = refresh_data(request_sheet_name, time.time())
        # "요청" 시트가 비어 있으면 초기화
        if st.session_state["request_df"].empty:
            worksheet2 = sheet.add_worksheet(title=request_sheet_name, rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
            names_in_master = st.session_state["master_df"]["이름"].unique()
            new_rows = [[name, "요청 없음", ""] for name in names_in_master]
            worksheet2.append_rows(new_rows)
            st.session_state["request_df"] = refresh_data(request_sheet_name, time.time())

    df_master = st.session_state["master_df"]
    df_request = st.session_state["request_df"]
    df_user_master = df_master[df_master["이름"] == name].copy()
    df_user_request = df_request[df_request["이름"] == name].copy()

    # ✅ 월 정보
    근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
    요일리스트 = ["월", "화", "수", "목", "금"]
    year, month = next_month.year, next_month.month
    _, last_day = calendar.monthrange(year, month)
    week_labels = [f"{i+1}주차" for i in range(4)]

    # ✅ 마스터 스케쥴 캘린더
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name}님의 마스터 스케쥴</h6>", unsafe_allow_html=True)

    if df_user_master.empty:
        base = {요일: "근무없음" for 요일 in 요일리스트}
        master_data = {week: base.copy() for week in week_labels}
    elif df_user_master["주차"].eq("매주").all():
        base = df_user_master[df_user_master["주차"] == "매주"].set_index("요일")["근무여부"].to_dict()
        master_data = {week: base.copy() for week in week_labels}
    else:
        master_data = {}
        for week in week_labels:
            week_df = df_user_master[df_user_master["주차"] == week]
            master_data[week] = week_df.set_index("요일")["근무여부"].to_dict() if not week_df.empty else {요일: "근무없음" for 요일 in 요일리스트}

    events_master = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
    status_colors_master = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}

    for day in range(1, last_day + 1):
        date_obj = datetime.date(year, month, day)
        weekday = date_obj.weekday()
        if weekday in weekday_map:
            day_name = weekday_map[weekday]
            week_num = (day - 1) // 7
            if week_num >= len(week_labels):
                continue
            week = week_labels[week_num]
            status = master_data.get(week, {}).get(day_name, "근무없음")
            if status != "근무없음":
                events_master.append({
                    "title": status,
                    "start": date_obj.strftime("%Y-%m-%d"),
                    "end": date_obj.strftime("%Y-%m-%d"),
                    "color": status_colors_master.get(status, "#E0E0E0")
                })

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
        "showNonCurrentDates": True,
        "fixedWeekCount": False
    }

    st_calendar(events=events_master, options=calendar_options)

    # ✅ 요청사항 캘린더
    st.divider()
    st.markdown(f"<h6 style='font-weight:bold;'>🙋‍♂️ {name} 님의 {month_str} 요청사항</h6>", unsafe_allow_html=True)

    if df_user_request.empty or (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
        st.info("📍 당월 요청사항 없음")
    else:
        status_colors_request = {
            "휴가": "#48A6A7",
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

        events_request = []
        for _, row in df_user_request.iterrows():
            분류 = row["분류"]
            날짜정보 = row["날짜정보"]
            if not 날짜정보 or 분류 == "요청 없음":
                continue
            if "~" in 날짜정보:
                시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
                시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
                종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
                events_request.append({
                    "title": label_map.get(분류, 분류),
                    "start": 시작.strftime("%Y-%m-%d"),
                    "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                    "color": status_colors_request.get(분류, "#E0E0E0")
                })
            else:
                for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                    try:
                        dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                        events_request.append({
                            "title": label_map.get(분류, 분류),
                            "start": dt.strftime("%Y-%m-%d"),
                            "end": dt.strftime("%Y-%m-%d"),
                            "color": status_colors_request.get(분류, "#E0E0E0")
                        })
                    except:
                        continue

        st_calendar(events=events_request, options=calendar_options)