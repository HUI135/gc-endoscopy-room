import numpy as np
import streamlit as st
import pandas as pd
import calendar
import datetime
import time
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar  # 이름 바꿔주기
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import streamlit.components.v1 as components

# 🔒 로그인 체크
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

if st.session_state.get("login_success", False):
    st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")

    if st.sidebar.button("로그아웃"):
        # 세션 상태 초기화
        st.session_state["login_success"] = False
        st.session_state["is_admin"] = False
        st.session_state["is_admin_authenticated"] = False
        st.session_state["employee_id"] = None
        st.session_state["name"] = None
        st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
        time.sleep(5)
        # Home.py로 이동 (메인 페이지)
        st.rerun()

    name = st.session_state.get("name", None)

    # st.header(f"🔍 내 스케쥴 보기", divider='rainbow')

    # ✅ 사용자 인증 정보 가져오기
    def get_gspread_client():
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        # ✨ JSON처럼 강제 파싱 (줄바꿈 처리 문제 해결)
        service_account_info = dict(st.secrets["gspread"])
        # 🟢 private_key 줄바꿈 복원
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")

        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    
    url = st.secrets["google_sheet"]["url"]
    gc = get_gspread_client()
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터") 

    # ✅ 로그인 사용자 정보
    employee_id = st.session_state.get("employee_id", "00000")

    # ✅ 기존 스케줄 불러오기
    try:
        data = worksheet1.get_all_records()
        df_all = pd.DataFrame(data)
        df_user = df_all[df_all["사번"] == employee_id]
    except:
        df_user = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])

    # ✅ 월 정보
    근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
    요일리스트 = ["월", "화", "수", "목", "금"]
    today = pd.Timestamp.today()
    next_month = today.replace(day=1) + pd.DateOffset(months=1)
    _, last_day = calendar.monthrange(next_month.year, next_month.month)
    dates = pd.date_range(start=next_month, end=next_month.replace(day=last_day))
    week_nums = sorted(set(d.isocalendar()[1] for d in dates))
    month_str = next_month.strftime("%Y년 %m월")

    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name}님의 {month_str} 마스터 스케쥴</h6>", unsafe_allow_html=True)

    def load_schedule():
        try:
            data = worksheet1.get_all_records()
            df_all = pd.DataFrame(data)
            df_user = df_all[df_all["이름"] == name].copy()
        except:
            df_all = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            df_user = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        return df_all, df_user

    name = st.session_state["name"]

    today = datetime.date.today()
    next_month = today.replace(day=1) + relativedelta(months=1)
    year, month = next_month.year, next_month.month
    month_str = next_month.strftime("%Y년 %m월")

    df_all, df_user = load_schedule()
    week_labels = [f"{i+1}주차" for i in range(4)]

    # 2️⃣ master_data 생성
    if df_user.empty:
        base = {요일: "근무없음" for 요일 in ["월", "화", "수", "목", "금"]}
        master_data = {week: base.copy() for week in week_labels}
    elif df_user["주차"].eq("매주").all():
        base = df_user[df_user["주차"] == "매주"].set_index("요일")["근무여부"].to_dict()
        master_data = {week: base.copy() for week in week_labels}
    else:
        master_data = {}
        for week in week_labels:
            week_df = df_user[df_user["주차"] == week]
            if week_df.empty:
                master_data[week] = {요일: "근무없음" for 요일 in ["월", "화", "수", "목", "금"]}
            else:
                master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()

    # 3️⃣ 다음달 날짜별 events 생성
    events = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
    _, last_day = calendar.monthrange(year, month)

    status_colors = {
        "오전": "#48A6A7",
        "오후": "#5F99AE",
        "오전 & 오후": "#F38C79",
    }

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
                events.append({
                    "title": f"{status}",
                    "start": date_obj.strftime("%Y-%m-%d"),
                    "end": date_obj.strftime("%Y-%m-%d"),
                    "color": status_colors.get(status, "#E0E0E0")
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

    st_calendar(events=events, options=calendar_options)

    calendar_preview = pd.DataFrame.from_dict(master_data, orient="index")
    calendar_preview.index.name = "주차"
    calendar_preview.reset_index(inplace=True)

    st.divider()
    st.markdown(f"<h6 style='font-weight:bold;'>🙋‍♂️ {name} 님의 {month_str} 요청사항</h6>", unsafe_allow_html=True)

    try:
        worksheet_master = sheet.worksheet("마스터")
        data_master = worksheet_master.get_all_records()
        df_master = pd.DataFrame(data_master)
        names_in_master = df_master["이름"].unique()  # "이름" 열에서 유니크한 이름 목록

    except Exception as e:
        st.error(f"마스터 시트를 불러오는 데 문제가 발생했습니다: {e}")
        st.stop()  # 이후 코드를 실행하지 않음

    # ✅ "요청사항" 시트 불러오기
    try:
        worksheet2 = sheet.worksheet(f"{month_str} 요청")
    except WorksheetNotFound:
        worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
        worksheet2.append_row(["이름", "분류", "날짜정보"])  # 헤더 추가
        st.write(names_in_master)
        
        # 새로운 행을 "요청" 시트에 추가
        new_rows = [{"이름": name, "분류": '요청 없음', "날짜정보": ''} for name in names_in_master]
        
        # 각 새로운 행을 시트에 추가
        for row in new_rows:
            worksheet2.append_row([row["이름"], row["분류"], row["날짜정보"]])

    # ✅ 기존 스케줄 불러오기
    try:
        data = worksheet2.get_all_records()
        if not data:  # 데이터가 없으면 빈 데이터프레임 생성
            df_all = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
            # st.warning(f"아직까지 {month_str}에 작성된 요청사항이 없습니다.")
            # st.stop()  # 데이터가 없으면 이후 코드를 실행하지 않음
        else:
            df_all = pd.DataFrame(data)
    except Exception as e:
        # 예외 발생 시 처리
        df_all = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.warning(f"데이터를 불러오는 데 문제가 발생했습니다: {e}")
        st.stop()  # 이후 코드를 실행하지 않음

    df_user = df_all[df_all["이름"] == name].copy()

    if df_user.empty or (df_user["분류"].nunique() == 1 and df_user["분류"].unique()[0] == "요청 없음"):
        st.info("📍 당월 요청사항 없음")
    else:
        # 익월 범위 지정
        today = datetime.date.today()
        next_month = today.replace(day=1) + relativedelta(months=1)
        year, month = next_month.year, next_month.month
        month_str = next_month.strftime("%Y년 %m월")
        _, last_day = calendar.monthrange(year, month)

        # 2️⃣ events 생성
        status_colors = {
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
            "보충 불가(오후)": "보충🚫(오전)",
            "꼭 근무(오전)": "꼭근무(오전)",
            "꼭 근무(오후)": "꼭근무(오후)",
        }

        events = []
        for _, row in df_user.iterrows():
            분류 = row["분류"]
            날짜정보 = row["날짜정보"]

            if not 날짜정보 or 분류 == "요청 없음":
                continue

            if "~" in 날짜정보:
                # 기간 선택: "2025-04-01 ~ 2025-04-03"
                시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
                시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
                종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
                events.append({
                    "title": label_map.get(분류, 분류),
                    "start": 시작.strftime("%Y-%m-%d"),
                    "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                    "color": status_colors.get(분류, "#E0E0E0")
                })
            else:
                # 단일 혹은 쉼표로 나열된 날짜들
                for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                    try:
                        dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                        events.append({
                            "title": label_map.get(분류, 분류),
                            "start": dt.strftime("%Y-%m-%d"),
                            "end": dt.strftime("%Y-%m-%d"),
                            "color": status_colors.get(분류, "#E0E0E0")
                        })
                    except:
                        continue

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

        st_calendar(events=events, options=calendar_options)