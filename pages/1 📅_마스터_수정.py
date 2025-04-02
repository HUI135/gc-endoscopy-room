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

    name = st.session_state.get("name", None)

    # ✅ 사용자 인증 정보 가져오기
    def get_gspread_client():
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    
    url = st.secrets["google_sheet"]["url"]
    gc = get_gspread_client()
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터")

    # ✅ 데이터 새로고침 함수 (캐싱 적용)
    @st.cache_data
    def refresh_data(_timestamp):
        try:
            data = worksheet1.get_all_records()
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"데이터 로드 중 오류 발생: {e}")
            return pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])

    # ✅ 초기 데이터 로드 (세션 상태 활용)
    if "df_all" not in st.session_state or "last_updated" not in st.session_state:
        st.session_state["df_all"] = refresh_data(time.time())
        st.session_state["last_updated"] = time.time()
    df_all = st.session_state["df_all"]
    df_user = df_all[df_all["이름"] == name]

    # ✅ 이름이 마스터 시트에 없으면 초기 데이터 추가
    if df_user.empty:
        st.info(f"{name} 님의 마스터 데이터가 존재하지 않습니다. 초기 데이터를 추가합니다.")
        initial_rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": "근무없음"} for 요일 in ["월", "화", "수", "목", "금"]]
        initial_df = pd.DataFrame(initial_rows)
        initial_df["요일"] = pd.Categorical(initial_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        initial_df = initial_df.sort_values(by=["이름", "주차", "요일"])
        df_all = pd.concat([df_all, initial_df], ignore_index=True)
        df_user = initial_df
        worksheet1.clear()
        worksheet1.update([df_all.columns.values.tolist()] + df_all.values.tolist())
        st.session_state["df_all"] = df_all
        st.session_state["last_updated"] = time.time()
        st.cache_data.clear()

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
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name} 님의 마스터 스케줄 편집</h6>", unsafe_allow_html=True)

    # ✅ 주차 리스트
    has_weekly = "매주" in df_user["주차"].values if not df_user.empty else False
    if has_weekly:
        week_labels = ["매주"]
    else:
        week_labels = [f"{i+1}주" for i in range(len(week_nums))]

    # ✅ "매주" & "근무없음" 여부 확인
    all_no_work = False
    if has_weekly and not df_user.empty:
        all_no_work = df_user["근무여부"].eq("근무없음").all()

    # ✅ "매주"로 변환 로직
    if not df_user.empty and not has_weekly:
        updated = False
        pivot_df = df_user.pivot(index="요일", columns="주차", values="근무여부")
        if pivot_df.apply(lambda x: x.nunique() == 1, axis=1).all():
            df_user["주차"] = "매주"
            df_user = df_user.drop_duplicates(subset=["이름", "주차", "요일"])
            updated = True
        if updated:
            df_user["요일"] = pd.Categorical(df_user["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_user = df_user.sort_values(by=["이름", "주차", "요일"])
            df_all = df_all[df_all["이름"] != name]
            df_all = pd.concat([df_all, df_user], ignore_index=True)
            df_all["요일"] = pd.Categorical(df_all["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_all = df_all.sort_values(by=["이름", "주차", "요일"])
            worksheet1.clear()
            worksheet1.update([df_all.columns.values.tolist()] + df_all.values.tolist())
            st.session_state["df_all"] = df_all
            st.session_state["last_updated"] = time.time()
            st.cache_data.clear()

    # 🌙 월 단위 일괄 설정
    with st.expander("📅 월 단위로 일괄 설정"):
        default_bulk = {요일: "근무없음" for 요일 in 요일리스트}
        if has_weekly and all_no_work:
            st.info("마스터 입력이 필요합니다.")
        elif has_weekly and not all_no_work:
            weekly_df = df_user[df_user["주차"] == "매주"]
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
            rows = [{"이름": name, "주차": "매주", "요일": 요일, "근무여부": {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}[요일]} for 요일 in 요일리스트]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_all = df_all[df_all["이름"] != name]
            df_result = pd.concat([df_all, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            worksheet1.clear()
            worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            st.session_state["df_all"] = df_result
            st.session_state["last_updated"] = time.time()
            st.cache_data.clear()
            st.success("편집하신 내용을 저장하였습니다 ✅")
            df_user = df_result[df_result["이름"] == name]

    # 📅 주 단위로 설정
    with st.expander("📅 주 단위로 설정"):
        st.markdown("**요일별로 근무 여부를 선택해주세요.**")
        week_labels = [f"{i+1}주" for i in range(len(week_nums))]
        
        master_data = {}
        for week in week_labels:
            week_df = df_user[df_user["주차"] == week]
            if not week_df.empty:
                master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()
            else:
                if "매주" in df_user["주차"].values:
                    weekly_df = df_user[df_user["주차"] == "매주"]
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
            rows = [{"이름": name, "주차": week, "요일": 요일, "근무여부": 근무} for week, days in master_data.items() for 요일, 근무 in days.items()]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            df_all = df_all[df_all["이름"] != name]
            df_result = pd.concat([df_all, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            worksheet1.clear()
            worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            st.session_state["df_all"] = df_result
            st.session_state["last_updated"] = time.time()
            st.cache_data.clear()
            st.success("편집하신 내용을 저장하였습니다 ✅")
            df_user = df_result[df_result["이름"] == name]

    # ✅ 캘린더 섹션
    st.divider()
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name} 님의 마스터 스케쥴</h6>", unsafe_allow_html=True)

    today = datetime.date.today()
    next_month = today.replace(day=1) + relativedelta(months=1)
    year, month = next_month.year, next_month.month
    week_labels = [f"{i+1}주" for i in range(len(week_nums))]

    master_data = {}
    for week in week_labels:
        week_df = df_user[df_user["주차"] == week]
        if not week_df.empty:
            master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()
        else:
            if "매주" in df_user["주차"].values:
                weekly_df = df_user[df_user["주차"] == "매주"]
                master_data[week] = weekly_df.set_index("요일")["근무여부"].to_dict()
            else:
                master_data[week] = {요일: "근무없음" for 요일 in 요일리스트}

    events = []
    weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
    _, last_day = calendar.monthrange(year, month)
    status_colors = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}

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