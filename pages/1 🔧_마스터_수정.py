import numpy as np
import streamlit as st
import pandas as pd
import calendar
import datetime
import time
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar  # 이름 바꿔주기\
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from google.oauth2.service_account import Credentials
import gspread
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

    # st.header(f"🔧 마스터 수정", divider='rainbow')

    # ✅ 사용자 인증 정보 가져오기
    def get_gspread_client():
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        # ✨ JSON처럼 강제 파싱 (줄바꿈 처리 문제 해결)
        service_account_info = dict(st.secrets["gspread"])
        # 🟢 private_key 줄바꿈 복원
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")

        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    
    url = "https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?gid=0#gid=0"
    gc = get_gspread_client()
    sheet = gc.open_by_url(url)
    worksheet1 = sheet.worksheet("마스터")

    # ✅ 로그인 사용자 정보
    employee_id = st.session_state.get("employee_id", "00000")

    # ✅ 기존 스케줄 불러오기
    try:
        data = worksheet1.get_all_records()
        df_all = pd.DataFrame(data)
        df_user = df_all[df_all["이름"] == name]
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

    # st.write(" ")
    # st.markdown(f"<h6 style='font-weight:bold;'>📋 {name} 님의  {month_str} 마스터 스케줄 편집 (1안)</h6>", unsafe_allow_html=True)

    # # ✅ 주차별 일정을 비교하여 모두 동일하면 매주로, 다르면 주 단위로 보기
    # is_weekly_default = False
    # if not df_user.empty:
    #     df_wide = df_user.pivot(index="요일", columns="주차", values="근무여부")
    #     if "매주" in df_wide.columns:
    #         is_weekly_default = True
    #     elif df_wide.nunique(axis=1).max() <= 1:
    #         is_weekly_default = True

    # # ✅ 주차별 보기 토글 (조건부 차단 포함)
    # disable_weekly_toggle = not is_weekly_default
    # is_weekly = st.checkbox("주 단위로 보기", value=not is_weekly_default, disabled=disable_weekly_toggle)
    # if not is_weekly_default and not is_weekly:
    #     st.warning("주 단위로 일정이 다르기 때문에 선택을 해제할 수 없습니다.")

    # # ✅ 주차리스트 생성
    # 주차리스트 = [f"{i+1}주차" for i in range(len(week_nums))] if is_weekly else ["매주"]

    # # ✅ 초기 데이터 구성
    # if df_user.empty:
    #     rows = []
    #     for week in 주차리스트:
    #         for day in 요일리스트:
    #             rows.append({"이름": name, "주차": week, "요일": day, "근무여부": "근무없음"})
    #     df = pd.DataFrame(rows)
    # else:
    #     # ✅ 사용자의 기존 데이터를 주차 필터로 필터링
    #     df = df_user[df_user["주차"].isin(주차리스트)].copy()

    #     # ✅ 매주 → 주차별 보기로 바뀐 경우: 매주 데이터를 복제하여 각 주차로 확장
    #     if df.empty and "매주" in df_user["주차"].values and is_weekly:
    #         df_weekly = df_user[df_user["주차"] == "매주"]
    #         rows = []
    #         for week in 주차리스트:
    #             for _, row in df_weekly.iterrows():
    #                 rows.append({"이름": name, "주차": week, "요일": row["요일"], "근무여부": row["근무여부"]})
    #         df = pd.DataFrame(rows)

    #     # ✅ 주차별 → 매주 보기로 바뀐 경우: 첫 주차 데이터를 대표값으로 사용
    #     elif df.empty and "매주" not in df_user["주차"].values and not is_weekly:
    #         first_week = [f"{i+1}주차" for i in range(len(week_nums))][0]
    #         df = df_user[df_user["주차"] == first_week].copy()
    #         df["주차"] = "매주"

    # # ✅ AgGrid 구성 및 출력
    # gb = GridOptionsBuilder.from_dataframe(df)
    # gb.configure_column("근무여부", editable=True, cellEditor="agSelectCellEditor",
    #                     cellEditorParams={"values": 근무옵션})
    # gb.configure_column("이름", editable=False)
    # gb.configure_column("주차", editable=False)
    # gb.configure_column("요일", editable=False)
    # gridOptions = gb.build()

    # grid_return = AgGrid(
    #     df,
    #     gridOptions=gridOptions,
    #     update_mode=GridUpdateMode.VALUE_CHANGED,
    #     fit_columns_on_grid_load=True,
    #     height=300
    # )

    # updated_df = grid_return["data"]

    # if st.button("💾 저장", key="save"):
    #     df_all.loc[df_all["이름"] == name, :] = pd.NA  # 기존 행 초기화
    #     df_all = df_all.dropna(how="all")  # 전부 NA인 행 제거
    #     df_result = pd.concat([df_all, updated_df], ignore_index=True)
    #     worksheet1.clear()
    #     worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
    #     st.success("Google Sheets에 저장되었습니다 ✅")
    #     time.sleep(2)
    #     st.cache_data.clear()
    #     st.rerun()

    # # 주차 리스트 생성
    # dates = [next_month.replace(day=i) for i in range(1, last_day + 1)]
    # weeks = sorted(set(d.isocalendar()[1] for d in dates))
    # week_labels = [f"{i+1}주차" for i in range(len(weeks))]

    # options = ['오전', '오후', '오전 & 오후', '근무없음']

    # # 마스터 데이터 초기화
    # master_data = {week: {"월": "근무없음", "화": "근무없음", "수": "근무없음", "목": "근무없음", "금": "근무없음"} for week in week_labels}

    # st.divider()
    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name} 님의 {month_str} 마스터 스케줄 편집</h6>", unsafe_allow_html=True)

    # ✅ 주차 리스트
    use_weekly = df_user["주차"].eq("매주").all()
    week_labels = [f"{i+1}주차" for i in range(len(week_nums))]

    # ✅ master_data 생성
    if df_user.empty:
        base = {요일: "근무없음" for 요일 in 요일리스트}
        master_data = {week: base.copy() for week in week_labels}
    else:
        if use_weekly:
            base = df_user[df_user["주차"] == "매주"].set_index("요일")["근무여부"].to_dict()
            master_data = {week: base.copy() for week in week_labels}
        else:
            master_data = {}
            for week in week_labels:
                week_df = df_user[df_user["주차"] == week]
                if week_df.empty:
                    master_data[week] = {요일: "근무없음" for 요일 in 요일리스트}
                else:
                    master_data[week] = week_df.set_index("요일")["근무여부"].to_dict()

    # 🌙 월 단위 일괄 설정
    with st.expander("📅 월 단위로 일괄 설정"):
        if not use_weekly:
            st.warning("현재 주차별 근무 일정이 다릅니다. 월 단위로 초기화하려면 내용을 입력하세요.")

        default_bulk = df_user[df_user["주차"] == "매주"].set_index("요일")["근무여부"].to_dict() if use_weekly else {}

        col1, col2, col3, col4, col5 = st.columns(5)
        월값 = col1.selectbox("월", 근무옵션, index=근무옵션.index(default_bulk.get("월", "근무없음")), key="월_bulk")
        화값 = col2.selectbox("화", 근무옵션, index=근무옵션.index(default_bulk.get("화", "근무없음")), key="화_bulk")
        수값 = col3.selectbox("수", 근무옵션, index=근무옵션.index(default_bulk.get("수", "근무없음")), key="수_bulk")
        목값 = col4.selectbox("목", 근무옵션, index=근무옵션.index(default_bulk.get("목", "근무없음")), key="목_bulk")
        금값 = col5.selectbox("금", 근무옵션, index=근무옵션.index(default_bulk.get("금", "근무없음")), key="금_bulk")

        if st.button("💾 월 단위 저장", key="save_monthly"):
            for week in week_labels:
                master_data[week] = {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}

            rows = []
            for week, days in master_data.items():
                for 요일, 근무 in days.items():
                    rows.append({"이름": name, "주차": week, "요일": 요일, "근무여부": 근무})
            updated_df = pd.DataFrame(rows)

            df_all.loc[df_all["이름"] == name, :] = pd.NA  # 기존 행 초기화
            df_all = df_all.dropna(how="all")  # 전부 NA인 행 제거
            df_result = pd.concat([df_all, updated_df], ignore_index=True)
            worksheet1.clear()
            worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            st.success("Google Sheets에 저장되었습니다 ✅")
            time.sleep(2)
            st.cache_data.clear()
            st.rerun()


    # 📅 주 단위 설정 UI
    with st.expander("📅 주 단위로 설정"):
        st.markdown("**요일별로 근무 여부를 선택해주세요.**")

        for week in week_labels:
            st.markdown(f"**🗓 {week}**")
            col1, col2, col3, col4, col5 = st.columns(5)

            master_data[week]["월"] = col1.selectbox(f"{week} - 월", 근무옵션, index=근무옵션.index(master_data[week]["월"]), key=f"{week}_월")
            master_data[week]["화"] = col2.selectbox(f"{week} - 화", 근무옵션, index=근무옵션.index(master_data[week]["화"]), key=f"{week}_화")
            master_data[week]["수"] = col3.selectbox(f"{week} - 수", 근무옵션, index=근무옵션.index(master_data[week]["수"]), key=f"{week}_수")
            master_data[week]["목"] = col4.selectbox(f"{week} - 목", 근무옵션, index=근무옵션.index(master_data[week]["목"]), key=f"{week}_목")
            master_data[week]["금"] = col5.selectbox(f"{week} - 금", 근무옵션, index=근무옵션.index(master_data[week]["금"]), key=f"{week}_금")

        if st.button("💾 주 단위 저장", key="save_weekly"):
            rows = []
            for week, days in master_data.items():
                for 요일, 근무 in days.items():
                    rows.append({"이름": name, "주차": week, "요일": 요일, "근무여부": 근무})
            updated_df = pd.DataFrame(rows)

            df_all.loc[df_all["이름"] == name, :] = pd.NA
            df_all = df_all.dropna(how="all")

            # 요일 순서 정의
            weekday_order = ["월", "화", "수", "목", "금"]

            # "요일" 열을 Categorical 타입으로 변환하며 순서 지정
            df_all["요일"] = pd.Categorical(df_all["요일"], categories=weekday_order, ordered=True)

            # 데이터프레임 정렬: "이름" (가나다순), "주차" (숫자순), "요일" (월-금 순서)
            df_all = df_all.sort_values(by=["이름", "주차", "요일"])

            df_result = pd.concat([df_all, updated_df], ignore_index=True)
            worksheet1.clear()
            worksheet1.update([df_result.columns.values.tolist()] + df_result.values.tolist())
            st.success("Google Sheets에 저장되었습니다 ✅")
            time.sleep(2)
            st.cache_data.clear()
            st.rerun()


    st.divider()
    st.markdown(f"<h6 style='font-weight:bold;'>📅 {name} 님의 {month_str} 마스터 스케쥴</h6>", unsafe_allow_html=True)

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