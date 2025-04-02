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
from gspread.exceptions import WorksheetNotFound
import streamlit.components.v1 as components

# st.header(f"✉️ 요청사항 입력", divider='rainbow')

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
    
    # 익월 범위 지정
    today = datetime.date.today()
    next_month = today.replace(day=1) + relativedelta(months=1)
    next_month_start = next_month
    _, last_day = calendar.monthrange(next_month.year, next_month.month)
    next_month_end = next_month.replace(day=last_day)
    month_str = next_month.strftime("%Y년 %m월")

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

    # ✅ 로그인 사용자 정보
    employee_id = st.session_state.get("employee_id", "00000")

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

    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>🙋‍♂️ {name} 님의 {month_str} 요청사항 입력</h6>", unsafe_allow_html=True)

    # 옵션 정의
    요청분류 = ["휴가", "학회", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
    날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

    col1, col2 = st.columns(2)
    분류 = col1.selectbox("요청 분류", 요청분류)
    방식 = ""
    if 분류 != "요청 없음":
        방식 = col2.selectbox("날짜 선택 방식", 날짜선택방식)

    # 익월 범위 지정
    today = datetime.date.today()
    next_month = today.replace(day=1) + relativedelta(months=1)
    next_month_start = next_month
    _, last_day = calendar.monthrange(next_month.year, next_month.month)
    next_month_end = next_month.replace(day=last_day)

    # 날짜 입력 방식
    날짜정보 = ""
    if 분류 != "요청 없음":
        if 방식 == "일자 선택":
            날짜 = st.multiselect(
                "요청 일자", 
                [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)],
                format_func=lambda x: x.strftime("%Y-%m-%d")  # 날짜 형식 지정
            )
            if 날짜:  # 선택된 날짜가 있을 경우
                날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜])  # 여러 날짜 선택 시, ','로 구분하여 날짜정보에 할당
        elif 방식 == "기간 선택":
            날짜범위 = st.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end)
            if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                시작, 종료 = 날짜범위
                날짜정보 = f"{시작.strftime('%Y-%m-%d')} ~ {종료.strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.multiselect("해당 주차를 선택하세요", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"])
            선택요일 = st.multiselect("해당 요일을 선택하세요", ["월", "화", "수", "목", "금"])
            주차_index = {"첫째주": 0, "둘째주": 1, "셋째주": 2, "넷째주": 3, "다섯째주": 4}
            요일_index = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
            날짜목록 = []
            for i in range(1, last_day + 1):
                날짜 = datetime.date(next_month.year, next_month.month, i)
                weekday = 날짜.weekday()
                week_of_month = (i - 1) // 7
                if weekday in 요일_index.values():
                    for 주차 in 선택주차:
                        if 주차 == "매주" or 주차_index.get(주차) == week_of_month:
                            for 요일 in 선택요일:
                                if weekday == 요일_index[요일]:
                                    날짜목록.append(날짜.strftime("%Y-%m-%d"))
            if 날짜목록:
                날짜정보 = ", ".join(날짜목록)

    # 요청 없음 선택 시 경고 문구 표시
    if 분류 == "요청 없음":
        st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

    # 저장 버튼
    if st.button("📅 추가"):
        try:
            if 분류 == "요청 없음":
                df_all = df_all[df_all["이름"] != name]
                st.warning("📍 기존 요청사항이 모두 삭제됩니다.")
                new_row = {"이름": name, "분류": 분류, "날짜정보": ""}
                df_all = pd.concat([df_all, pd.DataFrame([new_row])], ignore_index=True)
            elif 날짜정보:
                # ✅ 현재 사용자 데이터 중 '요청 없음'이 있다면 제거
                if not df_user[df_user["분류"] == "요청 없음"].empty:
                    df_all = df_all[~((df_all["이름"] == name) & (df_all["분류"] == "요청 없음"))]

                # ✅ 새로운 요청 추가
                new_row = {"이름": name, "분류": 분류, "날짜정보": 날짜정보}
                df_all = pd.concat([df_all, pd.DataFrame([new_row])], ignore_index=True)
            else:
                st.warning("날짜 정보를 올바르게 입력해주세요.")
                st.stop()

            df_all = df_all.fillna("")  # NaN -> 빈 문자열로

            # 정렬 (이름 -> 날짜정보 순으로)
            df_all = df_all.sort_values(by=["이름", "날짜정보"])

            worksheet2.clear()
            worksheet2.update([df_all.columns.tolist()] + df_all.astype(str).values.tolist())
            st.success("✅ 요청사항이 저장되었습니다!")
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"오류 발생: {e}")

    st.write(" ")
    st.write(" ")
    st.markdown(f"<h6 style='font-weight:bold;'>{month_str} 요청사항 삭제</h6>", unsafe_allow_html=True)

    if not df_user.empty:
        # "요청 없음" 분류가 포함된 행을 제외한 데이터만 선택지에 표시
        df_user_filtered = df_user[df_user['분류'] != '요청 없음']
        
        selected_rows = st.multiselect(
            "요청사항 선택",
            df_user_filtered.index,
            format_func=lambda x: f"{df_user.loc[x, '분류']} - {df_user.loc[x, '날짜정보']}"
        )
        
    if df_user.empty:
        st.info("📍 당월 요청사항 없음")

    if st.button("🗑️ 삭제") and selected_rows:
        df_all = df_all.drop(index=selected_rows)

        # 🔄 해당 이름의 요청사항이 모두 삭제되었는지 확인
        is_user_empty = df_all[df_all["이름"] == name].empty
        if is_user_empty:
            new_row = {"이름": name, "분류": "요청 없음", "날짜정보": ""}
            df_all = pd.concat([df_all, pd.DataFrame([new_row])], ignore_index=True)
            st.info("모든 요청사항이 삭제되어, '요청 없음' 항목이 자동으로 추가되었습니다.")

        df_all = df_all.fillna("")
        worksheet2.clear()
        worksheet2.update([df_all.columns.tolist()] + df_all.astype(str).values.tolist())
        st.success("선택한 요청사항이 삭제되었습니다!")
        time.sleep(2)
        st.rerun()

    st.divider()
    st.markdown(f"<h6 style='font-weight:bold;'>🙋‍♂️ {name} 님의 {month_str} 요청사항</h6>", unsafe_allow_html=True)

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