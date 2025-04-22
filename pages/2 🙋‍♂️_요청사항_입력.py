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

# 데이터 로드 함수 (캐싱 적용, 필요 시 무효화)
def load_master_data(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

def load_request_data(_gc, url, month_str):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(f"{month_str} 요청")
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

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
# today = datetime.date.today()
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()

next_month = today.replace(day=1) + relativedelta(months=1)
month_str = next_month.strftime("%Y년 %m월")
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)

# 초기 데이터 로드 및 세션 상태 설정
if "df_request" not in st.session_state:
    st.session_state["df_request"] = load_request_data(gc, url, month_str)
if "df_user_request" not in st.session_state:
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()

# 항상 최신 세션 상태를 참조
df_request = st.session_state["df_request"]
df_user_request = st.session_state["df_user_request"]

# 캘린더 표시
# st.markdown(f"<h6 style='font-weight:bold;'>🙋‍♂️ {name} 님의 {month_str} 요청사항</h6>", unsafe_allow_html=True)
st.header(f"🙋‍♂️ {name} 님의 {month_str} 요청사항", divider='rainbow')

if df_user_request.empty or (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    st.info("☑️ 당월에 입력하신 요청사항이 없습니다.")
else:
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

    st_calendar(events=events_request, options=calendar_options)

st.divider()

# 요청사항 입력 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🟢 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["휴가", "학회", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

# 세 개의 열로 변경
col1, col2, col3 = st.columns([1,1,2])
분류 = col1.selectbox("요청 분류", 요청분류, key="category_select")
방식 = col2.selectbox("날짜 선택 방식", 날짜선택방식, key="method_select") if 분류 != "요청 없음" else ""

# 날짜 입력 로직
날짜정보 = ""
if 분류 != "요청 없음":
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

        # 첫 번째 일요일 찾기
        first_sunday = None
        for i in range(1, last_day + 1):
            date_obj = datetime.date(next_month.year, next_month.month, i)
            if date_obj.weekday() == 6:  # 일요일
                first_sunday = i
                break

        for i in range(1, last_day + 1):
            날짜 = datetime.date(next_month.year, next_month.month, i)
            weekday = 날짜.weekday()
            # 주차 계산: 첫 번째 일요일 기준
            if i < first_sunday:
                week_of_month = 0  # 첫 번째 일요일 이전은 1주차
            else:
                week_of_month = (i - first_sunday) // 7 + 1  # 첫 번째 일요일 이후 주차 계산
            if weekday in 요일_index.values() and any(주차 == "매주" or 주차_index.get(주차) == week_of_month for 주차 in 선택주차):
                if weekday in [요일_index[요일] for 요일 in 선택요일]:
                    날짜목록.append(날짜.strftime("%Y-%m-%d"))
        날짜정보 = ", ".join(날짜목록) if 날짜목록 else ""

if 분류 == "요청 없음":
    st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

# 저장 로직
if st.button("📅 추가"):
    sheet = gc.open_by_url(url)
    worksheet2 = sheet.worksheet(f"{month_str} 요청")
    if 분류 == "요청 없음":
        df_request = df_request[df_request["이름"] != name]
        df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": ""}])], ignore_index=True)
        df_request = df_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_request.columns.tolist()] + df_request.astype(str).values.tolist())
        st.session_state["df_request"] = df_request
        st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy()
        st.success("✅ 요청사항이 저장되었습니다!")
        st.cache_data.clear()  # 캐시 무효화
        st.session_state["df_request"] = load_request_data(gc, url, month_str)
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
        st.rerun()  # 페이지 새로고침
    elif 날짜정보:
        df_request = df_request[~((df_request["이름"] == name) & (df_request["분류"] == "요청 없음"))]
        df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": 날짜정보}])], ignore_index=True)
        df_request = df_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_request.columns.tolist()] + df_request.astype(str).values.tolist())
        st.session_state["df_request"] = df_request
        st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy()
        st.success("✅ 요청사항이 저장되었습니다!")
        st.cache_data.clear()  # 캐시 무효화
        st.session_state["df_request"] = load_request_data(gc, url, month_str)
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
        st.rerun()  # 페이지 새로고침
    else:
        st.warning("날짜 정보를 올바르게 입력해주세요.")

# 삭제 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_request.empty and not (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_user_request[df_user_request['분류'] != '요청 없음'].iterrows()]
    selected_items = st.multiselect("요청사항 선택", options, key="delete_select")
    if st.button("🗑️ 삭제") and selected_items:
        sheet = gc.open_by_url(url)
        worksheet2 = sheet.worksheet(f"{month_str} 요청")
        selected_indices = []
        for item in selected_items:
            for idx, row in df_user_request.iterrows():
                if f"{row['분류']} - {row['날짜정보']}" == item:
                    selected_indices.append(idx)
        df_request = df_request.drop(index=selected_indices)
        if df_request[df_request["이름"] == name].empty:
            df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": "요청 없음", "날짜정보": ""}])], ignore_index=True)
        df_request = df_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_request.columns.tolist()] + df_request.astype(str).values.tolist())
        st.session_state["df_request"] = df_request
        st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy()
        st.success("✅ 선택한 요청사항이 삭제되었습니다!")
        st.cache_data.clear()  # 캐시 무효화
        st.session_state["df_request"] = load_request_data(gc, url, month_str)
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
        # st.success("데이터가 새로고침되었습니다!")
        st.rerun()  # 페이지 새로고침
else:
    st.info("📍 요청사항 없음")