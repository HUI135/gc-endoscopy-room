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

# set_page_config()를 스크립트 최상단으로 이동
st.set_page_config(page_title="요청사항 입력", layout="wide", page_icon="🙋‍♂️")

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수 (캐싱 적용, 필요 시 무효화)
def load_master_data_page2(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

def load_request_data_page2(_gc, url, month_str):
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
today = datetime.datetime.strptime("2025-03-10", "%Y-%m-%d").date()

next_month = today.replace(day=1) + relativedelta(months=1)
month_str = next_month.strftime("%Y년 %m월")
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
    st.success("데이터가 새로고침되었습니다.")
    time.sleep(1)
    st.rerun()

# 초기 데이터 로드 및 세션 상태 설정
if "df_request" not in st.session_state:
    st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
if "df_user_request" not in st.session_state:
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()

# 항상 최신 세션 상태를 참조
df_request = st.session_state["df_request"]
df_user_request = st.session_state["df_user_request"]

# 캘린더 표시
st.header(f"🙋‍♂️ {name} 님의 {month_str} 요청사항", divider='rainbow')
st.write("- 휴가 / 보충 불가 / 꼭 근무 관련 요청사항이 있을 경우 반드시 기재해 주세요.\n- 요청사항은 매월 기재해 주셔야 하며, 별도 요청이 없을 경우에도 반드시 '요청 없음'을 입력해 주세요.")

if df_user_request.empty or (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    st.info("☑️ 당월에 입력하신 요청사항이 없습니다.")
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "height": 500, "headerToolbar": {"left": "", "center": "", "right": ""}}
    st_calendar(options=calendar_options)
else:
    status_colors_request = {"휴가": "#FE7743", "보충 어려움(오전)": "#FFB347", "보충 어려움(오후)": "#FFA07A", "보충 불가(오전)": "#FFB347", "보충 불가(오후)": "#FFA07A", "꼭 근무(오전)": "#4CAF50", "꼭 근무(오후)": "#2E8B57"}
    label_map = {"휴가": "휴가", "보충 어려움(오전)": "보충⚠️(오전)", "보충 어려움(오후)": "보충⚠️(오후)", "보충 불가(오전)": "보충🚫(오전)", "보충 불가(오후)": "보충🚫(오후)", "꼭 근무(오전)": "꼭근무(오전)", "꼭 근무(오후)": "꼭근무(오후)"}
    events_request = []
    for _, row in df_user_request.iterrows():
        분류, 날짜정보 = row["분류"], row["날짜정보"]
        if not 날짜정보 or 분류 == "요청 없음": continue
        if "~" in 날짜정보:
            시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
            시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
            종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
            events_request.append({"title": label_map.get(분류, 분류), "start": 시작.strftime("%Y-%m-%d"), "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
        else:
            for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                try:
                    dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                    events_request.append({"title": label_map.get(분류, 분류), "start": dt.strftime("%Y-%m-%d"), "end": dt.strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
                except: continue
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "editable": False, "selectable": False, "eventDisplay": "block", "dayHeaderFormat": {"weekday": "short"}, "themeSystem": "bootstrap", "height": 500, "headerToolbar": {"left": "", "center": "", "right": ""}, "showNonCurrentDates": True, "fixedWeekCount": False}
    st_calendar(events=events_request, options=calendar_options)

st.divider()

# 요청사항 입력 UI
st.markdown(f"<h6 style='font-weight:bold;'>🟢 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["휴가", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

# --- [수정] 4개의 열로 변경하여 '추가' 버튼을 같은 행에 배치 ---
col1, col2, col3, col4 = st.columns([2, 2, 4, 1])
with col1:
    분류 = st.selectbox("요청 분류", 요청분류, key="category_select")
with col2:
    방식 = st.selectbox("날짜 선택 방식", 날짜선택방식, key="method_select") if 분류 != "요청 없음" else ""

# 날짜 입력 로직
날짜정보 = ""
with col3:
    if 분류 != "요청 없음":
        if 방식 == "일자 선택":
            weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
            def format_date(date_obj):
                return f"{date_obj.strftime('%m월 %d일')} ({weekday_map[date_obj.weekday()]})"
            날짜 = st.multiselect("요청 일자", [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)], format_func=format_date, key="date_multiselect")
            날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜]) if 날짜 else ""
        elif 방식 == "기간 선택":
            날짜범위 = st.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end, key="date_range")
            if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                날짜정보 = f"{날짜범위[0].strftime('%Y-%m-%d')} ~ {날짜범위[1].strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"], key="week_select")
            선택요일 = st.multiselect("요일 선택", ["월", "화", "수", "목", "금"], key="day_select")
            주차_index, 요일_index = {"첫째주": 0, "둘째주": 1, "셋째주": 2, "넷째주": 3, "다섯째주": 4}, {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
            날짜목록 = []
            first_day = next_month_start
            first_sunday_offset = (6 - first_day.weekday()) % 7
            for i in range(last_day):
                current_date = first_day + datetime.timedelta(days=i)
                if current_date.month != next_month.month: continue
                week_of_month = (current_date.day + first_sunday_offset - 1) // 7
                if current_date.weekday() in 요일_index.values() and any(주차 == "매주" or 주차_index.get(주차) == week_of_month for 주차 in 선택주차):
                    if current_date.weekday() in [요일_index[요일] for 요일 in 선택요일]:
                        날짜목록.append(current_date.strftime("%Y-%m-%d"))
            날짜정보 = ", ".join(날짜목록) if 날짜목록 else ""

with col4:
    # --- [수정] 버튼 정렬을 위한 공백 추가 ---
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    # 저장 로직
    if st.button("📅 추가", use_container_width=True):
        sheet = gc.open_by_url(url)
        worksheet2 = sheet.worksheet(f"{month_str} 요청")
        if 분류 == "요청 없음":
            df_request = df_request[df_request["이름"] != name]
            df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": ""}])], ignore_index=True)
        elif 날짜정보:
            df_request = df_request[~((df_request["이름"] == name) & (df_request["분류"] == "요청 없음"))]
            df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": 날짜정보}])], ignore_index=True)
        else:
            st.warning("날짜 정보를 올바르게 입력해주세요.")
            st.stop()
        
        df_request = df_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([df_request.columns.tolist()] + df_request.astype(str).values.tolist())
        st.cache_data.clear()
        st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["이름"] == name].copy()
        st.success("✅ 요청사항이 저장되었습니다!")
        st.rerun()

if 분류 == "요청 없음":
    st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

# 삭제 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_request.empty and not (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    # --- [수정] 컬럼을 사용해 '삭제' 버튼을 같은 행에 배치 ---
    del_col1, del_col2 = st.columns([4, 1])
    with del_col1:
        options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_user_request[df_user_request['분류'] != '요청 없음'].iterrows()]
        selected_items = st.multiselect("삭제할 요청사항 선택", options, key="delete_select")
    
    with del_col2:
        # --- [수정] 버튼 정렬을 위한 공백 추가 ---
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        if st.button("🗑️ 삭제", use_container_width=True) and selected_items:
            sheet = gc.open_by_url(url)
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
            selected_indices = []
            for item in selected_items:
                for idx, row in df_request.iterrows():
                    if row['이름'] == name and f"{row['분류']} - {row['날짜정보']}" == item:
                        selected_indices.append(idx)
            
            df_request = df_request.drop(index=selected_indices).reset_index(drop=True)
            if df_request[df_request["이름"] == name].empty:
                df_request = pd.concat([df_request, pd.DataFrame([{"이름": name, "분류": "요청 없음", "날짜정보": ""}])], ignore_index=True)
            
            df_request = df_request.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
            worksheet2.clear()
            worksheet2.update([df_request.columns.tolist()] + df_request.astype(str).values.tolist())
            st.cache_data.clear()
            st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
            st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["이름"] == name].copy()
            st.success("✅ 선택한 요청사항이 삭제되었습니다!")
            st.rerun()
else:
    st.info("📍 삭제할 요청사항이 없습니다.")