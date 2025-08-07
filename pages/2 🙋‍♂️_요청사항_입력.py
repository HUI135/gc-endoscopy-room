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
import menu

st.set_page_config(page_title="요청사항 입력", page_icon="🙋‍♂️", layout="wide")

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

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수 (캐싱 적용, 필요 시 무효화)
@st.cache_data(ttl=3600, show_spinner=False)
def load_master_data(_gc, url):
    sheet = _gc.open_by_url(url)
    worksheet_master = sheet.worksheet("마스터")
    return pd.DataFrame(worksheet_master.get_all_records())

@st.cache_data(ttl=60, show_spinner=False)
def load_request_data_page2(_gc, url, month_str):
    sheet = _gc.open_by_url(url)
    try:
        worksheet = sheet.worksheet(f"{month_str} 요청")
    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
        worksheet.append_row(["이름", "분류", "날짜정보"])
    data = worksheet.get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])

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

# 캘린더 이벤트 생성 함수 (마스터 스케줄과 요청사항 모두 처리)
def create_calendar_events(df_master, df_request):
    status_colors_master = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}
    events = []
    
    # 마스터 데이터에서 이벤트 생성
    if not df_master.empty:
        next_month_calc = today.replace(day=1) + relativedelta(months=1)
        year, month = next_month_calc.year, next_month_calc.month
        c = calendar.Calendar(firstweekday=6)
        month_calendar = c.monthdatescalendar(year, month)

        week_labels = {}
        for i, week in enumerate(month_calendar):
            for date_obj in week:
                if date_obj.month == month:
                    if i == 0: week_label = "첫째주"
                    elif i == 1: week_label = "둘째주"
                    elif i == 2: week_label = "셋째주"
                    elif i == 3: week_label = "넷째주"
                    elif i == 4: week_label = "다섯째주"
                    else: continue
                    week_labels[date_obj] = week_label
        
        요일_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}

        for _, row in df_master.iterrows():
            주차, 요일, 근무여부 = row['주차'], row['요일'], row['근무여부']
            if 근무여부 == "근무없음":
                continue

            for date_obj, week_label in week_labels.items():
                if date_obj.weekday() == 요일_map.get(요일):
                    if 주차 == '매주' or (주차 != '매주' and 주차 == week_label):
                        events.append({
                            "title": f"{근무여부}",
                            "start": date_obj.strftime("%Y-%m-%d"),
                            "end": date_obj.strftime("%Y-%m-%d"),
                            "color": status_colors_master.get(근무여부, "#E0E0E0")
                        })
    
    # 요청사항 이벤트 생성
    status_colors_request = {
        "휴가": "#A1C1D3",
        "보충 어려움(오전)": "#FFD3B5",
        "보충 어려움(오후)": "#FFD3B5",
        "보충 불가(오전)": "#FFB6C1",
        "보충 불가(오후)": "#FFB6C1",
        "꼭 근무(오전)": "#C3E6CB",
        "꼭 근무(오후)": "#C3E6CB",
    }
    label_map = {
        "휴가": "휴가🎉",
        "보충 어려움(오전)": "보충⚠️(오전)",
        "보충 어려움(오후)": "보충⚠️(오후)",
        "보충 불가(오전)": "보충🚫(오전)",
        "보충 불가(오후)": "보충🚫(오후)",
        "꼭 근무(오전)": "꼭근무(오전)",
        "꼭 근무(오후)": "꼭근무(오후)",
    }

    if not df_request.empty:
        for _, row in df_request.iterrows():
            분류, 날짜정보 = row["분류"], row["날짜정보"]
            if not 날짜정보 and 분류 != "요청 없음":
                continue
            
            # '요청 없음' 이벤트를 만들지 않도록 코드 제거
            if 분류 == "요청 없음":
                continue
            
            if "~" in 날짜정보:
                시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
                시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
                종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
                events.append({"title": f"{label_map.get(분류, 분류)}", "start": 시작.strftime("%Y-%m-%d"), "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
            else:
                for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                    try:
                        dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                        events.append({"title": f"{label_map.get(분류, 분류)}", "start": dt.strftime("%Y-%m-%d"), "end": dt.strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
                    except:
                        continue
    return events


# --- 초기 데이터 로딩 및 세션 상태 초기화 ---
# 페이지 로드 시에만 한 번 실행
def initialize_data():
    """페이지에 필요한 모든 데이터를 한 번에 로드하고 세션 상태에 저장합니다."""
    # 캐시를 비워 최신 데이터를 가져오도록 합니다.
    st.cache_data.clear() 

    # 데이터 로드
    st.session_state["df_master"] = load_master_data(gc, url)
    st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)

    # 유저별 데이터 필터링
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
    st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()

# 'initial_load_done_page2'가 없으면 초기화 함수를 실행합니다.
# 이 블록은 페이지가 로드될 때 단 한 번만 실행됩니다.
if "initial_load_done_page2" not in st.session_state:
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        initialize_data()
        st.session_state["initial_load_done_page2"] = True
    # st.rerun()을 제거하여 불필요한 재실행을 막습니다.

# 항상 최신 세션 상태를 참조
df_request = st.session_state["df_request"]
df_user_request = st.session_state["df_user_request"]
df_user_master = st.session_state["df_user_master"]

# 새로고침 버튼 (맨 상단)
def refresh_data():
    with st.spinner("데이터를 다시 불러오는 중입니다..."):
        initialize_data()
    st.success("데이터가 새로고침되었습니다.")
    # 콜백 함수 내에서 st.rerun() 제거

st.header(f"🙋‍♂️ {name} 님의 {month_str} 요청사항", divider='rainbow')

if st.button("🔄 새로고침 (R)", on_click=refresh_data):
    pass
st.write("- 휴가 / 보충 불가 / 꼭 근무 관련 요청사항이 있을 경우 반드시 기재해 주세요.\n- 요청사항은 매월 기재해 주셔야 하며, 별도 요청이 없을 경우에도 반드시 '요청 없음'을 입력해 주세요.")

events_combined = create_calendar_events(df_user_master, df_user_request)

if not events_combined:
    st.info("☑️ 당월에 입력하신 요청사항 또는 마스터 스케줄이 없습니다.")
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "height": 600, "headerToolbar": {"left": "", "center": "", "right": ""}}
    st_calendar(options=calendar_options)
else:
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "editable": False, "selectable": False, "eventDisplay": "block", "dayHeaderFormat": {"weekday": "short"}, "themeSystem": "bootstrap", "height": 700, "headerToolbar": {"left": "", "center": "", "right": ""}, "showNonCurrentDates": True, "fixedWeekCount": False, "eventOrder": "title"}
    st_calendar(events=events_combined, options=calendar_options)

st.divider()

# 요청사항 입력 UI
st.markdown(f"<h6 style='font-weight:bold;'>🟢 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["휴가", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

col1, col2, col3, col4 = st.columns([2, 2, 4, 1])

with col1:
    분류 = st.selectbox("요청 분류", 요청분류, key="category_select")

with col2:
    is_disabled = (분류 == "요청 없음")
    방식 = st.selectbox(
        "날짜 선택 방식",
        날짜선택방식,
        key="method_select",
        disabled=is_disabled
    )
    if is_disabled:
        방식 = ""

with col3:
    if not is_disabled:
        if 방식 == "일자 선택":
            weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
            def format_date(date_obj):
                return f"{date_obj.strftime('%m월 %d일')} ({weekday_map[date_obj.weekday()]})"
            st.multiselect("요청 일자", [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)], format_func=format_date, key="date_multiselect")
        elif 방식 == "기간 선택":
            st.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end, key="date_range")
        elif 방식 == "주/요일 선택":
            st.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"], key="week_select")
            st.multiselect("요일 선택", ["월", "화", "수", "목", "금", "토", "일"], key="day_select")
            
def add_request_callback():
    분류 = st.session_state["category_select"]
    날짜정보 = ""
    is_disabled = (분류 == "요청 없음")
    
    if not is_disabled:
        방식 = st.session_state.get("method_select", "")
        if 방식 == "일자 선택":
            날짜 = st.session_state.get("date_multiselect", [])
            날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜]) if 날짜 else ""
        elif 방식 == "기간 선택":
            날짜범위 = st.session_state.get("date_range", ())
            if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                날짜정보 = f"{날짜범위[0].strftime('%Y-%m-%d')} ~ {날짜범위[1].strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.session_state.get("week_select", [])
            선택요일 = st.session_state.get("day_select", [])
            날짜목록 = []
            
            if 선택주차 and 선택요일:
                c = calendar.Calendar(firstweekday=6)
                month_calendar = c.monthdatescalendar(next_month.year, next_month.month)
                
                요일_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}
                선택된_요일_인덱스 = [요일_map[요일] for 요일 in 선택요일]
                for i, week in enumerate(month_calendar):
                    주차_이름 = ""
                    if i == 0: 주차_이름 = "첫째주"
                    elif i == 1: 주차_이름 = "둘째주"
                    elif i == 2: 주차_이름 = "셋째주"
                    elif i == 3: 주차_이름 = "넷째주"
                    elif i == 4: 주차_이름 = "다섯째주"
                    
                    if "매주" in 선택주차 or 주차_이름 in 선택주차:
                        for date in week:
                            if date.month == next_month.month and date.weekday() in 선택된_요일_인덱스:
                                날짜목록.append(date.strftime("%Y-%m-%d"))

            날짜정보 = ", ".join(sorted(list(set(날짜목록))))
            if not 날짜목록 and 선택주차 and 선택요일:
                st.warning(f"⚠️ {month_str}에는 해당 주차/요일의 날짜가 없습니다. 다른 조합을 선택해주세요.")
                return
                
    if not 날짜정보 and 분류 != "요청 없음":
        st.warning("날짜 정보를 올바르게 입력해주세요.")
        return
    
    with st.spinner("요청사항을 추가 중입니다..."):
        sheet = gc.open_by_url(url)
        worksheet2 = sheet.worksheet(f"{month_str} 요청")
        
        # '요청 없음' 데이터가 있으면 삭제하고, 새 요청사항을 추가합니다.
        df_to_save = st.session_state["df_request"][~((st.session_state["df_request"]["이름"] == name) & (st.session_state["df_request"]["분류"] == "요청 없음"))].copy()
        
        if 분류 == "요청 없음":
            df_to_save = pd.concat([df_to_save, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": ""}])], ignore_index=True)
        else:
            new_request_data = {"이름": name, "분류": 분류, "날짜정보": 날짜정보}
            df_to_save = pd.concat([df_to_save, pd.DataFrame([new_request_data])], ignore_index=True)

        df_to_save = df_to_save.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        
        worksheet2.clear()
        worksheet2.update([df_to_save.columns.tolist()] + df_to_save.astype(str).values.tolist())
        
        st.session_state["df_request"] = df_to_save
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
        
        st.toast("요청사항이 추가되었습니다! 📅", icon="✅")
        # 콜백 함수 내에서 st.rerun() 제거

with col4:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    st.button("📅 추가", use_container_width=True, on_click=add_request_callback)

if st.session_state.get("category_select", "요청 없음") == "요청 없음":
    st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

# 삭제 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_request.empty and not (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    del_col1, del_col2 = st.columns([4, 0.5])
    with del_col1:
        options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_user_request[df_user_request['분류'] != '요청 없음'].iterrows()]
        st.multiselect("삭제할 요청사항 선택", options, key="delete_select")

    def delete_requests_callback():
        selected_items = st.session_state.get("delete_select", [])
        if not selected_items:
            st.warning("삭제할 항목을 선택해주세요.")
            return

        with st.spinner("요청사항을 삭제 중입니다..."):
            sheet = gc.open_by_url(url)
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
            
            rows_to_delete_indices = []
            for item in selected_items:
                parts = item.split(" - ", 1)
                if len(parts) == 2:
                    분류_str, 날짜정보_str = parts
                    matching_rows = st.session_state["df_request"][
                        (st.session_state["df_request"]['이름'] == name) & 
                        (st.session_state["df_request"]['분류'] == 분류_str) & 
                        (st.session_state["df_request"]['날짜정보'] == 날짜정보_str)
                    ]
                    rows_to_delete_indices.extend(matching_rows.index.tolist())
            
            if rows_to_delete_indices:
                df_to_save = st.session_state["df_request"].drop(index=rows_to_delete_indices).reset_index(drop=True)
                
                # '요청 없음'을 자동으로 추가하는 코드 제거
                # if df_to_save[df_to_save["이름"] == name].empty:
                #     df_to_save = pd.concat([df_to_save, pd.DataFrame([{"이름": name, "분류": "요청 없음", "날짜정보": ""}])], ignore_index=True)
                
                df_to_save = df_to_save.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                
                worksheet2.clear()
                worksheet2.update([df_to_save.columns.tolist()] + df_to_save.astype(str).values.tolist())
                
                st.session_state["df_request"] = df_to_save
                st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()

                st.toast("요청사항이 삭제되었습니다! 🗑️", icon="✅")
                st.success("✅ 선택한 요청사항이 삭제되었습니다!")
            else:
                st.warning("삭제할 항목을 찾을 수 없습니다.")
        
        # 콜백 함수 내에서 st.rerun() 제거

    with del_col2:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        st.button("🗑️ 삭제", use_container_width=True, on_click=delete_requests_callback)
else:
    st.info("📍 삭제할 요청사항이 없습니다.")