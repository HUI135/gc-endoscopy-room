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

st.set_page_config(page_title="마스터 수정", page_icon="🙋‍♂️", layout="wide")

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

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
    st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
    st.success("데이터가 새로고침되었습니다.")
    time.sleep(1)
    st.rerun()

st.write("- 휴가 / 보충 불가 / 꼭 근무 관련 요청사항이 있을 경우 반드시 기재해 주세요.\n- 요청사항은 매월 기재해 주셔야 하며, 별도 요청이 없을 경우에도 반드시 '요청 없음'을 입력해 주세요.")

if df_user_request.empty or (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    st.info("☑️ 당월에 입력하신 요청사항이 없습니다.")
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "height": 600, "headerToolbar": {"left": "", "center": "", "right": ""}}
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
    calendar_options = {"initialView": "dayGridMonth", "initialDate": next_month.strftime("%Y-%m-%d"), "editable": False, "selectable": False, "eventDisplay": "block", "dayHeaderFormat": {"weekday": "short"}, "themeSystem": "bootstrap", "height": 600, "headerToolbar": {"left": "", "center": "", "right": ""}, "showNonCurrentDates": True, "fixedWeekCount": False}
    st_calendar(events=events_request, options=calendar_options)

st.divider()

# 요청사항 입력 UI
st.markdown(f"<h6 style='font-weight:bold;'>🟢 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["휴가", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
날짜선택방식 = ["일자 선택", "기간 선택", "주/요일 선택"]

col1, col2, col3, col4 = st.columns([2, 2, 4, 1])

with col1:
    분류 = st.selectbox("요청 분류", 요청분류, key="category_select")

with col2:
    # [수정] 위젯을 항상 렌더링하되, 비활성화(disabled)하여 상태 관리 오류를 방지합니다.
    is_disabled = (분류 == "요청 없음")
    방식 = st.selectbox(
        "날짜 선택 방식",
        날짜선택방식,
        key="method_select",
        disabled=is_disabled  # '요청 없음'일 때 비활성화
    )
    if is_disabled:
        방식 = ""  # 비활성화 시에는 로직 처리를 위해 값을 비워줍니다.

# 날짜 입력 로직
날짜정보 = ""
with col3:
    if not is_disabled: # 비활성화가 아닐 때만 날짜 선택 위젯을 보여줍니다.
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
            선택요일 = st.multiselect("요일 선택", ["월", "화", "수", "목", "금", "토", "일"], key="day_select") # 주말 포함
            
            날짜목록 = []
            
            if 선택주차 and 선택요일:
                # 달력의 첫째 주 시작 요일을 기준으로 주차 계산
                # 4월 1일이 수요일이면 첫째 주 일~화는 3월 마지막 주
                # calendar.Calendar(firstweekday=6) # 0:월, 6:일
                c = calendar.Calendar(firstweekday=6) # 일요일부터 시작하는 달력 객체 생성
                
                # 해당 월의 모든 날짜를 주(list) 단위로 가져옴
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
                    
                    # 사용자가 선택한 주차에 해당하거나 "매주"를 선택했을 경우
                    if "매주" in 선택주차 or 주차_이름 in 선택주차:
                        for date in week:
                            # 해당 날짜가 현재 월에 속하고, 선택한 요일에 해당할 경우
                            if date.month == next_month.month and date.weekday() in 선택된_요일_인덱스:
                                날짜목록.append(date.strftime("%Y-%m-%d"))

            날짜정보 = ", ".join(날짜목록) if 날짜목록 else ""
            if not 날짜목록 and 선택주차 and 선택요일:
                st.warning(f"⚠️ {month_str}에는 해당 주차/요일의 날짜가 없습니다. 다른 조합을 선택해주세요.")
                st.time(2)
                
with col4:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    if st.button("📅 추가", use_container_width=True):
        if not 날짜정보 and 분류 != "요청 없음":
            st.warning("날짜 정보를 올바르게 입력해주세요.")
            st.stop() # st.stop()를 사용하여 아래 코드가 실행되지 않도록 함
        
        sheet = gc.open_by_url(url)
        worksheet2 = sheet.worksheet(f"{month_str} 요청")
        new_row = {"이름": name, "분류": 분류, "날짜정보": 날짜정보}
        
        if 분류 == "요청 없음":
            st.session_state["df_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] != name]
            st.session_state["df_request"] = pd.concat([st.session_state["df_request"], pd.DataFrame([new_row])], ignore_index=True)
            st.success("✅ '요청 없음'으로 저장되었습니다.")
        else:
            st.session_state["df_request"] = st.session_state["df_request"][~((st.session_state["df_request"]["이름"] == name) & (st.session_state["df_request"]["분류"] == "요청 없음"))]
            st.session_state["df_request"] = pd.concat([st.session_state["df_request"], pd.DataFrame([new_row])], ignore_index=True)
            st.success("✅ 요청사항이 저장되었습니다!")

        st.session_state["df_request"] = st.session_state["df_request"].sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
        worksheet2.clear()
        worksheet2.update([st.session_state["df_request"].columns.tolist()] + st.session_state["df_request"].astype(str).values.tolist())
        st.cache_data.clear()
        st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
        
        st.rerun() # 변경 사항을 캘린더에 즉시 반영하기 위해 reruun()을 사용

if 분류 == "요청 없음":
    st.markdown("<span style='color:red;'>⚠️ 요청 없음을 추가할 경우, 기존에 입력하였던 요청사항은 전부 삭제됩니다.</span>", unsafe_allow_html=True)

# 삭제 UI
st.write(" ")
st.markdown(f"<h6 style='font-weight:bold;'>🔴 요청사항 삭제</h6>", unsafe_allow_html=True)
if not df_user_request.empty and not (df_user_request["분류"].nunique() == 1 and df_user_request["분류"].unique()[0] == "요청 없음"):
    # --- [수정] 컬럼을 사용해 '삭제' 버튼을 같은 행에 배치 ---
    del_col1, del_col2 = st.columns([4, 0.5])
    with del_col1:
        options = [f"{row['분류']} - {row['날짜정보']}" for _, row in df_user_request[df_user_request['분류'] != '요청 없음'].iterrows()]
        selected_items = st.multiselect("삭제할 요청사항 선택", options, key="delete_select")
    
    with del_col2:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        if st.button("🗑️ 삭제", use_container_width=True) and selected_items:
            sheet = gc.open_by_url(url)
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
            
            rows_to_delete = []
            for item in selected_items:
                parts = item.split(" - ", 1)
                if len(parts) == 2:
                    분류_str, 날짜정보_str = parts
                    matching_rows = st.session_state["df_request"][
                        (st.session_state["df_request"]['이름'] == name) & 
                        (st.session_state["df_request"]['분류'] == 분류_str) & 
                        (st.session_state["df_request"]['날짜정보'] == 날짜정보_str)
                    ]
                    rows_to_delete.extend(matching_rows.index.tolist())
            
            if rows_to_delete:
                st.session_state["df_request"] = st.session_state["df_request"].drop(index=rows_to_delete).reset_index(drop=True)
                
                if st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].empty:
                    st.session_state["df_request"] = pd.concat([st.session_state["df_request"], pd.DataFrame([{"이름": name, "분류": "요청 없음", "날짜정보": ""}])], ignore_index=True)
                
                st.session_state["df_request"] = st.session_state["df_request"].sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                
                worksheet2.clear()
                worksheet2.update([st.session_state["df_request"].columns.tolist()] + st.session_state["df_request"].astype(str).values.tolist())
                st.cache_data.clear()
                st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
                st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy()
                
                st.success("✅ 선택한 요청사항이 삭제되었습니다!")
                st.rerun() # 변경 사항을 캘린더에 즉시 반영하기 위해 reruun()을 사용
            else:
                st.warning("삭제할 항목을 찾을 수 없습니다.")

else:
    st.info("📍 삭제할 요청사항이 없습니다.")