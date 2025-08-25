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
    st.switch_page("Home.py")
    st.stop()

# 전역 변수로 gspread 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 클라이언트 초기화 중 오류 발생: {str(e)}")
        st.stop()

# 데이터 로드 함수 (캐싱 적용, 필요 시 무효화)
@st.cache_data(ttl=3600, show_spinner=False)
def load_master_data(_gc, url):
    try:
        sheet = _gc.open_by_url(url)
        worksheet_master = sheet.worksheet("마스터")
        return pd.DataFrame(worksheet_master.get_all_records())
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (마스터 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"마스터 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

@st.cache_data(ttl=60, show_spinner=False)
def load_request_data_page2(_gc, url, month_str):
    try:
        sheet = _gc.open_by_url(url)
        try:
            worksheet = sheet.worksheet(f"{month_str} 요청")
        except WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
            worksheet.append_row(["이름", "분류", "날짜정보"])
        data = worksheet.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (요청 데이터): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"요청사항 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

# 기본 설정
try:
    gc = get_gspread_client()
    url = st.secrets["google_sheet"]["url"]
    if "name" not in st.session_state:
        st.error("⚠️ 사용자 이름이 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
        st.stop()
    name = st.session_state["name"]
    today = datetime.date.today()

    next_month = today.replace(day=1) + relativedelta(months=1)
    month_str = next_month.strftime("%Y년 %-m월")
    next_month_start = next_month
    _, last_day = calendar.monthrange(next_month.year, next_month.month)
    next_month_end = next_month.replace(day=last_day)
except NameError as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()
except Exception as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()

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
def initialize_data():
    """페이지에 필요한 모든 데이터를 한 번에 로드하고 세션 상태에 저장합니다."""
    try:
        st.session_state["df_master"] = load_master_data(gc, url)
        st.session_state["df_request"] = load_request_data_page2(gc, url, month_str)
        if st.session_state["df_request"].empty:
            st.warning("⚠️ 요청사항 데이터가 비어 있습니다. Google Sheet를 확인해주세요.")
        st.session_state["df_user_request"] = st.session_state["df_request"][st.session_state["df_request"]["이름"] == name].copy() if not st.session_state["df_request"].empty else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["df_user_master"] = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy() if not st.session_state["df_master"].empty else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"데이터 초기화 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (데이터 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"데이터 초기화 중 오류 발생: {str(e)}")
        st.stop()

# 데이터 새로고침 및 스피너 로직을 통합
def refresh_and_update():
    """데이터를 새로고침하고 UI를 업데이트합니다."""
    try:
        with st.spinner("데이터를 다시 불러오는 중입니다..."):
            st.cache_data.clear()  # 캐시 지우기
            initialize_data()
        st.success("데이터가 새로고침되었습니다.", icon="🔄")
        time.sleep(1)
        st.rerun()  # 새로고침 후 UI 전체를 다시 그립니다.
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {str(e)}")
        st.stop()

# --- 콜백 함수 정의 ---
# 요청사항 추가 콜백 함수
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
                add_placeholder.warning(f"⚠️ {month_str}에는 해당 주차/요일의 날짜가 없습니다. 다른 조합을 선택해주세요.")
                return
            
    if not 날짜정보 and 분류 != "요청 없음":
        add_placeholder.warning("날짜 정보를 올바르게 입력해주세요.")
        return

    # Check for duplicate request
    if 분류 != "요청 없음":
        existing_request = st.session_state["df_request"][
            (st.session_state["df_request"]["이름"] == name) &
            (st.session_state["df_request"]["분류"] == 분류) &
            (st.session_state["df_request"]["날짜정보"] == 날짜정보)
        ]
        if not existing_request.empty:
            add_placeholder.error("⚠️ 이미 존재하는 요청사항입니다.")
            return

    with add_placeholder.container():
        with st.spinner("요청사항을 추가 중입니다..."):
            try:
                sheet = gc.open_by_url(url)
                try:
                    worksheet2 = sheet.worksheet(f"{month_str} 요청")
                except WorksheetNotFound:
                    worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                    worksheet2.append_row(["이름", "분류", "날짜정보"])
                
                # "요청 없음"일 경우 해당 사용자의 모든 요청사항 제거
                if 분류 == "요청 없음":
                    df_to_save = st.session_state["df_request"][st.session_state["df_request"]["이름"] != name].copy()
                    df_to_save = pd.concat([df_to_save, pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": ""}])], ignore_index=True)
                else:
                    # 다른 요청사항 추가: 기존 "요청 없음" 레코드 제거 후 새 요청 추가
                    df_to_save = st.session_state["df_request"][~((st.session_state["df_request"]["이름"] == name) & (st.session_state["df_request"]["분류"] == "요청 없음"))].copy()
                    new_request_data = {"이름": name, "분류": 분류, "날짜정보": 날짜정보}
                    df_to_save = pd.concat([df_to_save, pd.DataFrame([new_request_data])], ignore_index=True)

                df_to_save = df_to_save.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                
                try:
                    worksheet2.clear()
                    worksheet2.update([df_to_save.columns.tolist()] + df_to_save.astype(str).values.tolist())
                except gspread.exceptions.APIError as e:
                    st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                    st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
                    st.stop()
                
                st.session_state["df_request"] = df_to_save
                st.session_state["df_user_request"] = df_to_save[df_to_save["이름"] == name].copy()
            
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (요청 추가): {str(e)}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"요청 추가 중 오류 발생: {str(e)}")
                st.stop()
        
        st.success("요청이 성공적으로 기록되었습니다.")
        time.sleep(1.5)
        st.rerun()

# 요청사항 삭제 콜백 함수
def delete_requests_callback():
    selected_items = st.session_state.get("delete_select", [])
    if not selected_items:
        delete_placeholder.warning("삭제할 항목을 선택해주세요.")
        return

    with delete_placeholder.container():
        with st.spinner("요청사항을 삭제 중입니다..."):
            try:
                sheet = gc.open_by_url(url)
                try:
                    worksheet2 = sheet.worksheet(f"{month_str} 요청")
                except WorksheetNotFound:
                    st.error("요청사항이 저장된 시트를 찾을 수 없습니다.")
                    st.stop()
                
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
                    
                    df_to_save = df_to_save.sort_values(by=["이름", "날짜정보"]).fillna("").reset_index(drop=True)
                    
                    try:
                        worksheet2.clear()
                        worksheet2.update([df_to_save.columns.tolist()] + df_to_save.astype(str).values.tolist())
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
                        st.stop()
                    
                    st.session_state["df_request"] = df_to_save
                    st.session_state["df_user_request"] = df_to_save[df_to_save["이름"] == name].copy()
                else:
                    st.warning("삭제할 항목을 찾을 수 없습니다.")
                    return
            
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (요청 삭제): {str(e)}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"요청 삭제 중 오류 발생: {str(e)}")
                st.stop()
        
        st.success("요청이 성공적으로 삭제되었습니다.")
        time.sleep(1.5)
        st.rerun()

# --- UI 렌더링 시작 ---
# 첫 페이지 로드 시에만 데이터 로드
if "initial_load_done_page2" not in st.session_state:
    try:
        with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
            initialize_data()
        st.session_state["initial_load_done_page2"] = True
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (초기 데이터 로드): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

df_request = st.session_state["df_request"]
df_user_request = st.session_state["df_user_request"]
df_user_master = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()

st.header(f"🙋‍♂️ {name} 님의 {month_str} 요청사항", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    refresh_and_update()

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
                return f"{date_obj.strftime('%-m월 %-d일')} ({weekday_map[date_obj.weekday()]})"
            st.multiselect("요청 일자", [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)], format_func=format_date, key="date_multiselect")
        elif 방식 == "기간 선택":
            st.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end, key="date_range")
        elif 방식 == "주/요일 선택":
            st.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"], key="week_select")
            st.multiselect("요일 선택", ["월", "화", "수", "목", "금", "토", "일"], key="day_select")
            
with col4:
    st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
    st.button("📅 추가", use_container_width=True, on_click=add_request_callback)

add_placeholder = st.empty() # 추가 버튼의 다음 라인에 placeholder 선언

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

    with del_col2:
        st.markdown("<div>&nbsp;</div>", unsafe_allow_html=True)
        st.button("🗑️ 삭제", use_container_width=True, on_click=delete_requests_callback)
    
    delete_placeholder = st.empty() # 삭제 버튼의 다음 라인에 placeholder 선언
else:
    st.info("📍 삭제할 요청사항이 없습니다.")