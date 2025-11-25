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
from gspread.exceptions import WorksheetNotFound, APIError
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

# 기본 설정
try:
    gc = get_gspread_client()
    url = st.secrets["google_sheet"]["url"]
    if "name" not in st.session_state:
        st.error("⚠️ 사용자 이름이 설정되지 않았습니다. Home 페이지에서 로그인해주세요.")
        st.stop()
    name = st.session_state["name"]
    
    # 오늘 날짜를 기준으로 다음 달 1일을 계산합니다.
    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    now = datetime.datetime.now(kst)
    today = now.date()
    # today = now.date() - relativedelta(months=1)

    next_month_date = today.replace(day=1) + relativedelta(months=1)

    # 모든 날짜 관련 변수를 다음 달 기준으로 설정합니다.
    month_str = next_month_date.strftime("%Y년 %-m월")
    month_start = next_month_date
    year, month = next_month_date.year, next_month_date.month
    _, last_day = calendar.monthrange(year, month)
    month_end = next_month_date.replace(day=last_day)

    week_nums = sorted(set(d.isocalendar()[1] for d in pd.date_range(start=month_start, end=month_end)))
    week_labels = [f"{i+1}주" for i in range(len(week_nums))]
except NameError as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()
except Exception as e:
    st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
    st.error(f"초기 설정 중 오류 발생: {str(e)}")
    st.stop()

# 캘린더 이벤트 생성 함수 (마스터, 토요일, 요청사항, 휴관일 모두 처리)
def create_calendar_events(df_master, df_request, df_saturday_schedule, df_closing_days, current_user_name):
    events = []
    
    # 빠른 조회를 위해 휴관일 날짜를 세트(set)으로 변환
    closing_dates_set = set(df_closing_days['날짜'].dt.date) if not df_closing_days.empty else set()

    # --- 1. 마스터 데이터(평일)에서 이벤트 생성 (휴관일 제외) ---
    status_colors_master = {"오전": "#48A6A7", "오후": "#FCB454", "오전 & 오후": "#F38C79"}
    if not df_master.empty:
        master_data = {}
        요일리스트 = ["월", "화", "수", "목", "금"] # 평일만 처리
        every_week_df = df_master[df_master["주차"] == "매주"]
        
        for week in week_labels:
            master_data[week] = {}
            week_df = df_master[df_master["주차"] == week]
            for day in 요일리스트:
                day_specific = week_df[week_df["요일"] == day]
                if not day_specific.empty:
                    master_data[week][day] = day_specific.iloc[0]["근무여부"]
                elif not every_week_df.empty:
                    day_every = every_week_df[every_week_df["요일"] == day]
                    master_data[week][day] = day_every.iloc[0]["근무여부"] if not day_every.empty else "근무없음"
                else:
                    master_data[week][day] = "근무없음"

        weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금"}
        _, last_day_of_month = calendar.monthrange(year, month)
        first_sunday = next((day for day in range(1, 8) if datetime.date(year, month, day).weekday() == 6), None)

        for day_num in range(1, last_day_of_month + 1):
            date_obj = datetime.date(year, month, day_num)
            
            # 만약 해당 날짜가 휴관일이면, 마스터 일정 이벤트를 생성하지 않음
            if date_obj in closing_dates_set:
                continue

            if date_obj.weekday() in weekday_map: # 평일(월~금)만 해당
                day_name = weekday_map[date_obj.weekday()]
                if first_sunday is None: week_num = (date_obj.day + datetime.date(year, month, 1).weekday()) // 7
                else: week_num = (day_num - first_sunday) // 7 + 1 if day_num >= first_sunday else 0
                if week_num >= len(week_labels): continue
                week = week_labels[week_num]
                status = master_data.get(week, {}).get(day_name, "근무없음")
                if status and status != "근무없음":
                    events.append({"title": f"{status}", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors_master.get(status, "#E0E0E0")})

    # --- 2. 토요/휴일 스케줄 데이터에서 이벤트 생성 ---
    status_colors_saturday = {"토요근무": "#6A5ACD", "당직": "#FF6347"}
    if not df_saturday_schedule.empty:
        saturdays_in_month = df_saturday_schedule[(df_saturday_schedule['날짜'].dt.year == year) & (df_saturday_schedule['날짜'].dt.month == month)]
        for _, row in saturdays_in_month.iterrows():
            date_obj = row['날짜'].date()
            if isinstance(row.get('근무', ''), str) and current_user_name in row.get('근무', ''):
                events.append({"title": "토요근무", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors_saturday.get("토요근무")})
            if isinstance(row.get('당직', ''), str) and current_user_name == row.get('당직', '').strip():
                events.append({"title": "당직", "start": date_obj.strftime("%Y-%m-%d"), "color": status_colors_saturday.get("당직")})

    # --- 3. 요청사항 이벤트 생성 ---
    status_colors_request = {"휴가": "#A1C1D3", "학회": "#B4ABE4", "보충 어려움(오전)": "#FFD3B5", "보충 어려움(오후)": "#FFD3B5", "보충 불가(오전)": "#FFB6C1", "보충 불가(오후)": "#FFB6C1", "꼭 근무(오전)": "#C3E6CB", "꼭 근무(오후)": "#C3E6CB"}
    label_map = {"휴가": "휴가🎉", "학회": "학회📚", "보충 어려움(오전)": "보충 어려움(오전)", "보충 어려움(오후)": "보충 어려움(오후)", "보충 불가(오전)": "보충 불가(오전)", "보충 불가(오후)": "보충 불가(오후)", "꼭 근무(오전)": "꼭근무(오전)", "꼭 근무(오후)": "꼭근무(오후)"}
    if not df_request.empty:
        for _, row in df_request.iterrows():
            분류, 날짜정보 = row["분류"], row["날짜정보"]
            if not 날짜정보 or 분류 == "요청 없음": continue
            if "~" in 날짜정보:
                시작_str, 종료_str = [x.strip() for x in 날짜정보.split("~")]
                시작 = datetime.datetime.strptime(시작_str, "%Y-%m-%d").date()
                종료 = datetime.datetime.strptime(종료_str, "%Y-%m-%d").date()
                events.append({"title": f"{label_map.get(분류, 분류)}", "start": 시작.strftime("%Y-%m-%d"), "end": (종료 + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
            else:
                for 날짜 in [d.strip() for d in 날짜정보.split(",")]:
                    try:
                        dt = datetime.datetime.strptime(날짜, "%Y-%m-%d").date()
                        events.append({"title": f"{label_map.get(분류, 분류)}", "start": dt.strftime("%Y-%m-%d"), "color": status_colors_request.get(분류, "#E0E0E0")})
                    except: continue

    # --- 4. 휴관일 이벤트 생성 ---
    if not df_closing_days.empty:
        for date_obj in df_closing_days['날짜']:
            events.append({
                "title": "휴관일", 
                "start": date_obj.strftime("%Y-%m-%d"), 
                "color": "#DC143C"  # 붉은색 계열 (Crimson)
            })

    return events

# --- 초기 데이터 로딩 및 세션 상태 초기화 ---
def initialize_data():
    """페이지에 필요한 모든 데이터를 한 번에 로드하고 세션 상태에 저장합니다."""
    try:
        # 스프레드시트를 한 번만 엽니다.
        sheet = gc.open_by_url(url)

        # 1. 마스터 데이터 로드
        worksheet_master = sheet.worksheet("마스터")
        df_master = pd.DataFrame(worksheet_master.get_all_records())
        
        # 2. 요청사항 데이터 로드 및 시트 객체 저장
        sheet_name = f"{month_str} 요청"
        try:
            worksheet_request = sheet.worksheet(sheet_name)
        except WorksheetNotFound:
            worksheet_request = sheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            worksheet_request.append_row(["이름", "분류", "날짜정보"])
            st.info(f"'{sheet_name}' 시트가 새로 생성되었습니다.")
        df_request = pd.DataFrame(worksheet_request.get_all_records())

        if df_request.empty:
            df_request = pd.DataFrame(columns=["이름", "분류", "날짜정보"])

        # 3. 모든 데이터를 세션 상태에 저장 (worksheet 객체 포함)
        st.session_state["worksheet_master"] = worksheet_master
        st.session_state["worksheet_request"] = worksheet_request
        st.session_state["df_master"] = df_master
        st.session_state["df_request"] = df_request
        st.session_state["df_user_master"] = df_master[df_master["이름"] == name].copy() if not df_master.empty else pd.DataFrame()
        # st.session_state["df_user_request"] = df_request[df_request["이름"] == name].copy() if not df_request.empty else pd.DataFrame()

    except (APIError, Exception) as e:
        st.error(f"데이터 초기화 중 오류가 발생했습니다: {e}")
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
            날짜범위 = st.session_state.get("date_range", []) 
            if isinstance(날짜범위, list) and len(날짜범위) == 2:
                날짜정보 = f"{날짜범위[0].strftime('%Y-%m-%d')} ~ {날짜범위[1].strftime('%Y-%m-%d')}"
            elif isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                날짜정보 = f"{날짜범위[0].strftime('%Y-%m-%d')} ~ {날짜범위[1].strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.session_state.get("week_select", [])
            선택요일 = st.session_state.get("day_select", [])
            날짜목록 = []

            if 선택주차 and 선택요일:
                c = calendar.Calendar(firstweekday=6) # 주는 일요일부터 시작
                month_calendar = c.monthdatescalendar(year, month)

                요일_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}
                선택된_요일_인덱스 = [요일_map[요일] for 요일 in 선택요일]

                # ▼▼▼ [수정된 부분] '첫째주' 등을 생성하는 로직을 삭제하고 week_labels를 직접 사용합니다. ▼▼▼
                for i, week in enumerate(month_calendar):
                    # 해당 월의 주차 개수를 초과하는 경우를 방지
                    if i < len(week_labels):
                        # UI에서 사용하는 주차 이름 ('1주', '2주' 등)을 직접 가져옴
                        current_week_label = week_labels[i]

                        # 사용자가 선택한 주차에 현재 주차가 포함되어 있는지 확인
                        if "매주" in 선택주차 or current_week_label in 선택주차:
                            for date in week:
                                # 해당 월의 날짜이면서, 선택한 요일이 맞는지 확인
                                if date.month == month and date.weekday() in 선택된_요일_인덱스:
                                    날짜목록.append(date.strftime("%Y-%m-%d"))

            날짜정보 = ", ".join(sorted(list(set(날짜목록))))
            if not 날짜목록 and 선택주차 and 선택요일:
                add_placeholder.warning(f"⚠️ {month_str}에는 해당 주차/요일의 날짜가 없습니다. 다른 조합을 선택해주세요.")
                time.sleep(1.5)
                return
            
    if not 날짜정보 and 분류 != "요청 없음":
        add_placeholder.warning("날짜 정보를 올바르게 입력해주세요.")
        return

    if 분류 != "요청 없음":
        existing_request = st.session_state["df_request"][
            (st.session_state["df_request"]["이름"] == name) &
            (st.session_state["df_request"]["분류"] == 분류) &
            (st.session_state["df_request"]["날짜정보"] == 날짜정보)
        ]
        if not existing_request.empty:
            add_placeholder.error("⚠️ 이미 존재하는 요청사항입니다.")
            time.sleep(1.5)
            return

    with add_placeholder.container():
        with st.spinner("요청사항을 추가 중입니다..."):
            try:
                # 세션에서 워크시트 객체를 가져옵니다.
                worksheet2 = st.session_state["worksheet_request"]

                # '요청 없음'은 특별 처리: 기존 것을 지우고 추가해야 합니다. (아래 삭제 로직 참조)
                if 분류 == "요청 없음":
                    st.error("'요청 없음' 기능은 기존 모든 요청을 삭제해야 하므로, 삭제 로직과 결합해야 합니다. (별도 구현 필요)")
                    # 여기서 사용자(name)의 모든 행을 삭제하는 로직을 먼저 실행한 뒤,
                    # new_request_data = [name, 분류, ""]
                    # worksheet2.append_row(new_request_data)
                    # 위와 같이 추가해야 합니다.
                    return

                # --- ▼▼▼ 핵심 수정 부분 ▼▼▼ ---
                # 시트 맨 끝에 한 줄만 추가합니다.
                new_request_data = [name, 분류, 날짜정보]
                worksheet2.append_row(new_request_data)
                # --- ▲▲▲ 핵심 수정 부분 ▲▲▲ ---

                # 성공 후, 화면에 즉시 반영하기 위해 st.session_state의 DataFrame도 업데이트합니다.
                df_request = st.session_state["df_request"]
                new_df = pd.DataFrame([{"이름": name, "분류": 분류, "날짜정보": 날짜정보}])
                st.session_state["df_request"] = pd.concat([df_request, new_df], ignore_index=True)

            except gspread.exceptions.APIError as e:
                st.warning(f"Google Sheets API 오류: {str(e)}")
                st.stop()
            except Exception as e:
                st.error(f"요청 추가 중 오류 발생: {str(e)}")
                st.stop()

        st.success("요청이 성공적으로 기록되었습니다.")
        time.sleep(1.5)
    
    st.session_state.date_multiselect = []
    st.session_state.week_select = []
    st.session_state.day_select = []
    st.session_state.category_select = "휴가"
    
# 요청사항 삭제 콜백 함수
def delete_requests_callback():
    selected_items = st.session_state.get("delete_select", [])
    if not selected_items:
        delete_placeholder.warning("삭제할 항목을 선택해주세요.")
        return

    with delete_placeholder.container():
        with st.spinner("요청사항을 삭제 중입니다..."):
            try:
                worksheet2 = st.session_state["worksheet_request"]
                selected_items = st.session_state.get("delete_select", []) # UI에서 선택된 항목들

                # --- ▼▼▼ 핵심 수정 부분 ▼▼▼ ---
                # 1. 시트의 현재 데이터를 모두 실시간으로 다시 가져옵니다.
                all_records = worksheet2.get_all_records()
                rows_to_delete_indices = []

                # 2. 삭제할 행의 인덱스를 찾습니다. (헤더가 1행이므로 데이터는 2행부터 시작)
                for index, record in enumerate(all_records):
                    # UI에 표시된 형식('분류 - 날짜정보')과 동일하게 문자열 생성
                    item_str = f"{record['분류']} - {record['날짜정보']}"
                    # 현재 로그인한 사용자의 요청이고, 삭제 대상으로 선택된 항목인지 확인
                    if record['이름'] == name and item_str in selected_items:
                        # gspread는 1-기반 인덱스 사용, +2는 헤더(1행)와 0-기반(enumerate) 때문
                        rows_to_delete_indices.append(index + 2)

                # 3. 찾은 인덱스의 행들을 삭제합니다. (중요: 인덱스가 엉키지 않게 역순으로 정렬 후 삭제)
                if rows_to_delete_indices:
                    for row_index in sorted(rows_to_delete_indices, reverse=True):
                        worksheet2.delete_rows(row_index)
                # --- ▲▲▲ 핵심 수정 부분 ▲▲▲ ---

                # 성공 후, 화면에 즉시 반영하기 위해 st.session_state의 DataFrame도 업데이트합니다.
                # (간단하게는 전체 데이터를 다시 불러오는 것이 가장 정확합니다.)
                updated_records = worksheet2.get_all_records()
                st.session_state["df_request"] = pd.DataFrame(updated_records)


            except gspread.exceptions.APIError as e:
                st.warning(f"Google Sheets API 오류: {str(e)}")
                st.stop()
            except Exception as e:
                st.error(f"요청 삭제 중 오류 발생: {str(e)}")
                st.stop()

        st.success("요청이 성공적으로 삭제되었습니다.")
        time.sleep(1.5)

# 토요/휴일 스케줄 데이터 로드 함수 (새로 추가)
@st.cache_data(show_spinner=False)
def load_saturday_schedule(_gc, url, year):
    """지정된 연도의 토요/휴일 스케줄 데이터를 로드하는 함수"""
    try:
        sheet = _gc.open_by_url(url)
        worksheet_name = f"{year}년 토요/휴일 스케줄"
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            st.warning(f"⚠️ '{worksheet_name}' 시트에 데이터가 없습니다.")
            return pd.DataFrame(columns=["날짜", "근무", "당직"])
        
        df = pd.DataFrame(data)
        # '날짜' 열이 비어있거나 잘못된 형식의 데이터를 제외하고 datetime으로 변환
        df = df[df['날짜'] != '']
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True) # 날짜 변환 실패한 행 제거
        return df
    except WorksheetNotFound:
        st.info(f"'{year}년 토요/휴일 스케줄' 시트를 찾을 수 없습니다. 토요일 근무가 표시되지 않을 수 있습니다.")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])
    except Exception as e:
        st.error(f"토요/휴일 스케줄 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame(columns=["날짜", "근무", "당직"])


@st.cache_data(show_spinner=False)
def load_closing_days(_gc, url, year):
    """지정된 연도의 휴관일 데이터를 로드하는 함수"""
    try:
        sheet = _gc.open_by_url(url)
        worksheet_name = f"{year}년 휴관일"
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame(columns=["날짜"])
        
        df = pd.DataFrame(data)
        df = df[df['날짜'] != '']
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df.dropna(subset=['날짜'], inplace=True)
        return df
    except WorksheetNotFound:
        st.info(f"'{worksheet_name}' 시트를 찾을 수 없습니다. 휴관일이 표시되지 않습니다.")
        return pd.DataFrame(columns=["날짜"])
    except Exception as e:
        st.error(f"휴관일 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame(columns=["날짜"])


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
        st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (초기 데이터 로드): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"초기 데이터 로드 중 오류 발생: {str(e)}")
        st.stop()

df_request = st.session_state["df_request"]
df_user_request = df_request[df_request["이름"] == name].copy()
df_user_master = st.session_state["df_master"][st.session_state["df_master"]["이름"] == name].copy()

if 'date_range' not in st.session_state:
    st.session_state.date_range = [] 

st.header(f"🙋‍♂️ {name} 님의 {month_str} 요청사항", divider='rainbow')

if st.button("🔄 새로고침 (R)"):
    # 캐시와 로딩 완료 상태를 모두 초기화합니다.
    st.cache_data.clear()
    st.session_state.pop("initial_load_done_page2", None)
    # 페이지를 새로고침하면 맨 위의 로딩 로직이 다시 실행됩니다.
    st.rerun()

st.write("- 휴가 / 보충 불가 / 꼭 근무 관련 요청사항이 있을 경우 반드시 기재해 주세요.\n- 요청사항은 매월 기재해 주셔야 하며, 별도 요청이 없을 경우에도 반드시 '요청 없음'을 입력해 주세요.")

# 토요 스케줄 데이터 로드 (추가)
df_saturday = load_saturday_schedule(gc, url, year)

# ▼▼▼ [수정됨] 휴관일 데이터를 불러오고, 캘린더와 날짜 선택 목록에 모두 적용 ▼▼▼
df_closing_days = load_closing_days(gc, url, year)
closing_dates_set = set(df_closing_days['날짜'].dt.date) if not df_closing_days.empty else set()

# events_combined 생성 부분 수정 (휴관일 데이터와 사용자 이름 추가)
events_combined = create_calendar_events(df_user_master, df_user_request, df_saturday, df_closing_days, name)

if not events_combined:
    st.info("☑️ 당월에 입력하신 요청사항 또는 마스터 스케줄이 없습니다.")
    calendar_options = {"initialView": "dayGridMonth", "initialDate": month_start.strftime("%Y-%m-%d"), "height": 600, "headerToolbar": {"left": "", "center": "title", "right": ""}}
    # st_calendar(options=calendar_options)
else:
    calendar_options = {"initialView": "dayGridMonth", "initialDate": month_start.strftime("%Y-%m-%d"), "editable": False, "selectable": False, "eventDisplay": "block", "dayHeaderFormat": {"weekday": "short"}, "themeSystem": "bootstrap", "height": 700, "headerToolbar": {"left": "", "center": "title", "right": ""}, "showNonCurrentDates": True, "fixedWeekCount": False, "eventOrder": "title"}
    # st_calendar(events=events_combined, options=calendar_options)

# st.html 부터 시작하는 부분을 아래 코드로 전부 교체하세요.
st.html("""
<style>
    /* --- 1. 공통 스타일 --- */
    .calendar-title { text-align: center; font-size: 24px; font-weight: bold; margin-bottom: 20px; }
    .schedule-container {
        background-color: var(--secondary-background-color);
        color: var(--text-color);
        border: 1px solid rgba(128, 128, 128, 0.4);
        padding: 10px;
        border-radius: 10px; /* 스케줄 컨테이너도 동일하게 둥글게 */
        margin-bottom: 15px;
    }

    /* --- 2. HTML 캘린더 스타일 --- */

    .html-calendar {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
    }
    .html-calendar th, .html-calendar td {
        border: 1px solid rgba(128, 128, 128, 0.4);
        vertical-align: top;
        padding: 0;
    }
    .html-calendar th {
        font-weight: bold;
        text-align: center;
        padding: 10px 0;
        background-color: var(--secondary-background-color);
        color: var(--text-color);
        border-bottom: 2px solid rgba(128, 128, 128, 0.4);
    }
    
    .day-cell-content-wrapper {
        min-height: 120px;
        padding: 6px;
    }

    .html-calendar .day-number { font-weight: bold; font-size: 14px; margin-bottom: 5px; }
    .html-calendar .other-month .day-number { color: #ccc; }
    .html-calendar .saturday { color: #4169E1 !important; }
    .html-calendar .sunday { color: #DC143C !important; }

    .event-item {
        font-size: 13px; padding: 4px 5px; border-radius: 3px;
        margin-bottom: 3px; color: white; overflow: hidden;
        text-overflow: ellipsis; white-space: nowrap;
    }

    /* --- 3. 모바일 화면 대응 --- */
    @media (max-width: 768px) {
        .calendar-table-container { overflow-x: auto; }
        .html-calendar { min-width: 500px; }
        
        .day-cell-content-wrapper {
            min-height: 90px;
        }
        
        .day-number, .html-calendar th { font-size: 11px !important; }
        .event-item {
            font-size: 11px !important; padding: 4px !important;
            white-space: normal !important; word-break: break-all !important;
            line-height: 1.1 !important;
        }
    }
</style>
""")

if df_user_request.empty:
    with st.container(border=True):
        st.write(f"🔔 {month_str}에 등록하신 '요청사항'이 없습니다.")

# 2. 캘린더 UI 렌더링 (HTML Table 방식 - 최종 수정)

st.write(" ")
st.markdown(f'<div class="calendar-title">{month_str} 요청사항</div>', unsafe_allow_html=True)

events_by_date = {}
for event in events_combined:
    start_date = datetime.datetime.strptime(event['start'], "%Y-%m-%d").date()
    if 'end' in event and event['start'] != event['end']:
        end_date = datetime.datetime.strptime(event['end'], "%Y-%m-%d").date()
        for i in range((end_date - start_date).days):
            current_date = start_date + datetime.timedelta(days=i)
            if current_date not in events_by_date:
                events_by_date[current_date] = []
            events_by_date[current_date].append(event)
    else:
        if start_date not in events_by_date:
            events_by_date[start_date] = []
        events_by_date[start_date].append(event)

cal = calendar.Calendar(firstweekday=6)
month_days = cal.monthdatescalendar(year, month)
days_of_week = ["일", "월", "화", "수", "목", "금", "토"] 

html_string = "<div class='calendar-table-container'>"
html_string += "<table class='html-calendar'>"
html_string += "<thead><tr>"
for day in days_of_week:
    day_class = ""
    if day == "토": day_class = "saturday"
    elif day == "일": day_class = "sunday"
    html_string += f"<th class='{day_class}'>{day}</th>"
html_string += "</tr></thead>"

html_string += "<tbody>"
for week in month_days:
    html_string += "<tr>"
    for day_date in week:
        event_html = ""
        if day_date in events_by_date:
            for event in events_by_date[day_date]:
                color = event.get('color', '#6c757d')
                title = event['title']
                event_html += f"<div class='event-item' style='background-color:{color};' title='{title}'>{title}</div>"
        
        cell_class = ""
        if day_date.month != month: cell_class += " other-month"
        if day_date.weekday() == 6: cell_class += " sunday"
        if day_date.weekday() == 5: cell_class += " saturday"

        # ▼▼▼ [핵심 수정] 셀(td) 안에 내용물을 감싸는 div 추가 ▼▼▼
        html_string += f"<td class='{cell_class}'>"
        html_string += "<div class='day-cell-content-wrapper'>"
        html_string += f"<div class='day-number'>{day_date.day}</div>"
        html_string += f"<div class='events-container'>{event_html}</div>"
        html_string += "</div></td>"
    html_string += "</tr>"
html_string += "</tbody></table></div>"

st.markdown(html_string, unsafe_allow_html=True)

# 이번 달 토요/휴일 스케줄 필터링 및 출력
st.write("") # 캘린더와 간격을 주기 위해 빈 줄 추가
current_month_schedule_df = df_saturday[
    (df_saturday['날짜'].dt.year == year) & 
    (df_saturday['날짜'].dt.month == month)
].sort_values(by='날짜')

if not current_month_schedule_df.empty:
    # 요일 한글 변환 맵
    weekday_map_ko = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    
    # 날짜를 "월 일(요일)" 형식의 리스트로 변환
    schedule_list = [
        date.strftime(f"%-m월 %-d일({weekday_map_ko[date.weekday()]})") 
        for date in current_month_schedule_df['날짜']
    ]
    
    # 최종 문자열 생성
    schedule_str = ", ".join(schedule_list)
    
    styled_text = f"""
    <div class="schedule-container">
        📅 <strong>이번 달 토요/휴일 스케줄:</strong> {schedule_str}
    </div>
    """
    st.markdown(styled_text, unsafe_allow_html=True)

else:
    # 스케줄이 없을 경우에도 동일한 스타일 적용
    styled_text = """
    <div class="schedule-container">
        📅 이번 달에는 예정된 토요/휴일 근무가 없습니다.
    </div>
    """
    st.markdown(styled_text, unsafe_allow_html=True)
st.divider()

# 요청사항 입력 UI
st.markdown(f"<h6 style='font-weight:bold;'>🟢 요청사항 입력</h6>", unsafe_allow_html=True)
요청분류 = ["휴가", "학회", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
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
            # 1. 해당 월의 모든 평일(월~금) 날짜를 가져옵니다.
            all_days_in_month = [month_start + datetime.timedelta(days=i) for i in range((month_end - month_start).days + 1)]
            weekdays_in_month = [day for day in all_days_in_month if day.weekday() < 5]

            # 2. '토요/휴일 스케줄'에 등록된 날짜를 가져옵니다.
            schedule_dates = df_saturday[
                (df_saturday['날짜'].dt.year == year) &
                (df_saturday['날짜'].dt.month == month)
            ]['날짜'].dt.date.tolist()

            # 3. 두 리스트를 합치고, 중복을 제거합니다.
            base_selectable_dates = sorted(list(set(weekdays_in_month + schedule_dates)))
            
            # ▼▼▼ [수정됨] 최종 선택지에서 휴관일을 제외합니다. ▼▼▼
            selectable_dates = [d for d in base_selectable_dates if d not in closing_dates_set]
            
            # 날짜 포맷팅 함수 정의
            weekday_map = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
            def format_date(date_obj):
                return f"{date_obj.strftime('%-m월 %-d일')} ({weekday_map[date_obj.weekday()]})"
            
            # 수정된 날짜 리스트로 multiselect 위젯 생성
            st.multiselect("요청 일자", 
                          selectable_dates, 
                          format_func=format_date, 
                          key="date_multiselect")

        # ▼▼▼ [추가된 코드] 기간 선택 방식에 대한 UI를 추가합니다. ▼▼▼
        elif 방식 == "기간 선택":
            st.date_input(
                "요청 기간",
                key="date_range",
                value=(), # 초기 선택값을 비워둡니다.
                min_value=month_start,
                max_value=month_end
            )

        # ▼▼▼ [추가된 코드] 주/요일 선택 방식에 대한 UI를 추가합니다. ▼▼▼
        elif 방식 == "주/요일 선택":
            week_options = ["매주"] + week_labels
            day_options = ["월", "화", "수", "목", "금", "토", "일"]
            
            sub_col1, sub_col2 = st.columns(2)
            with sub_col1:
                st.multiselect("주차 선택", week_options, key="week_select")
            with sub_col2:
                st.multiselect("요일 선택", day_options, key="day_select")

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