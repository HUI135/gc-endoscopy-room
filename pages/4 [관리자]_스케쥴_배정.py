import numpy as np
import json
import streamlit as st
import pandas as pd
import calendar
import datetime
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar as st_calendar  # 이름 바꿔주기
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# 관리자 페이지 구현
# st.header("🔒 관리자 페이지", divider = 'rainbow')

# 🔒 관리자 페이지 체크
if "login_success" not in st.session_state or not st.session_state["login_success"]:
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()  # 로그인되지 않으면 페이지 진행 안함

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
    
if st.session_state.get("is_admin_authenticated", False):

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

    # 익월 범위 지정
    # today = datetime.date.today()
    today = datetime.datetime.strptime('2025-03-31', '%Y-%m-%d').date()
    next_month = today.replace(day=1) + relativedelta(months=1)
    next_month_start = next_month
    _, last_day = calendar.monthrange(next_month.year, next_month.month)
    next_month_end = next_month.replace(day=last_day)
    month_str = next_month.strftime("%Y년 %m월")
    ########
    month_str = ("2025년 04월")

    worksheet1 = sheet.worksheet("마스터")
    data = worksheet1.get_all_records()
    df_all = pd.DataFrame(data)  # 데이터를 DataFrame으로 변환
    
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

    try:
        data2 = worksheet2.get_all_records()
        df_all2 = pd.DataFrame(data2)  # 데이터를 DataFrame으로 변환
    except Exception as e:
        # 예외 발생 시 처리
        df_all2 = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.warning(f"데이터를 불러오는 데 문제가 발생했습니다: {e}")
        st.stop()  # 이후 코드를 실행하지 않음

    st.subheader("✨ 최종 마스터")

    st.write(" ")
    st.markdown("**✅ 근무 테이블**")
    # 근무여부 분리 함수
    def split_shift(row):
        shifts = []
        if row["근무여부"] == "오전 & 오후":
            shifts.extend([(row["이름"], row["주차"], row["요일"], "오전"), 
                        (row["이름"], row["주차"], row["요일"], "오후")])
        elif row["근무여부"] == "오전":
            shifts.append((row["이름"], row["주차"], row["요일"], "오전"))
        elif row["근무여부"] == "오후":
            shifts.append((row["이름"], row["주차"], row["요일"], "오후"))
        # "근무없음"은 제외
        return shifts

    # 데이터프레임 변환
    shift_list = []
    for _, row in df_all.iterrows():
        shift_list.extend(split_shift(row))

    # 새로운 데이터프레임 생성
    df_split = pd.DataFrame(shift_list, columns=["이름", "주차", "요일", "시간대"])

    # 요일별, 시간대별 근무 목록 생성
    weekday_order = ["월", "화", "수", "목", "금"]
    time_slots = ["오전", "오후"]
    result = {}

    for day in weekday_order:
        for time in time_slots:
            key = f"{day} {time}"
            # 해당 요일과 시간대 필터링
            df_filtered = df_split[(df_split["요일"] == day) & (df_split["시간대"] == time)]
            
            # "매주" 근무자
            every_week = df_filtered[df_filtered["주차"] == "매주"]["이름"].unique()
            
            # 특정 주차 근무자
            specific_weeks = df_filtered[df_filtered["주차"] != "매주"]
            specific_week_dict = {}
            for name in specific_weeks["이름"].unique():
                weeks = specific_weeks[specific_weeks["이름"] == name]["주차"].tolist()
                if weeks:
                    specific_week_dict[name] = sorted(weeks, key=lambda x: int(x.replace("주", "")))
            
            # 결과 문자열 생성
            employees = list(every_week)
            for name, weeks in specific_week_dict.items():
                week_str = ",".join(weeks)
                employees.append(f"{name}({week_str})")
            
            result[key] = ", ".join(employees) if employees else ""  # 근무이 없으면 빈 문자열

    # 데이터프레임으로 변환
    df_result = pd.DataFrame(list(result.items()), columns=["시간대", "근무"])

    # 결과 출력
    st.dataframe(df_result)

    st.markdown("**☑️ 보충 테이블**")
    # 주차와 요일별 근무 상태를 나누기
    weekday_list = ["월", "화", "수", "목", "금"]
    shift_list = ["오전", "오후"]

    # 주차별, 요일별, 시간대별로 근무자 리스트를 필터링
    week_shift_data = {}
    for week in df_all["주차"].unique():
        week_shift_data[week] = {}
        for weekday in weekday_list:
            week_shift_data[week][weekday] = {}
            for shift in shift_list:
                week_shift_data[week][weekday][shift] = df_all[
                    (df_all["주차"] == week) & 
                    (df_all["요일"] == weekday) & 
                    (df_all["근무여부"].str.contains(shift))
                ]["이름"].tolist()

    # 전체 근무 목록
    names_in_master = set(df_all["이름"].unique())

    # 근무 이름에서 주차/요청 정보 제거 함수
    def clean_name(employee):
        if "(" in employee:
            return employee.split("(")[0]
        return employee

    # 보충 계산 함수
    def calculate_supplement(df_result, names_in_master):
        supplement = []
        weekday_order = ["월", "화", "수", "목", "금"]
        shift_list = ["오전", "오후"]

        for day in weekday_order:
            for shift in shift_list:
                time_slot = f"{day} {shift}"
                # 마스터에서 해당 시간대 데이터 가져오기
                row = df_result[df_result["시간대"] == time_slot].iloc[0]
                employees = [clean_name(emp.strip()) for emp in row["근무"].split(", ")]
                current_employees = set(employees)
                supplement_employees = names_in_master - current_employees

                # 당일 오전 근무 여부 확인 (오후 보충 조건)
                if shift == "오후":
                    morning_slot = f"{day} 오전"
                    morning_employees = set()
                    if morning_slot in df_result["시간대"].values:
                        morning_row = df_result[df_result["시간대"] == morning_slot].iloc[0]
                        morning_employees = set(clean_name(emp.strip()) for emp in morning_row["근무"].split(", "))
                    for emp in supplement_employees.copy():
                        if emp not in morning_employees:
                            supplement_employees.remove(emp)
                            supplement_employees.add(f"{emp}🔺")

                # 결과 추가
                supplement.append({
                    "시간대": time_slot,
                    "보충": ", ".join(sorted(supplement_employees)) if supplement_employees else ""
                })

        return pd.DataFrame(supplement)

    # 보충 데이터프레임 생성
    df_supplement = calculate_supplement(df_result, names_in_master)

    # 결과 출력
    st.dataframe(df_supplement)

    st.markdown("**✉️ 요청사항 테이블**")
    st.dataframe(df_all2)

# 관리자 권한 확인
else:    
    st.warning("관리자 권한이 없습니다.")
    st.stop()  # 관리자 권한 없으면 페이지 진행 안함