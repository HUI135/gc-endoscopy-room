import streamlit as st
import pandas as pd
import datetime
import calendar
from io import BytesIO
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import time
import io
import xlsxwriter
import platform
import openpyxl
import random
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
from openpyxl.comments import Comment
from datetime import timedelta
import menu

st.set_page_config(page_title="스케줄 배정", page_icon="🗓️", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# random.seed(42)

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]
today = datetime.date.today()
month_dt = today.replace(day=1) + relativedelta(months=1)
month_str = month_dt.strftime("%Y년 %-m월")
_, last_day = calendar.monthrange(month_dt.year, month_dt.month)

# Google Sheets 클라이언트 초기화
@st.cache_resource # 이 함수 자체를 캐싱하여 불필요한 초기화 반복 방지
@st.cache_resource
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        gc = gspread.authorize(credentials)
        return gc
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {e.response.status_code} - {e.response.text}")
        st.stop()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 인증 정보 로드 중 오류: {type(e).__name__} - {e}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 클라이언트 초기화 또는 인증 실패: {type(e).__name__} - {e}")
        st.stop()


# 데이터 로드 함수 (세션 상태 활용으로 쿼터 절약)
@st.cache_data(ttl=3600) # 데이터를 1시간 동안 캐시. 개발 중에는 ttl을 0으로 설정하거나 캐시를 자주 지우세요.
@st.cache_data(ttl=3600)
def load_data_page5():
    required_keys = ["df_master", "df_request", "df_cumulative", "df_shift", "df_supplement"]
    if "data_loaded" not in st.session_state or not st.session_state["data_loaded"] or not all(key in st.session_state for key in required_keys):
        url = st.secrets["google_sheet"]["url"]
        try:
            gc = get_gspread_client()
            if gc is None:
                st.stop()
            sheet = gc.open_by_url(url)
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (스프레드시트 열기): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"스프레드시트 URL 로드 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"스프레드시트 열기 실패: {type(e).__name__} - {e}")
            st.stop()

        # 마스터 시트
        try:
            worksheet1 = sheet.worksheet("마스터")
            st.session_state["df_master"] = pd.DataFrame(worksheet1.get_all_records())
            st.session_state["worksheet1"] = worksheet1
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 ('마스터' 시트 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except gspread.exceptions.WorksheetNotFound:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error("❌ '마스터' 시트를 찾을 수 없습니다. 시트 이름을 확인해주세요.")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"'마스터' 시트 로드 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"'마스터' 시트 로드 실패: {type(e).__name__} - {e}")
            st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            st.session_state["data_loaded"] = False
            st.stop()

        # 요청사항 시트
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 요청' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            try:
                worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                worksheet2.append_row(["이름", "분류", "날짜정보"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "요청 없음", ""] for name in names_in_master]
                for row in new_rows:
                    try:
                        worksheet2.append_row(row)
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (요청사항 시트 초기화): {e.response.status_code} - {e.response.text}")
                        st.stop()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 ('{month_str} 요청' 시트 생성): {e.response.status_code} - {e.response.text}")
                st.stop()
            except NameError as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 요청' 시트 생성 중 오류: {type(e).__name__} - {e}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 요청' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.stop()

        try:
            st.session_state["df_request"] = pd.DataFrame(worksheet2.get_all_records()) if worksheet2.get_all_records() else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
            st.session_state["worksheet2"] = worksheet2
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (요청사항 데이터 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"요청사항 데이터 로드 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"요청사항 데이터 로드 실패: {type(e).__name__} - {e}")
            st.stop()

        # 누적 시트
        try:
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 누적' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            try:
                worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
                worksheet4.append_row([f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "", "", "", ""] for name in names_in_master]
                for row in new_rows:
                    try:
                        worksheet4.append_row(row)
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (누적 시트 초기화): {e.response.status_code} - {e.response.text}")
                        st.stop()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 ('{month_str} 누적' 시트 생성): {e.response.status_code} - {e.response.text}")
                st.stop()
            except NameError as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 누적' 시트 생성 중 오류: {type(e).__name__} - {e}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 누적' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.stop()
        
        try:
            df_cumulative_temp = pd.DataFrame(worksheet4.get_all_records()) if worksheet4.get_all_records() else pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
            if not df_cumulative_temp.empty:
                df_cumulative_temp.rename(columns={df_cumulative_temp.columns[0]: '이름'}, inplace=True)
                for col_name in ["오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]:
                    if col_name in df_cumulative_temp.columns:
                        df_cumulative_temp[col_name] = pd.to_numeric(df_cumulative_temp[col_name], errors='coerce').fillna(0).astype(int)
            st.session_state["df_cumulative"] = df_cumulative_temp
            st.session_state["worksheet4"] = worksheet4
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (누적 데이터 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"누적 데이터 로드 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"누적 데이터 로드 실패: {type(e).__name__} - {e}")
            st.stop()

        # df_shift와 df_supplement 생성 및 세션 상태에 저장
        try:
            st.session_state["df_shift"] = generate_shift_table(st.session_state["df_master"])
            st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], st.session_state["df_master"]["이름"].unique())
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"근무/보충 테이블 생성 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"근무/보충 테이블 생성 실패: {type(e).__name__} - {e}")
            st.stop()

        st.session_state["data_loaded"] = True

# 근무 테이블 생성 함수
def generate_shift_table(df_master):
    def split_shift(row):
        shifts = []
        if row["근무여부"] == "오전 & 오후":
            shifts.extend([(row["이름"], row["주차"], row["요일"], "오전"), (row["이름"], row["주차"], row["요일"], "오후")])
        elif row["근무여부"] in ["오전", "오후"]:
            shifts.append((row["이름"], row["주차"], row["요일"], row["근무여부"]))
        return shifts

    shift_list = [shift for _, row in df_master.iterrows() for shift in split_shift(row)]
    df_split = pd.DataFrame(shift_list, columns=["이름", "주차", "요일", "시간대"])

    weekday_order = ["월", "화", "수", "목", "금"]
    time_slots = ["오전", "오후"]
    result = {}
    for day in weekday_order:
        for time in time_slots:
            key = f"{day} {time}"
            df_filtered = df_split[(df_split["요일"] == day) & (df_split["시간대"] == time)]
            every_week = df_filtered[df_filtered["주차"] == "매주"]["이름"].unique()
            specific_weeks = df_filtered[df_filtered["주차"] != "매주"]
            specific_week_dict = {name: sorted(specific_weeks[specific_weeks["이름"] == name]["주차"].tolist(), 
                                               key=lambda x: int(x.replace("주", ""))) 
                                  for name in specific_weeks["이름"].unique() if specific_weeks[specific_weeks["이름"] == name]["주차"].tolist()}
            employees = list(every_week) + [f"{name}({','.join(weeks)})" for name, weeks in specific_week_dict.items()]
            result[key] = ", ".join(employees) if employees else ""
    
    return pd.DataFrame(list(result.items()), columns=["시간대", "근무"])

# 보충 테이블 생성 함수
def generate_supplement_table(df_result, names_in_master):
    supplement = []
    weekday_order = ["월", "화", "수", "목", "금"]
    shift_list = ["오전", "오후"]
    names_in_master = set(names_in_master)

    for day in weekday_order:
        for shift in shift_list:
            time_slot = f"{day} {shift}"
            row = df_result[df_result["시간대"] == time_slot].iloc[0]
            employees = set(emp.split("(")[0].strip() for emp in row["근무"].split(", ") if emp)
            supplement_employees = names_in_master - employees

            if shift == "오후":
                morning_slot = f"{day} 오전"
                morning_employees = set(df_result[df_result["시간대"] == morning_slot].iloc[0]["근무"].split(", ") 
                                        if morning_slot in df_result["시간대"].values else [])
                supplement_employees = {emp if emp in morning_employees else f"{emp}🔺" for emp in supplement_employees}

            supplement.append({"시간대": time_slot, "보충": ", ".join(sorted(supplement_employees)) if supplement_employees else ""})

    return pd.DataFrame(supplement)

def split_column_to_multiple(df, column_name, prefix):
    """
    데이터프레임의 특정 열을 쉼표로 분리하여 여러 열로 변환하는 함수
    
    Parameters:
    - df: 입력 데이터프레임
    - column_name: 분리할 열 이름 (예: "근무", "보충")
    - prefix: 새로운 열 이름의 접두사 (예: "근무", "보충")
    
    Returns:
    - 새로운 데이터프레임
    """
    # 줄바꿈(\n)을 쉼표로 변환
    if column_name not in df.columns:
        st.warning(f"⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.stop()
        return df
    
    # 쉼표로 분리하여 리스트로 변환
    split_data = df[column_name].str.split(", ", expand=True)
    
    # 최대 열 수 계산 (가장 많은 인원을 가진 행 기준)
    max_cols = split_data.shape[1]
    
    # 새로운 열 이름 생성 (예: 근무1, 근무2, ...)
    new_columns = [f"{prefix}{i+1}" for i in range(max_cols)]
    split_data.columns = new_columns
    
    # 원래 데이터프레임에서 해당 열 삭제
    df = df.drop(columns=[column_name])
    
    # 분리된 데이터를 원래 데이터프레임에 추가
    df = pd.concat([df, split_data], axis=1)

    return df

st.header("🗓️ 스케줄 배정", divider='rainbow')
st.write("- 먼저 새로고침 버튼으로 최신 데이터를 불러온 뒤, 배정을 진행해주세요.")

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    try:
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state["data_loaded"] = False
        load_data_page5()
        st.success("데이터가 새로고침되었습니다.")
        time.sleep(1)
        st.rerun()
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (새로고침): {e.response.status_code} - {e.response.text}")
        st.stop()
    except NameError as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {type(e).__name__} - {e}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"새로고침 중 오류 발생: {type(e).__name__} - {e}")
        st.stop()

# 메인 로직
load_data_page5()
# Use .get() with fallback to avoid KeyError
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
# df_cumulative 컬럼 이름은 load_data_page5에서 '이름'으로 변경되었음
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])) # fallback 컬럼도 '이름'으로 통일
df_shift = st.session_state.get("df_shift", pd.DataFrame())  # 세션 상태에서 가져오기
df_supplement = st.session_state.get("df_supplement", pd.DataFrame())  # 세션 상태에서 가져오기

st.divider()
st.subheader(f"✨ {month_str} 테이블 종합")

# 데이터 전처리: 근무 테이블과 보충 테이블의 열 분리
df_shift_processed = split_column_to_multiple(df_shift, "근무", "근무")
df_supplement_processed = split_column_to_multiple(df_supplement, "보충", "보충")

# Excel 다운로드 함수 (다중 시트)
def excel_download(name, sheet1, name1, sheet2, name2, sheet3, name3, sheet4, name4):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        sheet1.to_excel(writer, sheet_name=name1, index=False)
        sheet2.to_excel(writer, sheet_name=name2, index=False)
        sheet3.to_excel(writer, sheet_name=name3, index=False)
        sheet4.to_excel(writer, sheet_name=name4, index=False)
    
    excel_data = output.getvalue()
    return excel_data

# 근무 테이블
st.write(" ")
st.markdown("**✅ 근무 테이블**")
st.dataframe(df_shift, use_container_width=True, hide_index=True)

# 보충 테이블 (중복된 df_master 표시 제거, df_supplement 표시)
st.markdown("**☑️ 보충 테이블**")
st.dataframe(df_supplement, use_container_width=True, hide_index=True)

# 요청사항 테이블
st.markdown("**🙋‍♂️ 요청사항 테이블**")
st.dataframe(df_request, use_container_width=True, hide_index=True)

# 누적 테이블
st.markdown("**➕ 누적 테이블**")
st.dataframe(df_cumulative, use_container_width=True, hide_index=True)

# 다운로드 버튼 추가
excel_data = excel_download(
    name=f"{month_str} 테이블 종합",
    sheet1=df_shift_processed, name1="근무 테이블",
    sheet2=df_supplement_processed, name2="보충 테이블",
    sheet3=df_request, name3="요청사항 테이블",
    sheet4=df_cumulative, name4="누적 테이블"
)
st.download_button(
    label="📥 상단 테이블 다운로드",
    data=excel_data,
    file_name=f"{month_str} 테이블 종합.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# 근무 배정 로직
# 누적 근무 횟수 추적용 딕셔너리 초기화
current_cumulative = {'오전': {}, '오후': {}}

# 익월(다음 달) 평일 생성
_, last_day = calendar.monthrange(today.year, today.month)
next_month = today.replace(day=1) + relativedelta(months=1)
dates = pd.date_range(start=next_month, end=next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1]))
weekdays = [d for d in dates if d.weekday() < 5]
week_numbers = {d.to_pydatetime().date(): (d.day - 1) // 7 + 1 for d in dates}
day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금'}
# df_final 초기화
df_final = pd.DataFrame(columns=['날짜', '요일', '주차', '시간대', '근무자', '상태', '메모', '색상'])

# 데이터프레임 로드 확인 (Streamlit UI로 변경)
st.divider()
st.subheader(f"✨ {month_str} 스케줄 배정 수행")
# st.write("df_request 확인:", df_request.head())
# st.write("df_cumulative 확인:", df_cumulative.head())

# 날짜 범위 파싱 함수
def parse_date_range(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str) or date_str.strip() == '':
        return []
    date_str = date_str.strip()
    result = []
    if ',' in date_str:
        for single_date in date_str.split(','):
            single_date = single_date.strip()
            try:
                parsed_date = datetime.datetime.strptime(single_date, '%Y-%m-%d')
                if parsed_date.weekday() < 5:
                    result.append(single_date)
            except ValueError:
                pass # 이 메시지는 너무 많이 나올 수 있어 주석 처리
        return result
    if '~' in date_str:
        try:
            start_date, end_date = date_str.split('~')
            start_date = start_date.strip()
            end_date = end_date.strip()
            start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            date_list = pd.date_range(start=start, end=end)
            return [d.strftime('%Y-%m-%d') for d in date_list if d.weekday() < 5]
        except ValueError as e:
            pass # 이 메시지는 너무 많이 나올 수 있어 주석 처리
            return []
    try:
        parsed_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        if parsed_date.weekday() < 5:
            return [date_str]
        return []
    except ValueError:
        pass # 이 메시지는 너무 많이 나올 수 있어 주석 처리
        return []

# 근무자 상태 업데이트 함수
def update_worker_status(df, date_str, time_slot, worker, status, memo, color, day_map, week_numbers):
    """df_final 데이터프레임을 안전하게 업데이트하는 함수"""
    date_obj = pd.to_datetime(date_str)
    worker_stripped = worker.strip()
    
    existing_indices = df.index[
        (df['날짜'] == date_str) &
        (df['시간대'] == time_slot) &
        (df['근무자'] == worker_stripped)
    ].tolist()

    if existing_indices:
        df.loc[existing_indices, ['상태', '메모', '색상']] = [status, memo, color]
    else:
        new_row = pd.DataFrame([{
            '날짜': date_str,
            '요일': day_map.get(date_obj.weekday(), ''),
            '주차': week_numbers.get(date_obj.date(), 0),
            '시간대': time_slot,
            '근무자': worker_stripped,
            '상태': status,
            '메모': memo,
            '색상': color
        }])
        df = pd.concat([df, new_row], ignore_index=True)
    return df

# df_final에서 특정 worker가 특정 날짜, 시간대에 '제외' 상태이며 특정 메모를 가지고 있는지 확인하는 헬퍼 함수
def is_worker_already_excluded_with_memo(df_data, date_s, time_s, worker_s):
    # 해당 날짜, 시간대, 근무자의 모든 기록을 가져옴
    worker_records = df_data[
        (df_data['날짜'] == date_s) &
        (df_data['시간대'] == time_s) &
        (df_data['근무자'] == worker_s)
    ]
    if worker_records.empty:
        return False # 해당 근무자 기록 자체가 없으면 당연히 제외되지 않음

    # '제외' 또는 '추가제외' 상태인 기록만 필터링
    excluded_records = worker_records[worker_records['상태'].isin(['제외', '추가제외'])]
    if excluded_records.empty:
        return False # 제외된 기록이 없으면 False

    # 제외된 기록 중 해당 메모를 포함하는지 확인 (str.contains가 Series를 반환하므로 .any() 사용)
    return excluded_records['메모'].str.contains('보충 위해 제외됨|인원 초과로 인한 제외|오전 추가제외로 인한 오후 제외', na=False).any()


# df_final_unique와 df_excel을 기반으로 스케줄 데이터 변환

def transform_schedule_data(df, df_excel, month_start, month_end):
    # [수정] '근무', '보충', '추가보충' 상태를 모두 포함하도록 필터링
    df = df[df['상태'].isin(['근무', '보충', '추가보충'])][['날짜', '시간대', '근무자', '요일']].copy()
    
    # 전체 날짜 범위 생성
    date_range = pd.date_range(start=month_start, end=month_end)
    date_list = [f"{d.month}월 {d.day}일" for d in date_range]
    weekday_list = [d.strftime('%a') for d in date_range]
    weekday_map = {'Mon': '월', 'Tue': '화', 'Wed': '수', 'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'}
    weekdays = [weekday_map[w] for w in weekday_list]
    
    # 결과 DataFrame 초기화
    columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 6)]
    result_df = pd.DataFrame(columns=columns)
    
    # 각 날짜별로 처리
    for date, weekday in zip(date_list, weekdays):
        date_key = datetime.datetime.strptime(date, '%m월 %d일').replace(year=2025).strftime('%Y-%m-%d')
        date_df = df[df['날짜'] == date_key]
        
        # 평일 데이터 (df_final_unique에서 가져옴)
        morning_workers = date_df[date_df['시간대'] == '오전']['근무자'].tolist()[:12]
        morning_data = morning_workers + [''] * (12 - len(morning_workers))
        afternoon_workers = date_df[date_df['시간대'] == '오후']['근무자'].tolist()[:5]
        afternoon_data = afternoon_workers + [''] * (5 - len(afternoon_workers))
        
        # 토요일 데이터 (df_excel에서 가져옴)
        if weekday == '토':
            excel_row = df_excel[df_excel['날짜'] == date]
            if not excel_row.empty:
                morning_data = [excel_row[str(i)].iloc[0] if str(i) in excel_row.columns and pd.notna(excel_row[str(i)].iloc[0]) else '' for i in range(1, 13)]
        
        # df_excel에서 해당 날짜의 온콜 데이터 가져오기
        oncall_worker = ''
        excel_row = df_excel[df_excel['날짜'] == date]
        if not excel_row.empty:
            oncall_worker = excel_row['오전당직(온콜)'].iloc[0] if '오전당직(온콜)' in excel_row.columns else ''
        
        row_data = [date, weekday] + morning_data + [oncall_worker] + afternoon_data
        result_df = pd.concat([result_df, pd.DataFrame([row_data], columns=columns)], ignore_index=True)
    
    return result_df

df_cumulative_next = df_cumulative.copy()

# 세션 상태 초기화 (기존 코드 유지)
if "assigned" not in st.session_state:
    st.session_state.assigned = False
if "downloaded" not in st.session_state:
    st.session_state.downloaded = False
if "output" not in st.session_state:
    st.session_state.output = None

# 휴관일 선택 UI 추가
st.write(" ")
st.markdown("**📅 센터 휴관일 추가**")

# month_str에 해당하는 평일 날짜 생성 (이미 정의된 weekdays 사용)
holiday_options = []
for date in weekdays:
    date_str = date.strftime('%Y-%m-%d')
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    day_name = day_map[date_obj.weekday()]
    holiday_format = f"{date_obj.month}월 {date_obj.day}일({day_name})"
    holiday_options.append((holiday_format, date_str))

# st.multiselect로 휴관일 선택
selected_holidays = st.multiselect(
    label=f"{month_str} 평일 중 휴관일을 선택",
    options=[option[0] for option in holiday_options],
    default=[],
    key="holiday_select",
    help="선택한 날짜는 근무 배정에서 제외됩니다."
)

# 선택된 휴관일을 날짜 형식(YYYY-MM-DD)으로 변환
holiday_dates = []
for holiday in selected_holidays:
    for option in holiday_options:
        if option[0] == holiday:
            holiday_dates.append(option[1])
            break

# df_master와 df_request에서 이름 추출 및 중복 제거
names_in_master = set(df_master["이름"].unique().tolist())
names_in_request = set(df_request["이름"].unique().tolist())
all_names = sorted(list(names_in_master.union(names_in_request)))  # 중복 제거 후 정렬

# --- UI 개선: 토요/휴일 스케줄 입력 ---
# 세션 상태 초기화
if "special_schedule_count" not in st.session_state:
    st.session_state.special_schedule_count = 1
if "special_schedules" not in st.session_state:
    st.session_state.special_schedules = []

# UI: 토요/휴일 스케줄 입력
st.write(" ")
st.markdown("**📅 토요/휴일 스케줄 입력**")

# 전체 인원 목록 준비
all_names = sorted(list(st.session_state["df_master"]["이름"].unique()))

# month_str과 month_dt 정의
today = datetime.date.today()
month_dt = today.replace(day=1) + relativedelta(months=1)
month_format = "%#m" if platform.system() == "Windows" else "%-m"
month_str = month_dt.strftime(f"%Y년 {month_format}월")
_, last_day = calendar.monthrange(month_dt.year, month_dt.month)  # month_dt의 연도와 월로 last_day 계산

# 토요/휴일 스케줄 입력 UI
st.write(" ")
st.markdown("**📅 토요/휴일 스케줄 입력**")
special_schedules = []
for i in range(st.session_state.special_schedule_count):
    cols = st.columns([2, 3])
    with cols[0]:
        selected_date = st.date_input(
            label=f"날짜 선택",
            value=None,
            min_value=month_dt,  # month_dt.date() → month_dt
            max_value=month_dt.replace(day=last_day),  # .date() 제거            key=f"special_date_{i}",
            help="주말, 공휴일 등 정규 스케줄 외 근무가 필요한 날짜를 선택하세요."
        )
    with cols[1]:
        if selected_date:
            selected_workers = st.multiselect(
                label=f"근무 인원 선택",
                options=all_names,
                key=f"special_workers_{i}"
            )
            if selected_workers:
                special_schedules.append((selected_date.strftime('%Y-%m-%d'), selected_workers))
# 입력 필드 추가 버튼
if st.button("➕ 토요/휴일 스케줄 추가"):
    st.session_state.special_schedule_count += 1
    st.rerun()

if special_schedules:
    st.session_state.special_schedules = special_schedules  # 세션 상태 업데이트

# Google Sheets 저장 함수 수정
# Google Sheets 저장 함수 수정
def save_special_schedules_to_sheets(special_schedules, month_str, client):
    try:
        spreadsheet = client.open_by_url(st.secrets["google_sheet"]["url"])
        sheet_name = f"{month_str} 토요/휴일 일자"
        
        # 기존 시트가 있으면 가져오고, 없으면 새로 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=2)
        
        # 시트 초기화 및 헤더 설정
        worksheet.clear()
        headers = ["날짜", "근무 인원"]
        worksheet.update('A1', [headers], value_input_option='RAW')
        
        # 스케줄 데이터가 있을 경우에만 저장
        if special_schedules:
            schedule_df = pd.DataFrame(
                [(s[0], ", ".join(s[1])) for s in special_schedules],
                columns=headers
            )
            data_to_save = schedule_df.values.tolist()
            worksheet.update('A2', data_to_save, value_input_option='RAW')
            return True
        else:
            # 스케줄이 없으면 빈 데이터로 초기화
            worksheet.update('A2', [[]], value_input_option='RAW')
            st.warning("별도 토요/휴일 스케줄이 없습니다. 빈 시트로 저장됩니다.")
            return True
            
    except gspread.exceptions.APIError as e:
        st.error(f"Google Sheets API 오류 (토요/휴일 스케줄 저장): {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        st.error(f"토요/휴일 스케줄 저장 실패: {type(e).__name__} - {e}")
        return False

def exec_balancing_pass(df_final, active_weekdays, time_slot, target_count, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers):
    """V1의 안정적인 루프 방식으로 1:1 인원 이동을 수행하는 함수"""
    moved_workers = set()
    iteration = 0
    while iteration < 100:
        iteration += 1
        
        # 목록을 매번 새로 계산하여 현재 상태를 정확히 반영
        excess_dates = []
        shortage_dates = []
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            workers_on_date = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
            count = len(workers_on_date)
            if count > target_count: excess_dates.append([date_str, count - target_count])
            elif count < target_count: shortage_dates.append([date_str, target_count - count])
        
        if not excess_dates or not shortage_dates: break

        any_match_in_pass = False
        processed_excess_dates = set()

        for excess_idx in range(len(excess_dates)):
            if excess_idx >= len(excess_dates): break
            excess_date, _ = excess_dates[excess_idx]
            if excess_date in processed_excess_dates: continue

            excess_workers = df_final[(df_final['날짜'] == excess_date) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
            must_work_on_excess = {r['이름'] for _, r in df_request.iterrows() if excess_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}
            movable_workers = [w for w in excess_workers if w not in must_work_on_excess and w not in moved_workers]

            random.shuffle(movable_workers) # 이동 가능한 근무자 목록을 무작위로 섞습니다.

            for worker_to_move in movable_workers:
                for shortage_idx in range(len(shortage_dates)):
                    if shortage_idx >= len(shortage_dates): break
                    shortage_date, __ = shortage_dates[shortage_idx]
                    
                    is_movable = True
                    # 모든 제약조건 검사
                    shortage_day_name = day_map[pd.to_datetime(shortage_date).weekday()]
                    shortage_shift_key = f"{shortage_day_name} {time_slot}"
                    supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shortage_shift_key]
                    if not supplement_row.empty:
                        supplement_pool = set(val.replace('🔺','').strip() for val in supplement_row.iloc[0, 1:].dropna())
                        if worker_to_move not in supplement_pool: is_movable = False
                    
                    if is_movable and worker_to_move in initial_master_assignments.get((shortage_date, time_slot), set()): is_movable = False
                    
                    if is_movable:
                        no_supplement_req = {r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
                        if worker_to_move in no_supplement_req: is_movable = False

                    if is_movable and time_slot == '오후':
                        morning_workers = set(df_final[(df_final['날짜'] == shortage_date) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                        must_work_pm = {r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == '꼭 근무(오후)'}
                        if worker_to_move not in morning_workers and worker_to_move not in must_work_pm: is_movable = False

                    if is_movable:
                        df_final = update_worker_status(df_final, excess_date, time_slot, worker_to_move, '제외', f'{pd.to_datetime(shortage_date).strftime("%-m월 %-d일")} 보충', '🔵 파란색', day_map, week_numbers)
                        df_final = update_worker_status(df_final, shortage_date, time_slot, worker_to_move, '보충', f'{pd.to_datetime(excess_date).strftime("%-m월 %-d일")}에서 이동', '🟢 초록색', day_map, week_numbers)
                        moved_workers.add(worker_to_move)
                        any_match_in_pass = True
                        
                        excess_dates[excess_idx][1] -= 1
                        shortage_dates[shortage_idx][1] -= 1
                        if excess_dates[excess_idx][1] == 0: excess_dates.pop(excess_idx)
                        if shortage_dates[shortage_idx][1] == 0: shortage_dates.pop(shortage_idx)
                        break
                if any_match_in_pass: break
            if any_match_in_pass: break
        
        if not any_match_in_pass:
             break
            
    return df_final

def exec_final_adjustment(df_final, active_weekdays, time_slot, target_count, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative):
    """최종 추가 보충/제외를 수행하는 함수"""
    for date in active_weekdays:
        date_str = date.strftime('%Y-%m-%d')
        current_workers = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
        
        if len(current_workers) < target_count:
            needed = target_count - len(current_workers)
            day_name = day_map[pd.to_datetime(date_str).weekday()]
            shift_key = f"{day_name} {time_slot}"
            supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shift_key]
            supplement_candidates = []
            if not supplement_row.empty:
                supplement_candidates = [val.replace('🔺', '').strip() for val in supplement_row.iloc[0, 1:].dropna()]

            no_supplement_on_date = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
            supplement_candidates = [w for w in supplement_candidates if w not in current_workers and w not in no_supplement_on_date]
            
            if time_slot == '오후':
                morning_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                supplement_candidates = [w for w in supplement_candidates if w in morning_workers]
            
            supplement_candidates.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0))

            for worker_to_add in supplement_candidates[:needed]:
                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_add, '추가보충', '', '🟡 노란색', day_map, week_numbers)
                current_cumulative.setdefault(time_slot, {})[worker_to_add] = current_cumulative.get(time_slot, {}).get(worker_to_add, 0) + 1
        
        elif len(current_workers) > target_count:
            over_count = len(current_workers) - target_count
            must_work_on_date = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}
            
            removable_workers = [w for w in current_workers if w not in must_work_on_date]
            removable_workers.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0), reverse=True)

            for worker_to_remove in removable_workers[:over_count]:
                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_remove, '추가제외', '', '🟣 보라색', day_map, week_numbers)
                current_cumulative.setdefault(time_slot, {})[worker_to_remove] = current_cumulative.get(time_slot, {}).get(worker_to_remove, 0) - 1
                if time_slot == '오전':
                    afternoon_worker_record = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오후') & (df_final['근무자'] == worker_to_remove) & (df_final['상태'].isin(['근무', '보충']))]
                    if not afternoon_worker_record.empty:
                        df_final = update_worker_status(df_final, date_str, '오후', worker_to_remove, '추가제외', '오전 추가제외로 동시 제외', '🟣 보라색', day_map, week_numbers)
                        current_cumulative.setdefault('오후', {})[worker_to_remove] = current_cumulative.get('오후', {}).get(worker_to_remove, 0) - 1
    return df_final, current_cumulative

def update_worker_status(df, date_str, time_slot, worker, status, memo, color, day_map, week_numbers):
    """df_final 데이터프레임을 안전하게 업데이트하는 함수"""
    date_obj = pd.to_datetime(date_str)
    worker_stripped = worker.strip()
    
    existing_indices = df.index[
        (df['날짜'] == date_str) &
        (df['시간대'] == time_slot) &
        (df['근무자'] == worker_stripped)
    ].tolist()

    if existing_indices:
        df.loc[existing_indices, ['상태', '메모', '색상']] = [status, memo, color]
    else:
        new_row = pd.DataFrame([{
            '날짜': date_str,
            '요일': day_map.get(date_obj.weekday(), ''),
            '주차': week_numbers.get(date_obj.date(), 0),
            '시간대': time_slot,
            '근무자': worker_stripped,
            '상태': status,
            '메모': memo,
            '색상': color
        }])
        df = pd.concat([df, new_row], ignore_index=True)
    return df

def exec_balancing_pass(df_final, active_weekdays, time_slot, target_count, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers):
    """V1의 안정적인 루프 방식으로 1:1 인원 이동을 수행하는 함수"""
    moved_workers = set()
    iteration = 0
    while iteration < 100:
        iteration += 1
        
        # 목록을 매번 새로 계산하여 현재 상태를 정확히 반영
        excess_dates = []
        shortage_dates = []
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            workers_on_date = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
            count = len(workers_on_date)
            if count > target_count: excess_dates.append([date_str, count - target_count])
            elif count < target_count: shortage_dates.append([date_str, target_count - count])
        
        if not excess_dates or not shortage_dates: break

        any_match_in_pass = False
        processed_excess_dates = set()

        for excess_idx in range(len(excess_dates)):
            if excess_idx >= len(excess_dates): break
            excess_date, _ = excess_dates[excess_idx]
            if excess_date in processed_excess_dates: continue

            excess_workers = df_final[(df_final['날짜'] == excess_date) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
            must_work_on_excess = {r['이름'] for _, r in df_request.iterrows() if excess_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}
            movable_workers = [w for w in excess_workers if w not in must_work_on_excess and w not in moved_workers]

            for worker_to_move in movable_workers:
                for shortage_idx in range(len(shortage_dates)):
                    if shortage_idx >= len(shortage_dates): break
                    shortage_date, __ = shortage_dates[shortage_idx]
                    
                    is_movable = True
                    # 모든 제약조건 검사
                    shortage_day_name = day_map[pd.to_datetime(shortage_date).weekday()]
                    shortage_shift_key = f"{shortage_day_name} {time_slot}"
                    supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shortage_shift_key]
                    if not supplement_row.empty:
                        supplement_pool = set(val.replace('🔺','').strip() for val in supplement_row.iloc[0, 1:].dropna())
                        if worker_to_move not in supplement_pool: is_movable = False
                    
                    if is_movable and worker_to_move in initial_master_assignments.get((shortage_date, time_slot), set()): is_movable = False
                    
                    if is_movable:
                        no_supplement_req = {r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
                        if worker_to_move in no_supplement_req: is_movable = False

                    if is_movable and time_slot == '오후':
                        morning_workers = set(df_final[(df_final['날짜'] == shortage_date) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                        must_work_pm = {r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == '꼭 근무(오후)'}
                        if worker_to_move not in morning_workers and worker_to_move not in must_work_pm: is_movable = False

                    if is_movable:
                        df_final = update_worker_status(df_final, excess_date, time_slot, worker_to_move, '제외', f'{pd.to_datetime(shortage_date).strftime("%-m월 %-d일")} 보충', '🔵 파란색', day_map, week_numbers)
                        df_final = update_worker_status(df_final, shortage_date, time_slot, worker_to_move, '보충', f'{pd.to_datetime(excess_date).strftime("%-m월 %-d일")}에서 이동', '🟢 초록색', day_map, week_numbers)
                        moved_workers.add(worker_to_move)
                        any_match_in_pass = True
                        
                        excess_dates[excess_idx][1] -= 1
                        shortage_dates[shortage_idx][1] -= 1
                        if excess_dates[excess_idx][1] == 0: excess_dates.pop(excess_idx)
                        if shortage_dates[shortage_idx][1] == 0: shortage_dates.pop(shortage_idx)
                        break
                if any_match_in_pass: break
            if any_match_in_pass: break
        
        if not any_match_in_pass:
             break
            
    return df_final

def exec_final_adjustment(df_final, active_weekdays, time_slot, target_count, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative):
    """최종 추가 보충/제외를 수행하는 함수"""
    for date in active_weekdays:
        date_str = date.strftime('%Y-%m-%d')
        current_workers = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].unique()
        
        if len(current_workers) < target_count:
            needed = target_count - len(current_workers)
            day_name = day_map[pd.to_datetime(date_str).weekday()]
            shift_key = f"{day_name} {time_slot}"
            supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shift_key]
            supplement_candidates = []
            if not supplement_row.empty:
                supplement_candidates = [val.replace('🔺', '').strip() for val in supplement_row.iloc[0, 1:].dropna()]

            no_supplement_on_date = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'보충 불가({time_slot})'}
            supplement_candidates = [w for w in supplement_candidates if w not in current_workers and w not in no_supplement_on_date]
            
            if time_slot == '오후':
                morning_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
                supplement_candidates = [w for w in supplement_candidates if w in morning_workers]
            
            supplement_candidates.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0))

            for worker_to_add in supplement_candidates[:needed]:
                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_add, '추가보충', '', '🟡 노란색', day_map, week_numbers)
                current_cumulative.setdefault(time_slot, {})[worker_to_add] = current_cumulative.get(time_slot, {}).get(worker_to_add, 0) + 1
        
        elif len(current_workers) > target_count:
            over_count = len(current_workers) - target_count
            must_work_on_date = {r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r.get('날짜정보'))) and r.get('분류') == f'꼭 근무({time_slot})'}
            
            removable_workers = [w for w in current_workers if w not in must_work_on_date]
            removable_workers.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0), reverse=True)

            for worker_to_remove in removable_workers[:over_count]:
                df_final = update_worker_status(df_final, date_str, time_slot, worker_to_remove, '추가제외', '', '🟣 보라색', day_map, week_numbers)
                current_cumulative.setdefault(time_slot, {})[worker_to_remove] = current_cumulative.get(time_slot, {}).get(worker_to_remove, 0) - 1
                if time_slot == '오전':
                    afternoon_worker_record = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오후') & (df_final['근무자'] == worker_to_remove) & (df_final['상태'].isin(['근무', '보충']))]
                    if not afternoon_worker_record.empty:
                        df_final = update_worker_status(df_final, date_str, '오후', worker_to_remove, '추가제외', '오전 추가제외로 동시 제외', '🟣 보라색', day_map, week_numbers)
                        current_cumulative.setdefault('오후', {})[worker_to_remove] = current_cumulative.get('오후', {}).get(worker_to_remove, 0) - 1
    return df_final, current_cumulative

# ========================= 메인 실행 로직 전체 교체 =========================

if st.button("🚀 근무 배정 실행", type="primary", use_container_width=True):
    st.session_state.assigned = False
    st.session_state.output = None
    st.session_state.downloaded = False
    special_schedules = st.session_state.get("special_schedules", [])
    
    with st.spinner("근무 배정 중..."):
        time.sleep(1)
        client = get_gspread_client()
        save_special_schedules_to_sheets(special_schedules, month_str, client)

        # --- 0단계: 모든 초기화 ---
        df_final = pd.DataFrame(columns=['날짜', '요일', '주차', '시간대', '근무자', '상태', '메모', '색상'])
        month_dt = datetime.datetime.strptime(month_str, "%Y년 %m월")
        _, last_day = calendar.monthrange(month_dt.year, month_dt.month)  # month_dt에 맞게 last_day 계산
        all_month_dates = pd.date_range(start=month_dt, end=month_dt.replace(day=last_day))
        weekdays = [d for d in all_month_dates if d.weekday() < 5]
        active_weekdays = [d for d in weekdays if d.strftime('%Y-%m-%d') not in holiday_dates]
        day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
        week_numbers = {d.to_pydatetime().date(): (d.day - 1) // 7 + 1 for d in all_month_dates}

        initial_master_assignments = {}
        for date in active_weekdays:
            date_str, day_name, week_num = date.strftime('%Y-%m-%d'), day_map[date.weekday()], week_numbers[date.date()]
            for ts in ['오전', '오후']:
                shift_key, base_workers = f"{day_name} {ts}", set()
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                if not shift_row.empty:
                    for col in shift_row.columns[1:]:
                        worker_info = shift_row[col].values[0]
                        if pd.notna(worker_info):
                            worker_name = str(worker_info).split('(')[0].strip()
                            if '(' in str(worker_info) and f'{week_num}주' in str(worker_info):
                                base_workers.add(worker_name)
                            elif '(' not in str(worker_info):
                                base_workers.add(worker_name)
                initial_master_assignments[(date_str, ts)] = base_workers
        
        current_cumulative = {'오전': {}, '오후': {}}

        # === ☀️ 1단계: 오전 스케줄링 전체 완료 ===
        time_slot_am = '오전'
        target_count_am = 12
        
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            requests_on_date = df_request[df_request['날짜정보'].apply(lambda x: date_str in parse_date_range(str(x)))]
            vacationers = set(requests_on_date[requests_on_date['분류'] == '휴가']['이름'].tolist())
            base_workers = initial_master_assignments.get((date_str, time_slot_am), set())
            must_work = set(requests_on_date[requests_on_date['분류'] == f'꼭 근무({time_slot_am})']['이름'].tolist())
            final_workers = (base_workers - vacationers) | (must_work - vacationers)
            for worker in final_workers:
                df_final = update_worker_status(df_final, date_str, time_slot_am, worker, '근무', '' if worker in must_work else '', '🟠 주황색' if worker in must_work else '기본', day_map, week_numbers)
            for vac in (vacationers & base_workers):
                df_final = update_worker_status(df_final, date_str, time_slot_am, vac, '제외', '', '🔴 빨간색', day_map, week_numbers)
        
        df_final = exec_balancing_pass(df_final, active_weekdays, time_slot_am, target_count_am, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers)
        df_final, current_cumulative = exec_final_adjustment(df_final, active_weekdays, time_slot_am, target_count_am, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative)

        # === 🌙 2단계: 오후 스케줄링 전체 완료 ===
        time_slot_pm = '오후'
        target_count_pm = 5
        
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            morning_workers = set(df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'])
            requests_on_date = df_request[df_request['날짜정보'].apply(lambda x: date_str in parse_date_range(str(x)))]
            vacationers = set(requests_on_date[requests_on_date['분류'] == '휴가']['이름'].tolist())
            base_workers = initial_master_assignments.get((date_str, time_slot_pm), set())
            must_work = set(requests_on_date[requests_on_date['분류'] == f'꼭 근무({time_slot_pm})']['이름'].tolist())
            
            eligible_workers = morning_workers | must_work
            final_workers = (base_workers & eligible_workers) - vacationers
            final_workers.update((must_work & eligible_workers) - vacationers)

            for worker in final_workers:
                df_final = update_worker_status(df_final, date_str, time_slot_pm, worker, '근무', '' if worker in must_work else '', '🟠 주황색' if worker in must_work else '기본', day_map, week_numbers)
            for vac in (vacationers & base_workers):
                 if not df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot_pm) & (df_final['근무자'] == vac) & (df_final['상태'] == '근무')].empty: continue
                 df_final = update_worker_status(df_final, date_str, time_slot_pm, vac, '제외', '', '🔴 빨간색', day_map, week_numbers)
        
        df_final = exec_balancing_pass(df_final, active_weekdays, time_slot_pm, target_count_pm, initial_master_assignments, df_supplement_processed, df_request, day_map, week_numbers)
        df_final, current_cumulative = exec_final_adjustment(df_final, active_weekdays, time_slot_pm, target_count_pm, df_supplement_processed, df_request, day_map, week_numbers, current_cumulative)

        # === 📤 3단계: 최종 결과 생성 및 저장 ===
        df_cumulative_next = df_cumulative.copy().set_index('이름')
        for worker, count in current_cumulative.get('오전', {}).items():
            if worker in df_cumulative_next.index: df_cumulative_next.loc[worker, '오전누적'] += count
            else: df_cumulative_next.loc[worker] = [count, 0, 0, 0]
        for worker, count in current_cumulative.get('오후', {}).items():
            if worker in df_cumulative_next.index: df_cumulative_next.loc[worker, '오후누적'] += count
            else: df_cumulative_next.loc[worker] = [0, count, 0, 0]
        df_cumulative_next.reset_index(inplace=True)

        if special_schedules:
            for date_str, workers in special_schedules:
                if not df_final.empty: df_final = df_final[df_final['날짜'] != date_str].copy()
                for worker in workers:
                    df_final = update_worker_status(df_final, date_str, '오전', worker, '근무', '', '특수근무색', day_map, week_numbers)
        
        color_priority = {'🟠 주황색': 0, '🟢 초록색': 1, '🟡 노란색': 2, '기본': 3, '🔴 빨간색': 4, '🔵 파란색': 5, '🟣 보라색': 6, '특수근무색': -1}
        df_final['색상_우선순위'] = df_final['색상'].map(color_priority)
        df_final_unique = df_final.sort_values(by=['날짜', '시간대', '근무자', '색상_우선순위']).drop_duplicates(subset=['날짜', '시간대', '근무자'], keep='first')
        
        # [오류 수정] 엑셀 생성에 필요한 변수들 정의
        full_day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
        df_schedule = pd.DataFrame({'날짜': [d.strftime('%Y-%m-%d') for d in all_month_dates], '요일': [full_day_map.get(d.weekday()) for d in all_month_dates]})
        worker_counts_all = df_final_unique.groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
        max_morning_workers = int(worker_counts_all.get('오전', pd.Series(0)).max())
        max_afternoon_workers = int(worker_counts_all.get('오후', pd.Series(0)).max())

        # Excel 출력용 DataFrame 생성
        columns = ['날짜', '요일'] + [str(i) for i in range(1, max_morning_workers + 1)] + [''] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, max_afternoon_workers + 1)]
        df_excel = pd.DataFrame(index=df_schedule.index, columns=columns)

        for idx, row in df_schedule.iterrows():
            date = row['날짜']
            date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
            df_excel.at[idx, '날짜'] = f"{date_obj.month}월 {date_obj.day}일"
            df_excel.at[idx, '요일'] = row['요일']
            
            # 평일, 주말 모두 df_final_unique에서 데이터 가져오기 (정렬 포함)
            morning_workers_for_excel = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오전')]
            morning_workers_for_excel_sorted = morning_workers_for_excel.sort_values(by=['색상_우선순위', '근무자'])['근무자'].tolist()
            for i, worker_name in enumerate(morning_workers_for_excel_sorted, 1):
                if i <= max_morning_workers: df_excel.at[idx, str(i)] = worker_name
            
            afternoon_workers_for_excel = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후')]
            afternoon_workers_for_excel_sorted = afternoon_workers_for_excel.sort_values(by=['색상_우선순위', '근무자'])['근무자'].tolist()
            for i, worker_name in enumerate(afternoon_workers_for_excel_sorted, 1):
                if i <= max_afternoon_workers: df_excel.at[idx, f'오후{i}'] = worker_name
            
            # 토요일 UI 입력 덮어쓰기
            if row['요일'] == '토':
                for special_date, workers in special_schedules:
                    if date == special_date:
                        workers_padded = workers[:10] + [''] * (10 - len(workers[:10]))
                        for i in range(1, 11): df_excel.at[idx, str(i)] = workers_padded[i-1]
        
            oncall_counts = df_cumulative.set_index('이름')['오전당직 (온콜)'].to_dict()
            oncall_assignments = {worker: int(count) if count else 0 for worker, count in oncall_counts.items()}
            oncall = {}
            afternoon_counts = df_final_unique[(df_final_unique['시간대'] == '오후') & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]['근무자'].value_counts().to_dict()
            workers_priority = sorted(oncall_assignments.items(), key=lambda x: (-x[1], afternoon_counts.get(x[0], 0)))
            all_dates = df_final_unique['날짜'].unique().tolist()
            remaining_dates = set(all_dates)
            
        # 토요/휴일 스케줄 날짜 목록을 미리 준비합니다.
        special_schedule_dates_set = {s[0] for s in special_schedules}

        for worker, count in workers_priority:
            if count <= 0: continue
            eligible_dates = df_final_unique[(df_final_unique['시간대'] == '오후') & (df_final_unique['근무자'] == worker) & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]['날짜'].unique()
                
            # 토요/휴일 스케줄은 오전당직(온콜) 배정 대상에서 제외합니다.
            eligible_dates = [d for d in eligible_dates if d in remaining_dates and d not in special_schedule_dates_set]
        
            if not eligible_dates: continue
            
            selected_dates = random.sample(eligible_dates, min(count, len(eligible_dates)))
            for selected_date in selected_dates:
                oncall[selected_date] = worker
                remaining_dates.remove(selected_date)
        
        # 남아있는 날짜 중 토요/휴일 스케줄이 아닌 날짜에 대해서만 경고를 출력합니다.
        for date in remaining_dates:
            if date in special_schedule_dates_set:
                # 토요/휴일은 경고를 출력하지 않고 건너뜁니다.
                continue
                
            afternoon_workers_df = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후') & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]
            afternoon_workers = afternoon_workers_df['근무자'].tolist()
            if afternoon_workers:
                selected_worker = random.choice(afternoon_workers)
                oncall[date] = selected_worker
            else:
                date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%-m월 %-d일')
                st.warning(f"⚠️ {formatted_date}에는 오후 근무자가 없어 오전당직(온콜)을 배정할 수 없습니다.")

        for idx, row in df_schedule.iterrows():
                date = row['날짜']
                df_excel.at[idx, '오전당직(온콜)'] = oncall.get(date, '')
        actual_oncall_counts = {}
        for date, worker in oncall.items(): actual_oncall_counts[worker] = actual_oncall_counts.get(worker, 0) + 1
        for worker, actual_count in actual_oncall_counts.items():
                max_count = oncall_assignments.get(worker, 0)
                if actual_count > max_count: st.info(f"오전당직(온콜) 횟수 제한 한계로, {worker} 님이 최대 배치 {max_count}회가 아닌 {actual_count}회 배치되었습니다.")

        # 플랫폼에 따라 폰트 선택
        if platform.system() == "Windows":
            font_name = "맑은 고딕"  # Windows에서 기본 제공
        else:
            font_name = "Arial"  # Mac에서 기본 제공, Windows에서도 사용 가능

        # 폰트 정의
        duty_font = Font(name=font_name, size=9, bold=True, color="FF69B4")  # 볼드체 + 핑크색
        default_font = Font(name=font_name, size=9)  # 기본 폰트 (볼드체 없음, 검은색)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "스케줄"

        # 1. 색상 맵에 특수근무용 색상 추가
        color_map = {
            '🔴 빨간색': 'C00000', '🟠 주황색': 'FFD966', '🟢 초록색': '92D050',
            '🟡 노란색': 'FFFF00', '🔵 파란색': '0070C0', '🟣 보라색': '7030A0',
            '기본': 'FFFFFF', '특수근무색': 'B7DEE8'  # 특수근무 셀 색상
        }
        # 2. 특수근무일/빈 날짜용 색상 미리 정의
        special_day_fill = PatternFill(start_color='95B3D7', end_color='95B3D7', fill_type='solid')
        empty_day_fill = PatternFill(start_color='808080', end_color='808080', fill_type='solid')
        default_day_fill = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')

        # 헤더 생성
        for col_idx, col_name in enumerate(df_excel.columns, 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.value = col_name
            cell.fill = PatternFill(start_color='000000', end_color='000000', fill_type='solid')
            cell.font = Font(name=font_name, size=9, color='FFFFFF', bold=True)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin', color='000000'),
                                right=Side(style='thin', color='000000'),
                                top=Side(style='thin', color='000000'),
                                bottom=Side(style='thin', color='000000'))

        border = Border(left=Side(style='thin', color='000000'),
                        right=Side(style='thin', color='000000'),
                        top=Side(style='thin', color='000000'),
                        bottom=Side(style='thin', color='000000'))

        # 데이터 행 순회하며 스타일 적용
        for row_idx, (idx, row) in enumerate(df_excel.iterrows(), 2):
            date_str_lookup = df_schedule.at[idx, '날짜']
            special_schedule_dates_set = {s[0] for s in special_schedules}
            is_special_day = date_str_lookup in special_schedule_dates_set
            is_empty_day = df_final_unique[df_final_unique['날짜'] == date_str_lookup].empty and not is_special_day

            # 행 전체 스타일 적용
            for col_idx, col_name in enumerate(df_excel.columns, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.value = row[col_name]
                cell.font = default_font  # 기본 폰트로 초기화
                cell.border = border
                cell.alignment = Alignment(horizontal='center', vertical='center')

                # 우선순위 1: 빈 날짜 행 전체 음영 처리
                if is_empty_day:
                    cell.fill = empty_day_fill
                    continue  # 빈 행은 아래 스타일 로직을 건너뜀

                # 우선순위 2: 그 외의 경우, 각 셀에 맞는 스타일 적용
                if col_name == '날짜':
                    cell.fill = empty_day_fill  # '날짜' 열은 항상 회색
                elif col_name == '요일':
                    if is_special_day:
                        cell.fill = special_day_fill  # 특수근무일 '요일' 셀
                    else:
                        cell.fill = default_day_fill  # 일반 '요일' 셀
                elif str(col_name).isdigit() or '오후' in str(col_name):
                    worker = row[col_name]
                    if worker:
                        time_slot_lookup = '오전' if str(col_name).isdigit() else '오후'
                        worker_data = df_final_unique[(df_final_unique['날짜'] == date_str_lookup) & (df_final_unique['시간대'] == time_slot_lookup) & (df_final_unique['근무자'] == worker)]
                        if not worker_data.empty:
                            color_name = worker_data.iloc[0]['색상']
                            cell.fill = PatternFill(start_color=color_map.get(color_name, 'FFFFFF'), end_color=color_map.get(color_name, 'FFFFFF'), fill_type='solid')
                            memo_text = worker_data.iloc[0]['메모']
                            if memo_text:  # 메모가 있을 경우에만 추가 (특수근무는 메모가 ''이므로 추가 안됨)
                                cell.comment = Comment(memo_text, "Schedule Bot")
                elif col_name == '오전당직(온콜)':
                    if row[col_name]:
                        cell.font = duty_font  # 오전당직(온콜): 볼드체 + 핑크색
                    else:
                        cell.font = default_font  # 빈 셀: 기본 폰트

        ws.column_dimensions['A'].width = 10
        for col in ws.columns:
            if col[0].column_letter != 'A':
                ws.column_dimensions[col[0].column_letter].width = 7

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        st.session_state.output = output

        # ... 이하 G-Sheet 저장 및 다운로드 버튼 표시 로직
        month_dt = datetime.datetime.strptime(month_str, "%Y년 %m월")
        # 다다음달 설정
        next_month_dt = (month_dt + relativedelta(months=1)).replace(day=1)
        next_month_str = next_month_dt.strftime("%Y년 %-m월")
        # 스케줄 저장은 익월로
        month_start = month_dt.replace(day=1)
        month_end = month_dt.replace(day=last_day)  # last_day 사용

        # 날짜 및 인원 입력
        special_schedules = []
        for i in range(st.session_state.special_schedule_count):
            cols = st.columns([2, 3])
            with cols[0]:
                selected_date = st.date_input(
                    label=f"날짜 선택",
                    value=None,
                    min_value=month_dt,
                    max_value=month_dt.replace(day=last_day),
                    key=f"special_date_{i}",
                    help="주말, 공휴일 등 정규 스케줄 외 근무가 필요한 날짜를 선택하세요."
                )
            with cols[1]:
                if selected_date:
                    selected_workers = st.multiselect(
                        label=f"근무 인원 선택",
                        options=all_names,
                        key=f"special_workers_{i}"
                    )
                    if selected_workers:
                        special_schedules.append((selected_date.strftime('%Y-%m-%d'), selected_workers))

        try:
            url = st.secrets["google_sheet"]["url"]
            gc = get_gspread_client()
            if gc is None: st.stop()
            sheet = gc.open_by_url(url)
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (연결 단계): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"Google Sheets 연결 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"Google Sheets 연결 중 오류: {type(e).__name__} - {e}")
            st.stop()

        df_schedule_to_save = transform_schedule_data(df_final_unique, df_excel, month_start, month_end)
        try:
            try:
                worksheet_schedule = sheet.worksheet(f"{month_str} 스케줄")
            except gspread.exceptions.WorksheetNotFound:
                worksheet_schedule = sheet.add_worksheet(title=f"{month_str} 스케줄", rows=1000, cols=50)
            worksheet_schedule.clear()
            data_to_save = [df_schedule_to_save.columns.tolist()] + df_schedule_to_save.astype(str).values.tolist()
            worksheet_schedule.update('A1', data_to_save, value_input_option='RAW')
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 ({month_str} 스케줄 저장): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"{month_str} 스케줄 저장 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"{month_str} 스케줄 저장 중 오류: {type(e).__name__} - {e}")
            st.stop()

        df_cumulative_next.rename(columns={'이름': next_month_str}, inplace=True)
        try:
            try:
                worksheet_cumulative = sheet.worksheet(f"{next_month_str} 누적")
            except gspread.exceptions.WorksheetNotFound:
                worksheet_cumulative = sheet.add_worksheet(title=f"{next_month_str} 누적", rows=1000, cols=20)
            worksheet_cumulative.clear()
            cumulative_data_to_save = [df_cumulative_next.columns.tolist()] + df_cumulative_next.values.tolist()
            worksheet_cumulative.update('A1', cumulative_data_to_save, value_input_option='USER_ENTERED')
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접수되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 ({next_month_str} 누적 저장): {e.response.status_code} - {e.response.text}")
            st.stop()
        except NameError as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"{next_month_str} 누적 저장 중 오류: {type(e).__name__} - {e}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"{next_month_str} 누적 저장 중 오류: {type(e).__name__} - {e}")
            st.stop()

        st.session_state.assigned = True
        st.session_state.output = output

        st.write(" ")
        st.markdown(f"**➕ {next_month_str} 누적 테이블**")
        st.dataframe(df_cumulative_next, use_container_width=True, hide_index=True)
        st.success(f"✅ {next_month_str} 누적 테이블이 Google Sheets에 저장되었습니다.")
        st.divider()
        st.success(f"✅ {month_str} 스케줄 테이블이 Google Sheets에 저장되었습니다.")

        st.markdown("""<style>.download-button > button { background-color: #4CAF50; color: white; border-radius: 5px; padding: 10px; font-size: 16px; }</style>""", unsafe_allow_html=True)
        if st.session_state.assigned and not st.session_state.downloaded:
            with st.container():
                st.download_button(
                    label="📥 최종 스케줄 다운로드",
                    data=st.session_state.output,
                    file_name=f"{month_str} 스케줄.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_schedule_button",
                    use_container_width=True,
                    type="primary",
                    on_click=lambda: st.session_state.update({"downloaded": True})
                )