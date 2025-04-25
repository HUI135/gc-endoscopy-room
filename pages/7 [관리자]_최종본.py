import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import random
import time
from datetime import datetime, date
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment

# 세션 상태 초기화
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.batch_update([
                {"range": "A1:D", "values": [[]]},
                {"range": "A1", "values": data}
            ])
            return
        except Exception as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.warning(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# 데이터 로드 (캐싱 사용)
@st.cache_data
def load_data(month_str):
    return load_data_no_cache(month_str)

# 데이터 로드 (캐싱 미사용)
def load_data_no_cache(month_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    
    try:
        worksheet_room = sheet.worksheet(f"{month_str} 방배정")
        df_room = pd.DataFrame(worksheet_room.get_all_records())
    except Exception as e:
        st.error(f"스케줄 시트를 불러오는 데 실패: {e}")
        st.stop()
    
    st.session_state["df_room"] = df_room
    st.session_state["data_loaded"] = True
    
    return df_room

# 메인
month_str = "2025년 04월"
next_month_start = date(2025, 4, 1)
next_month_end = date(2025, 4, 30)

# 로그인 체크
if "login_success" not in st.session_state or not st.session_state["login_success"]:
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# 관리자 권한 체크
if not st.session_state.get("is_admin_authenticated", False):
    st.warning("⚠️ 관리자 권한이 없습니다.")
    st.stop()

# 사이드바
st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
if st.sidebar.button("로그아웃"):
    st.session_state.clear()
    st.success("로그아웃되었습니다.")
    st.rerun()

# 새로고침 버튼 (맨 위로 이동)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    df_room = load_data_no_cache(month_str)
    st.session_state["df_room"] = df_room
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 메인
st.subheader(f"✨ {month_str} 방 배정 확인")

# Google Sheets에서 방배정 데이터 로드
df_room = load_data(month_str)
st.dataframe(df_room)

# 수정된 방배정 파일 업로드
st.write(" ")
st.subheader(f"✨ {month_str} 방 배정 수정 파일 업로드")
st.write("- 모든 인원의 근무 횟수가 원본과 동일한지, 누락 및 추가 인원이 있는지 확인합니다.")
st.write("- 날짜별 오전(8:30, 9:00, 9:30, 10:00) 및 오후(13:30) 시간대에 동일 인물이 중복 배정되지 않았는지 확인합니다.")
uploaded_file = st.file_uploader("방배정 수정 파일", type=["xlsx", "csv"])

if uploaded_file:
    # 업로드된 파일 읽기
    try:
        if uploaded_file.name.endswith(".xlsx"):
            df_room_md = pd.read_excel(uploaded_file)
        elif uploaded_file.name.endswith(".csv"):
            df_room_md = pd.read_csv(uploaded_file)
        else:
            st.error("지원되지 않는 파일 형식입니다. XLSX 또는 CSV 파일을 업로드해주세요.")
            st.stop()

        # 데이터프레임 컬럼 확인
        if not df_room.columns.equals(df_room_md.columns):
            st.error("업로드된 파일의 컬럼이 원본 데이터와 일치하지 않습니다.")
            st.stop()

        # 날짜별 오전/오후 중복 배정 확인
        morning_slots = [col for col in df_room_md.columns if col.startswith(('8:30', '9:00', '9:30', '10:00')) and col != '온콜']
        afternoon_slots = [col for col in df_room_md.columns if col.startswith('13:30')]
        duplicate_errors = []

        for idx, row in df_room_md.iterrows():
            date_str = row['날짜']
            # 오전 슬롯 중복 확인
            morning_assignments = [row[col] for col in morning_slots if pd.notna(row[col]) and row[col].strip()]
            morning_counts = Counter(morning_assignments)
            for person, count in morning_counts.items():
                if person and count > 1:
                    duplicate_errors.append(f"{date_str}: {person}이(가) 오전 시간대에 {count}번 중복 배정되었습니다.")

            # 오후 슬롯 중복 확인
            afternoon_assignments = [row[col] for col in afternoon_slots if pd.notna(row[col]) and row[col].strip()]
            afternoon_counts = Counter(afternoon_assignments)
            for person, count in afternoon_counts.items():
                if person and count > 1:
                    duplicate_errors.append(f"{date_str}: {person}이(가) 오후 시간대에 {count}번 중복 배정되었습니다.")

        # 각 인원의 전체 근무 횟수 계산 (df_room)
        count_room = Counter()
        for _, row in df_room.drop(columns=["날짜", "요일"]).iterrows():
            for value in row:
                if pd.notna(value) and value.strip():
                    count_room[value] += 1

        # 각 인원의 전체 근무 횟수 계산 (df_room_md)
        count_room_md = Counter()
        for _, row in df_room_md.drop(columns=["날짜", "요일"]).iterrows():
            for value in row:
                if pd.notna(value) and value.strip():
                    count_room_md[value] += 1

        # 근무 횟수 비교
        all_names = set(count_room.keys()).union(set(count_room_md.keys()))
        count_discrepancies = []
        for name in all_names:
            orig_count = count_room.get(name, 0)
            mod_count = count_room_md.get(name, 0)
            if orig_count != mod_count:
                if mod_count < orig_count:
                    count_discrepancies.append(f"{name}이(가) 기존 파일보다 근무가 {orig_count - mod_count}회 적습니다.")
                elif mod_count > orig_count:
                    count_discrepancies.append(f"{name}이(가) 기존 파일보다 근무가 {mod_count - orig_count}회 많습니다.")

        # 결과 출력
        if duplicate_errors or count_discrepancies:
            if duplicate_errors:
                for error in duplicate_errors:
                    st.warning(error)
            if count_discrepancies:
                for warning in count_discrepancies:
                    st.warning(warning)
        else:
            st.success("모든 인원의 근무 횟수가 원본과 동일하며, 중복 배정 오류가 없습니다!")

    except Exception as e:
        st.error(f"파일 처리 중 오류 발생: {str(e)}")