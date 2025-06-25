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
month_str = "2025년 04월"

# Google Sheets 클라이언트 초기화
@st.cache_resource # 이 함수 자체를 캐싱하여 불필요한 초기화 반복 방지
def get_gspread_client():
    # st.write("DEBUG: get_gspread_client() 호출 시작") # 너무 자주 나올 수 있어 주석 처리
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        gc = gspread.authorize(credentials)
        # st.success("✅ Google Sheets 클라이언트 인증 성공!") # 성공 메시지는 load_data_page5에서만
        # st.write("DEBUG: get_gspread_client() 호출 종료")
        return gc
    except Exception as e:
        st.error(f"❌ Google Sheets 클라이언트 초기화 또는 인증 실패: {type(e).__name__} - {e}")
        st.exception(e) # 상세 스택 트레이스 출력
        st.stop() # 치명적인 오류이므로 앱 중단


# 데이터 로드 함수 (세션 상태 활용으로 쿼터 절약)
@st.cache_data(ttl=3600) # 데이터를 1시간 동안 캐시. 개발 중에는 ttl을 0으로 설정하거나 캐시를 자주 지우세요.
def load_data_page5():
    st.write("DEBUG: load_data_page5() 호출 시작") # 디버그 메시지
    required_keys = ["df_master", "df_request", "df_cumulative", "df_shift", "df_supplement"]
    if "data_loaded" not in st.session_state or not st.session_state["data_loaded"] or not all(key in st.session_state for key in required_keys):
        st.write("DEBUG: 데이터 로드 필요. Google Sheets에서 데이터 가져오는 중...") # 디버그 메시지
        url = st.secrets["google_sheet"]["url"]
        gc = get_gspread_client() # 캐싱된 클라이언트 가져오기
        if gc is None: # get_gspread_client에서 이미 stop()을 하지만, 방어 코드
            st.stop()

        try:
            sheet = gc.open_by_url(url)
            st.write(f"DEBUG: 스프레드시트 '{url}' 열기 성공.") # 디버그 메시지
        except APIError as e:
            st.error(f"❌ 스프레드시트 열기 API 오류: {e.response.status_code} - {e.response.text}")
            st.exception(e) # 상세 스택 트레이스 출력
            st.stop()
        except Exception as e:
            st.error(f"❌ 스프레드시트 열기 실패: {type(e).__name__} - {e}")
            st.exception(e) # 상세 스택 트레이스 출력
            st.stop()

        # 마스터 시트
        try:
            worksheet1 = sheet.worksheet("마스터")
            st.session_state["df_master"] = pd.DataFrame(worksheet1.get_all_records())
            st.session_state["worksheet1"] = worksheet1
            st.write("DEBUG: '마스터' 시트 로드 성공.") # 디버그 메시지
        except WorksheetNotFound:
            st.error("❌ '마스터' 시트를 찾을 수 없습니다. 시트 이름을 확인해주세요.")
            st.stop()
        except APIError as e:
            st.error(f"❌ '마스터' 시트 로드 API 오류: {e.response.status_code} - {e.response.text}")
            st.exception(e)
            st.stop()
        except Exception as e:
            st.error(f"❌ '마스터' 시트 로드 실패: {type(e).__name__} - {e}")
            st.exception(e)
            st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            st.session_state["data_loaded"] = False
            st.stop()

        # 요청사항 시트
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
            st.write(f"DEBUG: '{month_str} 요청' 시트 로드 성공.") # 디버그 메시지
        except WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 요청' 시트를 찾을 수 없습니다. 새로 생성합니다.")
            try:
                worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                worksheet2.append_row(["이름", "분류", "날짜정보"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "요청 없음", ""] for name in names_in_master]
                for row in new_rows:
                    worksheet2.append_row(row)
                st.write(f"DEBUG: '{month_str} 요청' 시트 새로 생성 및 초기 데이터 추가 성공.") # 디버그 메시지
            except APIError as e:
                st.error(f"❌ '{month_str} 요청' 시트 생성/초기화 API 오류: {e.response.status_code} - {e.response.text}")
                st.exception(e)
                st.stop()
            except Exception as e:
                st.error(f"❌ '{month_str} 요청' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.exception(e)
                st.stop()

        st.session_state["df_request"] = pd.DataFrame(worksheet2.get_all_records()) if worksheet2.get_all_records() else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["worksheet2"] = worksheet2

        # 누적 시트
        try:
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
            st.write(f"DEBUG: '{month_str} 누적' 시트 로드 성공.") # 디버그 메시지
        except WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 누적' 시트를 찾을 수 없습니다. 새로 생성합니다.")
            try:
                worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
                worksheet4.append_row([f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "", "", "", ""] for name in names_in_master]
                for row in new_rows:
                    worksheet4.append_row(row)
                st.write(f"DEBUG: '{month_str} 누적' 시트 새로 생성 및 초기 데이터 추가 성공.") # 디버그 메시지
            except APIError as e:
                st.error(f"❌ '{month_str} 누적' 시트 생성/초기화 API 오류: {e.response.status_code} - {e.response.text}")
                st.exception(e)
                st.stop()
            except Exception as e:
                st.error(f"❌ '{month_str} 누적' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.exception(e)
                st.stop()
        
        # --- 수정: df_cumulative 로드 후 첫 번째 컬럼 이름을 '이름'으로 강제 변경 및 숫자 컬럼 타입 변환 ---
        df_cumulative_temp = pd.DataFrame(worksheet4.get_all_records()) if worksheet4.get_all_records() else pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
        if not df_cumulative_temp.empty:
            # 첫 번째 컬럼의 실제 이름이 무엇이든 '이름'으로 변경
            df_cumulative_temp.rename(columns={df_cumulative_temp.columns[0]: '이름'}, inplace=True)
            # 모든 누적 관련 컬럼을 숫자로 변환 (오류 방지)
            for col_name in ["오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]:
                if col_name in df_cumulative_temp.columns:
                    # errors='coerce'를 사용하여 변환 불가능한 값은 NaN으로 만들고, fillna(0)으로 0으로 채움
                    df_cumulative_temp[col_name] = pd.to_numeric(df_cumulative_temp[col_name], errors='coerce').fillna(0).astype(int)
        st.session_state["df_cumulative"] = df_cumulative_temp
        # --- 수정 끝 ---

        st.session_state["worksheet4"] = worksheet4

        # df_shift와 df_supplement 생성 및 세션 상태에 저장
        st.session_state["df_shift"] = generate_shift_table(st.session_state["df_master"])
        st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], st.session_state["df_master"]["이름"].unique())

        st.session_state["data_loaded"] = True
        st.write("DEBUG: load_data_page5() 호출 종료 (성공)") # 디버그 메시지


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
    df[column_name] = df[column_name].str.replace("\n", ", ")
    
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

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.cache_resource.clear() # @st.cache_resource 적용 시 캐시 초기화
    st.session_state["data_loaded"] = False  # 데이터 리로드 강제
    load_data_page5()  # load_data_page5 호출로 모든 데이터 갱신
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 메인 로직
load_data_page5()
# Use .get() with fallback to avoid KeyError
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
# df_cumulative 컬럼 이름은 load_data_page5에서 '이름'으로 변경되었음
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])) # fallback 컬럼도 '이름'으로 통일
df_shift = st.session_state.get("df_shift", pd.DataFrame())  # 세션 상태에서 가져오기
df_supplement = st.session_state.get("df_supplement", pd.DataFrame())  # 세션 상태에서 가져오기

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
st.dataframe(df_shift, use_container_width=True)

# 보충 테이블 (중복된 df_master 표시 제거, df_supplement 표시)
st.markdown("**☑️ 보충 테이블**")
st.dataframe(df_supplement, use_container_width=True)

# 요청사항 테이블
st.markdown("**🙋‍♂️ 요청사항 테이블**")
st.dataframe(df_request, use_container_width=True)

# 누적 테이블
st.markdown("**➕ 누적 테이블**")
st.dataframe(df_cumulative, use_container_width=True)

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

# 2025년 4월 평일 생성
next_month = datetime.datetime(2025, 4, 1)
_, last_day = calendar.monthrange(next_month.year, next_month.month)
dates = pd.date_range(start=next_month, end=next_month.replace(day=last_day))
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
                # st.write(f"잘못된 날짜 형식 무시됨: {single_date}") # DEBUG 메시지로 변경
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
            # st.write(f"잘못된 날짜 범위 무시됨: {date_str}, 에러: {e}") # DEBUG 메시지로 변경
            pass # 이 메시지는 너무 많이 나올 수 있어 주석 처리
            return []
    try:
        parsed_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        if parsed_date.weekday() < 5:
            return [date_str]
        return []
    except ValueError:
        # st.write(f"잘못된 날짜 형식 무시됨: {date_str}") # DEBUG 메시지로 변경
        pass # 이 메시지는 너무 많이 나올 수 있어 주석 처리
        return []

# 근무자 상태 업데이트 함수
def update_worker_status(df, date_str, time_slot, worker, status, memo, color):
    existing = df[
        (df['날짜'] == date_str) &
        (df['시간대'] == time_slot) &
        (df['근무자'] == worker.strip())
    ]
    if not existing.empty:
        df.loc[existing.index, ['상태', '메모', '색상']] = [status, memo, color]
    else:
        new_row = pd.DataFrame({
            '날짜': [date_str],
            '요일': [day_map[pd.to_datetime(date_str).weekday()]],
            '주차': [week_numbers.get(date_obj.date())],
            '시간대': [time_slot],
            '근무자': [worker.strip()],
            '상태': [status],
            '메모': [memo],
            '색상': [color]
        })
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
    # '근무'와 '보충' 상태만 필터링 (평일 데이터)
    df = df[df['상태'].isin(['근무', '보충'])][['날짜', '시간대', '근무자', '요일']].copy()
    
    # 전체 날짜 범위 생성
    date_range = pd.date_range(start=month_start, end=month_end)
    # 날짜를 "4월 1일" 형태로 포맷팅
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

# 근무 배정 로직 (날짜 관련 변수 설정)
month_dt = datetime.datetime.strptime(month_str, "%Y년 %m월")
_, last_day = calendar.monthrange(month_dt.year, month_dt.month)
all_month_dates = pd.date_range(start=month_dt, end=month_dt.replace(day=last_day))
weekdays = [d for d in all_month_dates if d.weekday() < 5]
# 이 부분: 키를 .date() 객체로 생성
week_numbers = {d.to_pydatetime().date(): (d.day - 1) // 7 + 1 for d in all_month_dates}
day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

# --- UI 개선: 토요/휴일 스케줄 입력 ---
st.markdown("**📅 토요/휴일 스케줄 입력**")

# 전체 인원 목록 준비
all_names = sorted(list(df_master["이름"].unique()))

# special_schedules 리스트 초기화
special_schedules = []

# st.session_state을 사용하여 추가된 입력 필드 수 관리
if 'special_schedule_count' not in st.session_state:
    st.session_state.special_schedule_count = 1

for i in range(st.session_state.special_schedule_count):
    cols = st.columns([2, 3])
    with cols[0]:
        # 날짜 선택 위젯 (전체 월 대상)
        selected_date = st.date_input(
            label=f"날짜 선택",
            value=None,
            min_value=month_dt.date(),
            max_value=month_dt.replace(day=last_day).date(),
            key=f"special_date_{i}",
            help="주말, 공휴일 등 정규 스케줄 외 근무가 필요한 날짜를 선택하세요."
        )
    with cols[1]:
        if selected_date:
            # 인원 선택 위젯 (제한 없음)
            selected_workers = st.multiselect(
                label=f"근무 인원 선택",
                options=all_names,
                key=f"special_workers_{i}"
            )
            # 선택된 스케줄 정보를 리스트에 저장
            special_schedules.append((selected_date.strftime('%Y-%m-%d'), selected_workers))

# 입력 필드 추가 버튼
if st.button("➕ 토요/휴일 스케줄 추가"):
    st.session_state.special_schedule_count += 1
    st.rerun()

if st.button("🚀 근무 배정 실행"):
    # 버튼 클릭 시 세션 상태 초기화
    st.session_state.assigned = False
    st.session_state.output = None
    st.session_state.downloaded = False

    special_schedule_dates = [s[0] for s in special_schedules]

    with st.spinner("근무 배정 중..."):
        time.sleep(1)

        # --- 로직 시작 ---
        
        # 날짜별 오전 근무 제외 인원 추적용 딕셔너리
        excluded_morning_workers = {date.strftime('%Y-%m-%d'): set() for date in weekdays}

        # 휴관일을 제외한 평일 리스트 생성
        active_weekdays = [date for date in weekdays if date.strftime('%Y-%m-%d') not in holiday_dates]

        # --- BUG FIX: '최초 배정자' 명단 생성 ---
        initial_master_assignments = {}
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            day_name = day_map[date.weekday()]
            week_num = week_numbers[date.date()]
            for time_slot in ['오전', '오후']:
                shift_key = f"{day_name} {time_slot}"
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                base_workers = set()
                if not shift_row.empty:
                    for col in [f'근무{i}' for i in range(1, 15)]:
                        worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                        if worker:
                            if '(' in worker:
                                name, weeks = worker.split('(')
                                name = name.strip()
                                weeks = weeks.rstrip(')').split(',')
                                if f'{week_num}주' in weeks: base_workers.add(name)
                            else: base_workers.add(worker)
                initial_master_assignments[(date_str, time_slot)] = base_workers
        
        # 1단계: 모든 날짜에 대해 기본 배정 및 휴가자 처리
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            requests_on_date = df_request[df_request['날짜정보'].apply(lambda x: date_str in parse_date_range(str(x)))]
            vacationers = requests_on_date[requests_on_date['분류'] == '휴가']['이름'].tolist()
            
            for time_slot in ['오전', '오후']:
                base_workers = initial_master_assignments.get((date_str, time_slot), set())
                must_work = requests_on_date[requests_on_date['분류'] == f'꼭 근무({time_slot})']['이름'].tolist()
                
                final_workers = [w for w in base_workers if w not in vacationers]
                for mw in must_work:
                    if mw not in final_workers and mw not in vacationers: final_workers.append(mw)
                
                if time_slot == '오후':
                    morning_workers_on_date = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'] == '근무')]['근무자'].tolist()
                    final_workers = [w for w in final_workers if (w in morning_workers_on_date or w in must_work)]

                for worker in final_workers:
                    memo = f'꼭 근무({time_slot}) 위해 배정됨' if worker in must_work else ''
                    color = '🟠 주황색' if worker in must_work else '기본'
                    df_final = update_worker_status(df_final, date_str, time_slot, worker, '근무', memo, color)

                for vac in vacationers:
                    if vac in base_workers:
                        df_final = update_worker_status(df_final, date_str, time_slot, vac, '제외', '휴가로 제외됨', '🔴 빨간색')
                        if time_slot == '오전': excluded_morning_workers[date_str].add(vac)

        # '이동 완료자' 명단
        moved_workers_in_balancing = set() 

        # 2/3/4단계 통합: 1:1 인원 이동 (기본 보충/제외)
        for time_slot in ['오전', '오후']:
            target_count = 12 if time_slot == '오전' else 5
            
            iteration = 0
            while True:
                iteration += 1
                if iteration > 100:
                    st.warning(f"⚠️ {time_slot} 인원 이동 로직이 100회를 초과하여 중단됩니다.")
                    break
                
                excess_dates, shortage_dates = [], []
                for date in active_weekdays:
                    date_str = date.strftime('%Y-%m-%d')
                    workers_on_date = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].tolist()
                    count = len(workers_on_date)
                    if count > target_count: excess_dates.append((date_str, count - target_count))
                    elif count < target_count: shortage_dates.append((date_str, target_count - count))

                if not excess_dates or not shortage_dates: break
                
                any_match_found_in_pass = False
                
                for excess_date, _ in excess_dates:
                    excess_workers = df_final[(df_final['날짜'] == excess_date) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].tolist()
                    must_work_on_excess = [r['이름'] for _, r in df_request.iterrows() if excess_date in parse_date_range(str(r['날짜정보'])) and r['분류'] == f'꼭 근무({time_slot})']
                    movable_workers = [w for w in excess_workers if w not in must_work_on_excess]
                    if not movable_workers: continue
                    movable_workers.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0), reverse=True)
                    
                    for worker_to_move in movable_workers:
                        if worker_to_move in moved_workers_in_balancing: continue

                        for shortage_date, __ in shortage_dates:
                            can_move = True
                            
                            # --- BUG FIX: '원조 멤버'는 보충 금지 ---
                            initial_workers_on_shortage = initial_master_assignments.get((shortage_date, time_slot), set())
                            if worker_to_move in initial_workers_on_shortage:
                                can_move = False
                                continue # 다음 부족일 탐색
                            # --- BUG FIX END ---
                            
                            no_supplement_on_shortage = [r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r['날짜정보'])) and r['분류'] == f'보충 불가({time_slot})']
                            
                            if worker_to_move in no_supplement_on_shortage: can_move = False
                            
                            if time_slot == '오후':
                                morning_workers_on_shortage = df_final[(df_final['날짜'] == shortage_date) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'].tolist()
                                must_work_on_shortage_afternoon = [r['이름'] for _, r in df_request.iterrows() if shortage_date in parse_date_range(str(r['날짜정보'])) and r['분류'] == '꼭 근무(오후)']
                                if worker_to_move not in morning_workers_on_shortage and worker_to_move not in must_work_on_shortage_afternoon: can_move = False

                            if can_move:
                                excess_date_formatted = pd.to_datetime(excess_date).strftime('%-m월 %-d일')
                                shortage_date_formatted = pd.to_datetime(shortage_date).strftime('%-m월 %-d일')
                                
                                df_final = update_worker_status(df_final, excess_date, time_slot, worker_to_move, '제외', f'{shortage_date_formatted} 보충 위해 제외됨', '🔵 파란색')
                                df_final = update_worker_status(df_final, shortage_date, time_slot, worker_to_move, '보충', f'{excess_date_formatted}에서 제외되어 보충됨', '🟢 초록색')
                                
                                if time_slot == '오전': excluded_morning_workers[excess_date].add(worker_to_move)
                                
                                moved_workers_in_balancing.add(worker_to_move)
                                any_match_found_in_pass = True
                                break 
                        if any_match_found_in_pass: break
                    if any_match_found_in_pass: break
                if not any_match_found_in_pass: break
        
        # 5단계: 최종 추가 보충/제외 수행
        for date in active_weekdays:
            date_str = date.strftime('%Y-%m-%d')
            for time_slot in ['오전', '오후']:
                target_count = 12 if time_slot == '오전' else 5
                
                # '보충' 상태까지 포함한 현재 인원
                current_workers = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == time_slot) & (df_final['상태'].isin(['근무', '보충']))]['근무자'].tolist()
                
                if len(current_workers) < target_count: # 인원이 부족할 때만 추가 보충
                    needed = target_count - len(current_workers)
                    day_name = day_map[pd.to_datetime(date_str).weekday()]
                    shift_key = f"{day_name} {time_slot}"
                    supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shift_key]
                    supplement_candidates = []
                    if not supplement_row.empty:
                        for col in supplement_row.columns[1:]:
                             worker = supplement_row[col].values[0]
                             if pd.notna(worker): supplement_candidates.append(worker.replace('🔺', '').strip())
                    
                    no_supplement_on_date = [r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r['날짜정보'])) and r['분류'] == f'보충 불가({time_slot})']
                    supplement_candidates = [w for w in supplement_candidates if w not in current_workers and w not in no_supplement_on_date]
                    if time_slot == '오후':
                         morning_workers = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오전') & (df_final['상태'].isin(['근무', '보충', '추가보충']))]['근무자'].tolist()
                         supplement_candidates = [w for w in supplement_candidates if w in morning_workers]
                    supplement_candidates.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0))
                    for _ in range(needed):
                        if not supplement_candidates: break
                        worker_to_add = supplement_candidates.pop(0)
                        df_final = update_worker_status(df_final, date_str, time_slot, worker_to_add, '추가보충', '인원 부족으로 인한 추가 보충', '🟡 노란색')
                        current_cumulative[time_slot][worker_to_add] = current_cumulative.get(time_slot, {}).get(worker_to_add, 0) + 1
                
                elif len(current_workers) > target_count: # 인원이 초과될 때만 추가 제외
                    over_count = len(current_workers) - target_count
                    must_work_on_date = [r['이름'] for _, r in df_request.iterrows() if date_str in parse_date_range(str(r['날짜정보'])) and r['분류'] == f'꼭 근무({time_slot})']
                    removable_workers = [w for w in current_workers if w not in must_work_on_date]
                    removable_workers.sort(key=lambda w: current_cumulative.get(time_slot, {}).get(w, 0), reverse=True)
                    for _ in range(over_count):
                        if not removable_workers: break
                        worker_to_remove = removable_workers.pop(0)
                        df_final = update_worker_status(df_final, date_str, time_slot, worker_to_remove, '추가제외', '인원 초과로 인한 추가 제외', '🟣 보라색')
                        current_cumulative[time_slot][worker_to_remove] = current_cumulative.get(time_slot, {}).get(worker_to_remove, 0) - 1
                        if time_slot == '오전':
                            excluded_morning_workers[date_str].add(worker_to_remove)
                            is_afternoon_worker = df_final[(df_final['날짜'] == date_str) & (df_final['시간대'] == '오후') & (df_final['근무자'] == worker_to_remove) & (df_final['상태'].isin(['근무', '보충']))].shape[0] > 0
                            if is_afternoon_worker:
                                df_final = update_worker_status(df_final, date_str, '오후', worker_to_remove, '추가제외', '오전 추가제외로 인한 오후 제외', '🟣 보라색')
                                current_cumulative['오후'][worker_to_remove] = current_cumulative.get('오후', {}).get(worker_to_remove, 0) - 1
        
        # 다음 달 누적 근무량 계산
        df_cumulative_next = df_cumulative.copy().set_index('이름')
        for worker, count in current_cumulative.get('오전', {}).items():
            if worker in df_cumulative_next.index: df_cumulative_next.loc[worker, '오전누적'] = count
        for worker, count in current_cumulative.get('오후', {}).items():
             if worker in df_cumulative_next.index: df_cumulative_next.loc[worker, '오후누적'] = count
        df_cumulative_next.reset_index(inplace=True)

        if special_schedules:
            for date_str, workers in special_schedules:
                # 해당 날짜의 자동 배정된 모든 기록(오전/오후)을 df_final에서 삭제
                if not df_final.empty:
                    df_final = df_final[df_final['날짜'] != date_str].copy()

                # 입력된 인원을 '오전' 근무로 새로 추가
                for worker in workers:
                    # 함수 정의에 맞게 7개의 인자만 전달하도록 수정
                    df_final = update_worker_status(df_final, date_str, '오전', worker, '근무', '', '특수근무색')
        
        # 엑셀 및 구글시트 출력을 위한 최종 데이터 생성
        _, last_day = calendar.monthrange(next_month.year, next_month.month)
        all_month_dates = pd.date_range(start=next_month, end=next_month.replace(day=last_day))
        full_day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
        df_schedule = pd.DataFrame({'날짜': [d.strftime('%Y-%m-%d') for d in all_month_dates], '요일': [full_day_map.get(d.weekday()) for d in all_month_dates]})
        
        worker_counts_all = df_final.groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
        max_morning_workers = int(worker_counts_all.get('오전', pd.Series(0)).max()) if '오전' in worker_counts_all else 0
        max_afternoon_workers = int(worker_counts_all.get('오후', pd.Series(0)).max()) if '오후' in worker_counts_all else 0
        
        color_priority = {'🟠 주황색': 0, '🟢 초록색': 1, '🟡 노란색': 2, '기본': 3, '🔴 빨간색': 4, '🔵 파란색': 5, '🟣 보라색': 6}
        df_final['색상_우선순위'] = df_final['색상'].map(color_priority)
        df_final_unique = df_final.sort_values(by=['날짜', '시간대', '근무자', '색상_우선순위']).groupby(['날짜', '시간대', '근무자']).first().reset_index()

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
        for worker, count in workers_priority:
            if count <= 0: continue
            eligible_dates = df_final_unique[(df_final_unique['시간대'] == '오후') & (df_final_unique['근무자'] == worker) & (df_final_unique['상태'].isin(['근무', '보충', '추가보충']))]['날짜'].unique()
            eligible_dates = [d for d in eligible_dates if d in remaining_dates]
            if not eligible_dates: continue
            selected_dates = random.sample(eligible_dates, min(count, len(eligible_dates)))
            for selected_date in selected_dates:
                oncall[selected_date] = worker
                remaining_dates.remove(selected_date)
        if remaining_dates:
            for date in remaining_dates:
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
        
        wb = Workbook()
        ws = wb.active
        ws.title = "스케줄"
        
        # 1. 색상 맵에 특수근무용 색상 추가
        color_map = {
            '🔴 빨간색': 'C00000', '🟠 주황색': 'FFD966', '🟢 초록색': '92D050', 
            '🟡 노란색': 'FFFF00', '🔵 파란색': '0070C0', '🟣 보라색': '7030A0', 
            '기본': 'FFFFFF', '특수근무색': 'B7DEE8' # 특수근무 셀 색상
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
            cell.font = Font(size=9, color='FFFFFF')
            cell.alignment = Alignment(horizontal='center', vertical='center')

        border = Border(left=Side(style='thin', color='000000'), right=Side(style='thin', color='000000'), top=Side(style='thin', color='000000'), bottom=Side(style='thin', color='000000'))
        
        # 데이터 행 순회하며 스타일 적용
        for row_idx, (idx, row) in enumerate(df_excel.iterrows(), 2):
            date_str_lookup = df_schedule.at[idx, '날짜']
            is_special_day = date_str_lookup in special_schedule_dates
            is_empty_day = df_final_unique[df_final_unique['날짜'] == date_str_lookup].empty and not is_special_day
            
            # 행 전체 스타일 적용
            for col_idx, col_name in enumerate(df_excel.columns, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell.value = row[col_name]
                cell.font = Font(size=9)
                cell.border = border
                cell.alignment = Alignment(horizontal='center', vertical='center')

                # 우선순위 1: 빈 날짜 행 전체 음영 처리
                if is_empty_day:
                    cell.fill = empty_day_fill
                    continue # 빈 행은 아래 스타일 로직을 건너뜀

                # 우선순위 2: 그 외의 경우, 각 셀에 맞는 스타일 적용
                if col_name == '날짜':
                    cell.fill = empty_day_fill # '날짜' 열은 항상 회색
                elif col_name == '요일':
                    if is_special_day:
                        cell.fill = special_day_fill # 특수근무일 '요일' 셀
                    else:
                        cell.fill = default_day_fill # 일반 '요일' 셀
                elif str(col_name).isdigit() or '오후' in str(col_name):
                    worker = row[col_name]
                    if worker:
                        time_slot_lookup = '오전' if str(col_name).isdigit() else '오후'
                        worker_data = df_final_unique[(df_final_unique['날짜'] == date_str_lookup) & (df_final_unique['시간대'] == time_slot_lookup) & (df_final_unique['근무자'] == worker)]
                        if not worker_data.empty:
                            color_name = worker_data.iloc[0]['색상']
                            cell.fill = PatternFill(start_color=color_map.get(color_name, 'FFFFFF'), end_color=color_map.get(color_name, 'FFFFFF'), fill_type='solid')
                            memo_text = worker_data.iloc[0]['메모']
                            if memo_text: # 메모가 있을 경우에만 추가 (특수근무는 메모가 ''이므로 추가 안됨)
                                cell.comment = Comment(memo_text, "Schedule Bot")
        
        ws.column_dimensions['A'].width = 10
        for col in ws.columns:
            if col[0].column_letter != 'A':
                ws.column_dimensions[col[0].column_letter].width = 7

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        st.session_state.output = output
        
        import calendar
        # ... 이하 G-Sheet 저장 및 다운로드 버튼 표시 로직은 기존과 동일
        month_dt = datetime.datetime.strptime(month_str, "%Y년 %m월") 
        next_month_dt = (month_dt + timedelta(days=32)).replace(day=1)
        next_month_str = next_month_dt.strftime("%Y년 %m월")
        next_month_start = month_dt.replace(day=1)
        _, last_day = calendar.monthrange(month_dt.year, month_dt.month)
        next_month_end = month_dt.replace(day=last_day)
        
        try:
            url = st.secrets["google_sheet"]["url"]
            gc = get_gspread_client()
            if gc is None: st.stop()
            sheet = gc.open_by_url(url)
        except Exception as e:
            st.error(f"❌ Google Sheets 연결 중 오류 발생 (저장 단계): {e}")
            st.exception(e)
            st.stop()
            
        df_schedule_to_save = transform_schedule_data(df_final_unique, df_excel, next_month_start, next_month_end)
        
        try:
            worksheet_schedule = sheet.worksheet(f"{month_str} 스케줄")
        except WorksheetNotFound:
            worksheet_schedule = sheet.add_worksheet(title=f"{month_str} 스케줄", rows=1000, cols=50)
        worksheet_schedule.clear()
        data_to_save = [df_schedule_to_save.columns.tolist()] + df_schedule_to_save.astype(str).values.tolist()
        worksheet_schedule.update('A1', data_to_save, value_input_option='RAW')
        
        df_cumulative_next.rename(columns={'이름': next_month_str}, inplace=True)
        
        try:
            worksheet_cumulative = sheet.worksheet(f"{next_month_str} 누적")
        except WorksheetNotFound:
            worksheet_cumulative = sheet.add_worksheet(title=f"{next_month_str} 누적", rows=1000, cols=20)
        worksheet_cumulative.clear()
        cumulative_data_to_save = [df_cumulative_next.columns.tolist()] + df_cumulative_next.values.tolist()
        worksheet_cumulative.update('A1', cumulative_data_to_save, value_input_option='USER_ENTERED')

        st.session_state.assigned = True
        st.session_state.output = output
        
        st.write(" ")
        st.markdown(f"**➕ {next_month_str} 누적 테이블**")
        st.dataframe(df_cumulative_next)
        st.success(f"✅ {next_month_str} 누적 테이블이 Google Sheets에 저장되었습니다.")
        st.divider()
        st.success(f"✅ {month_str} 스케줄 테이블이 Google Sheets에 저장되었습니다.")

        st.markdown("""<style>.download-button > button { ... }</style>""", unsafe_allow_html=True)
        if st.session_state.assigned and not st.session_state.downloaded:
            with st.container():
                st.download_button(
                    label="📥 최종 스케줄 다운로드",
                    data=st.session_state.output,
                    file_name=f"{month_str} 스케줄.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_schedule_button",
                    type="primary",
                    on_click=lambda: st.session_state.update({"downloaded": True})
                )