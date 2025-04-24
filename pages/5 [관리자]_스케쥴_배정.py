import streamlit as st
import pandas as pd
import datetime
import calendar
from io import BytesIO
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import time
import io
import xlsxwriter
import random
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
from openpyxl.comments import Comment
from datetime import timedelta

random.seed(42)

# 🔒 관리자 페이지 체크
if "login_success" not in st.session_state or not st.session_state["login_success"]:
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# 사이드바
st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
if st.sidebar.button("로그아웃"):
    st.session_state.clear()
    st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
    time.sleep(5)
    st.rerun()

# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]
month_str = "2025년 04월"

# 새로고침 버튼 (맨 상단)
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    st.session_state["data_loaded"] = False  # 데이터 리로드 강제
    load_data()  # load_data 호출로 모든 데이터 갱신
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수 (세션 상태 활용으로 쿼터 절약)
def load_data():
    required_keys = ["df_master", "df_request", "df_cumulative"]
    if "data_loaded" not in st.session_state or not st.session_state["data_loaded"] or not all(key in st.session_state for key in required_keys):
        url = st.secrets["google_sheet"]["url"]
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)

        # 마스터 시트
        try:
            worksheet1 = sheet.worksheet("마스터")
            st.session_state["df_master"] = pd.DataFrame(worksheet1.get_all_records())
            st.session_state["worksheet1"] = worksheet1
        except Exception as e:
            st.error(f"마스터 시트를 불러오는 데 문제가 발생했습니다: {e}")
            st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            st.session_state["data_loaded"] = False
            st.stop()

        # 요청사항 시트
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except WorksheetNotFound:
            worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
            names_in_master = st.session_state["df_master"]["이름"].unique()
            new_rows = [[name, "요청 없음", ""] for name in names_in_master]
            for row in new_rows:
                worksheet2.append_row(row)
        st.session_state["df_request"] = pd.DataFrame(worksheet2.get_all_records()) if worksheet2.get_all_records() else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["worksheet2"] = worksheet2

        # 누적 시트
        try:
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
        except WorksheetNotFound:
            worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
            worksheet4.append_row([f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
            names_in_master = st.session_state["df_master"]["이름"].unique()
            new_rows = [[name, "", "", "", ""] for name in names_in_master]
            for row in new_rows:
                worksheet4.append_row(row)
        st.session_state["df_cumulative"] = pd.DataFrame(worksheet4.get_all_records()) if worksheet4.get_all_records() else pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
        st.session_state["worksheet4"] = worksheet4

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

# 메인 로직
if st.session_state.get("is_admin_authenticated", False):
    load_data()
    # Use .get() with fallback to avoid KeyError
    df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
    df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
    df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]))
    df_shift = generate_shift_table(df_master)
    df_supplement = generate_supplement_table(df_shift, df_master["이름"].unique())

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
    st.dataframe(df_shift)

    # 보충 테이블 (중복된 df_master 표시 제거, df_supplement 표시)
    st.markdown("**☑️ 보충 테이블**")
    st.dataframe(df_supplement)

    # 요청사항 테이블
    st.markdown("**🙋‍♂️ 요청사항 테이블**")
    st.dataframe(df_request)

    # 누적 테이블
    st.markdown("**➕ 누적 테이블**")
    st.dataframe(df_cumulative)

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
    week_numbers = {d: (d.day - 1) // 7 + 1 for d in dates}
    day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금'}

    # df_final 초기화
    df_final = pd.DataFrame(columns=['날짜', '요일', '주차', '시간대', '근무자', '상태', '메모', '색상'])

    # 데이터프레임 로드 확인 (Streamlit UI로 변경)
    st.divider()
    st.subheader(f"✨ {month_str} 스케쥴 배정 확인")
    # st.write("df_shift_processed 확인:", df_shift_processed.head())
    # st.write("df_supplement_processed 확인:", df_supplement_processed.head())
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
                    st.write(f"잘못된 날짜 형식 무시됨: {single_date}")
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
                st.write(f"잘못된 날짜 범위 무시됨: {date_str}, 에러: {e}")
                return []
        try:
            parsed_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            if parsed_date.weekday() < 5:
                return [date_str]
            return []
        except ValueError:
            st.write(f"잘못된 날짜 형식 무시됨: {date_str}")
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
                '주차': [week_numbers[pd.to_datetime(date_str)]],
                '시간대': [time_slot],
                '근무자': [worker.strip()],
                '상태': [status],
                '메모': [memo],
                '색상': [color]
            })
            df = pd.concat([df, new_row], ignore_index=True)
        return df

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
        label=f"{month_str} 평일 중 휴관일을 선택하세요",
        options=[option[0] for option in holiday_options],
        default=[],
        help="선택한 날짜는 근무 배정에서 제외됩니다."
    )

    # 선택된 휴관일을 날짜 형식(YYYY-MM-DD)으로 변환
    holiday_dates = []
    for holiday in selected_holidays:
        for option in holiday_options:
            if option[0] == holiday:
                holiday_dates.append(option[1])
                break

    # 근무 배정 버튼
    st.write(" ")
    if st.button("🚀 근무 배정 실행"):
        # 버튼 클릭 시 세션 상태 초기화
        st.session_state.assigned = False
        st.session_state.output = None
        st.session_state.downloaded = False

        with st.spinner("근무 배정 중..."):
            time.sleep(1)

            # 날짜별 오전 근무 제외 인원 추적용 딕셔너리 (모든 날짜에 대해 초기화)
            excluded_morning_workers = {date.strftime('%Y-%m-%d'): set() for date in weekdays}

            # 휴관일을 제외한 평일 리스트 생성
            active_weekdays = [date for date in weekdays if date.strftime('%Y-%m-%d') not in holiday_dates]

            # 1단계: 모든 날짜에 대해 오전 기본 배정 및 휴가자 처리 (휴관일 제외)
            for date in active_weekdays:
                day_name = day_map[date.weekday()]
                week_num = week_numbers[date]
                date_str = date.strftime('%Y-%m-%d')

                # 휴가자 및 요청 사전 처리
                vacationers = []
                must_work_morning = []
                must_work_afternoon = []
                no_supplement_morning = []
                no_supplement_afternoon = []
                hard_supplement_morning = []
                hard_supplement_afternoon = []

                for _, row in df_request.iterrows():
                    date_info = row['날짜정보']
                    name = row['이름']
                    category = row['분류']
                    if pd.isna(date_info) or not date_info:
                        continue
                    applicable_dates = parse_date_range(date_info)
                    if date_str in applicable_dates:
                        if category == '휴가':
                            vacationers.append(name)
                        elif category == '꼭 근무(오전)':
                            must_work_morning.append(name)
                        elif category == '꼭 근무(오후)':
                            must_work_afternoon.append(name)
                        elif category == '보충 불가(오전)':
                            no_supplement_morning.append(name)
                        elif category == '보충 불가(오후)':
                            no_supplement_afternoon.append(name)
                        elif category == '보충 어려움(오전)':
                            hard_supplement_morning.append(name)
                        elif category == '보충 어려움(오후)':
                            hard_supplement_afternoon.append(name)

                # 휴가자 사전 처리 (오전만)
                time_slot = '오전'
                shift_key = f'{day_name} {time_slot}'
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                master_workers = set()
                if not shift_row.empty:
                    for col in [f'근무{i}' for i in range(1, 15)]:
                        worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                        if worker:
                            if '(' in worker:
                                name, weeks = worker.split('(')
                                name = name.strip()
                                weeks = weeks.rstrip(')').split(',')
                                if f'{week_num}주' in weeks:
                                    master_workers.add(name)
                            else:
                                master_workers.add(worker)

                for vac in vacationers:
                    if vac in master_workers:
                        df_final = update_worker_status(df_final, date_str, time_slot, vac, '제외', '휴가로 제외됨', '🔴 빨간색')
                        excluded_morning_workers[date_str].add(vac)

                # 오전 기본 배정
                target_count = 12
                must_work = must_work_morning
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                workers = []
                initial_workers = set()
                if not shift_row.empty:
                    for col in [f'근무{i}' for i in range(1, 15)]:
                        worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                        if worker:
                            if '(' in worker:
                                name, weeks = worker.split('(')
                                name = name.strip()
                                weeks = weeks.rstrip(')').split(',')
                                if f'{week_num}주' in weeks:
                                    workers.append(name)
                                    initial_workers.add(name)
                            else:
                                workers.append(worker)
                                initial_workers.add(worker)

                workers = [w for w in workers if w not in vacationers]
                initial_workers = initial_workers - set(vacationers)

                for mw in must_work:
                    if mw not in workers and mw not in vacationers:
                        workers.append(mw)
                        initial_workers.add(mw)

                for worker in workers:
                    status = '근무'
                    memo = ''
                    color = '기본'
                    if worker in must_work:
                        memo = f'꼭 근무({time_slot}) 위해 배정됨'
                        color = '🟠 주황색'
                    current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                    df_final = update_worker_status(df_final, date_str, time_slot, worker, status, memo, color)

            # 2단계: 모든 날짜에 대해 오전 보충/제외 수행 (휴관일 제외)
            time_slot = '오전'
            target_count = 12
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                day_name = day_map[date.weekday()]
                week_num = week_numbers[date]

                # 요청사항 재확인
                vacationers = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                must_work = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                no_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                hard_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 어려움({time_slot})']

                # 기본 보충/제외 전 근무자 출력
                current_workers = df_final[
                    (df_final['날짜'] == date_str) &
                    (df_final['시간대'] == time_slot) &
                    (df_final['상태'].isin(['근무', '보충']))
                ]['근무자'].tolist()

                # 기본 보충/제외
                moved_workers = set()
                supplemented_workers = {}
                excluded_workers = {}
                for d in active_weekdays:  # 휴관일 제외
                    d_str = d.strftime('%Y-%m-%d')
                    supplemented_workers[d_str] = []
                    excluded_workers[d_str] = []

                iteration = 0
                while True:
                    iteration += 1
                    excess_dates = []
                    shortage_dates = []
                    for d in active_weekdays:
                        d_str = d.strftime('%Y-%m-%d')
                        workers = df_final[
                            (df_final['날짜'] == d_str) &
                            (df_final['시간대'] == time_slot) &
                            (df_final['상태'].isin(['근무', '보충']))
                        ]['근무자'].tolist()
                        count = len(workers)
                        if count > target_count:
                            excess_dates.append((d_str, count - target_count))
                        elif count < target_count:
                            shortage_dates.append((d_str, target_count - count))

                    processed_excess = set()
                    any_matched = False
                    while excess_dates and shortage_dates:
                        excess_date, excess_count = excess_dates[0]
                        if excess_date in processed_excess:
                            excess_dates.pop(0)
                            continue
                        matchedCondividi

                        for i, (shortage_date, shortage_count) in enumerate(shortage_dates[:]):
                            if excess_count == 0 or shortage_count == 0:
                                continue
                            excess_workers = df_final[
                                (df_final['날짜'] == excess_date) &
                                (df_final['시간대'] == time_slot) &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_excess = [row['이름'] for _, row in df_request.iterrows() if excess_date in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                            shortage_initial = set(df_shift_processed[df_shift_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                .iloc[0, 1:].dropna().apply(lambda x: x.split('(')[0].strip()).tolist())
                            shortage_supplement = set(df_supplement_processed[df_supplement_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                    .iloc[0, 1:].dropna().apply(lambda x: x.strip('🔺')).tolist())
                            shortage_vacationers = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                            shortage_no_supplement = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                            shortage_supplement = shortage_supplement - set(shortage_vacationers) - set(shortage_no_supplement)

                            movable_workers = [
                                w for w in excess_workers
                                if w not in must_work_excess and w not in moved_workers
                            ]
                            movable_workers = [w for w in movable_workers if w in shortage_supplement and w not in shortage_initial]

                            moved = False
                            for _ in range(min(excess_count, shortage_count)):
                                if not movable_workers:
                                    break
                                worker = random.choice(movable_workers)
                                movable_workers.remove(worker)
                                moved_workers.add(worker)
                                excluded_workers[excess_date].append(worker)
                                supplemented_workers[shortage_date].append(worker)
                                df_final = update_worker_status(df_final, excess_date, time_slot, worker, '제외', f'{shortage_date} 보충 위해 제외됨', '🔵 파란색')
                                excluded_morning_workers[excess_date].add(worker)
                                df_final = update_worker_status(df_final, shortage_date, time_slot, worker, '보충', f'{excess_date}에서 제외되어 보충됨', '🟢 초록색')
                                current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                                excess_count -= 1
                                shortage_count -= 1
                                moved = True
                                if excess_count == 0 or shortage_count == 0:
                                    break

                            if moved:
                                matched = True
                                any_matched = True
                                if excess_count == 0:
                                    excess_dates.pop(0)
                                else:
                                    excess_dates[0] = (excess_date, excess_count)
                                if shortage_count == 0:
                                    shortage_dates.pop(i)
                                else:
                                    shortage_dates[i] = (shortage_date, shortage_count)
                                break
                        if not matched:
                            processed_excess.add(excess_date)
                            excess_dates.pop(0)

                    if not any_matched:
                        break

                    excess_dates = []
                    shortage_dates = []
                    for d in reversed(active_weekdays):
                        d_str = d.strftime('%Y-%m-%d')
                        workers = df_final[
                            (df_final['날짜'] == d_str) &
                            (df_final['시간대'] == time_slot) &
                            (df_final['상태'].isin(['근무', '보충']))
                        ]['근무자'].tolist()
                        count = len(workers)
                        if count > target_count:
                            excess_dates.append((d_str, count - target_count))
                        elif count < target_count:
                            shortage_dates.append((d_str, target_count - count))

                    processed_shortage = set()
                    any_matched = False
                    while excess_dates and shortage_dates:
                        shortage_date, shortage_count = shortage_dates[0]
                        if shortage_date in processed_shortage:
                            shortage_dates.pop(0)
                            continue
                        matched = False
                        for i, (excess_date, excess_count) in enumerate(excess_dates[:]):
                            if excess_count == 0 or shortage_count == 0:
                                continue
                            excess_workers = df_final[
                                (df_final['날짜'] == excess_date) &
                                (df_final['시간대'] == time_slot) &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_excess = [row['이름'] for _, row in df_request.iterrows() if excess_date in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                            shortage_initial = set(df_shift_processed[df_shift_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                .iloc[0, 1:].dropna().apply(lambda x: x.split('(')[0].strip()).tolist())
                            shortage_supplement = set(df_supplement_processed[df_supplement_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                    .iloc[0, 1:].dropna().apply(lambda x: x.strip('🔺')).tolist())
                            shortage_vacationers = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                            shortage_no_supplement = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                            shortage_supplement = shortage_supplement - set(shortage_vacationers) - set(shortage_no_supplement)

                            movable_workers = [
                                w for w in excess_workers
                                if w not in must_work_excess and w not in moved_workers
                            ]
                            movable_workers = [w for w in movable_workers if w in shortage_supplement and w not in shortage_initial]

                            moved = False
                            for _ in range(min(excess_count, shortage_count)):
                                if not movable_workers:
                                    break
                                worker = random.choice(movable_workers)
                                movable_workers.remove(worker)
                                moved_workers.add(worker)
                                excluded_workers[excess_date].append(worker)
                                supplemented_workers[shortage_date].append(worker)
                                df_final = update_worker_status(df_final, excess_date, time_slot, worker, '제외', f'{shortage_date} 보충 위해 제외됨', '🔵 파란색')
                                excluded_morning_workers[excess_date].add(worker)
                                df_final = update_worker_status(df_final, shortage_date, time_slot, worker, '보충', f'{excess_date}에서 제외되어 보충됨', '🟢 초록색')
                                current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                                excess_count -= 1
                                shortage_count -= 1
                                moved = True
                                if excess_count == 0 or shortage_count == 0:
                                    break
                            if moved:
                                matched = True
                                any_matched = True
                                if shortage_count == 0:
                                    shortage_dates.pop(0)
                                else:
                                    shortage_dates[0] = (shortage_date, shortage_count)
                                if excess_count == 0:
                                    excess_dates.pop(i)
                                else:
                                    excess_dates[i] = (excess_date, excess_count)
                                break
                        if not matched:
                            processed_shortage.add(shortage_date)
                            shortage_dates.pop(0)

                    if not any_matched:
                        break

            # 3단계: 모든 날짜에 대해 오후 기본 배정 (휴관일 제외)
            for date in active_weekdays:
                day_name = day_map[date.weekday()]
                week_num = week_numbers[date]
                date_str = date.strftime('%Y-%m-%d')

                # 요청사항 재확인 (오후 관련)
                vacationers = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                must_work_afternoon = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == '꼭 근무(오후)']

                # 휴가자 사전 처리 (오후만)
                time_slot = '오후'
                shift_key = f'{day_name} {time_slot}'
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                master_workers = set()
                if not shift_row.empty:
                    for col in [f'근무{i}' for i in range(1, 15)]:
                        worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                        if worker:
                            if '(' in worker:
                                name, weeks = worker.split('(')
                                name = name.strip()
                                weeks = weeks.rstrip(')').split(',')
                                if f'{week_num}주' in weeks:
                                    master_workers.add(name)
                            else:
                                master_workers.add(worker)

                for vac in vacationers:
                    if vac in master_workers:
                        df_final = update_worker_status(df_final, date_str, time_slot, vac, '제외', '휴가로 제외됨', '🔴 빨간색')

                # 오후 기본 배정
                target_count = 5
                must_work = must_work_afternoon
                shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                workers = []
                initial_workers = set()
                if not shift_row.empty:
                    for col in [f'근무{i}' for i in range(1, 15)]:
                        worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                        if worker:
                            if '(' in worker:
                                name, weeks = worker.split('(')
                                name = name.strip()
                                weeks = weeks.rstrip(')').split(',')
                                if f'{week_num}주' in weeks:
                                    workers.append(name)
                                    initial_workers.add(name)
                            else:
                                workers.append(worker)
                                initial_workers.add(worker)

                workers = [w for w in workers if w not in vacationers]
                initial_workers = initial_workers - set(vacationers)

                for mw in must_work:
                    if mw not in workers and mw not in vacationers:
                        workers.append(mw)
                        initial_workers.add(mw)

                # 오후 근무자: 오전 근무자 중에서 선택 (보충/제외 반영된 상태)
                morning_workers = df_final[
                    (df_final['날짜'] == date_str) &
                    (df_final['시간대'] == '오전') &
                    (df_final['상태'].isin(['근무', '보충']))
                ]['근무자'].tolist()
                workers = [w for w in workers if (w in morning_workers or w in must_work) and w not in excluded_morning_workers[date_str]]

                for worker in workers:
                    status = '근무'
                    memo = ''
                    color = '기본'
                    if worker in must_work:
                        memo = f'꼭 근무({time_slot}) 위해 배정됨'
                        color = '🟠 주황색'
                    current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                    df_final = update_worker_status(df_final, date_str, time_slot, worker, status, memo, color)

            # 4단계: 모든 날짜에 대해 오후 보충/제외 수행 (휴관일 제외)
            time_slot = '오후'
            target_count = 5
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                day_name = day_map[date.weekday()]
                week_num = week_numbers[date]

                # 요청사항 재확인
                vacationers = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                must_work = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                no_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                hard_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 어려움({time_slot})']

                # 기본 보충/제외 전 근무자 출력
                current_workers = df_final[
                    (df_final['날짜'] == date_str) &
                    (df_final['시간대'] == time_slot) &
                    (df_final['상태'].isin(['근무', '보충']))
                ]['근무자'].tolist()

                # 기본 보충/제외
                moved_workers = set()
                supplemented_workers = {}
                excluded_workers = {}
                for d in active_weekdays:
                    d_str = d.strftime('%Y-%m-%d')
                    supplemented_workers[d_str] = []
                    excluded_workers[d_str] = []

                iteration = 0
                while True:
                    iteration += 1
                    excess_dates = []
                    shortage_dates = []
                    for d in active_weekdays:
                        d_str = d.strftime('%Y-%m-%d')
                        workers = df_final[
                            (df_final['날짜'] == d_str) &
                            (df_final['시간대'] == time_slot) &
                            (df_final['상태'].isin(['근무', '보충']))
                        ]['근무자'].tolist()
                        count = len(workers)
                        if count > target_count:
                            excess_dates.append((d_str, count - target_count))
                        elif count < target_count:
                            shortage_dates.append((d_str, target_count - count))

                    processed_excess = set()
                    any_matched = False
                    while excess_dates and shortage_dates:
                        excess_date, excess_count = excess_dates[0]
                        if excess_date in processed_excess:
                            excess_dates.pop(0)
                            continue
                        matched = False
                        for i, (shortage_date, shortage_count) in enumerate(shortage_dates[:]):
                            if excess_count == 0 or shortage_count == 0:
                                continue
                            excess_workers = df_final[
                                (df_final['날짜'] == excess_date) &
                                (df_final['시간대'] == time_slot) &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_excess = [row['이름'] for _, row in df_request.iterrows() if excess_date in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                            shortage_initial = set(df_shift_processed[df_shift_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                .iloc[0, 1:].dropna().apply(lambda x: x.split('(')[0].strip()).tolist())
                            shortage_supplement = set(df_supplement_processed[df_supplement_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                    .iloc[0, 1:].dropna().apply(lambda x: x.strip('🔺')).tolist())
                            shortage_vacationers = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                            shortage_no_supplement = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                            shortage_supplement = shortage_supplement - set(shortage_vacationers) - set(shortage_no_supplement)

                            movable_workers = [
                                w for w in excess_workers
                                if w not in must_work_excess and w not in moved_workers
                            ]
                            morning_workers_shortage = df_final[
                                (df_final['날짜'] == shortage_date) &
                                (df_final['시간대'] == '오전') &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_shortage = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '꼭 근무(오후)']
                            movable_workers = [w for w in movable_workers if (w in morning_workers_shortage or w in must_work_shortage) and w not in excluded_morning_workers[shortage_date]]
                            movable_workers = [w for w in movable_workers if w in shortage_supplement and w not in shortage_initial]

                            moved = False
                            for _ in range(min(excess_count, shortage_count)):
                                if not movable_workers:
                                    break
                                worker = random.choice(movable_workers)
                                movable_workers.remove(worker)
                                moved_workers.add(worker)
                                excluded_workers[excess_date].append(worker)
                                supplemented_workers[shortage_date].append(worker)
                                df_final = update_worker_status(df_final, excess_date, time_slot, worker, '제외', f'{shortage_date} 보충 위해 제외됨', '🔵 파란색')
                                df_final = update_worker_status(df_final, shortage_date, time_slot, worker, '보충', f'{excess_date}에서 제외되어 보충됨', '🟢 초록색')
                                current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                                excess_count -= 1
                                shortage_count -= 1
                                moved = True
                                if excess_count == 0 or shortage_count == 0:
                                    break

                            if moved:
                                matched = True
                                any_matched = True
                                if excess_count == 0:
                                    excess_dates.pop(0)
                                else:
                                    excess_dates[0] = (excess_date, excess_count)
                                if shortage_count == 0:
                                    shortage_dates.pop(i)
                                else:
                                    shortage_dates[i] = (shortage_date, shortage_count)
                                break
                        if not matched:
                            processed_excess.add(excess_date)
                            excess_dates.pop(0)

                    if not any_matched:
                        break

                    excess_dates = []
                    shortage_dates = []
                    for d in reversed(active_weekdays):
                        d_str = d.strftime('%Y-%m-%d')
                        workers = df_final[
                            (df_final['날짜'] == d_str) &
                            (df_final['시간대'] == time_slot) &
                            (df_final['상태'].isin(['근무', '보충']))
                        ]['근무자'].tolist()
                        count = len(workers)
                        if count > target_count:
                            excess_dates.append((d_str, count - target_count))
                        elif count < target_count:
                            shortage_dates.append((d_str, target_count - count))

                    processed_shortage = set()
                    any_matched = False
                    while excess_dates and shortage_dates:
                        shortage_date, shortage_count = shortage_dates[0]
                        if shortage_date in processed_shortage:
                            shortage_dates.pop(0)
                            continue
                        matched = False
                        for i, (excess_date, excess_count) in enumerate(excess_dates[:]):
                            if excess_count == 0 or shortage_count == 0:
                                continue
                            excess_workers = df_final[
                                (df_final['날짜'] == excess_date) &
                                (df_final['시간대'] == time_slot) &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_excess = [row['이름'] for _, row in df_request.iterrows() if excess_date in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                            shortage_initial = set(df_shift_processed[df_shift_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                .iloc[0, 1:].dropna().apply(lambda x: x.split('(')[0].strip()).tolist())
                            shortage_supplement = set(df_supplement_processed[df_supplement_processed['시간대'] == f'{day_map[pd.to_datetime(shortage_date).weekday()]} {time_slot}']
                                                    .iloc[0, 1:].dropna().apply(lambda x: x.strip('🔺')).tolist())
                            shortage_vacationers = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                            shortage_no_supplement = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                            shortage_supplement = shortage_supplement - set(shortage_vacationers) - set(shortage_no_supplement)

                            movable_workers = [
                                w for w in excess_workers
                                if w not in must_work_excess and w not in moved_workers
                            ]
                            morning_workers_shortage = df_final[
                                (df_final['날짜'] == shortage_date) &
                                (df_final['시간대'] == '오전') &
                                (df_final['상태'].isin(['근무', '보충']))
                            ]['근무자'].tolist()
                            must_work_shortage = [row['이름'] for _, row in df_request.iterrows() if shortage_date in parse_date_range(row['날짜정보']) and row['분류'] == '꼭 근무(오후)']
                            movable_workers = [w for w in movable_workers if (w in morning_workers_shortage or w in must_work_shortage) and w not in excluded_morning_workers[shortage_date]]
                            movable_workers = [w for w in movable_workers if w in shortage_supplement and w not in shortage_initial]

                            moved = False
                            for _ in range(min(excess_count, shortage_count)):
                                if not movable_workers:
                                    break
                                worker = random.choice(movable_workers)
                                movable_workers.remove(worker)
                                moved_workers.add(worker)
                                excluded_workers[excess_date].append(worker)
                                supplemented_workers[shortage_date].append(worker)
                                df_final = update_worker_status(df_final, excess_date, time_slot, worker, '제외', f'{shortage_date} 보충 위해 제외됨', '🔵 파란색')
                                df_final = update_worker_status(df_final, shortage_date, time_slot, worker, '보충', f'{excess_date}에서 제외되어 보충됨', '🟢 초록색')
                                current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                                excess_count -= 1
                                shortage_count -= 1
                                moved = True
                                if excess_count == 0 or shortage_count == 0:
                                    break
                            if moved:
                                matched = True
                                any_matched = True
                                if shortage_count == 0:
                                    shortage_dates.pop(0)
                                else:
                                    shortage_dates[0] = (shortage_date, shortage_count)
                                if excess_count == 0:
                                    excess_dates.pop(i)
                                else:
                                    excess_dates[i] = (excess_date, excess_count)
                                break
                        if not matched:
                            processed_shortage.add(shortage_date)
                            shortage_dates.pop(0)

                    if not any_matched:
                        break

            # 5단계: 모든 날짜에 대해 추가 보충/제외 수행 (휴관일 제외)
            for date in active_weekdays:
                date_str = date.strftime('%Y-%m-%d')
                day_name = day_map[date.weekday()]
                week_num = week_numbers[date]
                supplemented_morning_workers = df_final[
                    (df_final['날짜'] == date_str) &
                    (df_final['시간대'] == '오전') &
                    (df_final['상태'].isin(['근무', '보충']))
                ]['근무자'].tolist()

                for time_slot in ['오전', '오후']:
                    target_count = 12 if time_slot == '오전' else 5

                    # 기본 보충/제외 전 근무자 출력
                    current_workers = df_final[
                        (df_final['날짜'] == date_str) &
                        (df_final['시간대'] == time_slot) &
                        (df_final['상태'].isin(['근무', '보충']))
                    ]['근무자'].tolist()

                    # 요청사항 재확인
                    vacationers = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == '휴가']
                    must_work = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'꼭 근무({time_slot})']
                    no_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 불가({time_slot})']
                    hard_supplement = [row['이름'] for _, row in df_request.iterrows() if date_str in parse_date_range(row['날짜정보']) and row['분류'] == f'보충 어려움({time_slot})']

                    # df_shift_processed 초기 근무자
                    shift_key = f'{day_name} {time_slot}'
                    shift_row = df_shift_processed[df_shift_processed['시간대'] == shift_key]
                    initial_workers = set()
                    if not shift_row.empty:
                        for col in [f'근무{i}' for i in range(1, 15)]:
                            worker = shift_row[col].values[0] if col in shift_row.columns and pd.notna(shift_row[col].values[0]) else ''
                            if worker:
                                if '(' in worker:
                                    name, weeks = worker.split('(')
                                    name = name.strip()
                                    weeks = weeks.rstrip(')').split(',')
                                    if f'{week_num}주' in weeks:
                                        initial_workers.add(name)
                                else:
                                    initial_workers.add(worker)

                    # df_supplement_processed 보충 근무자
                    supplement_row = df_supplement_processed[df_supplement_processed['시간대'] == shift_key]
                    supplement_workers = []
                    if not supplement_row.empty:
                        for col in [f'보충{i}' for i in range(1, 13)]:
                            worker = supplement_row[col].values[0] if col in supplement_row.columns and pd.notna(supplement_row[col].values[0]) else ''
                            if worker:
                                name = worker.replace('🔺', '')
                                priority = 'low' if '🔺' in worker else 'normal'
                                if name not in vacationers and name not in no_supplement:
                                    supplement_workers.append((name, priority))
                    if time_slot == '오후':
                        for worker in supplemented_morning_workers:
                            if worker not in [w for w, _ in supplement_workers] and worker not in vacationers and worker not in no_supplement:
                                supplement_workers.append((worker, 'normal'))
                    supplement_workers = [(w, p) for w, p in supplement_workers if w not in vacationers and w not in no_supplement]

                    # 오후 보충 제약
                    morning_workers = df_final[
                        (df_final['날짜'] == date_str) &
                        (df_final['시간대'] == '오전') &
                        (df_final['상태'].isin(['근무', '보충']))
                    ]['근무자'].tolist() if time_slot == '오후' else None

                    # 추가 보충
                    added_supplement_workers = []
                    added_exclude_workers = []
                    if len(current_workers) < target_count:
                        supplement_workers_with_cumulative = [
                            (w, df_cumulative_next[df_cumulative_next[f'{month_str}'] == w][f'{time_slot}누적'].iloc[0] if w in df_cumulative_next[f'{month_str}'].values else 0, p)
                            for w, p in supplement_workers if w not in current_workers
                        ]
                        supplement_workers_with_cumulative.sort(key=lambda x: (x[1], x[2] == 'low'))
                        while len(current_workers) < target_count and supplement_workers_with_cumulative:
                            worker, _, _ = supplement_workers_with_cumulative.pop(0)
                            if time_slot == '오후' and worker not in must_work:
                                if worker not in morning_workers or worker in excluded_morning_workers[date_str]:
                                    continue
                            current_workers.append(worker)
                            added_supplement_workers.append(worker)
                            current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) + 1
                            if worker in df_cumulative_next[f'{month_str}'].values:
                                df_cumulative_next.loc[df_cumulative_next[f'{month_str}'] == worker, f'{time_slot}누적'] += 1
                            else:
                                new_row = pd.DataFrame({
                                    f'{month_str}': [worker],
                                    f'{time_slot}누적': [1],
                                    '오전당직 (온콜)': [0],
                                    '오후당직': [0]
                                })
                                if time_slot == '오전':
                                    new_row['오후누적'] = [0]
                                else:
                                    new_row['오전누적'] = [0]
                                df_cumulative_next = pd.concat([df_cumulative_next, new_row], ignore_index=True)
                            df_final = update_worker_status(df_final, date_str, time_slot, worker, '보충', '인원 부족으로 인한 추가 보충', '🟡 노란색')

                    # 추가 제외
                    if len(current_workers) > target_count:
                        removable_workers = [
                            (w, df_cumulative_next[df_cumulative_next[f'{month_str}'] == w][f'{time_slot}누적'].iloc[0] if w in df_cumulative_next[f'{month_str}'].values else 0)
                            for w in current_workers if w not in must_work and w not in initial_workers
                        ]
                        if not removable_workers:
                            removable_workers = [
                                (w, df_cumulative_next[df_cumulative_next[f'{month_str}'] == w][f'{time_slot}누적'].iloc[0] if w in df_cumulative_next[f'{month_str}'].values else 0)
                                for w in current_workers if w not in must_work
                            ]
                        removable_workers.sort(key=lambda x: x[1], reverse=True)
                        while len(current_workers) > target_count and removable_workers:
                            worker, _ = removable_workers.pop(0)
                            current_workers.remove(worker)
                            added_exclude_workers.append(worker)
                            current_cumulative[time_slot][worker] = current_cumulative[time_slot].get(worker, 0) - 1
                            if worker in df_cumulative_next[f'{month_str}'].values:
                                df_cumulative_next.loc[df_cumulative_next[f'{month_str}'] == worker, f'{time_slot}누적'] -= 1
                            df_final = update_worker_status(df_final, date_str, time_slot, worker, '제외', '인원 초과로 인한 추가 제외', '🟣 보라색')
                            if time_slot == '오전':
                                if df_final[
                                    (df_final['날짜'] == date_str) &
                                    (df_final['시간대'] == '오후') &
                                    (df_final['근무자'] == worker)
                                ].empty:
                                    df_final = update_worker_status(df_final, date_str, '오후', worker, '제외', '오전 제외로 인한 오후 제외', '🟣 보라색')
                                    current_cumulative['오후'][worker] = current_cumulative['오후'].get(worker, 0) - 1
                                    if worker in df_cumulative_next[f'{month_str}'].values:
                                        df_cumulative_next.loc[df_cumulative_next[f'{month_str}'] == worker, '오후누적'] -= 1

                    # 최종 검증
                    final_count = len(df_final[
                        (df_final['날짜'] == date_str) &
                        (df_final['시간대'] == time_slot) &
                        (df_final['상태'].isin(['근무', '보충']))
                    ]['근무자'].tolist())

            # 2025년 4월 전체 평일 및 주말 생성
            _, last_day = calendar.monthrange(next_month.year, next_month.month)
            dates = pd.date_range(start=next_month, end=next_month.replace(day=last_day))
            week_numbers = {d: (d.day - 1) // 7 + 1 for d in dates}
            day_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}

            # df_schedule 생성 (평일 및 주말 포함)
            df_schedule = pd.DataFrame({
                '날짜': [d.strftime('%Y-%m-%d') for d in dates],
                '요일': [day_map[d.weekday()] for d in dates]
            })

            # 최대 근무자 수 계산 (모든 상태 포함)
            worker_counts_all = df_final.groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
            max_morning_workers_all = int(worker_counts_all.get('오전', pd.Series(0)).max()) if '오전' in worker_counts_all else 0
            max_afternoon_workers_all = int(worker_counts_all.get('오후', pd.Series(0)).max()) if '오후' in worker_counts_all else 0

            # 최대 근무자 수 계산 ('제외'가 아닌 근무자만) - 디버깅용
            worker_counts_active = df_final[df_final['상태'] != '제외'].groupby(['날짜', '시간대'])['근무자'].nunique().unstack(fill_value=0)
            max_morning_workers_active = int(worker_counts_active.get('오전', pd.Series(0)).max()) if '오전' in worker_counts_active else 0
            max_afternoon_workers_active = int(worker_counts_active.get('오후', pd.Series(0)).max()) if '오후' in worker_counts_active else 0

            # 최대 근무자 수 설정 (제한 제거)
            max_morning_workers = max_morning_workers_all
            max_afternoon_workers = max_afternoon_workers_all

            # 색상 우선순위 정의
            color_priority = {
                '🟠 주황색': 0,
                '🟢 초록색': 1,
                '🟡 노란색': 2,
                '기본': 3,
                '🔴 빨간색': 4,
                '🔵 파란색': 5,
                '🟣 보라색': 6,
            }

            # df_final에 색상 우선순위 열 추가
            df_final['색상_우선순위'] = df_final['색상'].map(color_priority)

            # df_final 중복 제거 (색상 우선순위가 높은 상태 선택)
            df_final_sorted = df_final.sort_values(by=['날짜', '시간대', '근무자', '색상_우선순위'])
            df_final_unique = df_final_sorted.groupby(['날짜', '시간대', '근무자']).first().reset_index()

            # 디버깅: 초록색 셀 존재 여부 확인
            green_cells = df_final_unique[df_final_unique['색상'] == '🟢 초록색']
            # st.write(f"df_final_unique에 초록색 셀 수: {len(green_cells)}")
            # if not green_cells.empty:
                # st.write("초록색 셀 샘플:")
                # st.write(green_cells[['날짜', '시간대', '근무자', '상태', '색상']].head())

            # df_excel 열 동적 생성
            columns = ['날짜', '요일'] + [str(i) for i in range(1, max_morning_workers + 1)] + [''] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, max_afternoon_workers + 1)]
            df_excel = pd.DataFrame(index=df_schedule.index, columns=columns)

            # 데이터 채우기
            for idx, row in df_schedule.iterrows():
                date = row['날짜']
                date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                df_excel.at[idx, '날짜'] = f"{date_obj.month}월 {date_obj.day}일"
                df_excel.at[idx, '요일'] = row['요일']

                # 오전 근무자 (모든 상태 포함)
                morning = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오전')]
                morning_workers = []
                for _, mrow in morning.iterrows():
                    morning_workers.append((mrow['근무자'], mrow['상태'], mrow['메모'], mrow['색상']))
                morning_workers.sort(key=lambda x: (color_priority[x[3]], x[0]))
                for i, worker_data in enumerate(morning_workers, 1):
                    if i <= max_morning_workers:
                        df_excel.at[idx, str(i)] = worker_data[0]

                # 오후 근무자 (모든 상태 포함)
                afternoon = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후')]
                afternoon_workers = []
                for _, arow in afternoon.iterrows():
                    afternoon_workers.append((arow['근무자'], arow['상태'], arow['메모'], arow['색상']))
                afternoon_workers.sort(key=lambda x: (color_priority[x[3]], x[0]))
                for i, worker_data in enumerate(afternoon_workers, 1):
                    if i <= max_afternoon_workers:
                        df_excel.at[idx, f'오후{i}'] = worker_data[0]

            # 오전당직(온콜) 배정
            oncall_counts = df_cumulative.set_index(f'{month_str}')['오전당직 (온콜)'].to_dict()
            oncall_assignments = {worker: int(count) if count else 0 for worker, count in oncall_counts.items()}
            oncall = {}  # 날짜별 오전당직(온콜) 배정 저장

            # 오후 근무 횟수를 계산하여 우선순위 결정
            afternoon_counts = df_final_unique[
                (df_final_unique['시간대'] == '오후') &
                (df_final_unique['상태'].isin(['근무', '보충']))
            ]['근무자'].value_counts().to_dict()

            # 근무자 리스트를 온콜 횟수와 오후 근무 횟수 기준으로 정렬
            workers_priority = sorted(
                oncall_assignments.items(),
                key=lambda x: (-x[1], afternoon_counts.get(x[0], 0))  # 온콜 횟수 내림차순, 오후 근무 횟수 오름차순
            )

            # df_final_unique에 존재하는 날짜만으로 remaining_dates 생성
            all_dates = df_final_unique['날짜'].unique().tolist()  # df_final_unique에 존재하는 날짜만 사용
            remaining_dates = set(all_dates)  # 아직 온콜이 배정되지 않은 날짜

            # 각 근무자별 온콜 배정
            for worker, count in workers_priority:
                if count <= 0:
                    continue

                # 해당 근무자가 오후 근무자로 있는 날짜 찾기
                eligible_dates = df_final_unique[
                    (df_final_unique['시간대'] == '오후') &
                    (df_final_unique['근무자'] == worker) &
                    (df_final_unique['상태'].isin(['근무', '보충']))
                ]['날짜'].unique()

                # 남은 날짜와 겹치는 날짜만 선택
                eligible_dates = [d for d in eligible_dates if d in remaining_dates]
                if not eligible_dates:
                    continue

                # count만큼 날짜 선택
                selected_dates = random.sample(eligible_dates, min(count, len(eligible_dates)))
                for selected_date in selected_dates:
                    oncall[selected_date] = worker
                    remaining_dates.remove(selected_date)  # 배정된 날짜 제거

            # 남은 날짜에 대해 오후 근무자 중 랜덤 배정
            random_assignments = []
            if remaining_dates:
                for date in remaining_dates:
                    # 해당 날짜의 오후 근무자 찾기
                    afternoon_workers_df = df_final_unique[
                        (df_final_unique['날짜'] == date) &
                        (df_final_unique['시간대'] == '오후') &
                        (df_final_unique['상태'].isin(['근무', '보충']))
                    ]
                    afternoon_workers = afternoon_workers_df['근무자'].tolist()

                    if afternoon_workers:
                        # 오후 근무자 중 랜덤으로 한 명 선택
                        selected_worker = random.choice(afternoon_workers)
                        oncall[date] = selected_worker
                        random_assignments.append((date, selected_worker))
                    else:
                        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%m월 %d일').lstrip('0')
                        st.warning(f"⚠️ {formatted_date}에는 오후 근무자가 없어 오전당직(온콜)을 배정할 수 없습니다.")
                        # 디버깅: 해당 날짜의 데이터 출력
                        st.write(f"{formatted_date}에 대한 df_final_unique 데이터:")
                        st.dataframe(afternoon_workers_df)

            # df_excel에 오전당직(온콜) 배정 반영
            for idx, row in df_schedule.iterrows():
                date = row['날짜']
                date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%Y-%m-%d')
                df_excel.at[idx, '오전당직(온콜)'] = oncall.get(formatted_date, '')

            # 추가: 실제 배치된 온콜 횟수 확인 및 초과 배치 메시지 출력
            actual_oncall_counts = {}
            for date, worker in oncall.items():
                actual_oncall_counts[worker] = actual_oncall_counts.get(worker, 0) + 1

            # df_cumulative의 최대 횟수와 비교
            for worker, actual_count in actual_oncall_counts.items():
                max_count = oncall_assignments.get(worker, 0)
                if actual_count > max_count:
                    st.info(f"오전당직(온콜) 횟수 제한 한계로, {worker} 님이 최대 배치 {max_count}회가 아닌 {actual_count}회 배치되었습니다.")
            
            # Excel 파일 생성
            wb = Workbook()
            ws = wb.active
            ws.title = "스케줄"

            # 열 헤더 추가
            for col_idx, col_name in enumerate(df_excel.columns, 1):
                cell = ws.cell(row=1, column=col_idx)
                cell.value = col_name
                cell.fill = PatternFill(start_color='000000', end_color='000000', fill_type='solid')
                cell.font = Font(size=9, color='FFFFFF')
                cell.alignment = Alignment(horizontal='center', vertical='center')

            # 검정색 테두리 스타일
            border = Border(
                left=Side(style='thin', color='000000'),
                right=Side(style='thin', color='000000'),
                top=Side(style='thin', color='000000'),
                bottom=Side(style='thin', color='000000')
            )

            # 색상 매핑
            color_map = {
                '🔴 빨간색': 'C00000',
                '🟠 주황색': 'FFD966',
                '🟢 초록색': '92D050',
                '🟡 노란색': 'FFFF00',
                '🟣 보라색': '7030A0',
                '기본': 'FFFFFF',
                '🔵 파란색': '0070C0'
            }

            # 데이터 추가 및 스타일 적용
            for row_idx, (idx, row) in enumerate(df_excel.iterrows(), 2):
                for col_idx, col_name in enumerate(df_excel.columns, 1):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.value = row[col_name]
                    cell.font = Font(size=9)
                    cell.border = border
                    cell.alignment = Alignment(horizontal='center', vertical='center')

                    # 날짜 열 스타일
                    if col_name == '날짜':
                        cell.fill = PatternFill(start_color='808080', end_color='808080', fill_type='solid')

                    # 요일 열 스타일
                    elif col_name == '요일':
                        if row['요일'] in ['토', '일']:
                            cell.fill = PatternFill(start_color='808080', end_color='808080', fill_type='solid')
                        else:
                            cell.fill = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')

                    # 오전 근무자 색상 및 메모 적용
                    elif col_name in [str(i) for i in range(1, max_morning_workers + 1)]:
                        date = datetime.datetime.strptime(row['날짜'], '%m월 %d일').replace(year=2025).strftime('%Y-%m-%d')
                        worker = row[col_name]
                        if worker:
                            worker_data = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오전') & (df_final_unique['근무자'] == worker)]
                            if not worker_data.empty:
                                status, memo, color = worker_data.iloc[0]['상태'], worker_data.iloc[0]['메모'], worker_data.iloc[0]['색상']
                                fill = PatternFill(start_color=color_map[color], end_color=color_map[color], fill_type='solid')
                                cell.fill = fill
                                if memo:
                                    cell.comment = Comment(memo, 'Huiyeon Kim')

                    # 오후 근무자 색상 및 메모 적용
                    elif col_name.startswith('오후'):
                        date = datetime.datetime.strptime(row['날짜'], '%m월 %d일').replace(year=2025).strftime('%Y-%m-%d')
                        worker = row[col_name]
                        if worker:
                            worker_data = df_final_unique[(df_final_unique['날짜'] == date) & (df_final_unique['시간대'] == '오후') & (df_final_unique['근무자'] == worker)]
                            if not worker_data.empty:
                                status, memo, color = worker_data.iloc[0]['상태'], worker_data.iloc[0]['메모'], worker_data.iloc[0]['색상']
                                fill = PatternFill(start_color=color_map[color], end_color=color_map[color], fill_type='solid')
                                cell.fill = fill
                                if memo:
                                    cell.comment = Comment(memo, 'Huiyeon Kim')

                    # 오전당직(온콜) 색상 적용
                    elif col_name == '오전당직(온콜)':
                        if row[col_name]:
                            cell.font = Font(size=9, bold=True, color='FF69B4')  # bold체, 핑크색 글자 (FF69B4)
                        else:
                            cell.font = Font(size=9)  # 기본 폰트 유지

           # 열 너비 설정
            ws.column_dimensions['A'].width = 10
            for col in ws.columns:
                if col[0].column_letter != 'A':
                    ws.column_dimensions[col[0].column_letter].width = 7

            # Excel 파일을 메모리에 저장
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            st.session_state.output = output  # 바이너리 데이터를 세션 상태에 저장


            # df_final_unique와 df_excel을 기반으로 스케줄 데이터 변환
            def transform_schedule_data(df, df_excel, month_start, month_end):
                # '근무'와 '보충' 상태만 필터링
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
                    morning_workers = date_df[date_df['시간대'] == '오전']['근무자'].tolist()[:12]
                    morning_data = morning_workers + [''] * (12 - len(morning_workers))
                    afternoon_workers = date_df[date_df['시간대'] == '오후']['근무자'].tolist()[:5]
                    afternoon_data = afternoon_workers + [''] * (5 - len(afternoon_workers))
                    
                    # df_excel에서 해당 날짜의 온콜 데이터 가져오기
                    oncall_worker = ''
                    excel_row = df_excel[df_excel['날짜'] == date]
                    if not excel_row.empty:
                        oncall_worker = excel_row['오전당직(온콜)'].iloc[0] if '오전당직(온콜)' in excel_row.columns else ''
                    
                    row_data = [date, weekday] + morning_data + [oncall_worker] + afternoon_data
                    result_df = pd.concat([result_df, pd.DataFrame([row_data], columns=columns)], ignore_index=True)
                
                return result_df

            # Google Sheets 저장 및 다운로드 로직 수정
            if st.session_state.get("is_admin_authenticated", False):
                # 날짜 설정
                month_dt = datetime.datetime.strptime(month_str, "%Y년 %m월")
                next_month_dt = (month_dt + timedelta(days=32)).replace(day=1)
                next_month_str = next_month_dt.strftime("%Y년 %m월")
                next_month_start = month_dt.replace(day=1)
                _, last_day = calendar.monthrange(month_dt.year, month_dt.month)
                next_month_end = month_dt.replace(day=last_day)

                # 구글 시트 열기
                try:
                    url = st.secrets["google_sheet"]["url"]
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(url)
                except Exception as e:
                    st.error(f"⚠️ Google Sheets 연결 중 오류 발생: {str(e)}")
                    st.stop()

                # df_final_unique와 df_excel을 기반으로 스케줄 데이터 변환
                df_schedule = transform_schedule_data(df_final_unique, df_excel, next_month_start, next_month_end)

                # Google Sheets에 스케쥴 저장
                try:
                    # 시트 존재 여부 확인 및 생성/재사용
                    try:
                        worksheet_schedule = sheet.worksheet(f"{month_str} 스케쥴")
                    except WorksheetNotFound:
                        worksheet_schedule = sheet.add_worksheet(title=f"{month_str} 스케쥴", rows=1000, cols=50)

                    # 기존 데이터 삭제 및 업데이트
                    worksheet_schedule.clear()
                    data_schedule = [df_schedule.columns.tolist()] + df_schedule.astype(str).values.tolist()
                    worksheet_schedule.update('A1', data_schedule, value_input_option='RAW')
                except Exception as e:
                    st.error(f"⚠️ {month_str} 스케쥴 테이블 저장 중 오류 발생: {str(e)}")
                    st.write(f"디버깅 정보: {type(e).__name__}, {str(e)}")
                    st.stop()

                # df_cumulative_next 처리
                df_cumulative_next.rename(columns={month_str: next_month_str}, inplace=True)

                # 다음 달 누적 시트 저장
                try:
                    # 시트 존재 여부 확인 및 생성/재사용
                    try:
                        worksheet = sheet.worksheet(f"{next_month_str} 누적")
                    except WorksheetNotFound:
                        worksheet = sheet.add_worksheet(title=f"{next_month_str} 누적", rows=1000, cols=20)

                    # 기존 데이터 삭제 및 업데이트
                    worksheet.clear()
                    data = [df_cumulative_next.columns.tolist()] + df_cumulative_next.values.tolist()
                    worksheet.update('A1', data, value_input_option='USER_ENTERED')
                except Exception as e:
                    st.error(f"⚠️ {next_month_str} 누적 테이블 저장 중 오류 발생: {str(e)}")
                    st.stop()

                # 세션 상태 설정
                st.session_state.assigned = True
                st.session_state.output = output  # 이미 생성된 output 사용
                st.session_state.sheet = sheet
                st.session_state.data_schedule = data_schedule
                st.session_state.df_cumulative_next = df_cumulative_next
                st.session_state.next_month_str = next_month_str

                # 1. 누적 테이블 출력
                st.write(" ")
                st.markdown(f"**➕ {next_month_str} 누적 테이블**")
                st.dataframe(df_cumulative_next)

                # 2. 누적 테이블 저장 완료 메시지
                st.success(f"✅ {next_month_str} 누적 테이블이 Google Sheets에 저장되었습니다.")

                # 3. 구분선
                st.divider()

                # 4. 스케쥴 테이블 저장 완료 메시지
                st.success(f"✅ {month_str} 스케쥴 테이블이 Google Sheets에 저장되었습니다.")

                # 5. 다운로드 버튼
                st.markdown("""
                    <style>
                    .download-button > button {
                        background: linear-gradient(90deg, #e74c3c 0%, #c0392b 100%) !important;
                        color: white !important;
                        font-weight: bold;
                        font-size: 16px;
                        border-radius: 12px;
                        padding: 12px 24px;
                        border: none;
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                        transition: all 0.3s ease;
                    }
                    .download-button > button:hover {
                        background: linear-gradient(90deg, #c0392b 0%, #e74c3c 100%) !important;
                        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
                        transform: translateY(-2px);
                    }
                    .download-button > button:active {
                        transform: translateY(0);
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    }
                    </style>
                """, unsafe_allow_html=True)

                if st.session_state.assigned and not st.session_state.downloaded:
                    with st.container():
                        st.download_button(
                            label="📥 최종 스케쥴 다운로드",
                            data=st.session_state.output,
                            file_name=f"{month_str} 스케쥴.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_schedule_button",
                            type="primary",
                            on_click=lambda: st.session_state.update({"downloaded": True})
                        )

            else:
                st.warning("⚠️ 관리자 권한이 없습니다.")
                st.stop()