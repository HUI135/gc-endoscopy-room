import pandas as pd
import streamlit as st
from io import BytesIO
from collections import defaultdict, Counter
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
import random
import time
import os
from datetime import datetime, date, timedelta
import re
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment

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
next_month = datetime(2025, 4, 1)
next_month_start = date(2025, 4, 1)
next_month_end = date(2025, 4, 30)
last_day = (datetime(2025, 5, 1) - datetime(2025, 4, 1)).days

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드 함수
def load_data():
    required_keys = ["df_schedule", "df_room_request"]
    if "data_loaded" not in st.session_state or not st.session_state["data_loaded"] or not all(key in st.session_state for key in required_keys):
        url = st.secrets["google_sheet"]["url"]
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)

        # 스케줄 시트
        try:
            worksheet1 = sheet.worksheet(f"{month_str} 스케쥴")
            st.session_state["df_schedule"] = pd.DataFrame(worksheet1.get_all_records())
            st.session_state["worksheet1"] = worksheet1
        except Exception as e:
            st.error(f"스케줄 시트를 불러오는 데 문제가 발생했습니다: {e}")
            st.session_state["df_schedule"] = pd.DataFrame(columns=["날짜", "요일"])
            st.session_state["data_loaded"] = False
            st.stop()

        # 방배정 요청 시트
        try:
            worksheet_room_request = sheet.worksheet(f"{month_str} 방배정 요청")
        except WorksheetNotFound:
            worksheet_room_request = sheet.add_worksheet(title=f"{month_str} 방배정 요청", rows="100", cols="20")
            worksheet_room_request.append_row(["이름", "분류", "날짜정보"])
            names_in_schedule = st.session_state["df_schedule"].iloc[:, 2:].stack().dropna().unique()
            new_rows = [[name, "요청 없음", ""] for name in names_in_schedule]
            for row in new_rows:
                worksheet_room_request.append_row(row)
        st.session_state["df_room_request"] = pd.DataFrame(worksheet_room_request.get_all_records()) if worksheet_room_request.get_all_records() else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["worksheet_room_request"] = worksheet_room_request

        st.session_state["data_loaded"] = True

# 최대 배정 한계 설정 UI
st.sidebar.header("최대 배정 한계 설정")
MAX_DUTY = st.sidebar.number_input("1. 최대 당직 합계", min_value=1, value=3, step=1)
MAX_EARLY = st.sidebar.number_input("2. 최대 이른방 합계", min_value=1, value=6, step=1)
MAX_LATE = st.sidebar.number_input("3. 최대 늦은방 합계", min_value=1, value=6, step=1)
MAX_ROOM = st.sidebar.number_input("4. 최대 방별 합계", min_value=1, value=3, step=1)

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
                parsed_date = datetime.strptime(single_date, '%Y-%m-%d')
                if parsed_date.weekday() < 5:  # 평일만 포함
                    result.append(single_date)
            except ValueError:
                st.write(f"잘못된 날짜 형식 무시됨: {single_date}")
        return result
    if '~' in date_str:
        try:
            start_date, end_date = date_str.split('~')
            start_date = start_date.strip()
            end_date = end_date.strip()
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            date_list = pd.date_range(start=start, end=end)
            return [d.strftime('%Y-%m-%d') for d in date_list if d.weekday() < 5]
        except ValueError as e:
            st.write(f"잘못된 날짜 범위 무시됨: {date_str}, 에러: {e}")
            return []
    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        if parsed_date.weekday() < 5:
            return [date_str]
        return []
    except ValueError:
        st.write(f"잘못된 날짜 형식 무시됨: {date_str}")
        return []

# 메인 로직
if st.session_state.get("is_admin_authenticated", False):
    load_data()
    df_schedule = st.session_state.get("df_schedule", pd.DataFrame(columns=["날짜", "요일"]))
    df_room_request = st.session_state.get("df_room_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))

    st.subheader("근무자 명단")
    st.dataframe(df_schedule)

    # 방 설정 UI
    st.subheader("방 설정")
    room_options = [str(i) for i in range(1, 13)]
    selected_rooms = {}

    st.markdown("**8:30 시간대**")
    col1, col2 = st.columns(2)
    with col1:
        num_830_rooms = st.number_input("8:30 방 개수", min_value=0, value=4, step=1, key="830_rooms")
    with col2:
        selected_830_rooms = st.multiselect("8:30 방 번호 선택", room_options, default=['1', '2', '4', '7'], max_selections=num_830_rooms, key="830_room_select")
    selected_rooms['8:30'] = selected_830_rooms

    st.markdown("**9:00 시간대**")
    col1, col2 = st.columns(2)
    with col1:
        num_900_rooms = st.number_input("9:00 방 개수", min_value=0, value=3, step=1, key="900_rooms")
    with col2:
        selected_900_rooms = st.multiselect("9:00 방 번호 선택", room_options, default=['10', '11', '12'], max_selections=num_900_rooms, key="900_room_select")
    selected_rooms['9:00'] = selected_900_rooms

    st.markdown("**9:30 시간대**")
    col1, col2 = st.columns(2)
    with col1:
        num_930_rooms = st.number_input("9:30 방 개수", min_value=0, value=3, step=1, key="930_rooms")
    with col2:
        selected_930_rooms = st.multiselect("9:30 방 번호 선택", room_options, default=['5', '6', '8'], max_selections=num_930_rooms, key="930_room_select")
    selected_rooms['9:30'] = selected_930_rooms

    st.markdown("**10:00 시간대**")
    col1, col2 = st.columns(2)
    with col1:
        num_1000_rooms = st.number_input("10:00 방 개수", min_value=0, value=2, step=1, key="1000_rooms")
    with col2:
        selected_1000_rooms = st.multiselect("10:00 방 번호 선택", room_options, default=['3', '9'], max_selections=num_1000_rooms, key="1000_room_select")
    selected_rooms['10:00'] = selected_1000_rooms

    st.markdown("**13:30 시간대**")
    col1, col2 = st.columns(2)
    with col1:
        num_1330_rooms = st.number_input("13:30 방 개수", min_value=0, value=4, step=1, key="1330_rooms")
    with col2:
        selected_1330_rooms = st.multiselect("13:30 방 번호 선택", room_options, default=['2', '3', '4', '9'], max_selections=num_1330_rooms, key="1330_room_select")
    selected_rooms['13:30'] = selected_1330_rooms

    st.markdown("**당직**")
    col1, col2 = st.columns(2)
    with col1:
        duty_830_room = st.selectbox("8:30 당직 방 선택", selected_830_rooms if selected_830_rooms else room_options, index=0 if selected_830_rooms else 0, key="830_duty")
    with col2:
        duty_1330_room = st.selectbox("13:30 당직 방 선택", selected_1330_rooms if selected_1330_rooms else room_options, index=0 if selected_1330_rooms else 0, key="1330_duty")

    # 동적 time_slots 및 time_groups 생성 (온콜 제외)
    time_slots = {}
    time_groups = {}

    if num_830_rooms > 0:
        time_groups['8:30'] = []
        for room in selected_830_rooms:
            slot_name = f"8:30({room})_당직" if room == duty_830_room else f"8:30({room})"
            time_slots[slot_name] = len(time_slots)
            time_groups['8:30'].append(slot_name)

    if num_900_rooms > 0:
        time_groups['9:00'] = []
        for room in selected_900_rooms:
            slot_name = f"9:00({room})"
            time_slots[slot_name] = len(time_slots)
            time_groups['9:00'].append(slot_name)

    if num_930_rooms > 0:
        time_groups['9:30'] = []
        for room in selected_930_rooms:
            slot_name = f"9:30({room})"
            time_slots[slot_name] = len(time_slots)
            time_groups['9:30'].append(slot_name)

    if num_1000_rooms > 0:
        time_groups['10:00'] = []
        for room in selected_1000_rooms:
            slot_name = f"10:00({room})"
            time_slots[slot_name] = len(time_slots)
            time_groups['10:00'].append(slot_name)

    if num_1330_rooms > 0:
        time_groups['13:30'] = []
        for room in selected_1330_rooms:
            slot_name = f"13:30({room})_당직" if room == duty_1330_room else f"13:30({room})"
            time_slots[slot_name] = len(time_slots)
            time_groups['13:30'].append(slot_name)

    weekday_slots = list(time_slots.keys())
    saturday_slots = [slot for slot in weekday_slots if not slot.startswith('13:30')]

    # 동적 memo_rules 생성
    memo_rules = {
        '당직 안됨': [slot for slot in time_slots if '_당직' in slot],
        '오전 당직 안됨': [slot for slot in time_slots if slot.startswith('8:30') and '_당직' in slot],
        '오후 당직 안됨': [slot for slot in time_slots if slot.startswith('13:30') and '_당직' in slot],
        '당직 아닌 이른방': [slot for slot in time_slots if slot.startswith('8:30') and '_당직' not in slot],
        '8:30': [slot for slot in time_slots if slot.startswith('8:30')],
        '9:00': [slot for slot in time_slots if slot.startswith('9:00')],
        '9:30': [slot for slot in time_slots if slot.startswith('9:30')],
        '10:00': [slot for slot in time_slots if slot.startswith('10:00')],
        '이른방': [slot for slot in time_slots if slot.startswith('8:30')],
        '오후 당직': [slot for slot in time_slots if slot.startswith('13:30') and '_당직' in slot],
        '오전 당직': [slot for slot in time_slots if slot.startswith('8:30') and '_당직' in slot],
        '오전 안됨': [slot for slot in time_slots if slot.startswith('13:30')],
        '오후 안됨': [slot for slot in time_slots if not slot.startswith('13:30')]
    }
    for i in range(1, 13):
        memo_rules[f'{i}번방'] = [slot for slot in time_slots if f'({i})' in slot]
    
    # 고정 배정 설정 UI
    st.subheader("고정 배정 설정")
    st.write("날짜, 시간대, 근무자를 선택하여 방을 고정 배정합니다.")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        # 평일만 포함된 날짜 목록 생성
        all_dates = [next_month_start + timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)]
        weekday_dates = [(d, d.strftime('%Y-%m-%d'), d.weekday()) for d in all_dates if d.weekday() < 5]
        weekday_display = [f"{date_str} ({['월', '화', '수', '목', '금'][weekday]})" for _, date_str, weekday in weekday_dates]
        selected_date_display = st.selectbox("날짜", weekday_display, key="fixed_date")
        selected_date = next(date_str for display, date_str, _ in zip(weekday_display, [d[1] for d in weekday_dates], [d[2] for d in weekday_dates]) if display == selected_date_display)
    with col2:
        time_slots_options = ['오전', '오후']
        selected_time_slot = st.selectbox("시간대", time_slots_options, key="fixed_time_slot")
    with col3:
        time_slot_mapping = {
            '오전': ['8:30', '9:00', '9:30', '10:00'],
            '오후': ['13:30']
        }
        selected_time_slots = time_slot_mapping[selected_time_slot]
        
        available_personnel = []
        if selected_date:
            date_data = df_schedule[df_schedule['날짜'] == selected_date]
            if not date_data.empty:
                for time_slot in selected_time_slots:
                    for slot in time_groups.get(time_slot, []):
                        col_name = slot
                        if col_name in date_data.columns:
                            personnel = date_data[col_name].dropna().tolist()
                            for p in personnel:
                                if p and p not in available_personnel:
                                    available_personnel.append(p)
        selected_person = st.selectbox("근무자", available_personnel if available_personnel else ["근무자 없음"], key="fixed_person")
    with col4:
        room_options = []
        for time_slot in selected_time_slots:
            room_options.extend(time_groups.get(time_slot, []))
        selected_room = st.selectbox("고정 방", room_options if room_options else ["방 없음"], key="fixed_room")
    
    if st.button("고정배정 추가"):
        if selected_date and selected_person != "근무자 없음" and selected_room != "방 없음":
            if 'fixed_assignments_ui' not in st.session_state:
                st.session_state['fixed_assignments_ui'] = {}
            if selected_date not in st.session_state['fixed_assignments_ui']:
                st.session_state['fixed_assignments_ui'][selected_date] = {}
            st.session_state['fixed_assignments_ui'][selected_date][selected_room] = selected_person
            st.success(f"{selected_date}의 {selected_room}에 {selected_person} 고정 배정 추가 완료!")
    
    if st.session_state.get('fixed_assignments_ui'):
        st.subheader("현재 고정 배정 목록")
        for date, assignments in st.session_state['fixed_assignments_ui'].items():
            for slot, person in assignments.items():
                st.write(f"{date}: {slot} -> {person}")

    def extract_data(df, is_schedule=True):
        data = {}
        if is_schedule:
            headers = df.columns.tolist()
            if '날짜' not in headers or '요일' not in headers:
                st.error("df_schedule에 '날짜' 또는 '요일' 열이 없습니다.")
                return data
            
            for _, row in df.iterrows():
                date_str = row['날짜']
                if isinstance(date_str, str):
                    try:
                        if "월" in date_str and "일" in date_str:
                            month, day = date_str.replace("월", "").replace("일", "").split()
                            year = 2025
                            date = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()
                            date_str = date.strftime('%Y-%m-%d')
                        else:
                            date = datetime.strptime(date_str, '%Y-%m-%d').date()
                            date_str = date.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
                else:
                    continue
                
                if date_str in data:
                    continue
                
                day_of_week_raw = row['요일']
                weekday_map = {
                    '월': '월요일', '화': '화요일', '수': '수요일', '목': '목요일', 
                    '금': '금요일', '토': '토요일', '일': '일요일',
                    'Mon': '월요일', 'Tue': '화요일', 'Wed': '수요일', 'Thu': '목요일', 
                    'Fri': '금요일', 'Sat': '토요일', 'Sun': '일요일',
                    'Monday': '월요일', 'Tuesday': '화요일', 'Wednesday': '수요일', 
                    'Thursday': '목요일', 'Friday': '금요일', 'Saturday': '토요일', 'Sunday': '일요일'
                }
                day_of_week = day_of_week_raw
                for key, value in weekday_map.items():
                    if key in str(day_of_week_raw):
                        day_of_week = value
                        break
                else:
                    weekday_num = date.weekday()
                    weekdays = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']
                    day_of_week = weekdays[weekday_num]
                
                personnel = []
                memo_dict = {}
                for col in headers[2:]:
                    cell_value = row[col]
                    if pd.notna(cell_value) and cell_value not in ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']:
                        values = str(cell_value).replace('/', ',').split(',')
                        for val in values:
                            val = val.strip()
                            if val:
                                personnel.append(val)
                personnel_with_suffix = []
                name_counts = Counter()
                for name in personnel:
                    name_counts[name] += 1
                    suffix = f"_{name_counts[name]}" if name_counts[name] > 1 else ""
                    personnel_with_suffix.append(f"{name}{suffix}")
                
                data[date_str] = {
                    'personnel': personnel_with_suffix,
                    'original_personnel': personnel,
                    'day': day_of_week,
                    'memos': memo_dict,
                    'headers': headers
                }
        
        else:  # df_room_request 처리
            headers = df.columns.tolist()
            if '이름' not in headers or '분류' not in headers or '날짜정보' not in headers:
                st.error("df_room_request에 '이름', '분류', '날짜정보' 열이 없습니다.")
                return data
            
            for _, row in df.iterrows():
                name = row['이름']
                category = row['분류']
                date_info = row['날짜정보']
                if pd.isna(date_info) or not date_info:
                    continue
                
                applicable_dates = parse_date_range(date_info)
                for date_str in applicable_dates:
                    if date_str not in data:
                        data[date_str] = {
                            'personnel': [],
                            'original_personnel': [],
                            'day': '',
                            'memos': {},
                            'fixed_assignments': {},
                            'headers': headers
                        }
                    if category in memo_rules:
                        data[date_str]['memos'][name] = category
                    for col in headers:
                        if col in time_slots:
                            cell_value = row[col]
                            if pd.notna(cell_value):
                                data[date_str]['fixed_assignments'][col] = name
                
        return data

    # df_schedule 및 df_room_request 데이터 추출
    df_schedule_data = extract_data(df_schedule, is_schedule=True)
    df_room_request_data = extract_data(df_room_request, is_schedule=False)

    # df_room_request_data에 요일 정보 추가
    for date_str in df_room_request_data:
        if date_str in df_schedule_data:
            df_room_request_data[date_str]['day'] = df_schedule_data[date_str]['day']

    if not df_schedule_data:
        st.error("df_schedule가 비어 있습니다. 데이터가 있는지, 형식이 맞는지 확인하세요.")
        st.stop()

    # 고정 배정 통합
    fixed_assignments_combined = {}
    for date in df_room_request_data:
        fixed_assignments_combined[date] = df_room_request_data[date].get('fixed_assignments', {})
    for date, assignments in st.session_state.get('fixed_assignments_ui', {}).items():
        if date not in fixed_assignments_combined:
            fixed_assignments_combined[date] = {}
        fixed_assignments_combined[date].update(assignments)

    # 인원 불일치 감지
    mismatch_warnings = []
    for date in fixed_assignments_combined.keys():
        if date in df_schedule_data:
            schedule_personnel = set(df_schedule_data[date]['original_personnel'])
            for slot, person in fixed_assignments_combined[date].items():
                if person not in schedule_personnel:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    formatted_date = date_obj.strftime('%m월 %d일')
                    mismatch_warnings.append(
                        f"df_schedule의 {formatted_date}에는 '{person}'이 없음에도, 고정 배정에서 '{person}'이 '{slot}'에 배치되어 있습니다. "
                        f"이 경우 {formatted_date}의 고정 배치 사항이 무시됩니다."
                    )

    if mismatch_warnings:
        for warning in mismatch_warnings:
            st.warning(warning)

    def apply_memo_rules(assignment, personnel, memos, fixed_personnel, slots, assigned_counts, personnel_counts, time_groups, assigned_by_time, total_early, total_late, total_duty, total_rooms, day_of_week, ignore_memos=None):
        if ignore_memos is None:
            ignore_memos = set()
        prioritized = []
        all_slots = set(slots)
        for person in personnel:
            original_name = person.split('_')[0]
            if original_name in memos and person not in fixed_personnel and original_name not in ignore_memos:
                rule = memos[original_name]
                if rule in memo_rules:
                    if rule in ['당직 안됨', '오전 당직 안됨', '오후 당직 안됨', '오전 안됨', '오후 안됨']:
                        forbidden_slots = memo_rules[rule]
                        allowed_slots = list(all_slots - set(forbidden_slots))
                        prioritized.append((person, allowed_slots))
                    else:
                        prioritized.append((person, memo_rules[rule]))
        remaining_slots = [i for i, x in enumerate(assignment) if x is None]
        memo_assignments = {}
        for person, allowed_slots in prioritized:
            original_name = person.split('_')[0]
            valid_slots = [
                i for i in remaining_slots 
                if slots[i] in allowed_slots 
                and assigned_counts[person] < personnel_counts[person]
                and person not in assigned_by_time.get(next(t for t, g in time_groups.items() if slots[i] in g), set())
                and total_early[original_name] < MAX_EARLY
                and total_late[original_name] < MAX_LATE
                and total_duty[original_name] < MAX_DUTY
            ]
            if valid_slots:
                slot_idx = random.choice(valid_slots)
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                memo_assignments.setdefault(slots[slot_idx], Counter())[person] += 1
                remaining_slots.remove(slot_idx)
                for time_group, group in time_groups.items():
                    if slots[slot_idx] in group:
                        assigned_by_time[time_group].add(person)
                if slots[slot_idx] in early_slots and day_of_week != '토요일':
                    total_early[original_name] += 1
                if slots[slot_idx] in late_slots and day_of_week != '토요일':
                    total_late[original_name] += 1
                if slots[slot_idx] in duty_slots and day_of_week != '토요일':
                    total_duty[original_name] += 1
                room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                if room_num and day_of_week != '토요일':
                    total_rooms[room_num.group(1)][original_name] += 1
        return assignment, memo_assignments

    def calculate_stats(assignment, slots, day_of_week):
        early_slots = [slot for slot in time_slots if slot.startswith('8:30') and '_당직' not in slot]
        late_slots = [slot for slot in time_slots if slot.startswith('10:00')]
        duty_slots = [slot for slot in time_slots if '_당직' in slot]
        slot_counts = {slot.replace('_당직', ''): Counter() for slot in time_slots.keys()}
        
        stats = Counter()
        early_count = Counter()
        late_count = Counter()
        duty_count = Counter()
        
        for slot, person in zip(slots, assignment):
            if person:
                original_name = person.split('_')[0]
                stats[original_name] += 1
                if day_of_week != '토요일':
                    if slot in early_slots:
                        early_count[original_name] += 1
                    if slot in late_slots:
                        late_count[original_name] += 1
                    if slot in duty_slots:
                        duty_count[original_name] += 1
                    slot_counts[slot.replace('_당직', '')][original_name] += 1
        
        return stats, early_count, late_count, duty_count, slot_counts

    def count_violations(total_early, total_late, total_duty, total_slots):
        violations = 0
        all_personnel = set(total_early.keys()) | set(total_late.keys()) | set(total_duty.keys()) | set().union(*[total_slots[slot].keys() for slot in total_slots])
        for person in all_personnel:
            if total_early.get(person, 0) > MAX_EARLY:
                violations += total_early.get(person, 0) - MAX_EARLY
            if total_late.get(person, 0) > MAX_LATE:
                violations += total_late.get(person, 0) - MAX_LATE
            if total_duty.get(person, 0) > MAX_DUTY:
                violations += total_duty.get(person, 0) - MAX_DUTY
            for slot in total_slots:
                if total_slots[slot].get(person, 0) > MAX_ROOM:
                    violations += total_slots[slot].get(person, 0) - MAX_ROOM
        return violations

    def assign_remaining(assignment, personnel_list, available_slots, slots, assigned_counts, personnel_counts, time_groups, assigned_by_original_time, total_early, total_late, total_duty, total_rooms, MAX_EARLY, MAX_LATE, MAX_DUTY, MAX_ROOM, day_of_week, early_slots, late_slots, duty_slots):
        personnel_list.sort()
        for person in personnel_list:
            if available_slots:
                original_name = person.split('_')[0]
                possible_slots = []
                
                for slot_idx in available_slots:
                    slot = slots[slot_idx]
                    time_group = next(t for t, g in time_groups.items() if slot in g)
                    if original_name not in assigned_by_original_time[time_group]:
                        possible_slots.append(slot_idx)
                
                if possible_slots:
                    slot_idx = possible_slots[0]
                    assignment[slot_idx] = person
                    assigned_counts[person] += 1
                    time_group = next(t for t, g in time_groups.items() if slots[slot_idx] in g)
                    assigned_by_original_time[time_group].add(original_name)
                    if slots[slot_idx] in early_slots and day_of_week != '토요일':
                        total_early[original_name] += 1
                    if slots[slot_idx] in late_slots and day_of_week != '토요일':
                        total_late[original_name] += 1
                    if slots[slot_idx] in duty_slots and day_of_week != '토요일':
                        total_duty[original_name] += 1
                    room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                    if room_num and day_of_week != '토요일':
                        total_rooms[room_num.group(1)][original_name] += 1
                    available_slots.remove(slot_idx)
        return assignment, available_slots

    def random_assign(personnel, slots, fixed_assignments, memos, day_of_week, time_groups, total_stats, current_date):
        random.seed(42)
        
        max_attempts = 100
        duty_slots = [slot for slot in time_slots if '_당직' in slot]
        early_slots = [slot for slot in time_slots if slot.startswith('8:30') and '_당직' not in slot]
        late_slots = [slot for slot in time_slots if slot.startswith('10:00')]
        
        total_personnel_count = sum(Counter(personnel).values())
        
        best_assignment = None
        best_fixed_assignments_record = None
        best_memo_assignments = None
        min_unassigned = float('inf')
        min_violations = float('inf')
        best_total_early = total_stats['early'].copy()
        best_total_late = total_stats['late'].copy()
        best_total_duty = total_stats['duty'].copy()
        best_total_slots = {slot: total_stats['slots'][slot].copy() for slot in total_stats['slots']}
        best_total_stats = total_stats['total'].copy()

        for attempt in range(max_attempts):
            assignment = [None] * len(slots)
            fixed_personnel = set()
            assigned_counts = Counter()
            personnel_counts = Counter(personnel)
            assigned_by_original_time = {time_group: set() for time_group in time_groups.keys()}
            fixed_assignments_record = {}
            memo_assignments = {}
            
            total_early = total_stats['early'].copy()
            total_late = total_stats['late'].copy()
            total_duty = total_stats['duty'].copy()
            total_rooms = {str(i): total_stats['rooms'][str(i)].copy() for i in range(1, 13)}
            
            # 고정 배치 적용
            for date, assignments in fixed_assignments.items():
                if date == current_date:
                    for fixed_slot, person in assignments.items():
                        if fixed_slot in slots:
                            slot_idx = slots.index(fixed_slot)
                            if assignment[slot_idx] is not None:
                                st.warning(
                                    f"DEBUG | {current_date}: {fixed_slot}에 이미 {assignment[slot_idx]}가 배정되어 있습니다. "
                                    f"{person}은 무시됩니다."
                                )
                                continue
                            if isinstance(person, list):
                                st.error(f"❌ person이 리스트임: {person} (type: {type(person)})")
                                continue
                            original_name = person.split('_')[0]
                            time_group = next(t for t, g in time_groups.items() if fixed_slot in g)
                            if original_name not in assigned_by_original_time[time_group]:
                                if isinstance(person, str):
                                    assignment[slot_idx] = person
                                else:
                                    st.error(f"❌ 잘못된 person 타입: {person} (type: {type(person)})")
                                    continue
                                fixed_personnel.add(person)
                                assigned_counts[person] += 1
                                fixed_assignments_record.setdefault(fixed_slot, Counter())[person] += 1
                                assigned_by_original_time[time_group].add(original_name)
                                if fixed_slot in early_slots and day_of_week != '토요일':
                                    total_early[original_name] += 1
                                if fixed_slot in late_slots and day_of_week != '토요일':
                                    total_late[original_name] += 1
                                if fixed_slot in duty_slots and day_of_week != '토요일':
                                    total_duty[original_name] += 1
                                room_num = re.search(r'\((\d+)\)', fixed_slot)
                                if room_num and day_of_week != '토요일':
                                    total_rooms[room_num.group(1)][original_name] += 1

            # 메모 기반 우선 배치
            assignment, memo_assignments = apply_memo_rules(
                assignment, personnel, memos, fixed_personnel, slots, assigned_counts,
                personnel_counts, time_groups, assigned_by_original_time, total_early,
                total_late, total_duty, total_rooms, day_of_week
            )

            # 당직 슬롯 배정
            available_slots = [i for i, slot in enumerate(slots) if assignment[i] is None]
            personnel_list = [p for p in personnel if assigned_counts[p] < personnel_counts[p]]
            duty_indices = [i for i in available_slots if slots[i] in duty_slots]
            personnel_list = sorted(personnel_list, key=lambda p: total_duty[p.split('_')[0]])
            for slot_idx in duty_indices:
                time_group = next(t for t, g in time_groups.items() if slots[slot_idx] in g)
                for person in personnel_list:
                    original_name = person.split('_')[0]
                    if (original_name not in assigned_by_original_time[time_group] and 
                        assigned_counts[person] < personnel_counts[person]):
                        if isinstance(person, str):
                            assignment[slot_idx] = person
                        else:
                            st.error(f"❌ 잘못된 person 타입: {person} (type: {type(person)})")
                            continue
                        assigned_counts[person] += 1
                        assigned_by_original_time[time_group].add(original_name)
                        if day_of_week != '토요일':
                            total_duty[original_name] += 1
                            if slots[slot_idx].startswith('8:30'):
                                total_early[original_name] += 1
                        room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                        if room_num and day_of_week != '토요일':
                            total_rooms[room_num.group(1)][original_name] += 1
                        available_slots.remove(slot_idx)
                        personnel_list = [p for p in personnel_list if assigned_counts[p] < personnel_counts[p]]
                        break

            # 이른방 슬롯 배정
            early_indices = [i for i in available_slots if slots[i] in early_slots]
            personnel_list = sorted(personnel_list, key=lambda p: total_early[p.split('_')[0]])
            for slot_idx in early_indices:
                time_group = next(t for t, g in time_groups.items() if slots[slot_idx] in g)
                for person in personnel_list:
                    original_name = person.split('_')[0]
                    if (original_name not in assigned_by_original_time[time_group] and 
                        assigned_counts[person] < personnel_counts[person]):
                        if isinstance(person, str):
                            assignment[slot_idx] = person
                        else:
                            st.error(f"❌ 잘못된 person 타입: {person} (type: {type(person)})")
                            continue
                        assigned_counts[person] += 1
                        assigned_by_original_time[time_group].add(original_name)
                        if day_of_week != '토요일':
                            total_early[original_name] += 1
                        room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                        if room_num and day_of_week != '토요일':
                            total_rooms[room_num.group(1)][original_name] += 1
                        available_slots.remove(slot_idx)
                        personnel_list = [p for p in personnel_list if assigned_counts[p] < personnel_counts[p]]
                        break

            # 늦은방 슬롯 배정
            late_indices = [i for i in available_slots if slots[i] in late_slots]
            personnel_list = sorted(personnel_list, key=lambda p: total_late[p.split('_')[0]])
            for slot_idx in late_indices:
                time_group = next(t for t, g in time_groups.items() if slots[slot_idx] in g)
                for person in personnel_list:
                    original_name = person.split('_')[0]
                    if (original_name not in assigned_by_original_time[time_group] and 
                        assigned_counts[person] < personnel_counts[person]):
                        if isinstance(person, str):
                            assignment[slot_idx] = person
                        else:
                            st.error(f"❌ 잘못된 person 타입: {person} (type: {type(person)})")
                            continue
                        assigned_counts[person] += 1
                        assigned_by_original_time[time_group].add(original_name)
                        if day_of_week != '토요일':
                            total_late[original_name] += 1
                        room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                        if room_num and day_of_week != '토요일':
                            total_rooms[room_num.group(1)][original_name] += 1
                        available_slots.remove(slot_idx)
                        personnel_list = [p for p in personnel_list if assigned_counts[p] < personnel_counts[p]]
                        break

            # 나머지 슬롯 배정
            available_slots = [i for i, slot in enumerate(slots) if assignment[i] is None]
            personnel_list = [p for p in personnel if assigned_counts[p] < personnel_counts[p]]
            assignment, available_slots = assign_remaining(
                assignment, personnel_list, available_slots, slots, assigned_counts, 
                personnel_counts, time_groups, assigned_by_original_time, total_early, 
                total_late, total_duty, total_rooms, MAX_EARLY, MAX_LATE, MAX_DUTY, 
                MAX_ROOM, day_of_week, early_slots, late_slots, duty_slots
            )

            # 강제 배정
            if available_slots and personnel_list:
                personnel_list = sorted(
                    personnel_list,
                    key=lambda p: (total_duty[p.split('_')[0]], total_early[p.split('_')[0]], total_late[p.split('_')[0]], sum(total_rooms[r][p.split('_')[0]] for r in total_rooms))
                )
                for slot_idx in available_slots[:len(personnel_list)]:
                    time_group = next(t for t, g in time_groups.items() if slots[slot_idx] in g)
                    for person in personnel_list:
                        original_name = person.split('_')[0]
                        if (assigned_counts[person] < personnel_counts[person] and 
                            original_name not in assigned_by_original_time[time_group]):
                            if isinstance(person, str):
                                assignment[slot_idx] = person
                            else:
                                st.error(f"❌ 잘못된 person 타입: {person} (type: {type(person)})")
                                continue
                            assigned_counts[person] += 1
                            assigned_by_original_time[time_group].add(original_name)
                            if slots[slot_idx] in early_slots and day_of_week != '토요일':
                                total_early[original_name] += 1
                            if slots[slot_idx] in late_slots and day_of_week != '토요일':
                                total_late[original_name] += 1
                            if slots[slot_idx] in duty_slots and day_of_week != '토요일':
                                total_duty[original_name] += 1
                            room_num = re.search(r'\((\d+)\)', slots[slot_idx])
                            if room_num and day_of_week != '토요일':
                                total_rooms[room_num.group(1)][original_name] += 1
                            available_slots.remove(slot_idx)
                            personnel_list = [p for p in personnel_list if assigned_counts[p] < personnel_counts[p]]
                            break

            # 미배정 인원 계산
            unassigned_count = sum(personnel_counts[p] - assigned_counts[p] for p in personnel_counts if personnel_counts[p] > assigned_counts[p])
            
            # 통계 계산 및 위반 확인
            stats, early_count, late_count, duty_count, slot_counts = calculate_stats(assignment, slots, day_of_week)
            temp_total_early = total_stats['early'].copy()
            temp_total_late = total_stats['late'].copy()
            temp_total_duty = total_stats['duty'].copy()
            temp_total_slots = {slot: total_stats['slots'][slot].copy() for slot in total_stats['slots']}
            temp_total_stats = total_stats['total'].copy()

            temp_total_early.update(early_count)
            temp_total_late.update(late_count)
            temp_total_duty.update(duty_count)
            for slot in slot_counts:
                temp_total_slots[slot].update(slot_counts[slot])
            temp_total_stats.update(stats)

            violations = count_violations(temp_total_early, temp_total_late, temp_total_duty, temp_total_slots)

            if unassigned_count < min_unassigned or (unassigned_count == min_unassigned and violations < min_violations):
                min_unassigned = unassigned_count
                min_violations = violations
                best_assignment = assignment.copy()
                best_fixed_assignments_record = fixed_assignments_record.copy()
                best_memo_assignments = memo_assignments.copy()
                best_total_early = temp_total_early.copy()
                best_total_late = temp_total_late.copy()
                best_total_duty = temp_total_duty.copy()
                best_total_slots = {slot: temp_total_slots[slot].copy() for slot in temp_total_slots}
                best_total_stats = temp_total_stats.copy()
                if min_unassigned == 0:
                    break

        if best_assignment is None:
            best_assignment = assignment
            best_fixed_assignments_record = fixed_assignments_record
            best_memo_assignments = memo_assignments
            best_total_early = temp_total_early
            best_total_late = temp_total_late
            best_total_duty = temp_total_duty
            best_total_slots = temp_total_slots
            best_total_stats = temp_total_stats

        total_stats['early'] = best_total_early
        total_stats['late'] = best_total_late
        total_stats['duty'] = best_total_duty
        total_stats['slots'] = best_total_slots
        total_stats['total'] = best_total_stats

        if min_unassigned > 0:
            unassigned = {p: personnel_counts[p] - Counter(best_assignment)[p] for p in personnel_counts if personnel_counts[p] > Counter(best_assignment)[p]}
        return best_assignment, best_fixed_assignments_record, best_memo_assignments

    # 슬롯 매핑 설정
    slot_mappings = {}
    for date, data in df_schedule_data.items():
        day_of_week = data['day']
        if day_of_week == '토요일':
            slot_mappings[date] = saturday_slots
        else:
            slot_mappings[date] = weekday_slots

    # total_stats 초기화
    if 'total_stats' not in st.session_state:
        st.session_state.total_stats = {
            'total': Counter(),
            'early': Counter(),
            'late': Counter(),
            'duty': Counter(),
            'slots': {slot.replace('_당직', ''): Counter() for slot in time_slots.keys()},
            'rooms': {str(i): Counter() for i in range(1, 13)}
        }
    total_stats = st.session_state.total_stats

    # 세션 상태 초기화 및 배정 계산
    if 'assignments' not in st.session_state:
        assignments = {}
        total_fixed_stats = {slot: Counter() for slot in time_slots.keys()}
        total_memo_stats = {slot: Counter() for slot in time_slots.keys()}
        
        for date, data in sorted(df_schedule_data.items()):
            personnel = data['personnel']
            original_personnel = data['original_personnel']
            memos = data['memos']
            day_of_week = data['day']

            slots = slot_mappings.get(date, weekday_slots)
            fixed_assignments = {date: fixed_assignments_combined.get(date, {})}

            if personnel:
                assignment, fixed_assignments_record, memo_assignments = random_assign(
                    personnel, slots, fixed_assignments, memos, day_of_week, time_groups, total_stats, current_date=date
                )
                assignments[date] = assignment
                for slot in fixed_assignments_record:
                    total_fixed_stats[slot].update(fixed_assignments_record[slot])
                for slot in memo_assignments:
                    total_memo_stats[slot].update(memo_assignments[slot])
            else:
                assignments[date] = [None] * len(slots)

        st.session_state.assignments = assignments
        st.session_state.slot_mappings = slot_mappings
        st.session_state.total_stats = total_stats
        st.session_state.total_fixed_stats = total_fixed_stats
        st.session_state.total_memo_stats = total_memo_stats
    else:
        assignments = st.session_state.assignments
        slot_mappings = st.session_state.slot_mappings
        total_stats = st.session_state.total_stats
        total_fixed_stats = st.session_state.total_fixed_stats
        total_memo_stats = st.session_state.total_memo_stats

    # 통합 배치 결과 DataFrame
    result_data = []
    all_columns = ['날짜', '요일'] + [str(i) for i in range(1, 13)] + ['온콜/오전당직', '오후1', '오후2', '오후3', '오후4']
    memo_mapping = {}

    for date in sorted(df_schedule_data.keys()):
        assigned_slots = slot_mappings.get(date, weekday_slots)
        assignment = assignments.get(date, [None] * len(assigned_slots))
        memos = df_schedule_data[date]['memos']
        
        slot_to_person = {slot: None for slot in time_slots.keys()}
        for slot, person in zip(assigned_slots, assignment):
            if person:
                original_name = person.split('_')[0] if '_' in person else person
                slot_to_person[slot] = original_name
                if original_name in memos:
                    memo_mapping[date][(original_name, slot)] = memos[original_name]
        
        # 결과 행 구성
        row = [date, df_schedule_data[date]['day']]
        # 방 번호 1~12
        for i in range(1, 13):
            person = None
            for slot in time_slots.keys():
                if f'({i})' in slot and slot_to_person[slot]:
                    person = slot_to_person[slot]
                    break
            row.append(person)
        # 온콜/오전당직 (8:30 당직)
        oem_duty = None
        for slot in time_slots.keys():
            if slot.startswith('8:30') and '_당직' in slot and slot_to_person[slot]:
                oem_duty = slot_to_person[slot]
                break
        row.append(oem_duty)
        # 오 nuts1~4 (13:30 방들)
        afternoon_slots = [slot for slot in time_slots.keys() if slot.startswith('13:30')]
        afternoon_assignments = [slot_to_person[slot] for slot in afternoon_slots]
        for i in range(4):
            row.append(afternoon_assignments[i] if i < len(afternoon_assignments) else None)
        
        result_data.append(row)

    if not result_data:
        st.error("result_data가 비어 있습니다. 배정 결과가 생성되지 않았습니다.")
        st.stop()

    result_df = pd.DataFrame(result_data, columns=all_columns)

    # 인원별 전체 통계 DataFrame
    all_personnel = set(total_stats['total'].keys())
    if not all_personnel:
        all_personnel = set().union(*[set(data['original_personnel']) for data in df_schedule_data.values()])
        if not all_personnel:
            st.error("인원 데이터가 없습니다. df_schedule를 확인하세요.")
            st.stop()

    stats_data = []
    slot_columns = list(set(slot.replace('_당직', '') for slot in time_slots.keys()))  # 중복 제거
    for person in all_personnel:
        row = {
            '인원': person,
            '전체 합계': total_stats['total'].get(person, 0),
            '이른방 합계': total_stats['early'].get(person, 0),
            '늦은방 합계': total_stats['late'].get(person, 0),
            '당직 합계': total_stats['duty'].get(person, 0)
        }
        for slot in slot_columns:
            row[f'{slot} 합계'] = total_stats['slots'].get(slot, Counter()).get(person, 0)
        stats_data.append(row)

    stats_df = pd.DataFrame(stats_data)
    stats_df = stats_df.sort_values(by='인원').reset_index(drop=True)

    # 정보 출력
    person_info = {}
    max_assignments = {
        '이른방 합계': MAX_EARLY, '늦은방 합계': MAX_LATE, '당직 합계': MAX_DUTY
    }
    for slot in slot_columns:
        max_assignments[f'{slot} 합계'] = MAX_ROOM

    for slot in total_fixed_stats:
        for person, count in total_fixed_stats[slot].items():
            if count > 0:
                if person not in person_info:
                    person_info[person] = {'fixed': {}, 'priority': {}, 'sums': {}}
                person_info[person]['fixed'][slot] = count

    for slot in total_memo_stats:
        for person, count in total_memo_stats[slot].items():
            if count > 0:
                if person not in person_info:
                    person_info[person] = {'fixed': {}, 'priority': {}, 'sums': {}}
                person_info[person]['priority'][slot] = count

    for idx, row in stats_df.iterrows():
        person = row['인원']
        if person not in person_info:
            person_info[person] = {'fixed': {}, 'priority': {}, 'sums': {}}
        for col in stats_df.columns[1:]:
            person_info[person]['sums'][col] = row[col]

    st.divider()
    st.write("### 👥 인원별 우선(고정) 배정 정보")

    html_content = ""
    sorted_names = sorted(person_info.keys())

    merged_info = defaultdict(lambda: {"fixed": [], "priority": []})

    for person, info in person_info.items():
        base_name = re.sub(r'_\d+$', '', person)
        for slot, count in info['fixed'].items():
            merged_info[base_name]["fixed"].append(f"{slot} {count}번 고정 배치")
        for slot, count in info['priority'].items():
            merged_info[base_name]["priority"].append(f"{slot} {count}번 우선배치")

    html_content = ""
    sorted_names = sorted(merged_info.keys())

    for person in sorted_names:
        info = merged_info[person]
        output = [f"<span class='person'>{person}: </span>"]
        fixed_str = " / ".join(info["fixed"])
        priority_str = " / ".join(info["priority"])
        if fixed_str or priority_str:
            if fixed_str:
                output.append(fixed_str)
            if priority_str:
                output.append(f" / {priority_str}" if fixed_str else priority_str)
            html_content += f"<p>{''.join(output)}</p>"

    st.markdown(
        f"""
        <style>
        .custom-callout {{
            background-color: #f0f8ff;
            padding: 8px;
            border-radius: 6px;
            border-left: 4px solid #4682b4;
            box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
            margin-bottom: 4px;
            font-size: 14px;
            color: #2C3E50;
            line-height: 1.3;
        }}
        .custom-callout p {{
            margin: 0;
            padding: 2px 0;
            text-align: left;
        }}
        .person {{
            font-weight: bold;
            color: #2C3E50;
        }}
        </style>
        <div class="custom-callout">{html_content}</div>
        """,
        unsafe_allow_html=True
    )

    st.divider()
    st.write("### ⚠️ 최대 배정 한계 초과 경고")

    warnings = []
    for person in sorted_names:
        info = person_info[person]
        for slot_sum, count in info['sums'].items():
            max_count = max_assignments.get(slot_sum, float('inf'))
            if count > max_count:
                warnings.append(f"<span class='person'>{person}: </span>{slot_sum} = {count} (최대 {max_count}번 초과)")

    if warnings:
        warning_text = "".join([f"<p>{w}</p>" for w in warnings])
        html_content = f"""
        <div class="custom-callout warning-callout">
            {warning_text}
        </div>
        """
    else:
        html_content = """
        <div class="custom-callout warning-callout">
            <p>모든 배정이 적절한 한계 내에 있습니다.</p>
        </div>
        """

    st.markdown(
        f"""
        <style>
        .custom-callout {{
            padding: 8px;
            border-radius: 6px;
            box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
            margin-bottom: 4px;
            font-size: 14px;
            color: #2C3E50;
            line-height: 1.3;
        }}
        .custom-callout p {{
            margin: 0;
            padding: 2px 0;
            text-align: left;
        }}
        .person {{
            font-weight: bold;
            color: #2C3E50;
        }}
        .warning-callout {{
            background-color: #fff3cd;
            border-left: 4px solid #ffa500;
        }}
        </style>
        {html_content}
        """,
        unsafe_allow_html=True
    )

    st.divider()
    st.write("### 통합 배치 결과")
    st.dataframe(result_df)

    # "재랜덤화" 버튼
    if st.button("재랜덤화", key="rerandomize_button"):
        st.session_state.pop('assignments', None)
        st.rerun()

    st.divider()
    st.write("### 인원별 전체 통계")
    st.dataframe(stats_df)

    # 엑셀 워크북 생성
    output_wb = Workbook()
    schedule_sheet = output_wb.active
    schedule_sheet.title = "Schedule"

    default_font = Font(name="맑은 고딕", size=9)
    bold_font = Font(name="맑은 고딕", size=9, bold=True)
    magenta_bold_font = Font(name="맑은 고딕", size=9, bold=True, color="FF00FF")
    alignment_center = Alignment(horizontal='center', vertical='center')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    date_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
    empty_row_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
    weekday_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    saturday_with_person_fill = PatternFill(start_color="BFBFBF", end_color="BFBFBF", fill_type="solid")

    schedule_header_colors = {
        **{f"{i}": PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid") for i in range(1, 13)},
        '온콜/오전당직': PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid"),
        '오후1': PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid"),
        '오후2': PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid"),
        '오후3': PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid"),
        '오후4': PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")
    }

    # 헤더 설정
    for col_idx, header in enumerate(all_columns, 1):
        cell = schedule_sheet.cell(row=1, column=col_idx, value=header)
        cell.font = bold_font
        cell.alignment = alignment_center
        cell.border = border
        cell.fill = schedule_header_colors.get(header, PatternFill())

    schedule_sheet.column_dimensions['A'].width = 12
    schedule_sheet.column_dimensions['B'].width = 8
    for col in range(3, len(all_columns) + 1):
        schedule_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 10

    for i, row in enumerate(result_data, 2):
        date = row[0]
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%m월 %d일')
        
        has_person = any(x is not None and x != '' for x in row[2:])

        date_cell = schedule_sheet.cell(row=i, column=1, value=formatted_date)
        date_cell.fill = date_fill
        date_cell.font = bold_font
        date_cell.alignment = alignment_center
        date_cell.border = border

        day_of_week = row[1]
        day_cell = schedule_sheet.cell(row=i, column=2, value=day_of_week)
        if not has_person:
            day_cell.fill = empty_row_fill
        elif day_of_week == '토요일':
            day_cell.fill = saturday_with_person_fill
        elif day_of_week in ['월요일', '화요일', '수요일', '목요일']:
            day_cell.fill = weekday_fill
        day_cell.font = default_font
        day_cell.alignment = alignment_center
        day_cell.border = border

        for j, value in enumerate(row[2:], 2):
            cell = schedule_sheet.cell(row=i, column=j+1, value=value)
            header = all_columns[j]
            if header == '온콜/오전당직':
                cell.font = magenta_bold_font
            else:
                cell.font = default_font
            if not has_person:
                cell.fill = empty_row_fill
            cell.alignment = alignment_center
            cell.border = border
            memo_key = (value, header) if value else None
            if value and date in memo_mapping and memo_key in memo_mapping[date]:
                memo = memo_mapping[date][memo_key]
                cell.comment = Comment(memo, "Memo")

    stats_sheet = output_wb.create_sheet(title="Personnel_Stats")

    personnel_fill = PatternFill(start_color="D0CECE", end_color="D0CECE", fill_type="solid")
    total_sum_fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    early_sum_fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
    late_sum_fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
    duty_sum_fill = PatternFill(start_color="FF00FF", end_color="FF00FF", fill_type="solid")
    slot_830_fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
    slot_900_fill = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
    slot_930_fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
    slot_1000_fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
    slot_1330_fill = PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")

    headers = ['인원', '전체 합계', '이른방 합계', '늦은방 합계', '당직 합계'] + [f'{slot} 합계' for slot in slot_columns]
    for col, header in enumerate(headers, 1):
        cell = stats_sheet.cell(row=1, column=col, value=header)
        cell.font = bold_font
        cell.alignment = alignment_center
        cell.border = border
        if header == '인원':
            cell.fill = personnel_fill
        elif header == '전체 합계':
            cell.fill = total_sum_fill
        elif header == '이른방 합계':
            cell.fill = early_sum_fill
        elif header == '늦은방 합계':
            cell.fill = late_sum_fill
        elif header == '당직 합계':
            cell.fill = duty_sum_fill
        elif any(slot in header for slot in time_slots if slot.startswith('8:30')):
            cell.fill = slot_830_fill
        elif any(slot in header for slot in time_slots if slot.startswith('9:00')):
            cell.fill = slot_900_fill
        elif any(slot in header for slot in time_slots if slot.startswith('9:30')):
            cell.fill = slot_930_fill
        elif any(slot in header for slot in time_slots if slot.startswith('10:00')):
            cell.fill = slot_1000_fill
        elif any(slot in header for slot in time_slots if slot.startswith('13:30')):
            cell.fill = slot_1330_fill

    for row_idx, row in enumerate(stats_df.values, 2):
        for col_idx, value in enumerate(row, 1):
            cell = stats_sheet.cell(row=row_idx, column=col_idx, value=value)
            cell.font = default_font
            cell.alignment = alignment_center
            cell.border = border
            header = headers[col_idx - 1]
            if header == '인원':
                cell.font = bold_font
                cell.fill = personnel_fill

    stats_sheet.column_dimensions['A'].width = 8
    for col in range(2, len(headers) + 1):
        stats_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 10

    output_stream = BytesIO()
    output_wb.save(output_stream)
    output_stream.seek(0)

    today = datetime.today().strftime("%Y-%m-%d")
    st.divider()
    st.write("### 결과 다운로드")
    st.write("- 통합 배치 결과, 인원별 전체 통계 엑셀 파일을 다운로드합니다.")
    st.download_button(
        label="다운로드",
        data=output_stream,
        file_name=f"{today}_내시경실배정.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
