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

# 세션 상태 초기화
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False
if "df_room_fix" not in st.session_state:
    st.session_state["df_room_fix"] = pd.DataFrame(columns=["이름", "분류", "날짜정보", "우선순위"])

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# 데이터 로드
@st.cache_data
def load_data(month_str):
    start_time = time.time()
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    
    try:
        worksheet_schedule = sheet.worksheet(f"{month_str} 스케쥴")
        df_schedule = pd.DataFrame(worksheet_schedule.get_all_records())
    except Exception as e:
        st.error(f"스케줄 시트를 불러오는 데 실패: {e}")
        st.stop()
    
    try:
        worksheet_request = sheet.worksheet(f"{month_str} 방배정 요청")
        df_room_request = pd.DataFrame(worksheet_request.get_all_records())
    except:
        worksheet_request = sheet.add_worksheet(f"{month_str} 방배정 요청", rows=100, cols=4)
        worksheet_request.append_row(["이름", "분류", "날짜정보", "우선순위"])
        df_room_request = pd.DataFrame(columns=["이름", "분류", "날짜정보", "우선순위"])
    
    st.session_state["df_schedule"] = df_schedule
    st.session_state["df_room_request"] = df_room_request
    st.session_state["worksheet_request"] = worksheet_request
    st.session_state["data_loaded"] = True
    
    return df_schedule, df_room_request, worksheet_request

# 근무 가능 일자 계산
@st.cache_data
def get_user_available_dates(name, df_schedule, month_start, month_end):
    start_time = time.time()
    available_dates = []
    for _, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            if "월" in date_str:
                date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            continue
        if month_start <= date_obj <= month_end and row['요일'] not in ['토요일', '일요일']:
            personnel = [row[str(i)] for i in range(1, 13)] + [row[f'오후{i}'] for i in range(1, 5)]
            if name in personnel:
                available_dates.append((date_obj, f"{date_str}({row['요일']}) 오전"))
                available_dates.append((date_obj, f"{date_str}({row['요일']}) 오후"))
    
    # 날짜순으로 정렬 (date_obj 기준)
    available_dates.sort(key=lambda x: x[0])
    # 정렬된 날짜 문자열 반환
    sorted_dates = [date_str for _, date_str in available_dates]
    
    return sorted_dates

# 요청 저장 (df_room_request용)
def save_to_gsheet(name, categories, dates, month_str, worksheet):
    start_time = time.time()
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    worksheet = sheet.worksheet(f"{month_str} 방배정 요청")
    df = pd.DataFrame(worksheet.get_all_records())
    
    new_rows = []
    for date in dates:
        for idx, cat in enumerate(categories):
            new_rows.append({"이름": name, "분류": cat, "날짜정보": date, "우선순위": 1.0 if idx == 0 else 0.5})
    
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    worksheet.clear()
    worksheet.update([df.columns.tolist()] + df.values.tolist())
    st.write(f"save_to_gsheet 실행 시간: {time.time() - start_time:.2f}초")
    return df

# df_schedule_md 생성
def create_df_schedule_md(df_schedule):
    start_time = time.time()
    df_schedule_md = df_schedule.copy().fillna('')
    for idx, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        oncall_worker = row['오전당직(온콜)']
        
        try:
            if isinstance(date_str, (float, int)):
                date_str = str(int(date_str))
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025) if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            st.error(f"날짜 파싱 오류: {date_str}, 오류: {str(e)}")
            continue
        
        afternoon_cols = ['오후1', '오후2', '오후3', '오후4', '오후5']
        if all(row[col] == '' for col in afternoon_cols):
            df_schedule_md.at[idx, '오전당직(온콜)'] = ''
            continue
        
        if pd.isna(oncall_worker) or oncall_worker == '':
            oncall_worker = ''
            df_schedule_md.at[idx, '오전당직(온콜)'] = ''
        
        if oncall_worker:
            morning_cols = [str(i) for i in range(1, 13)]
            for col in morning_cols + afternoon_cols:
                if row[col] == oncall_worker:
                    df_schedule_md.at[idx, col] = ''
        
        morning_cols = [str(i) for i in range(1, 13)]
        morning_workers = [row[col] for col in morning_cols if row[col]]
        if len(morning_workers) > 11:
            morning_workers = morning_workers[:11]
        morning_workers.extend([''] * (11 - len(morning_workers)))
        for i, col in enumerate([str(i) for i in range(1, 12)], 1):
            df_schedule_md.at[idx, col] = morning_workers[i-1]
        
        afternoon_workers = [row[col] for col in afternoon_cols if row[col]]
        if len(afternoon_workers) > 4:
            afternoon_workers = afternoon_workers[:4]
        afternoon_workers.extend([''] * (4 - len(afternoon_workers)))
        for i, col in enumerate(['오후1', '오후2', '오후3', '오후4'], 1):
            df_schedule_md.at[idx, col] = afternoon_workers[i-1]
    
    df_schedule_md = df_schedule_md.drop(columns=['12', '오후5'], errors='ignore')
    return df_schedule_md

def random_assign(personnel, slots, fixed_assignments, priority_assignments, time_groups, max_early, max_late, max_duty, max_room, total_stats, morning_personnel, afternoon_personnel):
    start_time = time.time()
    best_assignment = None
    min_violations = float('inf')
    
    early_slots = [s for s in slots if s.startswith('8:30') and '_당직' not in s]
    late_slots = [s for s in slots if s.startswith('10:00')]
    duty_slots = [s for s in slots if s.startswith('13:30') and '_당직' in s]
    morning_slots_830 = [s for s in slots if s.startswith('8:30') and '_당직' not in s]
    morning_slots_900 = [s for s in slots if s.startswith('9:00')]
    morning_slots_930 = [s for s in slots if s.startswith('9:30')]
    morning_slots_1000 = [s for s in slots if s.startswith('10:00')]
    afternoon_slots = [s for s in slots if s.startswith('13:30') and '_당직' not in s]
    
    daily_stats = {
        'early': Counter(),
        'late': Counter(),
        'duty': Counter(),
        'rooms': {str(i): Counter() for i in range(1, 13)}
    }
    
    for _ in range(50):
        assignment = [None] * len(slots)
        assigned_counts = Counter()
        available_slots = list(range(len(slots)))
        
        # 고정 배치 적용
        for slot, person in fixed_assignments.items():
            if slot in slots and person in personnel:
                slot_idx = slots.index(slot)
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                available_slots.remove(slot_idx)
                room_num = re.search(r'\((\d+)\)', slot).group(1)
                daily_stats['rooms'][room_num][person] += 1
                if slot in early_slots:
                    daily_stats['early'][person] += 1
                elif slot in late_slots:
                    daily_stats['late'][person] += 1
                elif slot in duty_slots:
                    daily_stats['duty'][person] += 1
        
        # 우선 배치 적용
        priority_pairs = [(slot, person, score) for (slot, person), score in priority_assignments.items() if slot in slots and person in personnel]
        priority_pairs.sort(key=lambda x: -x[2])
        for slot, person, _ in priority_pairs:
            slot_idx = slots.index(slot)
            if slot_idx in available_slots:
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                available_slots.remove(slot_idx)
                room_num = re.search(r'\((\d+)\)', slot).group(1)
                daily_stats['rooms'][room_num][person] += 1
                if slot in early_slots:
                    daily_stats['early'][person] += 1
                elif slot in late_slots:
                    daily_stats['late'][person] += 1
                elif slot in duty_slots:
                    daily_stats['duty'][person] += 1
        
        # 오전 배정
        morning_slots_all = morning_slots_830 + morning_slots_900 + morning_slots_930 + morning_slots_1000
        random.shuffle(morning_slots_all)
        morning_indices = [slots.index(slot) for slot in morning_slots_all if slots.index(slot) in available_slots]
        morning_personnel_list = morning_personnel.copy()
        random.shuffle(morning_personnel_list)
        
        for slot_idx in morning_indices:
            slot = slots[slot_idx]
            for person in morning_personnel_list:
                room_num = re.search(r'\((\d+)\)', slot).group(1)
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                available_slots.remove(slot_idx)
                if slot in early_slots:
                    daily_stats['early'][person] += 1
                elif slot in late_slots:
                    daily_stats['late'][person] += 1
                daily_stats['rooms'][room_num][person] += 1
                morning_personnel_list.remove(person)  # 중복 배정 허용, 리스트에서 제거
                break
            else:
                if morning_personnel_list:
                    person = random.choice(morning_personnel_list)
                    room_num = re.search(r'\((\d+)\)', slot).group(1)
                    assignment[slot_idx] = person
                    assigned_counts[person] += 1
                    available_slots.remove(slot_idx)
                    if slot in early_slots:
                        daily_stats['early'][person] += 1
                    elif slot in late_slots:
                        daily_stats['late'][person] += 1
                    daily_stats['rooms'][room_num][person] += 1
                    morning_personnel_list.remove(person)
        
        # 오후 배정
        afternoon_indices = [i for i in available_slots if slots[i] in afternoon_slots]
        afternoon_personnel_list = afternoon_personnel.copy()
        random.shuffle(afternoon_personnel_list)
        for slot_idx in afternoon_indices:
            for person in afternoon_personnel_list:
                room_num = re.search(r'\((\d+)\)', slots[slot_idx]).group(1)
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                available_slots.remove(slot_idx)
                daily_stats['rooms'][room_num][person] += 1
                afternoon_personnel_list.remove(person)
                break
            else:
                if afternoon_personnel_list:
                    person = random.choice(afternoon_personnel_list)
                    room_num = re.search(r'\((\d+)\)', slots[slot_idx]).group(1)
                    assignment[slot_idx] = person
                    assigned_counts[person] += 1
                    available_slots.remove(slot_idx)
                    daily_stats['rooms'][room_num][person] += 1
                    afternoon_personnel_list.remove(person)
        
        # 당직 배정
        duty_indices = [i for i in available_slots if slots[i] in duty_slots]
        for slot_idx in duty_indices:
            for person in afternoon_personnel_list:
                room_num = re.search(r'\((\d+)\)', slots[slot_idx]).group(1)
                assignment[slot_idx] = person
                assigned_counts[person] += 1
                available_slots.remove(slot_idx)
                daily_stats['duty'][person] += 1
                daily_stats['rooms'][room_num][person] += 1
                afternoon_personnel_list.remove(person)
                break
            else:
                if afternoon_personnel_list:
                    person = random.choice(afternoon_personnel_list)
                    room_num = re.search(r'\((\d+)\)', slots[slot_idx]).group(1)
                    assignment[slot_idx] = person
                    assigned_counts[person] += 1
                    available_slots.remove(slot_idx)
                    daily_stats['duty'][person] += 1
                    daily_stats['rooms'][room_num][person] += 1
                    afternoon_personnel_list.remove(person)
        
        # 제약 위반 계산
        violations = 0
        for person in set(personnel):
            if total_stats['early'][person] + daily_stats['early'][person] > max_early:
                violations += (total_stats['early'][person] + daily_stats['early'][person]) - max_early
            if total_stats['late'][person] + daily_stats['late'][person] > max_late:
                violations += (total_stats['late'][person] + daily_stats['late'][person]) - max_late
            if total_stats['duty'][person] + daily_stats['duty'][person] > max_duty:
                violations += (total_stats['duty'][person] + daily_stats['duty'][person]) - max_duty
            for room in daily_stats['rooms']:
                if total_stats['rooms'][room][person] + daily_stats['rooms'][room][person] > max_room:
                    violations += (total_stats['rooms'][room][person] + daily_stats['rooms'][room][person]) - max_room
        
        if violations < min_violations:
            min_violations = violations
            best_assignment = assignment.copy()
        if all(assignment[i] is not None for i in range(len(slots))):
            break
    
    # 디버깅
    total_morning_slots = len(morning_slots_830 + morning_slots_900 + morning_slots_930 + morning_slots_1000)
    st.write(f"오전 배정: {len([a for a in best_assignment if a is not None and slots[best_assignment.index(a)] in morning_slots_all])}/{total_morning_slots}, 인원: {set(best_assignment)}")
    st.write(f"오후 배정: {len([a for a in best_assignment if a is not None and slots[best_assignment.index(a)] in afternoon_slots + duty_slots])}/{len(afternoon_slots) + len(duty_slots)}, 인원: {set(best_assignment)}")
    
    return best_assignment, daily_stats

# 메인
month_str = "2025년 04월"
next_month_start = date(2025, 4, 1)
next_month_end = date(2025, 4, 30)

# 로그인 체크
if "login_success" not in st.session_state or not st.session_state["login_success"]:
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# 사이드바
st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
if st.sidebar.button("로그아웃"):
    st.session_state.clear()
    st.success("로그아웃되었습니다.")
    st.rerun()

# 최대 배정 한계
st.sidebar.header("최대 배정 한계 설정")
MAX_DUTY = st.sidebar.number_input("최대 당직 합계", min_value=1, value=3, step=1)
MAX_EARLY = st.sidebar.number_input("최대 이른방 합계", min_value=1, value=6, step=1)
MAX_LATE = st.sidebar.number_input("최대 늦은방 합계", min_value=1, value=6, step=1)
MAX_ROOM = st.sidebar.number_input("최대 방별 합계", min_value=1, value=3, step=1)

# 데이터 로드
df_schedule, df_room_request, worksheet_request = load_data(month_str)
df_room_fix = st.session_state["df_room_fix"]

# 근무자 명단
st.subheader("📋 근무자 명단")
df_schedule_md = create_df_schedule_md(df_schedule)
st.dataframe(df_schedule_md)

# 방 설정 UI
st.divider()
st.subheader("📋 방 설정")
room_options = [str(i) for i in range(1, 13)]
time_slots = {}
time_groups = {}
memo_rules = {}
with st.form("room_form"):
    # 8:30 시간대
    st.markdown("**🔷 8:30 시간대**")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        num_830 = st.number_input("방 개수", min_value=0, value=4, key="830_rooms")
    with col2:
        rooms_830 = st.multiselect("방 번호", room_options, default=['1', '2', '4', '7'], max_selections=num_830, key="830_room_select")
    with col3:
        duty_830 = st.selectbox("당직방", rooms_830, index=0, key="830_duty")
    
    # 9:00 시간대
    st.markdown("**🔷 9:00 시간대**")
    col1, col2 = st.columns([1, 3])
    with col1:
        num_900 = st.number_input("방 개수", min_value=0, value=3, key="900_rooms")
    with col2:
        rooms_900 = st.multiselect("방 번호", room_options, default=['10', '11', '12'], max_selections=num_900, key="900_room_select")
    
    # 9:30 시간대
    st.markdown("**🔷 9:30 시간대**")
    col1, col2 = st.columns([1, 3])
    with col1:
        num_930 = st.number_input("방 개수", min_value=0, value=3, key="930_rooms")
    with col2:
        rooms_930 = st.multiselect("방 번호", room_options, default=['5', '6', '8'], max_selections=num_930, key="930_room_select")
    
    # 10:00 시간대
    st.markdown("**🔷 10:00 시간대**")
    col1, col2 = st.columns([1, 3])
    with col1:
        num_1000 = st.number_input("방 개수", min_value=0, value=2, key="1000_rooms")
    with col2:
        rooms_1000 = st.multiselect("방 번호", room_options, default=['3', '9'], max_selections=num_1000, key="1000_room_select")
    
    # 13:30 시간대
    st.markdown("**🔶 13:30 시간대**")
    col1, col2 = st.columns([3, 1])
    with col1:
        num_1330 = 4
        rooms_1330 = st.multiselect("방 번호", room_options, default=['2', '3', '4', '9'], max_selections=num_1330, key="1330_room_select")
    with col2:
        duty_1330 = st.selectbox("당직방", rooms_1330, index=0, key="1330_duty")
    
    if st.form_submit_button("✅ 선택 완료"):
        if num_830 + num_900 + num_930 + num_1000 != 12:
            st.error("오전 방 개수 합계는 12여야 합니다.")
        elif len(rooms_830) != num_830 or len(rooms_900) != num_900 or len(rooms_930) != num_930 or len(rooms_1000) != num_1000 or len(rooms_1330) != num_1330:
            st.error("각 시간대의 방 번호 선택을 완료해주세요.")
        else:
            for room in rooms_830:
                slot = f"8:30({room})_당직" if room == duty_830 else f"8:30({room})"
                time_slots[slot] = len(time_slots)
                time_groups.setdefault('8:30', []).append(slot)
            for room in rooms_900:
                slot = f"9:00({room})"
                time_slots[slot] = len(time_slots)
                time_groups.setdefault('9:00', []).append(slot)
            for room in rooms_930:
                slot = f"9:30({room})"
                time_slots[slot] = len(time_slots)
                time_groups.setdefault('9:30', []).append(slot)
            for room in rooms_1000:
                slot = f"10:00({room})"
                time_slots[slot] = len(time_slots)
                time_groups.setdefault('10:00', []).append(slot)
            for room in rooms_1330:
                slot = f"13:30({room})_당직" if room == duty_1330 else f"13:30({room})"
                time_slots[slot] = len(time_slots)
                time_groups.setdefault('13:30', []).append(slot)
            
            memo_rules = {
                **{f'{i}번방': [s for s in time_slots if f'({i})' in s] for i in range(1, 12)},
                '이른방': [s for s in time_slots if s.startswith('8:30')],
                '당직 아닌 이른방': [s for s in time_slots if s.startswith('8:30') and '_당직' not in s],
                '8:30': [s for s in time_slots if s.startswith('8:30')],
                '9:00': [s for s in time_slots if s.startswith('9:00')],
                '9:30': [s for s in time_slots if s.startswith('9:30')],
                '10:00': [s for s in time_slots if s.startswith('10:00')]
            }
            
            st.session_state["time_slots"] = time_slots
            st.session_state["time_groups"] = time_groups
            st.session_state["memo_rules"] = memo_rules
            st.session_state["room_selection_confirmed"] = True
            st.success("방 선택 완료!")

# 고정 배치 입력 UI (기존 코드 유지)
st.divider()
st.subheader("🟢 고정 배치 입력")
요청분류 = ["1번방", "2번방", "3번방", "4번방", "5번방", "6번방", "7번방", "8번방", "9번방", "10번방", "11번방",
            "이른방", "당직 아닌 이른방", "8:30", "9:00", "9:30", "10:00"]
with st.form("fixed_form"):
    col1, col2, col3 = st.columns([2, 2, 3])
    with col1:
        name = st.selectbox("근무자", df_schedule.iloc[:, 2:].stack().dropna().unique())
    with col2:
        categories = st.multiselect("요청 분류", 요청분류)
    with col3:
        dates = st.multiselect("요청 일자", get_user_available_dates(name, df_schedule, next_month_start, next_month_end))
        
    if st.form_submit_button("추가"):
        new_rows = []
        for date in dates:
            for idx, cat in enumerate(categories):
                new_rows.append({"이름": name, "분류": cat, "날짜정보": date, "우선순위": 1.0 if idx == 0 else 0.5})
        df_room_fix = pd.concat([df_room_fix, pd.DataFrame(new_rows)], ignore_index=True)
        st.session_state["df_room_fix"] = df_room_fix
        st.success("고정 배치 저장 완료!")
    
    if not df_room_fix.empty:
        st.markdown("### 고정 배치 삭제")
        options = [f"{row['이름']} - {row['분류']} - {row['날짜정보']}" for _, row in df_room_fix.iterrows()]
        selected = st.multiselect("삭제할 항목", options)
        if st.form_submit_button("삭제"):
            indices = [i for i, opt in enumerate(options) if opt in selected]
            df_room_fix = df_room_fix.drop(indices).reset_index(drop=True)
            st.session_state["df_room_fix"] = df_room_fix
            st.success("선택한 고정 배치 삭제 완료!")

# 우선 배치 확인
st.divider()
st.subheader("✅ 우선 배치 확인")
if df_room_request.empty:
    st.info("☑️ 현재 우선 배치 요청이 없습니다.")
else:
    st.dataframe(df_room_request)

# df_room 생성 로직
if st.session_state.get("room_selection_confirmed") and st.button("방배정 시작"):
    random.seed(time.time())
    start_time = time.time()
    total_stats = {
        'early': Counter(),
        'late': Counter(),
        'duty': Counter(),
        'rooms': {str(i): Counter() for i in range(1, 13)}
    }
    
    assignments = {}
    slots = list(st.session_state["time_slots"].keys())
    assignable_slots = [s for s in slots if not (s.startswith('8:30') and s.endswith('_당직'))]
    
    date_cache = {}
    for _, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        try:
            if "월" in date_str:
                date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025)
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            date_cache[date_str] = date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    fixed_cells = {}
    priority_cells = {}
    for _, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        if date_str not in date_cache:
            continue
        
        formatted_date = date_cache[date_str]
        morning_personnel = [row[str(i)] for i in range(1, 12) if pd.notna(row[str(i)]) and row[str(i)]]
        afternoon_personnel = [row[f'오후{i}'] for i in range(1, 5) if pd.notna(row[f'오후{i}']) and row[f'오후{i}']]
        personnel = morning_personnel + afternoon_personnel
        
        if not personnel:
            assignments[formatted_date] = [None] * len(assignable_slots)
            continue
        
        if len(set(personnel)) < 15:
            st.warning(f"{date_str}: 인원 부족, 고유 인원 {len(set(personnel))}명, 필요 15명")
        
        fixed_assignments = {}
        if not df_room_fix.empty:
            filtered_fix = df_room_fix[df_room_fix['날짜정보'].str.contains(formatted_date, na=False)]
            for _, fix in filtered_fix.iterrows():
                for slot in st.session_state["memo_rules"].get(fix['분류'], []):
                    if slot in assignable_slots:
                        fixed_assignments[slot] = fix['이름']
                        fixed_cells[(formatted_date, slot)] = fix['분류']
        
        priority_assignments = {}
        if not df_room_request.empty:
            filtered_req = df_room_request[df_room_request['날짜정보'].str.contains(formatted_date, na=False)]
            for _, req in filtered_req.iterrows():
                for slot in st.session_state["memo_rules"].get(req['분류'], []):
                    if slot in assignable_slots:
                        priority_assignments[(slot, req['이름'])] = float(req.get('우선순위', 0.5))
                        priority_cells[(formatted_date, slot)] = req['분류']
        
        assignment, daily_stats = random_assign(
            personnel, assignable_slots, fixed_assignments, priority_assignments,
            st.session_state["time_groups"], MAX_EARLY, MAX_LATE, MAX_DUTY, MAX_ROOM, total_stats,
            morning_personnel, afternoon_personnel
        )
        assignments[formatted_date] = assignment
        
        for key in ['early', 'late', 'duty']:
            total_stats[key].update(daily_stats[key])
        for room in daily_stats['rooms']:
            total_stats['rooms'][room].update(daily_stats['rooms'][room])
    
    all_slots = ['8:30(1)_당직'] + \
                sorted([s for s in slots if s.startswith('8:30') and not s.endswith('_당직')]) + \
                sorted([s for s in slots if s.startswith('9:00')]) + \
                sorted([s for s in slots if s.startswith('9:30')]) + \
                sorted([s for s in slots if s.startswith('10:00')]) + \
                sorted([s for s in slots if s.startswith('13:30') and s.endswith('_당직')]) + \
                sorted([s for s in slots if s.startswith('13:30') and not s.endswith('_당직')]) + \
                ['온콜']
    columns = ['날짜', '요일'] + all_slots
    result_data = []
    
    for _, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        if date_str not in date_cache:
            continue
        
        formatted_date = date_cache[date_str]
        day_of_week = row['요일'] + "요일" if not row['요일'].endswith("요일") else row['요일']
        result_row = [date_str, day_of_week]
        
        personnel = [p for p in row[2:-1] if pd.notna(p) and p]
        has_person = bool(personnel)
        
        assignment = assignments.get(formatted_date, [None] * len(assignable_slots))
        for slot in all_slots:
            if slot == '8:30(1)_당직' or slot == '온콜':
                person = row['오전당직(온콜)'] if has_person else None
            else:
                person = assignment[assignable_slots.index(slot)] if slot in assignable_slots and assignment else None
            result_row.append(person if has_person else None)
        
        result_data.append(result_row)
    
    df_room = pd.DataFrame(result_data, columns=columns)
    st.subheader("통합 배치 결과")
    st.dataframe(df_room)
    
    # 통계 DataFrame
    stats_data = []
    all_personnel = set()
    for _, row in df_schedule_md.iterrows():
        personnel = [p for p in row[2:-1].dropna() if p]
        all_personnel.update(personnel)
    for person in sorted(all_personnel):
        stats_data.append({
            '인원': person,
            '이른방 합계': total_stats['early'][person],
            '늦은방 합계': total_stats['late'][person],
            '당직 합계': total_stats['duty'][person],
            **{f'{r}번방 합계': total_stats['rooms'][r][person] for r in total_stats['rooms']}
        })
    stats_df = pd.DataFrame(stats_data)
    st.subheader("인원별 통계")
    st.dataframe(stats_df)
    
    # 엑셀 다운로드
    wb = openpyxl.Workbook()
    sheet = wb.active
    sheet.title = "Schedule"
    
    for col_idx, header in enumerate(columns, 1):
        cell = sheet.cell(1, col_idx, header)
        cell.font = Font(bold=True, name="맑은 고딕", size=9)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        if header.startswith('8:30'):
            cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
        elif header.startswith('9:00'):
            cell.fill = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
        elif header.startswith('9:30'):
            cell.fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
        elif header.startswith('10:00'):
            cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
        elif header.startswith('13:30'):
            cell.fill = PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")
        elif header == '온콜':
            cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
    
    for row_idx, row in enumerate(result_data, 2):
        has_person = any(x for x in row[2:-1] if x is not None)  # 인원 유무 확인 (온콜 제외)
        formatted_date = date_cache[row[0]]
        for col_idx, value in enumerate(row, 1):
            cell = sheet.cell(row_idx, col_idx, value)
            if (columns[col_idx-1].endswith('_당직') or columns[col_idx-1] == '온콜') and value:
                cell.font = Font(name="맑은 고딕", size=9, bold=True, color="FF00FF")
            else:
                cell.font = Font(name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            if col_idx == 1:  # 날짜
                cell.fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
            elif not has_person and col_idx >= 2:  # 요일부터 온콜까지 회색 처리
                cell.fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
            elif col_idx == 2:  # 요일
                cell.fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
            
            # 고정/우선 배치 메모 추가
            if col_idx > 2 and value:  # 날짜, 요일 제외
                slot = columns[col_idx-1]
                if (formatted_date, slot) in fixed_cells:
                    cell.comment = Comment(f"고정 배치: {fixed_cells[(formatted_date, slot)]}", "System")
                elif (formatted_date, slot) in priority_cells:
                    cell.comment = Comment(f"우선 배치: {priority_cells[(formatted_date, slot)]}", "System")
    
    stats_sheet = wb.create_sheet("Stats")
    stats_columns = stats_df.columns.tolist()
    for col_idx, header in enumerate(stats_columns, 1):
        cell = stats_sheet.cell(1, col_idx, header)
        cell.font = Font(bold=True, name="맑은 고딕", size=9)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        if header == '인원':
            cell.fill = PatternFill(start_color="D0CECE", end_color="D0CECE", fill_type="solid")
        elif header == '이른방 합계':
            cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
        elif header == '늦은방 합계':
            cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
        elif header == '당직 합계':
            cell.fill = PatternFill(start_color="FF00FF", end_color="FF00FF", fill_type="solid")
    
    for row_idx, row in enumerate(stats_df.values, 2):
        for col_idx, value in enumerate(row, 1):
            cell = stats_sheet.cell(row_idx, col_idx, value)
            cell.font = Font(name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    st.subheader("결과 다운로드")
    st.download_button(
        label="다운로드",
        data=output,
        file_name=f"{datetime.today().strftime('%Y-%m-%d')}_내시경실배정.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    st.write(f"방배정 전체 실행 시간: {time.time() - start_time:.2f}초")