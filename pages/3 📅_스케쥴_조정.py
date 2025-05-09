import re
import streamlit as st
import pandas as pd
import gspread
from streamlit_calendar import calendar as st_calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import time
import random

# 세션 상태 초기화
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False
if "df_schedule" not in st.session_state:
    st.session_state["df_schedule"] = None
if "df_schedule_md" not in st.session_state:
    st.session_state["df_schedule_md"] = None

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    st.write(f"Google Sheets 업로드 시도: {worksheet.title}, 데이터 크기: {len(data)}행, {len(data[0])}열")
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='RAW')
            worksheet.resize(rows=len(data), cols=len(data[0]))
            st.write(f"업로드 성공: 시도 {attempt+1}/{retries}")
            return True
        except Exception as e:
            error_msg = str(e)
            if "Quota exceeded" in error_msg:
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.error(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {error_msg}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")
    return False

# Google Sheets 저장 함수
def save_to_google_sheets(df, month_str):
    if df.empty or df.shape[0] == 0:
        st.error("데이터프레임이 비어 있습니다. 저장할 데이터가 없습니다.")
        return False
    
    st.write(f"저장 시작: {month_str} 스케쥴, 데이터프레임 크기: {df.shape}")
    st.write("데이터프레임 미리보기:", df.head())
    
    gc = get_gspread_client()
    try:
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return False
    
    try:
        try:
            worksheet = sheet.worksheet(f"{month_str} 스케쥴")
            st.write(f"기존 시트 발견: {month_str} 스케쥴")
        except gspread.exceptions.WorksheetNotFound:
            st.write(f"새 시트 생성: {month_str} 스케쥴")
            worksheet = sheet.add_worksheet(title=f"{month_str} 스케쥴", rows=max(100, df.shape[0] + 10), cols=max(50, df.shape[1] + 10))
        
        expected_cols = ['날짜', '요일'] + [str(i) for i in range(1, 18)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 11)]
        df_ordered = df.reindex(columns=[col for col in expected_cols if col in df.columns or col in expected_cols], fill_value='')
        
        data = [df_ordered.columns.tolist()] + df_ordered.values.tolist()
        
        success = update_sheet_with_retry(worksheet, data)
        if success:
            st.write(f"Google Sheets 업로드 완료: {month_str} 스케쥴")
        return success
    except Exception as e:
        st.error(f"Google Sheets 저장 실패: {str(e)}")
        return False

# 데이터 로드 (캐싱 사용)
def load_data_page6(month_str):
    st.cache_data.clear()
    return load_data_page3plus_no_cache(month_str)

# 데이터 로드 (캐싱 미사용)
def load_data_page3plus_no_cache(month_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    
    try:
        worksheet_schedule = sheet.worksheet(f"{month_str} 스케쥴")
        df_schedule = pd.DataFrame(worksheet_schedule.get_all_records())
        st.write(f"데이터 로드 성공: {month_str} 스케쥴, 크기: {df_schedule.shape}")
    except gspread.exceptions.WorksheetNotFound:
        st.write(f"시트 없음: {month_str} 스케쥴, 빈 데이터프레임 생성")
        df_schedule = pd.DataFrame(columns=['날짜', '요일'] + [str(i) for i in range(1, 13)] + ['오전당직(온콜)'] + [f'오후{i}' for i in range(1, 6)])
    except Exception as e:
        st.error(f"스케줄 시트를 불러오는 데 실패: {e}")
        st.stop()
    
    st.session_state["df_schedule"] = df_schedule
    st.session_state["data_loaded"] = True
    return df_schedule

# df_schedule_md 생성
def create_df_schedule_md(df_schedule):
    df_schedule_md = df_schedule.copy().fillna('')
    morning_cols = [str(i) for i in range(1, 13)]  # 1~12
    afternoon_cols = ['오후1', '오후2', '오후3', '오후4', '오후5']
    
    for idx, row in df_schedule_md.iterrows():
        date_str = row['날짜']
        try:
            if isinstance(date_str, (float, int)):
                date_str = str(int(date_str))
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025) if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            st.error(f"날짜 파싱 오류: {date_str}, 오류: {str(e)}")
            continue
        
        # 오전 근무자 처리
        morning_workers = [row.get(col, '') for col in morning_cols if row.get(col, '')]
        if len(morning_workers) > 12:
            morning_workers = morning_workers[:12]
        morning_workers.extend([''] * (12 - len(morning_workers)))
        for i, col in enumerate(morning_cols):
            df_schedule_md.at[idx, col] = morning_workers[i]
        
        # 오후 근무자 처리
        afternoon_workers = [row.get(col, '') for col in afternoon_cols if row.get(col, '')]
        if len(afternoon_workers) > 5:
            afternoon_workers = afternoon_workers[:5]
        afternoon_workers.extend([''] * (5 - len(afternoon_workers)))
        for i, col in enumerate(afternoon_cols):
            df_schedule_md.at[idx, col] = afternoon_workers[i]
    
    st.write(f"df_schedule_md 생성 완료: 크기 {df_schedule_md.shape}")
    return df_schedule_md

# df_schedule을 캘린더 이벤트로 변환
def df_schedule_to_events(df_schedule, shift_type="morning"):
    events = []
    morning_cols = [str(i) for i in range(1, 18)]  # 최대 17열
    afternoon_cols = [f'오후{i}' for i in range(1, 11)]  # 최대 10열
    
    for idx, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            if "월" in date_str:
                date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
            else:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            st.warning(f"날짜 파싱 실패: {date_str}, 행 {idx} 건너뜀")
            continue
            
        if shift_type == "morning":
            workers = [row.get(col, '') for col in morning_cols if col in row and pd.notna(row.get(col, '')) and row.get(col, '')]
            time_slot = "08:00-12:00"
            color = "#28a745"  # 오전: 초록색
        else:
            workers = [row.get(col, '') for col in afternoon_cols if col in row and pd.notna(row.get(col, '')) and row.get(col, '')]
            time_slot = "13:00-17:00"
            color = "#007bff"  # 오후: 파란색
        
        for worker in workers:
            events.append({
                "title": worker,
                "start": f"{date_obj}T{time_slot.split('-')[0]}",
                "end": f"{date_obj}T{time_slot.split('-')[1]}",
                "color": color,
                "resourceId": worker,
                "editable": True
            })
    
    st.write(f"캘린더 이벤트 생성: {len(events)}개 이벤트")
    return events

# 이벤트로부터 df_schedule 업데이트
def update_schedule_from_events(events, df_schedule, shift_type):
    if not events:
        st.warning("업데이트할 이벤트가 없습니다. 원본 스케쥴을 유지합니다.")
        return df_schedule
    
    df_schedule_updated = df_schedule.fillna('').copy()
    morning_cols = [str(i) for i in range(1, 18)]  # 오전 최대 17열
    afternoon_cols = [f'오후{i}' for i in range(1, 11)]  # 오후 최대 10열
    target_cols = morning_cols if shift_type == "morning" else afternoon_cols
    max_workers = 12 if shift_type == "morning" else 5
    shift_name = "오전" if shift_type == "morning" else "오후"
    
    # 원본 스케쥴에서 날짜별 근무자 매핑
    date_workers = {}
    for idx, row in df_schedule_updated.iterrows():
        date_str = row['날짜']
        try:
            date_obj = datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date() if "월" in date_str else datetime.strptime(date_str, '%Y-%m-%d').date()
            workers = [row.get(col, '') for col in target_cols if col in row and row.get(col, '')]
            date_workers[date_obj] = workers
        except ValueError:
            continue
    
    # 임시 저장 및 스왑 로그
    temp_date_workers = date_workers.copy()
    swap_log = set()
    processed_moves = set()
    
    # 추가 열 동적 추가
    for col in target_cols[max_workers:]:
        if col not in df_schedule_updated.columns:
            df_schedule_updated[col] = ''
            st.write(f"추가 열 생성: {col}")
    
    # 날짜별 이벤트 그룹화
    event_groups = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        date_str = event.get('start', '').split('T')[0]
        if not date_str:
            continue
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        worker = event.get('title', '')
        if not worker:
            continue
        if date_obj not in event_groups:
            event_groups[date_obj] = set()
        event_groups[date_obj].add(worker)
    
    # 이벤트 로그
    st.write(f"탐지된 이벤트 그룹: {event_groups}")
    
    # 이동 이벤트 탐지
    moves = []
    for date_obj, event_workers in event_groups.items():
        orig_workers = set(temp_date_workers.get(date_obj, []))
        new_workers = event_workers - orig_workers  # 새로 추가된 근무자
        removed_workers = orig_workers - event_workers  # 제거된 근무자
        
        for worker in new_workers:
            # 원래 날짜 찾기
            orig_date = None
            for d, workers in temp_date_workers.items():
                if worker in workers and d != date_obj and (worker, date_obj, d) not in processed_moves:
                    orig_date = d
                    break
            if orig_date:
                moves.append((worker, date_obj, orig_date))
                processed_moves.add((worker, date_obj, orig_date))
        
        for worker in removed_workers:
            # 새로운 날짜 찾기
            new_date = None
            for d, workers in event_groups.items():
                if worker in workers and d != date_obj and (worker, d, date_obj) not in processed_moves:
                    new_date = d
                    break
            if new_date:
                moves.append((worker, new_date, date_obj))
                processed_moves.add((worker, new_date, date_obj))
    
    # 교환 쌍 탐지
    swap_pairs = []
    single_moves = []
    processed_workers = set()
    for worker, new_date, orig_date in moves:
        if worker in processed_workers:
            continue
        swap_worker = None
        for w, w_new_date, w_orig_date in moves:
            if w_new_date == orig_date and w_orig_date == new_date and w != worker and w not in processed_workers:
                swap_worker = w
                break
        if swap_worker:
            swap_pairs.append((worker, new_date, swap_worker, orig_date))
            processed_workers.add(worker)
            processed_workers.add(swap_worker)
        else:
            single_moves.append((worker, new_date, orig_date))
            processed_workers.add(worker)
    
    # 교환 쌍 및 단일 이동 로그
    st.write(f"탐지된 교환 쌍: {swap_pairs}")
    st.write(f"탐지된 단일 이동: {single_moves}")
    
    # 교환 처리
    for worker, new_date, swap_worker, orig_date in swap_pairs:
        row_idx = df_schedule_updated[df_schedule_updated['날짜'].apply(
            lambda x: datetime.strptime(x, '%m월 %d일').replace(year=2025).date() 
            if "월" in x else datetime.strptime(x, '%Y-%m-%d').date()
        ) == new_date].index
        
        if row_idx.empty:
            st.warning(f"날짜 {new_date.strftime('%Y-%m-%d')}에 해당하는 행이 없습니다.")
            continue
        row_idx = row_idx[0]
        
        current_workers = temp_date_workers.get(new_date, []).copy()
        if worker not in current_workers and swap_worker in current_workers:
            current_workers[current_workers.index(swap_worker)] = worker
            swap_log.add((worker, new_date.strftime('%m월 %d일')))
            
            orig_workers = temp_date_workers.get(orig_date, []).copy()
            if worker in orig_workers:
                orig_workers[orig_workers.index(worker)] = swap_worker
                temp_date_workers[orig_date] = orig_workers
                swap_log.add((swap_worker, orig_date.strftime('%m월 %d일')))
                
                orig_row_idx = df_schedule_updated[df_schedule_updated['날짜'].apply(
                    lambda x: datetime.strptime(x, '%m월 %d일').replace(year=2025).date() 
                    if "월" in x else datetime.strptime(x, '%Y-%m-%d').date()
                ) == orig_date].index
                if not orig_row_idx.empty:
                    orig_row_idx = orig_row_idx[0]
                    for col in target_cols:
                        df_schedule_updated.at[orig_row_idx, col] = ''
                    for j, w in enumerate(orig_workers):
                        if j < len(target_cols):
                            df_schedule_updated.at[orig_row_idx, target_cols[j]] = w
                
                temp_date_workers[new_date] = current_workers
                for col in target_cols:
                    df_schedule_updated.at[row_idx, col] = ''
                for i, w in enumerate(current_workers):
                    if i < len(target_cols):
                        df_schedule_updated.at[row_idx, target_cols[i]] = w
    
    # 단일 이동 처리
    for worker, new_date, orig_date in single_moves:
        if worker in processed_workers:
            continue
        row_idx = df_schedule_updated[df_schedule_updated['날짜'].apply(
            lambda x: datetime.strptime(x, '%m월 %d일').replace(year=2025).date() 
            if "월" in x else datetime.strptime(x, '%Y-%m-%d').date()
        ) == new_date].index
        
        if row_idx.empty:
            st.warning(f"날짜 {new_date.strftime('%Y-%m-%d')}에 해당하는 행이 없습니다.")
            continue
        row_idx = row_idx[0]
        
        current_workers = temp_date_workers.get(new_date, []).copy()
        if worker not in current_workers:
            if '' in current_workers:
                current_workers[current_workers.index('')] = worker
            else:
                current_workers.append(worker)
            swap_log.add((worker, new_date.strftime('%m월 %d일')))
            
            orig_workers = temp_date_workers.get(orig_date, []).copy()
            if worker in orig_workers:
                orig_workers[orig_workers.index(worker)] = ''
                temp_date_workers[orig_date] = orig_workers
                
                orig_row_idx = df_schedule_updated[df_schedule_updated['날짜'].apply(
                    lambda x: datetime.strptime(x, '%m월 %d일').replace(year=2025).date() 
                    if "월" in x else datetime.strptime(x, '%Y-%m-%d').date()
                ) == orig_date].index
                if not orig_row_idx.empty:
                    orig_row_idx = orig_row_idx[0]
                    for col in target_cols:
                        df_schedule_updated.at[orig_row_idx, col] = ''
                    for j, w in enumerate(orig_workers):
                        if j < len(target_cols):
                            df_schedule_updated.at[orig_row_idx, target_cols[j]] = w
                
                temp_date_workers[new_date] = current_workers
                for col in target_cols:
                    df_schedule_updated.at[row_idx, col] = ''
                for i, w in enumerate(current_workers):
                    if i < len(target_cols):
                        df_schedule_updated.at[row_idx, target_cols[i]] = w
        processed_workers.add(worker)
    
    # 근무자 수 체크 및 알림
    for date_obj, workers in temp_date_workers.items():
        num_workers = len([w for w in workers if w])
        if num_workers != max_workers and num_workers != 0:  # 0명은 빈 날짜로 간주
            st.warning(f"{date_obj.strftime('%m월 %d일')} {shift_name} 근무자가 총 {num_workers}명입니다. 배정을 마쳐주세요.")
    
    # 추가 열 제거
    cols_to_drop = [col for col in target_cols[max_workers:] if col in df_schedule_updated.columns and all(df_schedule_updated[col] == '')]
    if cols_to_drop:
        df_schedule_updated.drop(columns=cols_to_drop, inplace=True)
    
    # 스왑 알림 출력
    for worker, date in swap_log:
        st.info(f"{date}에 {worker} 근무가 수정되었습니다.")
    
    st.write(f"df_schedule_md 업데이트 완료: 크기 {df_schedule_updated.shape}")
    st.write("업데이트된 df_schedule_md 미리보기:", df_schedule_updated.head())
    return df_schedule_updated

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

# 데이터 로드 호출
df_schedule = load_data_page6(month_str)
st.session_state["df_schedule"] = df_schedule

# df_schedule_md 초기화
if "df_schedule_md" not in st.session_state or st.session_state["df_schedule_md"] is None:
    st.session_state["df_schedule_md"] = create_df_schedule_md(df_schedule)

# 새로고침 버튼
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    df_schedule = load_data_page3plus_no_cache(month_str)
    st.session_state["df_schedule"] = df_schedule
    st.session_state["df_schedule_md"] = create_df_schedule_md(df_schedule)
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 메인 앱 로직
st.header(f"📅 {month_str} 스케쥴표", divider='rainbow')
st.write(" ")

# 시간대 선택
shift_type = st.selectbox("시간대 선택", ["morning", "afternoon"], format_func=lambda x: "오전" if x == "morning" else "오후")

# 캘린더 이벤트 생성
events = df_schedule_to_events(st.session_state["df_schedule_md"], shift_type)

# 캘린더 옵션
calendar_options = {
    "editable": True,
    "selectable": True,
    "initialView": "dayGridMonth",
    "initialDate": "2025-04-01",
    "events": events,
    "eventClick": "function(info) { alert('근무자: ' + info.event.title + '\\n날짜: ' + info.event.start.toISOString().split('T')[0]); }",
    "eventDrop": "function(info) { alert('스케쥴이 이동되었습니다: ' + info.event.title + ' -> ' + info.event.start.toISOString().split('T')[0]); }",
    "dayHeaderFormat": {"weekday": "short"},
    "themeSystem": "bootstrap",
    "height": 500,
    "headerToolbar": {"left": "", "center": "", "right": ""},
    "showNonCurrentDates": False,
    "fixedWeekCount": False,
    "eventOrder": "source",
    "displayEventTime": False
}

# 캘린더 렌더링
state = st_calendar(
    events=events,
    options=calendar_options,
    custom_css="""
    .fc-event-past {
        opacity: 0.8;
    }
    .fc-event-title {
        font-weight: 700;
    }
    .fc-toolbar-title {
        font-size: 2rem;
    }
    """,
    key=f"calendar_{shift_type}"
)

# 캘린더 인터랙션 처리
if state.get("eventsSet"):
    updated_events = state["eventsSet"]
    events_list = updated_events.get("events", []) if isinstance(updated_events, dict) else updated_events
    
    if isinstance(events_list, list) and (not events_list or isinstance(events_list[0], dict)):
        if events_list:
            st.write(f"캘린더 이벤트 처리 시작: {len(events_list)}개 이벤트")
            st.write("전체 이벤트:", events_list)
            st.session_state["df_schedule_md"] = update_schedule_from_events(events_list, st.session_state["df_schedule_md"], shift_type)
            st.write("캘린더 조정 완료, 저장 버튼을 눌러 Google Sheets에 반영하세요.")
        else:
            st.warning("빈 이벤트 리스트입니다. 스케쥴을 업데이트하지 않습니다.")
    else:
        st.error(f"유효하지 않은 events 리스트 형식: {events_list}")

# 저장 버튼
if st.button("💾 저장"):
    success = save_to_google_sheets(st.session_state["df_schedule_md"], month_str)
    if success:
        st.success("스케쥴이 성공적으로 저장되었습니다.")
    else:
        st.error("스케쥴 저장에 실패했습니다. 다시 시도해주세요.")