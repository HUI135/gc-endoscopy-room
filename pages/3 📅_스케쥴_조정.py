import re
import streamlit as st
import pandas as pd
import gspread
from streamlit_calendar import calendar as st_calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import time
import random
import hashlib

# 세션 상태 초기화
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False
if "df_schedule" not in st.session_state:
    st.session_state["df_schedule"] = None
if "last_events_hash" not in st.session_state:
    st.session_state["last_events_hash"] = None
if "processed_moves" not in st.session_state:
    st.session_state["processed_moves"] = set()
if "original_workers_by_date" not in st.session_state:
    st.session_state["original_workers_by_date"] = None
if "initial_events_hash" not in st.session_state:
    st.session_state["initial_events_hash"] = None
if "swap_log_history" not in st.session_state:
    st.session_state["swap_log_history"] = []

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
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='RAW')
            worksheet.resize(rows=len(data), cols=len(data[0]))
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

# Google Sheets 저장 함수 (스케쥴 데이터)
def save_to_google_sheets(df, month_str):
    if df.empty or df.shape[0] == 0:
        st.error("스케쥴 데이터프레임이 비어 있습니다. 저장할 데이터가 없습니다.")
        return False
    
    gc = get_gspread_client()
    try:
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return False
    
    try:
        try:
            worksheet = sheet.worksheet(f"{month_str} 스케쥴")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=f"{month_str} 스케쥴", rows=max(100, df.shape[0] + 10), cols=max(50, df.shape[1] + 10))
        
        # 필요한 열만 포함
        expected_cols = ['날짜', '요일', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '온콜', '오후1', '오후2', '오후3', '오후4', '오후5']
        df_ordered = df.reindex(columns=expected_cols, fill_value='')
        
        data = [df_ordered.columns.tolist()] + df_ordered.values.tolist()
        
        success = update_sheet_with_retry(worksheet, data)
        return success
    except Exception as e:
        st.error(f"Google Sheets 저장 실패: {str(e)}")
        return False

# Google Sheets에 조정사항 로그 저장
def save_to_google_sheets_log(log_entry, month_str):
    gc = get_gspread_client()
    try:
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return False
    
    try:
        try:
            worksheet = sheet.worksheet(f"{month_str} 스케쥴 조정사항")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=f"{month_str} 스케쥴 조정사항", rows=100, cols=2)
            worksheet.update('A1', [['Timestamp', '조정사항']])
        
        # 기존 로그 로드
        existing_logs = worksheet.get_all_values()[1:]  # 헤더 제외
        existing_log_entries = [row[1] for row in existing_logs if len(row) > 1]
        
        # 중복 로그 방지
        if log_entry not in existing_log_entries:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            worksheet.append_row([timestamp, log_entry], value_input_option='RAW')
        return True
    except Exception as e:
        st.error(f"Google Sheets 로그 저장 실패: {str(e)}")
        return False

# Google Sheets에서 조정사항 로그 로드
def load_swap_log_history(month_str):
    gc = get_gspread_client()
    try:
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return []
    
    try:
        try:
            worksheet = sheet.worksheet(f"{month_str} 스케쥴 조정사항")
        except gspread.exceptions.WorksheetNotFound:
            return []
        
        logs = worksheet.get_all_values()[1:]  # 헤더 제외
        log_entries = [row[1] for row in logs if len(row) > 1]
        return log_entries
    except Exception as e:
        st.error(f"Google Sheets 로그 로드 실패: {str(e)}")
        return []

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
        # 열 이름 정규화
        column_mapping = {
            '오전당직(온콜)': '온콜'
        }
        df_schedule.rename(columns=column_mapping, inplace=True)
        # 누락된 열 추가 및 데이터 정제
        expected_cols = ['날짜', '요일', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '온콜', '오후1', '오후2', '오후3', '오후4', '오후5']
        for col in expected_cols:
            if col not in df_schedule.columns:
                df_schedule[col] = ''
        df_schedule = df_schedule.reindex(columns=expected_cols, fill_value='')
        # 빈 값 및 NaN을 명시적 문자열로 대체
        df_schedule.fillna('', inplace=True)
    except gspread.exceptions.WorksheetNotFound:
        df_schedule = pd.DataFrame(columns=['날짜', '요일', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '온콜', '오후1', '오후2', '오후3', '오후4', '오후5'])
    except Exception as e:
        st.error(f"스케쥴 시트를 불러오는 데 실패: {e}")
        st.stop()
    
    st.session_state["df_schedule"] = df_schedule
    st.session_state["data_loaded"] = True
    
    # 원본 근무자 상태 저장 (오전/오후 구분)
    morning_cols = [str(i) for i in range(1, 13)]
    afternoon_cols = [f'오후{i}' for i in range(1, 6)]
    original_workers_by_date = {}
    for _, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date() if '-' in date_str else datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
        except Exception:
            continue
        original_workers_by_date[d] = {
            "morning": set([row.get(col, '') for col in morning_cols if row.get(col, '')]),
            "afternoon": set([row.get(col, '') for col in afternoon_cols if row.get(col, '')])
        }
    st.session_state["original_workers_by_date"] = original_workers_by_date
    
    # Google Sheets에서 조정사항 로그 로드
    st.session_state["swap_log_history"] = load_swap_log_history(month_str)
    
    return df_schedule

# df_schedule을 캘린더 이벤트로 변환
def df_schedule_to_events(df_schedule, shift_type="morning"):
    events = []
    morning_cols = [str(i) for i in range(1, 13)]
    afternoon_cols = [f'오후{i}' for i in range(1, 6)]
    
    for idx, row in df_schedule.iterrows():
        date_str = row['날짜']
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date() if '-' in date_str else datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
        except ValueError:
            st.warning(f"날짜 파싱 실패: {date_str}, 행 {idx} 건너뜀")
            continue
            
        if shift_type == "morning":
            workers = [row.get(col, '') for col in morning_cols if col in row]
            time_slot = "08:30-12:00"
            color = "#28a745"  # 오전: 초록색
        else:
            workers = [row.get(col, '') for col in afternoon_cols if col in row]
            time_slot = "13:30-17:00"
            color = "#007bff"  # 오후: 파란색
        
        for worker in workers:
            if worker.strip():  # 비어 있지 않은 근무자만 추가
                events.append({
                    "title": worker.strip(),
                    "start": f"{date_obj}T{time_slot.split('-')[0]}",
                    "end": f"{date_obj}T{time_slot.split('-')[1]}",
                    "color": color,
                    "resourceId": worker.strip(),
                    "editable": True
                })
    
    # 초기 이벤트 해시 저장
    events_key = hashlib.sha256(str(sorted([(e['title'], e['start'], e['end'], e.get('color', ''), e.get('backgroundColor', '')) for e in events if isinstance(e, dict)])).encode()).hexdigest()
    if not st.session_state.get("initial_events_hash"):
        st.session_state["initial_events_hash"] = events_key
    
    return events

# 이벤트로부터 df_schedule 업데이트
def update_schedule_from_events(events, df_schedule, shift_type):
    if not events:
        st.warning("업데이트할 이벤트가 없습니다. 원본 스케쥴을 유지합니다.")
        return df_schedule
    
    # 이벤트 해시 생성
    events_key = hashlib.sha256(str(sorted([(e['title'], e['start'], e['end'], e.get('color', ''), e.get('backgroundColor', '')) for e in events if isinstance(e, dict)])).encode()).hexdigest()
    
    # 초기 이벤트와 동일하면 스킵
    if events_key == st.session_state.get("initial_events_hash"):
        return df_schedule

    st.session_state["last_events_hash"] = events_key

    df_schedule_updated = df_schedule.copy()
    morning_cols = [str(i) for i in range(1, 13)]
    afternoon_cols = [f'오후{i}' for i in range(1, 6)]
    target_cols = morning_cols if shift_type == "morning" else afternoon_cols
    max_workers = 12 if shift_type == "morning" else 5
    shift_name = "🟢 오전" if shift_type == "morning" else "🔵 오후"

    # 원본 스케쥴에서 날짜별 근무자 매핑 (이동 전 상태)
    date_workers = {}
    for idx, row in df_schedule_updated.iterrows():
        date_str = row['날짜']
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date() if '-' in date_str else datetime.strptime(date_str, '%m월 %d일').replace(year=2025).date()
            workers = [row.get(col, '') for col in target_cols if col in row]
            date_workers[date_obj] = workers
        except ValueError:
            continue

    swap_log = set()
    processed_moves = set()

    # 새로운 근무자 상태 (이벤트 기반)
    event_groups = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        date_str = event.get('start', '').split('T')[0]
        if not date_str:
            continue
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            continue
        worker = event.get('title', '')
        if not worker:
            continue
        if date_obj not in event_groups:
            event_groups[date_obj] = set()
        event_groups[date_obj].add(worker)

    # 이동 전 근무자 상태 (세션 상태에서 가져옴)
    original_workers_by_date = st.session_state.get("original_workers_by_date", {})
    if not original_workers_by_date:
        st.warning("원본 근무자 상태가 없습니다. 이동 탐지 불가.")
        return df_schedule_updated

    # 새로운 근무자 상태 (이동 후 상태)
    new_workers_by_date = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        date_str = event.get('start', '').split('T')[0]
        if not date_str:
            continue
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            continue
        worker = event.get('title', '')
        if not worker:
            continue
        new_workers_by_date.setdefault(d, set()).add(worker)

    added = {}
    removed = {}
    for d in set(list(original_workers_by_date.keys()) + list(new_workers_by_date.keys())):
        orig = original_workers_by_date.get(d, {}).get(shift_type, set())
        new = new_workers_by_date.get(d, set())
        added[d] = new - orig
        removed[d] = orig - new

    swap_pairs = []
    to_remove = []
    processed_pairs = set()

    # 교환 쌍 탐지 (중복 방지)
    for d1 in sorted(list(added.keys())):
        for worker in list(added[d1]):
            for d2 in sorted(list(removed.keys())):
                if d1 >= d2:
                    continue
                if worker in removed[d2]:
                    for w2 in list(added.get(d2, set())):
                        if w2 in removed.get(d1, set()):
                            pair_key = tuple(sorted([(worker, d1), (w2, d2)]))
                            if pair_key not in processed_pairs:
                                swap_pairs.append((worker, d1, w2, d2))
                                to_remove.append((worker, d1, w2, d2))
                                processed_pairs.add(pair_key)
                                log_entry = f"{d2.strftime('%m월 %d일')} {shift_name} {worker} ↔ {d1.strftime('%m월 %d일')} {shift_name} {w2}"
                                if log_entry not in st.session_state["swap_log_history"]:
                                    st.session_state["swap_log_history"].append(log_entry)
                                    save_to_google_sheets_log(log_entry, month_str)
                                st.write(f"교환 쌍 추가: {worker} ({d2}) <-> {w2} ({d1})")
                                st.success("캘린더 조정 완료, 저장 버튼을 눌러 반영하세요.")
                                break

    # 제거 처리 (순회 후)
    for worker, d1, w2, d2 in to_remove:
        added[d1].discard(worker)
        removed[d2].discard(worker)
        added[d2].discard(w2)
        removed[d1].discard(w2)
        processed_moves.add((worker, d1, d2))
        processed_moves.add((w2, d2, d1))

    # 교환 처리
    for worker, new_date, swap_worker, orig_date in swap_pairs:
        row_idx_new = df_schedule_updated[df_schedule_updated['날짜'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d').date() if '-' in x else datetime.strptime(x, '%m월 %d일').replace(year=2025).date()
        ) == new_date].index
        row_idx_orig = df_schedule_updated[df_schedule_updated['날짜'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d').date() if '-' in x else datetime.strptime(x, '%m월 %d일').replace(year=2025).date()
        ) == orig_date].index
        if row_idx_new.empty or row_idx_orig.empty:
            continue
        row_idx_new = row_idx_new[0]
        row_idx_orig = row_idx_orig[0]

        # 대상 열의 근무자 가져오기
        current_workers_new = df_schedule_updated.loc[row_idx_new, target_cols].tolist()
        current_workers_orig = df_schedule_updated.loc[row_idx_orig, target_cols].tolist()
        
        # 근무자 인덱스 찾기
        new_worker_index = None
        orig_worker_index = None
        for i, w in enumerate(current_workers_new):
            if w == swap_worker:
                new_worker_index = i
                break
        for i, w in enumerate(current_workers_orig):
            if w == worker:
                orig_worker_index = i
                break
        
        if new_worker_index is not None and orig_worker_index is not None:
            # 근무자 스왑
            current_workers_new[new_worker_index] = worker
            current_workers_orig[orig_worker_index] = swap_worker
            # 대상 열에만 업데이트
            for i, col in enumerate(target_cols):
                df_schedule_updated.at[row_idx_new, col] = current_workers_new[i]
                df_schedule_updated.at[row_idx_orig, col] = current_workers_orig[i]
            swap_log.add((worker, new_date.strftime('%m월 %d일')))
            swap_log.add((swap_worker, orig_date.strftime('%m월 %d일')))

        # 상태 갱신
        original_workers_by_date[new_date] = {
            "morning": set([df_schedule_updated.loc[row_idx_new, col] for col in morning_cols if df_schedule_updated.loc[row_idx_new, col]]),
            "afternoon": set([df_schedule_updated.loc[row_idx_new, col] for col in afternoon_cols if df_schedule_updated.loc[row_idx_new, col]])
        }
        original_workers_by_date[orig_date] = {
            "morning": set([df_schedule_updated.loc[row_idx_orig, col] for col in morning_cols if df_schedule_updated.loc[row_idx_orig, col]]),
            "afternoon": set([df_schedule_updated.loc[row_idx_orig, col] for col in afternoon_cols if df_schedule_updated.loc[row_idx_orig, col]])
        }

    for date_obj, workers in date_workers.items():
        num_workers = len([w for w in workers if w])
        # 토요일(weekday == 5)은 10명 근무 정상, 그 외는 max_workers
        if date_obj.weekday() == 5 and shift_type == "morning":
            if num_workers != 10 and num_workers != 0:
                st.warning(f"{date_obj.strftime('%m월 %d일')} {shift_name} 근무자가 총 {num_workers}명입니다. 배정을 마쳐주세요.")
        else:
            if num_workers != max_workers and num_workers != 0:
                st.warning(f"{date_obj.strftime('%m월 %d일')} {shift_name} 근무자가 총 {num_workers}명입니다. 배정을 마쳐주세요.")

    st.session_state["processed_moves"] = processed_moves
    st.session_state["original_workers_by_date"] = original_workers_by_date
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

# 새로고침 버튼
if st.button("🔄 새로고침 (R)"):
    st.cache_data.clear()
    df_schedule = load_data_page3plus_no_cache(month_str)
    st.session_state["df_schedule"] = df_schedule
    st.session_state["last_events_hash"] = None
    st.session_state["initial_events_hash"] = None
    st.session_state["processed_moves"] = set()
    st.session_state["original_workers_by_date"] = None
    st.session_state["swap_log_history"] = []  # 로그 초기화
    st.success("데이터가 새로고침되었습니다.")
    st.rerun()

# 메인 앱 로직
st.header(f"📅 {month_str} 내시경실 조정", divider='rainbow')
st.write(" ")

# 안내 문구
st.write("- 두 날짜에서 한 명씩 인원을 선택하여 드래그 다운으로 방배정을 교환한 후, 저장 버튼을 눌러주세요.")

# 누적 조정사항 로그 표시 (박스 스타일)
if st.session_state["swap_log_history"]:
    with st.container():
        log_items = "".join([f"<li>{log.replace('🟢 오전', '오전').replace('🔵 오후', '오후')}</li>" for log in st.session_state["swap_log_history"]])
        st.markdown(
            f"""
            <style>
            .log-box {{
                border: 1px solid #ddd;
                padding: 10px;
                border-radius: 5px;
                background-color: #f9f9f9;
            }}
            .log-box ul {{
                list-style-type: none;
                padding-left: 20px;
            }}
            .log-box li {{
                margin-bottom: 5px;
                list-style-type: "- ";
            }}
            </style>
            <div class="log-box">
            <h6>현재까지 조정사항</h6>
            <ul>
            {log_items}
            </ul>
            </div>
            """,
            unsafe_allow_html=True
        )
st.write(" ")

# 시간대 선택
shift_type = st.selectbox("시간대 선택", ["morning", "afternoon"], format_func=lambda x: "🟢 오전" if x == "morning" else "🔵 오후")

# 캘린더 이벤트 생성
events = df_schedule_to_events(st.session_state["df_schedule"], shift_type)

# 캘린더 옵션
calendar_options = {
    "editable": True,
    "selectable": True,
    "initialView": "dayGridMonth",
    "initialDate": "2025-04-01",
    "events": events,
    "eventClick": "function(info) { alert('근무자: ' + info.event.title + '\\n날짜: ' + info.event.start.toISOString().split('T')[0]); }",
    "eventDrop": "function(info) { alert('방배정이 이동되었습니다: ' + info.event.title + ' -> ' + info.event.start.toISOString().split('T')[0]); }",
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
            # 이벤트 해시 생성 및 비교
            events_key = hashlib.sha256(str(sorted([(e['title'], e['start'], e['end'], e.get('color', ''), e.get('backgroundColor', '')) for e in events_list if isinstance(e, dict)])).encode()).hexdigest()
            if events_key != st.session_state.get("initial_events_hash"):
                st.session_state["df_schedule"] = update_schedule_from_events(events_list, st.session_state["df_schedule"], shift_type)
        else:
            st.warning("빈 이벤트 리스트입니다. 스케쥴을 업데이트하지 않습니다.")
    else:
        st.error(f"유효하지 않은 events 리스트 형식: {events_list}")

# 저장 버튼
if st.button("💾 저장"):
    if st.session_state["df_schedule"] is None or st.session_state["df_schedule"].empty:
        st.error("스케쥴 데이터가 없습니다. 새로고침 후 다시 시도해주세요.")
    else:
        success = save_to_google_sheets(st.session_state["df_schedule"], month_str)
        if success:
            st.success("저장이 완료되었습니다.")
            st.session_state["last_events_hash"] = None  # 해시 초기화
            st.session_state["initial_events_hash"] = None  # 초기 해시 리셋
            st.rerun()  # 페이지 갱신
        else:
            st.error("스케쥴 저장에 실패했습니다. 다시 시도해주세요.")
            st.rerun()  # 실패 시에도 페이지 갱신