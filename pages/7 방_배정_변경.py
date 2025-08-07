import re
import streamlit as st
import pandas as pd
import gspread
from collections import Counter
from google.oauth2.service_account import Credentials
import time
from datetime import datetime
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu # menu.py가 있다고 가정
import os

# --- 페이지 기본 설정 ---
st.set_page_config(page_title="방 배정 변경", page_icon="🔄", layout="wide")

# 현재 페이지 파일 이름 저장
st.session_state.current_page = os.path.basename(__file__)

# menu.py의 menu() 함수 호출
menu.menu()

# --- 로그인 확인 ---
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# --- 세션 상태 초기화 ---
if "change_data_loaded" not in st.session_state:
    st.session_state["change_data_loaded"] = False
if "df_final_assignment" not in st.session_state:
    st.session_state["df_final_assignment"] = pd.DataFrame()
if "df_change_requests" not in st.session_state:
    st.session_state["df_change_requests"] = pd.DataFrame()
if "changed_cells_log" not in st.session_state:
    st.session_state["changed_cells_log"] = set()

# --- Google Sheets 연동 함수 (기존 코드 재사용) ---
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update('A1', data, value_input_option='RAW')
            return
        except Exception as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                st.error(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# --- 데이터 로드 함수 (새 페이지용) ---
@st.cache_data(ttl=600) # 10분 캐시
def load_data_for_change_page(month_str):
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])

    # 1. 최종 방배정 결과 시트 불러오기
    try:
        worksheet_final = sheet.worksheet(f"{month_str} 방배정")
        df_final = pd.DataFrame(worksheet_final.get_all_records())
        df_final = df_final.fillna('')
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{month_str} 방배정' 시트를 찾을 수 없습니다. '방 배정' 페이지에서 먼저 배정을 수행해주세요.")
        st.stop()

    # 2. 방배정 변경 요청 시트 불러오기
    try:
        worksheet_req = sheet.worksheet(f"{month_str} 방배정 변경요청")
        df_req = pd.DataFrame(worksheet_req.get_all_records())
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"'{month_str} 방배정 변경요청' 시트가 없습니다. 빈 테이블로 시작합니다.")
        df_req = pd.DataFrame(columns=['RequestID', '요청일시', '요청자', '요청자 사번', '요청 근무일', '요청자 방배정', '상대방', '상대방 방배정'])

    return df_final, df_req

# --- 방배정 변경사항 적용 함수 ---
# 기존의 apply_assignment_swaps 함수를 아래 코드로 교체해주세요.
# 기존의 apply_assignment_swaps 함수를 아래 코드로 교체해주세요.
def apply_assignment_swaps(df_assignment, df_requests):
    df_modified = df_assignment.copy()
    changed_log = set()
    applied_count = 0
    error_found = False

    for _, req in df_requests.iterrows():
        try:
            # 1. 변경 요청 데이터 파싱
            swap_request_str = str(req.get('변경 요청', '')).strip()
            raw_slot_info = str(req.get('변경 요청한 방배정', '')).strip()
            
            if not swap_request_str or not raw_slot_info:
                st.warning(f"⚠️ 요청 처리 불가: '변경 요청' 또는 '변경 요청한 방배정' 컬럼이 비어 있습니다.")
                continue
            
            # '강승주 -> 서지연'에서 변경 전/후 인원 추출
            if '->' not in swap_request_str:
                st.warning(f"⚠️ '변경 요청' 형식이 올바르지 않습니다: '{swap_request_str}'. '이름1 -> 이름2' 형식으로 입력해주세요.")
                continue
            
            old_person, new_person = [p.strip() for p in swap_request_str.split('->')]

            # '04월 04일 (금) - 8:30(1)_당직'에서 날짜와 슬롯 추출
            slot_info_parts = raw_slot_info.split(' - ')
            if len(slot_info_parts) != 2:
                st.warning(f"⚠️ '변경 요청한 방배정' 형식이 올바르지 않습니다: '{raw_slot_info}'. '04월 04일 (금) - 8:30(1)_당직' 형식으로 입력해주세요.")
                continue
            
            date_part, slot_part = slot_info_parts
            date_match = re.search(r'(\d+)월\s*(\d+)일', date_part)
            if not date_match:
                st.warning(f"⚠️ 날짜 정보 '{date_part}'를 찾을 수 없습니다.")
                continue
            
            target_date_str = f"{int(date_match.group(1))}월 {int(date_match.group(2))}일"
            target_slot = slot_part
            
            # 2. 방배정표에서 해당 날짜의 행과 슬롯 찾기
            row_indices = df_modified.index[df_modified['날짜'] == target_date_str].tolist()
            if not row_indices:
                st.warning(f"⚠️ 요청 처리 불가: 방배정표에서 날짜 '{target_date_str}'를 찾을 수 없습니다.")
                continue
            
            target_row_idx = row_indices[0]
            
            if target_slot not in df_modified.columns:
                 st.error(f"❌ 적용 실패: 슬롯 '{target_slot}'을(를) 방 배정표에서 찾을 수 없습니다.")
                 error_found = True
                 continue

            # 3. 방배정 교체 로직
            #    `target_slot`에 `old_person`이 있는지 확인하고 `new_person`으로 교체
            current_assigned_person = str(df_modified.at[target_row_idx, target_slot]).strip()
            if current_assigned_person == old_person:
                df_modified.at[target_row_idx, target_slot] = new_person
                
                changed_log.add((target_date_str, target_slot, new_person))
                applied_count += 1
            else:
                st.error(f"❌ 적용 실패: {target_date_str}의 '{target_slot}'에 '{old_person}'이(가) 배정되어 있지 않습니다. 현재 배정된 인원: '{current_assigned_person}'")
                error_found = True
        
        except KeyError as e:
            st.error(f"⚠️ 요청 처리 중 오류 발생: 시트에 '{e}' 컬럼이 없습니다. (요청 정보: {req.to_dict()})")
            error_found = True
        except Exception as e:
            st.error(f"⚠️ 요청 처리 중 시스템 오류 발생: {e} (요청 정보: {req.to_dict()})")
            error_found = True

    if applied_count > 0:
        st.toast(f"✅ 총 {applied_count}건의 변경 요청이 반영되었습니다.", icon="🎉")
    if error_found:
        st.toast("⚠️ 일부 요청 적용에 실패했습니다. 메시지를 확인하세요.", icon="🚨")
    elif applied_count == 0 and not df_requests.empty:
        st.toast("ℹ️ 새롭게 반영할 유효한 변경 요청이 없습니다.", icon="🧐")

    return df_modified, changed_log
def calculate_statistics(result_df: pd.DataFrame) -> pd.DataFrame:
    """최종 방배정 결과 DataFrame을 바탕으로 인원별 통계를 계산합니다."""
    total_stats = {
        'early': Counter(), 'late': Counter(), 'morning_duty': Counter(), 'afternoon_duty': Counter(),
        'rooms': {str(i): Counter() for i in range(1, 13)}
    }
    
    all_personnel = sorted([p for p in pd.unique(result_df.iloc[:, 2:].values.ravel('K')) if pd.notna(p) and p])
    SMALL_TEAM_THRESHOLD = 13

    for _, row in result_df.iterrows():
        personnel_in_row = [p for p in row.iloc[2:].dropna() if p]
        if 0 < len(personnel_in_row) < SMALL_TEAM_THRESHOLD:
            continue

        for slot_name in result_df.columns[2:]:
            person = row[slot_name]
            if not person or pd.isna(person):
                continue

            if slot_name.startswith('8:30') and not slot_name.endswith('_당직'):
                total_stats['early'][person] += 1
            elif slot_name.startswith('10:00'):
                total_stats['late'][person] += 1
            
            if slot_name == '온콜' or (slot_name.startswith('8:30') and slot_name.endswith('_당직')):
                total_stats['morning_duty'][person] += 1
            elif slot_name.startswith('13:30') and slot_name.endswith('_당직'):
                total_stats['afternoon_duty'][person] += 1
            
            match = re.search(r'\((\d+)\)', slot_name)
            if match:
                room_num = match.group(1)
                if room_num in total_stats['rooms']:
                    total_stats['rooms'][room_num][person] += 1

    stats_data = [{
        '인원': p,
        '이른방 합계': total_stats['early'][p], '늦은방 합계': total_stats['late'][p],
        '오전 당직 합계': total_stats['morning_duty'][p], '오후 당직 합계': total_stats['afternoon_duty'][p],
        **{f'{r}번방 합계': total_stats['rooms'][r][p] for r in total_stats['rooms']}
    } for p in all_personnel]
    
    return pd.DataFrame(stats_data)

# --- UI 및 데이터 핸들링 ---
month_str = "2025년 04월" # 필요시 날짜 선택 UI로 변경 가능

st.header("🔄 스케줄 배정", divider='rainbow')

if st.button("🔄 새로고침(R)"):
    st.cache_data.clear()
    st.session_state.change_data_loaded = False
    st.rerun()

if not st.session_state.change_data_loaded:
    df_final, df_req = load_data_for_change_page(month_str)
    st.session_state.df_final_assignment = df_final
    st.session_state.df_change_requests = df_req
    st.session_state.changed_cells_log = set()
    st.session_state.change_data_loaded = True

st.write(" ")
st.subheader(f"✅ {month_str} 방배정 변경 및 최종 확정")

st.write(" ")
st.write("**📋 방배정 변경 요청 목록**")
st.write("- 아래 변경 요청 목록을 확인하고, 스케줄을 수정 후 저장하세요.")
if not st.session_state.df_change_requests.empty:
    df_display = st.session_state.df_change_requests.copy()
    if 'RequestID' in df_display.columns:
        df_display = df_display.drop(columns=['RequestID'])
    if '요청자 사번' in df_display.columns:
        df_display = df_display.drop(columns=['요청자 사번'])
    st.dataframe(df_display, use_container_width=True, hide_index=True)
else:
    st.info("접수된 변경 요청이 없습니다.")

st.write(" ")
st.write("**✍️ 방배정 최종 수정**")
st.write("- 요청사항을 일괄 적용하거나, 셀을 더블클릭하여 직접 수정한 후 **최종 저장 버튼**을 누르세요.")

if st.button("🔄 요청사항 일괄 적용"):
    if not st.session_state.df_change_requests.empty:
        current_df = st.session_state.df_final_assignment
        requests_df = st.session_state.df_change_requests
        modified_df, changes = apply_assignment_swaps(current_df, requests_df)
        st.session_state.df_final_assignment = modified_df
        st.session_state.changed_cells_log.update(changes)
    else:
        st.toast("ℹ️ 처리할 변경 요청이 없습니다.", icon="🧐")

# Data Editor와 변경사항 감지
edited_df = st.data_editor(
    st.session_state.df_final_assignment,
    use_container_width=True,
    key="assignment_editor",
    disabled=['날짜', '요일'],
    hide_index=True
)

# 실시간 수동 변경 감지 및 로깅
if not edited_df.equals(st.session_state.df_final_assignment):
    diff_mask = (edited_df != st.session_state.df_final_assignment) & (edited_df.notna() | st.session_state.df_final_assignment.notna())
    for col in diff_mask.columns:
        if diff_mask[col].any():
            for idx in diff_mask.index[diff_mask[col]]:
                date_val = edited_df.at[idx, '날짜']
                new_val = edited_df.at[idx, col]
                old_val = st.session_state.df_final_assignment.at[idx, col]
                
                # 로그 추가 (새로운 값 기준)
                st.session_state.changed_cells_log.add((date_val, col, new_val))
                # 이전 값에 대한 로그는 제거 (A->B->C로 바뀔 때 B의 하이라이트는 제거)
                st.session_state.changed_cells_log.discard((date_val, col, old_val))

    st.session_state.df_final_assignment = edited_df.copy()

st.divider()
st.caption("📝 현재까지 기록된 변경사항 로그")
if st.session_state.changed_cells_log:
    log_df = pd.DataFrame(list(st.session_state.changed_cells_log), columns=['날짜', '슬롯', '변경된 인원'])
    st.dataframe(log_df.sort_values(by=['날짜', '슬롯']).reset_index(drop=True), use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")

if st.button("✍️ 최종 변경사항 Google Sheets에 저장 및 방배정 수행", type="primary", use_container_width=True):
    final_df_to_save = st.session_state.df_final_assignment

    st.write(" ")
    st.subheader(f"💡 {month_str} 최종 방배정 결과", divider='rainbow')
    
    st.markdown("**✅ 통합 배치 결과**")
    st.dataframe(final_df_to_save, use_container_width=True, hide_index=True)

    stats_df = calculate_statistics(final_df_to_save)
    st.markdown("**☑️ 인원별 통계**")
    st.dataframe(stats_df, use_container_width=True, hide_index=True)

    # 1. Google Sheets에 저장
    with st.spinner("Google Sheets에 저장 중..."):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
            worksheet_final = sheet.worksheet(f"{month_str} 방배정")
            
            final_data_list = [final_df_to_save.columns.tolist()] + final_df_to_save.fillna('').values.tolist()
            update_sheet_with_retry(worksheet_final, final_data_list)
            st.success("✅ Google Sheets에 최종 방배정표가 성공적으로 저장되었습니다.")
        except Exception as e:
            st.error(f"Google Sheets 저장 중 오류 발생: {e}")
            st.stop()

    # 2. Excel 파일 생성
    with st.spinner("Excel 파일을 생성 중입니다..."):
        wb = openpyxl.Workbook()
        sheet = wb.active
        sheet.title = "Schedule"
        
        # 스타일 정의 (요청하신 색상으로 변경)
        highlight_fill = PatternFill(start_color="F2DCDB", end_color="F2DCDB", fill_type="solid")
        duty_font = Font(name="맑은 고딕", size=9, bold=True, color="FF00FF")
        default_font = Font(name="맑은 고딕", size=9)
        
        columns = final_df_to_save.columns.tolist()
        
        # 헤더 렌더링
        for col_idx, header in enumerate(columns, 1):
            cell = sheet.cell(1, col_idx, header)
            cell.font = Font(bold=True, name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            if header.startswith('8:30') or header == '온콜': cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
            elif header.startswith('9:00'): cell.fill = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
            elif header.startswith('9:30'): cell.fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
            elif header.startswith('10:00'): cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
            elif header.startswith('13:30'): cell.fill = PatternFill(start_color="CC99FF", end_color="CC99FF", fill_type="solid")

        # 데이터 렌더링
        for row_idx, row_data in enumerate(final_df_to_save.itertuples(index=False), 2):
            has_person = any(val for val in row_data[2:] if val)

            current_date_str = row_data[0]
            assignment_cells = row_data[2:]
            personnel_in_row = [p for p in assignment_cells if p]
            is_no_person_day = not any(personnel_in_row)
            SMALL_TEAM_THRESHOLD_FORMAT = 15
            is_small_team_day = (0 < len(personnel_in_row) < SMALL_TEAM_THRESHOLD_FORMAT)
            
            current_date_str = row_data[0]
            for col_idx, value in enumerate(row_data, 1):
                cell = sheet.cell(row_idx, col_idx, value if value else None)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                
                special_day_fill = PatternFill(start_color="BFBFBF", end_color="BFBFBF", fill_type="solid") # 소수 근무일 '요일' 색상
                no_person_day_fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid") # 근무자 없는 날 색상
                default_yoil_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid") # 기본 '요일' 색상

                if col_idx == 1:
                    cell.fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")
                elif col_idx == 2: # '요일' 열
                    if is_no_person_day:
                        cell.fill = no_person_day_fill   # 1순위: 근무자 없는 날
                    elif is_small_team_day:
                        cell.fill = special_day_fill     # 2순위: 소수 인원 근무일
                    else:
                        cell.fill = default_yoil_fill    # 3순위: 일반 근무일
                elif is_no_person_day and col_idx >= 3: # 근무자 없는 날의 배정 슬롯
                    cell.fill = no_person_day_fill

                slot_name = columns[col_idx-1]
                
                # 변경사항 하이라이트
                if (current_date_str, slot_name, str(value)) in st.session_state.changed_cells_log:
                    cell.fill = highlight_fill
                
                # 당직 폰트
                if (slot_name.endswith('_당직') or slot_name == '온콜') and value:
                    cell.font = duty_font
                else:
                    cell.font = default_font

        stats_sheet = wb.create_sheet("Stats")
        stats_columns = stats_df.columns.tolist()
        for col_idx, header in enumerate(stats_columns, 1):
             stats_sheet.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = 12
             cell = stats_sheet.cell(1, col_idx, header)
             cell.font = Font(bold=True, name="맑은 고딕", size=9)
             cell.alignment = Alignment(horizontal='center', vertical='center')
             cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
             if header == '인원': cell.fill = PatternFill(start_color="D0CECE", end_color="D0CECE", fill_type="solid")
             elif header == '이른방 합계': cell.fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
             elif header == '늦은방 합계': cell.fill = PatternFill(start_color="C6E0B4", end_color="C6E0B4", fill_type="solid")
             elif '당직' in header: cell.fill = PatternFill(start_color="FFC0CB", end_color="FFC0CB", fill_type="solid")
             elif '번방' in header: cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
        
        for row_idx, row in enumerate(stats_df.values, 2):
            for col_idx, value in enumerate(row, 1):
                cell = stats_sheet.cell(row_idx, col_idx, value)
                cell.font = Font(name="맑은 고딕", size=9)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        st.session_state.download_file = output
        st.session_state.download_filename = f"{month_str} 방배정_최종확정.xlsx"

st.divider()
# 다운로드 버튼 표시
if 'download_file' in st.session_state and st.session_state.download_file:
    st.download_button(
        label="📥 최종 확정본 다운로드",
        data=st.session_state.download_file,
        file_name=st.session_state.download_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )
    st.session_state.download_file = None # 다운로드 후 초기화
