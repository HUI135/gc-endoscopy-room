import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu # menu.py가 있다고 가정

# --- 페이지 기본 설정 ---
st.set_page_config(page_title="방 배정 변경", page_icon="🔄", layout="wide")

import os
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
def load_data_for_change_page(month_str):
    st.cache_data.clear() # 항상 최신 데이터 로드
    gc = get_gspread_client()
    sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])

    # 1. 최종 방배정 결과 시트 불러오기
    try:
        worksheet_final = sheet.worksheet(f"{month_str} 방배정")
        df_final = pd.DataFrame(worksheet_final.get_all_records())
        # 빈 값을 None 대신 빈 문자열 ''로 처리
        df_final = df_final.fillna('')
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"'{month_str} 방배정' 시트를 찾을 수 없습니다. '방 배정' 페이지에서 먼저 배정을 수행해주세요.")
        time.sleep(1)
        st.stop()

    # 2. 방배정 변경 요청 시트 불러오기
    try:
        worksheet_req = sheet.worksheet(f"{month_str} 방배정 변경요청")
        df_req = pd.DataFrame(worksheet_req.get_all_records())
    except gspread.exceptions.WorksheetNotFound:
        st.warning(f"'{month_str} 방배정 변경요청' 시트가 없습니다. 빈 테이블로 시작합니다.")
        # [수정된 부분] 실제 시트의 컬럼 구조를 반영
        df_req = pd.DataFrame(columns=['RequestID', '요청일시', '요청자', '요청자 사번', '요청 근무일', '요청자 방배정', '상대방', '상대방 방배정'])

    st.session_state["df_final_assignment"] = df_final
    st.session_state["df_change_requests"] = df_req
    st.session_state["change_data_loaded"] = True
    
    return df_final, df_req

# --- 방배정 변경사항 적용 함수 ---
def apply_assignment_swaps(df_assignment, df_requests):
    df_modified = df_assignment.copy()
    changed_log = set()
    applied_count = 0

    for _, req in df_requests.iterrows():
        try:
            # [수정된 부분] 실제 컬럼명으로 데이터 파싱
            req_person = str(req['요청자']).strip()
            
            # '요청 근무일'에서 'M월 D일' 형식의 날짜 추출
            raw_date_str = str(req['요청 근무일']).strip()
            date_match = re.search(r'(\d+)월\s*(\d+)일', raw_date_str)
            if not date_match:
                st.warning(f"요청 처리 불가: '{raw_date_str}'에서 날짜 정보를 찾을 수 없습니다.")
                continue
            
            # 방배정 시트의 날짜 형식('4월 1일')과 일치시키기
            req_date = f"{int(date_match.group(1))}월 {int(date_match.group(2))}일"
            other_date = req_date # 교환은 같은 날짜에 일어난다고 가정

            req_slot = str(req['요청자 방배정']).strip()
            other_person = str(req['상대방']).strip()
            other_slot = str(req['상대방 방배정']).strip()

            # 요청자 위치 찾기
            req_row_idx = df_modified.index[df_modified['날짜'] == req_date].tolist()
            # 상대방 위치는 요청자와 동일
            other_row_idx = req_row_idx

            if not req_row_idx:
                st.warning(f"요청 처리 불가: 방배정 표에서 날짜 '{req_date}'를 찾을 수 없습니다.")
                continue

            req_idx, other_idx = req_row_idx[0], other_row_idx[0]

            # 원래 값이 맞는지 확인
            if df_modified.at[req_idx, req_slot] == req_person and df_modified.at[other_idx, other_slot] == other_person:
                # 값 교환
                df_modified.at[req_idx, req_slot] = other_person
                df_modified.at[other_idx, other_slot] = req_person
                
                # 변경 로그 기록 (Excel 하이라이트를 위해)
                changed_log.add((req_date, req_slot, other_person))
                changed_log.add((other_date, other_slot, req_person))
                applied_count += 1
            else:
                # [수정된 부분] 요청하신 에러 메시지
                st.error(f"적용 실패: {req_date}의 '{req_person}' 또는 {other_date}의 '{other_person}'을 방 배정에서 찾을 수 없습니다.")

        except KeyError as e:
            st.error(f"요청 처리 중 오류 발생: 시트에 '{e}' 컬럼이 없습니다. (요청 정보: {req.to_dict()})")
        except Exception as e:
            st.error(f"요청 처리 중 시스템 오류 발생: {e} (요청 정보: {req.to_dict()})")


    if applied_count > 0:
        st.success(f"총 {applied_count}건의 변경 요청이 반영되었습니다.")
        time.sleep(1)
    elif applied_count == 0 and not df_requests.empty:
        st.info("반영할 유효한 변경 요청이 없습니다. 요청 내용을 확인해주세요.")
        time.sleep(1)
            
    return df_modified, changed_log

month_str = "2025년 04월" # 필요시 날짜 선택 UI로 변경 가능

# 새로고침 버튼
if st.button("🔄 새로고침(R)"):
    load_data_for_change_page(month_str)
    st.rerun()

# --- 메인 UI ---
st.write(" ")
st.subheader(f"🔄 {month_str} 방배정 변경 및 최종 확정")

# 데이터 로드
df_final, df_req = load_data_for_change_page(month_str)

# --- 1. 변경 요청 목록 표시 ---
st.write(" ")
st.write("**📋 방배정 변경 요청 목록**")
st.write("- 아래 변경 요청 목록을 확인하고, 스케줄을 수정 후 저장하세요.")
if not st.session_state["df_change_requests"].empty:
    df_display = st.session_state["df_change_requests"].copy()
    if 'RequestID' in df_display.columns:
        df_display = df_display.drop(columns=['RequestID'])
    st.dataframe(df_display, use_container_width=True, hide_index=True)
else:
    st.info("접수된 변경 요청이 없습니다.")

# --- 2. 방배정 수정 ---
st.write(" ")
st.write("**✍️ 방배정 최종 수정**")
st.write("- 요청사항을 일괄 적용하거나, 셀을 더블클릭하여 직접 수정한 후 **최종 저장 버튼**을 누르세요.")

# 데이터 에디터 (수동 수정)

if st.button("🔄 요청사항 일괄 적용"):
    if not st.session_state["df_change_requests"].empty:
        modified_df, changes = apply_assignment_swaps(st.session_state["df_final_assignment"], st.session_state["df_change_requests"])
        st.session_state["df_final_assignment"] = modified_df
        st.session_state["changed_cells_log"].update(changes)
        st.rerun()
    else:
        st.info("처리할 변경 요청이 없습니다.")

edited_df = st.data_editor(
    st.session_state["df_final_assignment"], 
    use_container_width=True, 
    key="assignment_editor",
    disabled=['날짜', '요일'], hide_index=True)

# --- 3. 최종 저장 및 다운로드 ---
if st.button("✍️ 최종 변경사항 Google Sheets에 저장 및 Excel 생성", type="primary", use_container_width=True):
    final_df = edited_df.copy()
    
    # 수동 변경사항 감지 및 로그 기록
    original_df = st.session_state["df_final_assignment"]
    if not original_df.equals(final_df):
        st.info("수동으로 변경된 내역을 감지하고 로그에 추가합니다...")
        # DataFrame 비교하여 변경된 셀 찾기
        diff_mask = (original_df != final_df) & (original_df.notna() | final_df.notna())
        for col in diff_mask.columns:
            if diff_mask[col].any():
                for idx in diff_mask.index[diff_mask[col]]:
                    date_val = final_df.at[idx, '날짜']
                    new_val = final_df.at[idx, col]
                    st.session_state["changed_cells_log"].add((date_val, col, new_val))
    
    # 1. Google Sheets에 저장
    try:
        st.info("최종 확정된 방배정표를 Google Sheets에 저장합니다...")
        gc = get_gspread_client()
        sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
        worksheet_final = sheet.worksheet(f"{month_str} 방배정")
        
        final_data_list = [final_df.columns.tolist()] + final_df.fillna('').values.tolist()
        update_sheet_with_retry(worksheet_final, final_data_list)
        st.success("✅ Google Sheets에 최종 방배정표가 성공적으로 저장되었습니다.")

        # 저장 후 현재 상태를 최신으로 업데이트
        st.session_state["df_final_assignment"] = final_df.copy()

    except Exception as e:
        st.error(f"Google Sheets 저장 중 오류 발생: {e}")
        st.stop()

    # 2. Excel 파일 생성 및 다운로드 (기존 코드 거의 그대로 재사용)
    with st.spinner("Excel 파일을 생성 중입니다..."):
        wb = openpyxl.Workbook()
        sheet = wb.active
        sheet.title = "Schedule"
        
        # 스타일 정의
        sky_blue_fill = PatternFill(start_color="CCEEFF", end_color="CCEEFF", fill_type="solid")
        duty_font = Font(name="맑은 고딕", size=9, bold=True, color="FF00FF")
        default_font = Font(name="맑은 고딕", size=9)
        
        columns = final_df.columns.tolist()
        
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
        for row_idx, row_data in enumerate(final_df.itertuples(index=False), 2):
            current_date_str = row_data[0]
            for col_idx, value in enumerate(row_data, 1):
                cell = sheet.cell(row_idx, col_idx, value if value else None)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                
                slot_name = columns[col_idx-1]
                
                # 변경사항 하이라이트
                if (current_date_str, slot_name, value) in st.session_state["changed_cells_log"]:
                    cell.fill = sky_blue_fill
                
                # 당직 폰트
                if (slot_name.endswith('_당직') or slot_name == '온콜') and value:
                    cell.font = duty_font
                else:
                    cell.font = default_font

        # BytesIO에 파일 저장
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        st.session_state['download_file'] = output
        st.session_state['download_filename'] = f"{month_str} 방배정_최종확정.xlsx"

# 다운로드 버튼 표시
if 'download_file' in st.session_state and st.session_state['download_file'] is not None:
    st.divider()
    st.download_button(
        label="📥 최종 확정본 다운로드",
        data=st.session_state['download_file'],
        file_name=st.session_state['download_filename'],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )
    # 다운로드 후 세션 상태 초기화
    st.session_state['download_file'] = None

st.divider()
st.caption("변경사항 로그 (이 셀들이 Excel에서 하이라이트됩니다)")
if st.session_state["changed_cells_log"]:
    log_df = pd.DataFrame(list(st.session_state["changed_cells_log"]), columns=['날짜', '슬롯', '변경된 인원'])
    st.dataframe(log_df.sort_values(by=['날짜', '슬롯']).reset_index(drop=True), use_container_width=True, hide_index=True)
else:
    st.info("기록된 변경사항이 없습니다.")