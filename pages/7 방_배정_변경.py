import re
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import time
from datetime import date
from io import BytesIO
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
import menu

menu.menu()

# 상수 정의
MONTH_STR = "2025년 04월"
NEXT_MONTH_START = date(2025, 4, 1)
NEXT_MONTH_END = date(2025, 4, 30)
ROOM_MAPPING = {
    '8:30(1)_당직': '1', '8:30(2)': '2', '8:30(4)': '4', '8:30(7)': '7',
    '9:00(10)': '10', '9:00(11)': '11', '9:00(12)': '12',
    '9:30(5)': '5', '9:30(6)': '6', '9:30(8)': '8',
    '10:00(3)': '3', '10:00(9)': '9',
    '13:30(2)_당직': '2', '13:30(3)': '3', '13:30(4)': '4', '13:30(9)': '9'
}
COLOR_MAPPING = {
    '8:30': "FFE699", '9:00': "F8CBAD", '9:30': "B4C6E7", '10:00': "C6E0B4",
    '13:30': "CC99FF", '온콜': "FFE699", '날짜': "808080", '요일_토': "BFBFBF",
    '요일': "FFF2CC", 'no_person': "808080", '인원': "D0CECE", '당직 합계': "FF00FF",
    '이른방 합계': "FFE699", '늦은방 합계': "C6E0B4"
}

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
            else:
                st.warning(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
            time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# 데이터 로드 함수
@st.cache_data
def load_data_page7(month_str):
    return load_data_page7_no_cache(month_str)

def load_data_page7_no_cache(month_str):
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

# 중복 배정 확인 함수
def check_duplicates(df, morning_slots, afternoon_slots):
    duplicate_errors = []
    for idx, row in df.iterrows():
        date_str = row['날짜']
        morning_assignments = [row[col] for col in morning_slots if pd.notna(row[col]) and row[col].strip()]
        afternoon_assignments = [row[col] for col in afternoon_slots if pd.notna(row[col]) and row[col].strip()]
        
        morning_counts = Counter(morning_assignments)
        afternoon_counts = Counter(afternoon_assignments)
        
        for person, count in morning_counts.items():
            if person and count > 1:
                duplicate_errors.append(f"{date_str}: {person}이(가) 오전 시간대에 {count}번 중복 배정되었습니다.")
        for person, count in afternoon_counts.items():
            if person and count > 1:
                duplicate_errors.append(f"{date_str}: {person}이(가) 오후 시간대에 {count}번 중복 배정되었습니다.")
    
    return duplicate_errors

# 근무 횟수 비교 함수
def compare_counts(df_original, df_modified):
    count_original = Counter()
    count_modified = Counter()
    
    for _, row in df_original.drop(columns=["날짜", "요일"]).iterrows():
        for value in row:
            if pd.notna(value) and value.strip():
                count_original[value] += 1
    
    for _, row in df_modified.drop(columns=["날짜", "요일"]).iterrows():
        for value in row:
            if pd.notna(value) and value.strip():
                count_modified[value] += 1
    
    all_names = set(count_original.keys()).union(set(count_modified.keys()))
    discrepancies = []
    for name in all_names:
        orig_count = count_original.get(name, 0)
        mod_count = count_modified.get(name, 0)
        if orig_count != mod_count:
            if mod_count < orig_count:
                discrepancies.append(f"{name}이(가) 기존 파일보다 근무가 {orig_count - mod_count}회 적습니다.")
            elif mod_count > orig_count:
                discrepancies.append(f"{name}이(가) 기존 파일보다 근무가 {mod_count - orig_count}회 많습니다.")
    
    return discrepancies

# 통계 계산 함수
def calculate_stats(df):
    all_personnel = set()
    for _, row in df.drop(columns=["날짜", "요일"]).iterrows():
        personnel = [p for p in row if pd.notna(p) and p.strip()]
        all_personnel.update(personnel)
    
    total_stats = {
        'early': Counter(),  # 8:30 시작 (당직 제외)
        'late': Counter(),   # 10:00 시작
        'duty': Counter(),   # 당직
        'rooms': {str(i): Counter() for i in range(1, 13)}  # 방 번호 AscendingSort
    }
    
    for _, row in df.iterrows():
        for col in df.columns:
            if col in ['날짜', '요일']:
                continue
            person = row[col]
            if pd.notna(person) and person.strip():
                # 이른방 (8:30, 당직 제외)
                if col.startswith('8:30') and not col.endswith('_당직'):
                    total_stats['early'][person] += 1
                # 늦은방 (10:00)
                if col.startswith('10:00'):
                    total_stats['late'][person] += 1
                # 당직 (8:30(1)_당직, 13:30(2)_당직)
                if col in ['8:30(1)_당직', '13:30(2)_당직']:
                    total_stats['duty'][person] += 1
                # 방별
                if col in ROOM_MAPPING:
                    room_num = ROOM_MAPPING[col]
                    total_stats['rooms'][room_num][person] += 1
    
    stats_data = [
        {
            '인원': person,
            '이른방 합계': total_stats['early'][person],
            '늦은방 합계': total_stats['late'][person],
            '당직 합계': total_stats['duty'][person],
            **{f'{r}번방 합계': total_stats['rooms'][r][person] for r in total_stats['rooms']}
        }
        for person in sorted(all_personnel)
    ]
    
    return pd.DataFrame(stats_data)

# 엑셀 파일 생성 함수
def create_excel_file(df, stats_df, request_cells=None, date_cache=None):
    wb = openpyxl.Workbook()
    sheet = wb.active
    sheet.title = "Schedule"
    
    columns = df.columns.tolist()
    result_data = df.values.tolist()
    
    # 헤더 스타일링
    for col_idx, header in enumerate(columns, 1):
        cell = sheet.cell(1, col_idx, header)
        cell.font = Font(bold=True, name="맑은 고딕", size=9)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        
        for time_key, color in COLOR_MAPPING.items():
            if header.startswith(time_key):
                cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                break
    
    # 데이터 스타일링
    for row_idx, row in enumerate(result_data, 2):
        has_person = any(x for x in row[2:-1] if x)
        formatted_date = date_cache.get(row[0], '') if date_cache else row[0]
        
        for col_idx, value in enumerate(row, 1):
            cell = sheet.cell(row_idx, col_idx, value)
            cell.font = Font(name="맑은 고딕", size=9, bold=(columns[col_idx-1].endswith('_당직') or columns[col_idx-1] == '온콜') and value, color="FF00FF" if (columns[col_idx-1].endswith('_당직') or columns[col_idx-1] == '온콜') and value else "000000")
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            
            if col_idx == 1:
                cell.fill = PatternFill(start_color=COLOR_MAPPING['날짜'], end_color=COLOR_MAPPING['날짜'], fill_type="solid")
            elif col_idx == 2:
                cell.fill = PatternFill(start_color=COLOR_MAPPING['요일_토'] if value == '토' and has_person else COLOR_MAPPING['요일'], end_color=COLOR_MAPPING['요일_토'] if value == '토' and has_person else COLOR_MAPPING['요일'], fill_type="solid")
            elif not has_person and col_idx >= 3:
                cell.fill = PatternFill(start_color=COLOR_MAPPING['no_person'], end_color=COLOR_MAPPING['no_person'], fill_type="solid")
            
            if col_idx > 2 and value and formatted_date and request_cells:
                slot = columns[col_idx-1]
                if (formatted_date, slot) in request_cells and value == request_cells[(formatted_date, slot)]['이름']:
                    cell.comment = Comment(f"배정 요청: {request_cells[(formatted_date, slot)]['분류']}", "System")
    
    # 통계 시트 추가
    stats_sheet = wb.create_sheet("Stats")
    stats_columns = stats_df.columns.tolist()
    
    for col_idx, header in enumerate(stats_columns, 1):
        cell = stats_sheet.cell(1, col_idx, header)
        cell.font = Font(bold=True, name="맑은 고딕", size=9)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        
        for key, color in COLOR_MAPPING.items():
            if header == key:
                cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                break
    
    for row_idx, row in enumerate(stats_df.values, 2):
        for col_idx, value in enumerate(row, 1):
            cell = stats_sheet.cell(row_idx, col_idx, value)
            cell.font = Font(name="맑은 고딕", size=9)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output

# 메인 로직
def main():
    # 로그인 및 관리자 권한 체크
    if "login_success" not in st.session_state or not st.session_state["login_success"]:
        st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
        st.stop()
    
    # 새로고침 버튼
    if st.button("🔄 새로고침 (R)"):
        st.cache_data.clear()
        df_room = load_data_page7_no_cache(MONTH_STR)
        st.session_state["df_room"] = df_room
        st.success("데이터가 새로고침되었습니다.")
        st.rerun()
    
    # 메인 UI
    st.subheader(f"✨ {MONTH_STR} 방 배정 조정")
    st.write("- 직접 이름을 수정하여 방 배정을 조정할 수 있습니다.")
    df_room = load_data_page7(MONTH_STR)
    edited_df = st.data_editor(
        df_room,
        use_container_width=True,
        num_rows="fixed",
        key="editor1"
    )
    
    # 방 배정 조정 확인
    st.divider()
    st.subheader(f"✨ {MONTH_STR} 방 배정 조정 확인")
    st.write("- 모든 인원의 근무 횟수가 원본과 동일한지, 누락 및 추가 인원이 있는지 확인합니다.")
    st.write("- 날짜별 오전(8:30, 9:00, 9:30, 10:00) 및 오후(13:30) 시간대에 동일 인물이 중복 배정되지 않았는지 확인합니다.")
    
    if st.button("확인"):
        try:
            df_room_md = edited_df.copy()
            
            if not df_room.columns.equals(df_room_md.columns):
                st.error("수정된 데이터의 컬럼이 원본 데이터와 일치하지 않습니다.")
                st.stop()
            
            morning_slots = [col for col in df_room_md.columns if col.startswith(('8:30', '9:00', '9:30', '10:00')) and col != '온콜']
            afternoon_slots = [col for col in df_room_md.columns if col.startswith('13:30')]
            
            duplicate_errors = check_duplicates(df_room_md, morning_slots, afternoon_slots)
            count_discrepancies = compare_counts(df_room, df_room_md)
            
            if duplicate_errors or count_discrepancies:
                for error in duplicate_errors:
                    st.warning(error)
                for warning in count_discrepancies:
                    st.warning(warning)
            else:
                st.success("모든 인원의 근무 횟수가 원본과 동일하며, 중복 배정 오류가 없습니다!")
                # st.write(" ")
                # st.markdown("**✅ 통합 배치 결과**")
                # st.dataframe(df_room_md)
                
                stats_df = calculate_stats(df_room_md)
                st.write(" ")
                st.markdown("**📊 인원별 통계**")
                st.dataframe(stats_df)
                
                # 엑셀 파일 생성 및 다운로드
                excel_file = create_excel_file(df_room_md, stats_df)  # request_cells, date_cache 전달 필요 시 추가
                st.divider()
                st.download_button(
                    label="📥 최종 방배정 다운로드",
                    data=excel_file,
                    file_name=f"{MONTH_STR} 방배정 최종.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
                
                # Google Sheets 저장
                gc = get_gspread_client()
                sheet = gc.open_by_url(st.secrets["google_sheet"]["url"])
                try:
                    worksheet_result = sheet.worksheet(f"{MONTH_STR} 방배정 최종")
                except:
                    worksheet_result = sheet.add_worksheet(f"{MONTH_STR} 방배정 최종", rows=100, cols=len(df_room.columns))
                    worksheet_result.append_row(df_room.columns.tolist())
                
                update_sheet_with_retry(worksheet_result, [df_room.columns.tolist()] + df_room_md.values.tolist())
                st.success(f"✅ {MONTH_STR} 방배정 최종 테이블이 Google Sheets에 저장되었습니다.")
        
        except Exception as e:
            st.error(f"데이터 처리 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()