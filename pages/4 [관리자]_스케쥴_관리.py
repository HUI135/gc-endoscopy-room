import numpy as np
import json
import streamlit as st
import pandas as pd
import calendar
import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# # 특정 사용자(사번 65579) 접속 제한
# ALLOWED_EMPLOYEE_ID = 65579

# 로그인 및 사번 체크
if "login_success" not in st.session_state or not st.session_state["login_success"]:
    st.warning("⚠️ Home 페이지에서 비밀번호와 사번을 먼저 입력해주세요.")
    st.stop()

# if st.session_state["employee_id"] != ALLOWED_EMPLOYEE_ID:
#     st.error("⚠️ 이 페이지에 접근할 권한이 없습니다.")
#     st.stop()

# 사이드바
st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
if st.sidebar.button("로그아웃"):
    st.session_state.clear()
    st.success("로그아웃되었습니다. 🏠 Home 페이지로 돌아가 주세요.")
    time.sleep(5)
    st.rerun()

# Google Sheets 클라이언트 초기화
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# Google Sheets 업데이트 함수 (쿼터 초과 및 동시성 처리)
def update_sheet_with_retry(worksheet, data, retries=5, delay=10):
    for attempt in range(retries):
        try:
            worksheet.batch_update([
                {"range": "A1:D", "values": [[]]},  # Clear range
                {"range": "A1", "values": data}
            ])
            return
        except APIError as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                raise e
        except Exception as e:
            st.warning(f"업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
            time.sleep(delay)
    st.error("Google Sheets 업데이트 실패: 재시도 횟수 초과")

# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]
month_str = "2025년 04월"

if "data_loaded" not in st.session_state:
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        
        # 매핑 시트
        mapping = sheet.worksheet("매핑")
        st.session_state["mapping"] = mapping
        mapping_data = mapping.get_all_records()
        df_map = pd.DataFrame(mapping_data) if mapping_data else pd.DataFrame(columns=["이름", "사번"])
        st.session_state["df_map"] = df_map
        
        # 마스터 시트
        worksheet1 = sheet.worksheet("마스터")
        st.session_state["worksheet1"] = worksheet1
        master_data = worksheet1.get_all_records()
        df_master = pd.DataFrame(master_data) if master_data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        st.session_state["df_master"] = df_master
        
        # 요청사항 시트
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except WorksheetNotFound:
            worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
        st.session_state["worksheet2"] = worksheet2
        request_data = worksheet2.get_all_records()
        df_request = pd.DataFrame(request_data) if request_data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["df_request"] = df_request

        # Constraint Enforcement
        missing_in_master = set(df_map["이름"]) - set(df_master["이름"])
        if missing_in_master:
            new_master_rows = []
            for name in missing_in_master:
                for day in ["월", "화", "수", "목", "금"]:
                    new_master_rows.append({
                        "이름": name,
                        "주차": "매주",
                        "요일": day,
                        "근무여부": "근무없음"
                    })
            new_master_df = pd.DataFrame(new_master_rows)
            df_master = pd.concat([df_master, new_master_df], ignore_index=True)
            df_master["요일"] = pd.Categorical(
                df_master["요일"], 
                categories=["월", "화", "수", "목", "금"], 
                ordered=True
            )
            df_master = df_master.sort_values(by=["이름", "주차", "요일"])
            update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist())
            st.session_state["df_master"] = df_master

        missing_in_request = set(df_master["이름"]) - set(df_request["이름"])
        if missing_in_request:
            new_request_rows = [{"이름": name, "분류": "요청 없음", "날짜정보": ""} for name in missing_in_request]
            new_request_df = pd.DataFrame(new_request_rows)
            df_request = pd.concat([df_request, new_request_df], ignore_index=True)
            df_request = df_request.sort_values(by=["이름", "날짜정보"])
            update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
            st.session_state["df_request"] = df_request

        st.session_state["data_loaded"] = True
        
    except Exception as e:
        st.error(f"시트를 불러오는 데 문제가 발생했습니다: {e}")
        st.write(f"Error details: {type(e).__name__}, {str(e)}")
        st.session_state["df_map"] = pd.DataFrame(columns=["이름", "사번"])
        st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        st.session_state["df_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["data_loaded"] = False

# 세션 상태에서 데이터 가져오기 및 None 체크
mapping = st.session_state.get("mapping")
df_map = st.session_state.get("df_map", pd.DataFrame(columns=["이름", "사번"]))
worksheet1 = st.session_state.get("worksheet1")
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
worksheet2 = st.session_state.get("worksheet2")
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
names_in_master = df_master["이름"].unique() if not df_master.empty else []

# 익월 범위 지정
today = datetime.datetime.strptime('2025-03-31', '%Y-%m-%d').date()
next_month = today.replace(day=1) + relativedelta(months=1)
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)

if st.session_state.get("is_admin_authenticated", False):
    st.subheader("📁 스케쥴 시트 이동")
    st.markdown("https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?usp=sharing")

    # 명단 관리 탭
    st.divider()
    st.subheader("📋 명단 관리")
    st.write(" - 매핑 시트, 마스터 시트, 요청사항 시트에 인원을 추가/삭제합니다.\n- 아래 명단에 존재하는 인원만 시스템 로그인이 가능합니다.")

    # 세션 상태에서 df_master 관리
    if "df_master" not in st.session_state or st.session_state["df_master"].empty:
        st.session_state["df_master"] = df_master.copy() if not df_master.empty else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])

    df_master = st.session_state["df_master"]
    
    # '사번' 열을 문자열로 변환
    if not df_map.empty:
        df_map["사번"] = df_map["사번"].astype(str).str.zfill(5)

    # 데이터프레임을 출력
    st.dataframe(df_map.reset_index(drop=True), height=200)

    # 명단 추가 및 삭제 섹션 (단일 폼으로 처리)
    with st.form("fixed_form_namelist"):
        col_add, col_delete = st.columns([1.8, 1.2])

        # 🟢 명단 추가
        with col_add:
            st.markdown("**🟢 명단 추가**")
            col_name, col_id = st.columns(2)
            with col_name:
                new_employee_name = st.text_input("이름 입력", key="new_employee_name_input")
            with col_id:
                new_employee_id = st.number_input("5자리 사번 입력", min_value=0, max_value=99999, step=1, format="%05d")
            
            submit_add = st.form_submit_button("✔️ 추가")
            if submit_add:
                if not new_employee_name:
                    st.error("이름을 입력하세요.")
                elif new_employee_name in df_map["이름"].values:
                    st.error(f"이미 존재하는 이름입니다: {new_employee_name}님은 이미 목록에 있습니다.")
                else:
                    # 신규 클라이언트 생성
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(url)
                    
                    # 로컬 데이터 업데이트
                    new_mapping_row = pd.DataFrame([[new_employee_name, int(new_employee_id)]], columns=df_map.columns)
                    df_map = pd.concat([df_map, new_mapping_row], ignore_index=True).sort_values(by="이름")
                    update_sheet_with_retry(mapping, [df_map.columns.values.tolist()] + df_map.values.tolist())

                    new_row = pd.DataFrame({
                        "이름": [new_employee_name] * 5,
                        "주차": ["매주"] * 5,
                        "요일": ["월", "화", "수", "목", "금"],
                        "근무여부": ["근무없음"] * 5
                    })
                    df_master = pd.concat([df_master, new_row], ignore_index=True)
                    df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
                    df_master = df_master.sort_values(by=["이름", "주차", "요일"])
                    update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist())

                    new_worksheet2_row = pd.DataFrame([[new_employee_name, "요청 없음", ""]], columns=df_request.columns)
                    df_request = pd.concat([df_request, new_worksheet2_row], ignore_index=True)
                    update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())

                    # 세션 상태 업데이트
                    st.session_state["df_map"] = df_map
                    st.session_state["df_master"] = df_master
                    st.session_state["df_request"] = df_request
                    st.cache_data.clear()

                    st.success(f"{new_employee_name}님이 추가되었습니다!")
                    time.sleep(2)
                    st.rerun()

        # 🔴 명단 삭제
        with col_delete:
            st.markdown("**🔴 명단 삭제**")
            selected_employee_name = st.selectbox("이름 선택", df_map["이름"].unique() if not df_map.empty else [])
            
            submit_delete = st.form_submit_button("🗑️ 삭제")
            if submit_delete:
                df_map = df_map[df_map["이름"] != selected_employee_name]
                df_master = df_master[df_master["이름"] != selected_employee_name]
                df_request = df_request[df_request["이름"] != selected_employee_name]

                # 신규 클라이언트 생성
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                
                update_sheet_with_retry(mapping, [df_map.columns.values.tolist()] + df_map.values.tolist())
                update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist())
                update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())

                st.session_state["df_map"] = df_map
                st.session_state["df_master"] = df_master
                st.session_state["df_request"] = df_request
                st.cache_data.clear()

                st.success(f"{selected_employee_name}님이 삭제되었습니다!")
                time.sleep(2)
                st.rerun()

    # 마스터 관리 탭
    st.divider()
    st.subheader("📋 마스터 관리")
    st.write("- 셀을 클릭하면 해당 인원의 마스터 정보를 수정할 수 있습니다.")
    selected_employee_name = st.selectbox("이름 선택", df_master["이름"].unique() if not df_master.empty else [])
    df_employee = df_master[df_master["이름"] == selected_employee_name]

    근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
    요일리스트 = ["월", "화", "수", "목", "금"]
    gb = GridOptionsBuilder.from_dataframe(df_employee)
    gb.configure_column("근무여부", editable=True, cellEditor="agSelectCellEditor", cellEditorParams={"values": 근무옵션})
    gb.configure_column("이름", editable=False)
    gb.configure_column("주차", editable=True, cellEditor="agSelectCellEditor", cellEditorParams={"values": ["매주", "1주", "2주", "3주", "4주", "5주", "6주"]})
    gb.configure_column("요일", editable=True, cellEditor="agSelectCellEditor", cellEditorParams={"values": 요일리스트})
    gridOptions = gb.build()

    grid_return = AgGrid(df_employee, gridOptions=gridOptions, update_mode=GridUpdateMode.VALUE_CHANGED, fit_columns_on_grid_load=True, height=200)
    updated_df = grid_return["data"]

    if st.button("💾 저장", key="save"):
        df_master = df_master[df_master["이름"] != selected_employee_name]
        df_result = pd.concat([df_master, updated_df], ignore_index=True)
        df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df_result = df_result.sort_values(by=["이름", "주차", "요일"])

        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        worksheet1 = sheet.worksheet("마스터")
        update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist())
        st.session_state["df_master"] = df_result
        st.session_state["worksheet1"] = worksheet1
        st.cache_data.clear()

        st.success("✅ 수정사항이 저장되었습니다!")
        time.sleep(2)
        st.rerun()

    # 요청사항 관리 탭
    st.divider()
    st.subheader("📋 요청사항 관리")
    st.write("- 마스터에 등록되지 않은 인원은 관리자가 직접 요청사항을 추가해야 하며, 이름을 수기로 입력해 주세요.\n- '꼭 근무'로 요청된 사항은 해당 인원이 마스터에서 모두 '근무없음' 상태더라도 반드시 배정됩니다.")

    if df_request["분류"].nunique() == 1 and df_request["분류"].iloc[0] == '요청 없음':
        st.warning(f"⚠️ 아직까지 {month_str}에 작성된 요청사항이 없습니다.")

    요청분류 = ["휴가", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]
    st.dataframe(df_request.reset_index(drop=True), height=200)

    # 요청사항 관리 섹션 (단일 폼으로 처리)
    with st.form("fixed_form_request"):
        # 인원 추가/삭제 섹션
        st.markdown("**🙋‍♂️ 인원 추가/삭제**")
        col_add, col_delete = st.columns(2)

        # 인원 추가
        with col_add:
            new_employee_name = st.text_input("이름 입력", placeholder="추가할 이름을 입력하세요.")
            
            submit_add_employee = st.form_submit_button("✔️ 추가")
            if submit_add_employee:
                if not new_employee_name:
                    st.error("이름을 입력하세요.")
                elif new_employee_name in df_request["이름"].values:
                    st.error(f"이미 존재하는 이름입니다: {new_employee_name}님은 이미 목록에 있습니다.")
                else:
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(url)
                    worksheet2 = sheet.worksheet(f"{month_str} 요청")
                    
                    new_row = pd.DataFrame([{"이름": new_employee_name, "분류": "요청 없음", "날짜정보": ""}], columns=df_request.columns)
                    df_request = pd.concat([df_request, new_row], ignore_index=True)
                    df_request = df_request.sort_values(by=["이름", "날짜정보"])
                    update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
                    st.session_state["df_request"] = df_request
                    st.session_state["worksheet2"] = worksheet2
                    st.cache_data.clear()

                    st.success(f"{new_employee_name}님이 추가되었습니다!")
                    time.sleep(1)
                    st.rerun()

        # 인원 삭제
        with col_delete:
            selected_employee_name = st.selectbox("이름 선택", df_request["이름"].unique() if not df_request.empty else [])
            
            submit_delete_employee = st.form_submit_button("🗑️ 삭제")
            if submit_delete_employee:
                df_request = df_request[df_request["이름"] != selected_employee_name]
                
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet2 = sheet.worksheet(f"{month_str} 요청")
                
                update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
                st.session_state["df_request"] = df_request
                st.session_state["worksheet2"] = worksheet2
                st.cache_data.clear()

                st.success(f"{selected_employee_name}님이 삭제되었습니다!")
                time.sleep(1)
                st.rerun()

        # 요청사항 추가 섹션
        st.write(" ")
        st.markdown("**🟢 요청사항 추가**")
        row1_col1, row1_col2, row1_col3, row1_col4 = st.columns([1, 1, 1, 1.5])

        with row1_col1:
            이름 = st.selectbox("이름 선택", df_request["이름"].unique() if not df_request.empty else [], key="add_employee_select")

        with row1_col2:
            분류 = st.selectbox("요청 분류", 요청분류)

        날짜정보 = ""
        if 분류 != "요청 없음":
            with row1_col3:
                방식 = st.selectbox("날짜 선택 방식", ["일자 선택", "기간 선택", "주/요일 선택"])

            with row1_col4:
                if 방식 == "일자 선택":
                    날짜 = st.multiselect(
                        "요청 일자",
                        [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)],
                        format_func=lambda x: x.strftime("%Y-%m-%d"),
                    )
                    if 날짜:
                        날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜])
                
                elif 방식 == "기간 선택":
                    날짜범위 = st.date_input(
                        "요청 기간",
                        value=(next_month_start, next_month_start + datetime.timedelta(days=1)),
                        min_value=next_month_start,
                        max_value=next_month_end,
                    )
                    if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                        시작, 종료 = 날짜범위
                        날짜정보 = f"{시작.strftime('%Y-%m-%d')} ~ {종료.strftime('%Y-%m-%d')}"
                
                elif 방식 == "주/요일 선택":
                    선택주차 = st.multiselect("주차 선택", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"])
                    선택요일 = st.multiselect("요일 선택", ["월", "화", "수", "목", "금"])
                    주차_index = {"첫째주": 0, "둘째주": 1, "셋째주": 2, "넷째주": 3, "다섯째주": 4}
                    요일_index = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
                    날짜목록 = []
                    for i in range(1, last_day + 1):
                        날짜 = datetime.date(next_month_start.year, next_month_start.month, i)
                        weekday = 날짜.weekday()
                        week_of_month = (i - 1) // 7
                        if weekday in 요일_index.values():
                            for 주차 in 선택주차:
                                if 주차 == "매주" or 주차_index.get(주차) == week_of_month:
                                    for 요일 in 선택요일:
                                        if weekday == 요일_index[요일]:
                                            날짜목록.append(날짜.strftime("%Y-%m-%d"))
                    if 날짜목록:
                        날짜정보 = ", ".join(날짜목록)

        submit_add_request = st.form_submit_button("📅 추가")
        if submit_add_request:
            if not 이름:
                st.warning("이름을 입력해주세요.")
            elif 분류 == "요청 없음":
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet2 = sheet.worksheet(f"{month_str} 요청")
                
                df_request = df_request[df_request["이름"] != 이름]
                new_row = pd.DataFrame([{"이름": 이름, "분류": 분류, "날짜정보": ""}], columns=df_request.columns)
                df_request = pd.concat([df_request, new_row], ignore_index=True)
                df_request = df_request.sort_values(by=["이름", "날짜정보"])
                update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
                st.session_state["df_request"] = df_request
                st.session_state["worksheet2"] = worksheet2
                st.cache_data.clear()
                st.success("✅ 요청사항이 저장되었습니다!")
                time.sleep(1)
                st.rerun()
            elif 날짜정보:
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                worksheet2 = sheet.worksheet(f"{month_str} 요청")
                
                if not df_request[(df_request["이름"] == 이름) & (df_request["분류"] == "요청 없음")].empty:
                    df_request = df_request[~((df_request["이름"] == 이름) & (df_request["분류"] == "요청 없음"))]
                new_row = pd.DataFrame([{"이름": 이름, "분류": 분류, "날짜정보": 날짜정보}], columns=df_request.columns)
                df_request = pd.concat([df_request, new_row], ignore_index=True)
                df_request = df_request.sort_values(by=["이름", "날짜정보"])
                update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
                st.session_state["df_request"] = df_request
                st.session_state["worksheet2"] = worksheet2
                st.cache_data.clear()
                st.success("✅ 요청사항이 저장되었습니다!")
                time.sleep(1)
                st.rerun()
            else:
                st.warning("날짜 정보를 올바르게 입력해주세요.")

        # 요청사항 삭제 섹션
        st.write(" ")
        st.markdown("**🔴 요청사항 삭제**")
        if not df_request.empty:
            col0, col1 = st.columns([1, 2])
            with col0:
                selected_employee_id2 = st.selectbox("이름 선택", df_request["이름"].unique() if not df_request.empty else [], key="delete_employee_select")
            with col1:
                df_employee2 = df_request[df_request["이름"] == selected_employee_id2]
                df_employee2_filtered = df_employee2[df_employee2["분류"] != "요청 없음"]
                if not df_employee2_filtered.empty:
                    selected_rows = st.multiselect(
                        "요청사항 선택",
                        df_employee2_filtered.index,
                        format_func=lambda x: f"{df_employee2_filtered.loc[x, '분류']} - {df_employee2_filtered.loc[x, '날짜정보']}",
                    )
                else:
                    st.info("📍 선택한 이름에 대한 요청사항이 없습니다.")
                    selected_rows = []
        else:
            st.info("📍 당월 요청사항 없음")
            selected_rows = []

        submit_delete_request = st.form_submit_button("📅 삭제")
        if submit_delete_request and selected_rows:
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
            
            df_request = df_request.drop(index=selected_rows)
            is_user_empty = df_request[df_request["이름"] == selected_employee_id2].empty
            if is_user_empty:
                new_row = pd.DataFrame([{"이름": selected_employee_id2, "분류": "요청 없음", "날짜정보": ""}], columns=df_request.columns)
                df_request = pd.concat([df_request, new_row], ignore_index=True)
            df_request = df_request.sort_values(by=["이름", "날짜정보"])
            update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist())
            st.session_state["df_request"] = df_request
            st.session_state["worksheet2"] = worksheet2
            st.cache_data.clear()
            st.success("선택한 요청사항이 삭제되었습니다!")
            time.sleep(1)
            st.rerun()

else:
    st.warning("⚠️ 관리자 권한이 없습니다.")
    st.stop()