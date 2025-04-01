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

# Gspread 클라이언트 초기화 함수
def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    service_account_info = dict(st.secrets["gspread"])
    service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
    return gspread.authorize(credentials)

# Google Sheets 업데이트 함수 (쿼터 초과 대비)
def update_sheet_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update(data)
            return
        except APIError as e:
            if "Quota exceeded" in str(e):
                st.warning(f"API 쿼터 초과, {delay}초 후 재시도 ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                raise e
    st.error("API 쿼터 초과로 업데이트 실패")

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
        st.session_state["df_map"] = pd.DataFrame(mapping.get_all_records())

        # 마스터 시트
        worksheet1 = sheet.worksheet("마스터")
        st.session_state["worksheet1"] = worksheet1
        st.session_state["df_master"] = pd.DataFrame(worksheet1.get_all_records())
        
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
        st.session_state["worksheet2"] = worksheet2
        st.session_state["df_all2"] = pd.DataFrame(worksheet2.get_all_records())

        st.session_state["data_loaded"] = True
    except Exception as e:
        st.error(f"시트를 불러오는 데 문제가 발생했습니다: {e}")
        st.stop()

# 세션 상태에서 데이터 가져오기
mapping = st.session_state["mapping"]
df_map = st.session_state["df_map"]
worksheet1 = st.session_state["worksheet1"]
df_master = st.session_state["df_master"]
worksheet2 = st.session_state["worksheet2"]
df_all2 = st.session_state["df_all2"]
names_in_master = df_master["이름"].unique()

# 익월 범위 지정
today = datetime.datetime.strptime('2025-03-31', '%Y-%m-%d').date()
next_month = today.replace(day=1) + relativedelta(months=1)
next_month_start = next_month
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_end = next_month.replace(day=last_day)

if st.session_state.get("is_admin_authenticated", False):
    st.subheader("📁 스케쥴 시트 이동")
    st.markdown("https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?usp=sharing")

    st.divider()
    st.subheader("📋 명단 관리")
    st.write("- 매핑 시트와 마스터 시트에서 명단을 추가/삭제됩니다.")
    st.write("- 명단에 존재하는 인원만 시스템 로그인이 가능합니다.")

    # 세션 상태에서 df_all 관리
    if "df_all" not in st.session_state:
        st.session_state["df_all"] = df_master.copy()

    df_all = st.session_state["df_all"]

    st.dataframe(df_map, height=200)

    # # AgGrid 구성 및 출력
    # gb = GridOptionsBuilder.from_dataframe(df_map)
    # gridOptions = gb.build()
    # grid_return = AgGrid(df_map, gridOptions=gridOptions, update_mode=GridUpdateMode.VALUE_CHANGED, fit_columns_on_grid_load=True, width=100, height=200)

    # 명단 추가
    st.write(" ")
    st.markdown("🟢 명단 추가")
    col1, col2 = st.columns(2)
    with col1:
        new_employee_name = st.text_input("추가할 인원의 이름을 입력하세요.", key="new_employee_name_input")
    with col2:
        new_employee_id = st.number_input("추가할 인원의 5자리 사번을 입력하세요", min_value=0, max_value=99999, step=1, format="%05d")

    if st.button("✔️ 추가", key="add_button"):
        if not new_employee_name:
            st.error("이름을 입력하세요.")
        elif new_employee_name in df_map["이름"].values:
            st.error(f"이미 존재하는 이름입니다: {new_employee_name}님은 이미 목록에 있습니다.")
        else:
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
            df_all = pd.concat([df_all, new_row], ignore_index=True)
            df_all["요일"] = pd.Categorical(df_all["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_all = df_all.sort_values(by=["이름", "주차", "요일"])
            update_sheet_with_retry(worksheet1, [df_all.columns.values.tolist()] + df_all.values.tolist())

            new_worksheet2_row = pd.DataFrame([[new_employee_name, "요청 없음", ""]], columns=df_all2.columns)
            df_all2 = pd.concat([df_all2, new_worksheet2_row], ignore_index=True)
            update_sheet_with_retry(worksheet2, [df_all2.columns.tolist()] + df_all2.astype(str).values.tolist())

            # 세션 상태 업데이트
            st.session_state["df_map"] = df_map
            st.session_state["df_all"] = df_all
            st.session_state["df_all2"] = df_all2

            st.success(f"{new_employee_name}님이 추가되었습니다!")
            time.sleep(2)
            st.rerun()

    # 명단 삭제
    st.write(" ")
    st.markdown("🔴 명단 삭제")
    selected_employee_name = st.selectbox("삭제할 인원을 선택하세요.", df_map["이름"].unique())

    if st.button("💀 삭제", key="delete_button"):
        df_map = df_map[df_map["이름"] != selected_employee_name]
        df_all = df_all[df_all["이름"] != selected_employee_name]
        df_all2 = df_all2[df_all2["이름"] != selected_employee_name]

        update_sheet_with_retry(mapping, [df_map.columns.values.tolist()] + df_map.values.tolist())
        update_sheet_with_retry(worksheet1, [df_all.columns.values.tolist()] + df_all.values.tolist())
        update_sheet_with_retry(worksheet2, [df_all2.columns.tolist()] + df_all2.astype(str).values.tolist())

        st.session_state["df_map"] = df_map
        st.session_state["df_all"] = df_all
        st.session_state["df_all2"] = df_all2

        st.success(f"{selected_employee_name}님이 삭제되었습니다!")
        time.sleep(2)
        st.rerun()

    st.divider()
    st.subheader("📋 마스터 관리")
    selected_employee_name = st.selectbox("이름을 선택하여 마스터를 관리하세요.", df_all["이름"].unique())
    df_employee = df_all[df_all["이름"] == selected_employee_name]

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
        df_all = df_all[df_all["이름"] != selected_employee_name]
        df_result = pd.concat([df_all, updated_df], ignore_index=True)
        df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df_result = df_result.sort_values(by=["이름", "주차", "요일"])

        update_sheet_with_retry(worksheet1, [df_result.columns.values.tolist()] + df_result.values.tolist())
        st.session_state["df_all"] = df_result

        st.success("Google Sheets에 저장되었습니다 ✅")
        time.sleep(2)
        st.rerun()

    st.divider()
    st.subheader("📋 요청사항 관리")
    if df_all2["분류"].nunique() == 1 and df_all2["분류"].iloc[0] == '요청 없음':
        st.warning(f"⚠️ 아직까지 {month_str}에 작성된 요청사항이 없습니다.")

    # 요청분류 설정
    요청분류 = ["휴가", "학회", "보충 어려움(오전)", "보충 어려움(오후)", "보충 불가(오전)", "보충 불가(오후)", "꼭 근무(오전)", "꼭 근무(오후)", "요청 없음"]

    st.dataframe(df_all2, height=200)
    
    st.write(" ")
    st.markdown("🟢 요청사항 추가")
    col0, col1, col2 = st.columns(3)
    selected_employee_id2 = col0.selectbox("이름 선택", df_all2["이름"].unique())
    분류 = col1.selectbox("요청 분류", 요청분류)
    방식 = ""
    if 분류 != "요청 없음":
        방식 = col2.selectbox("날짜 선택 방식", ["일자 선택", "기간 선택", "주/요일 선택"])

    df_employee2 = df_all2[df_all2["이름"] == selected_employee_id2]
    날짜정보 = ""
    if 분류 != "요청 없음":
        if 방식 == "일자 선택":
            날짜 = st.multiselect(
                "요청 일자", 
                [next_month_start + datetime.timedelta(days=i) for i in range((next_month_end - next_month_start).days + 1)],
                format_func=lambda x: x.strftime("%Y-%m-%d")  # 날짜 형식 지정
            )
            if 날짜:  # 선택된 날짜가 있을 경우
                날짜정보 = ", ".join([d.strftime("%Y-%m-%d") for d in 날짜])  # 여러 날짜 선택 시, ','로 구분하여 날짜정보에 할당
        elif 방식 == "기간 선택":
            날짜범위 = st.date_input("요청 기간", value=(next_month_start, next_month_start + datetime.timedelta(days=1)), min_value=next_month_start, max_value=next_month_end)
            if isinstance(날짜범위, tuple) and len(날짜범위) == 2:
                시작, 종료 = 날짜범위
                날짜정보 = f"{시작.strftime('%Y-%m-%d')} ~ {종료.strftime('%Y-%m-%d')}"
        elif 방식 == "주/요일 선택":
            선택주차 = st.multiselect("해당 주차를 선택하세요", ["첫째주", "둘째주", "셋째주", "넷째주", "다섯째주", "매주"])
            선택요일 = st.multiselect("해당 요일을 선택하세요", ["월", "화", "수", "목", "금"])
            주차_index = {"첫째주": 0, "둘째주": 1, "셋째주": 2, "넷째주": 3, "다섯째주": 4}
            요일_index = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4}
            날짜목록 = []
            for i in range(1, last_day + 1):
                날짜 = datetime.date(next_month.year, next_month.month, i)
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

    if st.button("📅 추가"):
        if 분류 == "요청 없음":
            df_all2 = df_all2[df_all2["이름"] != selected_employee_id2]
            new_row = pd.DataFrame([{"이름": selected_employee_id2, "분류": 분류, "날짜정보": ""}], columns=df_all2.columns)
            df_all2 = pd.concat([df_all2, new_row], ignore_index=True)
        elif 날짜정보:
            if not df_employee2[df_employee2["분류"] == "요청 없음"].empty:
                df_all2 = df_all2[~((df_all2["이름"] == selected_employee_id2) & (df_all2["분류"] == "요청 없음"))]
            new_row = pd.DataFrame([{"이름": selected_employee_id2, "분류": 분류, "날짜정보": 날짜정보}], columns=df_all2.columns)
            df_all2 = pd.concat([df_all2, new_row], ignore_index=True)
        else:
            st.warning("날짜 정보를 올바르게 입력해주세요.")
            st.stop()

        df_all2 = df_all2.sort_values(by=["이름", "날짜정보"])
        update_sheet_with_retry(worksheet2, [df_all2.columns.tolist()] + df_all2.astype(str).values.tolist())
        st.session_state["df_all2"] = df_all2

        st.success("✅ 요청사항이 저장되었습니다!")
        time.sleep(2)
        st.rerun()

    st.write(" ")
    st.markdown("🔴 요청사항 삭제")
    if not df_all2.empty:
        col0, col1 = st.columns(2)
        selected_employee_id2 = col0.selectbox("이름 선택", df_all2["이름"].unique(), key="delete_employee_select")
        df_employee2 = df_all2[df_all2["이름"] == selected_employee_id2]
        df_employee2_filtered = df_employee2[df_employee2["분류"] != "요청 없음"]
        selected_rows = col1.multiselect(
            "요청사항 선택",
            df_employee2_filtered.index,
            format_func=lambda x: f"{df_employee2_filtered.loc[x, '분류']} - {df_employee2_filtered.loc[x, '날짜정보']}"
        )
    else:
        st.info("📍 당월 요청사항 없음")
        selected_rows = []

    if st.button("🗑️ 삭제") and selected_rows:
        # 선택된 요청사항 삭제
        df_all2 = df_all2.drop(index=selected_rows)

        # 해당 사용자의 요청사항이 모두 삭제된 경우 "요청 없음" 추가
        is_user_empty = df_all2[df_all2["이름"] == selected_employee_id2].empty
        if is_user_empty:
            new_row = pd.DataFrame([{"이름": selected_employee_id2, "분류": "요청 없음", "날짜정보": ""}], columns=df_all2.columns)
            df_all2 = pd.concat([df_all2, new_row], ignore_index=True)

        # 이름과 날짜정보를 기준으로 정렬
        df_all2 = df_all2.sort_values(by=["이름", "날짜정보"])

        # 시트 업데이트
        update_sheet_with_retry(worksheet2, [df_all2.columns.tolist()] + df_all2.astype(str).values.tolist())
        
        # 세션에 데이터 저장
        st.session_state["df_all2"] = df_all2

        st.success("선택한 요청사항이 삭제되었습니다!")
        time.sleep(2)
        st.rerun()

    if df_all2.empty:
        st.info("📍 당월 요청사항 없음")

else:
    st.warning("관리자 권한이 없습니다.")
    st.stop()