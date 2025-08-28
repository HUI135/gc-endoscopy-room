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
import uuid
import menu
import io
from collections import Counter

st.set_page_config(page_title="스케줄 관리", page_icon="⚙️", layout="wide")

st.header("⚙️ 스케줄 관리", divider='rainbow')

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

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
    week_labels = [f"{i}주" for i in range(1, 6)]  # 최대 5주 가정
    result = {}

    for day in weekday_order:
        for time in time_slots:
            key = f"{day} {time}"
            employees = {}
            for name in df_split["이름"].unique():
                every_week = df_split[
                    (df_split["이름"] == name) & 
                    (df_split["요일"] == day) & 
                    (df_split["시간대"] == time) & 
                    (df_split["주차"] == "매주")
                ]
                specific_weeks = sorted(
                    df_split[
                        (df_split["이름"] == name) & 
                        (df_split["요일"] == day) & 
                        (df_split["시간대"] == time) & 
                        (df_split["주차"].isin(week_labels))
                    ]["주차"].tolist(),
                    key=lambda x: int(x.replace("주", ""))
                )
                if not every_week.empty:
                    employees[name] = None
                elif specific_weeks:
                    if set(specific_weeks) == set(week_labels):
                        employees[name] = None
                    else:
                        employees[name] = specific_weeks

            employee_list = []
            for name, weeks in employees.items():
                if weeks:
                    employee_list.append(f"{name}({','.join(weeks)})")
                else:
                    employee_list.append(name)
            
            result[key] = ", ".join(sorted(employee_list)) if employee_list else ""
    
    return pd.DataFrame(list(result.items()), columns=["시간대", "근무"])

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
    if column_name not in df.columns:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.stop()
        return df
    
    split_data = df[column_name].str.split(", ", expand=True)
    max_cols = split_data.shape[1]
    new_columns = [f"{prefix}{i+1}" for i in range(max_cols)]
    split_data.columns = new_columns
    df = df.drop(columns=[column_name])
    df = pd.concat([df, split_data], axis=1)
    return df

# Google Sheets 클라이언트 초기화
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        service_account_info = dict(st.secrets["gspread"])
        service_account_info["private_key"] = service_account_info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scope)
        return gspread.authorize(credentials)
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (클라이언트 초기화): {str(e)}")
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"Google Sheets 인증 정보를 불러오는 데 실패했습니다: {str(e)}")
        st.stop()

# Google Sheets 업데이트 함수
def update_sheet_with_retry(worksheet, data, retries=3, delay=5):
    for attempt in range(retries):
        try:
            worksheet.clear()
            worksheet.update(data, "A1")
            return True
        except gspread.exceptions.APIError as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ API 요청이 지연되고 있습니다. {delay}초 후 재시도합니다... ({attempt+1}/{retries})")
                time.sleep(delay)
                delay *= 2
            else:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (시트 업데이트): {str(e)}")
                st.stop()
        except Exception as e:
            if attempt < retries - 1:
                st.warning(f"⚠️ 업데이트 실패, {delay}초 후 재시도 ({attempt+1}/{retries}): {str(e)}")
                time.sleep(delay)
                delay *= 2
            else:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"Google Sheets 업데이트 실패: {str(e)}")
                st.stop()
    return False

def load_request_data_page4():
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        
        # 매핑 시트 로드
        mapping = sheet.worksheet("매핑")
        st.session_state["mapping"] = mapping
        mapping_values = mapping.get_all_values()
        if not mapping_values or len(mapping_values) <= 1:
            df_map = pd.DataFrame(columns=["이름", "사번"])
        else:
            headers = mapping_values[0]
            data = mapping_values[1:]
            df_map = pd.DataFrame(data, columns=headers)
            if "이름" in df_map.columns and "사번" in df_map.columns:
                df_map = df_map[["이름", "사번"]]
            else:
                df_map = pd.DataFrame(columns=["이름", "사번"])
        
        if df_map.empty:
            st.error("매핑 시트에 데이터가 없습니다. 스케줄 관리를 진행할 수 없습니다.")
            st.session_state["df_map"] = df_map
            return False
        
        st.session_state["df_map"] = df_map
        
        # 요청사항 시트 로드
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 요청' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
            worksheet2.append_row(["이름", "분류", "날짜정보"])
        request_data = worksheet2.get_all_records()
        df_request = pd.DataFrame(request_data) if request_data else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["df_request"] = df_request
        st.session_state["worksheet2"] = worksheet2
        
        # 마스터 시트 로드
        worksheet1 = sheet.worksheet("마스터")
        master_data = worksheet1.get_all_records()
        df_master = pd.DataFrame(master_data) if master_data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
        df_master = df_master.sort_values(by=["이름", "주차", "요일"])
        st.session_state["df_master"] = df_master
        st.session_state["worksheet1"] = worksheet1
        
        # 누적 시트 로드
        try:
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 누적' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
            worksheet4.append_row(["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
            names_in_master = df_master["이름"].unique()
            new_rows = [[name, 0, 0, 0, 0] for name in names_in_master]
            for row in new_rows:
                worksheet4.append_row(row)
        df_cumulative_temp = pd.DataFrame(worksheet4.get_all_records()) if worksheet4.get_all_records() else pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
        if not df_cumulative_temp.empty:
            df_cumulative_temp.rename(columns={df_cumulative_temp.columns[0]: '이름'}, inplace=True)
            for col_name in ["오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]:
                if col_name in df_cumulative_temp.columns:
                    df_cumulative_temp[col_name] = pd.to_numeric(df_cumulative_temp[col_name], errors='coerce').fillna(0).astype(int)
        st.session_state["df_cumulative"] = df_cumulative_temp
        st.session_state["edited_df_cumulative"] = df_cumulative_temp.copy()
        st.session_state["worksheet4"] = worksheet4
        
        # 근무 및 보충 테이블 생성
        st.session_state["df_shift"] = generate_shift_table(df_master)
        st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], df_master["이름"].unique())
        
        return True

    except APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (데이터 로드): {str(e)}")
        return False
    except WorksheetNotFound as e:
        st.error(f"필수 시트를 찾을 수 없습니다: {e}. '매핑'과 '마스터' 시트가 있는지 확인해주세요.")
        return False
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"데이터 로드 중 오류 발생: {str(e)}")
        return False

# 초기 데이터 로드 및 세션 상태 설정
url = st.secrets["google_sheet"]["url"]
today = datetime.date.today()
month_str = (today.replace(day=1) + relativedelta(months=1)).strftime("%Y년 %-m월")

if st.button("🔄 새로고침 (R)"):
    success = False
    with st.spinner("데이터를 다시 불러오는 중입니다..."):
        try:
            success = load_request_data_page4()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"새로고침 중 예측하지 못한 오류 발생: {str(e)}")
            success = False
    
    if success:
        st.session_state["data_loaded"] = True
        st.success("데이터가 성공적으로 새로고침되었습니다!")
        time.sleep(1)
        st.rerun()
        
if "data_loaded" not in st.session_state:
    try:
        gc = get_gspread_client()
        sheet = gc.open_by_url(url)
        
        mapping = sheet.worksheet("매핑")
        st.session_state["mapping"] = mapping
        mapping_data = mapping.get_all_records()
        df_map = pd.DataFrame(mapping_data) if mapping_data else pd.DataFrame(columns=["이름", "사번"])
        
        if df_map.empty:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error("매핑 시트에 데이터가 없습니다.")
            st.session_state["df_map"] = df_map
            st.session_state["data_loaded"] = False
            st.stop()
            
        st.session_state["df_map"] = df_map
        
        worksheet1 = sheet.worksheet("마스터")
        st.session_state["worksheet1"] = worksheet1
        master_data = worksheet1.get_all_records()
        df_master = pd.DataFrame(master_data) if master_data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        st.session_state["df_master"] = df_master
        
        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except gspread.exceptions.WorksheetNotFound:
            try:
                worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                worksheet2.append_row(["이름", "분류", "날짜정보"])
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (시트 생성): {str(e)}")
                st.stop()
        st.session_state["worksheet2"] = worksheet2
        load_request_data_page4()

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
            if not update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist()):
                st.error("마스터 시트 업데이트 실패")
                st.session_state["df_map"] = df_map
                st.session_state["data_loaded"] = False
                st.stop()
            st.session_state["df_master"] = df_master

        missing_in_request = set(df_master["이름"]) - set(st.session_state["df_request"]["이름"])
        if missing_in_request:
            new_request_rows = [{"이름": name, "분류": "요청 없음", "날짜정보": ""} for name in missing_in_request]
            new_request_df = pd.DataFrame(new_request_rows)
            df_request = pd.concat([st.session_state["df_request"], new_request_df], ignore_index=True)
            df_request = df_request.sort_values(by=["이름", "날짜정보"])
            if not update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist()):
                st.error("요청사항 시트 업데이트 실패")
                st.session_state["df_map"] = df_map
                st.session_state["data_loaded"] = False
                st.stop()
            st.session_state["df_request"] = df_request

        st.session_state["data_loaded"] = True
        
    except gspread.exceptions.APIError as e:
        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
        st.error(f"Google Sheets API 오류 (초기 데이터 로드): {str(e)}")
        st.session_state["df_map"] = pd.DataFrame(columns=["이름", "사번"])
        st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        st.session_state["df_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["data_loaded"] = False
        st.stop()
    except Exception as e:
        st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
        st.error(f"시트를 불러오는 데 문제가 발생했습니다: {str(e)}")
        st.session_state["df_map"] = pd.DataFrame(columns=["이름", "사번"])
        st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
        st.session_state["df_request"] = pd.DataFrame(columns=["이름", "분류", "날짜정보"])
        st.session_state["data_loaded"] = False
        st.stop()

def load_data_page4():
    required_keys = ["df_master", "df_request", "df_cumulative", "df_shift", "df_supplement"]
    if "data_loaded" not in st.session_state or not st.session_state["data_loaded"] or not all(key in st.session_state for key in required_keys):
        url = st.secrets["google_sheet"]["url"]
        try:
            gc = get_gspread_client()
            if gc is None:
                st.stop()
            sheet = gc.open_by_url(url)
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (스프레드시트 열기): {e.response.status_code} - {e.response.text}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"스프레드시트 열기 실패: {type(e).__name__} - {e}")
            st.stop()

        try:
            worksheet1 = sheet.worksheet("마스터")
            master_data = worksheet1.get_all_records()
            df_master = pd.DataFrame(master_data) if master_data else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_master = df_master.sort_values(by=["이름", "주차", "요일"])
            st.session_state["df_master"] = df_master
            st.session_state["worksheet1"] = worksheet1
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 ('마스터' 시트 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except gspread.exceptions.WorksheetNotFound:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error("❌ '마스터' 시트를 찾을 수 없습니다. 시트 이름을 확인해주세요.")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"'마스터' 시트 로드 실패: {type(e).__name__} - {e}")
            st.session_state["df_master"] = pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            st.session_state["data_loaded"] = False
            st.stop()

        try:
            worksheet2 = sheet.worksheet(f"{month_str} 요청")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 요청' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            try:
                worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                worksheet2.append_row(["이름", "분류", "날짜정보"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "요청 없음", ""] for name in names_in_master]
                for row in new_rows:
                    try:
                        worksheet2.append_row(row)
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (요청사항 시트 초기화): {e.response.status_code} - {e.response.text}")
                        st.stop()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 ('{month_str} 요청' 시트 생성): {e.response.status_code} - {e.response.text}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 요청' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.stop()

        try:
            st.session_state["df_request"] = pd.DataFrame(worksheet2.get_all_records()) if worksheet2.get_all_records() else pd.DataFrame(columns=["이름", "분류", "날짜정보"])
            st.session_state["worksheet2"] = worksheet2
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (요청사항 데이터 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"요청사항 데이터 로드 실패: {type(e).__name__} - {e}")
            st.stop()

        try:
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"⚠️ '{month_str} 누적' 시트를 찾을 수 없습니다. 시트를 새로 생성합니다.")
            try:
                worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
                worksheet4.append_row([f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                names_in_master = st.session_state["df_master"]["이름"].unique()
                new_rows = [[name, "", "", "", ""] for name in names_in_master]
                for row in new_rows:
                    try:
                        worksheet4.append_row(row)
                    except gspread.exceptions.APIError as e:
                        st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                        st.error(f"Google Sheets API 오류 (누적 시트 초기화): {e.response.status_code} - {e.response.text}")
                        st.stop()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 ('{month_str} 누적' 시트 생성): {e.response.status_code} - {e.response.text}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"'{month_str} 누적' 시트 생성/초기화 실패: {type(e).__name__} - {e}")
                st.stop()
        
        try:
            df_cumulative_temp = pd.DataFrame(worksheet4.get_all_records()) if worksheet4.get_all_records() else pd.DataFrame(columns=[f"{month_str}", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
            if not df_cumulative_temp.empty:
                df_cumulative_temp.rename(columns={df_cumulative_temp.columns[0]: '이름'}, inplace=True)
                for col_name in ["오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]:
                    if col_name in df_cumulative_temp.columns:
                        df_cumulative_temp[col_name] = pd.to_numeric(df_cumulative_temp[col_name], errors='coerce').fillna(0).astype(int)
            st.session_state["df_cumulative"] = df_cumulative_temp
            st.session_state["worksheet4"] = worksheet4
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (누적 데이터 로드): {e.response.status_code} - {e.response.text}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"누적 데이터 로드 실패: {type(e).__name__} - {e}")
            st.stop()

        try:
            st.session_state["df_shift"] = generate_shift_table(st.session_state["df_master"])
            st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], st.session_state["df_master"]["이름"].unique())
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"근무/보충 테이블 생성 실패: {type(e).__name__} - {e}")
            st.stop()

        st.session_state["data_loaded"] = True

# 세션 상태에서 데이터 가져오기
df_map = st.session_state.get("df_map", pd.DataFrame(columns=["이름", "사번"]))
mapping = st.session_state.get("mapping")
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
worksheet1 = st.session_state.get("worksheet1")
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]))
worksheet4 = st.session_state.get("worksheet4")
names_in_master = df_master["이름"].unique() if not df_master.empty else []

today = datetime.date.today()
next_month = today.replace(day=1) + relativedelta(months=1)
_, last_day = calendar.monthrange(next_month.year, next_month.month)
next_month_start = next_month
next_month_end = next_month.replace(day=last_day)

st.write(" ")
st.subheader("📁 스케줄 시트 이동")
st.markdown("https://docs.google.com/spreadsheets/d/1Y32fb0fGU5UzldiH-nwXa1qnb-ePdrfTHGnInB06x_A/edit?usp=sharing")

st.divider()
st.subheader("📋 명단 관리")
st.write(" - 매핑 시트, 마스터 시트, 요청사항 시트, 누적 시트에서 인원을 추가/삭제합니다.\n- 아래 명단에 존재하는 인원만 해당 사번으로 시스템 로그인이 가능합니다.")

if "df_master" not in st.session_state or st.session_state["df_master"].empty:
    st.session_state["df_master"] = df_master.copy() if not df_master.empty else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])

df_master = st.session_state["df_master"]

if not df_map.empty:
    df_map["사번"] = df_map["사번"].astype(str).str.zfill(5)

st.dataframe(df_map.reset_index(drop=True), height=200, width=500, use_container_width=True, hide_index=True)

if "add_transaction_id" not in st.session_state:
    st.session_state["add_transaction_id"] = None

with st.form("fixed_form_namelist"):
    col_add, col_delete = st.columns([1.8, 1.2])

    with col_add:
        st.markdown("**🟢 명단 추가**")
        col_name, col_id = st.columns(2)
        with col_name:
            new_employee_name = st.text_input("이름 입력", key="new_employee_name_input")
        with col_id:
            new_employee_id = st.number_input("5자리 사번 입력", min_value=0, max_value=99999, step=1, format="%05d")
        
        submit_add = st.form_submit_button("✔️ 추가")
        if submit_add:
            try:
                transaction_id = str(uuid.uuid4())
                if st.session_state["add_transaction_id"] == transaction_id:
                    st.warning("이미 처리된 추가 요청입니다. 새로고침 후 다시 시도하세요.")
                elif not new_employee_name:
                    st.error("이름을 입력하세요.")
                elif new_employee_name in df_map["이름"].values:
                    st.error(f"이미 존재하는 이름입니다: {new_employee_name}님은 이미 목록에 있습니다.")
                else:
                    st.session_state["add_transaction_id"] = transaction_id
                    gc = get_gspread_client()
                    sheet = gc.open_by_url(url)
                    
                    # 매핑 시트 업데이트
                    new_mapping_row = pd.DataFrame([[new_employee_name, int(new_employee_id)]], columns=df_map.columns)
                    df_map = pd.concat([df_map, new_mapping_row], ignore_index=True).sort_values(by="이름")
                    if not update_sheet_with_retry(mapping, [df_map.columns.values.tolist()] + df_map.values.tolist()):
                        st.error("매핑 시트 업데이트 실패")
                        st.stop()

                    # 마스터 시트 업데이트
                    new_row = pd.DataFrame({
                        "이름": [new_employee_name] * 5,
                        "주차": ["매주"] * 5,
                        "요일": ["월", "화", "수", "목", "금"],
                        "근무여부": ["근무없음"] * 5
                    })
                    df_master = pd.concat([df_master, new_row], ignore_index=True)
                    df_master["요일"] = pd.Categorical(df_master["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
                    df_master = df_master.sort_values(by=["이름", "주차", "요일"])
                    if not update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist()):
                        st.error("마스터 시트 업데이트 실패")
                        st.stop()

                    # 요청사항 시트 업데이트
                    if "worksheet2" not in st.session_state or st.session_state["worksheet2"] is None:
                        try:
                            worksheet2 = sheet.worksheet(f"{month_str} 요청")
                        except gspread.exceptions.WorksheetNotFound:
                            try:
                                worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                                worksheet2.append_row(["이름", "분류", "날짜정보"])
                            except gspread.exceptions.APIError as e:
                                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                                st.error(f"Google Sheets API 오류 (요청사항 시트 생성): {str(e)}")
                                st.stop()
                        st.session_state["worksheet2"] = worksheet2
                    else:
                        worksheet2 = st.session_state["worksheet2"]

                    new_worksheet2_row = pd.DataFrame([[new_employee_name, "요청 없음", ""]], columns=df_request.columns)
                    df_request = pd.concat([df_request, new_worksheet2_row], ignore_index=True)
                    if not update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist()):
                        st.error("요청사항 시트 업데이트 실패")
                        st.stop()

                    # 누적 시트 업데이트
                    if "worksheet4" not in st.session_state or st.session_state["worksheet4"] is None:
                        try:
                            worksheet4 = sheet.worksheet(f"{month_str} 누적")
                        except gspread.exceptions.WorksheetNotFound:
                            try:
                                worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
                                worksheet4.append_row(["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                            except gspread.exceptions.APIError as e:
                                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                                st.error(f"Google Sheets API 오류 (누적 시트 생성): {str(e)}")
                                st.stop()
                        st.session_state["worksheet4"] = worksheet4
                    else:
                        worksheet4 = st.session_state["worksheet4"]

                    new_cumulative_row = pd.DataFrame([[new_employee_name, 0, 0, 0, 0]], columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                    df_cumulative = pd.concat([df_cumulative, new_cumulative_row], ignore_index=True)
                    if not update_sheet_with_retry(worksheet4, [df_cumulative.columns.tolist()] + df_cumulative.values.tolist()):
                        st.error("누적 시트 업데이트 실패")
                        st.stop()

                    st.session_state["df_map"] = df_map
                    st.session_state["df_master"] = df_master
                    st.session_state["df_request"] = df_request
                    st.session_state["df_cumulative"] = df_cumulative
                    st.session_state["edited_df_cumulative"] = df_cumulative.copy()
                    st.success(f"{new_employee_name}님을 명단 및 누적 테이블에 추가하였습니다.")
                    time.sleep(1.5)
                    st.rerun()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (명단 추가): {str(e)}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"명단 추가 중 오류 발생: {str(e)}")
                st.stop()

    with col_delete:
        st.markdown("**🔴 명단 삭제**")
        sorted_names = sorted(df_map["이름"].unique()) if not df_map.empty else []
        selected_employee_name = st.selectbox("이름 선택", sorted_names, key="delete_employee_select")
        
        submit_delete = st.form_submit_button("🗑️ 삭제")
        if submit_delete:
            try:
                gc = get_gspread_client()
                sheet = gc.open_by_url(url)
                
                # 매핑 시트 업데이트
                df_map = df_map[df_map["이름"] != selected_employee_name]
                if not update_sheet_with_retry(mapping, [df_map.columns.values.tolist()] + df_map.values.tolist()):
                    st.error("매핑 시트 업데이트 실패")
                    st.stop()

                # 마스터 시트 업데이트
                df_master = df_master[df_master["이름"] != selected_employee_name]
                if not update_sheet_with_retry(worksheet1, [df_master.columns.tolist()] + df_master.values.tolist()):
                    st.error("마스터 시트 업데이트 실패")
                    st.stop()

                # 요청사항 시트 업데이트
                if "worksheet2" not in st.session_state or st.session_state["worksheet2"] is None:
                    try:
                        worksheet2 = sheet.worksheet(f"{month_str} 요청")
                    except gspread.exceptions.WorksheetNotFound:
                        try:
                            worksheet2 = sheet.add_worksheet(title=f"{month_str} 요청", rows="100", cols="20")
                            worksheet2.append_row(["이름", "분류", "날짜정보"])
                        except gspread.exceptions.APIError as e:
                            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                            st.error(f"Google Sheets API 오류 (요청사항 시트 생성): {str(e)}")
                            st.stop()
                    st.session_state["worksheet2"] = worksheet2
                else:
                    worksheet2 = st.session_state["worksheet2"]

                df_request = df_request[df_request["이름"] != selected_employee_name]
                if not update_sheet_with_retry(worksheet2, [df_request.columns.tolist()] + df_request.astype(str).values.tolist()):
                    st.error("요청사항 시트 업데이트 실패")
                    st.stop()

                # 누적 시트 업데이트
                if "worksheet4" not in st.session_state or st.session_state["worksheet4"] is None:
                    try:
                        worksheet4 = sheet.worksheet(f"{month_str} 누적")
                    except gspread.exceptions.WorksheetNotFound:
                        try:
                            worksheet4 = sheet.add_worksheet(title=f"{month_str} 누적", rows="100", cols="20")
                            worksheet4.append_row(["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"])
                        except gspread.exceptions.APIError as e:
                            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                            st.error(f"Google Sheets API 오류 (누적 시트 생성): {str(e)}")
                            st.stop()
                    st.session_state["worksheet4"] = worksheet4
                else:
                    worksheet4 = st.session_state["worksheet4"]

                df_cumulative = df_cumulative[df_cumulative["이름"] != selected_employee_name]
                if not update_sheet_with_retry(worksheet4, [df_cumulative.columns.tolist()] + df_cumulative.values.tolist()):
                    st.error("누적 시트 업데이트 실패")
                    st.stop()

                st.session_state["df_map"] = df_map
                st.session_state["df_master"] = df_master
                st.session_state["df_request"] = df_request
                st.session_state["df_cumulative"] = df_cumulative
                st.session_state["edited_df_cumulative"] = df_cumulative.copy()
                st.success(f"{selected_employee_name}님을 명단 및 누적 테이블에서 삭제하였습니다.")
                time.sleep(1.5)
                st.rerun()
            except gspread.exceptions.APIError as e:
                st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
                st.error(f"Google Sheets API 오류 (명단 삭제): {str(e)}")
                st.stop()
            except Exception as e:
                st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
                st.error(f"명단 삭제 중 오류 발생: {str(e)}")
                st.stop()


st.divider()
st.subheader("📋 마스터 관리")
st.write("- 월 단위 또는 주 단위로 선택한 인원의 마스터 스케줄을 수정할 수 있습니다.")

sorted_names = sorted(df_master["이름"].unique()) if not df_master.empty else []
selected_employee_name = st.selectbox("이름 선택", sorted_names, key="master_employee_select")
df_user_master = df_master[df_master["이름"] == selected_employee_name].copy()

근무옵션 = ["오전", "오후", "오전 & 오후", "근무없음"]
요일리스트 = ["월", "화", "수", "목", "금"]

today = datetime.date.today()
next_month = today.replace(day=1) + relativedelta(months=1)
_, last_day = calendar.monthrange(next_month.year, next_month.month)
c = calendar.Calendar(firstweekday=6)
month_calendar = c.monthdatescalendar(next_month.year, next_month.month)
week_nums = [i + 1 for i, _ in enumerate(month_calendar) if any(date.month == next_month.month for date in month_calendar[i])]

# 월 단위로 일괄 설정
with st.expander("📅 월 단위로 일괄 설정"):
    has_weekly = "매주" in df_user_master["주차"].values
    has_weekly_specific = any(w in df_user_master["주차"].values for w in [f"{i}주" for i in week_nums])
    
    # 기본값 설정: df_user_master에서 최신 데이터 가져오기
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    default_bulk = {}
    if has_weekly_specific:
        week_labels = [f"{i}주" for i in week_nums]
        for day in 요일리스트:
            day_values = []
            for week in week_labels:
                week_df = df_user_master[df_user_master["주차"] == week]
                day_specific = week_df[week_df["요일"] == day]
                if not day_specific.empty:
                    day_values.append(day_specific.iloc[0]["근무여부"])
                elif not every_week_df.empty:
                    day_every = every_week_df[every_week_df["요일"] == day]
                    if not day_every.empty:
                        day_values.append(day_every.iloc[0]["근무여부"])
                    else:
                        day_values.append("근무없음")
                else:
                    day_values.append("근무없음")
            if day_values:
                if all(v == day_values[0] for v in day_values):
                    default_bulk[day] = day_values[0]
                else:
                    most_common = Counter(day_values).most_common(1)[0][0]
                    default_bulk[day] = most_common
            else:
                default_bulk[day] = "근무없음"
    elif has_weekly:
        weekly_df = df_user_master[df_user_master["주차"] == "매주"]
        default_bulk = weekly_df.set_index("요일")["근무여부"].to_dict()
    # For missing days, set to "근무없음"
    for day in 요일리스트:
        if day not in default_bulk:
            default_bulk[day] = "근무없음"

    if has_weekly and all(df_user_master[df_user_master["주차"] == "매주"]["근무여부"] == "근무없음"):
        st.info("마스터 입력이 필요합니다.")
    elif has_weekly_specific:
        st.warning("현재 주차별 근무 일정이 다릅니다. 주 단위 스케줄을 확인하신 후, 월 단위로 초기화하려면 내용을 입력하세요.")

    col1, col2, col3, col4, col5 = st.columns(5)
    월값 = col1.selectbox("월", 근무옵션, index=근무옵션.index(default_bulk.get("월", "근무없음")), key=f"월_bulk_{selected_employee_name}")
    화값 = col2.selectbox("화", 근무옵션, index=근무옵션.index(default_bulk.get("화", "근무없음")), key=f"화_bulk_{selected_employee_name}")
    수값 = col3.selectbox("수", 근무옵션, index=근무옵션.index(default_bulk.get("수", "근무없음")), key=f"수_bulk_{selected_employee_name}")
    목값 = col4.selectbox("목", 근무옵션, index=근무옵션.index(default_bulk.get("목", "근무없음")), key=f"목_bulk_{selected_employee_name}")
    금값 = col5.selectbox("금", 근무옵션, index=근무옵션.index(default_bulk.get("금", "근무없음")), key=f"금_bulk_{selected_employee_name}")

    if st.button("💾 월 단위 저장", key="save_monthly"):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            
            # 월 단위 데이터로 덮어쓰기
            rows = [{"이름": selected_employee_name, "주차": "매주", "요일": 요일, "근무여부": {"월": 월값, "화": 화값, "수": 수값, "목": 목값, "금": 금값}[요일]} for 요일 in 요일리스트]
            updated_df = pd.DataFrame(rows)
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            
            df_master = df_master[df_master["이름"] != selected_employee_name]
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            
            if update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist()):
                st.session_state["df_master"] = df_result
                st.session_state["worksheet1"] = worksheet1
                st.session_state["df_user_master"] = df_result[df_result["이름"] == selected_employee_name].copy()
                
                with st.spinner("근무 및 보충 테이블 갱신 중..."):
                    st.session_state["df_shift"] = generate_shift_table(df_result)
                    st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], df_result["이름"].unique())
                
                st.success("월 단위 수정사항이 저장되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("마스터 시트 저장 실패")
                st.stop()
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (월 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"월 단위 저장 중 오류 발생: {str(e)}")
            st.stop()

with st.expander("📅 주 단위로 설정"):
    st.markdown("**주 단위로 근무 여부가 다른 경우 아래 내용들을 입력해주세요.**")
    week_labels = [f"{i}주" for i in week_nums]
    
    # 최신 df_user_master 가져오기
    df_user_master = df_master[df_master["이름"] == selected_employee_name].copy()
    st.session_state["df_user_master"] = df_user_master
    
    # master_data 초기화: 요일별로 체크
    master_data = {}
    every_week_df = df_user_master[df_user_master["주차"] == "매주"]
    for week in week_labels:
        master_data[week] = {}
        week_df = df_user_master[df_user_master["주차"] == week]
        for day in 요일리스트:
            # 해당 주의 해당 요일 확인
            day_specific = week_df[week_df["요일"] == day]
            if not day_specific.empty:
                master_data[week][day] = day_specific.iloc[0]["근무여부"]
            # 없으면 매주에서 가져옴
            elif not every_week_df.empty:
                day_every = every_week_df[every_week_df["요일"] == day]
                if not day_every.empty:
                    master_data[week][day] = day_every.iloc[0]["근무여부"]
                else:
                    master_data[week][day] = "근무없음"
            else:
                master_data[week][day] = "근무없음"

    # UI: selectbox에 최신 데이터 반영
    for week in week_labels:
        st.markdown(f"**🗓 {week}**")
        col1, col2, col3, col4, col5 = st.columns(5)
        master_data[week]["월"] = col1.selectbox(f"월", 근무옵션, index=근무옵션.index(master_data[week]["월"]), key=f"{week}_월_{selected_employee_name}")
        master_data[week]["화"] = col2.selectbox(f"화", 근무옵션, index=근무옵션.index(master_data[week]["화"]), key=f"{week}_화_{selected_employee_name}")
        master_data[week]["수"] = col3.selectbox(f"수", 근무옵션, index=근무옵션.index(master_data[week]["수"]), key=f"{week}_수_{selected_employee_name}")
        master_data[week]["목"] = col4.selectbox(f"목", 근무옵션, index=근무옵션.index(master_data[week]["목"]), key=f"{week}_목_{selected_employee_name}")
        master_data[week]["금"] = col5.selectbox(f"금", 근무옵션, index=근무옵션.index(master_data[week]["금"]), key=f"{week}_금_{selected_employee_name}")

    # 나머지 저장 버튼 로직은 그대로
    if st.button("💾 주 단위 저장", key="save_weekly"):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet1 = sheet.worksheet("마스터")
            
            rows = []
            for 요일 in 요일리스트:
                week_shifts = [master_data[week][요일] for week in week_labels]
                if all(shift == week_shifts[0] for shift in week_shifts):
                    rows.append({"이름": selected_employee_name, "주차": "매주", "요일": 요일, "근무여부": week_shifts[0]})
                else:
                    for week in week_labels:
                        rows.append({"이름": selected_employee_name, "주차": week, "요일": 요일, "근무여부": master_data[week][요일]})
            
            df_master = df_master[df_master["이름"] != selected_employee_name]
            updated_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"])
            updated_df["요일"] = pd.Categorical(updated_df["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            updated_df = updated_df.sort_values(by=["이름", "주차", "요일"])
            
            df_result = pd.concat([df_master, updated_df], ignore_index=True)
            df_result["요일"] = pd.Categorical(df_result["요일"], categories=["월", "화", "수", "목", "금"], ordered=True)
            df_result = df_result.sort_values(by=["이름", "주차", "요일"])
            
            if update_sheet_with_retry(worksheet1, [df_result.columns.tolist()] + df_result.values.tolist()):
                st.session_state["df_master"] = df_result
                st.session_state["worksheet1"] = worksheet1
                st.session_state["df_user_master"] = df_result[df_result["이름"] == selected_employee_name].copy()
                
                with st.spinner("근무 및 보충 테이블 갱신 중..."):
                    st.session_state["df_shift"] = generate_shift_table(df_result)
                    st.session_state["df_supplement"] = generate_supplement_table(st.session_state["df_shift"], df_result["이름"].unique())
                
                st.success("주 단위 수정사항이 저장되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("마스터 시트 저장 실패")
                st.stop()
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (주 단위 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"주 단위 저장 중 오류 발생: {str(e)}")
            st.stop()

load_data_page4()
df_master = st.session_state.get("df_master", pd.DataFrame(columns=["이름", "주차", "요일", "근무여부"]))
df_request = st.session_state.get("df_request", pd.DataFrame(columns=["이름", "분류", "날짜정보"]))
df_cumulative = st.session_state.get("df_cumulative", pd.DataFrame(columns=["이름", "오전누적", "오후누적", "오전당직 (온콜)", "오후당직"]))
df_shift = st.session_state.get("df_shift", pd.DataFrame())
df_supplement = st.session_state.get("df_supplement", pd.DataFrame())

def excel_download(name, sheet1, name1, sheet2, name2, sheet3, name3, sheet4, name4):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        sheet1.to_excel(writer, sheet_name=name1, index=False)
        sheet2.to_excel(writer, sheet_name=name2, index=False)
        sheet3.to_excel(writer, sheet_name=name3, index=False)
        sheet4.to_excel(writer, sheet_name=name4, index=False)
    
    excel_data = output.getvalue()
    return excel_data

# 기존 코드에서 누적 테이블 및 버튼 부분만 수정
st.divider()
st.subheader(f"✨ {month_str} 테이블 종합")

df_shift_processed = split_column_to_multiple(st.session_state["df_shift"], "근무", "근무")
df_supplement_processed = split_column_to_multiple(st.session_state["df_supplement"], "보충", "보충")

st.write(" ")
st.markdown("**✅ 근무 테이블**")
st.dataframe(st.session_state["df_shift"], use_container_width=True, hide_index=True)

st.markdown("**☑️ 보충 테이블**")
st.dataframe(st.session_state["df_supplement"], use_container_width=True, hide_index=True)

# 누적 테이블
st.markdown("**➕ 누적 테이블**")
st.write("- 변동이 있는 경우, 수정 가능합니다.")

# 세션 상태에 편집된 누적 테이블 저장
if "edited_df_cumulative" not in st.session_state:
    st.session_state["edited_df_cumulative"] = df_cumulative.copy()

# 편집 가능한 테이블 표시
edited_df = st.data_editor(
    st.session_state["edited_df_cumulative"],
    use_container_width=True,
    hide_index=True,
    column_config={
        "이름": {"editable": False},  # 이름은 수정 불가
        "오전누적": {"type": "number"},
        "오후누적": {"type": "number"},
        "오전당직 (온콜)": {"type": "number"},
        "오후당직": {"type": "number"}
    }
)

# 저장 버튼과 다운로드 버튼을 같은 행에 배치
col_save, col_download = st.columns([1, 1])

with col_save:
    if st.button("💾 누적 테이블 수정사항 저장"):
        try:
            gc = get_gspread_client()
            sheet = gc.open_by_url(url)
            worksheet4 = sheet.worksheet(f"{month_str} 누적")
            
            # 편집된 데이터를 세션 상태에 저장
            st.session_state["edited_df_cumulative"] = edited_df
            st.session_state["df_cumulative"] = edited_df.copy()
            
            # Google Sheets에 업데이트
            update_data = [edited_df.columns.tolist()] + edited_df.values.tolist()
            if update_sheet_with_retry(worksheet4, update_data):
                st.success(f"{month_str} 누적 테이블이 성공적으로 저장되었습니다.")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("누적 테이블 저장 실패")
                st.stop()
        except gspread.exceptions.APIError as e:
            st.warning("⚠️ 너무 많은 요청이 접속되어 딜레이되고 있습니다. 잠시 후 재시도 해주세요.")
            st.error(f"Google Sheets API 오류 (누적 테이블 저장): {str(e)}")
            st.stop()
        except Exception as e:
            st.warning("⚠️ 새로고침 버튼을 눌러 데이터를 다시 로드해주십시오.")
            st.error(f"누적 테이블 저장 중 오류 발생: {str(e)}")
            st.stop()

with col_download:
    excel_data = excel_download(
        name=f"{month_str} 테이블 종합",
        sheet1=df_shift_processed, name1="근무 테이블",
        sheet2=df_supplement_processed, name2="보충 테이블",
        sheet3=st.session_state["df_request"], name3="요청사항 테이블",
        sheet4=st.session_state["df_cumulative"], name4="누적 테이블"
    )
    st.download_button(
        label="📥 상단 테이블 다운로드",
        data=excel_data,
        file_name=f"{month_str} 테이블 종합.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )