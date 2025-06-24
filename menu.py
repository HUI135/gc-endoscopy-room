import streamlit as st
import os
import re

def menu(current_page=""):
    # 현재 스크립트의 파일명을 가져옵니다. (예: 'Home.py', '1 📅_마스터_수정.py')
    current_page_basename = os.path.basename(current_page)

    # 파일명에서 숫자, 이모지, 확장자 등을 제거하여 깔끔한 '페이지 이름'을 만듭니다.
    try:
        display_name = re.sub(r"^\d+\s*.*?_", "", current_page_basename).replace(".py", "").replace("_", " ")
    except:
        display_name = "Home"


    # Streamlit의 기본 사이드바 페이지 목록을 숨기는 CSS
    st.markdown("""<style>[data-testid="stSidebarNav"] { display: none; }</style>""", unsafe_allow_html=True)

    # 로그인 상태일 때만 메뉴 표시
    if st.session_state.get("login_success", False):
        st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
        if st.sidebar.button("로그아웃", use_container_width=True):
            st.session_state.clear()
            st.switch_page("Home.py")
        st.sidebar.divider()

        # [신규] 현재 페이지 이름을 표시하는 컨테이너
        with st.sidebar.container(border=True):
            st.markdown(f"**현재 페이지:**\n\n`{display_name}`")
        
        st.sidebar.header("메뉴")
        
        # 현재 페이지의 버튼은 비활성화(disabled) 됩니다.
        if st.sidebar.button("🏠 Home", use_container_width=True, disabled=(current_page_basename == "Home.py")):
            st.switch_page("Home.py")
        
        # 일반 사용자 메뉴
        if st.sidebar.button("📅 마스터 수정", use_container_width=True, disabled=(current_page_basename == "1 📅_마스터_수정.py")):
            st.switch_page("pages/1 📅_마스터_수정.py")
        if st.sidebar.button("🙋‍♂️요청사항 입력", use_container_width=True, disabled=(current_page_basename == "2 🙋‍♂️_요청사항_입력.py")):
            st.switch_page("pages/2 🙋‍♂️_요청사항_입력.py")
        if st.sidebar.button("🏠 방배정 요청", use_container_width=True, disabled=(current_page_basename == "3 🏠_방배정_요청.py")):
            st.switch_page("pages/3 🏠_방배정_요청.py")
        if st.sidebar.button("🔍 스케줄 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔍_스케쥴_변경_요청.py")):
            st.switch_page("pages/3 🔍_스케쥴_변경_요청.py")
        if st.sidebar.button("🔔 방 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔔_방_변경_요청.py")):
            st.switch_page("pages/3 🔔_방_변경_요청.py")

        # 관리자일 경우, 관리자 메뉴 추가
        if st.session_state.get("is_admin", False):
            st.sidebar.divider()
            st.sidebar.header("관리자 메뉴")
            
            if st.session_state.get("admin_mode", False):
                if st.sidebar.button("⚙️ 스케줄 관리", use_container_width=True, disabled=(current_page_basename == "4 [관리자]_스케줄_관리.py")):
                    st.switch_page("pages/4 [관리자]_스케줄_관리.py")
                if st.sidebar.button("🗓️ 스케줄 배정", use_container_width=True, disabled=(current_page_basename == "5 [관리자]_스케줄_배정.py")):
                    st.switch_page("pages/5 [관리자]_스케줄_배정.py")
                if st.sidebar.button("🚪 방 배정", use_container_width=True, disabled=(current_page_basename == "6 [관리자]_방_배정.py")):
                    st.switch_page("pages/6 [관리자]_방_배정.py")
                if st.sidebar.button("🔄 방 배정 변경", use_container_width=True, disabled=(current_page_basename == "7 [관리자]_방_배정_변경.py")):
                    st.switch_page("pages/7 [관리자]_방_배정_변경.py")
            else:
                st.sidebar.info("관리자 메뉴를 보려면 Home 페이지에서 인증하세요.")