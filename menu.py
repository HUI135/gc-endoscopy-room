import streamlit as st
import os
import re

def menu():
    # st.session_state에서 현재 페이지 파일명을 가져옵니다.
    # 페이지가 처음 로드될 때를 대비해 기본값으로 Home.py를 설정합니다.
    current_page_basename = st.session_state.get("current_page", "Home.py")

    # 파일명에서 깔끔한 '페이지 이름'을 만듭니다.
    display_name = "Home"
    if current_page_basename != "Home.py":
        try:
            temp_name = re.sub(r"^\d+\s+", "", current_page_basename)
            cleaned = temp_name.replace(".py", "").replace("_", " ")
            display_name = cleaned if cleaned else current_page_basename.replace(".py", "")
        except Exception:
             display_name = current_page_basename.replace(".py", "")

    # 사이드바 UI 구성
    st.markdown("""<style>[data-testid="stSidebarNav"] { display: none; }</style>""", unsafe_allow_html=True)

    if st.session_state.get("login_success", False):
        st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
        if st.sidebar.button("로그아웃", use_container_width=True):
            st.session_state.clear()
            st.switch_page("Home.py")
        st.sidebar.divider()

        st.sidebar.header("메뉴")
        
        # 메뉴 버튼들 (기존과 동일한 비활성화 로직)
        if st.sidebar.button("🏠 Home", use_container_width=True, disabled=(current_page_basename == "Home.py")):
            st.switch_page("Home.py")
        
        if st.sidebar.button("📅 마스터 수정", use_container_width=True, disabled=(current_page_basename == "1 📅_마스터_수정.py")):
            st.switch_page("pages/1 📅_마스터_수정.py")

        # ... (다른 페이지 버튼들도 동일한 패턴)
        if st.sidebar.button("🙋‍♂️요청사항 입력", use_container_width=True, disabled=(current_page_basename == "2 🙋‍♂️_요청사항_입력.py")):
            st.switch_page("pages/2 🙋‍♂️_요청사항_입력.py")
        if st.sidebar.button("🏠 방배정 요청", use_container_width=True, disabled=(current_page_basename == "3 🏠_방배정_요청.py")):
            st.switch_page("pages/3 🏠_방배정_요청.py")
        if st.sidebar.button("🔍 스케줄 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔍_스케줄_변경_요청.py")):
            st.switch_page("pages/3 🔍_스케줄_변경_요청.py")
        if st.sidebar.button("🔔 방배정 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔔_방배정_변경_요청.py")):
            st.switch_page("pages/3 🔔_방배정_변경_요청.py")

        # 관리자 메뉴
        if st.session_state.get("is_admin", False):
            st.sidebar.divider()
            st.sidebar.header("관리자 메뉴")
            if st.session_state.get("admin_mode", False):
                if st.sidebar.button("⚙️ 스케줄 관리", use_container_width=True, disabled=(current_page_basename == "4 스케줄_관리.py")):
                    st.switch_page("pages/4 스케줄_관리.py")
                if st.sidebar.button("🗓️ 스케줄 배정", use_container_width=True, disabled=(current_page_basename == "5 스케줄_배정.py")):
                    st.switch_page("pages/5 스케줄_배정.py")
                if st.sidebar.button("🚪 방 배정", use_container_width=True, disabled=(current_page_basename == "6 방_배정.py")):
                    st.switch_page("pages/6 방_배정.py")
                if st.sidebar.button("🔄 방 배정 변경", use_container_width=True, disabled=(current_page_basename == "7 방_배정_변경.py")):
                    st.switch_page("pages/7 방_배정_변경.py")
            else:
                st.sidebar.info("관리자 메뉴를 보려면 Home 페이지에서 인증하세요.")