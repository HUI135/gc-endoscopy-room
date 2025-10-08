import streamlit as st
import os
import re

def menu():
    current_page_basename = st.session_state.get("current_page", "Home.py")

    # 사이드바 UI 구성
    # 기본 Streamlit 페이지 목록 숨기기
    st.markdown("""<style>[data-testid="stSidebarNav"] { display: none; }</style>""", unsafe_allow_html=True)

    if st.session_state.get("login_success", False):
        st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
        if st.sidebar.button("로그아웃", use_container_width=True):
            st.session_state.clear()
            st.switch_page("Home.py")
        st.sidebar.divider()

        st.sidebar.header("메뉴")
        
        if st.sidebar.button("🏠 Home", use_container_width=True, disabled=(current_page_basename == "Home.py")):
            st.switch_page("Home.py")

        # Home과 마스터 보기 사이 간격
        st.sidebar.markdown("<div style='margin-top: 4px;'></div>", unsafe_allow_html=True)

        if st.sidebar.button("📅 마스터 보기", use_container_width=True, disabled=(current_page_basename == "1 📅_마스터_보기.py")):
            st.switch_page("pages/1 📅_마스터_보기.py")

        if st.sidebar.button("🙋‍♂️ 요청사항 입력", use_container_width=True, disabled=(current_page_basename == "2 🙋‍♂️_요청사항_입력.py")):
            st.switch_page("pages/2 🙋‍♂️_요청사항_입력.py")
        
        if st.sidebar.button("📝 방배정 요청 입력", use_container_width=True, disabled=(current_page_basename == "3 📝_방배정_요청_입력.py")):
            st.switch_page("pages/3 📝_방배정_요청_입력.py")

        # 방배정 요청 입력과 스케줄 변경 요청 사이 간격
        st.sidebar.markdown("<div style='margin-top: 4px;'></div>", unsafe_allow_html=True)

        if st.sidebar.button("🔍 스케줄 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔍_스케줄_변경_요청.py")):
            st.switch_page("pages/3 🔍_스케줄_변경_요청.py")
            
        if st.sidebar.button("🔔 방배정 변경 요청", use_container_width=True, disabled=(current_page_basename == "3 🔔_방배정_변경_요청.py")):
            st.switch_page("pages/3 🔔_방배정_변경_요청.py")
        
        # 챗봇에게 물어보기 전 간격
        st.sidebar.markdown("<div style='margin-top: 4px;'></div>", unsafe_allow_html=True)

        if st.sidebar.button("🤖 챗봇에게 물어보기", use_container_width=True, disabled=(current_page_basename == "3 🤖_챗봇에게_물어보기.py")):
            st.switch_page("pages/3 🤖_챗봇에게_물어보기.py")
            
        # 관리자 메뉴
        if st.session_state.get("is_admin", False):
            st.sidebar.divider()
            st.sidebar.header("관리자 메뉴")
            if st.session_state.get("admin_mode", False):
                if st.sidebar.button("⚙️ 스케줄 관리", use_container_width=True, disabled=(current_page_basename == "4 스케줄_관리.py")):
                    st.switch_page("pages/4 스케줄_관리.py")

                if st.sidebar.button("🗓️ 스케줄 배정", use_container_width=True, disabled=(current_page_basename == "5 스케줄_배정.py")):
                    st.switch_page("pages/5 스케줄_배정.py")

                if st.sidebar.button("✍️ 스케줄 수정", use_container_width=True, disabled=(current_page_basename == "5 스케줄_수정.py")):
                    st.switch_page("pages/5 스케줄_수정.py")

                # 스케줄 배정과 방배정 사이 간격
                st.sidebar.markdown("<div style='margin-top: 4px;'></div>", unsafe_allow_html=True)
                
                if st.sidebar.button("🚪 방배정", use_container_width=True, disabled=(current_page_basename == "6 방배정.py")):
                    st.switch_page("pages/6 방배정.py")
                if st.sidebar.button("🔄 방배정 변경", use_container_width=True, disabled=(current_page_basename == "7 방배정_변경.py")):
                    st.switch_page("pages/7 방배정_변경.py")
            else:
                st.sidebar.info("관리자 메뉴를 보려면 Home 페이지에서 인증하세요.")

