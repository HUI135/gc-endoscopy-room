import streamlit as st

def menu():
    # Streamlit의 기본 사이드바 페이지 목록을 숨기는 CSS 트릭
    st.markdown("""
    <style>
        [data-testid="stSidebarNav"] {
            display: none;
        }
    </style>
    """, unsafe_allow_html=True)

    # 로그인 상태일 때만 메뉴 표시
    if st.session_state.get("login_success", False):
        # [수정] 사용자 정보와 로그아웃 버튼을 메뉴 상단으로 이동
        st.sidebar.write(f"현재 사용자: {st.session_state['name']} ({str(st.session_state['employee_id']).zfill(5)})")
        if st.sidebar.button("로그아웃", use_container_width=True):
            st.session_state.clear()
            st.switch_page("Home.py")
        st.sidebar.divider()

        st.sidebar.header("메뉴")
        
        # [수정] Home 페이지로 돌아가는 버튼 추가
        if st.sidebar.button("🏠 Home", use_container_width=True):
            st.switch_page("Home.py")
        
        # 일반 사용자 메뉴
        if st.sidebar.button("📅 마스터 수정", use_container_width=True):
            st.switch_page("pages/1 📅_마스터_수정.py")
        if st.sidebar.button("🙋‍♂️요청사항 입력", use_container_width=True):
            st.switch_page("pages/2 🙋‍♂️_요청사항_입력.py")
        if st.sidebar.button("🏠 방배정 요청", use_container_width=True):
            st.switch_page("pages/3 🏠_방배정_요청.py")
        if st.sidebar.button("🔍 스케줄 변경 요청", use_container_width=True):
            st.switch_page("pages/3 🔍_스케쥴_변경_요청.py")
        if st.sidebar.button("🔔 방 변경 요청", use_container_width=True):
            st.switch_page("pages/3 🔔_방_변경_요청.py")

        # 관리자일 경우, 관리자 메뉴 추가
        if st.session_state.get("is_admin", False):
            st.sidebar.divider()
            st.sidebar.header("관리자 메뉴")
            
            if st.session_state.get("admin_mode", False):
                # 관리자 모드가 활성화되면 관리자 메뉴 버튼을 보여줌
                if st.sidebar.button("⚙️ 스케줄 관리", use_container_width=True):
                    st.switch_page("pages/4 스케줄_관리.py")
                if st.sidebar.button("🗓️ 스케줄 배정", use_container_width=True):
                    st.switch_page("pages/5 스케줄_배정.py")
                if st.sidebar.button("🚪 방 배정", use_container_width=True):
                    st.switch_page("pages/6 방_배정.py")
                if st.sidebar.button("🔄 방 배정 변경", use_container_width=True):
                    st.switch_page("pages/7 방_배정_변경.py")
            else:
                # [수정] 이제 Home 버튼이 있으므로 이 메시지를 보고 Home으로 이동 가능
                st.sidebar.info("관리자 메뉴를 보려면 Home 페이지에서 인증하세요.")