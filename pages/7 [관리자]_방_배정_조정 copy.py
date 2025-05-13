import streamlit as st
import pandas as pd

# 초기 데이터 생성 (long format 예시)
data = {
    '날짜': ['4월 1일'] * 3,
    '시간': ['8:30(2)', '9:30(5)', '10:00(3)'],
    '이름': ['김정', '김지윤', '양선영']
}
df = pd.DataFrame(data)

# 데이터 편집 UI
st.write("📋 배정표 (편집 가능)")
edited_df = st.data_editor(
    df,
    use_container_width=True,
    num_rows="fixed",  # 행 고정
    key="editor1"
)

# 셀 선택 UI
st.subheader("🔁 셀 값 교환")
col1, col2 = st.columns(2)

with col1:
    idx1 = st.selectbox("셀1 - 행 번호", edited_df.index, key="row1")
with col2:
    idx2 = st.selectbox("셀2 - 행 번호", edited_df.index, key="row2")

if st.button("🔄 교환 실행"):
    # 이름 열의 값을 스왑
    temp = edited_df.loc[idx1, '이름']
    edited_df.loc[idx1, '이름'] = edited_df.loc[idx2, '이름']
    edited_df.loc[idx2, '이름'] = temp
    st.success(f"✅ '{idx1}번 행'과 '{idx2}번 행'의 이름을 교환했습니다!")

    # 결과 재출력
    st.write("🆕 변경된 배정표")
    st.data_editor(edited_df, use_container_width=True, num_rows="fixed", key="editor2")
