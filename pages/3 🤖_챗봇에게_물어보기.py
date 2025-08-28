import os
import streamlit as st
from openai import OpenAI
from langchain_core.documents import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
import time
import menu
import git
import shutil
import traceback
import json

# st.set_page_config(page_title="챗봇에게 물어보기", page_icon="🤖", layout="wide")

st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

# 관리자 로그인 처리
if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

# =========================
# 0) 상수 설정
# =========================
REPO_URL = "https://github.com/HUI135/gc-endoscopy-room.git"
BRANCH = "main"

# =========================
# 1) API 키 설정 및 검사
# =========================
try:
    OPENAI_API_KEY = st.secrets["gpt"]["openai_api_key"]
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
except (KeyError, TypeError):
    st.error("⚠️ 시스템 설정 오류가 발생했습니다. 관리자에게 문의하세요.")
    st.stop()

# OpenAI 연결 테스트
try:
    client = OpenAI(api_key=OPENAI_API_KEY)
    client.models.list()
except Exception as e:
    st.error(f"시스템 연결 오류: {e}")
    st.stop()

# =========================
# 2) 데이터 로드 (캐시)
# =========================
@st.cache_resource(show_spinner="데이터를 준비하는 중...")
def load_knowledge_base():
    repo_path = "./temp_repo"
    try:
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path, ignore_errors=True)

        git.Repo.clone_from(REPO_URL, repo_path, branch=BRANCH)

        docs = []
        file_extensions_to_load = ['.py', '.md', '.txt']
        
        for root, _, files in os.walk(repo_path):
            if ".git" in root:
                continue
            for file_name in files:
                if any(file_name.endswith(ext) for ext in file_extensions_to_load):
                    file_path = os.path.join(root, file_name)
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                        doc = Document(page_content=content, metadata={"source": file_path})
                        docs.append(doc)
                    except Exception as e:
                        st.warning(f"'{file_name}' 파일을 읽는 중 오류 발생: {e}")

        if not docs:
            st.warning("⚠️ 리포지토리에서 .py, .md, .txt 파일을 로드하지 못했습니다. 리포지토리 내용을 확인하세요.")
            return None

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        splits = text_splitter.split_documents(docs)

        if not splits:
            st.warning("⚠️ 파일을 분석 가능한 텍스트 조각으로 나누지 못했습니다.")
            return None

        embeddings = OpenAIEmbeddings(model="text-embedding-3-small", api_key=OPENAI_API_KEY)
        vectorstore = FAISS.from_documents(splits, embeddings)
        return vectorstore
    except Exception as e:
        st.error(f"❌ 데이터 로딩 중 오류가 발생했습니다: {e}")
        st.code(traceback.format_exc())
        return None
    finally:
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path, ignore_errors=True)

# =========================
# 3) Streamlit UI 설정
# =========================
st.header("🤖 챗봇에게 물어보기", divider='rainbow')
st.info("챗봇에게 궁금한 점을 물어보세요! 예: 앱 기능, 프로젝트 정보 등")
st.write()
st.divider()

vectorstore = load_knowledge_base()

if vectorstore is None:
    st.error("데이터베이스 초기화에 실패했습니다. 위의 로그를 확인하여 원인을 파악하거나 관리자에게 문의하세요.") 
    st.stop()

# =========================
# 4) 챗봇 설정
# =========================
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=OPENAI_API_KEY)

# ✅ 세션의 관리자 여부를 프롬프트에 넘길 컨텍스트로 구성
context = {
    "is_admin": bool(st.session_state.get("is_admin", False))
}

system_prompt = (
    "You are a friendly assistant for the GC Endoscopy app, designed to support professors in managing schedules and room assignments for the Gangnam Center endoscopy room. "
    "This app does NOT provide hospital information or booking services; it is solely for scheduling and room assignment management within the endoscopy room. "
    "Always refer to users as 'professors' and never use the terms 'staff' or 'workers' in responses. "
    "Always respond in Korean.\n"   # ← (권장) 한국어 고정

    "Answer questions clearly and simply for professors, focusing only on these pages: "
    "Home, 마스터 관리, 요청사항 입력, 방배정 요청, 스케줄 변경 요청, 방배정 변경 요청. "
    "These pages allow actions like viewing personal schedules, submitting schedule change requests, or submitting room assignment requests. "

    "For general questions about schedule or room assignment processes (e.g., 'How is scheduling done?' or 'How is room assignment done?'), "
    "provide brief, high-level answers suitable for professors (e.g., 'Room assignment reflects requests and evenly distributes rooms among professors' "
    "or 'Scheduling balances workloads for professors based on master schedules and requests'). "

    "# Admin disclosure policy (based on context.is_admin)\n"
    "- If context.is_admin == True: You MAY reference and explain admin-only features and pages, including but not limited to "
    "[관리자] 스케줄 관리, [관리자] 스케쥴 배정, [관리자] 방 배정, [관리자] 최종본, and direct master modifications. "
    "Provide succinct, step-by-step guidance when asked. Do NOT ask the user to switch modes again if context.is_admin is already True. "

    "- If context.is_admin is False or missing: Do NOT disclose or hint at admin-only features or page names. "
    "Politely redirect to professor-facing pages and high-level guidance only. "
    "If the user claims to be an admin but context.is_admin != True, request that they log in via the admin page first, once, without repeating. "

    "Admin-specific features are password-protected and accessible only via separate admin pages. "
    "Use the provided project information only when relevant to the user's question. "
    "Exclude content from admin-related pages unless context.is_admin == True.\n\n{context}"
)

prompt = ChatPromptTemplate.from_messages(
    [("system", system_prompt), ("human", "{input}")]
)
question_answer_chain = create_stuff_documents_chain(llm, prompt)
rag_chain = create_retrieval_chain(vectorstore.as_retriever(), question_answer_chain)

# =========================
# 5) 채팅 UI
# =========================
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "안녕하세요! 강남센터 내시경실 시스템에 대해 궁금한 점이 있으면 물어보세요! 😊"}
    ]

# 채팅 영역을 박스로 감싸기
chat_container = st.container()
with chat_container:
    # 이전 대화 내용 표시
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar="🏥" if message["role"] == "assistant" else None):
            st.markdown(message["content"])

# 입력창을 별도의 컨테이너로 분리
input_container = st.container()
with input_container:
    # 사용자 입력 처리
    if user_input := st.chat_input("궁금한 점을 입력하세요 (예: 이 앱은 무엇인가요?)"):
        st.session_state.messages.append({"role": "user", "content": user_input})
        with chat_container:  # 사용자 메시지와 답변을 chat_container에 추가
            with st.chat_message("user"):
                st.markdown(user_input)

            with st.chat_message("assistant", avatar="🏥"):
                with st.spinner("답변을 준비하는 중..."):
                    try:
                        response = rag_chain.invoke({"input": user_input})
                        answer = response["answer"]
                    except Exception as e:
                        answer = f"문제가 발생했습니다: {e}"
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})

st.markdown(
    """
    <style>
    /* 대화창 박스 */
    .stChatMessage {
        border-radius: 12px;
        padding: 12px;
        margin-bottom: 12px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    [data-testid="chat-message-container-user"] {
        background-color: #d9e6ff;
    }
    [data-testid="chat-message-container-assistant"] {
        background-color: #f5f5f5;
    }
    /* 입력창 컨테이너 */
    [data-testid="stChatInput"] {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        box-shadow: 0 4px 4px rgba(0,0,0,0.1);
        padding: 10px;
        position: sticky;
        bottom: 0;
        z-index: 1000;
        margin-top: 20px;
        box-sizing: border-box;
        overflow: hidden; /* 내부 요소가 컨테이너 벗어남 방지 */
    }
    /* 입력창 내부 입력 필드 */
    [data-testid="stChatInput"] input {
        width: calc(100% - 80px); /* 버튼 공간(약 40px) + 여백 확보 */
        border: 1px solid #e0e0e0; /* 내부 테두리 유지 */
        border-radius: 4px; /* 둥근 내부 테두리 */
        padding: 8px;
        box-sizing: border-box;
        margin-left:15px;
        outline: none;
        display: inline-block; /* 버튼과 나란히 배치 */
        vertical-align: middle;
    }
    /* 전송 버튼 */
    [data-testid="stChatInput"] button {
        width: 40px;
        height: 40px;
        padding: 0;
        margin-left: 15px; /* 입력 필드와 버튼 간 간격 */
        margin-right: 15px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 4px;
        vertical-align: middle;
    }
    </style>
    """,
    unsafe_allow_html=True
)