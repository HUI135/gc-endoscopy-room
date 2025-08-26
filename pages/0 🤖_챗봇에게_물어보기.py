import os
import streamlit as st
from openai import OpenAI
# [수정] DirectoryLoader 대신 Document 객체를 직접 사용합니다.
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

st.set_page_config(page_title="챗봇에게 물어보기", page_icon="🤖", layout="wide")

st.session_state.current_page = os.path.basename(__file__)
menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")
    st.stop()

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
            shutil.rmtree(repo_path)

        git.Repo.clone_from(REPO_URL, repo_path, branch=BRANCH)

        # [핵심 수정] DirectoryLoader 대신 파일을 직접 읽어 Document 객체를 생성합니다.
        docs = []
        file_extensions_to_load = ['.py', '.md', '.txt']
        
        loaded_files_list = []

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
                        loaded_files_list.append(file_path)
                    except Exception as e:
                        st.warning(f"'{file_path}' 파일을 읽는 중 오류 발생: {e}")
        
        if not docs:
            st.warning("⚠️ 리포지토리에서 텍스트 파일을 로드하지 못했습니다.")
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
        # 작업 완료 후 임시 폴더 정리
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)

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
system_prompt = (
    "You are a friendly assistant for the GC Endoscopy app. "
    "Answer questions clearly and simply using the provided project information.\n\n{context}"
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

# 이전 대화 내용 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar="🏥" if message["role"] == "assistant" else None):
        st.markdown(message["content"])

# 사용자 입력 처리
if user_input := st.chat_input("궁금한 점을 입력하세요 (예: 이 앱은 무엇인가요?)"):
    st.session_state.messages.append({"role": "user", "content": user_input})
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

# 스타일링
st.markdown(
    """
    <style>
    .stChatMessage {
        border-radius: 12px;
        padding: 12px;
        margin-bottom: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    [data-testid="chat-message-container-user"] {
        background-color: #d9e6ff;
    }
    [data-testid="chat-message-container-assistant"] {
        background-color: #f5f5f5;
    }
    .stTitle {
        color: #2c3e50;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)
