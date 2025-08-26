import os
import streamlit as st
from openai import OpenAI
from langchain_community.document_loaders import DirectoryLoader, UnstructuredFileLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
import git
import shutil
import glob
import logging
import time
import menu

st.set_page_config(page_title="챗봇에게 물어보기", page_icon="🤖", layout="wide")

import os
st.session_state.current_page = os.path.basename(__file__)

menu.menu()

# 로그인 체크 및 자동 리디렉션
if not st.session_state.get("login_success", False):
    st.warning("⚠️ Home 페이지에서 먼저 로그인해주세요.")
    st.error("1초 후 Home 페이지로 돌아갑니다...")
    time.sleep(1)
    st.switch_page("Home.py")  # Home 페이지로 이동
    st.stop()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =========================
# 0) 상수 설정
# =========================
REPO_URL = "https://github.com/HUI135/gc-endoscopy-room.git"
BRANCH = "main"

# =========================
# 1) API 키 설정 및 검사
# =========================
# Streamlit secrets에서 API 키를 안전하게 로드합니다.
try:
    OPENAI_API_KEY = st.secrets["gpt"]["openai_api_key"]
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
except (KeyError, TypeError):
    st.error("⚠️ OpenAI API 키를 찾을 수 없습니다. Streamlit secrets 설정을 확인해주세요.")
    st.stop()

# OpenAI 클라이언트 초기화 및 연결 테스트
try:
    client = OpenAI(api_key=OPENAI_API_KEY)
    client.models.list() 
    logging.info("OpenAI API 연결에 성공했습니다.")
except Exception as e:
    st.error(f"OpenAI API 연결에 실패했습니다: {e}")
    logging.error(f"OpenAI API 연결 오류: {e}")
    st.stop()

# =========================
# 2) 데이터 로드 (캐시)
# =========================
@st.cache_resource(show_spinner="데이터를 준비하는 중입니다...")
def load_knowledge_base():
    """
    Git 리포지토리에서 데이터를 클론하고, 문서를 로드하여 벡터 저장소를 생성합니다.
    """
    repo_path = "./temp_repo"
    try:
        # 기존에 클론된 리포지토리가 있다면 삭제합니다.
        if os.path.exists(repo_path):
            logging.info(f"기존 리포지토리 경로 '{repo_path}'를 삭제합니다.")
            shutil.rmtree(repo_path)

        # Git 리포지토리를 클론합니다.
        logging.info(f"'{REPO_URL}' 리포지토리의 '{BRANCH}' 브랜치를 클론합니다.")
        git.Repo.clone_from(REPO_URL, repo_path, branch=BRANCH)

        # 텍스트 기반 파일만 로드하도록 glob 패턴을 구체적으로 지정합니다.
        # 이렇게 하면 이미지나 다른 바이너리 파일 로드 시 발생하는 오류를 방지할 수 있습니다.
        loader = DirectoryLoader(
            repo_path,
            glob="**/*.{py,md,txt,html,css,js}", # 텍스트 기반 파일 확장자 지정
            loader_cls=UnstructuredFileLoader, # 다양한 파일 유형을 처리하기 위한 로더
            show_progress=True,
            recursive=True,
            use_multithreading=True
        )
        docs = loader.load()

        if not docs:
            st.warning("⚠️ 프로젝트에서 문서를 불러오지 못했습니다. 리포지토리에 텍스트 파일이 있는지 확인해주세요.")
            logging.warning("DirectoryLoader가 문서를 로드하지 못했습니다.")
            return None

        # 로드된 문서 소스 로깅
        loaded_sources = [doc.metadata.get('source', 'N/A') for doc in docs]
        logging.info(f"{len(loaded_sources)}개의 문서를 로드했습니다: {loaded_sources[:5]}") # 처음 5개만 로그

        # 텍스트를 청크 단위로 분할합니다.
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        splits = text_splitter.split_documents(docs)
        logging.info(f"문서를 {len(splits)}개의 청크로 분할했습니다.")

        # OpenAI 임베딩 모델을 사용하여 벡터 저장소를 생성합니다.
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        vectorstore = FAISS.from_documents(splits, embeddings)
        logging.info("FAISS 벡터 저장소 생성을 완료했습니다.")
        
        return vectorstore

    except Exception as e:
        st.error(f"프로젝트 데이터를 준비하는 중 오류가 발생했습니다: {e}")
        logging.error(f"load_knowledge_base 함수에서 예외 발생: {e}", exc_info=True)
        return None
    finally:
        # 임시 리포지토리 정리
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
            logging.info("임시 리포지토리 파일을 정리했습니다.")


# =========================
# 3) Streamlit UI 설정
# =========================
st.subheader("🤖 GC 내시경실 챗봇", divider="rainbow")
st.info("이 챗봇은 GC 내시경실 GitHub 리포지토리의 정보를 기반으로 답변합니다.\n\n앱의 기능, 코드 구조, 프로젝트 목적 등 무엇이든 물어보세요!")

# 데이터 로드 실행
vectorstore = load_knowledge_base()
if vectorstore is None:
    st.error("데이터베이스 초기화에 실패했습니다. 앱을 다시 시작하거나 관리자에게 문의하세요.")
    st.stop()

# =========================
# 4) LLM 및 RAG 체인 구성
# =========================
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

system_prompt = (
    "당신은 GC 내시경실 앱 프로젝트에 대해 설명하는 친절한 AI 어시스턴트입니다. "
    "제공된 프로젝트 정보를 바탕으로 사용자의 질문에 명확하고 간결하게 답변해주세요. "
    "기술적인 질문에는 코드의 맥락을 파악하여 설명하고, 일반적인 질문에는 사용자가 이해하기 쉽게 답변해주세요."
    "\n\n"
    "참고 정보:\n{context}"
)

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt),
        ("human", "{input}"),
    ]
)

# LangChain Expression Language (LCEL)을 사용하여 체인 구성
retriever = vectorstore.as_retriever()
question_answer_chain = create_stuff_documents_chain(llm, prompt)
rag_chain = create_retrieval_chain(retriever, question_answer_chain)


# =========================
# 5) 채팅 UI 로직
# =========================
if "messages" not in st.session_state:
    st.session_state.messages = []

# 이전 대화 내용 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 사용자 입력 처리
if user_input := st.chat_input("궁금한 점을 입력하세요..."):
    # 사용자 메시지 추가 및 표시
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # 어시스턴트 응답 생성 및 표시
    with st.chat_message("assistant"):
        with st.spinner("답변을 생각하고 있어요..."):
            try:
                response = rag_chain.invoke({"input": user_input})
                answer = response.get("answer", "답변을 생성하는 데 문제가 발생했습니다.")
            except Exception as e:
                answer = "죄송합니다, 답변을 생성하는 동안 오류가 발생했습니다. 다시 시도해주세요."
                logging.error(f"RAG 체인 실행 중 오류 발생: {e}", exc_info=True)
            
            st.markdown(answer)
            st.session_state.messages.append({"role": "assistant", "content": answer})

# =========================
# 6) 커스텀 스타일링
# =========================
st.markdown("""
<style>
    .stChatMessage {
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    /* Streamlit 1.36+ 에서는 data-testid 속성을 사용합니다 */
    [data-testid="chat-message-container-user"] {
        background-color: #e7f3ff;
    }
    [data-testid="chat-message-container-assistant"] {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)
