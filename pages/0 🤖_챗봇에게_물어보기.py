import os
import streamlit as st
from openai import OpenAI
from langchain_community.document_loaders import GitLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.vectorstores import FAISS
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
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

# =========================
# 0) 상수 설정
# =========================
REPO_URL = "https://github.com/HUI135/gc-endoscopy-room.git"
BRANCH = "main"

# =========================
# 1) API 키 설정 및 검사
# =========================
if "gpt" not in st.secrets or "openai_api_key" not in st.secrets["gpt"]:
    st.error("⚠️ 시스템 설정 오류가 발생했습니다. 관리자에게 문의하세요.")
    st.stop()

OPENAI_API_KEY = st.secrets["gpt"]["openai_api_key"].strip()
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# OpenAI 연결 테스트
try:
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "hello"}],
    )
except Exception as e:
    st.error(f"시스템 연결 오류: {e}")
    st.stop()

# =========================
# 2) 데이터 로드 (캐시)
# =========================
@st.cache_resource(show_spinner="데이터를 준비하는 중...")
def load_knowledge_base():
    repo_path = "./temp_repo"
    loader = GitLoader(
        clone_url=REPO_URL,
        repo_path=repo_path,
        branch=BRANCH,
        file_filter=lambda p: p.endswith((".py", ".md", ".txt"))
    )
    docs = loader.load()

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    splits = text_splitter.split_documents(docs)

    embeddings = OpenAIEmbeddings(model="text-embedding-3-small", api_key=OPENAI_API_KEY)
    vectorstore = FAISS.from_documents(splits, embeddings)
    return vectorstore

# =========================
# 3) Streamlit UI 설정
# =========================

# 메인 레이아웃
st.header("🤖 챗봇에게 물어보기", divider='rainbow')
st.write("- 챗봇에게 궁금한 점을 물어보세요! 예: 앱 기능, 프로젝트 정보 등")
st.write()

# 데이터 로드
with st.spinner("데이터를 준비하는 중..."):
    vectorstore = load_knowledge_base()

# =========================
# 4) 챗봇 설정
# =========================
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=OPENAI_API_KEY)

system_prompt = (
    "You are a friendly assistant for the GC Endoscopy app. "
    "Answer questions clearly and simply using the provided project information.\n\n{context}"
)
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt),
        ("human", "{input}"),
    ]
)

question_answer_chain = create_stuff_documents_chain(llm, prompt)
rag_chain = create_retrieval_chain(vectorstore.as_retriever(), question_answer_chain)

# =========================
# 5) 채팅 UI
# =========================
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "안녕하세요! GC Endoscopy 프로젝트에 대해 궁금한 점이 있으면 물어보세요! 😊"}
    ]

# 채팅 영역
chat_container = st.container()
with chat_container:
    for m in st.session_state.messages:
        with st.chat_message(m["role"], avatar="🏥" if m["role"] == "assistant" else None):
            st.markdown(m["content"])

# 입력창
user_input = st.chat_input("궁금한 점을 입력하세요 (예: 이 앱은 무엇인가요?)")
if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with chat_container:
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
    .stChatMessage[data-user="user"] {
        background-color: #d9e6ff;
        border-left: 4px solid #1e90ff;
    }
    .stChatMessage[data-user="assistant"] {
        background-color: #f5f5f5;
        border-left: 4px solid #2ecc71;
    }
    .stTitle {
        color: #2c3e50;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)