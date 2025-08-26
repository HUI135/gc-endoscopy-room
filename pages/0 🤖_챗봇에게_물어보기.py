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
import shutil

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
    st.write("OpenAI 연결 성공:", resp.choices[0].message.content)
except Exception as e:
    st.error(f"OpenAI 연결 오류: {e}")
    st.stop()

# =========================
# 2) 데이터 로드 (캐시)
# =========================
@st.cache_resource(show_spinner="데이터를 준비하는 중...")
def load_knowledge_base():
    repo_path = "./temp_repo"
    try:
        # 기존 클론 제거
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path, ignore_errors=True)

        # GitLoader로 클론 및 파일 로드
        loader = GitLoader(
            clone_url=REPO_URL,
            repo_path=repo_path,
            branch=BRANCH,
            file_filter=lambda p: (
                p.endswith((".py", ".md", ".txt")) and
                ".git" not in p and
                ".gitignore" not in p and
                "submodule" not in p.lower()
            )
        )
        docs = loader.load()

        if not docs:
            st.error("⚠️ 프로젝트에서 데이터를 불러오지 못했습니다. Streamlit Cloud의 'Manage app'에서 로그를 확인하거나 관리자에게 문의하세요.")
            return None

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        splits = text_splitter.split_documents(docs)

        embeddings = OpenAIEmbeddings(model="text-embedding-3-small", api_key=OPENAI_API_KEY)
        vectorstore = FAISS.from_documents(splits, embeddings)
        return vectorstore
    except Exception as e:
        st.error("⚠️ 프로젝트 데이터를 불러오는 중 오류가 발생했습니다. Streamlit Cloud의 'Manage app'에서 로그를 확인하거나 관리자에게 문의하세요.")
        return None

# =========================
# 3) Streamlit UI
# =========================
st.subheader("🤖 챗봇에게 물어보기", divider="rainbow")

# 데이터 로드
vectorstore = load_knowledge_base()
if vectorstore is None:
    st.stop()

# =========================
# 4) LLM & 체인 구성
# =========================
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=OPENAI_API_KEY,
)

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
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# 입력창
if user_input := st.chat_input("궁금한 점을 입력하세요 (예: 앱 기능, 프로젝트 정보 등)"):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("답변을 준비하는 중..."):
            try:
                response = rag_chain.invoke({"input": user_input})
                answer = response["answer"]
            except Exception as e:
                answer = f"문제가 발생했습니다. 다시 시도해 주세요."
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