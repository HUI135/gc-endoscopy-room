import streamlit as st
from langchain_community.document_loaders import GitLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain.vectorstores import FAISS
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate

# GitHub 리포지토리 URL과 브랜치 (당신의 리포로 바꾸세요)
REPO_URL = "https://github.com/HUI135/gc-endoscopy-room.git"
BRANCH = "main"

# API 키 (Streamlit 시크릿으로 관리 추천)
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]  # 또는 os.environ["OPENAI_API_KEY"]

# 지식 베이스 로드 함수 (앱 시작 시 한 번만)
@st.cache_resource
def load_knowledge_base():
    # Git 리포지토리 클론 및 로드
    loader = GitLoader(
        clone_url=REPO_URL,
        repo_path="./temp_repo",  # 임시 폴더
        branch=BRANCH,
        file_filter=lambda file_path: file_path.endswith((".py", ".md", ".txt"))  # 원하는 파일만 로드
    )
    docs = loader.load()

    # 텍스트 쪼개기
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    splits = text_splitter.split_documents(docs)

    # 임베딩과 벡터 스토어 생성
    embeddings = OpenAIEmbeddings(OPENAI_API_KEY=OPENAI_API_KEY)
    vectorstore = FAISS.from_documents(splits, embeddings)

    return vectorstore

# 메인 앱
st.title("My Streamlit App with GitHub-Based Chatbot")

# 지식 베이스 로드
vectorstore = load_knowledge_base()

# LLM 설정
llm = ChatOpenAI(model="gpt-3.5-turbo", OPENAI_API_KEY=OPENAI_API_KEY)

# 프롬프트 템플릿 (앱 기능 설명이나 FAQ에 맞춤)
system_prompt = (
    "You are an assistant for this Streamlit app. Use the following context to answer questions about the app's features, FAQ, or code from the GitHub repo."
    "\n\n{context}"
)
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt),
        ("human", "{input}"),
    ]
)

# 체인 생성
question_answer_chain = create_stuff_documents_chain(llm, prompt)
rag_chain = create_retrieval_chain(vectorstore.as_retriever(), question_answer_chain)

# 채팅 히스토리 유지
if "messages" not in st.session_state:
    st.session_state.messages = []

# 이전 메시지 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# 사용자 입력
if user_input := st.chat_input("Ask about the app features or FAQ:"):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # RAG 체인으로 응답 생성
    with st.chat_message("assistant"):
        response = rag_chain.invoke({"input": user_input})
        st.markdown(response["answer"])
        st.session_state.messages.append({"role": "assistant", "content": response["answer"]})