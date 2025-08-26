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
import git
import shutil
import traceback

# 데이터 로드 함수 (기존과 동일)
@st.cache_resource(show_spinner="데이터를 준비하는 중...")
def load_knowledge_base():
    repo_path = "./temp_repo"
    try:
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path, ignore_errors=True)

        git.Repo.clone_from("https://github.com/HUI135/gc-endoscopy-room.git", repo_path, branch="main")

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

        embeddings = OpenAIEmbeddings(model="text-embedding-3-small", api_key=st.secrets["gpt"]["openai_api_key"])
        vectorstore = FAISS.from_documents(splits, embeddings)
        return vectorstore
    except Exception as e:
        st.error(f"❌ 데이터 로딩 중 오류가 발생했습니다: {e}")
        st.code(traceback.format_exc())
        return None
    finally:
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path, ignore_errors=True)

# 챗봇 설정 및 렌더링 함수
def render_chatbot():
    # API 키 설정 및 검사
    try:
        OPENAI_API_KEY = st.secrets["gpt"]["openai_api_key"]
        os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
    except (KeyError, TypeError):
        st.error("⚠️ 시스템 설정 오류가 발생했습니다. 관리자에게 문의하세요.")
        return

    # OpenAI 연결 테스트
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        client.models.list()
    except Exception as e:
        st.error(f"시스템 연결 오류: {e}")
        return

    # 데이터 로드
    vectorstore = load_knowledge_base()
    if vectorstore is None:
        st.error("데이터베이스 초기화에 실패했습니다. 위의 로그를 확인하여 원인을 파악하거나 관리자에게 문의하세요.")
        return

    # 챗봇 설정
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=OPENAI_API_KEY)
    system_prompt = (
        "You are a friendly assistant for the GC Endoscopy app, designed to help general users of the Gangnam Center endoscopy services. "
        "Answer questions clearly and simply, focusing solely on how to use the app for general users (e.g., booking appointments, viewing hospital information, submitting requests like schedule or room assignment changes) "
        "or general information about endoscopy procedures. "
        "Do not mention or provide information about admin-specific features (e.g., schedule management, room assignment, or any direct system modifications) unless the user explicitly states 'I am an admin' or 'administrator' (e.g., 'I am an admin, how do I manage schedules?') and is in admin mode (st.session_state.admin_mode=True). "
        "Admin-specific features are password-protected and not accessible to general users, so they must not be referenced in responses to general users, even if keywords like 'schedule' or 'room assignment' are mentioned. "
        "For general users, focus on request submission features (e.g., submitting a schedule change request or room assignment request). "
        "Use the provided project information only when relevant to the user's question, and exclude admin-related files (e.g., '4 스케줄_관리.py', '5 스케줄_배정.py', '6 방_배정.py', '7 방_배정_변경.py') unless explicitly requested by an admin in admin mode.\n\n{context}"
    )
    prompt = ChatPromptTemplate.from_messages(
        [("system", system_prompt), ("human", "{input}")]
    )
    question_answer_chain = create_stuff_documents_chain(llm, prompt)
    rag_chain = create_retrieval_chain(vectorstore.as_retriever(), question_answer_chain)

    # 세션 상태 초기화
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "안녕하세요! 강남센터 내시경실 시스템에 대해 궁금한 점이 있으면 물어보세요! 😊"}
        ]
    
    # <<-- 변경점 1: 챗봇을 사이드바에 배치
    with st.sidebar:
        with st.popover("🤖 챗봇 열기"):
            # 팝업 내부에 대화 기록 표시
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

            # <<-- 변경점 2: 사용자 입력을 받기 위해 st.chat_input 사용
            if user_input := st.chat_input("궁금한 점을 입력하세요..."):
                # 사용자 메시지를 대화 기록에 추가하고 화면에 표시
                st.session_state.messages.append({"role": "user", "content": user_input})
                with st.chat_message("user"):
                    st.markdown(user_input)
                
                # 챗봇 응답 생성 및 표시
                with st.chat_message("assistant"):
                    with st.spinner("답변을 생각하고 있어요..."):
                        # 관리자 모드 확인 로직 (기존과 유사)
                        is_admin_query = "i am an admin" in user_input.lower() or "administrator" in user_input.lower()
                        is_admin_mode = st.session_state.get("admin_mode", False)

                        if is_admin_query and not is_admin_mode:
                            answer = "관리자 기능에 접근하려면 먼저 관리자 모드로 전환해주세요."
                        else:
                            try:
                                response = rag_chain.invoke({"input": user_input})
                                answer = response["answer"]
                            except Exception as e:
                                answer = f"죄송합니다, 답변을 생성하는 중 문제가 발생했습니다: {e}"
                        
                        st.markdown(answer)

                # 챗봇 응답을 대화 기록에 추가
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
                # <<-- 변경점 3: popover 내부에서 상호작용 후 st.rerun()을 호출해 UI를 즉시 업데이트
                st.rerun()

# 메인 앱 로직
def main():
    st.title("강남센터 내시경실 시스템")
    st.write("사이드바의 '>')를 누르고, 챗봇 버튼을 클릭하여 상담을 시작하세요.")
    render_chatbot()

if __name__ == "__main__":
    main()