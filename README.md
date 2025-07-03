import streamlit as st
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import FAISS
from langchain.text_splitter import RecursiveCharacterTextSplitter
from dotenv import load_dotenv
import os

load_dotenv()

# Streamlit UI
st.title("🔍 RAGView")
st.write("Upload a `.txt` file, then query and view top relevant text chunks using semantic search.")

# Upload file
uploaded_file = st.file_uploader("📄 Upload a `.txt` file", type=["txt"])
top_k = st.slider("Top N Chunks", min_value=1, max_value=10, value=3)

if uploaded_file:
    raw_text = uploaded_file.read().decode("utf-8")

    # Sentence-aware smart chunking
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=100,
        separators=["\n\n", "\n", ".", " "]
    )
    chunks = text_splitter.create_documents([raw_text])

    # Create vector store
    embedding_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    vector_store = FAISS.from_documents(chunks, embedding_model)

    # User query
    query = st.text_input("🔍 Ask a question to find matching chunks")

    if query:
        retriever = vector_store.as_retriever(search_kwargs={"k": top_k})
        matched_docs = retriever.get_relevant_documents(query)

        st.subheader("📚 Top Matching Chunks:")
        for i, doc in enumerate(matched_docs):
            st.markdown(f"**Chunk {i+1}:**")
            st.code(doc.page_content)
