import os
from pathlib import Path
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import FAISS


def init_corpus(corpus_path="corpus"):
    # 1. Collect files
    files = list(Path(corpus_path).rglob("*.*"))
    print(f"Found {len(files)} files in {corpus_path}")

    # 2. Split into chunks
    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    chunks, metadatas = [], []
    for file in files:
        try:
            text = Path(file).read_text(encoding="utf-8", errors="ignore")
            for chunk in splitter.split_text(text):
                chunks.append(chunk)
                metadatas.append({"file": str(file)})
        except Exception as e:
            print(f"Skipping {file}: {e}")

    # 3. Embed with Azure OpenAI (explicit env vars)
    embeddings = AzureOpenAIEmbeddings(
        deployment="text-embedding-3-small",   # must match your Azure deployment
        model="text-embedding-3-small",
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2023-05-15")
    )

    # 4. Save FAISS vector store
    faiss_store = FAISS.from_texts(chunks, embeddings, metadatas=metadatas)
    faiss_store.save_local("vectorstore")
    print("✅ Vectorstore created at ./vectorstore")

    return faiss_store


def search(faiss_store, query):
    results = faiss_store.similarity_search(query, k=3)
    for r in results:
        print(f"\n📄 From file: {r.metadata['file']}\n{r.page_content[:200]}...\n")
    return results


# Run directly
if __name__ == "__main__":
    store = init_corpus("corpus")  # make sure your repo's corpus folder is here
    search(store, "semrush pipeline json pattern")
