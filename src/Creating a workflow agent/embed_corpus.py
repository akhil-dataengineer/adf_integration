# %pip install -q langchain chromadb langchain-openai tiktoken

#hello

"""
Databricks embedding builder
- Reads your repo's ./corpus folder (requirements, design, test, build, prompts, misc)
- Indexes only text-like files (.txt,.md,.json,.py,.sql,.csv,.yml,.yaml,.ini,.cfg,.log,.xml)
- Saves a persistent Chroma index to DBFS: /dbfs/FileStore/rag_index
- Quick retrieval smoke test at the end
Notes:
- Set your key once: 1) via a Databricks secret, or 2) env var OPENAI_API_KEY.
"""

import os, glob, pathlib, shutil
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings

# ----------- CONFIG -------------
CORPUS_DIR_CANDIDATES = [
    "corpus",                                # if your notebook sits at repo root
    "./corpus",
    "/Workspace/Repos/akhil-dataengineer/adf_integration/corpus",  # fallback example
]
PERSIST_DIR = "/dbfs/FileStore/rag_index"
COPY_CORPUS_TO_DBFS = True                  # keeps a snapshot in DBFS for reference
DBFS_CORPUS_DST = "/dbfs/FileStore/corpus"  # where the snapshot lands
TEXT_EXTS = {".txt",".md",".json",".py",".sql",".csv",".yml",".yaml",".ini",".cfg",".log",".xml"}
CHUNK_SIZE = 1200
CHUNK_OVERLAP = 150
# --------------------------------

def _find_corpus_root():
    for c in CORPUS_DIR_CANDIDATES:
        if os.path.isdir(c):
            return os.path.abspath(c)
    raise FileNotFoundError("corpus folder not found. Open the notebook at repo root or update CORPUS_DIR_CANDIDATES.")

def _copy_corpus_to_dbfs(src, dst):
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    print(f"📦 copied corpus snapshot to {dst}")

def _load_text_files(root):
    files = [p for p in glob.glob(os.path.join(root, "**", "*"), recursive=True) if os.path.isfile(p)]
    texts, metas = [], []
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)

    total_files = 0
    for f in files:
        total_files += 1
        ext = pathlib.Path(f).suffix.lower()
        if ext in TEXT_EXTS:
            try:
                raw = pathlib.Path(f).read_text(encoding="utf-8", errors="ignore")
                for chunk in splitter.split_text(raw):
                    texts.append(chunk)
                    metas.append({"source": f.replace(root, "corpus")})
            except Exception as e:
                print(f"⚠️  skipped {f}: {e}")
        # non-text (pdf/pptx/docx/images) are skipped in this fast pass
    print(f"🧾 scanned {total_files} files → {len(texts)} chunks from {len(set([m['source'] for m in metas]))} text files")
    return texts, metas

def build_index():
    corpus_root = _find_corpus_root()
    print(f"📂 corpus root: {corpus_root}")

    if COPY_CORPUS_TO_DBFS:
        _copy_corpus_to_dbfs(corpus_root, DBFS_CORPUS_DST)

    if os.path.exists(PERSIST_DIR):
        shutil.rmtree(PERSIST_DIR)

    texts, metas = _load_text_files(corpus_root)

    embeddings = OpenAIEmbeddings()  # uses OPENAI_API_KEY or Azure env vars
    vs = Chroma.from_texts(
        texts=texts,
        embedding=embeddings,
        metadatas=metas,
        persist_directory=PERSIST_DIR,
        collection_name="agent_corpus",
    )
    vs.persist()
    print(f"✅ built Chroma index at {PERSIST_DIR}")

    # quick smoke test
    try:
        res = vs.similarity_search("bronze layer rules or semrush pipeline", k=3)
        print("🔎 sample hits:")
        for d in res:
            print(" -", d.metadata.get("source"), "→", (d.page_content[:120].replace("\n"," ") + "…"))
    except Exception as e:
        print("smoke test skipped:", e)

# run when %run ./embed_corpus
if __name__ == "__main__" or True:
    # If you store key in Databricks secrets, uncomment and set scope/key:
    # import os, json
    # os.environ["OPENAI_API_KEY"] = dbutils.secrets.get("your-scope", "openai-api-key")
    build_index()
