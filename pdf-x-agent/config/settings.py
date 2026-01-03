import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
PDF_DIR = Path("C:/Documents")  # Change this to your PDF folder
DATA_DIR = BASE_DIR / "data"
EMBEDDINGS_DIR = DATA_DIR / "embeddings"
PROCESSED_DIR = DATA_DIR / "processed"

# Create directories if they don't exist
for directory in [PDF_DIR, DATA_DIR, EMBEDDINGS_DIR, PROCESSED_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Model settings - USING QWEN2.5:4B
MODEL_TYPE = "ollama"
EMBEDDING_MODEL = "nomic-embed-text"  # Good embeddings model
LLM_MODEL = "qwen2.5:4b"  # Your Qwen model

# Ollama settings
OLLAMA_BASE_URL = "http://localhost:11434"

# X API settings
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "")

# Agent settings
MAX_POST_LENGTH = 280
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# Logging
LOG_LEVEL = "INFO"