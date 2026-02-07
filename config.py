"""Central configuration for the contract hierarchy system."""

import os
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Azure Document Intelligence
# ---------------------------------------------------------------------------
AZURE_DOCINT_ENDPOINT: str = os.getenv("AZURE_DOCINT_ENDPOINT", "")
AZURE_DOCINT_KEY: str = os.getenv("AZURE_DOCINT_KEY", "")

# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-5-mini")
OPENAI_EMBEDDING_MODEL: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///contract_hierarchy.db")

# ---------------------------------------------------------------------------
# ChromaDB
# ---------------------------------------------------------------------------
CHROMA_PERSIST_DIR: str = os.getenv("CHROMA_PERSIST_DIR", "./chroma_data")
CHROMA_COLLECTION_NAME: str = os.getenv("CHROMA_COLLECTION_NAME", "contract_clauses")

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
METADATA_CONFIDENCE_THRESHOLD: float = float(
    os.getenv("METADATA_CONFIDENCE_THRESHOLD", "0.7")
)
ENRICHMENT_CONFIDENCE_THRESHOLD: float = float(
    os.getenv("ENRICHMENT_CONFIDENCE_THRESHOLD", "0.7")
)

# ---------------------------------------------------------------------------
# PageIndex
# ---------------------------------------------------------------------------
PAGEINDEX_ENABLED: bool = os.getenv("PAGEINDEX_ENABLED", "false").lower() == "true"
PAGEINDEX_MODEL: str = os.getenv("PAGEINDEX_MODEL", "gpt-5-mini")
PAGEINDEX_VISION_MODEL: str = os.getenv("PAGEINDEX_VISION_MODEL", "gpt-5-mini")
PAGEINDEX_RETRIEVAL_TOP_K: int = int(os.getenv("PAGEINDEX_RETRIEVAL_TOP_K", "5"))
