"""OpenAI client wrapper for metadata resolution and clause enrichment.

Provides helper functions that call the Chat Completions API with structured
JSON output and automatic retry.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import config

logger = logging.getLogger(__name__)


def _get_client():
    """Lazily create and return an OpenAI client."""
    from openai import OpenAI

    if not config.OPENAI_API_KEY:
        raise RuntimeError(
            "OpenAI API key is not configured. Set OPENAI_API_KEY in your .env file."
        )
    return OpenAI(api_key=config.OPENAI_API_KEY)


def chat_json(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    max_retries: int = 1,
    temperature: float = 0.0,
) -> dict[str, Any]:
    """Call OpenAI Chat Completions and parse the response as JSON.

    Parameters
    ----------
    system_prompt : str
        The system-level instruction.
    user_prompt : str
        The user-level prompt containing context/data.
    model : str, optional
        Override the model from config.
    max_retries : int
        How many times to retry if the response is not valid JSON.
    temperature : float
        Sampling temperature.

    Returns
    -------
    dict
        Parsed JSON from the assistant's response.

    Raises
    ------
    ValueError
        If valid JSON cannot be obtained after retries.
    """
    client = _get_client()
    used_model = model or config.OPENAI_MODEL
    last_error: Exception | None = None

    for attempt in range(1 + max_retries):
        try:
            response = client.chat.completions.create(
                model=used_model,
                temperature=temperature,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            content = response.choices[0].message.content or "{}"
            return json.loads(content)
        except json.JSONDecodeError as exc:
            last_error = exc
            logger.warning(
                "Attempt %d/%d: invalid JSON from LLM -- retrying",
                attempt + 1,
                1 + max_retries,
            )
        except Exception as exc:
            last_error = exc
            logger.error("OpenAI API error: %s", exc)
            raise

    raise ValueError(
        f"Could not obtain valid JSON after {1 + max_retries} attempts. "
        f"Last error: {last_error}"
    )


def embed_texts(texts: list[str], *, model: str | None = None) -> list[list[float]]:
    """Return embeddings for a batch of texts.

    Parameters
    ----------
    texts : list[str]
        Texts to embed.
    model : str, optional
        Embedding model name (default from config).

    Returns
    -------
    list[list[float]]
        One embedding vector per input text.
    """
    client = _get_client()
    used_model = model or config.OPENAI_EMBEDDING_MODEL
    response = client.embeddings.create(input=texts, model=used_model)
    return [item.embedding for item in response.data]
