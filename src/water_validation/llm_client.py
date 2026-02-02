"""
LLM client for funding classification.
Supports Gemini and OpenAI providers.
"""
import json
import os
import re
from typing import Tuple


class LLMQuotaError(RuntimeError):
    """Raised when LLM quota is exceeded or rate-limited."""
    pass


def _extract_json(text: str) -> dict:
    """
    Robust JSON extraction that accepts:
    - pure JSON
    - JSON wrapped in ```json ... ```
    - JSON embedded in text (extracts first {...})
    """
    if not text:
        raise ValueError("Empty LLM response")

    t = text.strip()

    # Strip fenced code blocks (```json or ```)
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s*```$", "", t).strip()

    # If not pure JSON, try to find first {...}
    if not (t.startswith("{") and t.endswith("}")):
        m = re.search(r"\{.*\}", t, flags=re.DOTALL)
        if not m:
            raise ValueError(f"Could not find JSON object in response: {text!r}")
        t = m.group(0)

    return json.loads(t)


def _clamp01(x: float) -> float:
    """Clamp value to [0.0, 1.0]"""
    return max(0.0, min(1.0, float(x)))


def classify_funding_with_confidence(
    prompt: str,
    *,
    provider: str = "gemini",
    model: str = "gemini-1.5-flash",
) -> Tuple[str, float]:
    """
    Classify project funding using LLM.

    Returns: (label, confidence)
    - label: The predicted funding label
    - confidence: Float between 0 and 1

    Raises:
    - LLMQuotaError: When quota is exceeded or rate-limited
    - ValueError: When API keys are missing
    - Other exceptions: LLM API errors
    """
    provider_l = (provider or "").strip().lower()

    try:
        if provider_l == "gemini":
            import google.generativeai as gm

            api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
            if not api_key:
                raise RuntimeError("Missing GEMINI_API_KEY/GOOGLE_API_KEY")

            gm.configure(api_key=api_key)
            m = gm.GenerativeModel(model)
            resp = m.generate_content(prompt)
            text = (resp.text or "").strip()

        elif provider_l == "openai":
            from openai import OpenAI

            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise RuntimeError("Missing OPENAI_API_KEY")

            client = OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            text = (resp.choices[0].message.content or "").strip()

        else:
            raise ValueError(f"Unknown LLM provider: {provider!r} (expected 'gemini' or 'openai')")

    except Exception as e:
        # Detect quota / rate-limit errors (Gemini and OpenAI)
        msg = repr(e)
        if any(x in msg.lower() for x in ["resourceexhausted", "quota", "rate_limit", "rate-limit", "429"]):
            raise LLMQuotaError(msg) from e
        raise

    # Parse the JSON response
    obj = _extract_json(text)

    label = str(obj.get("label", "")).strip()
    conf = obj.get("confidence", 0.0)

    # Parse and clamp confidence
    try:
        conf = float(conf)
    except (ValueError, TypeError):
        conf = 0.0

    conf = _clamp01(conf)

    return label, conf


def generate_text(
    prompt: str,
    *,
    provider: str = "gemini",
    model: str = "gemini-1.5-flash",
) -> str:
    """
    Call LLM and return raw text response (no JSON parsing).
    Used for free-form text generation like executive summaries.
    """
    provider_l = (provider or "").strip().lower()

    try:
        if provider_l == "gemini":
            import google.generativeai as gm

            api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
            if not api_key:
                raise RuntimeError("Missing GEMINI_API_KEY/GOOGLE_API_KEY")

            gm.configure(api_key=api_key)
            m = gm.GenerativeModel(model)
            resp = m.generate_content(prompt)
            return (resp.text or "").strip()

        elif provider_l == "openai":
            from openai import OpenAI

            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise RuntimeError("Missing OPENAI_API_KEY")

            client = OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            return (resp.choices[0].message.content or "").strip()

        else:
            raise ValueError(f"Unknown LLM provider: {provider!r} (expected 'gemini' or 'openai')")

    except Exception as e:
        msg = repr(e)
        if any(x in msg.lower() for x in ["resourceexhausted", "quota", "rate_limit", "rate-limit", "429"]):
            raise LLMQuotaError(msg) from e
        raise
