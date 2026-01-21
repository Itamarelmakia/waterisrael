import json
import os
from typing import Tuple, Optional

ALLOWED = ["השקעה", "שיקום", "שיקום / שדרוג", "תחזוקה / שוטף"]

def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))

def _extract_label(text: str) -> Optional[str]:
    t = (text or "").strip()
    if t in ALLOWED:
        return t
    for lab in ALLOWED:
        if lab in t:
            return lab
    return None
"""
def _get_clientOpenAI():
    # import כאן כדי שלא יהיה תלות בזמן import של המודול
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI(api_key=api_key)

    """

def _get_client(provider: str):
    provider = (provider or "").strip().lower()

    if provider == "gemini":
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable")

        # Support the common Gemini SDK
        try:
            import google.generativeai as genai  # pip install google-generativeai
        except Exception as e:
            raise ImportError(
                "Gemini selected but google-generativeai is not installed. "
                "Install with: pip install google-generativeai"
            ) from e

        genai.configure(api_key=api_key)
        return genai

    if provider == "openai":
        from openai import OpenAI
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY environment variable")
        return OpenAI(api_key=api_key)

    raise ValueError(f"Unknown LLM provider: {provider!r} (expected 'gemini' or 'openai')")



#def classify_funding_with_confidence(prompt: str, model: str = "gpt-4o") -> Tuple[str, float]:
def classify_funding_with_confidence(
    prompt: str,
    *,
    model: str = "gemini-1.5-flash",
    provider: str = "gemini",
) -> tuple[str, float]:
    provider = (provider or "").strip().lower()
    client = _get_client(provider)

    if provider == "gemini":
        gm = client.GenerativeModel(model)
        resp = gm.generate_content(prompt)
        text = (resp.text or "").strip()

    else:
        # openai
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Return a JSON object only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        text = (resp.choices[0].message.content or "").strip()

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "Return ONLY valid JSON: {\"label\": <one of allowed>, \"confidence\": <0..1>} "
                    "No extra text."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        max_tokens=60,
    )

    raw = (resp.choices[0].message.content or "").strip()

    try:
        obj = json.loads(raw)
        label = str(obj.get("label", "")).strip()
        conf = obj.get("confidence", 0.0)
        if label not in ALLOWED:
            salv = _extract_label(raw)
            if salv:
                return salv, 0.3
            return label, 0.0
        return label, _clamp01(conf)
    except Exception:
        salv = _extract_label(raw)
        if salv:
            return salv, 0.3
        return raw[:50], 0.0

def classify_text(prompt: str, model: str = "gpt-4o") -> str:
    label, _ = classify_funding_with_confidence(prompt, model=model)
    return label
