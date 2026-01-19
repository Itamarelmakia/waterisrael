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

def _get_client():
    # import כאן כדי שלא יהיה תלות בזמן import של המודול
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI(api_key=api_key)

def classify_funding_with_confidence(prompt: str, model: str = "gpt-4o") -> Tuple[str, float]:
    client = _get_client()

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
