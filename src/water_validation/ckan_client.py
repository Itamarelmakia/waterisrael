"""
Street lookup client using data.gov.il CKAN API.

Fetches official street names for a given city and checks whether
a project name contains a recognized street.
"""
from __future__ import annotations

import json
import re
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass
from typing import Optional, Tuple, Set, List

RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
BASE_URL = "https://data.gov.il/api/3/action/datastore_search"
PAGE_LIMIT = 1000

# Common utility name prefixes to strip when extracting city name
_UTILITY_PREFIXES = [
    "תאגיד מי ",
    "מי ",
    "מים ",
    "תאגיד ",
    "מקורות ",
]

# Infrastructure/water stopwords — excluded from UNIGRAM candidates
_STOPWORDS = frozenset([
    "מכון", "מט\"ש", "קו", "קווים", "סניקה", "ביוב", "מים", "מאגר",
    "תחנה", "משאבה", "משאבות", "גנרטור", "מדידה", "לחצים", "מתקן",
    "שדרוג", "חיזוק", "החלפה", "מערב", "מזרח", "צפון", "דרום",
    "כולל", "תכנית", "תוכנית", "שיקום", "קווי", "מערכת", "קידוח",
    "צינור", "צנרת", "אספקה", "הספקה", "טיפול", "שפכים", "ניקוז",
])

# Module-level cache: {city_name_normalized: set_of_streets}
_streets_cache: dict[str, Set[str]] = {}


def _normalize_street(name: str) -> str:
    """Normalize a street name for comparison: strip, collapse whitespace, Hebrew spelling variants."""
    s = re.sub(r"\s+", " ", str(name).strip())
    # Normalize common Hebrew spelling variants (ktiv maleh / ktiv haser)
    s = re.sub(r"יי", "י", s)
    # קריית <-> קרית (standardize to קרית for consistent matching)
    s = re.sub(r"קריית", "קרית", s)
    return s


def utility_to_city_name(utility_name: str) -> str:
    """
    Extract city name from utility name.
    E.g. 'מי רעננה' -> 'רעננה', 'תאגיד מי כרמל' -> 'כרמל'.
    """
    name = _normalize_street(utility_name)
    for prefix in _UTILITY_PREFIXES:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def _fetch_page_with_retry(
    city_filter: str,
    offset: int,
    timeout: float = 10.0,
    retries: int = 2,
) -> dict:
    """Fetch a single page from the CKAN API with retries."""
    params = urllib.parse.urlencode({
        "resource_id": RESOURCE_ID,
        "filters": json.dumps({"city_name": city_filter}),
        "limit": PAGE_LIMIT,
        "offset": offset,
    })
    url = f"{BASE_URL}?{params}"
    backoffs = [0.5, 1.0]

    last_err: Exception | None = None
    for attempt in range(1 + retries):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoffs[min(attempt, len(backoffs) - 1)])
    raise last_err  # type: ignore[misc]


def fetch_streets_for_city(city_name: str, timeout: float = 10.0) -> Set[str]:
    """
    Fetch all street names for a given city from data.gov.il.
    Returns a set of normalized street names.
    Results are cached per city.

    Note: The API stores city names with trailing spaces (e.g. 'רעננה ').
    We try both the clean name and with trailing space.
    """
    cache_key = _normalize_street(city_name)
    if cache_key in _streets_cache:
        return _streets_cache[cache_key]

    streets: Set[str] = set()

    # Try with trailing space first (API convention), then without
    city_variants = [cache_key + " ", cache_key]

    for city_filter in city_variants:
        offset = 0
        while True:
            data = _fetch_page_with_retry(city_filter, offset, timeout=timeout)
            records = data.get("result", {}).get("records", [])
            if not records:
                break

            for rec in records:
                raw = rec.get("street_name", "")
                normed = _normalize_street(raw)
                if normed:
                    streets.add(normed)

            if len(records) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT

        if streets:
            break  # Found results, no need to try next variant

    _streets_cache[cache_key] = streets
    return streets


# ---------------------------------------------------------------------------
# Scoring-based street matching
# ---------------------------------------------------------------------------

_HEB_LETTER_RE = re.compile(r"^[\u0590-\u05FF]+$")


def _is_valid_unigram(token: str) -> bool:
    """Check if a token is a valid unigram candidate (Hebrew, >=3 chars, not stopword, no digits)."""
    if len(token) < 3:
        return False
    if any(c.isdigit() for c in token):
        return False
    # Strip punctuation for Hebrew check
    clean = re.sub(r"[^\u0590-\u05FF]", "", token)
    if not clean or len(clean) < 3:
        return False
    if token in _STOPWORDS or clean in _STOPWORDS:
        return False
    return True


# Minimum score to accept a street match (passing score)
PASSING_SCORE = 3


@dataclass
class StreetMatch:
    """Result of street matching with scoring."""
    found: bool
    street: Optional[str] = None
    score: int = 0
    match_type: str = ""  # "exact" | "substring_exact" | "bigram" | "unigram" | "pattern"
    candidate: str = ""
    exact_match: bool = False  # True when project name matches street 100% or contains exact street name


def _street_appears_as_word(street: str, text: str) -> bool:
    """True if street appears in text as a whole word (word boundaries)."""
    if not street or len(street) < 2:
        return False
    # Word boundary: start/space/+ before, space/+/end after
    pat = r"(^|[\s+])" + re.escape(street) + r"($|[\s+])"
    return bool(re.search(pat, text))


def find_best_street_match(
    project_name: str,
    streets: Set[str],
) -> StreetMatch:
    """
    Score-based street matching. Returns the best match.

    - Exact full match (project name equals a street after normalization): score = PASSING_SCORE, exact_match=True.
    - Substring exact match (project name contains a known street as whole word): score = PASSING_SCORE, exact_match=True.
    - bigram match: 3 points
    - unigram match: 1 point (alone does not pass; exact/unigram override fixes single-word streets)
    - "רחוב/רח'" pattern bonus: +2 points
    Threshold: best_score >= PASSING_SCORE to accept.
    """
    if not streets:
        return StreetMatch(found=False)

    norm = _normalize_street(project_name)
    tokens = norm.split()

    best = StreetMatch(found=False)

    # --- Exact full match override: 100% match after cleaning → passing score (single-word streets) ---
    if norm in streets:
        return StreetMatch(
            found=True,
            street=norm,
            score=PASSING_SCORE,
            match_type="exact",
            candidate=norm,
            exact_match=True,
        )

    # --- Substring exact match: project name contains a known street (whole word) ---
    substring_match: Optional[str] = None
    for street in streets:
        if _street_appears_as_word(street, norm):
            if substring_match is None or len(street) > len(substring_match):
                substring_match = street
    if substring_match is not None:
        return StreetMatch(
            found=True,
            street=substring_match,
            score=PASSING_SCORE,
            match_type="substring_exact",
            candidate=substring_match,
            exact_match=True,
        )

    # Collect pattern-matched names (רחוב/רח') for bonus scoring
    pattern_names: set[str] = set()
    for m in re.finditer(r"(?:רח['\u2019]|רחוב)\s+(\S+(?:\s+\S+)?)", norm):
        pattern_names.add(_normalize_street(m.group(1)))

    # Phase 1: Bigram candidates (score=3)
    for i in range(len(tokens) - 1):
        bigram = _normalize_street(f"{tokens[i]} {tokens[i+1]}")
        if bigram in streets:
            score = PASSING_SCORE
            if bigram in pattern_names:
                score += 2
            if score > best.score:
                best = StreetMatch(
                    found=True, street=bigram, score=score,
                    match_type="bigram", candidate=bigram,
                )

    # Phase 2: Unigram candidates (score=1, +2 if pattern)
    for t in tokens:
        clean = _normalize_street(t)
        if not _is_valid_unigram(clean):
            continue
        if clean in streets:
            score = 1
            if clean in pattern_names:
                score += 2
            if score > best.score:
                best = StreetMatch(
                    found=True, street=clean, score=score,
                    match_type="pattern" if clean in pattern_names else "unigram",
                    candidate=clean,
                )

    # Phase 3: Pattern-only candidates not yet checked
    for pname in pattern_names:
        if pname in streets:
            score = PASSING_SCORE  # pattern match = 1 base + 2 bonus
            if score > best.score:
                best = StreetMatch(
                    found=True, street=pname, score=score,
                    match_type="pattern", candidate=pname,
                )

    # Threshold: only accept if score >= PASSING_SCORE
    if best.score < PASSING_SCORE:
        return StreetMatch(found=False, street=best.street, score=best.score,
                           match_type=best.match_type, candidate=best.candidate)

    return best


# ---------------------------------------------------------------------------
# Helper: count meaningful Hebrew tokens (for decision boundaries)
# ---------------------------------------------------------------------------

def count_meaningful_tokens(text: str) -> int:
    """Count meaningful Hebrew tokens (no numbers, no single chars, no stopwords)."""
    norm = _normalize_street(text)
    count = 0
    for t in norm.split():
        clean = re.sub(r"[^\u0590-\u05FF]", "", t)
        if len(clean) < 2:
            continue
        if any(c.isdigit() for c in t):
            continue
        count += 1
    return count


def is_short_name(text: str) -> bool:
    """Check if project name is short enough for CLEAN status (≤ 3 meaningful tokens)."""
    return count_meaningful_tokens(text) <= 3


def has_explicit_street_pattern(text: str) -> bool:
    """Check if text has explicit 'רחוב' or 'רח'' pattern."""
    return bool(re.search(r"(?:רח['\u2019]|רחוב)\s+\S+", _normalize_street(text)))


def is_street_then_number(text: str) -> bool:
    """Check if text is exactly '<street>' or '<street> <number>'."""
    norm = _normalize_street(text)
    tokens = norm.split()
    if len(tokens) == 1:
        return True
    if len(tokens) == 2 and tokens[1].isdigit():
        return True
    return False


# ---------------------------------------------------------------------------
# Legacy API (kept for backward compatibility but delegates to scoring)
# ---------------------------------------------------------------------------

def _extract_street_candidates(text: str) -> List[str]:
    """Legacy: extract candidates. Now delegates to scoring internally."""
    norm = _normalize_street(text)
    candidates: List[str] = []

    for m in re.finditer(r"(?:רח['\u2019]|רחוב)\s+(\S+(?:\s+\S+)?)", norm):
        candidates.append(_normalize_street(m.group(1)))

    tokens = norm.split()
    for i in range(len(tokens) - 1):
        bigram = _normalize_street(f"{tokens[i]} {tokens[i+1]}")
        candidates.append(bigram)

    for t in tokens:
        clean = _normalize_street(t)
        if _is_valid_unigram(clean):
            candidates.append(clean)

    return candidates


def is_street_in_city(
    project_name: str,
    city_name: str,
    streets: Set[str],
) -> Tuple[bool, Optional[str]]:
    """
    Legacy API. Returns (found, matched_street).
    Now delegates to find_best_street_match with score threshold.
    """
    result = find_best_street_match(project_name, streets)
    return result.found, result.street
