"""
Street lookup client using data.gov.il CKAN API.

Fetches official street names for a given city and checks whether
a project name contains a recognized street.
"""
from __future__ import annotations

import json
import re
import urllib.request
import urllib.parse
from typing import Optional, Tuple, Set, List

RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
BASE_URL = "https://data.gov.il/api/3/action/datastore_search"
PAGE_LIMIT = 1000

# Common utility name prefixes to strip when extracting city name
_UTILITY_PREFIXES = [
    "מי ",
    "מים ",
    "תאגיד מי ",
    "תאגיד ",
    "מקורות ",
]

# Module-level cache: {city_name_normalized: set_of_streets}
_streets_cache: dict[str, Set[str]] = {}


def _normalize_street(name: str) -> str:
    """Normalize a street name for comparison: strip, collapse whitespace, remove niqqud-like yod variants."""
    s = re.sub(r"\s+", " ", str(name).strip())
    # Normalize common Hebrew spelling variants (ktiv maleh -> ktiv haser)
    # קריית -> קרית, שכוניית -> שכונית, etc.
    s = re.sub(r"יי", "י", s)
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


def _fetch_page(city_filter: str, offset: int, timeout: float) -> dict:
    """Fetch a single page from the CKAN API."""
    params = urllib.parse.urlencode({
        "resource_id": RESOURCE_ID,
        "filters": json.dumps({"city_name": city_filter}),
        "limit": PAGE_LIMIT,
        "offset": offset,
    })
    url = f"{BASE_URL}?{params}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_streets_for_city(city_name: str, timeout: float = 5.0) -> Set[str]:
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
            data = _fetch_page(city_filter, offset, timeout)
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


def _extract_street_candidates(text: str) -> List[str]:
    """
    Extract candidate street names from a project name.
    Handles patterns like "רח' X", "רחוב X", and general token/bigram extraction.
    """
    candidates: List[str] = []
    norm = _normalize_street(text)

    # Pattern 1: "רח' <name>" or "רחוב <name>"
    for m in re.finditer(r"(?:רח['\u2019]|רחוב)\s+(\S+(?:\s+\S+)?)", norm):
        candidates.append(_normalize_street(m.group(1)))

    # Pattern 2: all single tokens (words)
    tokens = norm.split()
    for t in tokens:
        clean = _normalize_street(t)
        if len(clean) >= 2:
            candidates.append(clean)

    # Pattern 3: bigrams (consecutive word pairs)
    for i in range(len(tokens) - 1):
        bigram = _normalize_street(f"{tokens[i]} {tokens[i+1]}")
        candidates.append(bigram)

    return candidates


def is_street_in_city(
    project_name: str,
    city_name: str,
    streets: Set[str],
) -> Tuple[bool, Optional[str]]:
    """
    Check if any token/bigram from project_name matches a known street.
    Returns (found, matched_street_name).
    """
    if not streets:
        return False, None

    candidates = _extract_street_candidates(project_name)
    for candidate in candidates:
        if candidate in streets:
            return True, candidate

    return False, None
