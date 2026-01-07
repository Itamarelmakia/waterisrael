from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable


def iter_tree(root: Path, max_depth: int = 4) -> Iterable[Path]:
    """Yield paths under root up to max_depth, skipping common noise."""
    skip = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".idea", ".vscode"}
    root = root.resolve()
    for p in sorted(root.rglob("*")):
        # depth relative to root
        try:
            rel = p.relative_to(root)
        except ValueError:
            continue
        if any(part in skip for part in rel.parts):
            continue
        if len(rel.parts) > max_depth:
            continue
        yield p


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    cwd = Path.cwd().resolve()
    this_file = Path(__file__).resolve()
    script_dir = this_file.parent

    print_header("BASIC PATHS")
    print(f"Current working directory (cwd): {cwd}")
    print(f"This script file:              {this_file}")
    print(f"This script directory:         {script_dir}")
    print(f"OS environment PYTHONPATH:     {os.environ.get('PYTHONPATH', '')}")

    print_header("PYTHON RUNTIME")
    print(f"Python executable:            {sys.executable}")
    print(f"Python version:               {sys.version.split()[0]}")

    print_header("sys.path (import search order)")
    for i, p in enumerate(sys.path):
        print(f"{i:02d}: {p}")

    # Try to locate your expected package folder(s)
    candidate_roots = []
    for name in ["src", "water_validation"]:
        cand = script_dir / name
        if cand.exists():
            candidate_roots.append(cand)

    print_header("CANDIDATE PROJECT ROOTS (found next to debug_env.py)")
    if candidate_roots:
        for c in candidate_roots:
            print(f"Found: {c}")
    else:
        print("No 'src' or 'water_validation' folder found next to debug_env.py.")

    # Print tree from project root (= where debug_env.py sits)
    print_header("PROJECT TREE (limited depth)")
    for p in iter_tree(script_dir, max_depth=6):
        rel = p.relative_to(script_dir)
        marker = "/" if p.is_dir() else ""
        print(f"{rel}{marker}")

    # Import resolution test (optional)
    print_header("IMPORT RESOLUTION TESTS")
    try:
        import water_validation  # type: ignore
        print("Imported water_validation OK")
        print("water_validation.__file__ =", getattr(water_validation, "__file__", None))
    except Exception as e:
        print("Failed importing water_validation:", repr(e))

    # Show what 'io' resolves to (to confirm the collision)
    try:
        import io  # stdlib
        print("stdlib io.__file__ =", getattr(io, "__file__", None))
    except Exception as e:
        print("Failed importing stdlib io:", repr(e))


if __name__ == "__main__":
    main()
