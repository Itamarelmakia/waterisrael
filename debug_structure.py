from __future__ import annotations

import os
import sys
from pathlib import Path

# -------- Settings --------
MAX_DEPTH = 6
IGNORE_DIRS = {
    ".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache",
    "node_modules", ".idea", ".vscode", "dist", "build"
}
TARGET_FILES = {
    "cli.py",
    "runner.py",
    "checks.py",
    "report.py",
    "models.py",
    "excel_io.py",
    "config.py",
    "utils.py",
}

# -------- Helpers --------
def safe_rel(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except Exception:
        return str(p)

def print_tree(root: Path, max_depth: int = 6) -> None:
    print("\n=== PROJECT TREE (depth <= {}) ===".format(max_depth))

    def walk(dir_path: Path, depth: int) -> None:
        if depth > max_depth:
            return

        entries = []
        try:
            entries = sorted(dir_path.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
        except PermissionError:
            print("  " * depth + "⛔ [permission denied]")
            return

        for e in entries:
            if e.is_dir() and e.name in IGNORE_DIRS:
                continue

            prefix = "  " * depth + ("📄 " if e.is_file() else "📁 ")
            print(prefix + e.name)

            if e.is_dir():
                walk(e, depth + 1)

    walk(root, 0)

def find_files(root: Path, names: set[str]) -> dict[str, list[Path]]:
    found: dict[str, list[Path]] = {n: [] for n in names}
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if p.name in names:
            found[p.name].append(p)
    return found

def find_outputs(root: Path) -> list[Path]:
    outputs = []
    for name in ["validation_output.xlsx", "validation_output_debug.xlsx"]:
        for p in root.rglob(name):
            outputs.append(p)
    return outputs

# -------- Main --------
def main() -> None:
    cwd = Path.cwd()
    root = cwd  # assume you run from project root
    print("=== ENV ===")
    print("CWD:", cwd)
    print("Python:", sys.executable)
    print("Python version:", sys.version.split()[0])
    print("OS:", os.name)

    # Quick sanity: show where cli.py is (if any)
    found = find_files(root, TARGET_FILES)
    print("\n=== KEY FILE LOCATIONS ===")
    for fname in sorted(TARGET_FILES):
        paths = found.get(fname, [])
        if not paths:
            print(f"- {fname}: NOT FOUND")
        else:
            for p in paths:
                print(f"- {fname}: {safe_rel(p, root)}")

    outs = find_outputs(root)
    print("\n=== OUTPUT FILES FOUND ===")
    if not outs:
        print("No validation_output*.xlsx found under this folder.")
    else:
        for p in outs:
            print("-", safe_rel(p, root))

    print_tree(root, MAX_DEPTH)

    print("\n=== NEXT STEP HINT ===")
    # suggest how to run if we found cli.py
    cli_paths = found.get("cli.py", [])
    if cli_paths:
        # choose the shortest relative path
        best = min(cli_paths, key=lambda p: len(str(p)))
        rel = safe_rel(best, root)
        print("Try running:")
        print(f'  python "{rel}" --rules R_15')
    else:
        print("No cli.py found. If your entry point is different, tell me the filename.")

if __name__ == "__main__":
    main()
