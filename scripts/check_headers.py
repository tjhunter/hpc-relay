# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from __future__ import annotations

import sys
from pathlib import Path

HEADER = """# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
"""

SKIP_DIRS = {".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".venv", "__pycache__"}


def _python_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def _header_start_line(lines: list[str]) -> int:
    if lines and lines[0].startswith("#!"):
        return 1
    return 0


def _has_header(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    start = _header_start_line(lines)
    header_line_count = len(HEADER.splitlines())
    return "".join(lines[start : start + header_line_count]) == HEADER


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    missing = [path.relative_to(root) for path in _python_files(root) if not _has_header(path)]
    if not missing:
        return 0

    print("Missing required ECMWF copyright header:", file=sys.stderr)
    for path in missing:
        print(f"  {path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
