import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).parent.parent.parent / "mock_data"
MANIFEST_PATH = DATA_DIR / "tools_manifest.json"
MANIFEST_VERSION = 3
MAX_SAMPLE_ROWS = 200
PROMPT_SAMPLE_ROWS = 3


@dataclass
class TableSchema:
    name: str
    path: str
    columns: list[str]
    column_types: dict[str, str]
    numeric_columns: list[str]
    date_columns: list[str]
    sample_rows: list[dict[str, str]]
    row_count_estimate: int


def _looks_number(value: str) -> bool:
    text = (value or "").strip().replace("$", "").replace(",", "").replace("%", "")
    if text == "":
        return False
    try:
        float(text)
        return True
    except ValueError:
        return False


def _looks_date(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    fmts = ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y %I:%M:%S %p", "%I:%M:%S %p")
    for fmt in fmts:
        try:
            datetime.strptime(text, fmt)
            return True
        except ValueError:
            continue
    return False


def _infer_column_type(values: list[str]) -> str:
    present = [v for v in values if (v or "").strip() != ""]
    if not present:
        return "string"
    numeric_hits = sum(1 for v in present if _looks_number(v))
    date_hits = sum(1 for v in present if _looks_date(v))
    total = len(present)
    if numeric_hits / total >= 0.9:
        return "number"
    if date_hits / total >= 0.85:
        return "date"
    return "string"


def discover_csv_tables(data_dir: Path = DATA_DIR) -> list[Path]:
    return sorted([p for p in data_dir.glob("*.csv") if p.is_file()], key=lambda p: p.name.lower())


def introspect_table(path: Path) -> TableSchema:
    rows: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = list(reader.fieldnames or [])
        for idx, row in enumerate(reader):
            if idx < MAX_SAMPLE_ROWS:
                rows.append({k: (v or "") for k, v in row.items()})
            else:
                break

    values_by_col: dict[str, list[str]] = {c: [] for c in columns}
    for row in rows:
        for col in columns:
            values_by_col[col].append(row.get(col, ""))

    column_types = {col: _infer_column_type(vals) for col, vals in values_by_col.items()}
    numeric_columns = [c for c, t in column_types.items() if t == "number"]
    date_columns = [c for c, t in column_types.items() if t == "date"]
    sample_rows = rows[:PROMPT_SAMPLE_ROWS]

    return TableSchema(
        name=path.stem,
        path=str(path),
        columns=columns,
        column_types=column_types,
        numeric_columns=numeric_columns,
        date_columns=date_columns,
        sample_rows=sample_rows,
        row_count_estimate=len(rows),
    )


def build_fingerprint(csv_paths: list[Path]) -> str:
    payload: list[dict[str, Any]] = []
    for path in csv_paths:
        stat = path.stat()
        with open(path, "rb") as f:
            head = f.read(8192)
        payload.append({
            "name": path.name,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "head_sha1": hashlib.sha1(head).hexdigest(),
        })
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
