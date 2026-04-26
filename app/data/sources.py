import asyncio
import csv
from pathlib import Path

from rapidfuzz import fuzz

DATA_DIR = Path(__file__).parent.parent.parent / "mock_data"
FUZZY_THRESHOLD = 75  # 0-100; lower = more lenient


def _load_csv(name: str) -> list[dict[str, str]]:
    with open(DATA_DIR / f"{name}.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _filter_rows(rows: list[dict], filters: dict | None) -> list[dict]:
    if not filters:
        return rows
    result = []
    for row in rows:
        if all(
            fuzz.partial_ratio(str(v).lower(), row.get(k, "").lower()) >= FUZZY_THRESHOLD
            for k, v in filters.items()
        ):
            result.append(row)
    return result


class DataSources:
    async def get_projects(self, filters: dict | None = None) -> list[dict]:
        rows = await asyncio.get_event_loop().run_in_executor(None, _load_csv, "projects")
        return _filter_rows(rows, filters)

    async def get_invoices(self, filters: dict | None = None) -> list[dict]:
        rows = await asyncio.get_event_loop().run_in_executor(None, _load_csv, "invoices")
        return _filter_rows(rows, filters)

    async def get_contracts(self, filters: dict | None = None) -> list[dict]:
        rows = await asyncio.get_event_loop().run_in_executor(None, _load_csv, "contracts")
        return _filter_rows(rows, filters)

    async def get_time_entries(self, filters: dict | None = None) -> list[dict]:
        rows = await asyncio.get_event_loop().run_in_executor(None, _load_csv, "time_entries")
        return _filter_rows(rows, filters)

    async def get_calendar(self, filters: dict | None = None) -> list[dict]:
        rows = await asyncio.get_event_loop().run_in_executor(None, _load_csv, "calendar")
        return _filter_rows(rows, filters)


data_sources = DataSources()
