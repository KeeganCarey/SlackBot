"""
Data source abstraction layer.

Replace or extend the `query` method with real connectors — a database,
REST API, CSV files, a data warehouse, etc.
"""

from typing import Any


class DataSources:
    async def query(self, query: str, filters: dict | None = None) -> Any:
        """
        Execute a data query and return results.

        TODO: implement real data connectors here. Examples:
          - SQL:  run a generated query against a database
          - API:  call an internal REST endpoint
          - File: load and filter a CSV / JSON dataset
        """
        raise NotImplementedError(
            "No data source is configured yet. "
            "Implement DataSources.query() in app/data/sources.py."
        )
