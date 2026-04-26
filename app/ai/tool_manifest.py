import json
import os
import re
from pathlib import Path
from typing import Any

from openai import OpenAI

from app.data.schema_introspect import (
    DATA_DIR,
    MANIFEST_PATH,
    MANIFEST_VERSION,
    TableSchema,
    build_fingerprint,
    discover_csv_tables,
    introspect_table,
)

SAFE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{2,63}$")
MAX_INFERRED_TOOLS = 5
REGENERATE_ON_FINGERPRINT_MISMATCH_ENV = "TOOL_MANIFEST_REGENERATE_ON_FINGERPRINT_MISMATCH"
REQUIRED_CORE_TOOL_NAMES = {
    "list_tables",
    "describe_table",
    "query_table",
    "aggregate_table",
    "group_aggregate_table",
    "message_workspace_user",
    "create_calendar_event",
}


def _env_true(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "on"}


def _make_core_tool_specs(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table_names = [t["name"] for t in tables]
    table_enum = ", ".join(table_names)
    common_filters_desc = "JSON object mapping column names to values, e.g. {\"Client\":\"Acme\"}"

    return [
        {
            "name": "list_tables",
            "description": "List all available CSV tables and key metadata.",
            "args": [],
        },
        {
            "name": "describe_table",
            "description": f"Describe schema for one table. Table must be one of: {table_enum}",
            "args": [
                {"name": "table", "type": "string", "description": "Table name"},
            ],
        },
        {
            "name": "query_table",
            "description": "Query rows from a table with optional filters/sorting/column selection.",
            "args": [
                {"name": "table", "type": "string", "description": "Table name"},
                {"name": "filters_json", "type": "string", "description": common_filters_desc},
                {"name": "select_columns_csv", "type": "string", "description": "Comma-separated columns to return"},
                {"name": "sort_by", "type": "string", "description": "Sort column"},
                {"name": "sort_order", "type": "string", "description": "asc or desc"},
                {"name": "limit", "type": "number", "description": "Max rows (default 25, max 200)"},
            ],
        },
        {
            "name": "aggregate_table",
            "description": "Compute aggregate over a table (min/max/sum/avg/median/mode/count/distinct_count). Use this for lowest/highest/most/least questions instead of returning full rows.",
            "args": [
                {"name": "table", "type": "string", "description": "Table name"},
                {"name": "aggregate_function", "type": "string", "description": "min|max|sum|avg|median|mode|count|distinct_count"},
                {"name": "aggregate_column", "type": "string", "description": "Column for aggregate; optional for count. Numeric-like text values (e.g. $12,500, 60%) are supported."},
                {"name": "filters_json", "type": "string", "description": common_filters_desc},
            ],
        },
        {
            "name": "group_aggregate_table",
            "description": "Group by one column and aggregate each group.",
            "args": [
                {"name": "table", "type": "string", "description": "Table name"},
                {"name": "group_by", "type": "string", "description": "Group by column"},
                {"name": "aggregate_function", "type": "string", "description": "min|max|sum|avg|median|mode|count|distinct_count"},
                {"name": "aggregate_column", "type": "string", "description": "Aggregate column; optional for count"},
                {"name": "filters_json", "type": "string", "description": common_filters_desc},
                {"name": "limit", "type": "number", "description": "Max groups (default 25, max 200)"},
            ],
        },
        {
            "name": "message_workspace_user",
            "description": "Send a direct message to a workspace user by fuzzy-matching their name.",
            "args": [
                {"name": "name", "type": "string", "description": "Target user's name (real or display name)"},
                {"name": "message", "type": "string", "description": "Message body to send"},
                {"name": "min_match_score", "type": "number", "description": "Optional fuzzy match score threshold 0-100 (default 70)"},
            ],
        },
        {
            "name": "create_calendar_event",
            "description": "Create a Google Calendar event in the configured account/calendar.",
            "args": [
                {"name": "summary", "type": "string", "description": "Event title"},
                {"name": "start_datetime", "type": "string", "description": "RFC3339 datetime, e.g. 2026-05-01T09:00:00"},
                {"name": "end_datetime", "type": "string", "description": "RFC3339 datetime, e.g. 2026-05-01T10:00:00"},
                {"name": "timezone", "type": "string", "description": "IANA timezone, e.g. America/Los_Angeles"},
                {"name": "description", "type": "string", "description": "Optional event description"},
                {"name": "location", "type": "string", "description": "Optional location"},
                {"name": "attendees_csv", "type": "string", "description": "Optional comma-separated attendee emails"},
                {"name": "calendar_id", "type": "string", "description": "Optional calendar ID (default primary)"},
                {"name": "send_updates", "type": "string", "description": "Optional all|externalOnly|none (default none)"},
            ],
        },
    ]


def _default_inferred_tools(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = {t["name"] for t in tables}
    inferred: list[dict[str, Any]] = []
    if "schedule" in names:
        inferred.append({
            "name": "employee_utilization",
            "description": "Summarize scheduled hours per employee, optionally within a date range.",
            "args": [
                {"name": "employee_name", "type": "string", "description": "Optional employee filter"},
                {"name": "start_date", "type": "string", "description": "Optional start date"},
                {"name": "end_date", "type": "string", "description": "Optional end date"},
                {"name": "limit", "type": "number", "description": "Max rows"},
            ],
            "plan": {
                "steps": [
                    {"op": "load_table", "table": "schedule"},
                    {"op": "apply_arg_filters", "mappings": [
                        {"arg": "employee_name", "column": "Employee Name"},
                        {"arg": "start_date", "column": "Start Date", "comparator": "gte"},
                        {"arg": "end_date", "column": "End Date", "comparator": "lte"},
                    ]},
                    {"op": "group_aggregate", "group_by": "Employee Name", "aggregate_function": "sum", "aggregate_column": "Number of hours"},
                    {"op": "sort", "by": "aggregate_value", "order": "desc"},
                    {"op": "top_n", "arg": "limit", "default": 25, "max": 200},
                ]
            },
        })
        inferred.append({
            "name": "overbooked_employees",
            "description": "Find employees whose scheduled hours exceed a threshold over filtered rows.",
            "args": [
                {"name": "threshold_hours", "type": "number", "description": "Minimum total hours (default 8)"},
                {"name": "date", "type": "string", "description": "Optional exact Start Date filter"},
            ],
            "plan": {
                "steps": [
                    {"op": "load_table", "table": "schedule"},
                    {"op": "apply_arg_filters", "mappings": [
                        {"arg": "date", "column": "Start Date"},
                    ]},
                    {"op": "group_aggregate", "group_by": "Employee Name", "aggregate_function": "sum", "aggregate_column": "Number of hours"},
                    {"op": "filter_result", "column": "aggregate_value", "comparator": "gt", "arg": "threshold_hours", "default": 8},
                    {"op": "sort", "by": "aggregate_value", "order": "desc"},
                ]
            },
        })
    if "project" in names and "schedule" in names:
        inferred.append({
            "name": "project_budget_vs_scheduled",
            "description": "Compare scheduled hours/amount against project budget by project.",
            "args": [
                {"name": "project_contains", "type": "string", "description": "Optional project name contains"},
                {"name": "limit", "type": "number", "description": "Max rows"},
            ],
            "plan": {
                "steps": [
                    {"op": "load_table", "table": "schedule", "as": "schedule"},
                    {"op": "apply_arg_filters", "table_alias": "schedule", "mappings": [
                        {"arg": "project_contains", "column": "Project"},
                    ]},
                    {"op": "group_aggregate", "table_alias": "schedule", "group_by": "Project", "aggregate_function": "sum", "aggregate_column": "Number of hours", "as": "hours"},
                    {"op": "group_aggregate", "table_alias": "schedule", "group_by": "Project", "aggregate_function": "sum", "aggregate_column": "Amount", "as": "amount"},
                    {"op": "load_table", "table": "project", "as": "project"},
                    {"op": "join", "left": "hours", "right": "project", "left_on": "group", "right_on": "Project", "as": "joined"},
                    {"op": "join", "left": "joined", "right": "amount", "left_on": "group", "right_on": "group", "as": "joined2"},
                    {"op": "project_columns", "table_alias": "joined2", "columns": ["group", "aggregate_value", "aggregate_value_right", "Budget", "Total Hours Budget"]},
                    {"op": "top_n", "arg": "limit", "default": 25, "max": 200},
                ]
            },
        })
    return inferred[:MAX_INFERRED_TOOLS]


def _build_model_prompt(tables: list[dict[str, Any]]) -> str:
    lines = [
        "You design supplemental read-only analytics tools for CSV tables.",
        "Return strict JSON: {\"tools\": [...]}.",
        "Each tool must have: name, description, args, plan.",
        "Args are [{name,type,description}] with type in [string,number].",
        "Plan steps must use only ops: load_table, apply_arg_filters, group_aggregate, sort, top_n, filter_result, join, project_columns.",
        "Do not use SQL, code, or free-form execution.",
        "Prefer 2-5 high-value tools.",
        "",
        "Table summaries:",
    ]
    for t in tables:
        lines.append(f"- {t['name']}: columns={t['columns']}")
        if t.get("sample_rows"):
            lines.append(f"  sample={t['sample_rows'][0]}")
    return "\n".join(lines)


def _validate_inferred_tools(tools: list[dict[str, Any]], tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid_names = {t["name"] for t in tables}
    valid_columns = {t["name"]: set(t["columns"]) for t in tables}
    allowed_ops = {
        "load_table", "apply_arg_filters", "group_aggregate", "sort", "top_n",
        "filter_result", "join", "project_columns",
    }
    out: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for tool in tools:
        name = str(tool.get("name", "")).strip()
        if not SAFE_NAME_RE.match(name):
            continue
        if name in seen_names or name in REQUIRED_CORE_TOOL_NAMES:
            continue
        steps = ((tool.get("plan") or {}).get("steps") or [])
        if not isinstance(steps, list) or not steps:
            continue
        ok = True
        for step in steps:
            op = str(step.get("op", ""))
            if op not in allowed_ops:
                ok = False
                break
            table = step.get("table")
            if table and table not in valid_names:
                ok = False
                break
            for key in ("group_by", "aggregate_column", "left_on", "right_on"):
                col = step.get(key)
                if not col:
                    continue
                # Best-effort cross-table validation for known table-scoped steps.
                if table and col not in valid_columns.get(table, set()) and key in ("group_by", "aggregate_column"):
                    ok = False
                    break
        if ok:
            args = tool.get("args") or []
            safe_args = []
            for arg in args:
                arg_name = str(arg.get("name", "")).strip()
                arg_type = str(arg.get("type", "string")).strip()
                if SAFE_NAME_RE.match(arg_name) and arg_type in {"string", "number"}:
                    safe_args.append({
                        "name": arg_name,
                        "type": arg_type,
                        "description": str(arg.get("description", "")).strip() or arg_name,
                    })
            out.append({
                "name": name,
                "description": str(tool.get("description", "")).strip() or name,
                "args": safe_args,
                "plan": {"steps": steps},
            })
            seen_names.add(name)
        if len(out) >= MAX_INFERRED_TOOLS:
            break
    return out


def _merge_inferred_tools(preferred: list[dict[str, Any]], baseline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tool in preferred + baseline:
        name = tool.get("name")
        if not name or name in seen:
            continue
        merged.append(tool)
        seen.add(name)
        if len(merged) >= MAX_INFERRED_TOOLS:
            break
    return merged


def _infer_with_model(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return []
    try:
        client = OpenAI(api_key=api_key)
        model = os.environ.get("OPENAI_MODEL", "gpt-5-nano")
        prompt = _build_model_prompt(tables)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        data = json.loads(raw)
        return list((data.get("tools") or []))
    except Exception as exc:
        print(f"[manifest] inferred-tool model generation failed: {exc}")
        return []


def _table_to_dict(t: TableSchema) -> dict[str, Any]:
    return {
        "name": t.name,
        "path": t.path,
        "columns": t.columns,
        "column_types": t.column_types,
        "numeric_columns": t.numeric_columns,
        "date_columns": t.date_columns,
        "sample_rows": t.sample_rows,
        "row_count_estimate": t.row_count_estimate,
    }


def _generate_manifest() -> dict[str, Any]:
    csv_paths = discover_csv_tables(DATA_DIR)
    tables = [_table_to_dict(introspect_table(path)) for path in csv_paths]
    fingerprint = build_fingerprint(csv_paths)
    core_tools = _make_core_tool_specs(tables)

    inferred_candidates = _validate_inferred_tools(_infer_with_model(tables), tables)
    default_inferred = _validate_inferred_tools(_default_inferred_tools(tables), tables)
    inferred_tools = _merge_inferred_tools(inferred_candidates, default_inferred)

    return {
        "version": MANIFEST_VERSION,
        "fingerprint": fingerprint,
        "tables": tables,
        "core_tools": core_tools,
        "inferred_tools": inferred_tools,
    }


def _is_manifest_valid(manifest: dict[str, Any]) -> bool:
    if not manifest:
        return False
    if int(manifest.get("version", 0)) != MANIFEST_VERSION:
        return False
    if not isinstance(manifest.get("tables"), list):
        return False
    if not isinstance(manifest.get("core_tools"), list):
        return False
    if not isinstance(manifest.get("inferred_tools"), list):
        return False
    core_names = {t.get("name") for t in manifest.get("core_tools", []) if isinstance(t, dict)}
    if not REQUIRED_CORE_TOOL_NAMES.issubset(core_names):
        return False
    disallowed_ops = {"rename_columns"}
    for tool in manifest.get("inferred_tools", []):
        steps = ((tool.get("plan") or {}).get("steps") or [])
        for step in steps:
            if str(step.get("op", "")).strip() in disallowed_ops:
                return False
    return True


def load_or_generate_manifest() -> dict[str, Any]:
    csv_paths = discover_csv_tables(DATA_DIR)
    current_fingerprint = build_fingerprint(csv_paths)
    regenerate_on_mismatch = _env_true(REGENERATE_ON_FINGERPRINT_MISMATCH_ENV, default=False)

    if MANIFEST_PATH.exists():
        try:
            raw = MANIFEST_PATH.read_text(encoding="utf-8").strip()
            if raw:
                manifest = json.loads(raw)
                if _is_manifest_valid(manifest):
                    if manifest.get("fingerprint") == current_fingerprint:
                        return manifest
                    if not regenerate_on_mismatch:
                        return manifest
        except Exception as exc:
            print(f"[manifest] failed to load existing manifest: {exc}")

    manifest = _generate_manifest()
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
