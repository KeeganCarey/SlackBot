import csv
import json
import os
import asyncio
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from rapidfuzz import fuzz, process
from slack_sdk.web.async_client import AsyncWebClient

from app.integrations.google_calendar import create_calendar_event


def _parse_number(value: Any) -> float | None:
    text = str(value or "").strip().replace("$", "").replace(",", "").replace("%", "")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    fmts = ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d")
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _json_obj(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_csv(path: str) -> list[dict[str, str]]:
    with open(Path(path), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _matches(row: dict[str, str], filters: dict[str, Any]) -> bool:
    for key, val in filters.items():
        row_val = str(row.get(key, "")).lower()
        q = str(val).lower()
        if q not in row_val:
            return False
    return True


def _apply_filters(rows: list[dict[str, str]], filters: dict[str, Any]) -> list[dict[str, str]]:
    if not filters:
        return rows
    return [r for r in rows if _matches(r, filters)]


def _sort_rows(rows: list[dict[str, Any]], sort_by: str, order: str) -> list[dict[str, Any]]:
    reverse = (order or "asc").lower() == "desc"
    if not sort_by:
        return rows

    def _key(row: dict[str, Any]):
        n = _parse_number(row.get(sort_by))
        if n is not None:
            return (0, n)
        d = _parse_date(row.get(sort_by))
        if d is not None:
            return (1, d.timestamp())
        return (2, str(row.get(sort_by, "")).lower())

    return sorted(rows, key=_key, reverse=reverse)


def _aggregate(rows: list[dict[str, Any]], fn: str, col: str | None) -> Any:
    f = (fn or "").lower()
    if f == "count":
        return len(rows)
    if not col:
        return None
    values = [r.get(col) for r in rows]
    if f == "distinct_count":
        return len(set(str(v) for v in values if str(v).strip() != ""))
    if f == "mode":
        present = [str(v) for v in values if str(v).strip() != ""]
        if not present:
            return None
        return Counter(present).most_common(1)[0][0]

    nums = [_parse_number(v) for v in values]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None
    if f == "min":
        return min(nums)
    if f == "max":
        return max(nums)
    if f == "sum":
        return sum(nums)
    if f == "avg":
        return sum(nums) / len(nums)
    if f == "median":
        nums_sorted = sorted(nums)
        n = len(nums_sorted)
        mid = n // 2
        if n % 2 == 1:
            return nums_sorted[mid]
        return (nums_sorted[mid - 1] + nums_sorted[mid]) / 2
    return None


class ToolDispatcher:
    def __init__(self, manifest: dict[str, Any]) -> None:
        self.manifest = manifest
        self.tables_by_name = {t["name"]: t for t in manifest.get("tables", [])}
        self.inferred_by_name = {t["name"]: t for t in manifest.get("inferred_tools", [])}
        token = os.environ.get("SLACK_BOT_TOKEN")
        self._slack_client = AsyncWebClient(token=token) if token else None
        self._users_cache: list[dict[str, Any]] | None = None

    def _validate_table(self, name: str) -> tuple[bool, str | None]:
        if name not in self.tables_by_name:
            return False, f"Unknown table '{name}'. Use list_tables to inspect available tables."
        return True, None

    async def dispatch(self, name: str, args: dict[str, Any]) -> list[dict[str, Any]]:
        if name == "list_tables":
            return [{
                "table": t["name"],
                "columns": ", ".join(t.get("columns", [])),
                "row_count_estimate": t.get("row_count_estimate", 0),
            } for t in self.manifest.get("tables", [])]
        if name == "describe_table":
            return self._describe_table(args)
        if name == "query_table":
            return self._query_table(args)
        if name == "aggregate_table":
            return self._aggregate_table(args)
        if name == "group_aggregate_table":
            return self._group_aggregate(args)
        if name == "message_workspace_user":
            return await self._message_workspace_user(args)
        if name == "create_calendar_event":
            return await self._create_calendar_event(args)
        if name in self.inferred_by_name:
            return self._run_inferred(name, args)
        return [{"error": f"Unknown tool: {name}"}]

    async def _workspace_users(self) -> list[dict[str, Any]]:
        if self._users_cache is not None:
            return self._users_cache
        if self._slack_client is None:
            return []

        users: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            resp = await self._slack_client.users_list(limit=200, cursor=cursor)
            members = resp.get("members", [])
            for m in members:
                if m.get("deleted") or m.get("is_bot"):
                    continue
                users.append(m)
            cursor = ((resp.get("response_metadata") or {}).get("next_cursor") or "").strip() or None
            if not cursor:
                break
        self._users_cache = users
        return users

    async def _message_workspace_user(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        target_name = str(args.get("name", "")).strip()
        message = str(args.get("message", "")).strip()
        min_score = int(args.get("min_match_score", 70) or 70)
        min_score = max(0, min(min_score, 100))

        if not target_name:
            return [{"error": "Missing required argument: name"}]
        if not message:
            return [{"error": "Missing required argument: message"}]
        if self._slack_client is None:
            return [{"error": "Slack client unavailable (SLACK_BOT_TOKEN not set)."}]

        users = await self._workspace_users()
        if not users:
            return [{"error": "Could not load workspace users."}]

        name_to_user: dict[str, dict[str, Any]] = {}
        choices: list[str] = []
        for user in users:
            profile = user.get("profile") or {}
            candidates = {
                str(user.get("name", "")).strip(),
                str(user.get("real_name", "")).strip(),
                str(user.get("real_name_normalized", "")).strip(),
                str(profile.get("display_name", "")).strip(),
                str(profile.get("display_name_normalized", "")).strip(),
                str(profile.get("real_name", "")).strip(),
                str(profile.get("real_name_normalized", "")).strip(),
            }
            for candidate in candidates:
                if not candidate:
                    continue
                key = candidate.lower()
                if key not in name_to_user:
                    name_to_user[key] = user
                    choices.append(key)

        if not choices:
            return [{"error": "No eligible users found to message."}]

        match = process.extractOne(target_name.lower(), choices, scorer=fuzz.WRatio, score_cutoff=min_score)
        if not match:
            suggestions = [m[0] for m in process.extract(target_name.lower(), choices, scorer=fuzz.WRatio, limit=3)]
            return [{
                "error": f"No user matched '{target_name}' with score >= {min_score}.",
                "suggestions": suggestions,
            }]

        matched_key, score, _ = match
        user = name_to_user[matched_key]
        user_id = user.get("id")
        if not user_id:
            return [{"error": "Matched user has no Slack user ID."}]

        convo = await self._slack_client.conversations_open(users=user_id)
        channel_id = ((convo.get("channel") or {}).get("id") or "")
        if not channel_id:
            return [{"error": f"Failed to open DM channel for user '{target_name}'."}]

        sent = await self._slack_client.chat_postMessage(channel=channel_id, text=message)
        return [{
            "ok": bool(sent.get("ok", True)),
            "matched_name": matched_key,
            "match_score": score,
            "user_id": user_id,
            "channel_id": channel_id,
            "message_ts": sent.get("ts"),
        }]

    async def _create_calendar_event(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        summary = str(args.get("summary", "")).strip()
        start_datetime = str(args.get("start_datetime", "")).strip()
        end_datetime = str(args.get("end_datetime", "")).strip()
        timezone = str(args.get("timezone", "")).strip() or os.environ.get("GOOGLE_CALENDAR_DEFAULT_TIMEZONE", "")
        description = str(args.get("description", "")).strip() or None
        location = str(args.get("location", "")).strip() or None
        attendees_csv = str(args.get("attendees_csv", "")).strip()
        calendar_id = str(args.get("calendar_id", "")).strip() or os.environ.get("GOOGLE_CALENDAR_DEFAULT_ID", "primary")
        send_updates = str(args.get("send_updates", "")).strip() or "none"

        if not summary:
            return [{"error": "Missing required argument: summary"}]
        if not start_datetime:
            return [{"error": "Missing required argument: start_datetime"}]
        if not end_datetime:
            return [{"error": "Missing required argument: end_datetime"}]
        if not timezone:
            return [{"error": "Missing required argument: timezone (or set GOOGLE_CALENDAR_DEFAULT_TIMEZONE)."}]

        attendees = [a.strip() for a in attendees_csv.split(",") if a.strip()] if attendees_csv else []

        try:
            loop = asyncio.get_running_loop()
            event = await loop.run_in_executor(
                None,
                lambda: create_calendar_event(
                    summary=summary,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    timezone=timezone,
                    description=description,
                    location=location,
                    attendees=attendees,
                    calendar_id=calendar_id,
                    send_updates=send_updates,
                ),
            )
            return [event]
        except Exception as exc:
            return [{"error": f"Failed to create calendar event: {exc}"}]

    def _describe_table(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        table = str(args.get("table", "")).strip()
        ok, err = self._validate_table(table)
        if not ok:
            return [{"error": err}]
        t = self.tables_by_name[table]
        return [{
            "table": t["name"],
            "columns": t["columns"],
            "column_types": t.get("column_types", {}),
            "numeric_columns": t.get("numeric_columns", []),
            "date_columns": t.get("date_columns", []),
            "sample_rows": t.get("sample_rows", []),
        }]

    def _query_table(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        table = str(args.get("table", "")).strip()
        ok, err = self._validate_table(table)
        if not ok:
            return [{"error": err}]
        t = self.tables_by_name[table]
        rows = _load_csv(t["path"])
        filters = _json_obj(args.get("filters_json"))
        rows = _apply_filters(rows, filters)

        sort_by = str(args.get("sort_by", "")).strip()
        if sort_by:
            rows = _sort_rows(rows, sort_by, str(args.get("sort_order", "asc")))

        select_csv = str(args.get("select_columns_csv", "")).strip()
        if select_csv:
            cols = [c.strip() for c in select_csv.split(",") if c.strip() in t["columns"]]
            if cols:
                rows = [{k: r.get(k, "") for k in cols} for r in rows]

        limit = int(args.get("limit", 25) or 25)
        limit = max(1, min(limit, 200))
        return rows[:limit]

    def _aggregate_table(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        table = str(args.get("table", "")).strip()
        ok, err = self._validate_table(table)
        if not ok:
            return [{"error": err}]
        t = self.tables_by_name[table]
        rows = _apply_filters(_load_csv(t["path"]), _json_obj(args.get("filters_json")))
        fn = str(args.get("aggregate_function", "")).strip().lower()
        col = str(args.get("aggregate_column", "")).strip() or None
        value = _aggregate(rows, fn, col)
        if value is None and fn != "count":
            return [{"error": f"Could not compute {fn} on column '{col}'"}]
        return [{
            "table": table,
            "aggregate_function": fn,
            "aggregate_column": col,
            "value": value,
            "row_count": len(rows),
        }]

    def _group_aggregate(self, args: dict[str, Any]) -> list[dict[str, Any]]:
        table = str(args.get("table", "")).strip()
        ok, err = self._validate_table(table)
        if not ok:
            return [{"error": err}]
        t = self.tables_by_name[table]
        group_by = str(args.get("group_by", "")).strip()
        if group_by not in t["columns"]:
            return [{"error": f"Unknown group_by column '{group_by}'"}]
        fn = str(args.get("aggregate_function", "count")).strip().lower()
        col = str(args.get("aggregate_column", "")).strip() or None

        rows = _apply_filters(_load_csv(t["path"]), _json_obj(args.get("filters_json")))
        groups: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            groups[str(r.get(group_by, ""))].append(r)

        out = []
        for key, vals in groups.items():
            agg = _aggregate(vals, fn, col)
            out.append({
                "group": key,
                "aggregate_value": agg,
                "row_count": len(vals),
            })
        out = _sort_rows(out, "aggregate_value", "desc")
        limit = int(args.get("limit", 25) or 25)
        limit = max(1, min(limit, 200))
        return out[:limit]

    def _run_inferred(self, name: str, args: dict[str, Any]) -> list[dict[str, Any]]:
        spec = self.inferred_by_name[name]
        tables: dict[str, list[dict[str, Any]]] = {}
        current_key = ""
        for step in spec.get("plan", {}).get("steps", []):
            op = step.get("op")
            if op == "load_table":
                table = step.get("table")
                if table not in self.tables_by_name:
                    return [{"error": f"Inferred tool '{name}' references unknown table '{table}'"}]
                alias = step.get("as") or table
                tables[alias] = _load_csv(self.tables_by_name[table]["path"])
                current_key = alias
            elif op == "apply_arg_filters":
                alias = step.get("table_alias") or current_key
                rows = tables.get(alias, [])
                for mapping in step.get("mappings", []):
                    arg_name = mapping.get("arg")
                    col = mapping.get("column")
                    if not arg_name or not col:
                        continue
                    val = args.get(arg_name)
                    if val in (None, ""):
                        continue
                    comparator = mapping.get("comparator", "contains")
                    if comparator == "contains":
                        rows = [r for r in rows if str(val).lower() in str(r.get(col, "")).lower()]
                    elif comparator in {"gte", "lte"}:
                        qd = _parse_date(val)
                        if qd is None:
                            continue
                        next_rows = []
                        for r in rows:
                            rd = _parse_date(r.get(col))
                            if rd is None:
                                continue
                            if comparator == "gte" and rd >= qd:
                                next_rows.append(r)
                            if comparator == "lte" and rd <= qd:
                                next_rows.append(r)
                        rows = next_rows
                tables[alias] = rows
            elif op == "group_aggregate":
                alias = step.get("table_alias") or current_key
                rows = tables.get(alias, [])
                group_by = step.get("group_by")
                fn = step.get("aggregate_function", "count")
                col = step.get("aggregate_column")
                groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for r in rows:
                    groups[str(r.get(group_by, ""))].append(r)
                out = []
                for key, vals in groups.items():
                    out.append({"group": key, "aggregate_value": _aggregate(vals, fn, col), "row_count": len(vals)})
                out_alias = step.get("as") or alias
                tables[out_alias] = out
                current_key = out_alias
            elif op == "sort":
                alias = step.get("table_alias") or current_key
                tables[alias] = _sort_rows(tables.get(alias, []), step.get("by", ""), step.get("order", "asc"))
            elif op == "top_n":
                alias = step.get("table_alias") or current_key
                n = int(args.get(step.get("arg", ""), step.get("default", 25)) or 25)
                n = max(1, min(n, int(step.get("max", 200))))
                tables[alias] = tables.get(alias, [])[:n]
            elif op == "filter_result":
                alias = step.get("table_alias") or current_key
                rows = tables.get(alias, [])
                col = step.get("column")
                comparator = step.get("comparator", "gt")
                threshold = args.get(step.get("arg", ""), step.get("default"))
                tnum = _parse_number(threshold)
                if tnum is None:
                    continue
                next_rows = []
                for r in rows:
                    val = _parse_number(r.get(col))
                    if val is None:
                        continue
                    if comparator == "gt" and val > tnum:
                        next_rows.append(r)
                    elif comparator == "gte" and val >= tnum:
                        next_rows.append(r)
                tables[alias] = next_rows
            elif op == "join":
                left = tables.get(step.get("left"), [])
                right = tables.get(step.get("right"), [])
                left_on = step.get("left_on")
                right_on = step.get("right_on")
                index: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for r in right:
                    index[str(r.get(right_on, ""))].append(r)
                joined = []
                for l in left:
                    key = str(l.get(left_on, ""))
                    for r in index.get(key, []):
                        out = dict(l)
                        for rk, rv in r.items():
                            if rk in out:
                                out[f"{rk}_right"] = rv
                            else:
                                out[rk] = rv
                        joined.append(out)
                alias = step.get("as") or "joined"
                tables[alias] = joined
                current_key = alias
            elif op == "project_columns":
                alias = step.get("table_alias") or current_key
                cols = step.get("columns", [])
                tables[alias] = [{c: row.get(c) for c in cols} for row in tables.get(alias, [])]
            else:
                return [{"error": f"Inferred tool step '{op}' is not allowed."}]

        result = tables.get(current_key, [])
        if not isinstance(result, list):
            return [{"error": f"Inferred tool '{name}' produced an invalid result."}]
        return result[:200]
