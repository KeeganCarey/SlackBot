"""
Dynamic tool definitions + dispatch backed by a cached tools manifest.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import date
from typing import Any

from google.genai import types as genai_types

from app.ai.tool_dispatch import ToolDispatcher
from app.ai.tool_manifest import load_or_generate_manifest

_MANIFEST = load_or_generate_manifest()
_DISPATCHER = ToolDispatcher(_MANIFEST)


def _build_data_preview() -> str:
    lines = ["Data table schemas with sample rows:"]
    for table in _MANIFEST.get("tables", []):
        cols = ", ".join(table.get("columns", []))
        lines.append(f"\n{table['name']} (columns: {cols})")
        for row in table.get("sample_rows", [])[:2]:
            lines.append("  sample: " + ", ".join(f"{k}={v}" for k, v in row.items()))
    return "\n".join(lines)


def _tool_names() -> list[str]:
    names = [t["name"] for t in _MANIFEST.get("core_tools", [])]
    names.extend(t["name"] for t in _MANIFEST.get("inferred_tools", []))
    return names


SYSTEM_PROMPT = (
    "You are a helpful assistant embedded in Slack. "
    "Keep responses conversational, concise, and well-formatted for Slack "
    "(use *bold*, _italic_, and bullet points where helpful). The format is mrkdwn, so **bold** is incorrect. Concise responses are preferred as it needs to be easy to quickly scan. The goal is efficiency.\n\n"
    f"You have access to data tools: {', '.join(_tool_names())}. "
    "Use tools whenever the user asks for data analysis, and never fabricate values. "
    "For lowest/highest/most/least/min/max questions, use aggregate/group tools first instead of query_table. "
    "Do not fetch whole tables when an aggregate can answer directly. "
    "Example: 'employee at lowest capacity' -> aggregate_table(table='employee_list', aggregate_function='min', aggregate_column='Capacity'). "
    f"Today's date is {date.today().isoformat()}.\n\n"
    + _build_data_preview()
)


def _gs(description: str) -> genai_types.Schema:
    return genai_types.Schema(type="STRING", description=description)


def _gemini_arg_schema(arg: dict[str, Any]) -> genai_types.Schema:
    arg_type = str(arg.get("type", "string")).lower()
    if arg_type == "number":
        return genai_types.Schema(type="NUMBER", description=arg["description"])
    return _gs(arg["description"])


def _tool_decl_to_gemini(tool: dict[str, Any]) -> genai_types.FunctionDeclaration:
    props = {a["name"]: _gemini_arg_schema(a) for a in tool.get("args", [])}
    return genai_types.FunctionDeclaration(
        name=tool["name"],
        description=tool.get("description", tool["name"]),
        parameters=genai_types.Schema(type="OBJECT", properties=props),
    )


def _tool_decl_to_openai(tool: dict[str, Any]) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for arg in tool.get("args", []):
        arg_type = "number" if str(arg.get("type", "string")).lower() == "number" else "string"
        properties[arg["name"]] = {"type": arg_type, "description": arg.get("description", arg["name"])}
    return {
        "type": "function",
        "function": {
            "name": tool["name"],
            "description": tool.get("description", tool["name"]),
            "parameters": {
                "type": "object",
                "properties": properties,
            },
        },
    }


_ALL_TOOLS = (_MANIFEST.get("core_tools", []) + _MANIFEST.get("inferred_tools", []))
GEMINI_TOOLS = [genai_types.Tool(function_declarations=[_tool_decl_to_gemini(t) for t in _ALL_TOOLS])]
OPENAI_TOOLS = [_tool_decl_to_openai(t) for t in _ALL_TOOLS]


async def dispatch(name: str, args: dict) -> list[dict]:
    return await _DISPATCHER.dispatch(name, args or {})


def format_results(name: str, results: list[dict]) -> str:
    if not results:
        return f"{name}: no results found."

    if len(results) == 1 and "error" in results[0]:
        return f"{name} error: {results[0]['error']}"

    if len(results) == 1 and "value" in results[0] and "aggregate_function" in results[0]:
        r = results[0]
        return (
            f"{name}: {r['aggregate_function']}({r.get('aggregate_column')}) = {r['value']} "
            f"(rows={r.get('row_count', 0)})"
        )

    lines = [f"{name}: {len(results)} result(s)"]
    for i, row in enumerate(results[:50], 1):
        fields = ", ".join(f"{k}={v}" for k, v in row.items())
        lines.append(f"  {i}. {fields}")

    if len(results) > 50:
        lines.append(f"  ... {len(results) - 50} more rows")

    # quick field cardinality hints for large result sets
    if len(results) > 10:
        keys = list(results[0].keys())
        for key in keys[:3]:
            values = [str(r.get(key, "")) for r in results]
            if values:
                top = Counter(values).most_common(1)[0]
                lines.append(f"  hint: most common {key}={top[0]} ({top[1]} rows)")
    return "\n".join(lines)
