"""
LM Studio backend agent using the OpenAI-compatible API.

Handles Gemma 4 native tool-call format (<|tool_call>...<tool_call|>) as a
fallback when LM Studio does not populate the standard tool_calls field.
"""

import asyncio
import json
import os
import re
from collections.abc import AsyncIterator
from datetime import date

from openai import AsyncOpenAI

from app.ai.tools import OPENAI_TOOLS, SYSTEM_PROMPT, dispatch, format_results

LMSTUDIO_URL = os.environ.get("LMSTUDIO_URL", "http://127.0.0.1:1234")
LMSTUDIO_MODEL = os.environ.get("LMSTUDIO_MODEL", "gemma4-E4B")

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(base_url=f"{LMSTUDIO_URL}/v1", api_key="lm-studio")
    return _client


_CALL_RE = re.compile(r"<\|tool_call>call:(\w+)\{(.*?)\}<tool_call\|>", re.DOTALL)
_STR_PARAM_RE = re.compile(r'(\w+):<\|"\|>(.*?)<\|"\|>')
_SCALAR_PARAM_RE = re.compile(r"(\w+):([^,{}<|]+)")


def _parse_gemma_tool_calls(content: str) -> list[tuple[str, dict]]:
    calls = []
    for call_match in _CALL_RE.finditer(content):
        name = call_match.group(1)
        params_str = call_match.group(2)
        params: dict = {}

        for m in _STR_PARAM_RE.finditer(params_str):
            params[m.group(1)] = m.group(2)

        cleaned = _STR_PARAM_RE.sub("", params_str)
        for m in _SCALAR_PARAM_RE.finditer(cleaned):
            key, val = m.group(1).strip(), m.group(2).strip()
            if key and val:
                try:
                    params[key] = int(val)
                except ValueError:
                    try:
                        params[key] = float(val)
                    except ValueError:
                        params[key] = val

        calls.append((name, params))
    return calls


def _gemma_tool_response(name: str, formatted: str) -> str:
    return f"<|tool_response>response:{name}\n{formatted}<tool_response|>"


def _safe_json_loads(raw: str) -> dict:
    try:
        return json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}


def _finalize_tool_calls(builders: dict[int, dict[str, str]]) -> list[dict]:
    tool_calls: list[dict] = []
    for idx in sorted(builders):
        entry = builders[idx]
        name = entry.get("name", "").strip()
        if not name:
            continue
        tool_calls.append({
            "id": entry.get("id") or f"call_{idx}",
            "type": "function",
            "function": {
                "name": name,
                "arguments": entry.get("arguments") or "{}",
            },
        })
    return tool_calls


class LMStudioAgent:
    def __init__(self, history: list[dict] | None = None) -> None:
        self._history: list[dict] = list(history or [])

    def _messages(self) -> list[dict]:
        system = {"role": "system", "content": SYSTEM_PROMPT.format(today=date.today().isoformat())}
        return [system] + self._history

    async def stream(self, user_message: str, on_tool_call=None) -> AsyncIterator[str]:
        self._history.append({"role": "user", "content": user_message})
        client = _get_client()

        print(f"[lmstudio] requesting {LMSTUDIO_MODEL} ({len(self._history)} turns in history)")

        accumulated = ""
        for _ in range(5):
            turn_text = ""
            tool_call_builders: dict[int, dict[str, str]] = {}
            saw_tool_delta = False

            stream = await client.chat.completions.create(
                model=LMSTUDIO_MODEL,
                messages=self._messages(),
                tools=OPENAI_TOOLS,
                stream=True,
            )

            async for chunk in stream:
                choices = chunk.choices or []
                if not choices:
                    continue
                delta = choices[0].delta

                for tc in delta.tool_calls or []:
                    saw_tool_delta = True
                    idx = tc.index if tc.index is not None else 0
                    builder = tool_call_builders.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                    if tc.id:
                        builder["id"] = tc.id
                    if tc.function:
                        if tc.function.name:
                            builder["name"] += tc.function.name
                        if tc.function.arguments:
                            builder["arguments"] += tc.function.arguments

                text = delta.content or ""
                if text:
                    turn_text += text
                    if "<|tool_call>" in turn_text:
                        saw_tool_delta = True
                    if not saw_tool_delta:
                        yield text

            tool_calls = _finalize_tool_calls(tool_call_builders)
            native_calls = _parse_gemma_tool_calls(turn_text) if not tool_calls else []

            if not tool_calls and not native_calls:
                accumulated = turn_text
                break

            if tool_calls:
                self._history.append({
                    "role": "assistant",
                    "content": turn_text,
                    "tool_calls": tool_calls,
                })
                calls_to_run = [
                    (tc["function"]["name"], _safe_json_loads(tc["function"]["arguments"]), tc["id"])
                    for tc in tool_calls
                ]
            else:
                self._history.append({"role": "assistant", "content": turn_text})
                calls_to_run = [(name, args, None) for name, args in native_calls]

            results = await asyncio.gather(*[
                dispatch(name, args) for name, args, _ in calls_to_run
            ])

            for (name, args, call_id), result in zip(calls_to_run, results):
                filters = args or None
                print(f"[tool] {name} filters={filters} - {len(result)} rows")
                if on_tool_call:
                    await on_tool_call(name, filters, len(result))

                formatted = format_results(name, result)
                if call_id is not None:
                    self._history.append({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": formatted,
                    })
                else:
                    self._history.append({
                        "role": "user",
                        "content": _gemma_tool_response(name, formatted),
                    })
        else:
            accumulated = ""

        print(f"[lmstudio] response complete ({len(accumulated)} chars)")
        self._history.append({"role": "assistant", "content": accumulated})
