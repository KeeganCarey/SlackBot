import asyncio
import json
import os
from collections.abc import AsyncIterator
from datetime import date

from openai import AsyncOpenAI

from app.ai.tools import OPENAI_TOOLS, SYSTEM_PROMPT, dispatch, format_results

# Set OPENAI_MODEL in your .env to override.
MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-nano")
REASONING_EFFORT = os.environ.get("OPENAI_REASONING_EFFORT", "low")
MAX_COMPLETION_TOKENS = int(os.environ.get("OPENAI_MAX_COMPLETION_TOKENS", "0"))

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


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


class OpenAIAgent:
    def __init__(self, history: list[dict] | None = None) -> None:
        self._history: list[dict] = list(history or [])

    def _messages(self) -> list[dict]:
        system = {"role": "system", "content": SYSTEM_PROMPT.format(today=date.today().isoformat())}
        return [system] + self._history

    async def stream(self, user_message: str, on_tool_call=None) -> AsyncIterator[str]:
        self._history.append({"role": "user", "content": user_message})
        client = _get_client()

        print(f"[openai] requesting {MODEL} ({len(self._history)} turns in history)")

        accumulated = ""
        loop = asyncio.get_running_loop()
        for _ in range(5):
            turn_text = ""
            tool_call_builders: dict[int, dict[str, str]] = {}
            saw_tool_delta = False
            started_at = loop.time()
            first_text_at: float | None = None
            text_chunk_count = 0

            req: dict[str, object] = {
                "model": MODEL,
                "messages": self._messages(),
                "tools": OPENAI_TOOLS,
                "stream": True,
                "reasoning_effort": REASONING_EFFORT,
            }
            if MAX_COMPLETION_TOKENS > 0:
                req["max_completion_tokens"] = MAX_COMPLETION_TOKENS

            stream = await client.chat.completions.create(**req)

            async for chunk in stream:
                choices = chunk.choices or []
                if not choices:
                    continue
                delta = choices[0].delta

                # Collect tool call fragments first to reduce accidental user-facing
                # text for turns that resolve into tool calls.
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
                    text_chunk_count += 1
                    if first_text_at is None:
                        first_text_at = loop.time()
                    turn_text += text
                    if not saw_tool_delta:
                        yield text

            tool_calls = _finalize_tool_calls(tool_call_builders)
            first_latency_ms = int((first_text_at - started_at) * 1000) if first_text_at is not None else -1
            total_turn_ms = int((loop.time() - started_at) * 1000)
            post_first_chars_per_s = 0.0
            if first_text_at is not None and turn_text:
                post_first_duration = max(loop.time() - first_text_at, 0.001)
                post_first_chars_per_s = len(turn_text) / post_first_duration
            print(
                f"[openai] turn streamed text_chunks={text_chunk_count} "
                f"first_text_ms={first_latency_ms} total_turn_ms={total_turn_ms} "
                f"post_first_chars_per_s={post_first_chars_per_s:.1f} "
                f"tool_calls={len(tool_calls)} effort={REASONING_EFFORT}"
            )
            if not tool_calls:
                accumulated = turn_text
                break

            self._history.append({
                "role": "assistant",
                "content": turn_text,
                "tool_calls": tool_calls,
            })

            results = await asyncio.gather(*[
                dispatch(tc["function"]["name"], _safe_json_loads(tc["function"]["arguments"]))
                for tc in tool_calls
            ])

            for tc, result in zip(tool_calls, results):
                name = tc["function"]["name"]
                filters = _safe_json_loads(tc["function"]["arguments"]) or None
                print(f"[tool] {name} filters={filters} - {len(result)} rows")
                if on_tool_call:
                    await on_tool_call(name, filters, len(result))
                self._history.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": format_results(name, result),
                })
        else:
            accumulated = ""

        print(f"[openai] response complete ({len(accumulated)} chars)")
        self._history.append({"role": "assistant", "content": accumulated})
