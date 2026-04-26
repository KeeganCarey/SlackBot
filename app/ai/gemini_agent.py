import asyncio
import os
from collections.abc import AsyncIterator
from datetime import date

from google.genai import types

from app.ai.client import get_client
from app.ai.tools import GEMINI_TOOLS, SYSTEM_PROMPT, dispatch, format_results

# Set GEMINI_MODEL in your .env to override.
MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")


def _to_gemini_history(history: list[dict]) -> list[types.Content]:
    result = []
    for msg in history:
        role = "model" if msg["role"] == "assistant" else "user"
        result.append(types.Content(role=role, parts=[types.Part(text=msg["content"])]))
    return result


def _fc_key(name: str, call_id: str | None, args: dict) -> str:
    return f"{call_id or ''}|{name}|{tuple(sorted(args.items()))}"


class GeminiAgent:
    def __init__(self, history: list[dict] | None = None) -> None:
        self._history: list[types.Content] = _to_gemini_history(history or [])

    async def stream(self, user_message: str, on_tool_call=None) -> AsyncIterator[str]:
        self._history.append(types.Content(role="user", parts=[types.Part(text=user_message)]))

        client = get_client()
        today = date.today().isoformat()
        tool_config = types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT.format(today=today),
            tools=GEMINI_TOOLS,
        )

        print(f"[gemini] requesting {MODEL} ({len(self._history)} turns in history)")

        accumulated = ""
        for _ in range(5):
            turn_text = ""
            saw_function_call = False
            function_calls: list[dict] = []
            seen_calls: set[str] = set()

            stream = await client.aio.models.generate_content_stream(
                model=MODEL,
                contents=self._history,
                config=tool_config,
            )

            async for chunk in stream:
                for candidate in chunk.candidates or []:
                    content = getattr(candidate, "content", None)
                    if not content:
                        continue
                    for part in content.parts or []:
                        fc = getattr(part, "function_call", None)
                        if not fc:
                            continue
                        saw_function_call = True
                        name = fc.name or ""
                        args = dict(fc.args or {})
                        call_id = getattr(fc, "id", None)
                        key = _fc_key(name, call_id, args)
                        if key in seen_calls:
                            continue
                        seen_calls.add(key)
                        function_calls.append({
                            "name": name,
                            "id": call_id,
                            "args": args,
                        })

                text = chunk.text or ""
                if text:
                    turn_text += text
                    if not saw_function_call:
                        yield text

            if not function_calls:
                accumulated = turn_text
                break

            assistant_parts: list[types.Part] = []
            if turn_text:
                assistant_parts.append(types.Part(text=turn_text))
            for fc in function_calls:
                assistant_parts.append(
                    types.Part(
                        function_call=types.FunctionCall(
                            name=fc["name"],
                            id=fc["id"],
                            args=fc["args"],
                        )
                    )
                )
            self._history.append(types.Content(role="model", parts=assistant_parts))

            results = await asyncio.gather(*[
                dispatch(fc["name"], fc["args"])
                for fc in function_calls
            ])

            function_response_parts = []
            for fc, result in zip(function_calls, results):
                filters = fc["args"] or None
                print(f"[tool] {fc['name']} filters={filters} - {len(result)} rows")
                if on_tool_call:
                    await on_tool_call(fc["name"], filters, len(result))
                function_response_parts.append(
                    types.Part(
                        function_response=types.FunctionResponse(
                            name=fc["name"],
                            id=fc["id"],
                            response={"output": format_results(fc["name"], result)},
                        )
                    )
                )

            self._history.append(types.Content(role="user", parts=function_response_parts))
        else:
            accumulated = ""

        print(f"[gemini] response complete ({len(accumulated)} chars)")
        self._history.append(types.Content(role="model", parts=[types.Part(text=accumulated)]))
