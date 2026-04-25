import os
from typing import AsyncIterator
from google.genai import types

from app.ai.client import get_client

# Set GEMINI_MODEL in your .env to override. Options:
#   gemini-2.5-pro          — most capable, slower, higher cost
#   gemini-2.5-flash        — best balance of speed and quality (recommended)
#   gemini-2.5-flash-lite   — fastest, lowest cost, lighter reasoning
#   gemini-2.0-flash        — previous-gen fast model, very stable
#   gemini-2.0-flash-lite   — previous-gen lightweight model
MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

SYSTEM_PROMPT = (
    "You are a helpful assistant embedded in Slack. "
    "Keep responses conversational and well-formatted for Slack "
    "(use *bold*, _italic_, and bullet points where helpful)."
)


class Agent:
    def __init__(self) -> None:
        self._history: list[types.Content] = []

    async def stream(self, user_message: str) -> AsyncIterator[str]:
        self._history.append(types.Content(role="user", parts=[types.Part(text=user_message)]))

        client = get_client()
        accumulated = ""

        async for chunk in await client.aio.models.generate_content_stream(
            model=MODEL,
            contents=self._history,
            config=types.GenerateContentConfig(system_instruction=SYSTEM_PROMPT),
        ):
            text = chunk.text or ""
            accumulated += text
            yield text

        self._history.append(
            types.Content(role="model", parts=[types.Part(text=accumulated)])
        )
