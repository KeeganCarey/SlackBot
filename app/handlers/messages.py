import asyncio
import os
from typing import Any

from slack_bolt.async_app import AsyncApp

from app.ai.agent import create_agent

_agents: dict[str, Any] = {}

# Fallback chat.update cadence (used when native chat stream APIs are unavailable).
UPDATE_INTERVAL = float(os.environ.get("SLACK_UPDATE_INTERVAL_SECONDS", "0.35"))
MIN_CHARS_PER_UPDATE = int(os.environ.get("SLACK_MIN_CHARS_PER_UPDATE", "8"))
SLACK_MAX_CHARS = 39_000  # Slack limit is 40k; leave a little headroom
SLACK_USE_NATIVE_STREAMING = os.environ.get("SLACK_USE_NATIVE_STREAMING", "true").lower() in {"1", "true", "yes"}
# Native streaming flush cadence (chat.start/append/stop path).
# This is intentionally much tighter than fallback chat.update.
NATIVE_STREAM_FLUSH_INTERVAL = float(os.environ.get("SLACK_NATIVE_STREAM_FLUSH_INTERVAL_SECONDS", "0.08"))
NATIVE_STREAM_MIN_CHARS = int(os.environ.get("SLACK_NATIVE_STREAM_MIN_CHARS", "18"))


def _env_true(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "on"}


# Keep backward compatibility with older key names.
SLACK_SHOW_TOOL_DEBUG = _env_true(
    "SLACK_SHOW_TOOL_DEBUG",
    default=_env_true("SLACK_SHOW_TOOL_CALLS", default=True),
)


def _thread_key(channel: str, thread_ts: str | None) -> str:
    return f"{channel}:{thread_ts or 'root'}"


def _slack_messages_to_history(messages: list[dict], current_ts: str) -> list[dict]:
    history = []
    for msg in messages:
        if msg.get("ts") == current_ts:
            continue
        text = msg.get("text", "").strip()
        if not text:
            continue
        if msg.get("bot_id"):
            role = "assistant"
        else:
            role = "user"
            if text.startswith("<@"):
                text = text.split(">", 1)[-1].strip()
        if text:
            history.append({"role": role, "content": text})
    return history


async def _load_history(client, channel: str, thread_ts: str, current_ts: str, channel_type: str) -> list[dict]:
    try:
        if channel_type == "im" and thread_ts == current_ts:
            result = await client.conversations_history(channel=channel, limit=50)
            messages = list(reversed(result["messages"]))
        else:
            result = await client.conversations_replies(channel=channel, ts=thread_ts)
            messages = result["messages"]
        return _slack_messages_to_history(messages, current_ts)
    except Exception as exc:
        print(f"[history] failed to load history: {exc}")
        return []


async def _call_slack_api(client, method: str, **payload):
    direct_name = method.replace(".", "_")
    direct = getattr(client, direct_name, None)
    if callable(direct):
        return await direct(**payload)
    return await client.api_call(api_method=method, http_verb="POST", json=payload)


def register_handlers(app: AsyncApp) -> None:
    @app.event("app_mention")
    async def handle_mention(event, say, client):
        print(f"[app_mention] received from user={event.get('user')} text={event.get('text')!r}")
        await _handle_message(event, say, client)

    @app.event("message")
    async def handle_message(event, say, client):
        if event.get("subtype") == "bot_message" or event.get("bot_id"):
            return

        channel_type = event.get("channel_type")
        thread_ts = event.get("thread_ts")

        print(f"[message event] channel_type={channel_type} thread_ts={thread_ts} subtype={event.get('subtype')}")

        if channel_type == "im":
            await _handle_message(event, say, client)
            return

        if thread_ts and _thread_key(event["channel"], thread_ts) in _agents:
            await _handle_message(event, say, client)


async def _handle_message(event: dict, say, client) -> None:
    user_text: str = event.get("text", "").strip()
    channel: str = event["channel"]
    channel_type: str = event.get("channel_type", "")
    current_ts: str = event.get("ts", "")
    thread_ts: str = event.get("thread_ts") or current_ts
    user_id: str | None = event.get("user")
    team_id: str | None = event.get("team")

    if user_text.startswith("<@"):
        user_text = user_text.split(">", 1)[-1].strip()

    if not user_text:
        return

    print(f"[message] channel={channel} thread={thread_ts} text={user_text!r}")

    key = _thread_key(channel, thread_ts)
    if key not in _agents:
        history = await _load_history(client, channel, thread_ts, current_ts, channel_type)
        _agents[key] = create_agent(history=history)

    accumulated = ""
    chunk_start = 0
    tool_log: list[str] = []
    data_sources: list[str] = []
    loop = asyncio.get_running_loop()

    use_native_streaming = SLACK_USE_NATIVE_STREAMING
    stream_ts: str | None = None
    msg_ts: str | None = None
    last_sent_text = ""
    last_update = 0.0
    last_native_flush = 0.0

    def _compose(answer_text: str, include_tools: bool) -> str:
        tool_text = "\n".join(tool_log) if include_tools and tool_log else ""
        if tool_text and answer_text and SLACK_SHOW_TOOL_DEBUG:
            text = f"{tool_text}\n\n{answer_text}"
        elif tool_text and SLACK_SHOW_TOOL_DEBUG:
            text = tool_text
        else:
            text = answer_text
        return text[:SLACK_MAX_CHARS]

    async def _fallback_render(text: str, *, force: bool = False) -> None:
        nonlocal msg_ts, last_sent_text, last_update
        if not text or (text == last_sent_text and not force):
            return
        if msg_ts is None:
            post = await client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text, mrkdwn=True)
            msg_ts = str(post["ts"])
        else:
            await client.chat_update(channel=channel, ts=msg_ts, text=text, blocks=[])
        last_sent_text = text
        last_update = loop.time()

    async def _render(text: str, *, force: bool = False) -> None:
        nonlocal use_native_streaming, stream_ts, msg_ts, last_sent_text, last_update, last_native_flush
        if not text or (text == last_sent_text and not force):
            return

        if not use_native_streaming:
            await _fallback_render(text, force=force)
            return

        try:
            if stream_ts is None:
                payload = {
                    "channel": channel,
                    "thread_ts": thread_ts,
                    "markdown_text": text,
                }
                # Streaming in channels requires recipient context.
                if channel_type != "im" and team_id and user_id:
                    payload["recipient_team_id"] = team_id
                    payload["recipient_user_id"] = user_id
                response = await _call_slack_api(client, "chat.startStream", **payload)
                stream_ts = str(response.get("ts"))
                msg_ts = stream_ts
                last_sent_text = text
                last_update = loop.time()
                return

            if text.startswith(last_sent_text):
                delta = text[len(last_sent_text):]
                if delta:
                    await _call_slack_api(
                        client,
                        "chat.appendStream",
                        channel=channel,
                        ts=stream_ts,
                        markdown_text=delta,
                    )
                    last_sent_text = text
                    last_update = loop.time()
                    last_native_flush = last_update
                return

            # Non-append modifications (for example removing tool logs) are handled
            # via chat.update, even when native streaming is active.
            await client.chat_update(channel=channel, ts=msg_ts, text=text, blocks=[])
            last_sent_text = text
            last_update = loop.time()
            last_native_flush = last_update
        except Exception as exc:
            print(f"[stream] native stream failed; falling back to chat.update: {exc}")
            use_native_streaming = False
            stream_ts = None
            msg_ts = None
            last_sent_text = ""
            last_update = 0.0
            await _fallback_render(text, force=True)

    async def _stop_stream_if_needed() -> None:
        nonlocal stream_ts
        if use_native_streaming and stream_ts:
            try:
                await _call_slack_api(client, "chat.stopStream", channel=channel, ts=stream_ts)
            finally:
                stream_ts = None

    async def _new_message() -> None:
        nonlocal msg_ts, chunk_start, last_sent_text, last_update, last_native_flush
        await _stop_stream_if_needed()
        msg_ts = None
        chunk_start = len(accumulated)
        last_sent_text = ""
        last_update = 0.0
        last_native_flush = 0.0

    async def on_tool_call(name: str, filters: dict | None, result_count: int) -> None:
        f = f" `{filters}`" if filters else ""
        tool_log.append(f"_{name}{f} - {result_count} rows_")
        if name not in data_sources:
            data_sources.append(name)
        await _render(_compose(accumulated[chunk_start:], include_tools=True), force=True)

    try:
        async for chunk in _agents[key].stream(user_text, on_tool_call=on_tool_call):
            accumulated += chunk
            current_answer = accumulated[chunk_start:]
            composed = _compose(current_answer, include_tools=bool(tool_log))

            if len(current_answer) >= SLACK_MAX_CHARS:
                await _render(composed, force=True)
                await _new_message()
                continue

            if use_native_streaming:
                # Avoid one network round-trip per model chunk; flush frequently
                # in tiny batches for smoother high-throughput streaming.
                if composed:
                    now = loop.time()
                    grown_chars = len(composed) - len(last_sent_text)
                    should_flush = (
                        msg_ts is None
                        or stream_ts is None
                        or grown_chars >= NATIVE_STREAM_MIN_CHARS
                        or (now - last_native_flush) >= NATIVE_STREAM_FLUSH_INTERVAL
                    )
                    if should_flush:
                        await _render(composed)
            else:
                now = loop.time()
                if composed and (msg_ts is None and stream_ts is None):
                    await _render(composed, force=True)
                elif (
                    composed
                    and now - last_update >= UPDATE_INTERVAL
                    and (len(composed) - len(last_sent_text) >= MIN_CHARS_PER_UPDATE)
                ):
                    await _render(composed)
    except Exception as exc:
        accumulated += f"\n\nError: {exc}"

    final_answer = accumulated[chunk_start:]
    final_text = final_answer[:SLACK_MAX_CHARS]
    if use_native_streaming:
        # Keep the streamed message as source-of-truth; do not do a final
        # whole-message rewrite at the end.
        final_stream_text = _compose(final_answer, include_tools=bool(tool_log))
        if final_stream_text:
            await _render(final_stream_text, force=True)
        await _stop_stream_if_needed()
    else:
        if final_text:
            await _render(final_text, force=True)
        await _stop_stream_if_needed()
        if final_text and final_text != last_sent_text and msg_ts:
            await client.chat_update(channel=channel, ts=msg_ts, text=final_text, blocks=[])

    # Keep streamed text as the canonical final output; no end-of-response
    # formatting rewrite.
