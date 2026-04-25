import asyncio
from slack_bolt.async_app import AsyncApp
from app.ai.agent import Agent

_agents: dict[str, Agent] = {}

# Slack chat.update is Tier 3: ~50 requests/minute per workspace.
# 1.2s between updates keeps us safely under that limit even with concurrent conversations.
UPDATE_INTERVAL = 1.2


def _thread_key(channel: str, thread_ts: str | None) -> str:
    return f"{channel}:{thread_ts or 'root'}"


def register_handlers(app: AsyncApp) -> None:
    @app.event("app_mention")
    async def handle_mention(event, say, client):
        await _handle_message(event, say, client)

    @app.event("message")
    async def handle_message(event, say, client):
        # Ignore bot messages to avoid reply loops
        if event.get("subtype") == "bot_message" or event.get("bot_id"):
            return

        channel_type = event.get("channel_type")
        thread_ts = event.get("thread_ts")

        # Always respond in DMs
        if channel_type == "im":
            await _handle_message(event, say, client)
            return

        # In channels, respond to thread replies only if the bot is already in that thread
        if thread_ts and _thread_key(event["channel"], thread_ts) in _agents:
            await _handle_message(event, say, client)


async def _handle_message(event: dict, say, client) -> None:
    user_text: str = event.get("text", "").strip()
    channel: str = event["channel"]
    thread_ts: str = event.get("thread_ts") or event.get("ts")

    if user_text.startswith("<@"):
        user_text = user_text.split(">", 1)[-1].strip()

    if not user_text:
        return

    placeholder = await say(text="...", thread_ts=thread_ts)
    msg_ts: str = placeholder["ts"]

    key = _thread_key(channel, thread_ts)
    if key not in _agents:
        _agents[key] = Agent()

    accumulated = ""
    last_update = asyncio.get_event_loop().time()

    try:
        async for chunk in _agents[key].stream(user_text):
            accumulated += chunk
            now = asyncio.get_event_loop().time()
            if now - last_update >= UPDATE_INTERVAL:
                await client.chat_update(channel=channel, ts=msg_ts, text=accumulated)
                last_update = now
    except Exception as exc:
        accumulated = f"Error: {exc}"

    await client.chat_update(channel=channel, ts=msg_ts, text=accumulated or "...")
