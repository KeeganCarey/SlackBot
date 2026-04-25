import os
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

from app.handlers.messages import register_handlers

app = AsyncApp(token=os.environ["SLACK_BOT_TOKEN"])

register_handlers(app)


async def start():
    handler = AsyncSocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    await handler.start_async()
