import asyncio
from dotenv import load_dotenv

load_dotenv()

from app.ai.tool_manifest import load_or_generate_manifest  # noqa: E402
from app.bot import start  # noqa: E402 — must come after load_dotenv


def main():
    load_or_generate_manifest()
    asyncio.run(start())


if __name__ == "__main__":
    main()
