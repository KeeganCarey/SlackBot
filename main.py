import asyncio
from dotenv import load_dotenv

load_dotenv()

from app.bot import start  # noqa: E402 — must come after load_dotenv


def main():
    asyncio.run(start())


if __name__ == "__main__":
    main()
