import os

# Set BACKEND in your .env. Options: gemini (default), openai, lmstudio
BACKEND = os.environ.get("BACKEND", "gemini").lower()


def create_agent(history: list[dict] | None = None):
    if BACKEND == "openai":
        from app.ai.openai_agent import OpenAIAgent
        return OpenAIAgent(history=history)
    if BACKEND == "lmstudio":
        from app.ai.lmstudio_agent import LMStudioAgent
        return LMStudioAgent(history=history)
    from app.ai.gemini_agent import GeminiAgent
    return GeminiAgent(history=history)
