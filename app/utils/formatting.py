def thinking_message() -> str:
    return "_Thinking..._"


def error_message(exc: Exception) -> str:
    return f":warning: Something went wrong: `{exc}`"
