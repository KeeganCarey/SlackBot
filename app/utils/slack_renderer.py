from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return text[: limit - 1] + "…"


@dataclass
class ResponseViewModel:
    title: str | None
    summary: str
    highlights: list[str]
    fields: list[tuple[str, str]]
    detail: str
    sources: list[str]
    mode: str


def _lines(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def _extract_title(lines: list[str]) -> str | None:
    if not lines:
        return None
    first = lines[0]
    if first.startswith("#"):
        return first.lstrip("#").strip()
    if first.startswith("*") and first.endswith("*") and len(first) > 4:
        return first.strip("*").strip()
    if len(first) <= 90 and ":" not in first:
        return first
    return None


def _extract_highlights(lines: list[str]) -> list[str]:
    out: list[str] = []
    for ln in lines:
        if ln.startswith(("- ", "* ", "• ")):
            out.append(ln[2:].strip())
        elif len(ln) > 3 and ln[0].isdigit() and ln[1:3] in {". ", ") "}:
            out.append(ln[3:].strip())
    return [x for x in out if x][:8]


def _extract_fields(lines: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for ln in lines:
        if ":" not in ln:
            continue
        left, right = ln.split(":", 1)
        key = left.strip("* ").strip()
        val = right.strip()
        if not key or not val:
            continue
        lk = key.lower()
        if lk in seen:
            continue
        seen.add(lk)
        if len(key) <= 42 and len(val) <= 120:
            pairs.append((key, val))
        if len(pairs) >= 6:
            break
    return pairs


def build_view_model(text: str, *, sources: list[str] | None = None, preferred_mode: str = "auto") -> ResponseViewModel:
    lines = _lines(text)
    title = _extract_title(lines)
    highlights = _extract_highlights(lines)
    fields = _extract_fields(lines)
    summary = _clip(lines[1] if title and len(lines) > 1 else (lines[0] if lines else text.strip()), 300)

    mode = preferred_mode
    if mode == "auto":
        mode = "rich" if (len(text) >= 320 or len(highlights) >= 3 or len(fields) >= 2) else "compact"
    if mode not in {"compact", "rich"}:
        mode = "compact"

    return ResponseViewModel(
        title=title,
        summary=summary,
        highlights=highlights,
        fields=fields,
        detail=_clip(text.strip(), 2600),
        sources=sources or [],
        mode=mode,
    )


def format_for_slack(vm: ResponseViewModel) -> list[dict]:
    blocks: list[dict] = []

    if vm.mode == "compact":
        if vm.title:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _clip(f"*{vm.title}*", 300)}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _clip(vm.summary, 1600)}})
    else:
        if vm.title:
            blocks.append({"type": "header", "text": {"type": "plain_text", "text": _clip(vm.title, 150)}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _clip(vm.summary, 1800)}})

        if vm.fields:
            fields = [{"type": "mrkdwn", "text": _clip(f"*{k}*\n{v}", 350)} for k, v in vm.fields]
            blocks.append({"type": "section", "fields": fields[:10]})

        if vm.highlights:
            body = "\n".join(f"• {h}" for h in vm.highlights)
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _clip(body, 1800)}})
        elif vm.detail and vm.detail != vm.summary:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _clip(vm.detail, 1800)}})

    footer: list[str] = []
    if vm.sources:
        footer.append(f"Based on: {', '.join(vm.sources)}")
    footer.append(datetime.now(timezone.utc).strftime("Updated %Y-%m-%d %H:%M UTC"))
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": " | ".join(footer)}]})
    return blocks[:45]

