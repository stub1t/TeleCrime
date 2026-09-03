#!/usr/bin/env python3
"""Telegram ↔ opencode command bridge.

Listens on Saved Messages (message-to-self) for messages starting with a
command prefix and executes them via `opencode run` in the repository.
The result is replied back to Saved Messages.

Usage:
    TELECRIME_* env vars must be set (source .env).
    python scripts/tg_command_bridge.py [--prefix "!oc "] [--timeout-min 60]

Design notes:
- Uses its OWN session copy (<data_dir>/telecrime_bridge.session) because the
  live session file is held by the worker container (SQLite locking).
- The pipeline notifier also writes to Saved Messages — the command prefix
  distinguishes operator commands from system notifications.
- Only one command runs at a time; further commands are acknowledged as busy.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent

LOGGER_NAME = "tg_command_bridge"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(LOGGER_NAME)

# Also log to the repo data dir so commands are inspectable next to the
# pipeline logs, not only in the systemd journal.
try:
    _data_dir = Path(
        os.environ.get("TELECRIME_DATA_DIR", str(Path(__file__).resolve().parent.parent / "data"))
    )
    _data_dir.mkdir(parents=True, exist_ok=True)
    _log_path = _data_dir / "tg_bridge.log"
    _fh = logging.FileHandler(_log_path)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(_fh)
except Exception:
    pass

_REPLY_LIMIT = 4000


def _load_env() -> None:
    """Source the repo .env if present (like the rest of the deployment)."""
    env_path = REPO_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


async def _ensure_session_copy(session_path: Path, live_path: Path) -> None:
    """Copy the live session once so we never lock the container's file."""
    if not session_path.exists() and live_path.exists():
        shutil.copy2(live_path, session_path)
        logger.info("Created bridge session copy from %s", live_path)


async def _run_opencode(
    prompt: str, timeout_seconds: int, session_id: str | None = None
) -> str:
    """Run `opencode run` non-interactively; return only the final answer.

    With --format json the streamed events are structured; we extract the text
    of the last assistant message instead of echoing raw CLI output (tool
    calls, ANSI codes, build banners) back to Telegram.

    Continuity: --continue resumes the most recent opencode session for this
    directory, so Telegram commands share that conversation's context. Using a
    fixed --session ID blocks when this TUI holds the same session open, so we
    rely on --continue instead.
    """

    # Allow service managers to provide a non-standard binary location without
    # baking a particular user's home directory into the bridge.
    opencode_bin = os.environ.get("OPENCODE_BIN") or shutil.which("opencode")
    if opencode_bin is None:
        return "opencode binary not found — install opencode for this user"
    cmd = [opencode_bin, "run", "--format", "json", "--auto"]
    if session_id:
        # Explicit session wins; otherwise resume the latest one.
        cmd += ["--session", session_id]
    else:
        cmd += ["--continue"]
    cmd.append(prompt)
    logger.info("executing: %s", " ".join(cmd[:4]) + " ...")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            proc.kill()
            try:
                out, _ = await proc.communicate()
            except Exception:
                out = b""
            return (
                f"opencode run timed out after {timeout_seconds // 60} min:\n"
                + _extract_answer(out.decode(errors="replace"))
            )
        text = out.decode(errors="replace")
    except FileNotFoundError:
        return "opencode binary not found — is opencode installed for this user?"
    answer = _extract_answer(text)
    if not answer:
        answer = text.strip()[-_REPLY_LIMIT:]
    status = "✔" if proc.returncode == 0 else f"✘ (rc={proc.returncode})"
    return f"{status} executed\n\n{answer}"


def _extract_answer(stream: str) -> str:
    """Pull the final assistant text out of the JSON event stream."""
    import re

    last_text: list[str] = []
    for line in stream.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except Exception:
            continue
        if event.get("type") != "message":
            continue
        message = event.get("message") or {}
        if message.get("role") != "assistant":
            continue
        parts = []
        for content in message.get("content") or []:
            if content.get("type") == "text" and content.get("text"):
                parts.append(content["text"])
        if parts:
            last_text = parts  # overwrite: keep the LAST assistant message
    if not last_text:
        return ""
    answer = "\n\n".join(last_text)
    # Strip leftover ANSI escapes just in case.
    answer = re.sub(r"\x1b\[[0-9;]*m", "", answer).strip()
    return answer[-_REPLY_LIMIT:]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram ↔ opencode bridge")
    parser.add_argument("--prefix", default="!oc ", help="Command prefix (default '!oc ')")
    parser.add_argument("--timeout-min", type=int, default=60, help="opencode run timeout")
    parser.add_argument("--poll", type=float, default=3.0, help="Poll interval seconds")
    parser.add_argument(
        "--session",
        default=os.environ.get("TELECRIME_BRIDGE_SESSION_ID"),
        help="opencode session ID to continue (keeps conversation context)",
    )
    args = parser.parse_args()

    _load_env()
    sys.path.insert(0, str(REPO_DIR))

    from telecrime.adapters.telegram import TelegramAdapter
    from telecrime.config import load_config

    config = load_config()
    data_dir = config.data_dir
    session_path = data_dir / "telecrime_bridge.session"
    live_path = data_dir / f"{config.telegram.session_name}.session"
    state_path = data_dir / "tg_bridge_state.json"
    await _ensure_session_copy(session_path, live_path)

    adapter = TelegramAdapter(config)
    adapter.config.telegram.session_name = session_path.stem
    # Point the adapter at the bridge copy.
    import dataclasses

    bridge_config = dataclasses.replace(
        adapter.config,
        telegram=dataclasses.replace(
            adapter.config.telegram, session_name=session_path.stem
        ),
    )
    adapter = TelegramAdapter(bridge_config)

    await adapter.connect(timeout=60)
    client = adapter.client
    assert client is not None
    # get_me outside the loop's try: a transient network/auth failure at
    # startup must not kill the bridge permanently (no supervisor restarts it).
    me = None
    for _attempt in range(3):
        try:
            me = await client.get_me()
            break
        except Exception as exc:
            logger.warning("get_me failed (attempt %d/3): %s", _attempt + 1, exc)
            await asyncio.sleep(5)
    if me is None:
        raise ConnectionError("Could not reach Telegram after 3 attempts")
    logger.info("Bridge connected as %s (id=%s)", me.first_name, me.id)

    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text())
        except Exception:
            state = {}
    last_seen_id = state.get("last_seen_id", 0)

    busy_until = 0.0
    while True:
        try:
            # Paginate: iter_messages(limit=10) drops older unread messages
            # when >10 arrive between polls and last_seen_id then jumps past
            # them — operator commands were silently lost under load.
            # limit=100 capped the TOTAL yielded messages, so >100 arrivals
            # between polls still dropped the oldest (incl. commands). Raise
            # the ceiling high enough to be effectively unbounded for a
            # human-scale Saved Messages backlog while keeping a safety cap.
            new_msgs: list = []
            async for page in client.iter_messages(me, limit=1000, min_id=last_seen_id):
                new_msgs.append(page)
                if len(new_msgs) >= 1000:
                    break
            new_msgs.reverse()
            for msg in new_msgs:
                last_seen_id = max(last_seen_id, msg.id)
                text = msg.text or ""
                if not text.startswith(args.prefix):
                    continue
                prompt = text[len(args.prefix):].strip()
                if not prompt:
                    continue
                logger.info("Command from msg %d: %s", msg.id, prompt[:80])
                if time.monotonic() < busy_until:
                    await client.send_message(
                        me,
                        f"⏳ Busy — another command is still running. "
                        f"Your command was ignored: {prompt[:100]}",
                    )
                    continue
                busy_until = time.monotonic() + args.timeout_min * 60 + 60
                try:
                    reply = await _run_opencode(
                        prompt, args.timeout_min * 60, session_id=args.session
                    )
                except Exception as exc:
                    logger.exception("opencode run crashed")
                    reply = f"✘ bridge error: {exc}"
                try:
                    await client.send_message(me, reply)
                except Exception:
                    logger.exception("Failed to send reply")
                busy_until = 0.0
            state["last_seen_id"] = last_seen_id
            try:
                state_path.write_text(json.dumps(state))
            except Exception:
                pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("poll error: %r", exc)
        await asyncio.sleep(args.poll)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
