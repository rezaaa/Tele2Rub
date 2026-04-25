from __future__ import annotations

import argparse
import asyncio
import builtins
import os
import shutil
import signal
import sys
from pathlib import Path


BACKUP_PATHS: list[tuple[Path, Path]] = []
BACKUP_DIR: Path | None = None
RESTORED = False


def session_candidates(session_name: str) -> list[Path]:
    return [
        Path(session_name),
        Path(f"{session_name}.session"),
        Path(f"{session_name}.sqlite"),
    ]


def backup_existing_session(session_name: str) -> None:
    global BACKUP_DIR

    candidates = [path for path in session_candidates(session_name) if path.exists()]
    if not candidates:
        return

    first_parent = candidates[0].parent if candidates[0].parent != Path("") else Path.cwd()
    BACKUP_DIR = first_parent / f".rubika_auth_backup_{Path(session_name).name}_{os.getpid()}"
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    for path in candidates:
        backup_path = BACKUP_DIR / path.name
        shutil.move(str(path), str(backup_path))
        BACKUP_PATHS.append((backup_path, path))


def restore_existing_session() -> None:
    global RESTORED
    if RESTORED:
        return
    RESTORED = True

    for backup_path, original_path in BACKUP_PATHS:
        try:
            if original_path.exists():
                original_path.unlink()
        except OSError:
            pass

        if backup_path.exists():
            shutil.move(str(backup_path), str(original_path))

    if BACKUP_DIR and BACKUP_DIR.exists():
        try:
            BACKUP_DIR.rmdir()
        except OSError:
            pass


def finalize_backup() -> None:
    for backup_path, _original_path in BACKUP_PATHS:
        try:
            if backup_path.exists():
                backup_path.unlink()
        except OSError:
            pass

    if BACKUP_DIR and BACKUP_DIR.exists():
        try:
            BACKUP_DIR.rmdir()
        except OSError:
            pass


def install_signal_handlers() -> None:
    def handle_abort(_signum, _frame) -> None:
        restore_existing_session()
        print("__AUTH_CANCELLED__", flush=True)
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, handle_abort)
    signal.signal(signal.SIGINT, handle_abort)


def build_input_handler(phone_number: str):
    def prompt_input(prompt: str = "") -> str:
        prompt_text = (prompt or "").strip()
        prompt_lower = prompt_text.lower()

        if "correct" in prompt_lower and ("y or n" in prompt_lower or "[y" in prompt_lower):
            print("__AUTH_CONFIRM_AUTO__", flush=True)
            return "y"

        if "code" in prompt_lower or "otp" in prompt_lower or "verify" in prompt_lower:
            print("__AUTH_OTP_PROMPT__", flush=True)
        elif "phone" in prompt_lower or "number" in prompt_lower:
            print("__AUTH_PHONE_AUTO__", flush=True)
            return phone_number
        elif prompt_text:
            print(f"__AUTH_PROMPT__:{prompt_text}", flush=True)

        value = sys.stdin.readline()
        if not value:
            raise EOFError("Authentication input stream closed.")

        return value.rstrip("\r\n")

    return prompt_input


async def run_auth(session_name: str, phone_number: str) -> None:
    try:
        from rubpy import Client
    except Exception as error:
        print(f"__AUTH_ERROR__:Unable to import rubpy: {error}", flush=True)
        raise SystemExit(1)

    builtins.input = build_input_handler(phone_number)
    backup_existing_session(session_name)

    client = Client(name=session_name)
    try:
        await client.start(phone_number=phone_number)
        await client.stop()
    except Exception as error:
        restore_existing_session()
        print(f"__AUTH_ERROR__:{error}", flush=True)
        raise SystemExit(1)

    finalize_backup()
    print("__AUTH_SUCCESS__", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("session_name")
    parser.add_argument("phone_number")
    return parser.parse_args()


if __name__ == "__main__":
    install_signal_handlers()
    args = parse_args()
    asyncio.run(run_auth(args.session_name, args.phone_number))
