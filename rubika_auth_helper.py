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
REQUIRES_USER_VERIFICATION = False


def session_base_path(session_name: str) -> Path:
    path = Path(session_name)
    if path.parent == Path(""):
        return Path.cwd() / path.name
    return path


def session_candidates(session_name: str) -> list[Path]:
    base_path = session_base_path(session_name)
    return [
        base_path,
        base_path.with_name(f"{base_path.name}.session"),
        base_path.with_name(f"{base_path.name}.sqlite"),
    ]


def build_temp_session_name(session_name: str) -> str:
    base_path = session_base_path(session_name)
    temp_name = f".walrus_auth_{base_path.name}_{os.getpid()}"
    return str(base_path.with_name(temp_name))


def cleanup_session_files(session_name: str) -> None:
    for path in session_candidates(session_name):
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass


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
        global REQUIRES_USER_VERIFICATION
        prompt_text = (prompt or "").strip()
        prompt_lower = prompt_text.lower()

        if "correct" in prompt_lower and ("y or n" in prompt_lower or "[y" in prompt_lower):
            print("__AUTH_CONFIRM_AUTO__", flush=True)
            return "y"

        if "code" in prompt_lower or "otp" in prompt_lower or "verify" in prompt_lower:
            REQUIRES_USER_VERIFICATION = True
            print("__AUTH_OTP_PROMPT__", flush=True)
        elif "phone" in prompt_lower or "number" in prompt_lower:
            print("__AUTH_PHONE_AUTO__", flush=True)
            return phone_number
        elif prompt_text:
            REQUIRES_USER_VERIFICATION = True
            print(f"__AUTH_PROMPT__:{prompt_text}", flush=True)

        value = sys.stdin.readline()
        if not value:
            raise EOFError("Authentication input stream closed.")

        return value.rstrip("\r\n")

    return prompt_input


async def run_auth(session_name: str, phone_number: str) -> None:
    global REQUIRES_USER_VERIFICATION
    try:
        from rubpy import Client
    except Exception as error:
        print(f"__AUTH_ERROR__:Unable to import rubpy: {error}", flush=True)
        raise SystemExit(1)

    temp_session_name = build_temp_session_name(session_name)
    cleanup_session_files(temp_session_name)
    REQUIRES_USER_VERIFICATION = False
    builtins.input = build_input_handler(phone_number)

    client = Client(name=temp_session_name)
    try:
        await client.start(phone_number=phone_number)
        await client.stop()
        if not REQUIRES_USER_VERIFICATION:
            raise RuntimeError(
                "Rubika login finished without asking for verification. The previous session may still be active."
            )
        backup_existing_session(session_name)
        cleanup_session_files(session_name)
        moved_any = False
        for source_path, target_path in zip(
            session_candidates(temp_session_name),
            session_candidates(session_name),
        ):
            if not source_path.exists():
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(target_path))
            moved_any = True
        if not moved_any:
            raise RuntimeError("Authenticated session files were not created.")
    except Exception as error:
        cleanup_session_files(temp_session_name)
        restore_existing_session()
        print(f"__AUTH_ERROR__:{error}", flush=True)
        raise SystemExit(1)

    cleanup_session_files(temp_session_name)
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
