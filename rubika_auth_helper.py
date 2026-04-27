from __future__ import annotations

import argparse
import asyncio
import os
import re
import shutil
import signal
import sys
from pathlib import Path


BACKUP_PATHS: list[tuple[Path, Path]] = []
BACKUP_DIR: Path | None = None
RESTORED = False


def session_base_path(session_name: str) -> Path:
    path = Path(session_name)
    if path.parent == Path(""):
        return Path.cwd() / path.name
    return path


def session_candidates(session_name: str) -> list[Path]:
    base_path = session_base_path(session_name)
    candidates: list[Path] = []
    for path in (
        base_path,
        base_path.with_name(f"{base_path.name}.rp"),
        base_path.with_name(f"{base_path.name}.session"),
        base_path.with_name(f"{base_path.name}.sqlite"),
    ):
        if path not in candidates:
            candidates.append(path)
    return candidates


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


def convert_farsi_digits(text: str) -> str:
    return text.translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789"))


def normalize_phone_number(phone_number: str) -> str:
    phone = convert_farsi_digits(phone_number)
    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    match = re.match(r"^(?:\+|00)?(\d{7,15})$", phone)
    if not match:
        raise ValueError("Invalid phone number.")

    normalized = match.group(1)
    if normalized.startswith("0"):
        normalized = f"98{normalized[1:]}"
    elif normalized.startswith("9") and len(normalized) == 10:
        normalized = f"98{normalized}"
    return normalized


def normalize_verification_code(code: str) -> str:
    return convert_farsi_digits(code).strip().replace(" ", "").replace("-", "")


def read_user_input(error_message: str) -> str:
    value = sys.stdin.readline()
    if not value:
        raise EOFError(error_message)
    return value.strip()


def update_status(result) -> str:
    return str(getattr(result, "status", "") or "")


def ensure_ok_status(result, action: str) -> None:
    status = update_status(result)
    if status != "OK":
        raise RuntimeError(f"{action} failed with status {status or 'unknown'}.")


async def run_auth(session_name: str, phone_number: str) -> None:
    try:
        from Crypto.PublicKey import RSA
        from Crypto.Signature import pkcs1_15
        from rubpy import Client
        from rubpy.crypto import Crypto
    except Exception as error:
        print(f"__AUTH_ERROR__:Unable to import rubpy: {error}", flush=True)
        raise SystemExit(1)

    backup_existing_session(session_name)
    cleanup_session_files(session_name)

    client = Client(name=session_name)
    try:
        await client.connect()

        normalized_phone = normalize_phone_number(phone_number)
        send_code_result = await client.send_code(phone_number=normalized_phone, send_type="SMS")
        if update_status(send_code_result) == "SendPassKey":
            hint = getattr(send_code_result, "hint_pass_key", "") or ""
            print(f"__AUTH_PASSKEY_PROMPT__:{hint}", flush=True)
            pass_key = read_user_input("Password input stream closed.")
            send_code_result = await client.send_code(
                phone_number=normalized_phone,
                pass_key=pass_key,
            )

        ensure_ok_status(send_code_result, "OTP request")
        phone_code_hash = getattr(send_code_result, "phone_code_hash", None)
        if not phone_code_hash:
            raise RuntimeError("Rubika did not return an OTP request token.")

        public_key, client.private_key = Crypto.create_keys()
        print("__AUTH_OTP_PROMPT__", flush=True)
        phone_code = normalize_verification_code(read_user_input("OTP input stream closed."))

        sign_in_result = await client.sign_in(
            phone_code=phone_code,
            phone_number=normalized_phone,
            phone_code_hash=phone_code_hash,
            public_key=public_key,
        )
        ensure_ok_status(sign_in_result, "OTP verification")

        sign_in_result.auth = Crypto.decrypt_RSA_OAEP(client.private_key, sign_in_result.auth)
        client.key = Crypto.passphrase(sign_in_result.auth)
        client.auth = sign_in_result.auth
        client.decode_auth = Crypto.decode_auth(client.auth)
        client.import_key = (
            pkcs1_15.new(RSA.import_key(client.private_key.encode()))
            if client.private_key is not None
            else None
        )
        client.session.insert(
            phone_number=sign_in_result.user.phone,
            auth=client.auth,
            guid=sign_in_result.user.user_guid,
            user_agent=client.user_agent,
            private_key=client.private_key,
        )
        await client.register_device(device_model=client.name)
        await client.stop()

        if not any(path.exists() for path in session_candidates(session_name)):
            raise RuntimeError("Authenticated session files were not created.")
    except Exception as error:
        try:
            await client.stop()
        except Exception:
            pass
        cleanup_session_files(session_name)
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
