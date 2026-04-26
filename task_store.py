from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from html import escape
from pathlib import Path
from typing import Callable, Optional


BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
QUEUE_DIR = BASE_DIR / "queue"
QUEUE_FILE = QUEUE_DIR / "tasks.jsonl"
PROCESSING_FILE = QUEUE_DIR / "processing.json"
FAILED_FILE = QUEUE_DIR / "failed.jsonl"
CANCEL_DIR = QUEUE_DIR / "cancelled"
WORKER_PID_FILE = QUEUE_DIR / "rub_worker.pid"
SETTINGS_FILE = QUEUE_DIR / "settings.json"
LRM = "\u200e"


def ensure_storage_dirs() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    CANCEL_DIR.mkdir(parents=True, exist_ok=True)


def session_file_candidates(session_name: str) -> list[Path]:
    base_path = Path(session_name)
    if not base_path.is_absolute():
        base_path = BASE_DIR / base_path

    candidates: list[Path] = []
    for path in (
        base_path,
        Path(f"{base_path}.rp"),
        Path(f"{base_path}.session"),
        Path(f"{base_path}.sqlite"),
    ):
        if path not in candidates:
            candidates.append(path)
    return candidates


def has_rubika_session(session_name: str) -> bool:
    return any(path.exists() for path in session_file_candidates(session_name))


def safe_filename(name: Optional[str], default: str = "file.bin") -> str:
    name = (name or default).strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.rstrip(". ")
    return name[:200] or default


def normalize_upload_filename(name: Optional[str], default: str = "file.bin") -> str:
    normalized = unicodedata.normalize("NFKC", (name or "").strip())
    stem, suffix = split_name(normalized or default)
    suffix = safe_filename((suffix or Path(default).suffix or ".bin").lower(), ".bin")

    cleaned_chars: list[str] = []
    for char in stem:
        category = unicodedata.category(char)
        if category[0] in {"L", "N", "M"}:
            cleaned_chars.append(char)
            continue
        if category == "Zs" or char in "._-()[]{} ":
            cleaned_chars.append(" " if category == "Zs" else char)
            continue
        cleaned_chars.append(" ")

    cleaned_stem = "".join(cleaned_chars)
    cleaned_stem = re.sub(r"[_-]+", " ", cleaned_stem)
    cleaned_stem = re.sub(r"\s*\.\s*", ".", cleaned_stem)
    cleaned_stem = re.sub(r"\s+", " ", cleaned_stem).strip(" .-_")

    fallback_stem = split_name(default)[0] or "file"
    safe_stem = safe_filename(cleaned_stem or fallback_stem, fallback_stem)
    return safe_filename(f"{safe_stem}{suffix}", f"{fallback_stem}{suffix}")


def split_name(filename: str) -> tuple[str, str]:
    path = Path(filename)
    return path.stem, path.suffix


def human_size(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 B"

    value = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]

    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024

    return f"{size_bytes} B"


def human_speed(bytes_per_second: float | int | None) -> str:
    speed = float(bytes_per_second or 0)
    if speed <= 0:
        return "0 B/s"
    return f"{human_size(int(speed))}/s"


def human_duration(seconds: float | int | None) -> str:
    total_seconds = max(0, int(seconds or 0))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def progress_bar(percent: int, width: int = 12) -> str:
    percent = max(0, min(100, percent))
    filled = round((percent / 100) * width)
    return f"[{'#' * filled}{'-' * (width - filled)}] {percent}%"


def progress_meter(percent: int, width: int = 12) -> str:
    percent = max(0, min(100, percent))
    filled = round((percent / 100) * width)
    return f"[{'#' * filled}{'-' * (width - filled)}]"


def truncate_middle(text: str, max_length: int = 42) -> str:
    text = (text or "").strip()
    if len(text) <= max_length:
        return text

    keep_left = max(8, (max_length - 3) // 2)
    keep_right = max(8, max_length - keep_left - 3)
    return f"{text[:keep_left]}...{text[-keep_right:]}"


def ltr_code(text: str) -> str:
    return f"<code>{LRM}{escape(text)}{LRM}</code>"


def build_status_text(
    *,
    task_id: str,
    file_name: str,
    file_size: int,
    stage: str,
    download_percent: int,
    upload_percent: int,
    upload_status: str,
    queue_position: int | None = None,
    note: str | None = None,
    attempt_text: str | None = None,
    speed_text: str | None = None,
    eta_text: str | None = None,
) -> str:
    safe_task_id = task_id or "-"
    safe_file_name = truncate_middle(file_name or "file")
    safe_stage = escape(stage)
    safe_upload_status = escape(upload_status)
    download_value = max(0, min(100, download_percent))
    upload_value = max(0, min(100, upload_percent))
    safe_size = human_size(file_size)

    lines = [
        "<b>⛵️ Walrus</b>",
        f"📍 <b>Status:</b> {safe_stage}",
        f"📝 <b>Note:</b> {safe_upload_status}",
        "",
        f"📄 <b>File:</b> {ltr_code(safe_file_name)}",
        f"📦 <b>Size:</b> {ltr_code(safe_size)}",
        f"🆔 <b>ID:</b> {ltr_code(safe_task_id)}",
        "",
        f"⬇️ <b>Download:</b> {ltr_code(progress_meter(download_value))} {ltr_code(f'{download_value}%')}",
        f"⬆️ <b>Upload:</b> {ltr_code(progress_meter(upload_value))} {ltr_code(f'{upload_value}%')}",
    ]

    if attempt_text:
        lines.append(f"🔁 <b>Attempt:</b> {ltr_code(attempt_text)}")

    if speed_text:
        lines.append(f"⚡ <b>Speed:</b> {ltr_code(speed_text)}")

    if eta_text:
        lines.append(f"⏱ <b>ETA:</b> {ltr_code(eta_text)}")

    if queue_position is not None:
        lines.append(f"⏳ <b>Queue:</b> {ltr_code(str(queue_position))}")

    if note:
        lines.append(escape(note))

    return "\n".join(lines)


def env_runtime_settings() -> dict:
    default_session = os.getenv("RUBIKA_SESSION", "rubika_session").strip() or "rubika_session"
    default_phone = os.getenv("RUBIKA_PHONE", "").strip()
    return {
        "rubika_session": default_session,
        "rubika_phone": default_phone,
    }


def normalize_runtime_settings(settings: Optional[dict] = None) -> dict:
    settings = settings or {}
    defaults = env_runtime_settings()

    rubika_session = (
        str(settings.get("rubika_session") or defaults["rubika_session"]).strip()
        or defaults["rubika_session"]
    )
    rubika_phone = str(settings.get("rubika_phone") or defaults["rubika_phone"]).strip()

    return {
        "rubika_session": rubika_session,
        "rubika_phone": rubika_phone,
        "rubika_target": "me",
    }


def load_runtime_settings() -> dict:
    ensure_storage_dirs()
    if not SETTINGS_FILE.exists():
        return normalize_runtime_settings()

    try:
        return normalize_runtime_settings(
            json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        )
    except Exception:
        return normalize_runtime_settings()


def save_runtime_settings(settings: dict) -> dict:
    ensure_storage_dirs()
    normalized = normalize_runtime_settings(settings)
    payload = {
        "rubika_session": normalized["rubika_session"],
        "rubika_phone": normalized["rubika_phone"],
    }
    temp_path = SETTINGS_FILE.with_suffix(".tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(SETTINGS_FILE)
    return normalized


def apply_runtime_settings(task: dict, settings: Optional[dict] = None) -> dict:
    runtime_settings = normalize_runtime_settings(settings or load_runtime_settings())
    task["rubika_session"] = runtime_settings["rubika_session"]
    task["rubika_target"] = runtime_settings["rubika_target"]
    return task


def append_task(task: dict) -> None:
    with open(QUEUE_FILE, "a", encoding="utf-8") as file:
        file.write(json.dumps(task, ensure_ascii=False) + "\n")


def read_queue_tasks() -> list[dict]:
    if not QUEUE_FILE.exists():
        return []

    tasks = []
    with open(QUEUE_FILE, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            tasks.append(json.loads(line))
    return tasks


def write_queue_tasks(tasks: list[dict]) -> None:
    temp_path = QUEUE_FILE.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as file:
        for task in tasks:
            file.write(json.dumps(task, ensure_ascii=False) + "\n")
    temp_path.replace(QUEUE_FILE)


def queue_size() -> int:
    return len(read_queue_tasks())


def find_queued_task(matcher: Callable[[dict], bool]) -> Optional[dict]:
    for task in read_queue_tasks():
        if matcher(task):
            return task
    return None


def remove_queued_task(task_id: str) -> Optional[dict]:
    tasks = read_queue_tasks()
    remaining = []
    removed_task = None

    for task in tasks:
        if removed_task is None and task.get("task_id") == task_id:
            removed_task = task
            continue
        remaining.append(task)

    if removed_task is not None:
        write_queue_tasks(remaining)

    return removed_task


def pop_first_task() -> Optional[dict]:
    tasks = read_queue_tasks()
    if not tasks:
        return None

    first_task = tasks[0]
    write_queue_tasks(tasks[1:])
    return first_task


def save_processing(task: dict) -> None:
    task["processing_updated_at"] = time.time()
    temp_path = PROCESSING_FILE.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(task, file, ensure_ascii=False, indent=2)
    temp_path.replace(PROCESSING_FILE)


def load_processing() -> Optional[dict]:
    if not PROCESSING_FILE.exists():
        return None

    with open(PROCESSING_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def clear_processing() -> None:
    if PROCESSING_FILE.exists():
        PROCESSING_FILE.unlink()


def save_worker_pid(pid: int) -> None:
    WORKER_PID_FILE.write_text(str(pid), encoding="utf-8")


def load_worker_pid() -> Optional[int]:
    if not WORKER_PID_FILE.exists():
        return None

    text = WORKER_PID_FILE.read_text(encoding="utf-8").strip()
    if not text:
        return None

    try:
        return int(text)
    except ValueError:
        return None


def clear_worker_pid() -> None:
    if WORKER_PID_FILE.exists():
        WORKER_PID_FILE.unlink()


def append_failed(task: dict, error: str) -> None:
    payload = {"task": task, "error": error}
    with open(FAILED_FILE, "a", encoding="utf-8") as file:
        file.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_failed_entries() -> list[dict]:
    if not FAILED_FILE.exists():
        return []

    entries = []
    with open(FAILED_FILE, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def find_failed_entry(task_id: str) -> Optional[dict]:
    for entry in reversed(read_failed_entries()):
        task = entry.get("task") or {}
        if task.get("task_id") == task_id:
            return entry
    return None


def cancel_path(task_id: str) -> Path:
    return CANCEL_DIR / f"{task_id}.cancel"


def mark_cancelled(task_id: str) -> None:
    cancel_path(task_id).write_text("cancelled", encoding="utf-8")


def is_cancelled(task_id: str) -> bool:
    return cancel_path(task_id).exists()


def clear_cancelled(task_id: str) -> None:
    path = cancel_path(task_id)
    if path.exists():
        path.unlink()


def cleanup_local_file(path_like: str) -> None:
    path = Path(path_like)
    if path.exists():
        path.unlink()
