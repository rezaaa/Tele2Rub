import json
import re
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


def ensure_storage_dirs() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    CANCEL_DIR.mkdir(parents=True, exist_ok=True)


def safe_filename(name: Optional[str], default: str = "file.bin") -> str:
    name = (name or default).strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = name.rstrip(". ")
    return name[:200] or default


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
) -> str:
    safe_task_id = escape(task_id or "-")
    safe_file_name = escape(truncate_middle(file_name or "file"))
    safe_stage = escape(stage)
    safe_upload_status = escape(upload_status)
    download_value = max(0, min(100, download_percent))
    upload_value = max(0, min(100, upload_percent))

    lines = [
        "<b>🎬 Tele2Rub</b>",
        f"<b>{safe_stage}</b>",
        f"{safe_upload_status}",
        "",
        f"🎞 <b>ویدیو:</b> <code>{safe_file_name}</code>",
        f"📦 <b>حجم:</b> {escape(human_size(file_size))}",
        f"🆔 <b>کد:</b> <code>{safe_task_id}</code>",
        "",
        f"⬇️ <b>دریافت:</b> <code>{progress_meter(download_value)}</code> {download_value}%",
        f"⬆️ <b>ارسال:</b> <code>{progress_meter(upload_value)}</code> {upload_value}%",
    ]

    if attempt_text:
        lines.append(f"🔁 <b>تلاش:</b> {escape(attempt_text)}")

    if queue_position is not None:
        lines.append(f"⏳ <b>صف:</b> {queue_position}")

    if note:
        lines.append(escape(note))

    lines.extend(
        [
            "",
            f"🛑 لغو: <code>/cancel {safe_task_id}</code>",
        ]
    )
    return "\n".join(lines)


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
