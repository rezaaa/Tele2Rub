from __future__ import annotations

import asyncio
import os
import re
import signal
import time
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from pyrogram import Client, enums, filters
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
import requests

from task_store import (
    DOWNLOAD_DIR,
    append_task,
    build_status_text,
    cleanup_local_file,
    ensure_storage_dirs,
    find_failed_entry,
    human_size,
    human_duration,
    human_speed,
    find_queued_task,
    is_cancelled,
    load_processing,
    load_worker_pid,
    ltr_code,
    mark_cancelled,
    normalize_upload_filename,
    queue_size,
    read_failed_entries,
    read_queue_tasks,
    remove_queued_task,
    safe_filename,
    split_name,
)


load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_TELEGRAM_ID = int(os.getenv("OWNER_TELEGRAM_ID", "0"))

ensure_storage_dirs()

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise RuntimeError("Please set API_ID, API_HASH and BOT_TOKEN in .env")

app = Client(
    "walrus",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

ACTIVE_DOWNLOADS: dict[str, dict] = {}
COMMANDS_READY = False

BTN_STATUS = "📊 Status"
BTN_TRANSFERS = "📋 Transfers"
BTN_CLEANUP = "🧹 Cleanup"
BTN_CANCEL = "🛑 Cancel"
MENU_BUTTONS = {BTN_STATUS, BTN_TRANSFERS, BTN_CLEANUP, BTN_CANCEL}
DIRECT_VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".webm",
    ".flv",
    ".m4v",
}
URL_PATTERN = re.compile(r"(?P<url>(?:https?|file)://\S+)", re.IGNORECASE)
DIRECT_DOWNLOAD_MAX_RETRIES = 5
DIRECT_DOWNLOAD_RETRY_DELAY = 3

MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_STATUS), KeyboardButton(BTN_TRANSFERS)],
        [KeyboardButton(BTN_CLEANUP), KeyboardButton(BTN_CANCEL)],
    ],
    resize_keyboard=True,
)

BOT_COMMANDS = [
    BotCommand("start", "Open the main menu"),
    BotCommand("status", "Show queue and storage status"),
    BotCommand("transfers", "List active and queued transfers"),
    BotCommand("retry", "Retry a failed transfer"),
    BotCommand("retry_all", "Retry all failed transfers"),
    BotCommand("cleanup", "Clean safe download leftovers"),
    BotCommand("cancel", "Cancel a transfer"),
]
MENU_BUTTON_FILTER = filters.create(
    lambda _filter, _client, message: (message.text or "").strip() in MENU_BUTTONS
)


async def ensure_bot_commands(client: Client) -> None:
    global COMMANDS_READY
    if COMMANDS_READY:
        return

    try:
        await client.set_bot_commands(BOT_COMMANDS)
        COMMANDS_READY = True
    except Exception:
        pass


def is_owner(user_id: int | None) -> bool:
    if not OWNER_TELEGRAM_ID:
        return True
    return bool(user_id and user_id == OWNER_TELEGRAM_ID)


async def ensure_authorized_message(message: Message) -> bool:
    if is_owner(getattr(message.from_user, "id", None)):
        return True
    return False


async def ensure_authorized_callback(callback_query: CallbackQuery) -> bool:
    if is_owner(getattr(callback_query.from_user, "id", None)):
        return True

    try:
        await callback_query.answer("Access denied.", show_alert=True)
    except Exception:
        pass
    return False


def build_menu_text() -> str:
    return "\n".join(
        [
            "<b>🎬 Walrus</b>",
            "📤 <b>Send a video or direct video link</b> and I will upload it to Rubika Saved Messages.",
        ]
    )


def main_action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 Status", callback_data="menu:status"),
                InlineKeyboardButton("📋 Transfers", callback_data="menu:transfers"),
            ],
            [
                InlineKeyboardButton("🧹 Cleanup", callback_data="menu:cleanup"),
                InlineKeyboardButton("🛑 Cancel", callback_data="menu:cancel"),
            ],
        ]
    )


def status_summary_keyboard(has_cleanup: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("📋 Details", callback_data="menu:transfers")]]
    if has_cleanup:
        rows.append([InlineKeyboardButton("🧹 Confirm Cleanup", callback_data="cleanup:confirm")])
    return InlineKeyboardMarkup(rows)


def cleanup_keyboard(has_candidates: bool) -> InlineKeyboardMarkup | None:
    if not has_candidates:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Confirm cleanup", callback_data="cleanup:confirm")]]
    )


async def send_menu(message: Message) -> None:
    await message.reply_text(
        build_menu_text(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=main_action_keyboard(),
    )


def iter_download_files() -> list[Path]:
    if not DOWNLOAD_DIR.exists():
        return []
    return sorted(path for path in DOWNLOAD_DIR.iterdir() if path.is_file())


def sum_file_sizes(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def protected_download_paths() -> set[Path]:
    protected: set[Path] = set()

    for active in ACTIVE_DOWNLOADS.values():
        path = active.get("download_path")
        if path:
            protected.add(Path(path).resolve())

    for task in read_queue_tasks():
        path = task.get("path")
        if path:
            protected.add(Path(path).resolve())

    processing_task = load_processing()
    if processing_task and processing_task.get("path"):
        protected.add(Path(processing_task["path"]).resolve())

    return protected


def cleanup_candidates() -> list[Path]:
    protected = protected_download_paths()
    candidates = []

    for path in iter_download_files():
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if resolved not in protected:
            candidates.append(path)

    return candidates


def compact_task_card(prefix: str, task: dict, status: str = "") -> str:
    task_id = task.get("task_id", "-")
    file_name = Path(task.get("file_name") or task.get("path") or "video").name
    stem, suffix = split_name(file_name)
    display_name = safe_filename(f"{stem[:30]}{suffix}", "video")
    size = human_size(int(task.get("file_size", 0) or 0))
    lines = [
        f"{prefix} <b>ID:</b> {ltr_code(task_id)}",
        f"🎞 <b>Video:</b> {ltr_code(display_name)}",
        f"📦 <b>Size:</b> {ltr_code(size)}",
    ]

    if status:
        lines.append(status)

    return "\n".join(lines)


def compact_button_label(prefix: str, task: dict) -> str:
    task_id = task.get("task_id", "-")
    file_name = Path(task.get("file_name") or task.get("path") or "video").name
    stem, suffix = split_name(file_name)
    display_name = safe_filename(f"{stem[:18]}{suffix}", "video")
    return f"{prefix} {display_name} - {task_id}"


def cancel_requested(task: dict | None) -> bool:
    if not task:
        return False

    task_id = task.get("task_id", "")
    return bool(task.get("cancelled")) or bool(task_id and is_cancelled(task_id))


def visible_active_downloads() -> list[dict]:
    return [task for task in ACTIVE_DOWNLOADS.values() if not cancel_requested(task)]


def visible_processing_task() -> dict | None:
    processing_task = load_processing()
    if cancel_requested(processing_task):
        return None
    return processing_task


def cancellable_tasks() -> list[tuple[str, dict]]:
    tasks: list[tuple[str, dict]] = []

    for active in visible_active_downloads():
        tasks.append(("⬇️", active))

    processing_task = visible_processing_task()
    if processing_task:
        tasks.append(("🚀", processing_task))

    for task in read_queue_tasks():
        tasks.append(("⏳", task))

    return tasks


def retryable_failed_tasks() -> list[dict]:
    tasks = []
    seen_task_ids: set[str] = set()

    for entry in reversed(read_failed_entries()):
        task = entry.get("task") or {}
        task_id = task.get("task_id")
        if not task_id or task_id in seen_task_ids:
            continue
        path = Path(task.get("path", ""))
        if path.exists():
            tasks.append(task)
            seen_task_ids.add(task_id)

    return tasks


def build_cancel_keyboard() -> InlineKeyboardMarkup | None:
    rows = []

    for prefix, task in cancellable_tasks()[:12]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label(prefix, task),
                    callback_data=f"cancel:{task_id}",
                )
            ]
        )

    if not rows:
        return None

    return InlineKeyboardMarkup(rows)


def transfers_action_keyboard() -> InlineKeyboardMarkup:
    rows = []
    retryable_failed = retryable_failed_tasks()

    for _prefix, task in cancellable_tasks()[:8]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label("🛑 Cancel", task),
                    callback_data=f"cancel:{task_id}",
                )
            ]
        )

    if retryable_failed:
        rows.append(
            [
                InlineKeyboardButton(
                    "🔁 Retry All Failed",
                    callback_data="retry_all",
                )
            ]
        )

    for task in retryable_failed[:8]:
        task_id = task.get("task_id")
        if not task_id:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    compact_button_label("🔁 Retry", task),
                    callback_data=f"retry:{task_id}",
                )
            ]
        )

    rows.extend(
        [
            [
                InlineKeyboardButton("📊 Status", callback_data="menu:status"),
                InlineKeyboardButton("🧹 Cleanup", callback_data="menu:cleanup"),
            ],
            [InlineKeyboardButton("🛑 Cancel List", callback_data="menu:cancel")],
        ]
    )

    return InlineKeyboardMarkup(rows)


def status_action_keyboard(task_id: str, action: str = "cancel") -> InlineKeyboardMarkup:
    if action == "retry":
        button = InlineKeyboardButton("🔁 Retry", callback_data=f"retry:{task_id}")
    else:
        button = InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel:{task_id}")

    return InlineKeyboardMarkup([[button]])


async def send_cancel_picker(message: Message) -> None:
    keyboard = build_cancel_keyboard()
    if not keyboard:
        await message.reply_text(
            "🛑 There are no active transfers to cancel.",
            reply_markup=MENU_KEYBOARD,
        )
        return

    await message.reply_text(
        "\n".join(
            [
                "<b>🛑 Cancel Transfer</b>",
                "",
                "Choose one transfer:",
            ]
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=keyboard,
    )


async def send_status_summary(message: Message) -> None:
    await message.reply_text(
        build_status_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_summary_keyboard(bool(cleanup_candidates())),
    )


async def send_transfers_summary(message: Message) -> None:
    await message.reply_text(
        build_transfers_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=transfers_action_keyboard(),
    )


async def send_cleanup_preview(message: Message) -> None:
    candidates = cleanup_candidates()
    await message.reply_text(
        build_cleanup_preview(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=cleanup_keyboard(bool(candidates)),
    )


async def run_cleanup(message: Message) -> None:
    candidates = cleanup_candidates()
    total_size = sum_file_sizes(candidates)
    removed_count = 0

    for path in candidates:
        try:
            path.unlink()
            removed_count += 1
        except OSError:
            pass

    await message.reply_text(
        "\n".join(
            [
                "<b>🧹 Cleanup Complete</b>",
                "",
                f"Removed files: <b>{removed_count}</b>",
                f"Freed space: <b>{human_size(total_size)}</b>",
            ]
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=main_action_keyboard(),
    )


def build_status_summary() -> str:
    queued = read_queue_tasks()
    active_downloads = visible_active_downloads()
    processing = visible_processing_task()
    failed_entries = read_failed_entries()
    files = iter_download_files()
    candidates = cleanup_candidates()

    lines = [
        "<b>📊 Walrus Status</b>",
        "",
        f"⬇️ <b>Active Downloads:</b> {ltr_code(str(len(active_downloads)))}",
        f"🚀 <b>Active Uploads:</b> {ltr_code(str(1 if processing else 0))}",
        f"⏳ <b>Queued:</b> {ltr_code(str(len(queued)))}",
        f"❌ <b>Failed:</b> {ltr_code(str(len(failed_entries)))}",
        f"📁 <b>Downloaded Files:</b> {ltr_code(f'{len(files)} / {human_size(sum_file_sizes(files))}')}",
        f"🧹 <b>Cleanup Available:</b> {ltr_code(f'{len(candidates)} / {human_size(sum_file_sizes(candidates))}')}",
    ]

    return "\n".join(lines)


def build_transfers_summary() -> str:
    queued = read_queue_tasks()
    active_downloads = visible_active_downloads()
    processing = visible_processing_task()
    failed_entries = read_failed_entries()
    lines = ["<b>📋 Transfers</b>", ""]

    if active_downloads:
        lines.append("<b>⬇️ Downloading</b>")
        for active in active_downloads[:5]:
            download_percent = active.get("download_percent", 0)
            status = f"⬇️ <b>Download:</b> {ltr_code(f'{download_percent}%')}"
            lines.append(compact_task_card("•", active, status))
            lines.append("")
        lines.append("")

    if processing:
        lines.append("<b>🚀 Uploading</b>")
        upload_percent = processing.get("upload_percent", 0)
        status = f"⬆️ <b>Upload:</b> {ltr_code(f'{upload_percent}%')}"
        if processing.get("attempt_text"):
            status += f"\n🔁 <b>Attempt:</b> {ltr_code(processing['attempt_text'])}"
        lines.append(compact_task_card("•", processing, status))
        lines.append("")

    if queued:
        lines.append("<b>⏳ Upload Queue</b>")
        for index, task in enumerate(queued[:8], start=1):
            lines.append(compact_task_card(f"{index}.", task))
            lines.append("")
        if len(queued) > 8:
            lines.append(f"... and {len(queued) - 8} more")
        lines.append("")

    retryable_failed = retryable_failed_tasks()

    if retryable_failed:
        lines.append("<b>❌ Retryable Failed Transfers</b>")
        for task in retryable_failed[:5]:
            lines.append(compact_task_card("•", task, "Tap a Retry button below."))
            lines.append("")
        if len(retryable_failed) > 5:
            lines.append(f"... and {len(retryable_failed) - 5} more")
        lines.append("")

    if len(lines) == 2:
        lines.append("No active transfers right now.")

    return "\n".join(lines)


def build_cleanup_preview() -> str:
    candidates = cleanup_candidates()
    total_size = sum_file_sizes(candidates)
    lines = [
        "<b>🧹 Downloads Cleanup</b>",
        "",
        f"🗑 <b>Files to remove:</b> {ltr_code(str(len(candidates)))}",
        f"💾 <b>Space to free:</b> {ltr_code(human_size(total_size))}",
    ]

    if candidates:
        lines.extend(
            [
                "",
                "These files are not active, queued, or processing.",
            ]
        )
    else:
        lines.append("Nothing to clean up.")

    return "\n".join(lines)


def get_media(message: Message):
    media_types = [
        ("video", message.video),
        ("audio", message.audio),
        ("voice", message.voice),
        ("photo", message.photo),
        ("animation", message.animation),
        ("video_note", message.video_note),
        ("sticker", message.sticker),
    ]

    for media_type, media in media_types:
        if media:
            return media_type, media

    return None, None


def extract_direct_url(text: str | None) -> str | None:
    if not text:
        return None

    match = URL_PATTERN.search(text.strip())
    if not match:
        return None

    return match.group("url").rstrip('.,!?)"]}>\'')


def path_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    return Path(unquote(parsed.path or "")).name


def is_direct_video_filename(name: str) -> bool:
    return Path(name).suffix.lower() in DIRECT_VIDEO_EXTENSIONS


def build_url_download_filename(url: str, task_id: str, fallback_suffix: str = ".mp4") -> str:
    original_name = normalize_upload_filename(path_name_from_url(url), f"video{fallback_suffix}")
    stem, suffix = split_name(original_name or "video")

    if suffix.lower() not in DIRECT_VIDEO_EXTENSIONS:
        suffix = fallback_suffix if fallback_suffix in DIRECT_VIDEO_EXTENSIONS else ".mp4"

    unique_name = f"{(stem or 'video')[:120]}_{task_id}{suffix}"
    return safe_filename(unique_name, f"video_{task_id}{suffix}")


class DirectDownloadCancelled(RuntimeError):
    pass


def is_transient_download_error(error_text: str) -> bool:
    return any(
        key in error_text
        for key in [
            "timeout",
            "timed out",
            "connection reset",
            "remote disconnected",
            "temporarily unavailable",
            "incomplete read",
            "chunkedencodingerror",
            "connection aborted",
            "502",
            "503",
            "504",
        ]
    )


def wait_for_direct_retry(seconds: int, should_cancel) -> None:
    for _ in range(seconds):
        if should_cancel():
            raise DirectDownloadCancelled("Cancelled by user.")
        time.sleep(1)


def response_total_size(response: requests.Response, downloaded: int) -> int:
    content_range = response.headers.get("content-range", "").strip()
    if content_range and "/" in content_range:
        total_text = content_range.rsplit("/", 1)[-1].strip()
        if total_text.isdigit():
            return int(total_text)

    content_length = int(response.headers.get("content-length") or 0)
    if response.status_code == 206 and content_length > 0:
        return downloaded + content_length
    return content_length


def build_download_filename(message: Message, media_type: str, media) -> str:
    original_name = getattr(media, "file_name", None)
    default_extensions = {
        "video": ".mp4",
        "audio": ".mp3",
        "voice": ".ogg",
        "photo": ".jpg",
        "animation": ".mp4",
        "video_note": ".mp4",
        "sticker": ".webp",
    }
    default_extension = default_extensions.get(media_type, ".bin")

    if not original_name:
        file_unique_id = getattr(media, "file_unique_id", None) or "file"
        original_name = f"{file_unique_id}{default_extension}"

    original_name = normalize_upload_filename(
        original_name,
        f"file{default_extension}",
    )
    stem, suffix = split_name(original_name)

    unique_name = f"{stem}_{message.id}{suffix or '.bin'}"
    return safe_filename(unique_name)


async def safe_edit_status(
    status_message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await status_message.edit_text(
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception:
        pass


async def edit_status_by_task(
    client: Client,
    task: dict,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await client.edit_message_text(
            chat_id=task["chat_id"],
            message_id=task["status_message_id"],
            text=text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception:
        pass


async def cancel_task_by_id(client: Client, message: Message, task_id: str) -> None:
    active = ACTIVE_DOWNLOADS.get(task_id)
    if active:
        active["cancelled"] = True
        text = build_status_text(
            task_id=task_id,
            file_name=active["file_name"],
            file_size=active["file_size"],
            stage="🛑 Cancelling",
            download_percent=active.get("download_percent", 0),
            upload_percent=active.get("upload_percent", 0),
            upload_status="Stopping the transfer.",
        )
        await edit_status_by_task(client, active, text)
        await message.reply_text(f"🛑 Cancel requested: {task_id}", reply_markup=MENU_KEYBOARD)
        return

    queued_task = remove_queued_task(task_id)
    if queued_task:
        cleanup_download_artifact(queued_task.get("path", ""))
        text = build_status_text(
            task_id=task_id,
            file_name=queued_task.get("file_name", Path(queued_task.get("path", "")).name or "file"),
            file_size=int(queued_task.get("file_size", 0)),
            stage="🛑 Cancelled",
            download_percent=100,
            upload_percent=0,
            upload_status="Removed from the queue.",
        )
        await edit_status_by_task(client, queued_task, text)
        await message.reply_text(f"🗑 Removed from queue: {task_id}", reply_markup=MENU_KEYBOARD)
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        mark_cancelled(task_id)
        worker_stopped = stop_rubika_worker()
        text = build_status_text(
            task_id=task_id,
            file_name=processing_task.get("file_name", Path(processing_task.get("path", "")).name or "file"),
            file_size=int(processing_task.get("file_size", 0)),
            stage="🛑 Cancelling",
            download_percent=100,
            upload_percent=int(processing_task.get("upload_percent", 0)),
            upload_status=(
                "Stopping the upload worker."
                if worker_stopped
                else "Stopping at the next safe checkpoint."
            ),
            attempt_text=processing_task.get("attempt_text"),
        )
        await edit_status_by_task(client, processing_task, text)
        await message.reply_text(f"🛑 Cancel requested: {task_id}", reply_markup=MENU_KEYBOARD)
        return

    if is_cancelled(task_id):
        await message.reply_text(f"🛑 Already cancelled: {task_id}", reply_markup=MENU_KEYBOARD)
        return

    await message.reply_text(f"🔎 Task not found: {task_id}", reply_markup=MENU_KEYBOARD)


def resolve_task_from_reply(status_message_id: int | None) -> tuple[str | None, dict | None]:
    if status_message_id is None:
        return None, None

    for task_id, payload in ACTIVE_DOWNLOADS.items():
        if payload["status_message_id"] == status_message_id:
            return task_id, payload

    queued_task = find_queued_task(
        lambda task: task.get("status_message_id") == status_message_id
    )
    if queued_task:
        return queued_task.get("task_id"), queued_task

    processing_task = load_processing()
    if processing_task and processing_task.get("status_message_id") == status_message_id:
        return processing_task.get("task_id"), processing_task

    return None, None


def cleanup_download_artifact(path_like: str) -> None:
    try:
        cleanup_local_file(path_like)
    except Exception:
        pass


def stop_rubika_worker() -> bool:
    pid = load_worker_pid()
    if not pid:
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        return True
    except OSError:
        return False


def make_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }

    def progress(current: int, total: int, client: Client, *_args) -> None:
        active = ACTIVE_DOWNLOADS.get(task_id)
        if active and active.get("cancelled"):
            client.stop_transmission()
            return

        if total <= 0:
            return

        percent = int((current * 100) / total)
        percent = min(100, max(0, percent))
        now = time.monotonic()

        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        speed_text = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        eta_text = None
        remaining = max(0, total - current)
        if remaining > 0 and state["speed_bps"] > 0:
            eta_text = human_duration(remaining / state["speed_bps"])

        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 10
            or now - state["last_update"] >= 2
        )

        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        if active is not None:
            active["download_percent"] = percent

        text = build_status_text(
            task_id=task_id,
            file_name=task_meta["file_name"],
            file_size=task_meta["file_size"],
            stage="⬇️ Downloading",
            download_percent=percent,
            upload_percent=0,
            upload_status="The video will enter the upload queue after download.",
            speed_text=speed_text,
            eta_text=eta_text,
        )
        loop.create_task(
            safe_edit_status(
                status_message,
                text,
                reply_markup=status_action_keyboard(task_id, "cancel"),
            )
        )

    return progress


def make_direct_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }

    def progress(current: int, total: int) -> None:
        active = ACTIVE_DOWNLOADS.get(task_id)
        if active and active.get("cancelled"):
            raise DirectDownloadCancelled("Cancelled by user.")

        if total > 0:
            task_meta["file_size"] = total
            if active is not None:
                active["file_size"] = total
            percent = min(100, max(0, int((current * 100) / total)))
        else:
            percent = 0

        now = time.monotonic()
        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        speed_text = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        eta_text = None
        if total > 0:
            remaining = max(0, total - current)
            if remaining > 0 and state["speed_bps"] > 0:
                eta_text = human_duration(remaining / state["speed_bps"])

        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 10
            or now - state["last_update"] >= 2
        )

        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        if active is not None:
            active["download_percent"] = percent

        text = build_status_text(
            task_id=task_id,
            file_name=task_meta["file_name"],
            file_size=task_meta["file_size"],
            stage="⬇️ Downloading",
            download_percent=percent,
            upload_percent=0,
            upload_status="Downloading the video from the link.",
            speed_text=speed_text,
            eta_text=eta_text,
        )
        loop.call_soon_threadsafe(
            lambda: loop.create_task(
                safe_edit_status(
                    status_message,
                    text,
                    reply_markup=status_action_keyboard(task_id, "cancel"),
                )
            )
        )

    return progress


def download_file_url(
    url: str,
    download_path: Path,
    progress,
    should_cancel,
    task_id: str,
) -> Path:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "file":
        source_path = Path(unquote(parsed.path or ""))
        if not source_path.exists() or not source_path.is_file():
            raise RuntimeError("Local file URL not found.")
        if not is_direct_video_filename(source_path.name):
            raise RuntimeError("The file URL must point to a video file.")

        total = source_path.stat().st_size
        copied = 0
        progress(0, total)
        with source_path.open("rb") as source, download_path.open("wb") as target:
            while True:
                if should_cancel():
                    raise DirectDownloadCancelled("Cancelled by user.")
                chunk = source.read(1024 * 256)
                if not chunk:
                    break
                target.write(chunk)
                copied += len(chunk)
                progress(copied, total)
        progress(total, total)
        return download_path

    if scheme not in {"http", "https"}:
        raise RuntimeError("Only http(s):// and file:// video URLs are supported.")

    downloaded = download_path.stat().st_size if download_path.exists() else 0
    last_error: Exception | None = None

    for attempt in range(1, DIRECT_DOWNLOAD_MAX_RETRIES + 1):
        if should_cancel():
            raise DirectDownloadCancelled("Cancelled by user.")

        resume_from = download_path.stat().st_size if download_path.exists() else 0
        headers = {}
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"

        try:
            with requests.get(
                url,
                stream=True,
                timeout=(15, 120),
                headers=headers,
            ) as response:
                if response.status_code == 416 and resume_from > 0:
                    total = response_total_size(response, 0)
                    if total > 0 and resume_from >= total:
                        progress(total, total)
                        return download_path
                    download_path.unlink(missing_ok=True)
                    continue

                response.raise_for_status()

                content_type = response.headers.get("content-type", "")
                if not (
                    content_type.lower().startswith("video/")
                    or is_direct_video_filename(path_name_from_url(response.url))
                    or is_direct_video_filename(download_path.name)
                ):
                    raise RuntimeError("The URL must point to a direct video file.")

                if resume_from > 0 and response.status_code != 206:
                    resume_from = 0
                    downloaded = 0
                    download_path.unlink(missing_ok=True)

                total = response_total_size(response, resume_from)
                if total > 0:
                    progress(resume_from, total)

                downloaded = resume_from
                mode = "ab" if resume_from > 0 else "wb"
                with download_path.open(mode) as target:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if should_cancel():
                            raise DirectDownloadCancelled("Cancelled by user.")
                        if not chunk:
                            continue
                        target.write(chunk)
                        downloaded += len(chunk)
                        progress(downloaded, total)

                if total > 0 and downloaded < total:
                    raise RuntimeError(
                        f"Download interrupted at {downloaded} of {total} bytes."
                    )

                progress(total or downloaded, total or downloaded)
                return download_path
        except Exception as error:
            if isinstance(error, DirectDownloadCancelled):
                raise

            last_error = error
            if attempt >= DIRECT_DOWNLOAD_MAX_RETRIES:
                break

            if not is_transient_download_error(str(error).lower()):
                break

            wait_for_direct_retry(DIRECT_DOWNLOAD_RETRY_DELAY * attempt, should_cancel)

    raise last_error if last_error else RuntimeError("Download failed.")


async def queue_downloaded_file(
    task_id: str,
    message: Message,
    status: Message,
    file_name: str,
    file_size: int,
    media_type: str,
    started_at: float,
    downloaded_path: Path,
    caption: str = "",
) -> None:
    file_name = normalize_upload_filename(file_name, downloaded_path.name)
    queue_position = queue_size() + (1 if load_processing() else 0) + 1
    task = {
        "task_id": task_id,
        "type": "local_file",
        "path": str(downloaded_path),
        "caption": caption,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "file_name": file_name,
        "file_size": file_size,
        "media_type": media_type,
        "started_at": started_at,
    }

    append_task(task)

    await safe_edit_status(
        status,
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=file_size,
            stage="⏳ Upload Queue",
            download_percent=100,
            upload_percent=0,
            upload_status="Waiting for upload to Rubika.",
            queue_position=queue_position,
        ),
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_menu(message)


@app.on_message(filters.private & filters.command("status"))
async def status_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_status_summary(message)


@app.on_message(filters.private & filters.command("transfers"))
async def transfers_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await send_transfers_summary(message)


@app.on_message(filters.private & filters.command("cleanup"))
async def cleanup_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    command = message.command or []
    confirm = len(command) > 1 and command[1].lower() == "confirm"

    if not confirm:
        await send_cleanup_preview(message)
        return

    await run_cleanup(message)


async def retry_task_by_id(client: Client, message: Message, task_id: str) -> None:
    if task_id in ACTIVE_DOWNLOADS:
        await message.reply_text(f"⬇️ This transfer is still downloading: {task_id}")
        return

    if find_queued_task(lambda task: task.get("task_id") == task_id):
        await message.reply_text(f"⏳ This transfer is already queued: {task_id}")
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        await message.reply_text(f"🚀 This transfer is already uploading: {task_id}")
        return

    failed_entry = find_failed_entry(task_id)
    if not failed_entry:
        await message.reply_text(f"🔎 Failed transfer not found: {task_id}")
        return

    task = dict(failed_entry.get("task") or {})
    path = Path(task.get("path", ""))
    if not path.exists():
        await message.reply_text(
            "\n".join(
                [
                    f"⚠️ Local file not found: {task_id}",
                    "It was probably cleaned up. Please send the video again.",
                ]
            ),
            reply_markup=MENU_KEYBOARD,
        )
        return

    task["upload_percent"] = 0
    task["attempt_text"] = None
    task["started_at"] = time.time()
    task["file_size"] = int(task.get("file_size") or path.stat().st_size)
    append_task(task)

    queue_position = queue_size() + (1 if load_processing() else 0)
    text = build_status_text(
        task_id=task_id,
        file_name=task.get("file_name", path.name),
        file_size=int(task.get("file_size", 0)),
        stage="🔁 Queued Again",
        download_percent=100,
        upload_percent=0,
        upload_status="The transfer was added back to the upload queue.",
        queue_position=queue_position,
    )
    await edit_status_by_task(
        client,
        task,
        text,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    await message.reply_text(
        f"🔁 Added back to queue: {task_id}",
        reply_markup=MENU_KEYBOARD,
    )


async def retry_all_failed_tasks(client: Client, message: Message) -> None:
    retryable_tasks = retryable_failed_tasks()
    if not retryable_tasks:
        await message.reply_text(
            "🔎 No retryable failed transfers were found.",
            reply_markup=MENU_KEYBOARD,
        )
        return

    queued_count = 0
    skipped_count = 0

    for task in retryable_tasks:
        task_id = task.get("task_id", "")
        if not task_id:
            skipped_count += 1
            continue

        if task_id in ACTIVE_DOWNLOADS:
            skipped_count += 1
            continue

        if find_queued_task(lambda queued: queued.get("task_id") == task_id):
            skipped_count += 1
            continue

        processing_task = load_processing()
        if processing_task and processing_task.get("task_id") == task_id:
            skipped_count += 1
            continue

        path = Path(task.get("path", ""))
        if not path.exists():
            skipped_count += 1
            continue

        retry_task = dict(task)
        retry_task["upload_percent"] = 0
        retry_task["attempt_text"] = None
        retry_task["speed_text"] = None
        retry_task["eta_text"] = None
        retry_task["started_at"] = time.time()
        retry_task["file_size"] = int(retry_task.get("file_size") or path.stat().st_size)
        append_task(retry_task)
        queued_count += 1

        queue_position = queue_size() + (1 if load_processing() else 0)
        text = build_status_text(
            task_id=task_id,
            file_name=retry_task.get("file_name", path.name),
            file_size=int(retry_task.get("file_size", 0)),
            stage="🔁 Queued Again",
            download_percent=100,
            upload_percent=0,
            upload_status="The transfer was added back to the upload queue.",
            queue_position=queue_position,
        )
        await edit_status_by_task(
            client,
            retry_task,
            text,
            reply_markup=status_action_keyboard(task_id, "cancel"),
        )

    if queued_count == 0:
        await message.reply_text(
            "⚠️ No failed transfers were added back to the queue.",
            reply_markup=MENU_KEYBOARD,
        )
        return

    lines = [
        "<b>🔁 Retry All Complete</b>",
        "",
        f"Added back to queue: <b>{queued_count}</b>",
    ]
    if skipped_count:
        lines.append(f"Skipped: <b>{skipped_count}</b>")

    await message.reply_text(
        "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & filters.command("retry"))
async def retry_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)

    if len(message.command) < 2:
        await message.reply_text(
            "🔁 Open Transfers and use a Retry button, or run /retry all.",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=main_action_keyboard(),
        )
        return

    task_id = message.command[1].strip()
    if task_id.lower() == "all":
        await retry_all_failed_tasks(client, message)
        return
    await retry_task_by_id(client, message, task_id)


@app.on_message(filters.private & filters.command("retry_all"))
async def retry_all_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    await ensure_bot_commands(client)
    await retry_all_failed_tasks(client, message)


@app.on_message(filters.private & MENU_BUTTON_FILTER)
async def menu_button_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    text = (message.text or "").strip()

    if text == BTN_STATUS:
        await status_handler(client, message)
    elif text == BTN_TRANSFERS:
        await transfers_handler(client, message)
    elif text == BTN_CLEANUP:
        await cleanup_handler(client, message)
    elif text == BTN_CANCEL:
        await send_cancel_picker(message)


@app.on_callback_query(filters.regex(r"^menu:"))
async def menu_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    action = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer()

    if action == "status":
        await send_status_summary(callback_query.message)
    elif action == "transfers":
        await send_transfers_summary(callback_query.message)
    elif action == "cleanup":
        await send_cleanup_preview(callback_query.message)
    elif action == "cancel":
        await send_cancel_picker(callback_query.message)


@app.on_callback_query(filters.regex(r"^cleanup:confirm$"))
async def cleanup_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    await callback_query.answer("Cleanup started.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await run_cleanup(callback_query.message)


@app.on_callback_query(filters.regex(r"^cancel:"))
async def cancel_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    task_id = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer("Cancel requested.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await cancel_task_by_id(client, callback_query.message, task_id)


@app.on_callback_query(filters.regex(r"^retry:"))
async def retry_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    task_id = (callback_query.data or "").split(":", 1)[1].strip()
    await callback_query.answer("Retry queued.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await retry_task_by_id(client, callback_query.message, task_id)


@app.on_callback_query(filters.regex(r"^retry_all$"))
async def retry_all_callback_handler(client: Client, callback_query: CallbackQuery):
    if not await ensure_authorized_callback(callback_query):
        return
    await callback_query.answer("Retrying all failed transfers.")

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await retry_all_failed_tasks(client, callback_query.message)


@app.on_message(filters.private & filters.command("cancel"))
async def cancel_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    task_id = None
    if message.command and len(message.command) > 1:
        task_id = message.command[1].strip()

    if not task_id and message.reply_to_message:
        task_id, _ = resolve_task_from_reply(message.reply_to_message.id)

    if not task_id:
        await send_cancel_picker(message)
        return

    await cancel_task_by_id(client, message, task_id)


@app.on_message(
    filters.private
    & (
        filters.video
        | filters.audio
        | filters.voice
        | filters.photo
        | filters.animation
        | filters.video_note
        | filters.sticker
    )
)
async def media_handler(client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return
    media_type, media = get_media(message)
    if not media:
        await message.reply_text("⚠️ This message cannot be processed.")
        return

    task_id = uuid.uuid4().hex[:10]
    file_name = build_download_filename(message, media_type, media)
    file_size = int(getattr(media, "file_size", 0) or 0)
    download_path = DOWNLOAD_DIR / file_name
    started_at = time.time()

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=file_size,
            stage="⏳ Preparing Download",
            download_percent=0,
            upload_percent=0,
            upload_status="The video will start downloading soon.",
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(download_path),
        "file_name": file_name,
        "file_size": file_size,
        "started_at": started_at,
        "cancelled": False,
        "download_percent": 0,
        "upload_percent": 0,
    }

    try:
        downloaded = await client.download_media(
            message,
            file_name=str(download_path),
            progress=make_download_progress_callback(
                task_id,
                status,
                {"file_name": file_name, "file_size": file_size},
            ),
            progress_args=(client,),
        )

        if ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled"):
            raise RuntimeError("Cancelled by user.")

        if not downloaded:
            raise RuntimeError("Download failed.")

        downloaded_path = Path(downloaded)
        if not downloaded_path.exists():
            raise RuntimeError("Downloaded file not found.")

        await queue_downloaded_file(
            task_id=task_id,
            message=message,
            status=status,
            file_name=file_name,
            file_size=file_size,
            media_type=media_type,
            started_at=started_at,
            downloaded_path=downloaded_path,
            caption=message.caption or "",
        )

    except Exception as e:
        active = ACTIVE_DOWNLOADS.get(task_id, {})
        was_cancelled = active.get("cancelled") or "cancelled by user" in str(e).lower()
        cleanup_download_artifact(str(download_path))

        if was_cancelled:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage="🛑 Cancelled",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="Transfer stopped.",
                ),
            )
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage="❌ Download Failed",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="The download did not complete.",
                    note=str(e),
                ),
            )
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


@app.on_message(filters.private & filters.text)
async def direct_video_url_handler(_client: Client, message: Message):
    if not await ensure_authorized_message(message):
        return

    text = (message.text or "").strip()
    if not text or text in MENU_BUTTONS or text.startswith("/"):
        return

    url = extract_direct_url(text)
    if not url:
        return

    task_id = uuid.uuid4().hex[:10]
    fallback_suffix = Path(path_name_from_url(url)).suffix.lower()
    if fallback_suffix not in DIRECT_VIDEO_EXTENSIONS:
        fallback_suffix = ".mp4"

    file_name = build_url_download_filename(url, task_id, fallback_suffix)
    download_path = DOWNLOAD_DIR / file_name
    started_at = time.time()
    task_meta = {"file_name": file_name, "file_size": 0}

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=0,
            stage="⏳ Preparing Download",
            download_percent=0,
            upload_percent=0,
            upload_status="The video link will start downloading soon.",
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=status_action_keyboard(task_id, "cancel"),
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(download_path),
        "file_name": file_name,
        "file_size": 0,
        "started_at": started_at,
        "cancelled": False,
        "download_percent": 0,
        "upload_percent": 0,
    }

    try:
        downloaded_path = await asyncio.to_thread(
            download_file_url,
            url,
            download_path,
            make_direct_download_progress_callback(task_id, status, task_meta),
            lambda: ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled", False),
            task_id,
        )

        if ACTIVE_DOWNLOADS.get(task_id, {}).get("cancelled"):
            raise DirectDownloadCancelled("Cancelled by user.")

        if not downloaded_path.exists():
            raise RuntimeError("Downloaded file not found.")

        file_size = task_meta["file_size"] or downloaded_path.stat().st_size
        await queue_downloaded_file(
            task_id=task_id,
            message=message,
            status=status,
            file_name=file_name,
            file_size=file_size,
            media_type="video",
            started_at=started_at,
            downloaded_path=downloaded_path,
            caption="",
        )
    except Exception as e:
        active = ACTIVE_DOWNLOADS.get(task_id, {})
        was_cancelled = active.get("cancelled") or isinstance(e, DirectDownloadCancelled)
        cleanup_download_artifact(str(download_path))

        if was_cancelled:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=task_meta.get("file_size", 0),
                    stage="🛑 Cancelled",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=0,
                    upload_status="Transfer stopped.",
                ),
            )
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=task_meta.get("file_size", 0),
                    stage="❌ Download Failed",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=0,
                    upload_status="The link download did not complete.",
                    note=str(e),
                ),
            )
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


if __name__ == "__main__":
    app.run()
