import asyncio
import os
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from pyrogram import Client, enums, filters
from pyrogram.types import BotCommand, KeyboardButton, Message, ReplyKeyboardMarkup

from task_store import (
    DOWNLOAD_DIR,
    append_task,
    build_status_text,
    cleanup_local_file,
    ensure_storage_dirs,
    find_failed_entry,
    human_size,
    find_queued_task,
    is_cancelled,
    load_processing,
    mark_cancelled,
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

ensure_storage_dirs()

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise RuntimeError("Please set API_ID, API_HASH and BOT_TOKEN in .env")

app = Client(
    "tel2rub",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

ACTIVE_DOWNLOADS: dict[str, dict] = {}
COMMANDS_READY = False

BTN_STATUS = "📊 وضعیت"
BTN_TRANSFERS = "📋 انتقال‌ها"
BTN_CLEANUP = "🧹 پاکسازی"
BTN_HELP = "ℹ️ راهنما"
BTN_CANCEL = "🛑 لغو"
MENU_BUTTONS = {BTN_STATUS, BTN_TRANSFERS, BTN_CLEANUP, BTN_HELP, BTN_CANCEL}

MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_STATUS), KeyboardButton(BTN_TRANSFERS)],
        [KeyboardButton(BTN_CLEANUP), KeyboardButton(BTN_HELP)],
        [KeyboardButton(BTN_CANCEL)],
    ],
    resize_keyboard=True,
)

BOT_COMMANDS = [
    BotCommand("start", "باز کردن منو"),
    BotCommand("menu", "نمایش منوی اصلی"),
    BotCommand("status", "وضعیت صف و فضای دانلود"),
    BotCommand("transfers", "لیست انتقال‌های فعال و صف"),
    BotCommand("retry", "تلاش دوباره انتقال ناموفق"),
    BotCommand("cleanup", "پاکسازی امن پوشه دانلود"),
    BotCommand("cancel", "لغو یک انتقال"),
    BotCommand("help", "راهنما"),
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


def build_menu_text() -> str:
    return "\n".join(
        [
            "<b>🎬 Tele2Rub</b>",
            "ویدیو بفرست تا خودکار به پیام‌های ذخیره شده‌ی روبیکا ارسال شود.",
            "",
            "📊 <b>وضعیت</b> - صف، انتقال فعال و فضای دانلود",
            "📋 <b>انتقال‌ها</b> - لیست کارهای فعال و منتظر",
            "🔁 <b>تلاش دوباره</b> - /retry <code>task_id</code>",
            "🧹 <b>پاکسازی</b> - حذف فایل‌های اضافه از downloads",
            "🛑 <b>لغو</b> - ریپلای روی پیام وضعیت یا /cancel <code>task_id</code>",
        ]
    )


async def send_menu(message: Message) -> None:
    await message.reply_text(
        build_menu_text(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
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


def compact_task_line(prefix: str, task: dict, detail: str = "") -> str:
    task_id = task.get("task_id", "-")
    file_name = Path(task.get("file_name") or task.get("path") or "video").name
    stem, suffix = split_name(file_name)
    display_name = safe_filename(f"{stem[:22]}{suffix}", "video")
    size = human_size(int(task.get("file_size", 0) or 0))
    detail_text = f" - {detail}" if detail else ""
    return f"{prefix} <code>{task_id}</code> - {display_name} ({size}){detail_text}"


def build_status_summary() -> str:
    queued = read_queue_tasks()
    processing = load_processing()
    failed_entries = read_failed_entries()
    files = iter_download_files()
    candidates = cleanup_candidates()

    lines = [
        "<b>📊 وضعیت Tele2Rub</b>",
        "",
        f"⬇️ دریافت فعال: <b>{len(ACTIVE_DOWNLOADS)}</b>",
        f"🚀 ارسال فعال: <b>{1 if processing else 0}</b>",
        f"⏳ در صف: <b>{len(queued)}</b>",
        f"❌ ناموفق: <b>{len(failed_entries)}</b>",
        f"📁 فایل‌های downloads: <b>{len(files)}</b> / {human_size(sum_file_sizes(files))}",
        f"🧹 قابل پاکسازی: <b>{len(candidates)}</b> / {human_size(sum_file_sizes(candidates))}",
        "",
        "برای جزئیات: /transfers",
    ]

    if candidates:
        lines.append("برای پاکسازی: /cleanup confirm")

    return "\n".join(lines)


def build_transfers_summary() -> str:
    queued = read_queue_tasks()
    processing = load_processing()
    failed_entries = read_failed_entries()
    lines = ["<b>📋 انتقال‌ها</b>", ""]

    if ACTIVE_DOWNLOADS:
        lines.append("<b>⬇️ در حال دریافت</b>")
        for active in list(ACTIVE_DOWNLOADS.values())[:5]:
            detail = f"{active.get('download_percent', 0)}%"
            lines.append(compact_task_line("•", active, detail))
        lines.append("")

    if processing:
        lines.append("<b>🚀 در حال ارسال</b>")
        detail = f"{processing.get('upload_percent', 0)}%"
        if processing.get("attempt_text"):
            detail += f" / تلاش {processing['attempt_text']}"
        lines.append(compact_task_line("•", processing, detail))
        lines.append("")

    if queued:
        lines.append("<b>⏳ صف ارسال</b>")
        for index, task in enumerate(queued[:8], start=1):
            lines.append(compact_task_line(f"{index}.", task))
        if len(queued) > 8:
            lines.append(f"… و {len(queued) - 8} مورد دیگر")
        lines.append("")

    retryable_failed = []
    for entry in reversed(failed_entries):
        task = entry.get("task") or {}
        path = Path(task.get("path", ""))
        if path.exists():
            retryable_failed.append(task)

    if retryable_failed:
        lines.append("<b>❌ ناموفق قابل تلاش دوباره</b>")
        for task in retryable_failed[:5]:
            lines.append(compact_task_line("•", task, "برای تلاش دوباره: /retry"))
        if len(retryable_failed) > 5:
            lines.append(f"… و {len(retryable_failed) - 5} مورد دیگر")
        lines.append("")

    if len(lines) == 2:
        lines.append("فعلا انتقال فعالی وجود ندارد.")

    lines.append("🛑 لغو: /cancel <code>task_id</code>")
    lines.append("🔁 تلاش دوباره: /retry <code>task_id</code>")
    return "\n".join(lines)


def build_cleanup_preview() -> str:
    candidates = cleanup_candidates()
    total_size = sum_file_sizes(candidates)
    lines = [
        "<b>🧹 پاکسازی downloads</b>",
        "",
        f"فایل قابل حذف: <b>{len(candidates)}</b>",
        f"فضای قابل آزادسازی: <b>{human_size(total_size)}</b>",
    ]

    if candidates:
        lines.extend(
            [
                "",
                "این فایل‌ها در صف یا انتقال فعال نیستند.",
                "برای تایید حذف بزن:",
                "<code>/cleanup confirm</code>",
            ]
        )
    else:
        lines.append("چیزی برای پاکسازی نیست.")

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


def build_download_filename(message: Message, media_type: str, media) -> str:
    original_name = getattr(media, "file_name", None)

    if not original_name:
        file_unique_id = getattr(media, "file_unique_id", None) or "file"

        default_extensions = {
            "video": ".mp4",
            "audio": ".mp3",
            "voice": ".ogg",
            "photo": ".jpg",
            "animation": ".mp4",
            "video_note": ".mp4",
            "sticker": ".webp",
        }

        original_name = f"{file_unique_id}{default_extensions.get(media_type, '.bin')}"

    original_name = safe_filename(original_name)
    stem, suffix = split_name(original_name)

    unique_name = f"{stem}_{message.id}{suffix or '.bin'}"
    return safe_filename(unique_name)


async def safe_edit_status(status_message: Message, text: str) -> None:
    try:
        await status_message.edit_text(text, parse_mode=enums.ParseMode.HTML)
    except Exception:
        pass


async def edit_status_by_task(client: Client, task: dict, text: str) -> None:
    try:
        await client.edit_message_text(
            chat_id=task["chat_id"],
            message_id=task["status_message_id"],
            text=text,
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception:
        pass


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


def make_download_progress_callback(task_id: str, status_message: Message, task_meta: dict):
    loop = asyncio.get_running_loop()
    state = {"last_percent": -1, "last_update": 0.0}

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
        should_emit = (
            percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 10
            or now - state["last_update"] >= 3
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
            stage="⬇️ در حال دریافت",
            download_percent=percent,
            upload_percent=0,
            upload_status="بعد از دریافت وارد صف ارسال می‌شود.",
        )
        loop.create_task(safe_edit_status(status_message, text))

    return progress


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    await send_menu(message)


@app.on_message(filters.private & filters.command("menu"))
async def menu_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    await send_menu(message)


@app.on_message(filters.private & filters.command("help"))
async def help_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    await message.reply_text(
        build_menu_text(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & filters.command("status"))
async def status_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    await message.reply_text(
        build_status_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & filters.command("transfers"))
async def transfers_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    await message.reply_text(
        build_transfers_summary(),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & filters.command("cleanup"))
async def cleanup_handler(client: Client, message: Message):
    await ensure_bot_commands(client)
    command = message.command or []
    confirm = len(command) > 1 and command[1].lower() == "confirm"

    if not confirm:
        await message.reply_text(
            build_cleanup_preview(),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=MENU_KEYBOARD,
        )
        return

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
                "<b>🧹 پاکسازی انجام شد</b>",
                "",
                f"فایل حذف‌شده: <b>{removed_count}</b>",
                f"فضای آزادشده: <b>{human_size(total_size)}</b>",
            ]
        ),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & filters.command("retry"))
async def retry_handler(client: Client, message: Message):
    await ensure_bot_commands(client)

    if len(message.command) < 2:
        await message.reply_text(
            "🔁 برای تلاش دوباره بزن: /retry <task_id>",
            reply_markup=MENU_KEYBOARD,
        )
        return

    task_id = message.command[1].strip()

    if task_id in ACTIVE_DOWNLOADS:
        await message.reply_text(f"⬇️ این انتقال هنوز در حال دریافت است: {task_id}")
        return

    if find_queued_task(lambda task: task.get("task_id") == task_id):
        await message.reply_text(f"⏳ این انتقال همین حالا در صف است: {task_id}")
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        await message.reply_text(f"🚀 این انتقال همین حالا در حال ارسال است: {task_id}")
        return

    failed_entry = find_failed_entry(task_id)
    if not failed_entry:
        await message.reply_text(f"🔎 انتقال ناموفق پیدا نشد: {task_id}")
        return

    task = dict(failed_entry.get("task") or {})
    path = Path(task.get("path", ""))
    if not path.exists():
        await message.reply_text(
            "\n".join(
                [
                    f"⚠️ فایل محلی پیدا نشد: {task_id}",
                    "احتمالا قبلا پاکسازی شده و باید دوباره ویدیو را ارسال کنی.",
                ]
            ),
            reply_markup=MENU_KEYBOARD,
        )
        return

    task["upload_percent"] = 0
    task["attempt_text"] = None
    task["file_size"] = int(task.get("file_size") or path.stat().st_size)
    append_task(task)

    queue_position = queue_size() + (1 if load_processing() else 0)
    text = build_status_text(
        task_id=task_id,
        file_name=task.get("file_name", path.name),
        file_size=int(task.get("file_size", 0)),
        stage="🔁 دوباره در صف",
        download_percent=100,
        upload_percent=0,
        upload_status="انتقال دوباره به صف ارسال اضافه شد.",
        queue_position=queue_position,
    )
    await edit_status_by_task(client, task, text)

    await message.reply_text(
        f"🔁 دوباره به صف اضافه شد: {task_id}",
        reply_markup=MENU_KEYBOARD,
    )


@app.on_message(filters.private & MENU_BUTTON_FILTER)
async def menu_button_handler(client: Client, message: Message):
    text = (message.text or "").strip()

    if text == BTN_STATUS:
        await status_handler(client, message)
    elif text == BTN_TRANSFERS:
        await transfers_handler(client, message)
    elif text == BTN_CLEANUP:
        await cleanup_handler(client, message)
    elif text == BTN_HELP:
        await help_handler(client, message)
    elif text == BTN_CANCEL:
        await message.reply_text(
            "🛑 برای لغو، روی پیام وضعیت ریپلای کن یا /cancel <task_id> بزن.",
            reply_markup=MENU_KEYBOARD,
        )


@app.on_message(filters.private & filters.command("cancel"))
async def cancel_handler(client: Client, message: Message):
    task_id = None
    if len(message.command) > 1:
        task_id = message.command[1].strip()

    if not task_id and message.reply_to_message:
        task_id, _ = resolve_task_from_reply(message.reply_to_message.id)

    if not task_id:
        await message.reply_text("🛑 برای لغو، روی پیام وضعیت ریپلای کن یا /cancel <task_id> بزن.")
        return

    active = ACTIVE_DOWNLOADS.get(task_id)
    if active:
        active["cancelled"] = True
        text = build_status_text(
            task_id=task_id,
            file_name=active["file_name"],
            file_size=active["file_size"],
            stage="🛑 در حال لغو",
            download_percent=active.get("download_percent", 0),
            upload_percent=active.get("upload_percent", 0),
            upload_status="انتقال متوقف می‌شود.",
        )
        await edit_status_by_task(client, active, text)
        await message.reply_text(f"🛑 لغو ثبت شد: {task_id}")
        return

    queued_task = remove_queued_task(task_id)
    if queued_task:
        cleanup_download_artifact(queued_task.get("path", ""))
        text = build_status_text(
            task_id=task_id,
            file_name=queued_task.get("file_name", Path(queued_task.get("path", "")).name or "file"),
            file_size=int(queued_task.get("file_size", 0)),
            stage="🛑 لغو شد",
            download_percent=100,
            upload_percent=0,
            upload_status="از صف حذف شد.",
        )
        await edit_status_by_task(client, queued_task, text)
        await message.reply_text(f"🗑 از صف حذف شد: {task_id}")
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        mark_cancelled(task_id)
        text = build_status_text(
            task_id=task_id,
            file_name=processing_task.get("file_name", Path(processing_task.get("path", "")).name or "file"),
            file_size=int(processing_task.get("file_size", 0)),
            stage="🛑 در حال لغو",
            download_percent=100,
            upload_percent=int(processing_task.get("upload_percent", 0)),
            upload_status="بعد از پایان بخش فعلی متوقف می‌شود.",
            attempt_text=processing_task.get("attempt_text"),
        )
        await edit_status_by_task(client, processing_task, text)
        await message.reply_text(f"🛑 لغو ثبت شد: {task_id}")
        return

    if is_cancelled(task_id):
        await message.reply_text(f"🛑 قبلا لغو شده: {task_id}")
        return

    await message.reply_text(f"🔎 شناسه پیدا نشد: {task_id}")


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
    media_type, media = get_media(message)
    if not media:
        await message.reply_text("⚠️ این پیام قابل پردازش نیست.")
        return

    task_id = uuid.uuid4().hex[:10]
    file_name = build_download_filename(message, media_type, media)
    file_size = int(getattr(media, "file_size", 0) or 0)
    download_path = DOWNLOAD_DIR / file_name

    status = await message.reply_text(
        build_status_text(
            task_id=task_id,
            file_name=file_name,
            file_size=file_size,
            stage="⏳ آماده دریافت",
            download_percent=0,
            upload_percent=0,
            upload_status="ویدیو به زودی دریافت می‌شود.",
        ),
        parse_mode=enums.ParseMode.HTML,
    )

    ACTIVE_DOWNLOADS[task_id] = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "status_message_id": status.id,
        "download_path": str(download_path),
        "file_name": file_name,
        "file_size": file_size,
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

        queue_position = queue_size() + (1 if load_processing() else 0) + 1
        task = {
            "task_id": task_id,
            "type": "local_file",
            "path": str(downloaded_path),
            "caption": message.caption or "",
            "chat_id": message.chat.id,
            "status_message_id": status.id,
            "file_name": file_name,
            "file_size": file_size,
            "media_type": media_type,
        }

        append_task(task)

        await safe_edit_status(
            status,
            build_status_text(
                task_id=task_id,
                file_name=file_name,
                file_size=file_size,
                stage="⏳ در صف ارسال",
                download_percent=100,
                upload_percent=0,
                upload_status="منتظر نوبت ارسال به روبیکا.",
                queue_position=queue_position,
            ),
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
                    stage="🛑 لغو شد",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="انتقال متوقف شد.",
                ),
            )
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage="❌ خطا در دریافت",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="دریافت انجام نشد.",
                    note=str(e),
                ),
            )
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


if __name__ == "__main__":
    app.run()
