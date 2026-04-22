import asyncio
import os
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from pyrogram import Client, enums, filters
from pyrogram.types import Message

from task_store import (
    DOWNLOAD_DIR,
    append_task,
    build_status_text,
    cleanup_local_file,
    ensure_storage_dirs,
    find_queued_task,
    is_cancelled,
    load_processing,
    mark_cancelled,
    queue_size,
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
            stage="در حال دانلود از تلگرام",
            download_percent=percent,
            upload_percent=0,
            upload_status="در انتظار تکمیل دانلود",
            note="بعد از دانلود، فایل وارد صف آپلود روبیکا می‌شود.",
        )
        loop.create_task(safe_edit_status(status_message, text))

    return progress


@app.on_message(filters.private & filters.command("start"))
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        "مدیا بفرست. روی پیام وضعیت هم می‌تونی /cancel ریپلای کنی تا کار لغو بشه."
    )


@app.on_message(filters.private & filters.command("cancel"))
async def cancel_handler(client: Client, message: Message):
    task_id = None
    if len(message.command) > 1:
        task_id = message.command[1].strip()

    if not task_id and message.reply_to_message:
        task_id, _ = resolve_task_from_reply(message.reply_to_message.id)

    if not task_id:
        await message.reply_text("برای لغو، /cancel <task_id> بزن یا روی پیام وضعیت ریپلای کن.")
        return

    active = ACTIVE_DOWNLOADS.get(task_id)
    if active:
        active["cancelled"] = True
        text = build_status_text(
            task_id=task_id,
            file_name=active["file_name"],
            file_size=active["file_size"],
            stage="درخواست لغو ثبت شد",
            download_percent=active.get("download_percent", 0),
            upload_percent=active.get("upload_percent", 0),
            upload_status="ارسال نخواهد شد",
            note="اگر دانلود در حال انجام باشد در اولین فرصت متوقف می‌شود.",
        )
        await edit_status_by_task(client, active, text)
        await message.reply_text(f"درخواست لغو برای {task_id} ثبت شد.")
        return

    queued_task = remove_queued_task(task_id)
    if queued_task:
        cleanup_download_artifact(queued_task.get("path", ""))
        text = build_status_text(
            task_id=task_id,
            file_name=queued_task.get("file_name", Path(queued_task.get("path", "")).name or "file"),
            file_size=int(queued_task.get("file_size", 0)),
            stage="لغو شد",
            download_percent=100,
            upload_percent=0,
            upload_status="از صف حذف شد",
            note="فایل موقت دانلودشده پاک شد.",
        )
        await edit_status_by_task(client, queued_task, text)
        await message.reply_text(f"کار {task_id} از صف حذف شد.")
        return

    processing_task = load_processing()
    if processing_task and processing_task.get("task_id") == task_id:
        mark_cancelled(task_id)
        text = build_status_text(
            task_id=task_id,
            file_name=processing_task.get("file_name", Path(processing_task.get("path", "")).name or "file"),
            file_size=int(processing_task.get("file_size", 0)),
            stage="درخواست لغو ثبت شد",
            download_percent=100,
            upload_percent=int(processing_task.get("upload_percent", 0)),
            upload_status="در حال توقف در اولین نقطه امن",
            note="اگر تلاش آپلود فعال باشد، قبل از تلاش بعدی متوقف می‌شود.",
            attempt_text=processing_task.get("attempt_text"),
        )
        await edit_status_by_task(client, processing_task, text)
        await message.reply_text(f"لغو برای {task_id} ثبت شد و به worker اعلام شد.")
        return

    if is_cancelled(task_id):
        await message.reply_text(f"کار {task_id} قبلا لغو شده.")
        return

    await message.reply_text(f"کاری با شناسه {task_id} پیدا نشد.")


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
        await message.reply_text("فایل قابل پردازش نیست.")
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
            stage="آماده‌سازی دانلود",
            download_percent=0,
            upload_percent=0,
            upload_status="در انتظار دانلود",
            note="بعد از تکمیل دانلود، فایل وارد صف آپلود روبیکا می‌شود.",
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
                stage="دانلود کامل شد",
                download_percent=100,
                upload_percent=0,
                upload_status="در صف آپلود روبیکا",
                queue_position=queue_position,
                note="وقتی worker شروع کند، همین پیام به‌روزرسانی می‌شود.",
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
                    stage="لغو شد",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="ارسال نشد",
                    note="دانلود نیمه‌کاره متوقف شد و فایل موقت پاک شد.",
                ),
            )
        else:
            await safe_edit_status(
                status,
                build_status_text(
                    task_id=task_id,
                    file_name=file_name,
                    file_size=file_size,
                    stage="خطا در دانلود",
                    download_percent=active.get("download_percent", 0),
                    upload_percent=active.get("upload_percent", 0),
                    upload_status="شروع نشد",
                    note=str(e),
                ),
            )
    finally:
        ACTIVE_DOWNLOADS.pop(task_id, None)


if __name__ == "__main__":
    app.run()
