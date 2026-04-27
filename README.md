# Walrus

[فارسی](README.fa.md)

Walrus is a Telegram bot that downloads files from Telegram or direct links and uploads them to Rubika. It is built for personal file transfer workflows where you want Telegram as the control panel and Rubika as the destination.

## Disclaimer

This project is shared for research, learning, and personal experimentation.
Do not use it for abuse, spam, unauthorized access, privacy violations, or any harmful or unlawful purpose.
You are responsible for using it in a way that respects platform rules, local laws, and other people's rights.

## Features

- Accepts videos, documents, audio, photos, and other Telegram file messages in private chat
- Accepts direct file links such as `https://...pdf`, `https://...zip`, and `file:///...mp4`
- Accepts multiple direct file links in one message and queues each one
- Sends a final summary message after a multi-link batch finishes intake
- Keeps a local upload queue to avoid overlapping jobs
- Shows live download and upload progress
- Supports upload retries for temporary Rubika errors
- Supports canceling active or queued transfers
- Supports retrying failed transfers when the downloaded file still exists
- Provides quick action buttons for status, transfers, cleanup, cancel, and retry
- Sends a completion notification when a transfer finishes successfully
- Shows total elapsed transfer time on successful uploads
- Lets you switch the active Rubika number/session from Telegram
- Lets you choose the upload destination from Telegram
- Supports `Rubika Saved Messages` and recent `Rubika channels` as destinations
- Uploads files with a sanitized, readable version of their original filename

## Requirements

- Python 3.9+
- Telegram `API_ID`
- Telegram `API_HASH`
- Telegram bot token

---

## Installation

```bash
git clone https://github.com/rezaaa/walrus.git
cd walrus
cp .env.example .env
```

## Configuration

Edit `.env` in the project root:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_telegram_bot_token
TELEGRAM_SESSION=walrus
RUBIKA_SESSION=rubsession
OWNER_TELEGRAM_ID=123456789
```

Variables:

- `API_ID` - from https://my.telegram.org
- `API_HASH` - from https://my.telegram.org
- `BOT_TOKEN` - from BotFather
- `TELEGRAM_SESSION` - optional Pyrogram session name; defaults to `walrus`
- `RUBIKA_SESSION` - session name or path used by `rubpy`
- `OWNER_TELEGRAM_ID` - optional; if set, only this Telegram user ID can use the bot

`TELEGRAM_SESSION` controls the Telegram/Pyrogram session file name, such as `walrus.session`.
`RUBIKA_SESSION` controls the session file name that `rubpy` uses on disk, such as `rubsession.rp`.
Changing the Rubika account from Telegram replaces the authenticated session file for that configured session name. It does not require editing `.env` or restarting the bot.

How to get your Telegram user ID:

- forward one of your messages to [@userinfobot](https://t.me/userinfobot)
- or message [@RawDataBot](https://t.me/RawDataBot) and use the value in `from.id`

Then put that number into `.env` as `OWNER_TELEGRAM_ID`.
If you leave it unset, the bot stays open for everyone.

## Setup

Walrus uses Telegram for Rubika account setup.

After the app is running, open the Telegram bot and send:

```text
/start
```

If no saved Rubika session exists yet, the bot will guide you through setup and create the session file automatically.

Account setup and account changes work like this:

1. Send `/start` for first setup, or open `⚙️ Settings`
2. Tap `📱 Change Account` or run `/set_rubika`
3. Send the Rubika phone number
4. If Rubika asks for an account password, send it in the bot
5. Wait for the OTP prompt
6. Send the OTP code

After a successful login, the current Rubika session is replaced and reused by the worker for future uploads.

`rubpy` stores the authenticated session on disk using the configured `RUBIKA_SESSION` name. With current versions of `rubpy`, that is typically a `.rp` file such as `rubsession.rp`.

## Install on Server With Script

Install the system packages once:

```bash
apt update && apt install -y git python3 python3-venv screen
```

Clone Walrus and configure `.env`:

```bash
cd /opt
git clone https://github.com/rezaaa/walrus.git
cd /opt/walrus
cp .env.example .env
nano .env
```

Run the setup script:

```bash
bash update.sh
```

`update.sh` creates `venv/` if needed, installs dependencies, stops any old `walrus` screen session, and starts the app in a new screen session.

Verify or attach to the running app:

```bash
screen -ls
screen -r walrus
```

Then open the Telegram bot and finish Rubika setup with `/start`. Detach from screen without stopping the app with `Ctrl + A`, then `D`.

## Update

Use the same script for later updates:

```bash
cd /opt/walrus
bash update.sh
```

The script updates the code, refreshes dependencies, restarts the `screen` session, and keeps the same `.env`.

Rubika setup and destination setup are handled from the Telegram bot UI. After updating, open the bot and run `/start` or `/settings` to confirm the Rubika account and upload destination.

If an older install behaves strangely after updating, run the setup again with `bash update.sh`. If the app still does not recover, clone the repository again, copy your `.env`, and complete Rubika setup from the bot UI.

## Run Manually

If you do not want to use `screen` or `update.sh`:

```bash
cd walrus
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

This starts:

- `telegram_bot.py` - Telegram receiver and downloader
- `rubika_worker.py` - Rubika upload worker

## Bot Controls

Main menu buttons:

- `📊 Status`
- `📋 Transfers`
- `🧹 Cleanup`
- `🛑 Cancel`
- `⚙️ Settings`

Available commands:

- `/start` - open the main menu
- `/settings` - show the current Rubika session and upload destination
- `/set_rubika` - start Rubika number setup in Telegram
- `/set_rubika <phone_number>` - start Rubika number setup directly with a phone number
- `/status` - show active downloads, active upload, queue, failed count, and local storage usage
- `/transfers` - show current downloads, current upload, queued items, and retryable failed transfers
- `/cleanup` - preview removable files in `downloads/`
- `/cleanup confirm` - delete safe cleanup candidates
- `/cancel` - show clickable cancel buttons for active jobs
- `/cancel <task_id>` - cancel a specific task
- `/retry <task_id>` - requeue a failed task if its local file still exists
- `/retry_all` - requeue every retryable failed task

Transfer message buttons:

- `🛑 Cancel` on active and queued transfers
- `🔁 Retry` on failed transfers
- `🔁 Retry All Failed` in Transfers when retryable failed items exist

Successful transfers:

- the original status message is updated with the total transfer time
- the bot sends a separate completion message when the upload finishes

Upload destinations:

- default destination is Rubika Saved Messages
- open `⚙️ Settings` and tap `📬 Destination` to change where future uploads go
- choose `☁️ Saved Messages` or `📣 Choose Channel`
- channel uploads require the active Rubika account to have permission to post in that channel
- already queued transfers keep the destination they had when they were queued

Direct link uploads:

- send a message containing a direct file URL
- you can include multiple direct file URLs in one message
- when a multi-link batch finishes, the bot sends one summary with queued, failed, and cancelled counts
- supported schemes: `https://`, `http://`, and `file://`
- the link should point to the actual file, not a webpage
- supported direct-link extensions include common video, image, audio, document, PDF, and archive formats such as `.pdf` and `.zip`

## Retry Policy

Rubika uploads retry automatically on temporary errors.

- Max attempts: `5`
- Base retry delay: `3` seconds
- Backoff: `3s`, `6s`, `9s`, `12s`, `15s`

If all retries fail, the task is written to `queue/failed.jsonl` and the local downloaded file is kept for inspection or retry.

## Cancellation

You can cancel a transfer in three ways:

- tap a `🛑 Cancel` button on the transfer message
- run `/cancel` and choose a task from the buttons
- run `/cancel <task_id>`

Behavior:

- Telegram download: stops as soon as possible
- Upload queue: removed immediately
- Rubika upload: stops at the next safe checkpoint

## Storage

Runtime files:

- `downloads/` - temporary downloaded files
- `queue/tasks.jsonl` - pending jobs
- `queue/processing.json` - the job currently being uploaded
- `queue/failed.jsonl` - failed jobs log
- `queue/cancelled/` - cancellation markers
- `queue/settings.json` - active Rubika session setting

Cleanup behavior:

- successful upload: local file is deleted
- canceled task: local file is deleted
- failed upload: local file is kept

## Troubleshooting

If the bot does not start:

- verify `.env`
- install dependencies
- confirm Telegram credentials are valid

If uploads fail:

- check the Rubika session
- if uploading to a channel, confirm the Rubika account can post there
- review `queue/failed.jsonl`
- confirm the file still exists in `downloads/`
- check server memory and swap if the process was killed

## Inspiration

This project started after a few storage/upload experiments:

- I first tried Arvan OSS as the target, but it was too slow.
- Then I tried Google Drive, but it got filtered.
- After that, I found [caffeinexz/Tele2Rub](https://github.com/caffeinexz/Tele2Rub) and used it as the inspiration for trying Rubika instead.
- The name **Walrus** is inspired by the Black Sails series: Captain Flint's ship, Walrus.

Walrus uses a simple queue-based flow:

1. The Telegram bot receives a file in a private chat, or a direct file URL in a text message.
2. The file is downloaded into `downloads/`.
3. A task is added to `queue/tasks.jsonl`.
4. The Rubika worker uploads the file.
5. The Telegram status message is updated during the whole transfer.
