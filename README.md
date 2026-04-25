# Walrus

Send a video to a Telegram bot and have it uploaded to Rubika Saved Messages or a Rubika channel.

## Disclaimer

This project is shared for research, learning, and personal experimentation.
Do not use it for abuse, spam, unauthorized access, privacy violations, or any harmful or unlawful purpose.
You are responsible for using it in a way that respects platform rules, local laws, and other people's rights.

## Inspiration

This project was originally inspired by [caffeinexz/Tele2Rub](https://github.com/caffeinexz/Tele2Rub).

Walrus uses a simple queue-based flow:

1. The Telegram bot receives a video in a private chat, or a direct video file URL in a text message.
2. The file is downloaded into `downloads/`.
3. A task is added to `queue/tasks.jsonl`.
4. The Rubika worker uploads the file.
5. The Telegram status message is updated during the whole transfer.

## Features

- Accepts video messages in Telegram private chat
- Accepts direct video file links such as `https://...mp4` and `file:///...mp4`
- Accepts multiple direct video file links in one message and queues each one
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
- Lets you switch uploads between Rubika Saved Messages and a Rubika channel
- Uploads videos with their original filename

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
- `/use_saved` - send new uploads to Saved Messages
- `/use_channel <channel_target>` - send new uploads to a Rubika channel
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

Direct link uploads:

- send a message containing a direct video file URL
- you can include multiple direct video URLs in one message
- when a multi-link batch finishes, the bot sends one summary with queued, failed, and cancelled counts
- supported schemes: `https://`, `http://`, and `file://`
- the link should point to the actual video file, not a webpage

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

- `downloads/` - temporary downloaded videos
- `queue/tasks.jsonl` - pending jobs
- `queue/processing.json` - the job currently being uploaded
- `queue/failed.jsonl` - failed jobs log
- `queue/cancelled/` - cancellation markers
- `queue/settings.json` - active Rubika session and destination settings

Cleanup behavior:

- successful upload: local file is deleted
- canceled task: local file is deleted
- failed upload: local file is kept

## Requirements

- Python 3.9+
- Telegram `API_ID`
- Telegram `API_HASH`
- Telegram bot token
- Valid Rubika session support through `rubpy`

---

## 🛠 Installation

```bash
git clone https://github.com/rezaaa/walrus.git
cd walrus
pip install -r requirements.txt
```

## Configuration

Create `.env` in the project root:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_telegram_bot_token
RUBIKA_SESSION=rubsession
OWNER_TELEGRAM_ID=123456789
```

Variables:

- `API_ID` - from https://my.telegram.org
- `API_HASH` - from https://my.telegram.org
- `BOT_TOKEN` - from BotFather
- `RUBIKA_SESSION` - session name or path used by `rubpy`
- `OWNER_TELEGRAM_ID` - optional; if set, only this Telegram user ID can use the bot

Runtime upload settings are stored in `queue/settings.json` after you change them from Telegram.
That lets you switch the active Rubika number/session and destination without editing `.env` or restarting the bot.

Changing the Rubika number now happens inside Telegram:

1. Tap `⚙️ Settings` then `📱 Change Rubika Number`, or run `/set_rubika`
2. Send the phone number
3. Wait for the OTP prompt
4. Send the OTP code

The bot replaces the existing stored Rubika session after a successful login.

How to get your Telegram user ID:

- forward one of your messages to [@userinfobot](https://t.me/userinfobot)
- or message [@RawDataBot](https://t.me/RawDataBot) and use the value in `from.id`

Then put that number into `.env` as `OWNER_TELEGRAM_ID`.
If you leave it unset, the bot stays open for everyone.

## First Run

On the first worker run, Rubika login may ask for:

1. Your phone number
2. The OTP code sent by Rubika

After that, the saved session is reused.

## Run

Start both processes with:

```bash
python3 main.py
```

This starts:

- `telegram_bot.py` - Telegram receiver and downloader
- `rubika_worker.py` - Rubika upload worker

## Run with Screen

```bash
screen -S walrus
cd /opt/walrus
source venv/bin/activate
python main.py
```

Detach without stopping the app:

```text
Ctrl + A, then D
```

Useful commands:

- `screen -ls`
- `screen -r walrus` - attach to the running session
- `screen -S walrus -X quit`

If multiple old sessions exist:

```bash
for s in $(screen -ls | awk '/walrus/ {print $1}'); do
  screen -S "$s" -X quit
done
```

## Update on Server

Typical restart flow:

```bash
cd /opt/walrus
git pull origin main
source venv/bin/activate
pip install -r requirements.txt

for s in $(screen -ls | awk '/walrus/ {print $1}'); do
  screen -S "$s" -X quit
done

screen -dmS walrus bash -lc 'cd /opt/walrus && source venv/bin/activate && python main.py'
```

Then verify:

```bash
screen -ls
```

Optional update script:

```bash
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/walrus"
BRANCH="main"
SCREEN_NAME="walrus"

echo "==> Updating code"
cd "$APP_DIR"
git pull --ff-only origin "$BRANCH"

echo "==> Installing dependencies"
"$APP_DIR/venv/bin/python" -m pip install -r requirements.txt

echo "==> Stopping old screen sessions"
for s in $(screen -ls | awk '/walrus/ {print $1}'); do
  screen -S "$s" -X quit || true
done

echo "==> Starting app in screen"
screen -dmS "$SCREEN_NAME" bash -lc "cd '$APP_DIR' && exec '$APP_DIR/venv/bin/python' main.py"

echo "==> Done"
echo "Check sessions with: screen -ls"
echo "Attach with: screen -r $SCREEN_NAME"
```

How to use it:

```bash
nano /opt/walrus/update.sh
```

Paste the script, save it, then run:

```bash
bash /opt/walrus/update.sh
```

## Troubleshooting

If the bot does not start:

- verify `.env`
- install dependencies
- confirm Telegram credentials are valid

If uploads fail:

- check the Rubika session
- review `queue/failed.jsonl`
- confirm the file still exists in `downloads/`
- check server memory and swap if the process was killed
