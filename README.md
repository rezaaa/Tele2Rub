# Tele2Rub

Automatically transfer files from a Telegram bot to your Rubika Saved Messages.

This project receives media in Telegram, downloads it locally, places it in a queue, and then uploads it to Rubika through a separate worker process. The queue-based design helps keep transfers stable and avoids overlapping jobs.

## Inspiration

This project was inspired by [caffeinexz/Tele2Rub](https://github.com/caffeinexz/Tele2Rub).

## Features

- Accepts media from a Telegram bot in private chat
- Queues jobs before uploading to Rubika
- Supports document, video, audio, voice, photo, animation, video note, and sticker inputs
- Shows step-by-step status updates in Telegram
- Displays download progress during Telegram download
- Shows upload percentage progress, status updates, and retry attempts for temporary failures
- Supports canceling a job with `/cancel`
- Sends files to Rubika Saved Messages
- Tries the original filename for every upload, including generic files
- Falls back to removing unsupported extensions only when Rubika needs it

## How It Works

The application runs two processes:

- `telebot.py`: receives files from Telegram, downloads them, and appends jobs to the queue
- `rub.py`: reads queued jobs and uploads them to Rubika

Flow overview:

1. A user sends media to the Telegram bot.
2. The bot downloads the file into the local `downloads/` directory.
3. A task is written to `queue/tasks.jsonl`.
4. The Rubika worker picks up the task.
5. The worker uploads the file to Rubika Saved Messages.
6. The status message in Telegram is updated during the process.

## Status Updates

Each transfer gets a Telegram status message that is updated as the task moves through the pipeline.

Current status coverage includes:

- Download preparation
- Download in progress
- Download completed
- Waiting in upload queue
- Upload starting
- Upload progress in percent
- Upload retry attempts for transient errors
- Canceled
- Completed successfully
- Failed after retries

## Retry Policy

Rubika uploads automatically retry on temporary errors.

- Maximum retries: `5`
- Base retry delay: `3` seconds
- Backoff behavior: delay increases by attempt number (`3s`, `6s`, `9s`, ...)

Transient upload errors currently include cases such as:

- `502`
- `bad gateway`
- `timeout`
- `cannot connect`
- `connection reset`
- `temporarily unavailable`
- `error uploading chunk`

If all retry attempts fail, the task is logged to `queue/failed.jsonl`.

## Canceling a Job

You can cancel a transfer in either of these ways:

- Reply to the status message with `/cancel`
- Run `/cancel <task_id>`

Cancellation behavior:

- If the file is still downloading from Telegram, the bot will stop it as soon as possible
- If the job is still queued, it is removed from the queue immediately
- If the worker is already uploading, cancellation is honored at the next safe check point, including between retries

## Temporary Files and Cleanup

Runtime files are created on demand:

- `downloads/`: temporary downloaded files
- `queue/tasks.jsonl`: pending jobs
- `queue/processing.json`: currently active job
- `queue/failed.jsonl`: failed jobs log
- `queue/cancelled/`: cancellation markers

Cleanup behavior:

- Successful upload: temporary file is deleted
- Canceled job: temporary file is deleted
- Failed upload after all retries: temporary file is kept for inspection or manual retry

## Requirements

- Python 3.9+
- A Telegram bot token
- Telegram `API_ID` and `API_HASH`
- A valid Rubika session for `rubpy`

## Installation

Clone the repository:

```bash
git clone https://github.com/rezaaa/Tele2Rub.git
cd Tele2Rub
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_telegram_bot_token
RUBIKA_SESSION=rubsession
```

Variables:

- `API_ID`: Telegram API ID from https://my.telegram.org
- `API_HASH`: Telegram API hash from https://my.telegram.org
- `BOT_TOKEN`: Bot token from BotFather
- `RUBIKA_SESSION`: session name or session path used by `rubpy`

## Setup Steps

Before the bot can work, you need to prepare both Telegram and Rubika access.

### 1. Get Telegram API Credentials

Go to https://my.telegram.org and sign in with your Telegram account.

Then:

1. Open **API Development Tools**
2. Create a new application
3. Copy your `API_ID`
4. Copy your `API_HASH`

Put both values into `.env`.

### 2. Create a Telegram Bot

Open Telegram and talk to **@BotFather**.

Then:

1. Run `/newbot`
2. Choose a bot name
3. Choose a bot username
4. Copy the bot token that BotFather gives you

Put that token into `.env` as `BOT_TOKEN`.

### 3. Prepare the Rubika Session

Set a session name in `.env`, for example:

```env
RUBIKA_SESSION=rubsession
```

On the first run, the Rubika worker will ask for:

1. Your Rubika phone number
2. The OTP / verification code sent to that number

After a successful login, the session is saved locally and reused on the next runs, so you usually only need to enter the phone number and OTP once.

## First Run

On the first run, the Rubika worker may ask you to log in and confirm the session.

Typical first-run flow:

1. Start the project
2. Enter your Rubika phone number when prompted
3. Enter the OTP / verification code
4. Wait for the session file to be created
5. Reuse that saved session on future runs

After that, the session is reused for later runs.

## Run the Project

Start both processes with:

```bash
python3 main.py
```

This launches:

- the Telegram bot process
- the Rubika worker process

## Run with Screen

If you run the project on a server, `screen` is a simple way to keep it running after you disconnect.

Start a new `screen` session:

```bash
screen -S tele2rub
```

Inside the session, start the project:

```bash
cd /opt/Tele2Rub
source venv/bin/activate
python main.py
```

Detach from the session without stopping the bot:

```text
Ctrl + A, then D
```

List active screen sessions:

```bash
screen -ls
```

Reattach to the running bot:

```bash
screen -r tele2rub
```

If `screen` asks for an exact session because multiple `tele2rub` sessions exist, use the full session id from `screen -ls`, for example:

```bash
screen -r 120682.tele2rub
```

Stop a running session:

```bash
screen -S tele2rub -X quit
```

If you have multiple old sessions and want to stop all of them:

```bash
for s in $(screen -ls | awk '/tele2rub/ {print $1}'); do
  screen -S "$s" -X quit
done
```

## Update on a Server with Screen

For a typical server setup using `/opt/Tele2Rub` and a virtual environment:

```bash
cd /opt/Tele2Rub
git pull origin main
source venv/bin/activate
pip install -r requirements.txt

for s in $(screen -ls | awk '/tele2rub/ {print $1}'); do
  screen -S "$s" -X quit
done

screen -dmS tele2rub bash -lc 'cd /opt/Tele2Rub && source venv/bin/activate && python main.py'
```

After restarting, verify that one clean session is running:

```bash
screen -ls
```

## Supported Telegram Inputs

The bot currently accepts these Telegram message types:

- Document
- Video
- Audio
- Voice
- Photo
- Animation
- Video note
- Sticker

## Project Structure

```text
.
├── main.py
├── telebot.py
├── rub.py
├── task_store.py
├── requirements.txt
└── README.md
```

Important files:

- `main.py`: starts both worker processes
- `telebot.py`: Telegram bot logic, download flow, status updates, cancel command
- `rub.py`: Rubika upload worker, retry handling, cleanup
- `task_store.py`: shared queue, processing, failure, and cancellation helpers

## Queue Storage

This project uses simple JSON-based files instead of a database.

- `queue/tasks.jsonl` stores pending jobs
- `queue/processing.json` stores the job currently being processed
- `queue/failed.jsonl` stores failed jobs and their error messages

This makes the system easy to inspect and debug manually.

## Notes

- Files are uploaded to Rubika as documents
- The worker first keeps the original filename for all uploads
- If Rubika rejects an uncommon extension, the worker retries without the extension
- Upload progress is driven by Rubika chunk callbacks and shown as a percent during transfer

## Troubleshooting

If the bot does not start:

- verify the `.env` values
- make sure all dependencies are installed
- confirm your Telegram API credentials are valid

If Rubika uploads fail:

- check that the Rubika session is valid
- review `queue/failed.jsonl`
- look for the downloaded file in `downloads/` if the job failed after retries

## License

No license file is included in this repository yet. Add one if you plan to distribute or open-source the project publicly.
