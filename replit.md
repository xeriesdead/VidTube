# Telegram Media Sharing Bot

## Overview

Telegram bot for media sharing with admin upload, broadcast, and scheduled broadcast features. Deployed on Railway; source managed on GitHub. Replit is used as a code editor only.

## Stack

- **Language**: Python 3
- **Framework**: FastAPI + uvicorn (webhook mode)
- **Bot library**: python-telegram-bot 20.7
- **Database**: SQLite (with WAL mode, connection pool)
- **Deployment**: Railway (uses `RAILWAY_PUBLIC_DOMAIN` for webhook URL)

## Key Files

- `main.py` — bot logic, FastAPI app, all handlers
- `config.py` — configuration loaded from environment variables
- `requirements.txt` — Python dependencies
- `Procfile` — Railway process definition

## Required Environment Variables

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather (required) |
| `CHANNEL` | Channel username e.g. `@YourChannel` |
| `CHANNEL_ID` | Channel numeric ID |
| `BOT_USERNAME` | Bot username without `@` |
| `ADMIN_IDS` | Comma-separated admin Telegram user IDs |
| `BACKUP_CHAT_ID` | Chat ID to receive DB backups |
| `WEBHOOK_DOMAIN` | Override webhook URL (optional) |

## Running (Railway)

Deployed automatically via GitHub push. Railway sets `RAILWAY_PUBLIC_DOMAIN` which the bot uses to register its webhook.

### Permanent reaction quota

- The bot must be an administrator in the VidTube channel to receive Telegram
  `message_reaction` updates.
- The first reaction by a user on a post adds one permanent quota. Each
  user/channel-post pair is credited once; removing or changing the reaction
  does not revoke or duplicate the credit.
- Saved reaction quotas are used after the user's current daily quota runs out.
  Existing forwarded-post bonuses remain available and keep their daily limit.

### Persistent database storage

SQLite must be stored on a Railway persistent Volume; the normal service filesystem
can be reset when a deployment or instance changes.

- Add a Railway Volume and mount it at `/data`
- Set `DATABASE_PATH=/data/database.db` in Railway Variables
- With an existing database, back it up before attaching or changing the volume,
  then restore the latest Telegram `.db` backup with `/import_db`.
  For backups larger than Telegram's Bot API download limit, compress them as
  `.db.gz` or `.zip`; `/import_db` extracts and validates them automatically.

The bot validates the SQLite file before replacing the active database and uses
SQLite's online backup API for new backups. New backups are gzip-compressed so
they are easier to download again through Telegram's bot file-size limit.

### Backup before a redeploy

- From an admin account, run `/backup_now` on the currently deployed bot before
  pushing a commit that will trigger a Railway deployment.
- The bot sends a current `.db.gz` snapshot to the admin who ran the command and
  to `BACKUP_CHAT_ID`, and reports whether delivery succeeded.
- If the active database is stored on the Railway Volume at `/data`, it persists
  through deployments and normally does not need to be restored. Without a
  Volume, keep the backup and use `/import_db` after deployment if the active
  database has reset.

## User Preferences

- This project runs on Railway + GitHub. Replit is used as a code editor only — do not set up a run workflow or attempt to run the bot here.
- Untuk setiap perubahan pada proyek ini, bantu commit dan push ke GitHub. Jika akses push belum tersedia, jelaskan hambatannya; jangan menyatakan sudah push sebelum berhasil.
