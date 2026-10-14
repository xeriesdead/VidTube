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

### Permanent discussion-post quota

- The bot must be an administrator in VidTube's linked discussion group to
  receive user-authored comment/reply message updates.
- Reactions on the channel post itself are not claim triggers; they do not give
  the bot a reliable user identity for a private claim.
- A user comment or reply to the automatic-forward copy of a VidTube channel post
  in the linked discussion group is eligible. Standalone group messages are not.
- The bot must receive the automatic-forward message to map it to its channel
  post, so test with a post mirrored after the bot is added as a group admin.
- A qualifying comment creates one pending private claim per user/channel post.
  The user must press the
  inline button in the bot's DM to add one permanent quota, once per
  user/channel-post pair.
- If Telegram will not let the bot DM the user because they have not started
  the bot, the claim stays pending and is sent after they use `/start`. The bot
  does not post quota notifications in the discussion group.
- Saved discussion-post quotas are used after the user's current daily quota runs out.
  Forwarding a channel post no longer grants quota; prior rewards remain in the database.

### Telegram Stars quota purchases

- `/buy_quota` sells one permanent quota for 1 Telegram Star (`XTR`).
- The quota is added only after Telegram confirms a successful payment and is
  stored in the existing SQLite database. Payment charge IDs prevent duplicate
  quota credits when Telegram retries an update.
- `/terms` explains the purchase and `/paysupport` sends a help request to the
  configured admins.
- Railway's webhook must deliver `pre_checkout_query` updates. The webhook
  secret is derived from the existing bot token, so no additional environment
  variable is needed.

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

### Premium media access

- Premium users can forward and save media sent through media links, manual broadcasts, and scheduled broadcasts. Media sent to non-Premium users remains protected.
- `/premium` sells 30 days for 10 Telegram Stars. Telegram's successful payment activates it automatically; renewal is not automatic, and a new purchase extends an active term by 30 days.
- `/saved` lists and reopens media saved from bot messages. Saved media is private to the owning account, and using `/saved` does not consume daily quota.
- `/profile` shows Premium status and its expiry. Admins can still grant or revoke manual access with `/set_premium <user_id> on|off`.
- `/buy_quota` remains a separate purchase that adds one permanent quota for 1 Star.
