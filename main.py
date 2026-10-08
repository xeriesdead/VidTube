# -*- coding: utf-8 -*-
"""
Telegram Bot - Webhook Mode
Kompatibel dengan Railway serverless (gratis)
✅ Webhook mode (Telegram push ke server)
✅ /cron endpoint untuk cron-job.org agar backup & schedule tetap jalan
✅ Parallel message processing
✅ Rate limiting
"""

import logging
import logging.handlers
import os
import asyncio
import gzip
import sqlite3
import random
import string
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional, List, Tuple
from html import escape as html_escape

from fastapi import FastAPI, Request, Response
from uvicorn import Config, Server
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton,
    ReplyKeyboardMarkup, BotCommand
)
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.ext import (
    Application, ApplicationHandlerStop, CallbackQueryHandler, CommandHandler,
    MessageHandler, filters
)

from config import (
    TOKEN, CHANNEL, CHANNEL_ID, BOT_USERNAME, ADMIN_IDS,
    HOST, PORT, DATABASE_PATH,
    BACKUP_CHAT_ID, AUTO_DELETE_TIMEOUT, BATCH_TIMEOUT,
    BACKUP_INTERVAL, BACKUP_DIR, MAX_BACKUPS,
    WEBHOOK_URL, WEBHOOK_PATH,
)

# ===== LOGGING =====
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            "logs/bot.log",
            maxBytes=5*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== DATABASE CONNECTION POOL =====

class DatabasePool:
    """Optimized connection pool"""
    def __init__(self, db_path, pool_size=5):
        self.db_path = db_path
        self.pool_size = pool_size
        self.connections = asyncio.Queue(maxsize=pool_size)
        self.write_lock = asyncio.Lock()
        self.read_semaphore = asyncio.Semaphore(pool_size * 2)

    async def init(self):
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_path, timeout=20, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            conn.execute("PRAGMA temp_store=MEMORY")
            await self.connections.put(conn)
        logger.info(f"✅ Database pool initialized with {self.pool_size} connections")

    async def get(self):
        return await self.connections.get()

    async def put(self, conn):
        await self.connections.put(conn)

    async def execute_write(self, query, params=()):
        async with self.write_lock:
            conn = await self.get()
            try:
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                return c.lastrowid
            except Exception as e:
                conn.rollback()
                logger.error(f"DB Write Error: {e}")
                raise
            finally:
                await self.put(conn)

    async def execute_read(self, query, params=(), fetch_one=False):
        async with self.read_semaphore:
            conn = await self.get()
            try:
                c = conn.cursor()
                c.execute(query, params)
                if fetch_one:
                    return c.fetchone()
                return c.fetchall()
            except Exception as e:
                logger.error(f"DB Read Error: {e}")
                return None if fetch_one else []
            finally:
                await self.put(conn)

    async def close(self):
        while not self.connections.empty():
            try:
                conn = self.connections.get_nowait()
                conn.close()
            except:
                pass

db_pool: Optional[DatabasePool] = None
WIB = timezone(timedelta(hours=7))
DAILY_QUOTA = 3
MAX_SHARE_BONUSES_PER_DAY = 10
_daily_quota_notice_task = None

BOT_TEXT = {
    "id": {
        "select_language": "🌐 Pilih bahasa / Select language 👇",
        "language_saved": "✅ Bahasa diatur ke Bahasa Indonesia.",
        "profile_button": "👤 Profil",
        "claim_button": "🎁 Klaim Kuota",
        "language_button": "🌐 Bahasa",
        "help_button": "ℹ️ Bantuan",
        "menu_placeholder": "Pilih menu bot",
        "welcome": (
            "👋 Halo! Saya bot media sharing.\n\n"
            "Untuk membuka media, tekan link yang diberikan admin.\n\n"
            f"📢 Join channel: {CHANNEL}\n"
            "🎁 Gunakan tombol Klaim Kuota untuk kuota harian."
        ),
        "help": (
            "ℹ️ <b>Panduan Bot</b>\n\n"
            "• Buka media melalui link dari admin.\n"
            "• Gunakan <b>Profil</b> untuk melihat kuota.\n"
            "• Tekan <b>Klaim Kuota</b> untuk mengambil 3 kuota harian.\n"
            "• Gunakan <b>Bahasa</b> untuk mengganti bahasa."
        ),
        "join_required": "⚠️ Wajib bergabung ke channel {channel} untuk mengakses media.",
        "join_retry": "🔄 Coba Lagi",
        "preparing": "⏳ Media sedang disiapkan. Coba lagi sebentar.",
        "invalid_link": "❌ Link tidak valid atau sudah kedaluwarsa.",
        "quota_unclaimed": (
            "🎁 <b>Kuota Belum Diklaim</b>\n\n"
            "Tekan tombol Klaim Kuota di keyboard untuk membuka file."
        ),
        "quota_empty": (
            "⛔ <b>Kuota Hari Ini Habis</b>\n\n"
            "Kamu sudah menggunakan semua {total} kuota hari ini. "
            "Kuota berikutnya tersedia setelah pukul 00.00 WIB.\n\n"
            "Buka channel {channel} dan teruskan postingan mana saja ke bot untuk "
            "mendapat +1 kuota. Maksimal {max_bonus} bonus per akun per hari; "
            "setiap postingan bisa memberi bonus sekali sehari."
        ),
        "share_post_button": "📢 Buka Channel VidTube",
        "share_bonus_success": (
            "✅ Bonus +1 kuota berhasil ditambahkan.\n"
            "Kuota tersisa: <b>{remaining}/{total}</b>\n"
            "Bonus hari ini: <b>{bonus_count}/{max_bonus}</b>"
        ),
        "share_bonus_duplicate": "Postingan ini sudah memberi bonus untuk akun Anda hari ini.",
        "share_bonus_daily_limit": "Batas {max_bonus} bonus kuota hari ini sudah tercapai.",
        "share_bonus_error": "Bonus kuota belum bisa ditambahkan. Coba lagi nanti.",
        "share_post_wrong_channel": "Teruskan postingan dari channel resmi {channel}.",
        "share_post_invalid_forward": (
            "Postingan ini tidak bisa diverifikasi. Gunakan fitur Teruskan "
            "langsung dari postingan di channel resmi."
        ),
        "share_post_seed_usage": (
            "Balas pesan forward terbaru dari {channel} dengan perintah "
            "/set_share_post untuk menyinkronkan postingan awal."
        ),
        "share_post_saved": "✅ Postingan terbaru untuk bonus kuota sudah disinkronkan.",
        "claim_success": "✅ 3 kuota berhasil diklaim. Selamat menggunakan!",
        "claim_already": "Kuota hari ini sudah diklaim.",
        "claim_expired": "Tombol klaim sudah kedaluwarsa. Gunakan /quota untuk kuota hari ini.",
        "claim_invalid": "Tombol klaim tidak valid.",
        "claim_not_yours": "Tombol klaim ini bukan milik Anda.",
        "claim_notification_title": "🎁 <b>Kuota Harian Tersedia</b>",
        "claim_notification_body": "Klaim 3 kuota untuk membuka hingga 3 link media hari ini.",
        "claim_notification_button": "🎁 Klaim 3 Kuota",
        "claim_notification_done": (
            "✅ <b>Kuota harian berhasil diklaim.</b>\n\n"
            "Kuota tersisa: <b>{remaining}/{total}</b>"
        ),
        "claim_notification_already": "Kuota hari ini sudah diklaim.",
        "profile_title": "👤 <b>ACCOUNT</b>",
        "profile_identity": "<b>IDENTITAS AKUN</b>",
        "profile_status": "<b>STATUS KUOTA</b>",
        "profile_name": "├ NAMA",
        "profile_username": "├ USERNAME",
        "profile_user_id": "└ USER ID",
        "profile_remaining": "├ TERSISA",
        "profile_used": "├ TERPAKAI HARI INI",
        "profile_claim_status": "└ STATUS",
        "profile_claimed": "Aktif",
        "profile_unclaimed": "Belum diklaim",
        "profile_total_files": "📂 Total file dibuka",
        "profile_reset": "⏰ Kuota harian tersedia mulai 00.00 WIB.",
        "media_expiry": "⌛ {count} media akan terhapus otomatis dalam 1 jam.",
        "expired_title": "📌 <b>File Telah Dihapus Otomatis</b>",
        "expired_body": (
            "File sebelumnya sudah dihapus sesuai pengaturan auto delete.\n\n"
            "Tekan tombol <b>Ambil File Lagi</b> jika ingin membuka ulang file tersebut."
        ),
        "take_again": "📥 Ambil File Lagi",
        "close": "✖ Tutup",
        "notification_invalid": "Notifikasi tidak valid.",
        "notification_not_yours": "Notifikasi ini bukan milik Anda.",
    },
    "en": {
        "select_language": "🌐 Pilih bahasa / Select language 👇",
        "language_saved": "✅ Language set to English.",
        "profile_button": "👤 Profile",
        "claim_button": "🎁 Claim Quota",
        "language_button": "🌐 Language",
        "help_button": "ℹ️ Help",
        "menu_placeholder": "Choose a bot menu",
        "welcome": (
            "👋 Hello! I am a media sharing bot.\n\n"
            "Open media using a link shared by the admin.\n\n"
            f"📢 Join the channel: {CHANNEL}\n"
            "🎁 Use Claim Quota to claim your daily quota."
        ),
        "help": (
            "ℹ️ <b>Bot Guide</b>\n\n"
            "• Open media using a link shared by the admin.\n"
            "• Use <b>Profile</b> to check your quota.\n"
            "• Press <b>Claim Quota</b> to claim 3 daily quotas.\n"
            "• Use <b>Language</b> to change the bot language."
        ),
        "join_required": "⚠️ Join channel {channel} to access this media.",
        "join_retry": "🔄 Try Again",
        "preparing": "⏳ Media is being prepared. Please try again shortly.",
        "invalid_link": "❌ This link is invalid or has expired.",
        "quota_unclaimed": (
            "🎁 <b>Quota Not Claimed</b>\n\n"
            "Press Claim Quota on the keyboard before opening files."
        ),
        "quota_empty": (
            "⛔ <b>Daily Quota Used</b>\n\n"
            "You have used all {total} quotas today. Your next quota is available "
            "after 00:00 WIB.\n\n"
            "Open the {channel} channel and forward any post to the bot for +1 quota. "
            "Up to {max_bonus} bonuses per account per day; each post can award a "
            "bonus once a day."
        ),
        "share_post_button": "📢 Open VidTube Channel",
        "share_bonus_success": (
            "✅ +1 quota added.\n"
            "Remaining quota: <b>{remaining}/{total}</b>\n"
            "Today's bonuses: <b>{bonus_count}/{max_bonus}</b>"
        ),
        "share_bonus_duplicate": "This post has already awarded a bonus to your account today.",
        "share_bonus_daily_limit": "You have reached today's limit of {max_bonus} quota bonuses.",
        "share_bonus_error": "The quota bonus could not be added. Please try again later.",
        "share_post_wrong_channel": "Forward a post from the official {channel} channel.",
        "share_post_invalid_forward": (
            "This post cannot be verified. Use Telegram's Forward action directly "
            "on a post in the official channel."
        ),
        "share_post_seed_usage": (
            "Reply to the latest forwarded {channel} post with /set_share_post "
            "to sync the initial post."
        ),
        "share_post_saved": "✅ The latest post for quota bonuses has been synced.",
        "claim_success": "✅ You claimed 3 quotas. Enjoy!",
        "claim_already": "Today's quota has already been claimed.",
        "claim_expired": "This claim button has expired. Use /quota to check today's quota.",
        "claim_invalid": "This claim button is invalid.",
        "claim_not_yours": "This claim button does not belong to you.",
        "claim_notification_title": "🎁 <b>Daily Quota Available</b>",
        "claim_notification_body": "Claim 3 quotas to open up to 3 media links today.",
        "claim_notification_button": "🎁 Claim 3 Quotas",
        "claim_notification_done": (
            "✅ <b>Daily quota claimed.</b>\n\n"
            "Remaining quota: <b>{remaining}/{total}</b>"
        ),
        "claim_notification_already": "Today's quota has already been claimed.",
        "profile_title": "👤 <b>ACCOUNT</b>",
        "profile_identity": "<b>ACCOUNT DETAILS</b>",
        "profile_status": "<b>QUOTA STATUS</b>",
        "profile_name": "├ NAME",
        "profile_username": "├ USERNAME",
        "profile_user_id": "└ USER ID",
        "profile_remaining": "├ REMAINING",
        "profile_used": "├ USED TODAY",
        "profile_claim_status": "└ STATUS",
        "profile_claimed": "Claimed",
        "profile_unclaimed": "Not claimed",
        "profile_total_files": "📂 Total files opened",
        "profile_reset": "⏰ Daily quota is available from 00:00 WIB.",
        "media_expiry": "⌛ {count} media item(s) will be deleted automatically in 1 hour.",
        "expired_title": "📌 <b>Media Automatically Deleted</b>",
        "expired_body": (
            "The previous media was deleted according to the auto-delete setting.\n\n"
            "Press <b>Get File Again</b> to open it again."
        ),
        "take_again": "📥 Get File Again",
        "close": "✖ Close",
        "notification_invalid": "This notification is invalid.",
        "notification_not_yours": "This notification does not belong to you.",
    },
}

LANGUAGE_BUTTONS = {
    "🇮🇩 Bahasa Indonesia": "id",
    "🇬🇧 English": "en",
}

def tr(language, key, **values):
    language = language if language in BOT_TEXT else "id"
    return BOT_TEXT[language][key].format(**values)

def command_keyboard(language):
    language = language if language in BOT_TEXT else "id"
    text = BOT_TEXT[language]
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(text["profile_button"]), KeyboardButton(text["claim_button"])],
            [KeyboardButton(text["language_button"]), KeyboardButton(text["help_button"])],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder=text["menu_placeholder"],
    )

def language_keyboard():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(label) for label in LANGUAGE_BUTTONS]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

# ===== INIT DATABASE =====

async def init_db():
    conn = await db_pool.get()
    try:
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS media(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                file_id TEXT NOT NULL,
                type TEXT NOT NULL,
                caption TEXT DEFAULT '',
                click INTEGER DEFAULT 0,
                ready INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        media_cols = [row[1] for row in c.execute("PRAGMA table_info(media)").fetchall()]
        media_info = c.execute("PRAGMA table_info(media)").fetchall()
        code_is_primary_key = any(row[1] == "code" and row[5] for row in media_info)
        if code_is_primary_key:
            # Older schemas allowed only one media row per link. Migrate without
            # changing existing rows so one link can now contain an album.
            c.execute("DROP TABLE IF EXISTS media_multi_migration")
            c.execute("""
                CREATE TABLE media_multi_migration(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    caption TEXT DEFAULT '',
                    click INTEGER DEFAULT 0,
                    ready INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            media_copy_columns = [
                column for column in
                ("code", "file_id", "type", "caption", "click", "ready", "created_at")
                if column in media_cols
            ]
            columns_sql = ", ".join(media_copy_columns)
            c.execute(
                f"INSERT INTO media_multi_migration ({columns_sql}) "
                f"SELECT {columns_sql} FROM media"
            )
            c.execute("DROP TABLE media")
            c.execute("ALTER TABLE media_multi_migration RENAME TO media")

        c.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                quota INTEGER NOT NULL DEFAULT 3,
                quota_date TEXT,
                quota_notice_date TEXT,
                language TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS share_post_state(
                channel_id INTEGER PRIMARY KEY,
                latest_message_id INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS share_quota_rewards(
                user_id INTEGER NOT NULL,
                post_id INTEGER NOT NULL,
                reward_date TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(user_id, post_id, reward_date)
            )
        """)
        reward_columns = c.execute(
            "PRAGMA table_info(share_quota_rewards)"
        ).fetchall()
        reward_primary_key = [
            column_name
            for _, column_name in sorted(
                (row[5], row[1]) for row in reward_columns if row[5]
            )
        ]
        if reward_primary_key != ["user_id", "post_id", "reward_date"]:
            # Preserve old claims but let the same post reward the user again
            # on a different day.
            c.execute("DROP TABLE IF EXISTS share_quota_rewards_migration")
            c.execute("""
                CREATE TABLE share_quota_rewards_migration(
                    user_id INTEGER NOT NULL,
                    post_id INTEGER NOT NULL,
                    reward_date TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(user_id, post_id, reward_date)
                )
            """)
            c.execute("""
                INSERT OR IGNORE INTO share_quota_rewards_migration
                    (user_id, post_id, reward_date, created_at)
                SELECT user_id, post_id, reward_date, created_at
                FROM share_quota_rewards
            """)
            c.execute("DROP TABLE share_quota_rewards")
            c.execute(
                "ALTER TABLE share_quota_rewards_migration "
                "RENAME TO share_quota_rewards"
            )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_share_rewards_user_date "
            "ON share_quota_rewards(user_id, reward_date)"
        )

        existing_cols = [row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()]
        if "username" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN username TEXT")
            logger.info("✅ Migrated users table: added username column")
        if "joined_at" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN joined_at TIMESTAMP")
            logger.info("✅ Migrated users table: added joined_at column")
        if "quota" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN quota INTEGER NOT NULL DEFAULT 3")
            logger.info("✅ Migrated users table: added quota column")
        if "quota_date" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN quota_date TEXT")
            logger.info("✅ Migrated users table: added quota_date column")
        if "quota_notice_date" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN quota_notice_date TEXT")
            logger.info("✅ Migrated users table: added quota_notice_date column")
        if "language" not in existing_cols:
            c.execute("ALTER TABLE users ADD COLUMN language TEXT")
            logger.info("✅ Migrated users table: added language column")

        # Give existing accounts their initial daily quota when this feature is
        # first enabled. Later days require the user to claim the notification.
        c.execute(
            "UPDATE users SET quota_date=? WHERE quota_date IS NULL OR quota_date=''",
            (datetime.now(WIB).date().isoformat(),)
        )

        c.execute("""
            CREATE TABLE IF NOT EXISTS backup_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_name TEXT,
                backup_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_size INTEGER
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_broadcasts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                schedule_time TEXT NOT NULL,
                msg_type TEXT NOT NULL,
                file_id TEXT,
                text_content TEXT,
                caption TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS link_access_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        media_cols = [row[1] for row in c.execute("PRAGMA table_info(media)").fetchall()]
        if "created_at" not in media_cols:
            c.execute("ALTER TABLE media ADD COLUMN created_at TIMESTAMP")
            logger.info("✅ Migrated media table: added created_at column")

        c.execute("CREATE INDEX IF NOT EXISTS idx_media_code ON media(code)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_media_ready ON media(ready)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON users(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_link_log_code ON link_access_log(code)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_link_log_user ON link_access_log(user_id)")

        conn.commit()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"DB Init Error: {e}")
        conn.rollback()
    finally:
        await db_pool.put(conn)

# ===== HELPER FUNCTIONS =====

def gen_code(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def save_user(uid, username=None):
    try:
        today = datetime.now(WIB).date().isoformat()
        await db_pool.execute_write(
            "INSERT OR IGNORE INTO users "
            "(user_id, username, quota, quota_date, language) "
            "VALUES (?, ?, ?, ?, NULL)",
            (uid, username, DAILY_QUOTA, today)
        )
    except Exception as e:
        logger.error(f"Error saving user: {e}")

async def get_user_language(uid):
    row = await db_pool.execute_read(
        "SELECT language FROM users WHERE user_id=?",
        (uid,),
        fetch_one=True
    )
    if row and row[0] in BOT_TEXT:
        return row[0]
    return None

async def set_user_language(uid, language):
    if language not in BOT_TEXT:
        return False
    try:
        await db_pool.execute_write(
            "UPDATE users SET language=? WHERE user_id=?",
            (language, uid)
        )
        return True
    except Exception as e:
        logger.error(f"Error setting language for {uid}: {e}")
        return False

async def get_daily_quota(uid, today=None):
    today = today or datetime.now(WIB).date().isoformat()
    row = await db_pool.execute_read(
        "SELECT quota, quota_date FROM users WHERE user_id=?",
        (uid,),
        fetch_one=True
    )
    if not row or row[1] != today:
        return 0, False
    return max(0, int(row[0] or 0)), True

async def claim_daily_quota(uid, today=None):
    today = today or datetime.now(WIB).date().isoformat()
    async with db_pool.write_lock:
        conn = await db_pool.get()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET quota=?, quota_date=? "
                "WHERE user_id=? AND (quota_date IS NULL OR quota_date<>?)",
                (DAILY_QUOTA, today, uid, today)
            )
            claimed = cursor.rowcount == 1
            conn.commit()
            cursor.execute(
                "SELECT quota, quota_date FROM users WHERE user_id=?",
                (uid,)
            )
            row = cursor.fetchone()
            remaining = max(0, int(row[0] or 0)) if row and row[1] == today else 0
            return claimed, remaining
        except Exception:
            conn.rollback()
            raise
        finally:
            await db_pool.put(conn)

async def consume_daily_quota(uid, today):
    """Atomically spend one quota for one successfully prepared media link."""
    async with db_pool.write_lock:
        conn = await db_pool.get()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET quota=quota-1 "
                "WHERE user_id=? AND quota_date=? AND quota>0",
                (uid, today)
            )
            consumed = cursor.rowcount == 1
            conn.commit()
            cursor.execute(
                "SELECT quota FROM users WHERE user_id=? AND quota_date=?",
                (uid, today)
            )
            row = cursor.fetchone()
            remaining = max(0, int(row[0] or 0)) if row else 0
            return consumed, remaining
        except Exception:
            conn.rollback()
            raise
        finally:
            await db_pool.put(conn)

async def get_share_bonus_count(uid, today=None):
    today = today or datetime.now(WIB).date().isoformat()
    row = await db_pool.execute_read(
        "SELECT COUNT(*) FROM share_quota_rewards "
        "WHERE user_id=? AND reward_date=?",
        (uid, today),
        fetch_one=True,
    )
    return int(row[0] or 0) if row else 0

async def get_latest_share_post_id():
    row = await db_pool.execute_read(
        "SELECT latest_message_id FROM share_post_state WHERE channel_id=?",
        (CHANNEL_ID,),
        fetch_one=True,
    )
    return int(row[0]) if row else None

async def save_latest_share_post(post_id):
    await db_pool.execute_write(
        """
        INSERT INTO share_post_state (channel_id, latest_message_id)
        VALUES (?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            latest_message_id=MAX(latest_message_id, excluded.latest_message_id),
            updated_at=CURRENT_TIMESTAMP
        """,
        (CHANNEL_ID, int(post_id)),
    )

def share_post_url(post_id=None):
    channel_handle = CHANNEL.strip().rstrip("/")
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if channel_handle.startswith(prefix):
            channel_handle = channel_handle.removeprefix(prefix).split("/", 1)[0]
            break
    channel_handle = channel_handle.lstrip("@/")
    channel_url = f"https://t.me/{channel_handle}"
    return f"{channel_url}/{int(post_id)}" if post_id is not None else channel_url

def share_post_keyboard(post_id, language):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            tr(language, "share_post_button"),
            url=share_post_url(post_id),
        )
    ]])

def get_forwarded_channel_post(message):
    origin = getattr(message, "forward_origin", None)
    if origin:
        origin_type = getattr(origin, "type", None)
        origin_type = getattr(origin_type, "value", origin_type)
        if origin_type == "channel":
            chat = getattr(origin, "chat", None)
            return (
                getattr(chat, "id", None),
                getattr(origin, "message_id", None),
            )

    chat = getattr(message, "forward_from_chat", None)
    if chat and getattr(chat, "type", None) == "channel":
        return (
            getattr(chat, "id", None),
            getattr(message, "forward_from_message_id", None),
        )
    return None

async def grant_share_quota(uid, post_id, today=None):
    """Grant one bonus per user/post/day, capped at ten rewards per WIB day."""
    today = today or datetime.now(WIB).date().isoformat()
    async with db_pool.write_lock:
        conn = await db_pool.get()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT quota, quota_date FROM users WHERE user_id=?",
                (uid,),
            )
            user_row = cursor.fetchone()
            if not user_row:
                return "user_not_found", 0, DAILY_QUOTA

            current_quota = max(0, int(user_row[0] or 0))
            quota_date = user_row[1]
            cursor.execute(
                "SELECT 1 FROM share_quota_rewards "
                "WHERE user_id=? AND post_id=? AND reward_date=?",
                (uid, post_id, today),
            )
            if cursor.fetchone():
                return "duplicate", current_quota if quota_date == today else 0, DAILY_QUOTA

            cursor.execute(
                "SELECT COUNT(*) FROM share_quota_rewards "
                "WHERE user_id=? AND reward_date=?",
                (uid, today),
            )
            bonus_count = int(cursor.fetchone()[0] or 0)
            if bonus_count >= MAX_SHARE_BONUSES_PER_DAY:
                return "daily_limit", current_quota if quota_date == today else 0, (
                    DAILY_QUOTA + bonus_count
                )

            cursor.execute(
                "INSERT INTO share_quota_rewards (user_id, post_id, reward_date) "
                "VALUES (?, ?, ?)",
                (uid, post_id, today),
            )
            remaining = current_quota + 1 if quota_date == today else DAILY_QUOTA + 1
            cursor.execute(
                "UPDATE users SET quota=?, quota_date=? WHERE user_id=?",
                (remaining, today, uid),
            )
            conn.commit()
            return "success", remaining, DAILY_QUOTA + bonus_count + 1
        except Exception:
            conn.rollback()
            raise
        finally:
            await db_pool.put(conn)

async def refund_daily_quota(uid, today):
    try:
        await db_pool.execute_write(
            "UPDATE users SET quota=MIN(?, quota+1) "
            "WHERE user_id=? AND quota_date=?",
            (DAILY_QUOTA + MAX_SHARE_BONUSES_PER_DAY, uid, today)
        )
    except Exception as e:
        logger.error(f"Error refunding daily quota for {uid}: {e}")

async def save_media(code, file_id, media_type, caption=""):
    try:
        await db_pool.execute_write(
            "INSERT INTO media (code, file_id, type, caption, click, ready) VALUES (?, ?, ?, ?, 0, 0)",
            (code, file_id, media_type, caption)
        )
        return True
    except Exception as e:
        logger.error(f"Error saving media: {e}")
        return False

async def set_ready(code):
    try:
        await db_pool.execute_write(
            "UPDATE media SET ready=1 WHERE code=?",
            (code,)
        )
        logger.info(f"✅ Code {code} marked ready")
    except Exception as e:
        logger.error(f"Error setting ready: {e}")

def is_admin(uid):
    return uid in ADMIN_IDS

async def check_joined(application, uid):
    # Admin selalu lolos force sub — agar bisa test link sendiri
    if is_admin(uid):
        return True
    try:
        member = await application.bot.get_chat_member(CHANNEL, uid)
        # "restricted" = sudah join tapi di-mute/dibatasi admin channel, tetap dihitung member
        return member.status in ["member", "administrator", "creator", "restricted"]
    except Exception as e:
        # Log detail error agar mudah diagnosa (misal: bot bukan admin channel)
        logger.warning(f"check_joined error untuk uid={uid}: {type(e).__name__}: {e}")
        # Jika bot tidak bisa mengecek (bukan admin channel, dll), tolak akses
        # → Pastikan bot sudah dijadikan Administrator di channel {CHANNEL}
        return False

async def get_total_users():
    result = await db_pool.execute_read("SELECT COUNT(*) FROM users", fetch_one=True)
    return result[0] if result else 0

async def get_total_media():
    result = await db_pool.execute_read("SELECT COUNT(*) FROM media", fetch_one=True)
    return result[0] if result else 0

async def get_total_clicks():
    result = await db_pool.execute_read(
        "SELECT SUM(link_clicks) FROM (SELECT MAX(click) AS link_clicks FROM media GROUP BY code)",
        fetch_one=True
    )
    return result[0] if result else 0

async def get_last_backup():
    return await db_pool.execute_read(
        "SELECT backup_name, file_size, backup_time FROM backup_log ORDER BY backup_time DESC LIMIT 1",
        fetch_one=True
    )

async def get_media_by_code(code) -> List[Tuple]:
    return await db_pool.execute_read(
        "SELECT file_id, type, caption FROM media WHERE code=? ORDER BY rowid",
        (code,)
    )

async def increment_clicks(code):
    try:
        await db_pool.execute_write(
            "UPDATE media SET click=click+1 WHERE code=?",
            (code,)
        )
    except Exception as e:
        logger.error(f"Error incrementing clicks: {e}")

async def remove_user(uid):
    """Hapus user dari database (misal: blokir bot atau akun tidak aktif)."""
    try:
        await db_pool.execute_write("DELETE FROM users WHERE user_id=?", (uid,))
        logger.info(f"🗑️ User {uid} dihapus dari database (blokir/nonaktif)")
    except Exception as e:
        logger.error(f"Error removing user {uid}: {e}")

async def log_link_access(code: str, user_id: int, username: str):
    try:
        await db_pool.execute_write(
            "INSERT INTO link_access_log (code, user_id, username) VALUES (?,?,?)",
            (code, user_id, username)
        )
    except Exception as e:
        logger.debug(f"log_link_access error: {e}")

# ===== BATCH & ALBUM MANAGEMENT =====

batch_buffer = {}
batch_timers = {}
album_cache = {}
album_timers = {}
ALBUM_TTL = 300
MAX_MEDIA_PER_LINK = 10

async def cleanup_expired_caches():
    now = datetime.now()
    expired = [gid for gid, data in album_cache.items() if now > data["expire_at"]]
    for gid in expired:
        album_cache.pop(gid, None)
    if expired:
        logger.info(f"🗑️ Cleaned {len(expired)} expired albums")

async def finalize_batch(application, uid, expected_code=None):
    batch = batch_buffer.get(uid)
    if not batch or (expected_code and batch["code"] != expected_code):
        return

    code = batch["code"]
    batch_buffer.pop(uid, None)
    timer = batch_timers.pop(uid, None)
    if timer and timer is not asyncio.current_task():
        timer.cancel()

    try:
        media_count = len(await get_media_by_code(code))
        if not media_count:
            return

        await set_ready(code)
        link = f"https://t.me/{BOT_USERNAME}?start={code}"
        await application.bot.send_message(
            uid,
            f"✅ LINK {'ALBUM' if media_count > 1 else 'MEDIA'} READY\n\n"
            f"🔗 {link}\n\n📌 Total media: {media_count}",
            parse_mode="HTML"
        )
        logger.info(f"✅ Link sent to {uid}: {code}")
    except Exception as e:
        logger.error(f"Error sending link to {uid}: {e}")

# ===== HANDLERS =====

async def show_language_selection(update, context):
    await update.message.reply_text(
        tr("id", "select_language"),
        reply_markup=language_keyboard()
    )

async def language_command(update, context):
    user = update.effective_user
    await save_user(user.id, user.username or "unknown")
    await show_language_selection(update, context)

async def menu_command(update, context):
    user = update.effective_user
    await save_user(user.id, user.username or "unknown")
    language = await get_user_language(user.id) or "id"
    await update.message.reply_text(
        tr(language, "help"),
        parse_mode="HTML",
        reply_markup=command_keyboard(language)
    )

async def claim_quota_command(update, context):
    user = update.effective_user
    await save_user(user.id, user.username or "unknown")
    language = await get_user_language(user.id) or "id"
    today = datetime.now(WIB).date().isoformat()
    claimed, _ = await claim_daily_quota(user.id, today)
    await update.message.reply_text(
        tr(language, "claim_success" if claimed else "claim_already"),
        reply_markup=command_keyboard(language)
    )

async def menu_text_handler(update, context):
    """Route persistent keyboard buttons and language choices."""
    user = update.effective_user
    text = (update.message.text or "").strip()
    await save_user(user.id, user.username or "unknown")

    if text in LANGUAGE_BUTTONS:
        language = LANGUAGE_BUTTONS[text]
        await set_user_language(user.id, language)
        await update.message.reply_text(
            f"{tr(language, 'language_saved')}\n\n{tr(language, 'welcome')}",
            reply_markup=command_keyboard(language)
        )
        return

    action = None
    for language in BOT_TEXT:
        for candidate, key in (
            ("profile", "profile_button"),
            ("claim", "claim_button"),
            ("language", "language_button"),
            ("help", "help_button"),
        ):
            if text == BOT_TEXT[language][key]:
                action = candidate
                break
        if action:
            break

    if action == "profile":
        await profile_command(update, context)
    elif action == "claim":
        await claim_quota_command(update, context)
    elif action == "language":
        await language_command(update, context)
    elif action == "help":
        language = await get_user_language(user.id) or "id"
        await update.message.reply_text(
            tr(language, "help"),
            parse_mode="HTML",
            reply_markup=command_keyboard(language)
        )

async def register_bot_commands(bot):
    command_sets = {
        "id": [
            BotCommand("start", "Mulai bot"),
            BotCommand("profile", "Lihat profil dan kuota"),
            BotCommand("quota", "Lihat kuota harian"),
            BotCommand("claim", "Klaim kuota harian"),
            BotCommand("language", "Ganti bahasa"),
            BotCommand("menu", "Tampilkan menu"),
        ],
        "en": [
            BotCommand("start", "Start the bot"),
            BotCommand("profile", "View profile and quota"),
            BotCommand("quota", "View daily quota"),
            BotCommand("claim", "Claim daily quota"),
            BotCommand("language", "Change language"),
            BotCommand("menu", "Show the menu"),
        ],
    }
    try:
        await bot.set_my_commands(command_sets["id"])
    except Exception as e:
        logger.warning(f"Could not register default bot commands: {e}")

    for language, commands in command_sets.items():
        try:
            await bot.set_my_commands(commands, language_code=language)
        except Exception as e:
            logger.warning(f"Could not register {language} bot commands: {e}")

async def start_command(update, context):
    uid = update.effective_user.id
    username = update.effective_user.username or "unknown"

    await save_user(uid, username)
    language = await get_user_language(uid) or "id"
    logger.info(f"👤 User {username} ({uid}) started bot")

    if not context.args:
        if not await get_user_language(uid):
            await show_language_selection(update, context)
            return
        await update.message.reply_text(
            tr(language, "welcome"),
            reply_markup=command_keyboard(language)
        )
        return

    code = context.args[0]
    logger.info(f"🔗 User {username} accessing code: {code}")

    joined = await check_joined(context.application, uid)
    logger.info(f"🔍 check_joined for {username} ({uid}): {joined}")
    if not joined:
        btn = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 JOIN CHANNEL", url=f"https://t.me/{CHANNEL[1:]}")],
            [InlineKeyboardButton(
                tr(language, "join_retry"),
                url=f"https://t.me/{BOT_USERNAME}?start={code}"
            )]
        ])
        await update.message.reply_text(
            tr(language, "join_required", channel=CHANNEL),
            reply_markup=btn
        )
        return

    ready_result, media_list = await asyncio.gather(
        db_pool.execute_read("SELECT ready FROM media WHERE code=? LIMIT 1", (code,), fetch_one=True),
        get_media_by_code(code),
        return_exceptions=True
    )

    if not ready_result or ready_result[0] == 0:
        await update.message.reply_text(tr(language, "preparing"))
        return

    if not media_list:
        await update.message.reply_text(tr(language, "invalid_link"))
        return

    quota_date = datetime.now(WIB).date().isoformat()
    consumed, _ = await consume_daily_quota(uid, quota_date)
    if not consumed:
        _, claimed_today = await get_daily_quota(uid, quota_date)
        reply_markup = None
        if not claimed_today:
            reply_markup = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    tr(language, "claim_notification_button"),
                    callback_data=f"claim_quota:{quota_date}:{uid}"
                )
            ]])
            message = tr(language, "quota_unclaimed")
        else:
            bonus_count = await get_share_bonus_count(uid, quota_date)
            message = tr(
                language,
                "quota_empty",
                total=DAILY_QUOTA + bonus_count,
                max_bonus=MAX_SHARE_BONUSES_PER_DAY,
                channel=CHANNEL,
            )
            reply_markup = share_post_keyboard(None, language)
        await update.message.reply_text(
            message,
            parse_mode="HTML",
            reply_markup=reply_markup
        )
        return

    context.application.create_task(increment_clicks(code))
    context.application.create_task(log_link_access(code, uid, username))

    sent_ids = []
    send_tasks = []

    for file_id, media_type, caption in media_list:
        task = send_media_item(context.bot, uid, file_id, media_type, caption, sent_ids)
        send_tasks.append(task)

    await asyncio.gather(*send_tasks, return_exceptions=True)

    if not sent_ids:
        await refund_daily_quota(uid, quota_date)

    if sent_ids:
        media_message_ids = list(sent_ids)
        expiry_notice_id = None
        try:
            msg = await update.message.reply_text(
                tr(language, "media_expiry", count=len(sent_ids)),
                reply_markup=command_keyboard(language)
            )
            expiry_notice_id = msg.message_id
        except Exception as e:
            logger.warning(f"Could not send expiry notice to {uid}: {e}")

        async def delete_later():
            try:
                await asyncio.sleep(AUTO_DELETE_TIMEOUT)
                deleted_media_count = 0
                for mid in media_message_ids:
                    try:
                        deleted = await context.bot.delete_message(uid, mid)
                        if deleted:
                            deleted_media_count += 1
                    except Exception as e:
                        logger.warning(
                            f"Could not delete media message {mid} for {uid}: {e}"
                        )

                if expiry_notice_id:
                    try:
                        await context.bot.delete_message(uid, expiry_notice_id)
                    except Exception as e:
                        logger.debug(
                            f"Could not delete expiry notice {expiry_notice_id} "
                            f"for {uid}: {e}"
                        )

                if media_message_ids and deleted_media_count == len(media_message_ids):
                    notification_language = await get_user_language(uid) or "id"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            tr(notification_language, "take_again"),
                            url=f"https://t.me/{BOT_USERNAME}?start={code}"
                        ),
                        InlineKeyboardButton(
                            tr(notification_language, "close"),
                            callback_data=f"expired_close:{uid}"
                        )
                    ]])
                    await context.bot.send_message(
                        uid,
                        f"{tr(notification_language, 'expired_title')}\n\n"
                        f"{tr(notification_language, 'expired_body')}",
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
            except Exception as e:
                logger.error(f"Auto-delete task failed for {uid}: {e}")

        context.application.create_task(delete_later())

async def set_share_post_command(update, context):
    user = update.effective_user
    language = await get_user_language(user.id) or "id"
    if not is_admin(user.id):
        await update.message.reply_text("❌ Admin only")
        return

    forwarded_message = update.message.reply_to_message
    forwarded_post = (
        get_forwarded_channel_post(forwarded_message)
        if forwarded_message else None
    )
    if not forwarded_post:
        await update.message.reply_text(
            tr(language, "share_post_seed_usage", channel=CHANNEL)
        )
        return

    source_chat_id, post_id = forwarded_post
    if source_chat_id != CHANNEL_ID or not post_id:
        await update.message.reply_text(
            tr(language, "share_post_wrong_channel", channel=CHANNEL)
        )
        return

    await save_latest_share_post(post_id)
    latest_post_id = await get_latest_share_post_id()
    await update.message.reply_text(
        tr(language, "share_post_saved"),
        reply_markup=share_post_keyboard(latest_post_id, language),
    )

async def handle_share_forward(update, context):
    message = update.message
    if not message:
        return

    forwarded_post = get_forwarded_channel_post(message)
    if not forwarded_post:
        return

    source_chat_id, post_id = forwarded_post
    user = update.effective_user
    language = await get_user_language(user.id) or "id"

    if source_chat_id != CHANNEL_ID:
        await message.reply_text(
            tr(language, "share_post_wrong_channel", channel=CHANNEL)
        )
        raise ApplicationHandlerStop

    if not post_id:
        await message.reply_text(
            tr(language, "share_post_invalid_forward")
        )
        raise ApplicationHandlerStop

    await save_user(user.id, user.username or "unknown")
    status, remaining, total = await grant_share_quota(user.id, post_id)
    if status == "success":
        bonus_count = await get_share_bonus_count(user.id)
        response = tr(
            language,
            "share_bonus_success",
            remaining=remaining,
            total=total,
            bonus_count=bonus_count,
            max_bonus=MAX_SHARE_BONUSES_PER_DAY,
        )
    elif status == "duplicate":
        response = tr(language, "share_bonus_duplicate")
    elif status == "daily_limit":
        response = tr(
            language,
            "share_bonus_daily_limit",
            max_bonus=MAX_SHARE_BONUSES_PER_DAY,
        )
    else:
        response = tr(language, "share_bonus_error")

    await message.reply_text(response, parse_mode="HTML")
    raise ApplicationHandlerStop

async def build_profile_message(user):
    uid = user.id
    language = await get_user_language(uid) or "id"
    today = datetime.now(WIB).date().isoformat()
    remaining, claimed_today = await get_daily_quota(uid, today)
    bonus_count = await get_share_bonus_count(uid, today)
    total_quota = DAILY_QUOTA + bonus_count
    total_files = await db_pool.execute_read(
        """
        SELECT COUNT(m.id)
        FROM link_access_log l
        JOIN media m ON m.code=l.code
        WHERE l.user_id=?
        """,
        (uid,),
        fetch_one=True
    )
    total_files = total_files[0] if total_files else 0
    used_today = total_quota - remaining if claimed_today else 0

    name = html_escape(user.full_name or "Pengguna")
    username = f"@{html_escape(user.username)}" if user.username else "-"
    claim_status = tr(
        language,
        "profile_claimed" if claimed_today else "profile_unclaimed"
    )
    quota_status = (
        f"{tr(language, 'profile_remaining')}: <b>{remaining}/{total_quota}</b>\n"
        f"{tr(language, 'profile_used')}: <b>{used_today}</b>\n"
        f"{tr(language, 'profile_claim_status')}: <b>{claim_status}</b>\n"
        f"{tr(language, 'profile_reset')}"
    )

    text = (
        f"{tr(language, 'profile_title')}\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"{tr(language, 'profile_identity')}\n"
        f"{tr(language, 'profile_name')}: {name}\n"
        f"{tr(language, 'profile_username')}: {username}\n"
        f"{tr(language, 'profile_user_id')}: <code>{uid}</code>\n\n"
        f"{tr(language, 'profile_status')}\n"
        f"{quota_status}\n\n"
        f"{tr(language, 'profile_total_files')}: <b>{total_files}</b>"
    )

    keyboard_rows = []
    if not claimed_today:
        keyboard_rows.append([InlineKeyboardButton(
            tr(language, "claim_notification_button"),
            callback_data=f"claim_quota:{today}:{uid}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard_rows) if keyboard_rows else None
    return text, reply_markup

async def profile_command(update, context):
    user = update.effective_user
    await save_user(user.id, user.username or "unknown")
    text, reply_markup = await build_profile_message(user)
    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=reply_markup
    )

async def claim_quota_callback(update, context):
    query = update.callback_query
    language = await get_user_language(query.from_user.id) or "id"
    try:
        _, claim_date, owner_id = query.data.split(":", 2)
        owner_id = int(owner_id)
    except (AttributeError, TypeError, ValueError):
        await query.answer(tr(language, "claim_invalid"), show_alert=True)
        return

    if query.from_user.id != owner_id:
        await query.answer(tr(language, "claim_not_yours"), show_alert=True)
        return

    today = datetime.now(WIB).date().isoformat()
    if claim_date != today:
        await query.answer(
            tr(language, "claim_expired"),
            show_alert=True
        )
        return

    await save_user(owner_id, query.from_user.username or "unknown")
    claimed, remaining = await claim_daily_quota(owner_id, today)
    if claimed:
        await query.answer(tr(language, "claim_success"))
    else:
        await query.answer(tr(language, "claim_notification_already"), show_alert=True)

    if query.message:
        if query.message.text and "ACCOUNT" in query.message.text:
            text, reply_markup = await build_profile_message(query.from_user)
        else:
            language = await get_user_language(owner_id) or "id"
            text = tr(
                language,
                "claim_notification_done",
                remaining=remaining,
                total=DAILY_QUOTA,
            )
            reply_markup = None
        try:
            await query.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.debug(f"Could not update quota claim message: {e}")

async def close_expired_notification(update, context):
    query = update.callback_query
    language = await get_user_language(query.from_user.id) or "id"
    try:
        owner_id = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer(tr(language, "notification_invalid"), show_alert=True)
        return

    if query.from_user.id != owner_id:
        await query.answer(tr(language, "notification_not_yours"), show_alert=True)
        return

    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Could not close expired notification for {owner_id}: {e}")

async def send_media_item(bot, uid, file_id, media_type, caption, sent_ids):
    try:
        if media_type == "photo":
            msg = await bot.send_photo(uid, file_id, caption=caption or "")
        elif media_type == "video":
            msg = await bot.send_video(uid, file_id, caption=caption or "")
        else:
            return

        sent_ids.append(msg.message_id)
        logger.info(f"✅ Sent {media_type} to {uid}")
    except Exception as e:
        logger.error(f"Error sending {media_type} to {uid}: {e}")

async def upload_handler(update, context):
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    msg = update.message
    file_id = None
    media_type = None

    if msg.photo:
        file_id = msg.photo[-1].file_id
        media_type = "photo"
    elif msg.video:
        file_id = msg.video.file_id
        media_type = "video"
    else:
        return

    caption = msg.caption or ""
    if uid not in batch_buffer:
        batch_buffer[uid] = {"code": gen_code(), "count": 0}
    batch = batch_buffer[uid]
    code = batch["code"]
    if not await save_media(code, file_id, media_type, caption):
        return
    batch["count"] += 1

    if batch["count"] >= MAX_MEDIA_PER_LINK:
        timer = batch_timers.pop(uid, None)
        if timer:
            timer.cancel()
        await finalize_batch(context.application, uid, expected_code=code)
        return

    if uid in batch_timers:
        batch_timers[uid].cancel()

    async def finalize_after_timeout():
        try:
            await asyncio.sleep(BATCH_TIMEOUT)
            if batch_timers.get(uid) is asyncio.current_task():
                await finalize_batch(
                    context.application, uid, expected_code=code
                )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Finalize error: {e}")

    task = context.application.create_task(finalize_after_timeout())
    batch_timers[uid] = task

# Track active broadcasts per admin
_active_broadcasts: dict = {}

async def broadcast_command(update, context):
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    if _active_broadcasts.get(uid):
        await update.message.reply_text(
            "⚠️ Broadcast sedang berjalan.\n"
            "Kirim /bc_cancel untuk membatalkan."
        )
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Reply ke pesan yang ingin di-broadcast.\n\n"
            "📌 Mendukung semua jenis: teks, foto, video, dokumen, dll."
        )
        return

    msg_to_send = update.message.reply_to_message
    users = await db_pool.execute_read("SELECT user_id FROM users")
    total = len(users)

    if total == 0:
        await update.message.reply_text("ℹ️ Tidak ada user.")
        return

    status_msg = await update.message.reply_text(
        f"📤 <b>Broadcast dimulai...</b>\n"
        f"👥 Target: <b>{total:,}</b> users",
        parse_mode="HTML"
    )

    _active_broadcasts[uid] = True
    sent = 0
    failed_blocked = 0
    failed_other = 0
    loop_start = asyncio.get_event_loop().time()
    last_update_time = loop_start

    try:
        for i, row in enumerate(users):
            user_id = row[0]

            if not _active_broadcasts.get(uid):
                break

            try:
                await asyncio.wait_for(
                    msg_to_send.copy(user_id),
                    timeout=10.0
                )
                sent += 1
                await asyncio.sleep(0.065)

            except asyncio.TimeoutError:
                failed_other += 1

            except RetryAfter as e:
                wait_sec = int(e.retry_after) + 2
                logger.warning(f"FloodWait {wait_sec}s saat broadcast")
                await asyncio.sleep(wait_sec)
                try:
                    await asyncio.wait_for(msg_to_send.copy(user_id), timeout=10.0)
                    sent += 1
                except Exception:
                    failed_other += 1

            except (Forbidden, BadRequest) as e:
                err = str(e).lower()
                if any(x in err for x in ["blocked", "deactivated", "not found", "chat not found", "kicked"]):
                    failed_blocked += 1
                    context.application.create_task(remove_user(user_id))
                else:
                    failed_other += 1

            except Exception as e:
                failed_other += 1
                logger.debug(f"Broadcast error uid={user_id}: {e}")

            now = asyncio.get_event_loop().time()
            if (i + 1) % 50 == 0 or (now - last_update_time) >= 10:
                last_update_time = now
                pct = ((i + 1) / total) * 100
                bar_filled = int(pct / 5)
                bar = "█" * bar_filled + "░" * (20 - bar_filled)
                try:
                    await status_msg.edit_text(
                        f"📤 <b>Broadcasting...</b>\n"
                        f"<code>[{bar}]</code> {pct:.1f}%\n"
                        f"{'━' * 28}\n"
                        f"📊 Proses: {i + 1:,} / {total:,}\n"
                        f"✅ Terkirim: <b>{sent:,}</b>\n"
                        f"🚫 Diblokir: <b>{failed_blocked:,}</b>\n"
                        f"❌ Error lain: <b>{failed_other:,}</b>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

    finally:
        cancelled = not _active_broadcasts.pop(uid, True)
        elapsed = asyncio.get_event_loop().time() - loop_start
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"

        title = "🚫 <b>Broadcast Dibatalkan</b>" if cancelled else "✅ <b>Broadcast Selesai</b>"
        summary = (
            f"{title}\n"
            f"{'━' * 28}\n"
            f"👥 Total Target: <b>{total:,}</b>\n"
            f"✅ Terkirim: <b>{sent:,}</b>\n"
            f"🚫 Diblokir bot: <b>{failed_blocked:,}</b>\n"
            f"❌ Error lain: <b>{failed_other:,}</b>\n"
            f"⏱️ Durasi: <b>{elapsed_str}</b>"
        )
        try:
            await status_msg.edit_text(summary, parse_mode="HTML")
        except Exception:
            try:
                await update.message.reply_text(summary, parse_mode="HTML")
            except Exception:
                pass

async def bc_cancel_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    if uid in _active_broadcasts:
        _active_broadcasts[uid] = False
        await update.message.reply_text("🛑 Membatalkan broadcast, mohon tunggu...")
    else:
        await update.message.reply_text("ℹ️ Tidak ada broadcast aktif saat ini.")

# ===== SCHEDULED BROADCAST =====

async def _save_schedule(admin_id, schedule_time, msg_type, file_id=None, text_content=None, caption=None):
    await db_pool.execute_write(
        "INSERT INTO scheduled_broadcasts (admin_id, schedule_time, msg_type, file_id, text_content, caption) VALUES (?,?,?,?,?,?)",
        (admin_id, schedule_time, msg_type, file_id, text_content, caption)
    )

async def _get_all_schedules():
    return await db_pool.execute_read(
        "SELECT id, admin_id, schedule_time, msg_type, file_id, text_content, caption FROM scheduled_broadcasts ORDER BY schedule_time"
    )

async def _delete_schedule(schedule_id: int):
    await db_pool.execute_write(
        "DELETE FROM scheduled_broadcasts WHERE id=?",
        (schedule_id,)
    )

async def bc_schedule_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan waktu pengiriman.\n\n"
            "📌 Cara pakai:\n"
            "1. Kirim/siapkan pesan yang ingin dijadwalkan\n"
            "2. Reply pesan itu dengan:\n"
            "   <code>/bc_schedule HH:MM</code>\n\n"
            "Contoh: <code>/bc_schedule 08:00</code>\n"
            "Mendukung: teks, foto, video, dokumen, animasi",
            parse_mode="HTML"
        )
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Reply ke pesan yang ingin dijadwalkan.\n\n"
            "Contoh: <code>/bc_schedule 08:00</code>",
            parse_mode="HTML"
        )
        return

    time_str = context.args[0].strip()
    try:
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Out of range")
        schedule_time = f"{hour:02d}:{minute:02d}"
    except Exception:
        await update.message.reply_text(
            "❌ Format waktu salah. Gunakan format 24 jam.\n"
            "Contoh: <code>/bc_schedule 08:00</code>",
            parse_mode="HTML"
        )
        return

    msg = update.message.reply_to_message
    msg_type = file_id = text_content = caption = None

    if msg.text:
        msg_type, text_content = "text", msg.text
    elif msg.photo:
        msg_type = "photo"
        file_id = msg.photo[-1].file_id
        caption = msg.caption or ""
    elif msg.video:
        msg_type = "video"
        file_id = msg.video.file_id
        caption = msg.caption or ""
    elif msg.document:
        msg_type = "document"
        file_id = msg.document.file_id
        caption = msg.caption or ""
    elif msg.animation:
        msg_type = "animation"
        file_id = msg.animation.file_id
        caption = msg.caption or ""
    else:
        await update.message.reply_text(
            "❌ Jenis pesan tidak didukung.\n"
            "Mendukung: teks, foto, video, dokumen, animasi/GIF."
        )
        return

    await _save_schedule(uid, schedule_time, msg_type, file_id, text_content, caption)
    logger.info(f"📅 Admin {uid} scheduled {msg_type} broadcast at {schedule_time}")

    await update.message.reply_text(
        f"✅ <b>Broadcast Dijadwalkan!</b>\n"
        f"{'━' * 28}\n"
        f"🕐 Waktu: <b>{schedule_time}</b> (setiap hari)\n"
        f"📩 Jenis pesan: <b>{msg_type}</b>\n\n"
        f"Lihat semua jadwal: /bc_schedules\n"
        f"Hapus jadwal: /bc_unschedule [id]",
        parse_mode="HTML"
    )

async def bc_schedules_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    rows = await _get_all_schedules()
    if not rows:
        await update.message.reply_text(
            "ℹ️ Tidak ada jadwal broadcast aktif.\n\n"
            "Buat jadwal baru:\n<code>/bc_schedule HH:MM</code>",
            parse_mode="HTML"
        )
        return

    lines = [f"📅 <b>Jadwal Broadcast Aktif ({len(rows)})</b>\n" + "━" * 28]
    for row in rows:
        sched_id, admin_id, sched_time, msg_type, *_ = row
        lines.append(f"• ID <code>{sched_id}</code>  ⏰ <b>{sched_time}</b>  📩 {msg_type}")
    lines.append(f"\n🗑️ Hapus: /bc_unschedule [id]")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def bc_unschedule_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan ID jadwal.\n"
            "Contoh: <code>/bc_unschedule 1</code>\n\n"
            "Lihat daftar ID: /bc_schedules",
            parse_mode="HTML"
        )
        return

    try:
        sched_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID harus berupa angka.")
        return

    await _delete_schedule(sched_id)
    await update.message.reply_text(
        f"✅ Jadwal ID <code>{sched_id}</code> berhasil dihapus.",
        parse_mode="HTML"
    )
    logger.info(f"📅 Admin {uid} deleted schedule ID={sched_id}")

async def _send_one_scheduled(bot, user_id, msg_type, file_id, text_content, caption):
    cap = caption or None
    if msg_type == "text":
        await bot.send_message(user_id, text_content)
    elif msg_type == "photo":
        await bot.send_photo(user_id, file_id, caption=cap)
    elif msg_type == "video":
        await bot.send_video(user_id, file_id, caption=cap)
    elif msg_type == "document":
        await bot.send_document(user_id, file_id, caption=cap)
    elif msg_type == "animation":
        await bot.send_animation(user_id, file_id, caption=cap)

async def _execute_scheduled_broadcast(row):
    sched_id, admin_id, schedule_time, msg_type, file_id, text_content, caption = row

    users = await db_pool.execute_read("SELECT user_id FROM users")
    total = len(users)
    if total == 0:
        return

    logger.info(f"📅 Scheduled broadcast ID={sched_id} ({schedule_time}) → {total} users")
    sent = failed_blocked = failed_other = 0

    for row_user in users:
        user_id = row_user[0]
        try:
            await asyncio.wait_for(
                _send_one_scheduled(application.bot, user_id, msg_type, file_id, text_content, caption),
                timeout=10.0
            )
            sent += 1
            await asyncio.sleep(0.065)

        except asyncio.TimeoutError:
            failed_other += 1

        except RetryAfter as e:
            wait_sec = int(e.retry_after) + 2
            logger.warning(f"FloodWait {wait_sec}s dalam scheduled broadcast")
            await asyncio.sleep(wait_sec)
            try:
                await asyncio.wait_for(
                    _send_one_scheduled(application.bot, user_id, msg_type, file_id, text_content, caption),
                    timeout=10.0
                )
                sent += 1
            except Exception:
                failed_other += 1

        except (Forbidden, BadRequest) as e:
            err = str(e).lower()
            if any(x in err for x in ["blocked", "deactivated", "not found", "chat not found", "kicked"]):
                failed_blocked += 1
            else:
                failed_other += 1

        except Exception as e:
            failed_other += 1
            logger.debug(f"Scheduled broadcast error uid={user_id}: {e}")

    summary = (
        f"📅 <b>Broadcast Terjadwal Selesai</b>\n"
        f"{'━' * 28}\n"
        f"⏰ Jadwal: <b>{schedule_time}</b>\n"
        f"📩 Jenis: <b>{msg_type}</b>\n"
        f"👥 Total Target: <b>{total:,}</b>\n"
        f"✅ Terkirim: <b>{sent:,}</b>\n"
        f"🚫 Diblokir: <b>{failed_blocked:,}</b>\n"
        f"❌ Error: <b>{failed_other:,}</b>"
    )
    await notify_admin(application.bot, summary)
    logger.info(f"📅 Scheduled ID={sched_id} done — sent={sent} blocked={failed_blocked} error={failed_other}")

# ===== SCHEDULE RUNNER (dipanggil oleh cron tick) =====

_schedule_fired_today: set = set()

async def run_schedule_tick():
    """Cek & jalankan jadwal yang waktunya tiba. Dipanggil dari /cron endpoint."""
    global _schedule_fired_today
    try:
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        today_str = now.strftime("%Y-%m-%d")

        schedules = await _get_all_schedules()
        for row in schedules:
            sched_id = row[0]
            sched_time = row[2]
            fire_key = (sched_id, today_str)

            if sched_time == current_time and fire_key not in _schedule_fired_today:
                _schedule_fired_today.add(fire_key)
                asyncio.create_task(_execute_scheduled_broadcast(row))
                logger.info(f"📅 Fired schedule ID={sched_id} at {current_time}")

        # Bersihkan catatan kemarin
        _schedule_fired_today = {(sid, d) for sid, d in _schedule_fired_today if d == today_str}

    except Exception as e:
        logger.error(f"Schedule tick error: {e}")

async def notify_daily_quota_claims(bot):
    """Kirim tombol klaim kuota pada hari WIB baru melalui cron."""
    today = datetime.now(WIB).date().isoformat()
    try:
        users = await db_pool.execute_read(
            "SELECT user_id FROM users "
            "WHERE (quota_date IS NULL OR quota_date<>?) "
            "AND (quota_notice_date IS NULL OR quota_notice_date<>?) "
            "ORDER BY user_id",
            (today, today)
        )

        for (user_id,) in users:
            _, claimed_today = await get_daily_quota(user_id, today)
            if claimed_today:
                await db_pool.execute_write(
                    "UPDATE users SET quota_notice_date=? WHERE user_id=?",
                    (today, user_id)
                )
                continue

            language = await get_user_language(user_id) or "id"
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    tr(language, "claim_notification_button"),
                    callback_data=f"claim_quota:{today}:{user_id}"
                )
            ]])
            notification = (
                f"{tr(language, 'claim_notification_title')}\n\n"
                f"{tr(language, 'claim_notification_body')}"
            )
            try:
                await bot.send_message(
                    user_id,
                    notification,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)
                try:
                    await bot.send_message(
                        user_id,
                        notification,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                except (Forbidden, BadRequest) as retry_error:
                    logger.info(
                        f"Could not notify quota claim for {user_id}: {retry_error}"
                    )
                    await db_pool.execute_write(
                        "UPDATE users SET quota_notice_date=? WHERE user_id=?",
                        (today, user_id)
                    )
                    continue
                except Exception as retry_error:
                    logger.warning(
                        f"Quota claim notification retry failed for {user_id}: "
                        f"{retry_error}"
                    )
                    continue
            except (Forbidden, BadRequest) as e:
                logger.info(f"Could not notify quota claim for {user_id}: {e}")
                await db_pool.execute_write(
                    "UPDATE users SET quota_notice_date=? WHERE user_id=?",
                    (today, user_id)
                )
                continue
            except Exception as e:
                logger.warning(f"Quota claim notification failed for {user_id}: {e}")
                continue

            await db_pool.execute_write(
                "UPDATE users SET quota_notice_date=? WHERE user_id=?",
                (today, user_id)
            )
            await asyncio.sleep(0.05)

        if users:
            logger.info(
                f"🎁 Sent daily quota claim notifications for {len(users)} pending users"
            )
    except Exception as e:
        logger.error(f"Daily quota notification task failed: {e}", exc_info=True)

# ===== BACKUP =====

async def notify_admin(bot, message: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Failed to notify admin {admin_id}: {e}")

async def send_backup_to_admin(
    application, backup_path, backup_name, file_size, chat_id=None
):
    destination_chat_id = BACKUP_CHAT_ID if chat_id is None else chat_id
    try:
        with open(backup_path, "rb") as f:
            await application.bot.send_document(
                destination_chat_id,
                f,
                caption=f"📦 Database Backup\n⏰ {backup_name}\n💾 Size: {file_size / (1024*1024):.2f} MB"
            )
        logger.info(f"✅ Backup sent to chat {destination_chat_id}")
        return True
    except Exception as e:
        logger.error(f"Error sending backup: {e}")
        return False

async def create_backup(application, additional_chat_id=None):
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)

        if not os.path.isfile(DATABASE_PATH):
            raise FileNotFoundError("Active database file does not exist")

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        raw_backup_name = f"database_{ts}.db"
        raw_backup_path = os.path.join(BACKUP_DIR, raw_backup_name)

        # Use SQLite's online backup API so the snapshot includes committed
        # WAL data and remains consistent while the bot is running.
        source_conn = sqlite3.connect(DATABASE_PATH, timeout=20)
        backup_conn = sqlite3.connect(raw_backup_path)
        try:
            source_conn.backup(backup_conn)
        finally:
            backup_conn.close()
            source_conn.close()

        # Compressed backups can be restored with /import_db.
        backup_name = f"{raw_backup_name}.gz"
        backup_path = os.path.join(BACKUP_DIR, backup_name)
        with open(raw_backup_path, "rb") as source_file, gzip.open(
            backup_path, "wb", compresslevel=6
        ) as compressed_file:
            shutil.copyfileobj(source_file, compressed_file)
        os.remove(raw_backup_path)
        file_size = os.path.getsize(backup_path)

        logger.info(f"📦 Backup created: {backup_path} ({file_size/(1024*1024):.2f} MB)")

        try:
            await db_pool.execute_write(
                "INSERT INTO backup_log (backup_name, file_size) VALUES (?, ?)",
                (backup_name, file_size)
            )
        except Exception as e:
            logger.error(f"Error logging backup: {e}")

        recipient_ids = [BACKUP_CHAT_ID]
        if additional_chat_id is not None:
            recipient_ids.append(additional_chat_id)
        recipient_ids = list(dict.fromkeys(recipient_ids))

        delivered_to = []
        failed_to = []
        for chat_id in recipient_ids:
            sent = await send_backup_to_admin(
                application, backup_path, backup_name, file_size, chat_id
            )
            (delivered_to if sent else failed_to).append(chat_id)

        try:
            files = sorted(os.listdir(BACKUP_DIR))
            if len(files) > MAX_BACKUPS:
                for old_file in files[:-MAX_BACKUPS]:
                    try:
                        os.remove(os.path.join(BACKUP_DIR, old_file))
                        logger.info(f"🗑️ Old backup deleted: {old_file}")
                    except:
                        pass
        except Exception as e:
            logger.error(f"Error cleaning backups: {e}")

        return {
            "backup_name": backup_name,
            "file_size": file_size,
            "delivered_to": delivered_to,
            "failed_to": failed_to,
        }
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return None

# State untuk cron backup
_last_backup_time: Optional[datetime] = None
_backup_done_on_start: bool = False

async def maybe_run_backup():
    """Jalankan backup jika sudah waktunya. Dipanggil dari /cron endpoint."""
    global _last_backup_time, _backup_done_on_start

    now = datetime.now()

    # Backup pertama: 5 menit setelah server pertama kali start
    if not _backup_done_on_start:
        if bot_start_time and (now - bot_start_time).total_seconds() >= 300:
            _backup_done_on_start = True
            _last_backup_time = now
            logger.info("📦 Cron: menjalankan backup pertama (5 menit setelah start)")
            asyncio.create_task(create_backup(application))
        return

    # Backup berikutnya: setiap BACKUP_INTERVAL detik
    if _last_backup_time is None or (now - _last_backup_time).total_seconds() >= BACKUP_INTERVAL:
        _last_backup_time = now
        logger.info("📦 Cron: menjalankan backup terjadwal")
        asyncio.create_task(create_backup(application))

async def backup_now_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    msg = await update.message.reply_text("⏳ Membuat backup database, harap tunggu...")
    result = await create_backup(context.application, additional_chat_id=uid)
    if not result:
        await msg.edit_text(
            "❌ Backup database gagal dibuat. Database aktif tidak ditemukan "
            "atau snapshot tidak dapat disiapkan."
        )
        return

    if uid not in result["delivered_to"]:
        if result["delivered_to"]:
            await msg.edit_text(
                "❌ Snapshot database dibuat, tetapi tidak dapat dikirim ke chat "
                "ini. Salinan berhasil dikirim ke chat backup yang dikonfigurasi."
            )
        else:
            await msg.edit_text(
                "❌ Snapshot database dibuat, tetapi gagal dikirim ke Telegram. "
                "Periksa koneksi bot dan coba lagi."
            )
        return

    message = (
        "✅ Backup database terbaru sudah dikirim ke chat ini.\n"
        f"📄 File: <code>{result['backup_name']}</code>\n"
        "Simpan file ini sebelum redeploy. Jika database aktif hilang setelah "
        "deploy, kirim file ini ke bot dengan caption <code>/import_db</code>."
    )
    if BACKUP_CHAT_ID != uid and BACKUP_CHAT_ID not in result["delivered_to"]:
        message += "\n⚠️ Salinan ke chat backup yang dikonfigurasi gagal dikirim."
    await msg.edit_text(message, parse_mode="HTML")

def unpack_database_upload(upload_path, file_name, destination_path):
    """Unpack .db/.db.gz/.zip upload into a SQLite database file."""
    lower_name = file_name.lower()

    if lower_name.endswith(".db.gz"):
        with gzip.open(upload_path, "rb") as source_file, open(
            destination_path, "wb"
        ) as destination_file:
            shutil.copyfileobj(source_file, destination_file)
        return

    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(upload_path) as archive:
            database_names = [
                name for name in archive.namelist()
                if name.lower().endswith(".db") and not name.endswith("/")
            ]
            if len(database_names) != 1:
                raise ValueError("ZIP harus berisi tepat satu file .db")
            with archive.open(database_names[0], "r") as source_file, open(
                destination_path, "wb"
            ) as destination_file:
                shutil.copyfileobj(source_file, destination_file)
        return

    shutil.move(upload_path, destination_path)


async def import_db_command(update, context):
    """Handle /import_db — restore database dari file .db/.gz/.zip"""
    uid = update.effective_user.id
    msg = update.message

    if not is_admin(uid):
        logger.warning(f"Unauthorized /import_db attempt from uid={uid}")
        await msg.reply_text(
            "❌ Perintah ini khusus admin.\n"
            "Gunakan /id untuk melihat ID Telegram akun ini, lalu "
            "pastikan ID tersebut ada di ADMIN_IDS Railway."
        )
        return

    # Cek apakah ada file yang di-reply atau dikirim bersamaan
    doc = None
    if msg.document:
        doc = msg.document
    elif msg.reply_to_message and msg.reply_to_message.document:
        doc = msg.reply_to_message.document

    if not doc:
        await msg.reply_text(
            "📥 <b>Import Database</b>\n\n"
            "Cara pakai:\n"
            "1. Kirim file <code>.db</code>, <code>.db.gz</code>, atau "
            "<code>.zip</code> ke bot\n"
            "2. Sambil kirim file, tulis caption: <code>/import_db</code>\n\n"
            "Atau:\n"
            "1. Reply ke file database yang sudah ada\n"
            "2. Ketik <code>/import_db</code>\n\n"
            "💡 Untuk file besar, kompres menjadi <code>.db.gz</code> atau "
            "<code>.db.zip</code> sebelum dikirim.\n\n"
            "⚠️ Database lama akan diganti permanen!",
            parse_mode="HTML"
        )
        return

    file_name = (doc.file_name or "").strip()
    lower_file_name = file_name.lower()
    supported_extensions = (".db", ".db.gz", ".zip")
    if not lower_file_name.endswith(supported_extensions):
        await msg.reply_text(
            "❌ File harus berekstensi <code>.db</code>, "
            "<code>.db.gz</code>, atau <code>.zip</code>",
            parse_mode="HTML"
        )
        return

    status = await msg.reply_text(
        "⏳ Mengunduh dan menyiapkan database...\n"
        "Arsip akan diekstrak dan divalidasi sebelum database aktif diganti."
    )

    upload_path = DATABASE_PATH + ".import_upload"
    tmp_path = DATABASE_PATH + ".import_tmp"
    try:
        # Download file dari Telegram
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(upload_path)
        unpack_database_upload(upload_path, file_name, tmp_path)

        # Validasi: pastikan file adalah SQLite database yang valid
        import sqlite3 as _sqlite3
        try:
            test_conn = _sqlite3.connect(tmp_path)
            integrity = test_conn.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise ValueError("integrity_check gagal")
            tables = {
                row[0]
                for row in test_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "media" not in tables or "users" not in tables:
                raise ValueError("tabel media/users tidak ditemukan")
            test_conn.close()
        except Exception as validation_error:
            try:
                test_conn.close()
            except Exception:
                pass
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            await status.edit_text("❌ File bukan database SQLite yang valid!")
            logger.warning(f"Database import ditolak: {validation_error}")
            return

        # Backup database lama dulu
        if os.path.exists(DATABASE_PATH):
            old_backup = DATABASE_PATH + ".before_import"
            shutil.copy2(DATABASE_PATH, old_backup)
            logger.info(f"📦 Database lama dibackup ke {old_backup}")

        # Tutup semua koneksi pool, ganti database, buka ulang
        await db_pool.close()
        shutil.move(tmp_path, DATABASE_PATH)
        await db_pool.init()
        await init_db()

        file_size = os.path.getsize(DATABASE_PATH)
        total_users = await get_total_users()
        total_media = await get_total_media()

        logger.info(f"✅ Database imported: {file_name} ({file_size/1024:.1f} KB)")
        await status.edit_text(
            f"✅ <b>Database berhasil diimport!</b>\n\n"
            f"📄 File: <code>{file_name}</code>\n"
            f"💾 Ukuran: {file_size/1024:.1f} KB\n"
            f"👥 Users: {total_users:,}\n"
            f"📹 Media: {total_media:,}",
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"import_db_command error: {e}", exc_info=True)
        error_text = str(e)
        if "file is too big" in error_text.lower():
            error_text = (
                "Telegram menolak unduhan karena file masih melebihi batas Bot API. "
                "Kirim versi .db.gz atau .zip yang lebih kecil."
            )
        await status.edit_text(f"❌ Gagal import database: {error_text}")
    finally:
        for temporary_path in (upload_path, tmp_path):
            try:
                if os.path.exists(temporary_path):
                    os.remove(temporary_path)
            except Exception:
                pass

async def id_command(update, context):
    """Tampilkan ID Telegram untuk memeriksa konfigurasi ADMIN_IDS."""
    uid = update.effective_user.id
    admin_status = "✅ Terdaftar sebagai admin" if is_admin(uid) else "❌ Bukan admin"
    await update.message.reply_text(
        f"🆔 Telegram ID: <code>{uid}</code>\n"
        f"🔐 Status: {admin_status}",
        parse_mode="HTML"
    )

async def cache_cleanup_task():
    logger.info("🗑️ Cache cleanup task started")
    while True:
        try:
            await asyncio.sleep(60)
            await cleanup_expired_caches()
        except asyncio.CancelledError:
            logger.info("🗑️ Cache cleanup task stopped")
            break
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")

# ===== STATS / LINK COMMANDS =====

async def link_stats_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Sertakan kode link.\n\n"
            "Contoh: <code>/link_stats qCQoSj7HcV</code>",
            parse_mode="HTML"
        )
        return

    code = context.args[0].strip()

    media_info, total_clicks_row, unique_users_row, first_access_row, last_access_row, recent_users = \
        await asyncio.gather(
            db_pool.execute_read(
                "SELECT type, caption, created_at FROM media WHERE code=? LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT click FROM media WHERE code=?",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT COUNT(DISTINCT user_id) FROM link_access_log WHERE code=?",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT accessed_at FROM link_access_log WHERE code=? ORDER BY accessed_at ASC LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT accessed_at FROM link_access_log WHERE code=? ORDER BY accessed_at DESC LIMIT 1",
                (code,), fetch_one=True
            ),
            db_pool.execute_read(
                "SELECT user_id, username, accessed_at FROM link_access_log "
                "WHERE code=? ORDER BY accessed_at DESC LIMIT 10",
                (code,)
            ),
            return_exceptions=True
        )

    if not media_info:
        await update.message.reply_text(
            f"❌ Kode <code>{code}</code> tidak ditemukan.",
            parse_mode="HTML"
        )
        return

    media_type  = media_info[0] if media_info else "?"
    caption     = (media_info[1] or "")[:40] or "-"
    created_at  = media_info[2] if media_info else "-"
    total_clicks = total_clicks_row[0] if total_clicks_row else 0
    unique_users = unique_users_row[0] if unique_users_row and not isinstance(unique_users_row, Exception) else 0
    first_access = first_access_row[0] if first_access_row and not isinstance(first_access_row, Exception) else "-"
    last_access  = last_access_row[0]  if last_access_row  and not isinstance(last_access_row,  Exception) else "-"

    lines = [
        f"🔗 <b>Link Stats</b>: <code>{code}</code>",
        f"{'━' * 28}",
        f"📩 Jenis: <b>{media_type}</b>",
        f"📝 Caption: {caption}",
        f"📅 Dibuat: {str(created_at)[:16]}",
        f"{'━' * 28}",
        f"👆 Total Klik: <b>{total_clicks:,}</b>",
        f"👥 User Unik: <b>{unique_users:,}</b>",
        f"🕐 Pertama Diakses: {str(first_access)[:16]}",
        f"🕐 Terakhir Diakses: {str(last_access)[:16]}",
    ]

    if recent_users and not isinstance(recent_users, Exception) and len(recent_users) > 0:
        lines.append(f"{'━' * 28}")
        lines.append(f"👤 <b>10 Akses Terakhir:</b>")
        for row in recent_users:
            r_uid, r_uname, r_time = row
            display = f"@{r_uname}" if r_uname and r_uname != "unknown" else f"id:{r_uid}"
            lines.append(f"  • {display}  <i>{str(r_time)[:16]}</i>")
    else:
        lines.append(f"{'━' * 28}")
        lines.append("ℹ️ Belum ada log akses tercatat.")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def top_links_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    top_rows, total_media = await asyncio.gather(
        db_pool.execute_read(
            """
            SELECT m.code, m.type,
                   CAST(m.click AS INTEGER) AS click,
                   m.caption,
                   COUNT(DISTINCT l.user_id) AS unique_users
            FROM media m
            LEFT JOIN link_access_log l ON l.code = m.code
            WHERE m.ready = 1
            GROUP BY m.code
            ORDER BY CAST(m.click AS INTEGER) DESC
            LIMIT 10
            """
        ),
        get_total_media(),
        return_exceptions=True
    )

    if not top_rows or isinstance(top_rows, Exception) or len(top_rows) == 0:
        await update.message.reply_text("ℹ️ Belum ada data link.")
        return

    lines = [
        f"🏆 <b>Top 10 Link Terpopuler</b>",
        f"📊 Dari total <b>{total_media:,}</b> link",
        f"{'━' * 28}",
    ]

    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(top_rows):
        code, media_type, clicks, caption, unique_users = row
        medal = medals[i] if i < 3 else f"{i + 1}."
        cap_short = (caption or "")[:25].strip()
        cap_display = f" — {cap_short}" if cap_short else ""
        try:
            clicks_int = int(clicks or 0)
        except (ValueError, TypeError):
            clicks_int = 0
        try:
            unique_int = int(unique_users or 0)
        except (ValueError, TypeError):
            unique_int = 0
        lines.append(
            f"{medal} <code>{code}</code> <i>({media_type})</i>{cap_display}\n"
            f"    👆 {clicks_int:,} klik  👥 {unique_int:,} user unik"
        )

    lines.append(f"{'━' * 28}")
    lines.append("Detail: /link_stats [kode]")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def _get_db_size() -> str:
    try:
        size = os.path.getsize(DATABASE_PATH)
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        else:
            return f"{size / (1024 * 1024):.2f} MB"
    except Exception:
        return "N/A"

async def _get_today_users() -> int:
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        result = await db_pool.execute_read(
            "SELECT COUNT(*) FROM users WHERE joined_at >= ?",
            (today,),
            fetch_one=True
        )
        return result[0] if result else 0
    except Exception:
        return 0

async def status_command(update, context):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    now = datetime.now()

    if bot_start_time:
        delta = now - bot_start_time
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
    else:
        uptime_str = "Unknown"

    total_users, total_media, db_size, today_users = await asyncio.gather(
        get_total_users(),
        get_total_media(),
        _get_db_size(),
        _get_today_users(),
        return_exceptions=True
    )

    webhook_info = ""
    try:
        wh = await application.bot.get_webhook_info()
        if wh.url:
            webhook_info = f"🔗 Webhook: <code>{wh.url[:50]}...</code>\n"
        else:
            webhook_info = "⚠️ Webhook belum di-set!\n"
    except Exception:
        pass

    status_text = (
        "🖥️ <b>BOT STATUS</b>\n"
        f"{'━' * 28}\n"
        f"🤖 Bot: @{BOT_USERNAME}\n"
        f"🟢 Mode: Webhook\n"
        f"{webhook_info}"
        f"⏱️ Uptime: <code>{uptime_str}</code>\n"
        f"🕐 Waktu: <code>{now.strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{'━' * 28}\n"
        f"👥 Total Users: <b>{total_users:,}</b>\n"
        f"📅 Aktif Hari Ini: <b>{today_users:,}</b>\n"
        f"📄 Total Media: <b>{total_media:,}</b>\n"
        f"💾 Database Size: <b>{db_size}</b>\n"
        f"{'━' * 28}\n"
        f"📢 Channel: {CHANNEL}\n"
    )

    await update.message.reply_text(status_text, parse_mode="HTML")

async def stats_command(update, context):
    uid = update.effective_user.id

    if not is_admin(uid):
        await update.message.reply_text("❌ Admin only")
        return

    total_users, total_media, total_clicks, last_backup = await asyncio.gather(
        get_total_users(),
        get_total_media(),
        get_total_clicks(),
        get_last_backup(),
        return_exceptions=True
    )

    stats_text = (
        "📊 BOT STATISTICS\n\n"
        f"👥 Total Users: {total_users:,}\n"
        f"📄 Total Media: {total_media:,}\n"
        f"🔗 Total Clicks: {total_clicks:,}\n\n"
    )

    if last_backup:
        backup_name, file_size, backup_time = last_backup
        stats_text += (
            f"📦 Last Backup:\n"
            f"  💾 {backup_name}\n"
            f"  📊 Size: {file_size / (1024*1024):.2f} MB\n"
            f"  ⏰ {backup_time}\n"
        )
    else:
        stats_text += "📦 No backups yet\n"

    await update.message.reply_text(stats_text)

async def error_handler(update, context):
    logger.error(f"❌ Error: {context.error}", exc_info=True)

# ===== FASTAPI APP =====

application = None
background_tasks = []
bot_start_time: datetime = None

@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    global application, db_pool, background_tasks, bot_start_time
    bot_start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("🤖 TELEGRAM BOT STARTING (WEBHOOK MODE)")
    logger.info("=" * 70)

    try:
        database_dir = os.path.dirname(os.path.abspath(DATABASE_PATH))
        os.makedirs(database_dir, exist_ok=True)
        logger.info(f"📁 Database path: {os.path.abspath(DATABASE_PATH)}")

        db_pool = DatabasePool(DATABASE_PATH, pool_size=5)
        await db_pool.init()

        await init_db()

        application = (
            Application.builder()
            .token(TOKEN)
            .updater(None)  # Disable built-in polling updater — kita pakai webhook
            .build()
        )
        application.add_error_handler(error_handler)

        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(
            MessageHandler(filters.ALL, handle_share_forward),
            group=-1,
        )
        application.add_handler(CallbackQueryHandler(
            close_expired_notification,
            pattern=r"^expired_close:\d+$"
        ))
        application.add_handler(CallbackQueryHandler(
            claim_quota_callback,
            pattern=r"^claim_quota:\d{4}-\d{2}-\d{2}:\d+$"
        ))
        application.add_handler(CommandHandler("profile", profile_command))
        application.add_handler(CommandHandler("quota", profile_command))
        application.add_handler(CommandHandler("claim", claim_quota_command))
        application.add_handler(
            CommandHandler("set_share_post", set_share_post_command)
        )
        application.add_handler(CommandHandler("language", language_command))
        application.add_handler(CommandHandler("menu", menu_command))
        application.add_handler(CommandHandler("bc", broadcast_command))
        application.add_handler(CommandHandler("bc_cancel", bc_cancel_command))
        application.add_handler(CommandHandler("bc_schedule", bc_schedule_command))
        application.add_handler(CommandHandler("bc_schedules", bc_schedules_command))
        application.add_handler(CommandHandler("bc_unschedule", bc_unschedule_command))
        application.add_handler(CommandHandler("link_stats", link_stats_command))
        application.add_handler(CommandHandler("top_links", top_links_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("backup_now", backup_now_command))
        application.add_handler(CommandHandler("import_db", import_db_command))
        application.add_handler(CommandHandler("id", id_command))
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            menu_text_handler
        ))
        # Process database files sent directly, including forwarded backups
        # with a /import_db caption.
        application.add_handler(MessageHandler(filters.Document.ALL, import_db_command))
        application.add_handler(MessageHandler(filters.PHOTO, upload_handler))
        application.add_handler(MessageHandler(filters.VIDEO, upload_handler))

        await application.initialize()
        await application.start()
        bot_commands_task = asyncio.create_task(
            register_bot_commands(application.bot)
        )

        cleanup_task_obj = asyncio.create_task(cache_cleanup_task())

        # Set webhook dan notify admin di background agar server langsung siap
        # terima request dari Telegram tanpa delay cold start
        # HANYA jalankan set_webhook jika ini berjalan di Railway (ada RAILWAY_PUBLIC_DOMAIN)
        # Jika di Replit, skip agar tidak override webhook Railway
        IS_RAILWAY = bool(os.getenv("RAILWAY_PUBLIC_DOMAIN", ""))

        async def setup_webhook_background():
            if not IS_RAILWAY:
                logger.info("ℹ️ Bukan Railway — skip set_webhook (hindari konflik dengan Railway)")
                return

            full_webhook_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
            allowed_updates = ["message", "callback_query", "channel_post"]
            try:
                wh = await application.bot.get_webhook_info()
                if wh.url != full_webhook_url:
                    await application.bot.set_webhook(
                        url=full_webhook_url,
                        allowed_updates=allowed_updates,
                        drop_pending_updates=True,
                    )
                    logger.info(f"✅ Webhook set: {full_webhook_url}")
                    await notify_admin(
                        application.bot,
                        f"✅ <b>Bot Online (Webhook Mode)</b>\n"
                        f"🤖 @{BOT_USERNAME}\n"
                        f"🔗 {full_webhook_url}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        f"📌 Pastikan cron-job.org sudah hit <code>/cron</code> setiap 5 menit."
                    )
                elif (
                    wh.allowed_updates is not None
                    and not set(allowed_updates).issubset(wh.allowed_updates)
                ):
                    await application.bot.set_webhook(
                        url=full_webhook_url,
                        allowed_updates=allowed_updates,
                        drop_pending_updates=False,
                    )
                    logger.info("✅ Webhook update types synced without dropping pending updates")
                else:
                    logger.info(f"✅ Webhook sudah benar: {full_webhook_url}")
            except Exception as e:
                logger.error(f"❌ setup_webhook_background error: {e}")

        webhook_setup_task = asyncio.create_task(setup_webhook_background())
        background_tasks = [
            cleanup_task_obj,
            bot_commands_task,
            webhook_setup_task,
        ]

        logger.info("=" * 70)
        logger.info("🟢 BOT IS RUNNING (WEBHOOK MODE) — server siap terima request")
        logger.info("=" * 70)

        yield

    except Exception as e:
        logger.error(f"❌ Startup error: {e}", exc_info=True)
        raise
    finally:
        logger.info("🛑 Shutting down bot...")

        for task in background_tasks:
            try:
                task.cancel()
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception:
                pass

        try:
            if application:
                # JANGAN delete_webhook() — webhook harus tetap aktif agar
                # Railway instance baru langsung bisa terima pesan dari Telegram
                await application.stop()
                await application.shutdown()
        except Exception:
            pass

        try:
            if db_pool:
                await db_pool.close()
        except Exception:
            pass

        logger.info("✅ Bot shutdown complete")


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Endpoint yang dipanggil Telegram setiap ada update baru."""
    try:
        data = await request.json()
        channel_post = data.get("channel_post")
        if channel_post:
            channel_chat = channel_post.get("chat", {})
            if (
                int(channel_chat.get("id", 0)) == CHANNEL_ID
                and channel_post.get("message_id")
            ):
                await save_latest_share_post(channel_post["message_id"])
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
        return Response(content="ok", status_code=200)
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return Response(content="error", status_code=500)


@fastapi_app.get("/cron")
async def cron_endpoint():
    """
    Endpoint untuk cron-job.org — hit setiap 5 menit.
    Menjalankan: cek backup, schedule broadcast, dan klaim kuota harian WIB.
    """
    global _daily_quota_notice_task
    try:
        if application and (
            _daily_quota_notice_task is None or _daily_quota_notice_task.done()
        ):
            _daily_quota_notice_task = application.create_task(
                notify_daily_quota_claims(application.bot)
            )

        await asyncio.gather(
            maybe_run_backup(),
            run_schedule_tick(),
        )
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "message": "Cron tasks executed"
        }
    except Exception as e:
        logger.error(f"Cron error: {e}")
        return {"status": "error", "error": str(e)}


@fastapi_app.get("/health")
async def health():
    try:
        total_users = await get_total_users()
        wh_url = ""
        try:
            wh = await application.bot.get_webhook_info()
            wh_url = wh.url or "not set"
        except Exception:
            pass
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "bot": BOT_USERNAME,
            "users": total_users,
            "mode": "webhook",
            "webhook_url": wh_url,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@fastapi_app.get("/")
async def root():
    return {
        "name": "Telegram Media Bot",
        "status": "running",
        "bot": BOT_USERNAME,
        "mode": "webhook",
        "version": "4.0-webhook"
    }


# ===== MAIN =====

def main():
    if not TOKEN:
        logger.error("❌ TOKEN not set in .env!")
        return

    logger.info(f"🚀 Bot Configuration (WEBHOOK MODE):")
    logger.info(f"   Bot: @{BOT_USERNAME}")
    logger.info(f"   Channel: {CHANNEL}")
    logger.info(f"   Webhook URL: {WEBHOOK_URL}{WEBHOOK_PATH}")
    logger.info(f"   Port: {PORT}")

    config = Config(
        app=fastapi_app,
        host=HOST,
        port=PORT,
        log_level="warning",
        workers=1,
        timeout_keep_alive=120,
        loop="asyncio",
    )

    server = Server(config)
    asyncio.run(server.serve())

if __name__ == "__main__":
    import time
    import signal

    _exit_requested = False

    def _handle_sigterm(signum, frame):
        global _exit_requested
        _exit_requested = True
        logger.info("🛑 SIGTERM diterima — bot akan berhenti bersih")

    signal.signal(signal.SIGTERM, _handle_sigterm)

    restart_delay = 10
    while not _exit_requested:
        try:
            main()
            if _exit_requested:
                logger.info("🛑 Bot berhenti karena SIGTERM")
                break
            logger.info(f"🔄 Bot keluar tak terduga, restart dalam {restart_delay}s...")
            time.sleep(restart_delay)
        except KeyboardInterrupt:
            logger.info("⚠️ Bot dihentikan oleh pengguna (Ctrl+C)")
            break
        except Exception as e:
            logger.error(f"❌ Fatal crash: {e}", exc_info=True)
            logger.info(f"🔄 Restart dalam {restart_delay}s...")
            time.sleep(restart_delay)
