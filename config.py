# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL = os.getenv("CHANNEL", "@Vid_Tube")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1004427442070"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "Vid_Tub_BOT")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "8441460682").split(",")]
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

# URL publik tempat bot di-deploy (Railway otomatis isi RAILWAY_PUBLIC_DOMAIN)
# Contoh: https://mybot.up.railway.app
_railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
_replit_domain = os.getenv("REPLIT_DOMAINS", "").split(",")[0].strip()
_manual_domain = os.getenv("WEBHOOK_DOMAIN", "")

if _manual_domain:
    WEBHOOK_URL = _manual_domain.rstrip("/")
elif _railway_domain:
    WEBHOOK_URL = f"https://{_railway_domain}"
elif _replit_domain:
    WEBHOOK_URL = f"https://{_replit_domain}"
else:
    WEBHOOK_URL = "https://yourdomain.com"

WEBHOOK_PATH = "/webhook/telegram"

# Railway's regular filesystem is ephemeral between deployments/restarts.
# When a persistent volume is mounted at /data, prefer it automatically;
# DATABASE_PATH can still override this for local development or another mount.
_persistent_db_dir = "/data" if os.path.isdir("/data") else "."
DATABASE_PATH = os.getenv(
    "DATABASE_PATH",
    os.path.join(_persistent_db_dir, "database.db")
)
BACKUP_CHAT_ID = int(os.getenv("BACKUP_CHAT_ID", "6787385893"))
AUTO_DELETE_TIMEOUT = int(os.getenv("AUTO_DELETE_TIMEOUT", "3600"))  # 1 jam
BATCH_TIMEOUT = 10
BACKUP_INTERVAL = 21600  # 6 jam
BACKUP_DIR = "backups"
MAX_BACKUPS = 10

WHOP_API_KEY = os.getenv("WHOP_API_KEY", "").strip()
WHOP_COMPANY_ID = os.getenv("WHOP_COMPANY_ID", "biz_lVUmWV3nvJp8Xs")
WHOP_API_VERSION_DATE = "2026-09-29"
WHOP_PREMIUM_TIERS = {
    "1d": {
        "plan_id": os.getenv("WHOP_PLAN_1D_ID", "plan_08oadDcUiJGjT"),
        "days": 1,
        "price": 1000,
    },
    "7d": {
        "plan_id": os.getenv("WHOP_PLAN_7D_ID", "plan_uAsdJomxdYUcm"),
        "days": 7,
        "price": 5000,
    },
    "15d": {
        "plan_id": os.getenv("WHOP_PLAN_15D_ID", "plan_ycDB2UwFbn9CK"),
        "days": 15,
        "price": 20000,
    },
}

if not TOKEN:
    raise ValueError("❌ TELEGRAM_BOT_TOKEN not set in .env!")
