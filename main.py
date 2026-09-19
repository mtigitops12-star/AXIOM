import asyncio
import json
import os
import html
import hashlib
import secrets
import uuid
import time
import re
import base64
import sqlite3
import socket
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import httpx
import logging
import psutil

try:
    import telebot
    from telebot.async_telebot import AsyncTeleBot
    from telebot import types
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    print("WARNING: Please install pyTelegramBotAPI to enable the Telegram Bot: pip install pyTelegramBotAPI")

log_queue = deque(maxlen=150)

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("IMMINENT-Gateway")

q_handler = QueueHandler()
q_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(q_handler)
logging.getLogger("uvicorn.error").addHandler(q_handler)
logging.getLogger("uvicorn.access").addHandler(q_handler)

app = FastAPI(title="IMMINENT Panel · RGB Neon", docs_url=None, redoc_url=None)

# Bump this on every release so the dashboard can notify already-open sessions
# that a new version is available / was just applied.
PANEL_VERSION = "3.0.0-neon"

# GitHub repo checked for update notifications
GITHUB_REPO = "luffy-sh-op/LUFFY_PANEL"

async def check_github_latest(force: bool = False) -> dict:
    """Fetches the latest release tag from GitHub, caches in SQLite.
    Only actually calls the API if force=True or no cached data exists."""
    conn = get_db()
    try:
        cur = conn.execute("SELECT latest_tag, latest_url, checked_at FROM github_cache WHERE id = 1")
        row = cur.fetchone()
    finally:
        conn.close()

    now = time.time()
    cached_tag = row["latest_tag"] if row else None
    cached_url = row["latest_url"] if row else None
    cached_at = row["checked_at"] if row else 0

    if not force and cached_tag and (now - cached_at) < 60:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    global http_client
    if http_client is None:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    new_tag = cached_tag
    new_url = cached_url
    try:
        r = await http_client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github+json"},
        )
        if r.status_code == 200:
            data = r.json()
            new_tag = data.get("tag_name") or data.get("name")
            new_url = data.get("html_url")
        else:
            r2 = await http_client.get(f"https://api.github.com/repos/{GITHUB_REPO}/commits/main")
            if r2.status_code == 200:
                data2 = r2.json()
                sha = data2.get("sha") or ""
                new_tag = sha[:7] if sha else cached_tag
                new_url = f"https://github.com/{GITHUB_REPO}/commit/{sha}" if sha else cached_url
    except Exception as e:
        logger.warning(f"GitHub version check failed: {e}")

    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO github_cache (id, latest_tag, latest_url, checked_at) VALUES (1, ?, ?, ?)",
                     (new_tag, new_url, now))
        conn.commit()
    finally:
        conn.close()

    # Create notification if a new version is detected
    if new_tag and new_tag != cached_tag and cached_tag:
        await create_notification(
            type="update",
            title=f"New version: {new_tag}",
            message=f"Panel version {cached_tag} → {new_tag} is available on GitHub.",
            link=new_url,
        )

    return {"tag": new_tag, "url": new_url, "checked_at": now}


async def github_check_loop():
    """Background task: check GitHub every 60 seconds for new releases."""
    await asyncio.sleep(10)  # initial delay
    while True:
        try:
            await check_github_latest(force=True)
        except Exception as e:
            logger.warning(f"GitHub periodic check error: {e}")
        await asyncio.sleep(60)


# ── Notifications ────────────────────────────────────────────────────────

async def create_notification(type: str, title: str, message: str, link: str | None = None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO notifications (type, title, message, link, created_at) VALUES (?, ?, ?, ?, ?)",
            (type, title, message, link, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating notification: {e}")
    finally:
        conn.close()

async def get_unread_notification_count() -> int:
    conn = get_db()
    try:
        cur = conn.execute("SELECT COUNT(*) as cnt FROM notifications WHERE seen = 0")
        row = cur.fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()

async def get_notifications(limit: int = 50) -> list:
    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, type, title, message, link, seen, created_at FROM notifications ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def _get_or_create_secret() -> str:
    """Returns a stable secret key across restarts.

    Previously this fell back to secrets.token_urlsafe(32) on every process
    start when SECRET_KEY wasn't set, which changed the key each restart.
    Since password hashes are salted with this secret, that made the stored
    admin password hash (and every changed password) unverifiable after any
    restart, effectively locking everyone out. We now persist a generated
    secret to a local file so it stays constant across restarts.
    """
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret
    secret_file = "/data/secret.key" if os.path.isdir("/data") else "secret.key"
    try:
        if os.path.exists(secret_file):
            with open(secret_file, "r", encoding="utf-8") as f:
                existing = f.read().strip()
                if existing:
                    return existing
    except Exception:
        pass
    new_secret = secrets.token_urlsafe(32)
    try:
        with open(secret_file, "w", encoding="utf-8") as f:
            f.write(new_secret)
    except Exception as e:
        logger.warning(f"Could not persist secret.key, sessions/passwords will reset on restart: {e}")
    return new_secret

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": _get_or_create_secret(),
    "telegram_token": "",
    "telegram_admin_id": "",
    "bot_lang": "en",
    "railway_token": "",
    "notify_connections": "0",
}

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/client", StaticFiles(directory="client"), name="client")

connections: dict = {}
connections_lock = asyncio.Lock()
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
daily_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = []
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

notified_uids = set()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000
# پورت همیشه ثابت روی 443 است — دیگه قابل تغییر توسط کاربر نیست
DEFAULT_PORT = 443
MIN_PORT, MAX_PORT = 1, 65535

# نوع پروتکل (auth scheme) و ترابرد به‌صورت دو بُعد جدا از هم هستن؛ کاربر برای هر
# کانفیگ هرکدوم رو مستقل از اون یکی انتخاب می‌کنه (مثلاً Trojan + XHTTP stream-up
# یا VLESS + WebSocket و ...). مقدار ذخیره‌شده‌ی نهایی همیشه "{auth}-{transport}"ه.
AUTH_TYPES = ("vless", "trojan")
DEFAULT_AUTH = "vless"

TRANSPORTS = ("ws", "xhttp-packet-up", "xhttp-stream-up")
DEFAULT_TRANSPORT = "ws"

PROTOCOLS = tuple(f"{a}-{t}" for a in AUTH_TYPES for t in TRANSPORTS)
DEFAULT_PROTOCOL = f"{DEFAULT_AUTH}-{DEFAULT_TRANSPORT}"

def split_protocol(protocol: str) -> tuple[str, str]:
    """مقدار ذخیره‌شده‌ی protocol ("auth-transport") رو به دو بخش auth/transport می‌شکونه."""
    protocol = normalize_protocol(protocol)
    auth, transport = protocol.split("-", 1)
    return auth, transport

def normalize_protocol(value: str | None) -> str:
    """قدیم‌ترها مقدار protocol فقط ترابرد بود (مثلاً 'xhttp-packet-up' بدون
    پیشوند auth) چون auth همیشه vless بود. این تابع مقادیر قدیمی رو به فرمت
    جدید 'auth-transport' تبدیل می‌کنه تا کانفیگ‌های قبلی خراب نشن."""
    value = (value or "").strip().lower()
    if value in PROTOCOLS:
        return value
    if value in TRANSPORTS:  # legacy value با auth ضمنی vless
        return f"vless-{value}"
    return DEFAULT_PROTOCOL

# Fingerprint (uTLS) های قابل انتخاب برای هر کانفیگ — مستقل برای هر پروتکل انتخاب می‌شه
FINGERPRINTS = ("chrome", "firefox", "safari", "ios", "android", "edge", "360", "qq", "random", "randomized")
DEFAULT_FINGERPRINT = "chrome"

# لیست بسته‌ی ALPNهای قابل‌انتخاب (دیگه فیلد آزاد نیست) — مستقل برای هر پروتکل انتخاب می‌شه
ALPN_OPTIONS = ("h3", "h2", "http/1.1", "h3,h2,http/1.1", "h3,h2", "h2,http/1.1")

# پیش‌فرض ALPN بر اساس نوع ترابرد، وقتی کاربر مقدار انتخاب نکرده (auth روی این تاثیری نداره)
DEFAULT_ALPN_BY_PROTOCOL = {}
for _auth in AUTH_TYPES:
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-ws"] = "http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-packet-up"] = "h2,http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-stream-up"] = "h2,http/1.1"
del _auth

# ═══════════════════ ساختار «variants» — هر لینک می‌تونه هم‌زمان هم VLESS هم Trojan ═══════════════════
# هر لینک به‌جای یک protocol واحد، یک variant مستقل برای هر auth type داره:
#   link["variants"] = {
#       "vless":  {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#       "trojan": {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#   }
# حداقل یکی از دو تا باید enabled باشه.

def default_variants() -> dict:
    return {
        "vless": {"enabled": True, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["vless-ws"]},
        "trojan": {"enabled": False, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["trojan-ws"]},
    }

def sanitize_variant(v: dict | None, auth: str) -> dict:
    v = v or {}
    transport = str(v.get("transport") or DEFAULT_TRANSPORT).strip().lower()
    if transport not in TRANSPORTS:
        transport = DEFAULT_TRANSPORT
    fp = str(v.get("fingerprint") or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT
    alpn = str(v.get("alpn") or "").strip()
    if alpn not in ALPN_OPTIONS:
        alpn = DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1")
    return {"enabled": bool(v.get("enabled", False)), "transport": transport, "fingerprint": fp, "alpn": alpn}

def sanitize_variants(variants: dict | None) -> dict:
    variants = variants or {}
    result = {auth: sanitize_variant(variants.get(auth), auth) for auth in AUTH_TYPES}
    if not any(result[a]["enabled"] for a in AUTH_TYPES):
        result["vless"]["enabled"] = True  # حداقل یکی باید فعال بمونه
    return result

def variants_from_legacy(protocol: str, fingerprint: str, alpn: str) -> dict:
    """کانفیگ‌های قدیمی که فقط یک protocol/fingerprint/alpn ستونی داشتن رو به فرمت جدید تبدیل می‌کنه."""
    auth, transport = split_protocol(protocol)
    variants = default_variants()
    for a in AUTH_TYPES:
        variants[a]["enabled"] = False
    variants[auth] = {
        "enabled": True, "transport": transport,
        "fingerprint": fingerprint or DEFAULT_FINGERPRINT,
        "alpn": alpn or DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1"),
    }
    return variants

def variants_to_legacy(variants: dict) -> tuple[str, str, str]:
    """برای پرشدن ستون‌های قدیمی protocol/fingerprint/alpn (صرفاً برای سازگاری با ابزارهای بیرونی)."""
    for auth in AUTH_TYPES:
        v = (variants or {}).get(auth, {})
        if v.get("enabled"):
            return f"{auth}-{v.get('transport', DEFAULT_TRANSPORT)}", v.get("fingerprint", DEFAULT_FINGERPRINT), v.get("alpn", "")
    return DEFAULT_PROTOCOL, DEFAULT_FINGERPRINT, ""

def variants_from_body(body: dict, base: dict | None = None) -> dict:
    """بدنه‌ی JSON درخواست (فیلدهای vless_enabled/vless_transport/... و trojan_*) رو
    به ساختار variants تبدیل می‌کنه. base مقادیر پیش‌فرض/موجود رو برای فیلدهایی که
    توی body نیومدن فراهم می‌کنه (برای PATCH جزئی)."""
    base = base or default_variants()
    result = {}
    for auth in AUTH_TYPES:
        cur = dict(base.get(auth, {}))
        if f"{auth}_enabled" in body:
            cur["enabled"] = bool(body.get(f"{auth}_enabled"))
        if f"{auth}_transport" in body:
            cur["transport"] = body.get(f"{auth}_transport")
        if f"{auth}_fingerprint" in body:
            cur["fingerprint"] = body.get(f"{auth}_fingerprint")
        if f"{auth}_alpn" in body:
            cur["alpn"] = body.get(f"{auth}_alpn")
        result[auth] = cur
    return sanitize_variants(result)

DB_FILE = "/data/panel.db" if os.path.isdir("/data") else "panel.db"
if os.path.isdir("/data"):
    logger.warning(f"[STARTUP] Persistent volume detected at /data -> using {DB_FILE} (data survives restarts/deploys)")
else:
    logger.warning(f"[STARTUP] NO persistent volume found at /data -> using EPHEMERAL {DB_FILE} (ALL links/data will be LOST on next restart/deploy!)")
DB_LOCK = asyncio.Lock()
bot = None
bot_polling_task: asyncio.Task | None = None

BOT_I18N = {
    "en": {
        "btn_stats": "📊 Stats",
        "btn_users": "👥 Users",
        "btn_top": "🔝 Top Users",
        "btn_create": "➕ Create User",
        "btn_addip": "🌐 Add Clean IP",
        "btn_lang": "فارسی",
        "welcome": "👑 <b>Welcome to IMMINENT Panel Telegram Bot!</b>\nManage your VLESS inbounds directly from your Telegram.",
        "lang_switched": "🌐 Language switched to <b>English</b>.",
        "stats": (
            "<b>📊 Server Status Dashboard</b>\n\n"
            "🌐 <b>Domain:</b> <code>{domain}</code>\n"
            "🔋 <b>CPU:</b> <code>{cpu:.1f}%</code>\n"
            "💾 <b>Memory:</b> <code>{mem:.1f}%</code>\n"
            "⏱ <b>Uptime:</b> <code>{uptime}</code>\n"
            "👥 <b>Active Connections:</b> <code>{active}</code>\n"
            "📈 <b>Total Traffic:</b> <code>{traffic} MB</code>\n"
            "🔑 <b>Total Inbounds:</b> <code>{links}</code>"
        ),
        "users_title": "<b>👥 Users List & Usage:</b>\n",
        "users_line": "• <b>{label}</b>: {used} / {limit} (⌛ {exp}) | {status}",
        "no_inbounds": "No inbounds found.",
        "status_on": "🟢 On",
        "status_off": "🔴 Off",
        "top_title": "<b>🔝 Top 5 Users by Usage:</b>\n",
        "top_line": "{i}. <b>{label}</b>: Used {used} of {limit}",
        "create_format": (
            "❌ <b>Invalid format.</b>\n"
            "Format: <code>/create [name] [limit_GB] [days]</code>\n"
            "Example: <code>/create Ali 15 30</code>"
        ),
        "create_bad_name": "❌ <b>Name must contain only English letters and numbers.</b>",
        "create_bad_limit": "❌ <b>Traffic limit must be a number.</b>",
        "create_bad_days": "❌ <b>Days valid must be an integer.</b>",
        "create_exists": "❌ <b>An inbound with the name '{label}' already exists.</b>",
        "create_success": (
            "✅ <b>Inbound Created Successfully!</b>\n\n"
            "👤 <b>Name:</b> <code>{label}</code>\n"
            "📊 <b>Quota:</b> <code>{quota}</code>\n"
            "⌛ <b>Expiry:</b> <code>{expiry}</code>\n\n"
            "🔗 <b>VLESS Link:</b>\n<code>{vless}</code>\n\n"
            "🌐 <b>Subscription URL:</b>\n<code>{sub}</code>"
        ),
        "unlimited": "Unlimited",
        "days_fmt": "{days} days",
        "addaddr_format": "❌ Format: <code>/addaddr [ip_or_domain]</code>",
        "addaddr_invalid": "❌ Invalid address format.",
        "addaddr_exists": "⚠️ Address '{addr}' is already in the list.",
        "addaddr_success": "✅ Clean IP/Domain <code>{addr}</code> successfully added.",
        "toggle_format": "❌ Format: <code>/{action} [username]</code>",
        "not_found": "❌ User '{name}' not found.",
        "toggle_success": "✅ User <code>{name}</code> successfully <b>{state}</b>.",
        "state_enabled": "Enabled",
        "state_disabled": "Disabled",
        "reset_format": "❌ Format: <code>/reset [username]</code>",
        "reset_success": "🔄 Usage reset to 0 for user <code>{name}</code>.",
        "create_guide": (
            "➕ <b>How to create a user:</b>\n\n"
            "Use the <code>/create</code> command. Format:\n"
            "<code>/create [name] [limit_GB] [days]</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/create Ali 15 30</code> (15GB limit, 30 days validity)\n"
            "• <code>/create Reza 0 0</code> (Unlimited, No Expiry)"
        ),
        "addip_guide": (
            "🌐 <b>How to add Clean IP:</b>\n\n"
            "Use the <code>/addaddr</code> command. Format:\n"
            "<code>/addaddr [ip_or_domain]</code>\n\n"
            "<b>Example:</b>\n"
            "• <code>/addaddr cf.example.com</code>\n"
            "• <code>/addaddr 1.1.1.1</code>"
        ),
        "quota_alert": (
            "⚠️ <b>Quota Alert!</b>\n"
            "User: <code>{label}</code> has reached their limit.\n"
            "Usage: <code>{used} / {limit}</code>"
        ),
        "expiry_alert": (
            "⏰ <b>Expiry Alert!</b>\n"
            "User: <code>{label}</code> has expired.\n"
            "Expiry date: <code>{exp}</code>"
        ),
    },
    "fa": {
        "btn_stats": "📊 آمار",
        "btn_users": "👥 کاربران",
        "btn_top": "🔝 پرمصرف‌ترین‌ها",
        "btn_create": "➕ ساخت کاربر",
        "btn_addip": "🌐 افزودن آی‌پی تمیز",
        "btn_lang": "English",
        "welcome": "👑 <b>به ربات تلگرامی پنل لافی خوش اومدی!</b>\nاینباندهای VLESS رو مستقیم از تلگرام مدیریت کن.",
        "lang_switched": "🌐 زبان به <b>فارسی</b> تغییر یافت.",
        "stats": (
            "<b>📊 وضعیت سرور</b>\n\n"
            "🌐 <b>دامنه:</b> <code>{domain}</code>\n"
            "🔋 <b>پردازنده:</b> <code>{cpu:.1f}%</code>\n"
            "💾 <b>رم:</b> <code>{mem:.1f}%</code>\n"
            "⏱ <b>آپ‌تایم:</b> <code>{uptime}</code>\n"
            "👥 <b>اتصالات فعال:</b> <code>{active}</code>\n"
            "📈 <b>ترافیک کل:</b> <code>{traffic} MB</code>\n"
            "🔑 <b>تعداد کاربران:</b> <code>{links}</code>"
        ),
        "users_title": "<b>👥 لیست کاربران و میزان مصرف:</b>\n",
        "users_line": "• <b>{label}</b>: {used} / {limit} (⌛ {exp}) | {status}",
        "no_inbounds": "هیچ کاربری یافت نشد.",
        "status_on": "🟢 فعال",
        "status_off": "🔴 غیرفعال",
        "top_title": "<b>🔝 ۵ کاربر پرمصرف:</b>\n",
        "top_line": "{i}. <b>{label}</b>: مصرف {used} از {limit}",
        "create_format": (
            "❌ <b>فرمت اشتباه است.</b>\n"
            "فرمت: <code>/create [نام] [حجم_GB] [روز]</code>\n"
            "مثال: <code>/create Ali 15 30</code>"
        ),
        "create_bad_name": "❌ <b>نام فقط باید شامل حروف انگلیسی و عدد باشد.</b>",
        "create_bad_limit": "❌ <b>حجم ترافیک باید عدد باشد.</b>",
        "create_bad_days": "❌ <b>تعداد روز باید عدد صحیح باشد.</b>",
        "create_exists": "❌ <b>کاربری با نام «{label}» از قبل وجود دارد.</b>",
        "create_success": (
            "✅ <b>کاربر با موفقیت ساخته شد!</b>\n\n"
            "👤 <b>نام:</b> <code>{label}</code>\n"
            "📊 <b>حجم:</b> <code>{quota}</code>\n"
            "⌛ <b>انقضا:</b> <code>{expiry}</code>\n\n"
            "🔗 <b>لینک VLESS:</b>\n<code>{vless}</code>\n\n"
            "🌐 <b>آدرس اشتراک:</b>\n<code>{sub}</code>"
        ),
        "unlimited": "نامحدود",
        "days_fmt": "{days} روز",
        "addaddr_format": "❌ فرمت: <code>/addaddr [آی‌پی_یا_دامنه]</code>",
        "addaddr_invalid": "❌ فرمت آدرس نامعتبر است.",
        "addaddr_exists": "⚠️ آدرس «{addr}» قبلاً در لیست موجود است.",
        "addaddr_success": "✅ آی‌پی/دامنه‌ی <code>{addr}</code> با موفقیت اضافه شد.",
        "toggle_format": "❌ فرمت: <code>/{action} [نام‌کاربری]</code>",
        "not_found": "❌ کاربر «{name}» پیدا نشد.",
        "toggle_success": "✅ کاربر <code>{name}</code> با موفقیت <b>{state}</b> شد.",
        "state_enabled": "فعال",
        "state_disabled": "غیرفعال",
        "reset_format": "❌ فرمت: <code>/reset [نام‌کاربری]</code>",
        "reset_success": "🔄 مصرف کاربر <code>{name}</code> به صفر بازنشانی شد.",
        "create_guide": (
            "➕ <b>راهنمای ساخت کاربر:</b>\n\n"
            "از دستور <code>/create</code> استفاده کن. فرمت:\n"
            "<code>/create [نام] [حجم_GB] [روز]</code>\n\n"
            "<b>مثال‌ها:</b>\n"
            "• <code>/create Ali 15 30</code> (۱۵ گیگ، ۳۰ روز اعتبار)\n"
            "• <code>/create Reza 0 0</code> (نامحدود، بدون انقضا)"
        ),
        "addip_guide": (
            "🌐 <b>راهنمای افزودن آی‌پی تمیز:</b>\n\n"
            "از دستور <code>/addaddr</code> استفاده کن. فرمت:\n"
            "<code>/addaddr [آی‌پی_یا_دامنه]</code>\n\n"
            "<b>مثال:</b>\n"
            "• <code>/addaddr cf.example.com</code>\n"
            "• <code>/addaddr 1.1.1.1</code>"
        ),
        "quota_alert": (
            "⚠️ <b>هشدار اتمام حجم!</b>\n"
            "کاربر: <code>{label}</code> به سقف مصرف رسید.\n"
            "مصرف: <code>{used} / {limit}</code>"
        ),
        "expiry_alert": (
            "⏰ <b>هشدار انقضا!</b>\n"
            "کاربر: <code>{label}</code> منقضی شد.\n"
            "تاریخ انقضا: <code>{exp}</code>"
        ),
    },
}

def bot_lang() -> str:
    return CONFIG.get("bot_lang") if CONFIG.get("bot_lang") in ("en", "fa") else "en"

def L(key: str, **kwargs) -> str:
    lang = bot_lang()
    template = BOT_I18N.get(lang, BOT_I18N["en"]).get(key) or BOT_I18N["en"].get(key, key)
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def build_main_keyboard():
    if not TELEBOT_AVAILABLE:
        return None
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(L("btn_stats"), callback_data="tg_stats"),
        types.InlineKeyboardButton(L("btn_users"), callback_data="tg_users"),
        types.InlineKeyboardButton(L("btn_top"), callback_data="tg_top"),
        types.InlineKeyboardButton(L("btn_create"), callback_data="tg_create_guide"),
        types.InlineKeyboardButton(L("btn_addip"), callback_data="tg_add_ip_guide"),
        types.InlineKeyboardButton(L("btn_lang"), callback_data="tg_lang_toggle"),
    )
    return kb

# ── SQLite Database ──────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS links (
            uuid TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            limit_bytes INTEGER DEFAULT 0,
            used_bytes INTEGER DEFAULT 0,
            max_connections INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            expires_at TEXT,
            protocol TEXT DEFAULT 'vless-ws',
            fingerprint TEXT DEFAULT 'chrome',
            alpn TEXT DEFAULT '',
            port INTEGER DEFAULT 443
        );
        CREATE TABLE IF NOT EXISTS custom_addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            expires_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS auth (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            password_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT,
            seen INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS github_cache (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            latest_tag TEXT,
            latest_url TEXT,
            checked_at REAL
        );
    """)
    conn.commit()
    # Migrate older DBs created before protocol/fingerprint/alpn/port existed
    existing_cols = {row["name"] for row in conn.execute("PRAGMA table_info(links)").fetchall()}
    for col, ddl in (
        ("protocol", "ALTER TABLE links ADD COLUMN protocol TEXT DEFAULT 'vless-ws'"),
        ("fingerprint", "ALTER TABLE links ADD COLUMN fingerprint TEXT DEFAULT 'chrome'"),
        ("alpn", "ALTER TABLE links ADD COLUMN alpn TEXT DEFAULT ''"),
        ("port", "ALTER TABLE links ADD COLUMN port INTEGER DEFAULT 443"),
        ("variants_json", "ALTER TABLE links ADD COLUMN variants_json TEXT DEFAULT ''"),
    ):
        if col not in existing_cols:
            conn.execute(ddl)
    conn.commit()
    # Ensure default auth row
    cur = conn.execute("SELECT password_hash FROM auth WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT INTO auth (id, password_hash) VALUES (1, ?)", (AUTH["password_hash"],))
        conn.commit()
    else:
        AUTH["password_hash"] = row["password_hash"]
    conn.close()
    migrate_json_to_sqlite()

def migrate_json_to_sqlite():
    json_file = "panel_db.json"
    if not os.path.exists(json_file):
        return
    conn = get_db()
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Migrate auth
        pw = data.get("auth_hash")
        if pw:
            conn.execute("INSERT OR REPLACE INTO auth (id, password_hash) VALUES (1, ?)", (pw,))
            AUTH["password_hash"] = pw
        # Migrate links
        links = data.get("links", {})
        for uid, link in links.items():
            variants = variants_from_legacy(link.get("protocol", DEFAULT_PROTOCOL), link.get("fingerprint", DEFAULT_FINGERPRINT), link.get("alpn", ""))
            legacy_protocol, legacy_fp, legacy_alpn = variants_to_legacy(variants)
            conn.execute("""
                INSERT OR REPLACE INTO links (uuid, label, limit_bytes, used_bytes, max_connections, created_at, active, expires_at, protocol, fingerprint, alpn, port, variants_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (uid, link.get("label", uid), link.get("limit_bytes", 0), link.get("used_bytes", 0),
                  link.get("max_connections", 0), link.get("created_at", datetime.now(timezone.utc).isoformat()),
                  1 if link.get("active", True) else 0, link.get("expires_at"),
                  legacy_protocol, legacy_fp, legacy_alpn, link.get("port", DEFAULT_PORT),
                  json.dumps(variants)))
            LINKS[uid] = dict(link)
            LINKS[uid]["variants"] = variants
        # Migrate addresses
        addresses = data.get("custom_addresses", [])
        CUSTOM_ADDRESSES.clear()
        for addr in addresses:
            conn.execute("INSERT OR IGNORE INTO custom_addresses (address) VALUES (?)", (addr,))
            CUSTOM_ADDRESSES.append(addr)
        # Migrate settings
        for key in ("telegram_token", "telegram_admin_id", "bot_lang"):
            val = data.get(key)
            if val:
                conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(val)))
                CONFIG[key] = val
        conn.commit()
        # Backup and remove old JSON
        os.rename(json_file, json_file + ".bak")
        logger.info(f"Migrated from {json_file} to SQLite database.")
    except Exception as e:
        logger.error(f"Migration error: {e}")
    finally:
        conn.close()

async def save_db():
    conn = get_db()
    try:
        async with DB_LOCK:
            # Save auth
            conn.execute("INSERT OR REPLACE INTO auth (id, password_hash) VALUES (1, ?)", (AUTH["password_hash"],))
            # Save links
            async with LINKS_LOCK:
                for uid, link in list(LINKS.items()):
                    variants = sanitize_variants(link.get("variants"))
                    legacy_protocol, legacy_fp, legacy_alpn = variants_to_legacy(variants)
                    conn.execute("""
                        INSERT OR REPLACE INTO links (uuid, label, limit_bytes, used_bytes, max_connections, created_at, active, expires_at, protocol, fingerprint, alpn, port, variants_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (uid, link["label"], link["limit_bytes"], link["used_bytes"],
                          link.get("max_connections", 0), link["created_at"],
                          1 if link.get("active", True) else 0, link.get("expires_at"),
                          legacy_protocol, legacy_fp, legacy_alpn, link.get("port", DEFAULT_PORT),
                          json.dumps(variants)))
            # Save addresses
            async with CUSTOM_ADDRESSES_LOCK:
                conn.execute("DELETE FROM custom_addresses")
                for addr in CUSTOM_ADDRESSES:
                    conn.execute("INSERT INTO custom_addresses (address) VALUES (?)", (addr,))
            # Save settings
            for key in ("telegram_token", "telegram_admin_id", "bot_lang", "railway_token", "notify_connections"):
                conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, CONFIG.get(key, "")))
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving DB: {e}")
    finally:
        conn.close()

def load_db():
    global CUSTOM_ADDRESSES, LINKS
    conn = get_db()
    try:
        # Load auth
        cur = conn.execute("SELECT password_hash FROM auth WHERE id = 1")
        row = cur.fetchone()
        if row:
            AUTH["password_hash"] = row["password_hash"]
        # Load links
        LINKS.clear()
        cur = conn.execute("SELECT * FROM links")
        for row in cur.fetchall():
            variants_raw = row["variants_json"] if "variants_json" in row.keys() else None
            variants = None
            if variants_raw:
                try:
                    variants = sanitize_variants(json.loads(variants_raw))
                except Exception:
                    variants = None
            if variants is None:
                variants = variants_from_legacy(row["protocol"], row["fingerprint"], row["alpn"])
            LINKS[row["uuid"]] = {
                "label": row["label"],
                "limit_bytes": row["limit_bytes"],
                "used_bytes": row["used_bytes"],
                "max_connections": row["max_connections"],
                "created_at": row["created_at"],
                "active": bool(row["active"]),
                "expires_at": row["expires_at"],
                "variants": variants,
                "port": row["port"] if row["port"] else DEFAULT_PORT,
            }
        # Load addresses
        CUSTOM_ADDRESSES.clear()
        cur = conn.execute("SELECT address FROM custom_addresses")
        rows = cur.fetchall()
        if rows:
            CUSTOM_ADDRESSES.extend(row["address"] for row in rows)
        # پاک‌سازی یک‌بارمصرف: آدرس پیش‌فرض قدیمی رو دیگه نمی‌خوایم، حتی اگه از قبل
        # تو دیتابیس ذخیره شده باشه.
        if "www.speedtest.net" in CUSTOM_ADDRESSES:
            CUSTOM_ADDRESSES.remove("www.speedtest.net")
            conn.execute("DELETE FROM custom_addresses WHERE address = ?", ("www.speedtest.net",))
            conn.commit()
        # Load settings
        cur = conn.execute("SELECT key, value FROM settings")
        for row in cur.fetchall():
            CONFIG[row["key"]] = row["value"]
    except Exception as e:
        logger.error(f"Error loading DB: {e}")
    finally:
        conn.close()

def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password("admin")}


async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    conn = get_db()
    try:
        conn.execute("INSERT INTO sessions (token, expires_at) VALUES (?, ?)", (token, time.time() + SESSION_TTL))
        conn.commit()
    finally:
        conn.close()
    return token

async def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    conn = get_db()
    try:
        cur = conn.execute("SELECT expires_at FROM sessions WHERE token = ?", (token,))
        row = cur.fetchone()
        if row is None or row["expires_at"] < time.time():
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            return False
        return True
    finally:
        conn.close()

async def destroy_session(token: str | None):
    if token:
        conn = get_db()
        try:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
        finally:
            conn.close()

async def clear_expired_sessions():
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (time.time(),))
        conn.commit()
    finally:
        conn.close()

async def require_auth(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return token

async def keep_alive():
    global http_client
    while True:
        await asyncio.sleep(600)
        try:
            await clear_expired_sessions()
            domain = get_domain()
            if domain and domain != "localhost" and http_client is not None:
                await http_client.get(f"https://{domain}/health")
        except Exception:
            pass

@app.on_event("startup")
async def startup():
    global http_client
    init_db()
    load_db()
    migrate_legacy_uuids()
    limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    http_client = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
    asyncio.create_task(keep_alive())
    asyncio.create_task(github_check_loop())
    await restart_telegram_bot()
    asyncio.create_task(telegram_notifier_cron())
    await ensure_default_link()

@app.on_event("shutdown")
async def shutdown():
    await _stop_telegram_bot()
    await clear_expired_sessions()
    if http_client:
        await http_client.aclose()

def get_domain() -> str:
    """Return the public host used in generated subscription/share links.

    Railway/Render env vars are preferred. PUBLIC_DOMAIN is also supported for
    custom deployments. The value is normalized so old configs cannot produce
    https://https://... URLs.
    """
    raw = (
        os.environ.get("PUBLIC_DOMAIN")
        or os.environ.get("RENDER_EXTERNAL_URL")
        or os.environ.get("RAILWAY_PUBLIC_DOMAIN")
        or "localhost"
    ).strip()
    raw = raw.replace("https://", "").replace("http://", "").strip().rstrip("/")
    return raw.split("/", 1)[0]

def generate_vless_link(
    uuid: str,
    remark: str = "IMMINENT",
    address: str = None,
    port: int = None,
    protocol: str = DEFAULT_PROTOCOL,
    fingerprint: str | None = None,
    alpn: str | None = None,
) -> str:
    """می‌سازد share-link متناسب با auth (vless/trojan) و ترابرد انتخاب‌شده
    (ws یا یکی از دو مد XHTTP: packet-up / stream-up). fingerprint/alpn در
    صورت ندادن، از پیش‌فرض‌های خودِ پروتکل استفاده می‌کنن. پورت همیشه 443 است
    و پارامتر port دیگه در نظر گرفته نمی‌شه.

    نکته‌ی مهم: برای هر دو auth، همون uid مخفیِ توی مسیر URL (/ws/{uid} یا
    /xhttp/{mode}/{uid}) واقعاً احراز هویت می‌کنه، نه UUID داخل هدر VLESS یا
    پسورد داخل هدر Trojan (که هیچ‌کدوم سمت سرور چک نمی‌شن) — پس برای Trojan هم
    از همون uid به‌عنوان password توی لینک استفاده می‌کنیم."""
    domain = get_domain()
    addr = address if address else domain

    protocol = normalize_protocol(protocol)
    auth, transport = split_protocol(protocol)

    fp = (fingerprint or DEFAULT_FINGERPRINT).strip().lower() or DEFAULT_FINGERPRINT
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT

    alpn_val = (alpn or "").strip()
    if alpn_val not in ALPN_OPTIONS:
        alpn_val = DEFAULT_ALPN_BY_PROTOCOL.get(protocol, "http/1.1")

    # پورت ثابت و همیشه 443 — هر مقدار ورودی نادیده گرفته می‌شه
    use_port = DEFAULT_PORT

    if transport == "ws":
        path = f"/ws/{auth}/{uuid}?ed=2048"
        base_params = {"security": "tls", "type": "ws", "host": domain, "path": path, "sni": domain, "fp": fp, "alpn": alpn_val}
    else:
        # xhttp-packet-up / xhttp-stream-up
        mode = transport.replace("xhttp-", "")  # packet-up | stream-up
        path = f"/xhttp/{auth}/{mode}/{uuid}"
        base_params = {"security": "tls", "type": "xhttp", "mode": mode, "host": domain, "path": path, "sni": domain, "fp": fp, "alpn": alpn_val}

    if auth == "vless":
        params = {"encryption": "none", **base_params}
        scheme = "vless"
    else:
        # trojan:// user-info بخش، password هست نه uuid؛ چون سمت سرور چک نمی‌شه از
        # همون uid استفاده می‌کنیم تا برای کاربر هم مشخص و یکتا بمونه.
        params = base_params
        scheme = "trojan"

    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"{scheme}://{uuid}@{addr}:{use_port}?{query}#{quote(remark)}"


def link_for_variant(link: dict, uid: str, auth: str, address: str = None) -> str | None:
    """اگه variant مربوط به این auth (vless/trojan) روی این لینک فعال باشه، share-link
    مربوطه رو می‌سازه؛ وگرنه None برمی‌گردونه."""
    variant = sanitize_variants(link.get("variants")).get(auth)
    if not variant or not variant.get("enabled"):
        return None
    protocol = f"{auth}-{variant['transport']}"
    return generate_vless_link(
        uid,
        remark=f"IMMINENT-{link.get('label', '')}",
        address=address,
        protocol=protocol,
        fingerprint=variant.get("fingerprint"),
        alpn=variant.get("alpn"),
    )

def links_for_all_variants(link: dict, uid: str, address: str = None) -> list[str]:
    """برای هر auth فعال روی این لینک، یک share-link می‌سازه (ممکنه ۱ یا ۲ تا خروجی بده)."""
    out = []
    for auth in AUTH_TYPES:
        share_link = link_for_variant(link, uid, auth, address=address)
        if share_link:
            out.append(share_link)
    return out

def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB": return int(value * 1024 * 1024 * 1024)
    if unit == "MB": return int(value * 1024 * 1024)
    if unit == "KB": return int(value * 1024)
    return int(value)

def parse_expires_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        normalised = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def seconds_until_expiry(expires_at_str: str | None) -> int | None:
    exp = parse_expires_at(expires_at_str)
    if exp is None:
        return None
    remaining = (exp - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(remaining))

async def ensure_default_link():
    async with LINKS_LOCK:
        if not LINKS:
            LINKS[str(uuid.uuid4())] = {
                "label": "Default",
                "limit_bytes": 0,
                "used_bytes": 0,
                "max_connections": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "active": True,
                "expires_at": None,
                "variants": default_variants(),
                "port": DEFAULT_PORT,
            }

async def find_uid_by_label(label: str) -> str | None:
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            if data["label"] == label:
                return uid
    return None

_UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

def migrate_legacy_uuids():
    """One-time migration: older versions of this panel used the link's label
    as its VLESS uuid (e.g. 'Default', 'Ali'). Modern clients (Hiddify, Clash
    Meta, and other sing-box/Xray-based apps) reject non-UUID ids outright.
    This rewrites any such legacy link to use a real UUID, keeping its label,
    quota, usage, and expiry intact."""
    conn = get_db()
    try:
        changed = False
        for old_uid, link in list(LINKS.items()):
            if _UUID_RE.match(old_uid):
                continue
            new_uid = str(uuid.uuid4())
            conn.execute("DELETE FROM links WHERE uuid = ?", (old_uid,))
            conn.execute("""
                INSERT OR REPLACE INTO links (uuid, label, limit_bytes, used_bytes, max_connections, created_at, active, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_uid, link["label"], link["limit_bytes"], link["used_bytes"],
                  link.get("max_connections", 0), link["created_at"],
                  1 if link.get("active", True) else 0, link.get("expires_at")))
            del LINKS[old_uid]
            LINKS[new_uid] = link
            changed = True
            logger.info(f"Migrated legacy link '{link['label']}' to a standard UUID.")
        if changed:
            conn.commit()
    except Exception as e:
        logger.error(f"Error migrating legacy uuids: {e}")
    finally:
        conn.close()

def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client:
        return websocket.client.host
    return "unknown"

def get_request_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"

async def count_connections_for_link(uid: str) -> int:
    async with connections_lock:
        return sum(1 for info in connections.values() if info.get("uuid") == uid)

async def remove_ip_from_link(uid: str, ip: str):
    async with connections_lock:
        if uid in link_ip_map:
            link_ip_map[uid].discard(ip)
            if not link_ip_map[uid]:
                link_ip_map.pop(uid, None)

async def close_connections_for_link(uid: str):
    async with connections_lock:
        to_close = [cid for cid, info in connections.items() if info.get("uuid") == uid]
    for cid in to_close:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000, reason="link deleted")
            except Exception:
                pass
        async with connections_lock:
            connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    async with connections_lock:
        link_ip_map.pop(uid, None)

def _is_admin_chat(chat_id, admin_id) -> bool:
    if str(chat_id) != str(admin_id):
        logger.warning(
            f"Telegram Bot: ignored message from chat_id={chat_id} "
            f"(configured admin_id={admin_id!r} does not match)"
        )
        return False
    return True

async def _stop_telegram_bot():
    global bot, bot_polling_task
    if bot is not None:
        try:
            bot.stop_polling()
        except Exception:
            pass
    if bot_polling_task is not None and not bot_polling_task.done():
        bot_polling_task.cancel()
        try:
            await bot_polling_task
        except (asyncio.CancelledError, Exception):
            pass
    bot = None
    bot_polling_task = None

async def restart_telegram_bot():
    global bot, bot_polling_task
    if not TELEBOT_AVAILABLE:
        logger.warning("Telegram Bot is disabled because pyTelegramBotAPI library is not installed.")
        return

    await _stop_telegram_bot()

    token = CONFIG.get("telegram_token")
    admin_id = CONFIG.get("telegram_admin_id")
    if not token or not admin_id:
        logger.info("Telegram Bot configuration is incomplete. Disabled.")
        return

    logger.info("Restarting Telegram Bot with official library...")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.get(f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=true")
            me_resp = await client.get(f"https://api.telegram.org/bot{token}/getMe")
            me_data = me_resp.json()
            if not me_data.get("ok"):
                logger.error(f"Telegram Bot: token rejected by Telegram ({me_data.get('description')}). Bot NOT started.")
                return
            logger.info(f"Telegram Bot: token verified, connected as @{me_data['result'].get('username')}")
    except Exception as e:
        logger.error(f"Telegram Bot: could not reach Telegram API, bot NOT started: {e}")
        return

    bot = AsyncTeleBot(token)

    @bot.message_handler(commands=['start'])
    async def cmd_start(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        await bot.send_message(message.chat.id, L("welcome"), parse_mode="HTML", reply_markup=build_main_keyboard())

    @bot.message_handler(commands=['stats'])
    async def cmd_stats(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        s_data = await get_internal_stats()
        await bot.send_message(message.chat.id, make_stats_text(s_data), parse_mode="HTML")

    @bot.message_handler(commands=['users'])
    async def cmd_users(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        utext = await make_users_text()
        await bot.send_message(message.chat.id, utext, parse_mode="HTML")

    @bot.message_handler(commands=['top'])
    async def cmd_top(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        utext = await make_top_users_text()
        await bot.send_message(message.chat.id, utext, parse_mode="HTML")

    @bot.message_handler(commands=['create'])
    async def cmd_create(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_create_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['addaddr'])
    async def cmd_addaddr(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_addaddr_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['disable'])
    async def cmd_disable(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_toggle_command(message.text, False)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['enable'])
    async def cmd_enable(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_toggle_command(message.text, True)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['reset'])
    async def cmd_reset(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_reset_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda call: True)
    async def handle_callback(call):
        if not _is_admin_chat(call.message.chat.id, admin_id):
            return
        await bot.answer_callback_query(call.id)

        if call.data == "tg_lang_toggle":
            CONFIG["bot_lang"] = "fa" if bot_lang() == "en" else "en"
            await save_db()
            await bot.send_message(call.message.chat.id, L("lang_switched"), parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_stats":
            s_data = await get_internal_stats()
            await bot.send_message(call.message.chat.id, make_stats_text(s_data), parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_users":
            utext = await make_users_text()
            await bot.send_message(call.message.chat.id, utext, parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_top":
            utext = await make_top_users_text()
            await bot.send_message(call.message.chat.id, utext, parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_create_guide":
            await bot.send_message(call.message.chat.id, L("create_guide"), parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_add_ip_guide":
            await bot.send_message(call.message.chat.id, L("addip_guide"), parse_mode="HTML", reply_markup=build_main_keyboard())

    async def _run_polling(bot_instance):
        try:
            await bot_instance.infinity_polling()
        except Exception as e:
            logger.error(f"Telegram Bot: polling loop stopped unexpectedly: {e}")

    bot_polling_task = asyncio.create_task(_run_polling(bot))
    logger.info("Telegram Bot is now polling for updates.")

async def send_tg_message(text: str):
    global bot
    admin_id = CONFIG.get("telegram_admin_id")
    if bot and admin_id:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending TG notification: {e}")

def _notify_connections_enabled() -> bool:
    return str(CONFIG.get("notify_connections", "0")) in ("1", "true", "True")

async def _log_connection_event(event: str, label: str, uid: str, ip: str, extra: str = ""):
    """Logs every client connect/disconnect and, if enabled in Settings,
    forwards the same event to the admin via Telegram."""
    verb = "Connected" if event == "connect" else "Disconnected"
    suffix = f" - {extra}" if extra else ""
    logger.info(f"{verb}: link='{label}' ({uid}) from {ip}{suffix}")
    if _notify_connections_enabled():
        icon = "🟢" if event == "connect" else "🔴"
        verb_fa = "متصل شد" if event == "connect" else "قطع اتصال شد"
        msg = f"{icon} <b>{html.escape(label)}</b> {verb_fa}\nIP: <code>{html.escape(ip)}</code>"
        if extra:
            msg += f"\n{html.escape(extra)}"
        await send_tg_message(msg)

def fmt_exp_py(ea: str | None) -> str:
    if not ea:
        return "∞"
    exp = parse_expires_at(ea)
    if not exp:
        return "∞"
    diff = exp - datetime.now(timezone.utc)
    seconds = diff.total_seconds()
    if seconds <= 0:
        return "Expired"
    days = int(seconds // 86400)
    if days > 0:
        return f"{days}d"
    hours = int(seconds // 3600)
    if hours > 0:
        return f"{hours}h"
    minutes = int(seconds // 60)
    return f"{minutes}m"

async def get_internal_stats():
    async with connections_lock:
        conn_count = len(connections)
    return {
        "active_connections": conn_count,
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
    }

def make_stats_text(s_data) -> str:
    return L(
        "stats",
        domain=s_data.get("domain", "-"),
        cpu=s_data.get("cpu_percent", 0),
        mem=s_data.get("memory_percent", 0),
        uptime=s_data.get("uptime", "-"),
        active=s_data.get("active_connections", 0),
        traffic=s_data.get("total_traffic_mb", 0),
        links=s_data.get("links_count", 0),
    )

async def make_users_text() -> str:
    lines = [L("users_title")]
    async with LINKS_LOCK:
        items = list(LINKS.items())

    if not items:
        return L("no_inbounds")

    for uid, data in items:
        used = _fmt_bytes(data["used_bytes"])
        limit = _fmt_bytes(data["limit_bytes"]) if data["limit_bytes"] > 0 else "∞"
        ex = fmt_exp_py(data.get("expires_at"))
        status = L("status_on") if data["active"] else L("status_off")
        lines.append(L("users_line", label=data['label'], used=used, limit=limit, exp=ex, status=status))

    return "\n".join(lines[:35])

async def make_top_users_text() -> str:
    lines = [L("top_title")]
    async with LINKS_LOCK:
        items = list(LINKS.items())
    if not items:
        return L("no_inbounds")

    sorted_items = sorted(items, key=lambda x: x[1].get("used_bytes", 0), reverse=True)[:5]
    for i, (uid, data) in enumerate(sorted_items, 1):
        used = _fmt_bytes(data["used_bytes"])
        limit = _fmt_bytes(data["limit_bytes"]) if data["limit_bytes"] > 0 else "∞"
        lines.append(L("top_line", i=i, label=data['label'], used=used, limit=limit))
    return "\n".join(lines)

async def handle_create_command(text: str):
    parts = text.split()
    if len(parts) < 2:
        return L("create_format")
    label = parts[1]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        return L("create_bad_name")

    limit_value = 0.0
    days_valid = 0

    if len(parts) >= 3:
        try:
            limit_value = float(parts[2])
        except ValueError:
            return L("create_bad_limit")

    if len(parts) >= 4:
        try:
            days_valid = int(parts[3])
        except ValueError:
            return L("create_bad_days")

    async with LINKS_LOCK:
        if any(v["label"] == label for v in LINKS.values()):
            return L("create_exists", label=label)

    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, "GB")
    expires_at = None
    if days_valid > 0:
        expires_at = (datetime.now(timezone.utc) + timedelta(days=days_valid)).isoformat()

    uid = str(uuid.uuid4())
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label,
            "limit_bytes": limit_bytes,
            "used_bytes": 0,
            "max_connections": 0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
            "expires_at": expires_at,
            "variants": default_variants(),
            "port": DEFAULT_PORT,
        }

    await save_db()
    vless_link = "\n".join(links_for_all_variants(LINKS[uid], uid))
    sub_url = f"https://{get_domain()}/sub/{uid}"

    quota_str = _fmt_bytes(limit_bytes) if limit_bytes > 0 else L("unlimited")
    expiry_str = L("days_fmt", days=days_valid) if days_valid > 0 else L("unlimited")

    return L(
        "create_success",
        label=label, quota=quota_str, expiry=expiry_str,
        vless=vless_link, sub=sub_url,
    )

async def handle_addaddr_command(text: str) -> str:
    parts = text.split()
    if len(parts) < 2:
        return L("addaddr_format")
    addr = parts[1].strip()
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', addr):
        return L("addaddr_invalid")
    async with CUSTOM_ADDRESSES_LOCK:
        if addr in CUSTOM_ADDRESSES:
            return L("addaddr_exists", addr=addr)
        CUSTOM_ADDRESSES.append(addr)
    await save_db()
    return L("addaddr_success", addr=addr)

async def handle_toggle_command(text: str, active_state: bool) -> str:
    parts = text.split()
    if len(parts) < 2:
        action_name = "enable" if active_state else "disable"
        return L("toggle_format", action=action_name)
    name = parts[1].strip()
    uid = await find_uid_by_label(name)
    if uid is None:
        return L("not_found", name=name)
    async with LINKS_LOCK:
        LINKS[uid]["active"] = active_state
    await save_db()
    state_str = L("state_enabled") if active_state else L("state_disabled")
    return L("toggle_success", name=name, state=state_str)

async def handle_reset_command(text: str) -> str:
    parts = text.split()
    if len(parts) < 2:
        return L("reset_format")
    name = parts[1].strip()
    uid = await find_uid_by_label(name)
    if uid is None:
        return L("not_found", name=name)
    async with LINKS_LOCK:
        LINKS[uid]["used_bytes"] = 0
    notified_uids.discard(f"quota_{name}")
    await save_db()
    return L("reset_success", name=name)

async def telegram_notifier_cron():
    while True:
        try:
            token = CONFIG.get("telegram_token")
            admin_id = CONFIG.get("telegram_admin_id")
            if not token or not admin_id:
                await asyncio.sleep(60)
                continue

            async with LINKS_LOCK:
                items = list(LINKS.items())
            
            for uid, data in items:
                if not data["active"]:
                    continue
                
                used = data["used_bytes"]
                limit = data["limit_bytes"]
                label = data["label"]
                
                if limit > 0 and used >= limit:
                    notif_key = f"quota_{uid}"
                    if notif_key not in notified_uids:
                        msg = L("quota_alert", label=label, used=_fmt_bytes(used), limit=_fmt_bytes(limit))
                        await send_tg_message(msg)
                        notified_uids.add(notif_key)
                        await create_notification(
                            type="quota",
                            title=f"Quota exceeded: {label}",
                            message=f"{label} used {_fmt_bytes(used)} of {_fmt_bytes(limit)}",
                        )
                
                expires_at_str = data.get("expires_at")
                if expires_at_str:
                    exp = parse_expires_at(expires_at_str)
                    if exp and exp < datetime.now(timezone.utc):
                        notif_key = f"expiry_{uid}"
                        if notif_key not in notified_uids:
                            msg = L("expiry_alert", label=label, exp=expires_at_str)
                            await send_tg_message(msg)
                            notified_uids.add(notif_key)
                            await create_notification(
                                type="expiry",
                                title=f"Expired: {label}",
                                message=f"{label} has expired on {expires_at_str}",
                            )
                            
        except Exception as e:
            logger.error(f"Error in notification cron: {e}")
            
        await asyncio.sleep(60)

@app.get("/")
async def root():
    return Response(content="OK", media_type="text/plain")

@app.get("/health")
async def health():
    async with connections_lock:
        conn_count = len(connections)
    return {"status": "ok", "connections": conn_count, "uptime": uptime()}

@app.get("/api/ping-check")
async def ping_check(host: str, port: int = 443):
    """Measures real TCP connect latency to a config's host from the panel's
    own server/network (not the visitor's browser), so results reflect the
    server's actual reachability instead of being limited by browser CORS."""
    if port < 1 or port > 65535:
        return {"host": host, "port": port, "ms": None, "reachable": False}
    start = time.time()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5.0)
        ms = round((time.time() - start) * 1000)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return {"host": host, "port": port, "ms": ms, "reachable": True}
    except Exception:
        return {"host": host, "port": port, "ms": None, "reachable": False}

@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password") or "")
    ip = get_request_ip(request)
    if hash_password(password) != AUTH["password_hash"]:
        await send_tg_message(f"⚠️ <b>تلاش ناموفق برای ورود به پنل</b>\nIP: <code>{html.escape(ip)}</code>")
        raise HTTPException(status_code=401, detail="Invalid password")
    token = await create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(key=SESSION_COOKIE, value=token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/")
    await send_tg_message(f"🟢 <b>ورود ادمین به پنل</b>\nIP: <code>{html.escape(ip)}</code>")
    return resp

@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    await destroy_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    await send_tg_message(f"🔴 <b>خروج ادمین از پنل</b>\nIP: <code>{html.escape(get_request_ip(request))}</code>")
    return resp

@app.get("/api/me")
async def api_me(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    return {"authenticated": await is_valid_session(token)}

@app.get("/api/version")
async def api_version():
    gh = await check_github_latest()
    latest = gh.get("tag")
    current = PANEL_VERSION.lstrip("vV")
    update_available = bool(latest) and latest.lstrip("vV") != current
    return {
        "version": PANEL_VERSION,
        "latest_github_version": latest,
        "update_available": update_available,
        "github_url": gh.get("url") or f"https://github.com/{GITHUB_REPO}/releases",
    }

@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    current = str(body.get("current_password") or "")
    new = str(body.get("new_password") or "")
    if hash_password(current) != AUTH["password_hash"]:
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    AUTH["password_hash"] = hash_password(new)
    await save_db()
    current_token = request.cookies.get(SESSION_COOKIE)
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions")
        if current_token:
            conn.execute("INSERT INTO sessions (token, expires_at) VALUES (?, ?)", (current_token, time.time() + SESSION_TTL))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.get("/api/settings")
async def get_settings(_=Depends(require_auth)):
    return {
        "telegram_token": CONFIG["telegram_token"],
        "telegram_admin_id": CONFIG["telegram_admin_id"],
        "railway_token": CONFIG.get("railway_token", ""),
        "notify_connections": CONFIG.get("notify_connections", "0") in ("1", "true", "True", True),
    }

@app.post("/api/settings")
async def update_settings(request: Request, _=Depends(require_auth)):
    body = await request.json()
    # Only touch fields the caller actually sent, so saving from one settings
    # form (e.g. just the Telegram fields) doesn't wipe out fields that
    # belong to another form (e.g. the Railway token).
    if "telegram_token" in body:
        CONFIG["telegram_token"] = (body.get("telegram_token") or "").strip()
    if "telegram_admin_id" in body:
        CONFIG["telegram_admin_id"] = (body.get("telegram_admin_id") or "").strip()
    if "railway_token" in body:
        CONFIG["railway_token"] = (body.get("railway_token") or "").strip()
    if "notify_connections" in body:
        CONFIG["notify_connections"] = "1" if body.get("notify_connections") else "0"
    await save_db()
    await restart_telegram_bot()
    return {"ok": True}

# ── Railway / Permanent Database ──────────────────────────────────────────

RAILWAY_API_URL = "https://backboard.railway.com/graphql/v2"

async def _railway_graphql(token: str, query: str, variables: dict = None) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {"query": query}
    if variables:
        body["variables"] = variables
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(RAILWAY_API_URL, json=body, headers=headers)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Railway API error: {r.status_code}")
        data = r.json()
        if "errors" in data:
            raise HTTPException(status_code=502, detail=data["errors"][0].get("message", "Railway API error"))
        return data.get("data", {})

@app.post("/api/railway/projects")
async def railway_list_projects(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = body.get("token", "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Railway token is required")

    projects = []
    seen_ids = set()

    # Personal-account-scoped projects (not inside any workspace)
    personal_data = await _railway_graphql(token, """
        query {
            projects {
                edges { node { id name } }
            }
        }
    """)
    for edge in personal_data.get("projects", {}).get("edges", []):
        node = edge.get("node", {})
        if node.get("id") and node["id"] not in seen_ids:
            seen_ids.add(node["id"])
            projects.append({"id": node["id"], "name": node.get("name", "Unnamed")})

    # Most Railway accounts now keep their projects inside a workspace, so we
    # also need to enumerate workspaces and fetch each one's projects.
    try:
        ws_data = await _railway_graphql(token, """
            query {
                me { workspaces { id name } }
            }
        """)
        workspaces = (ws_data.get("me") or {}).get("workspaces") or []
    except HTTPException:
        # A workspace- or project-scoped token can't call `me`; that's fine,
        # we just skip workspace enumeration and keep whatever we already have.
        workspaces = []

    for ws in workspaces:
        ws_id = ws.get("id")
        if not ws_id:
            continue
        try:
            ws_projects = await _railway_graphql(token, """
                query ($workspaceId: String!) {
                    projects(workspaceId: $workspaceId) {
                        edges { node { id name } }
                    }
                }
            """, {"workspaceId": ws_id})
        except HTTPException:
            continue
        for edge in ws_projects.get("projects", {}).get("edges", []):
            node = edge.get("node", {})
            if node.get("id") and node["id"] not in seen_ids:
                seen_ids.add(node["id"])
                projects.append({"id": node["id"], "name": node.get("name", "Unnamed")})

    return {"projects": projects}

async def _railway_resolve_service(token: str, project_id: str) -> dict:
    """Figures out which service in the project the volume should attach to.

    Railway limits each service to a single volume, and there's no reliable
    way to guess which of a project's services is "the panel" without extra
    input from the user - except that when this app is itself deployed on
    Railway, Railway automatically injects RAILWAY_SERVICE_ID into its own
    environment. We use that for a fully automatic match, and fall back to
    "only one service in the project" when it's not available or doesn't
    belong to this project.
    """
    data = await _railway_graphql(token, """
        query ($id: String!) {
            project(id: $id) {
                services { edges { node { id name } } }
            }
        }
    """, {"id": project_id})
    services = [e["node"] for e in ((data.get("project") or {}).get("services") or {}).get("edges", [])]
    if not services:
        raise HTTPException(status_code=400, detail="No services found in this project.")
    own_service_id = os.environ.get("RAILWAY_SERVICE_ID", "").strip()
    if own_service_id:
        match = next((s for s in services if s["id"] == own_service_id), None)
        if match:
            return match
    if len(services) == 1:
        return services[0]
    raise HTTPException(
        status_code=400,
        detail="Multiple services found in this project and the panel's own service couldn't be identified automatically. Make sure you're running this panel as a Railway service inside the selected project.",
    )

@app.post("/api/railway/volume-status")
async def railway_volume_status(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = body.get("token", "").strip()
    project_id = body.get("project_id", "").strip()
    if not token or not project_id:
        raise HTTPException(status_code=400, detail="Token and project_id are required")
    service = await _railway_resolve_service(token, project_id)
    data = await _railway_graphql(token, """
        query ($id: String!) {
            project(id: $id) {
                volumes {
                    edges {
                        node {
                            id
                            name
                            volumeInstances {
                                edges { node { id mountPath state serviceId environmentId } }
                            }
                        }
                    }
                }
            }
        }
    """, {"id": project_id})
    volumes = []
    for edge in ((data.get("project") or {}).get("volumes") or {}).get("edges", []):
        node = edge["node"]
        for vi_edge in (node.get("volumeInstances") or {}).get("edges", []):
            vi = vi_edge["node"]
            if vi.get("serviceId") == service["id"]:
                volumes.append({
                    "id": node["id"],
                    "name": node.get("name", ""),
                    "path": vi.get("mountPath", ""),
                    "state": vi.get("state", ""),
                })
    has_data_volume = any(v["path"] in ("data", "/data") for v in volumes) or bool(volumes)
    return {"volumes": volumes, "has_data_volume": has_data_volume, "service_name": service.get("name", "")}

@app.post("/api/railway/create-volume")
async def railway_create_volume(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = body.get("token", "").strip()
    project_id = body.get("project_id", "").strip()
    if not token or not project_id:
        raise HTTPException(status_code=400, detail="Token and project_id are required")
    service = await _railway_resolve_service(token, project_id)
    volume_input = {"projectId": project_id, "serviceId": service["id"], "mountPath": "/data"}
    env_id = os.environ.get("RAILWAY_ENVIRONMENT_ID", "").strip()
    if env_id:
        volume_input["environmentId"] = env_id
    data = await _railway_graphql(token, """
        mutation ($input: VolumeCreateInput!) {
            volumeCreate(input: $input) { id name }
        }
    """, {"input": volume_input})
    vol = data.get("volumeCreate") or {}
    if not vol.get("id"):
        raise HTTPException(status_code=502, detail="Failed to create volume")
    return {
        "id": vol["id"],
        "name": vol.get("name", ""),
        "path": "/data",
        "state": "creating",
    }

@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    async with connections_lock:
        conn_count = len(connections)
    return {
        "active_connections": conn_count,
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recent_errors": list(error_logs)[-10:],
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "hourly_traffic": dict(hourly_traffic),
    }

@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Inbound name must contain only English letters, numbers, and characters: - _ . space")
    if not label:
        raise HTTPException(status_code=400, detail="Inbound name is required")
    async with LINKS_LOCK:
        if any(v["label"] == label for v in LINKS.values()):
            raise HTTPException(status_code=400, detail="An inbound with this name already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    if max_conn < 0:
        max_conn = 0
    days_valid = body.get("days_valid")
    expires_at: str | None = None
    if days_valid is not None:
        try:
            days_valid = int(days_valid)
            if days_valid > 0:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=days_valid)).isoformat()
        except (ValueError, TypeError):
            pass

    variants = variants_from_body(body)
    # پورت همیشه 443 است؛ هر مقدار دیگه‌ای که فرانت بفرسته نادیده گرفته می‌شه
    port = DEFAULT_PORT

    uid = str(uuid.uuid4())
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label,
            "limit_bytes": limit_bytes,
            "used_bytes": 0,
            "max_connections": max_conn,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
            "expires_at": expires_at,
            "variants": variants,
            "port": port,
        }
    await save_db()
    return {
        "uuid": uid, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
        "max_connections": max_conn, "active": True, "created_at": LINKS[uid]["created_at"],
        "expires_at": expires_at,
        "variants": variants, "port": port,
        "vless_links": links_for_all_variants(LINKS[uid], uid),
    }

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        items = list(LINKS.items())
    for uid, data in items:
        result.append({
            "uuid": uid,
            "label": data["label"],
            "limit_bytes": data["limit_bytes"],
            "used_bytes": data["used_bytes"],
            "max_connections": data.get("max_connections", 0),
            "active": data["active"],
            "created_at": data["created_at"],
            "expires_at": data.get("expires_at"),
            "variants": sanitize_variants(data.get("variants")),
            "port": data.get("port", DEFAULT_PORT),
            "current_connections": await count_connections_for_link(uid),
            "vless_links": links_for_all_variants(data, uid),
        })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}

@app.patch("/api/links/{uid}")
async def toggle_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        if "active" in body:
            LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            limit_value = float(body.get("limit_value") or 0)
            limit_unit = body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
            notified_uids.discard(f"quota_{uid}")
        if "reset_usage" in body and body["reset_usage"]:
            LINKS[uid]["used_bytes"] = 0
            notified_uids.discard(f"quota_{uid}")
        if "label" in body:
            LINKS[uid]["label"] = str(body["label"])[:60]
        if "max_connections" in body:
            mc = int(body["max_connections"] or 0)
            LINKS[uid]["max_connections"] = mc if mc >= 0 else 0
        variant_keys = ("vless_enabled", "vless_transport", "vless_fingerprint", "vless_alpn",
                        "trojan_enabled", "trojan_transport", "trojan_fingerprint", "trojan_alpn")
        if any(k in body for k in variant_keys):
            LINKS[uid]["variants"] = variants_from_body(body, base=sanitize_variants(LINKS[uid].get("variants")))
        # پورت همیشه 443 است — دیگه از ورودی کاربر خونده نمی‌شه
        LINKS[uid]["port"] = DEFAULT_PORT
        if "days_valid" in body:
            try:
                dv = int(body["days_valid"])
                if dv > 0:
                    LINKS[uid]["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=dv)).isoformat()
                else:
                    LINKS[uid]["expires_at"] = None
                notified_uids.discard(f"expiry_{uid}")
            except (ValueError, TypeError):
                pass
    await save_db()
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        LINKS.pop(uid, None)
    await save_db()
    await close_connections_for_link(uid)
    return {"ok": True}

@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        return {"addresses": list(CUSTOM_ADDRESSES)}

@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="Address is required")
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', address):
        raise HTTPException(status_code=400, detail="Address must contain only English letters, numbers, and characters: - _ .")
    async with CUSTOM_ADDRESSES_LOCK:
        if address in CUSTOM_ADDRESSES:
            raise HTTPException(status_code=400, detail="Address already exists")
        CUSTOM_ADDRESSES.append(address)
    await save_db()
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

@app.delete("/api/addresses")
async def delete_all_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        CUSTOM_ADDRESSES.clear()
    await save_db()
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

@app.delete("/api/addresses/{index}")
async def delete_address(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES):
            CUSTOM_ADDRESSES.pop(index)
        else:
            raise HTTPException(status_code=404, detail="Address not found")
    await save_db()
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

# فایل‌های آماده‌ی IP که کنار main.py قرار می‌گیرن و با یک کلیک، همه‌شون یکجا
# (بدون رفت‌وبرگشت جدا برای هر آی‌پی) به لیست Clean IP اضافه می‌شن.
IP_IMPORT_FILES = {
    "railway": "railway_ips.txt",
}

def _parse_ip_file(path: str) -> list[str]:
    if not os.path.isfile(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if re.match(r'^[a-zA-Z0-9\-_.:/ ]+$', line):
                out.append(line)
    return out

@app.post("/api/addresses/import/{source}")
async def import_addresses(source: str, _=Depends(require_auth)):
    """همه‌ی آی‌پی‌های داخل railway_ips.txt رو یکجا (بدون تاخیر
    برای هرکدوم جدا) به لیست Clean IP اضافه می‌کنه."""
    filename = IP_IMPORT_FILES.get(source)
    if not filename:
        raise HTTPException(status_code=404, detail="unknown import source")
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    ips = _parse_ip_file(file_path)
    if not ips:
        raise HTTPException(status_code=404, detail=f"{filename} not found or empty next to main.py")
    added = 0
    async with CUSTOM_ADDRESSES_LOCK:
        existing = set(CUSTOM_ADDRESSES)
        for ip in ips:
            if ip not in existing:
                CUSTOM_ADDRESSES.append(ip)
                existing.add(ip)
                added += 1
    await save_db()
    return {"ok": True, "added": added, "total_in_file": len(ips), "addresses": list(CUSTOM_ADDRESSES)}

# ── Notifications API ────────────────────────────────────────────────────

@app.get("/api/notifications")
async def api_get_notifications(_=Depends(require_auth)):
    return {"notifications": await get_notifications()}

@app.get("/api/notifications/count")
async def api_notification_count(_=Depends(require_auth)):
    return {"count": await get_unread_notification_count()}

@app.post("/api/notifications/{nid}/seen")
async def api_mark_seen(nid: int, _=Depends(require_auth)):
    conn = get_db()
    try:
        conn.execute("UPDATE notifications SET seen = 1 WHERE id = ?", (nid,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.post("/api/notifications/seen-all")
async def api_mark_all_seen(_=Depends(require_auth)):
    conn = get_db()
    try:
        conn.execute("UPDATE notifications SET seen = 1 WHERE seen = 0")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.delete("/api/notifications")
async def api_clear_notifications(_=Depends(require_auth)):
    conn = get_db()
    try:
        conn.execute("DELETE FROM notifications")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}

@app.websocket("/ws/live-logs")
async def ws_live_logs(websocket: WebSocket, token: str | None = None):
    await websocket.accept()
    if not token or not await is_valid_session(token):
        await websocket.close(code=1008, reason="Unauthorized")
        return
    for item in list(log_queue):
        await websocket.send_text(item)
    last_idx = len(log_queue)
    try:
        while True:
            await asyncio.sleep(0.5)
            curr = list(log_queue)
            if len(curr) > last_idx:
                for idx in range(last_idx, len(curr)):
                    await websocket.send_text(curr[idx])
                last_idx = len(curr)
            elif len(curr) < last_idx:
                last_idx = len(curr)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

def _fmt_bytes(b: int) -> str:
    if b >= 1_073_741_824: return f"{b / 1_073_741_824:.1f}GB"
    if b >= 1_048_576: return f"{b / 1_048_576:.1f}MB"
    return f"{b / 1024:.1f}KB"

def public_base_url(request: Request | None = None) -> str:
    """Build a correct HTTPS public base URL when running behind a proxy."""
    domain = get_domain()
    if domain != "localhost":
        return f"https://{domain}"
    if request is not None:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host")
        if host:
            host = host.split(",", 1)[0].strip()
            proto = request.headers.get("x-forwarded-proto", "https").split(",", 1)[0].strip()
            return f"{proto}://{host}"
    return "http://localhost"

def generate_landing_page(link: dict, uid: str, addresses: list[str]) -> str:
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")

    usage_str = f"{_fmt_bytes(used)} / Unlimited" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    pct = round((used / limit) * 100, 1) if limit > 0 else 0
    rem = limit - used if limit > 0 else -1
    rem_str = _fmt_bytes(rem) if rem >= 0 else "Unlimited"

    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "Unlimited"
        expiry_days = None
    elif secs_left == 0:
        expiry_str = "Expired"
        expiry_days = 0
    else:
        days = secs_left // 86400
        hours = (secs_left % 86400) // 3600
        expiry_str = f"{days}d {hours}h"
        expiry_days = days

    # Parse expiry date string for display
    expiry_date_str = ""
    if expires_at_str:
        exp_dt = parse_expires_at(expires_at_str)
        if exp_dt:
            expiry_date_str = exp_dt.strftime("%d %b %Y").upper()

    configs = links_for_all_variants(link, uid)
    for addr in addresses:
        configs.extend(links_for_all_variants(link, uid, address=addr))

    # Sub URL for QR
    sub_url = f"https://{get_domain()}/sub/{uid}"
    configs_json = json.dumps(configs)

    is_active = link["active"]
    status_text = "Active" if is_active else "Inactive"
    
    # Color based on usage percentage
    if pct >= 90:
        ring_color1 = "#f87171"
        ring_color2 = "#ef4444"
    elif pct >= 70:
        ring_color1 = "#fbbf24"
        ring_color2 = "#f59e0b"
    else:
        ring_color1 = "#C81E2D"
        ring_color2 = "#A8172A"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AXIOM - {link['label']}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        *{{margin:0;padding:0;box-sizing:border-box}}
        :root{{
            --gold:#C81E2D;--gold2:#A8172A;--gold3:#7A0F1F;
            --gold-dim:rgba(200,30,45,0.1);--gold-glow:0 0 20px rgba(200,30,45,0.3);
            --bg:#040810;--bg2:#080f1a;--bg3:#0d1626;
            --surface:rgba(8,15,26,0.95);--surface2:rgba(13,22,38,0.9);
            --border:rgba(200,30,45,0.12);--border2:rgba(200,30,45,0.25);
            --text:rgba(255,255,255,0.92);--text2:rgba(0,245,255,0.8);--text3:rgba(255,255,255,0.4);
            --green:#4ade80;--red:#f87171;--yellow:#fbbf24;
        }}
        html,body{{height:100%;background:var(--bg);font-family:'Inter',sans-serif;color:var(--text)}}
        body{{padding:0;display:flex;flex-direction:column;align-items:center;min-height:100vh;overflow-x:hidden}}
        .music-dock{{position:fixed;top:18px;right:18px;z-index:20;display:flex;align-items:center;gap:10px;padding:9px 11px;border:1px solid rgba(0,245,255,.2);border-radius:18px;background:rgba(4,8,16,.68);backdrop-filter:blur(18px);box-shadow:0 0 30px rgba(0,245,255,.08)}}
        .music-orb{{width:34px;height:34px;border-radius:50%;display:grid;place-items:center;color:#fff;font-size:17px;background:linear-gradient(135deg,#00f5ff,#7c3aed,#ff2bd6);box-shadow:0 0 20px rgba(0,245,255,.22)}}
        .music-copy{{display:flex;flex-direction:column;line-height:1.2;min-width:125px}}
        .music-copy b{{font-size:10px;color:#fff}} .music-copy span{{font-size:8px;color:rgba(255,255,255,.45);margin-top:3px}}
        .music-btn{{width:34px;height:34px;border-radius:11px;border:1px solid rgba(0,245,255,.22);background:rgba(0,245,255,.08);color:#67e8f9;cursor:pointer;font-weight:800}}
        .music-btn.playing{{background:linear-gradient(135deg,#00d9ff,#8b5cf6,#ff2bd6);color:#020617;box-shadow:0 0 20px rgba(255,43,214,.2)}}
        @media(max-width:560px){{.music-dock{{top:10px;right:10px;padding:7px 8px}}.music-copy{{min-width:96px}}.music-copy b{{font-size:9px}}}}

        /* Animated background */
        .bg-glow{{position:fixed;inset:0;z-index:0;pointer-events:none;
            background:radial-gradient(ellipse 60% 40% at 50% -5%,rgba(200,30,45,0.08),transparent 60%),
                       radial-gradient(ellipse 40% 30% at 80% 80%,rgba(200,30,45,0.05),transparent 50%);}}
        .grid-bg{{position:fixed;inset:0;z-index:0;pointer-events:none;
            background-image:linear-gradient(rgba(200,30,45,0.03) 1px,transparent 1px),
                             linear-gradient(90deg,rgba(200,30,45,0.03) 1px,transparent 1px);
            background-size:48px 48px;}}
        /* Shooting stars */
        .shooting-stars{{position:fixed;inset:0;z-index:0;pointer-events:none;overflow:hidden}}
        .shooting-stars .star{{position:absolute;width:110px;height:1px;
            background:linear-gradient(90deg,transparent,rgba(200,30,45,0.55));
            filter:drop-shadow(0 0 4px rgba(200,30,45,0.35));
            opacity:0;transform:translate3d(0,0,0) rotate(18deg);
            animation:shoot 7s linear infinite}}
        .shooting-stars .star::after{{content:"";position:absolute;right:0;top:-1px;
            width:3px;height:3px;border-radius:50%;background:var(--gold);
            box-shadow:0 0 6px 1px rgba(200,30,45,0.7)}}
        .shooting-stars .star:nth-child(1){{top:8%;left:66%;animation-delay:0s}}
        .shooting-stars .star:nth-child(2){{top:24%;left:84%;animation-delay:2.6s;animation-duration:8s}}
        .shooting-stars .star:nth-child(3){{top:42%;left:58%;animation-delay:5.2s;animation-duration:6.5s}}
        .shooting-stars .star:nth-child(4){{top:16%;left:38%;animation-delay:3.8s;animation-duration:7.5s}}
        .shooting-stars .star:nth-child(5){{top:58%;left:88%;animation-delay:6.4s;animation-duration:9s}}
        @keyframes shoot{{
            0%{{opacity:0;transform:translate3d(0,0,0) rotate(18deg)}}
            6%{{opacity:0.75}}
            16%{{opacity:0}}
            100%{{opacity:0;transform:translate3d(-360px,118px,0) rotate(18deg)}}
        }}
        @media (prefers-reduced-motion: reduce){{
            .shooting-stars{{display:none}}
        }}
        /* Starfield */
        .starfield{{position:fixed;inset:0;z-index:0;pointer-events:none;overflow:hidden}}
        .starfield .s{{position:absolute;border-radius:50%;background:#fff;
            animation-name:twinkle;animation-timing-function:ease-in-out;animation-iteration-count:infinite}}
        @keyframes twinkle{{0%,100%{{opacity:.12}}50%{{opacity:.9}}}}
        @media (prefers-reduced-motion: reduce){{
            .starfield .s{{animation:none;opacity:.4}}
        }}


        .container{{width:100%;max-width:420px;padding:20px 16px 40px;position:relative;z-index:1}}

/* ==================== SUBSCRIPTION PORTAL / RGB GLASS v3 ==================== */
:root{{
 --gold:#00f5ff;--gold2:#8b5cf6;--gold3:#ff2bd6;--gold-dim:rgba(0,245,255,.10);
 --bg:#03040a;--bg2:#070914;--bg3:#0c1020;--surface:rgba(10,14,27,.72);--surface2:rgba(15,20,38,.82);
 --border:rgba(0,245,255,.16);--border2:rgba(255,43,214,.32);--text:#f8fafc;--text2:#67e8f9;--text3:rgba(203,213,225,.52)
}}
html,body{{background:#03040a!important}}
body{{background:radial-gradient(900px 520px at 12% -8%,rgba(0,245,255,.11),transparent 58%),radial-gradient(800px 520px at 95% 12%,rgba(139,92,246,.11),transparent 58%),radial-gradient(760px 500px at 55% 110%,rgba(255,43,214,.09),transparent 58%)!important}}
body:before{{content:"";position:fixed;inset:-40%;pointer-events:none;background:conic-gradient(from 0deg,transparent,rgba(0,245,255,.035),transparent 24%,rgba(255,43,214,.04),transparent 48%,rgba(139,92,246,.035),transparent 72%);animation:subAurora 18s linear infinite;z-index:0}}@keyframes subAurora{{to{{transform:rotate(360deg)}}}}
.bg-glow{{background:radial-gradient(ellipse 70% 45% at 50% 0,rgba(0,245,255,.09),transparent 62%),radial-gradient(ellipse 45% 35% at 100% 90%,rgba(255,43,214,.08),transparent 58%)!important}}
.grid-bg{{background-image:linear-gradient(rgba(0,245,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(255,43,214,.025) 1px,transparent 1px)!important;background-size:42px 42px!important}}
.container{{max-width:760px!important;padding:28px 18px 70px!important}}
.header{{text-align:left!important;padding:18px 4px 24px!important;position:relative}}.header:after{{content:"LIVE SUBSCRIPTION";display:block;color:rgba(103,232,249,.55);font-size:9px;letter-spacing:3px;margin-top:8px}}.header-logo{{gap:12px!important}}.header-title{{font-size:28px!important;letter-spacing:5px!important;background:linear-gradient(90deg,#fff,#67e8f9,#a78bfa,#ff8be9)!important;-webkit-background-clip:text!important;background-clip:text!important}}.header-sub{{font-size:12px!important;color:rgba(203,213,225,.55)!important;letter-spacing:1px!important}}
.ring-card,.qr-card,.configs-card{{background:linear-gradient(145deg,rgba(15,20,38,.84),rgba(6,10,20,.72))!important;border:1px solid transparent!important;box-shadow:0 25px 70px rgba(0,0,0,.35),inset 0 1px rgba(255,255,255,.04)!important;backdrop-filter:blur(20px) saturate(145%)!important;position:relative;overflow:hidden}}
.ring-card:before,.qr-card:before,.configs-card:before{{content:"";position:absolute;inset:0;border-radius:inherit;padding:1px;background:linear-gradient(120deg,rgba(0,245,255,.28),transparent 30%,rgba(139,92,246,.20) 55%,rgba(255,43,214,.28));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none}}
.ring-card{{border-radius:28px!important;padding:32px 28px!important}}.ring-wrap{{width:190px!important;height:190px!important}}.ring-svg{{width:190px!important;height:190px!important}}.ring-pct{{font-size:38px!important;background:linear-gradient(90deg,#67e8f9,#fff,#ff8be9);-webkit-background-clip:text;background-clip:text;color:transparent!important}}.ring-label{{letter-spacing:3px!important;color:#67e8f9!important}}.ring-bg{{stroke:rgba(255,255,255,.05)!important}}.ring-fill{{filter:drop-shadow(0 0 10px #00f5ff)!important}}
.info-row{{gap:10px!important}}.info-box{{background:rgba(0,245,255,.035)!important;border-color:rgba(0,245,255,.10)!important;border-radius:14px!important;padding:13px!important}}.info-box-val.gold{{color:#c4b5fd!important}}.info-box-val.green{{color:#67f5a1!important}}
.qr-card{{border-radius:24px!important;padding:26px!important}}.qr-wrap{{padding:12px!important;border-radius:18px!important;box-shadow:0 0 35px rgba(0,245,255,.16)!important}}.sub-link-display{{background:linear-gradient(90deg,rgba(0,245,255,.07),rgba(139,92,246,.08),rgba(255,43,214,.07))!important;border-color:rgba(0,245,255,.14)!important;color:#67e8f9!important;border-radius:13px!important;padding:11px 13px!important}}.copy-sub-btn{{background:linear-gradient(110deg,#00d9ff,#8b5cf6,#ff2bd6,#00d9ff)!important;background-size:300% 100%!important;animation:subRgb 5s linear infinite!important;color:#020617!important;border-radius:13px!important;box-shadow:0 0 28px rgba(0,245,255,.18)!important}}@keyframes subRgb{{to{{background-position:300% 0}}}}
.section-label{{color:#67e8f9!important;letter-spacing:3px!important;margin:24px 0 11px!important}}.platform-chips{{gap:8px!important}}.chip{{background:rgba(10,14,27,.7)!important;border-color:rgba(0,245,255,.11)!important;border-radius:999px!important;padding:9px 14px!important}}.chip.active,.chip:hover{{color:#fff!important;border-color:#8b5cf6!important;background:linear-gradient(90deg,rgba(0,245,255,.10),rgba(139,92,246,.10),rgba(255,43,214,.08))!important;box-shadow:0 0 18px rgba(139,92,246,.10)!important}}
.apps-grid{{grid-template-columns:repeat(3,1fr)!important;gap:11px!important}}.app-card{{background:rgba(10,14,27,.68)!important;border-color:rgba(0,245,255,.11)!important;border-radius:17px!important;padding:16px!important}}.app-card:hover{{border-color:#ff2bd6!important;box-shadow:0 0 26px rgba(255,43,214,.12)!important}}.app-icon{{border-radius:11px!important;background:linear-gradient(135deg,rgba(0,245,255,.10),rgba(139,92,246,.12))!important}}.app-name{{color:#fff!important}}.app-action{{color:rgba(203,213,225,.5)!important}}
.configs-card{{border-radius:24px!important;padding:20px!important}}.config-item{{background:rgba(0,245,255,.025)!important;border-color:rgba(0,245,255,.08)!important;border-radius:14px!important;padding:13px!important}}.config-icon{{background:linear-gradient(135deg,rgba(0,245,255,.11),rgba(255,43,214,.10))!important}}.btn-copy{{border-color:rgba(0,245,255,.16)!important;background:rgba(0,245,255,.07)!important;color:#67e8f9!important;border-radius:9px!important}}.btn-qr{{border-color:rgba(139,92,246,.22)!important;background:rgba(139,92,246,.09)!important;border-radius:9px!important}}.ping-btn{{background:linear-gradient(90deg,rgba(0,245,255,.06),rgba(139,92,246,.07),rgba(255,43,214,.06))!important;border-color:rgba(0,245,255,.14)!important;color:#67e8f9!important;border-radius:13px!important}}
.footer-links{{border-top-color:rgba(0,245,255,.08)!important}}.footer-link{{color:rgba(203,213,225,.5)!important}}.footer-link:hover{{color:#67e8f9!important}}
@media(max-width:560px){{.container{{padding:18px 14px 50px!important}}.header{{padding:12px 2px 18px!important}}.header-title{{font-size:24px!important}}.ring-card{{padding:24px 18px!important}}.apps-grid{{grid-template-columns:1fr 1fr!important}}.config-item{{align-items:flex-start}}.config-actions{{flex-direction:column}}.btn-copy,.btn-qr{{width:100%}}}}


        /* Header */
        .header{{text-align:center;padding:24px 0 20px}}
        .header-logo{{display:inline-flex;align-items:center;gap:10px;margin-bottom:8px}}
        .header-title{{font-size:22px;font-weight:900;letter-spacing:3px;
            background:linear-gradient(135deg,#fff,var(--gold));
            -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
        .header-sub{{font-size:11px;color:var(--text3);letter-spacing:2px;text-transform:uppercase}}

        /* Usage ring card */
        .ring-card{{background:var(--surface2);border:1px solid var(--border);border-radius:20px;
            padding:28px 24px;margin-bottom:14px;text-align:center;
            box-shadow:0 4px 24px rgba(0,0,0,0.4),inset 0 1px 0 rgba(200,30,45,0.08)}}
        .ring-wrap{{position:relative;width:160px;height:160px;margin:0 auto 20px}}
        .ring-svg{{width:160px;height:160px;transform:rotate(-90deg)}}
        .ring-bg{{fill:none;stroke:rgba(200,30,45,0.08);stroke-width:10}}
        .ring-fill{{fill:none;stroke-width:10;stroke-linecap:round;
            stroke-dasharray:440;stroke-dashoffset:{440 - (440 * min(pct,100)/100):.1f};
            stroke:url(#ringGrad);filter:drop-shadow(0 0 8px {ring_color1});
            transition:stroke-dashoffset 1s ease}}
        .ring-center{{position:absolute;inset:0;display:flex;flex-direction:column;
            align-items:center;justify-content:center}}
        .ring-pct{{font-size:32px;font-weight:900;color:#fff;letter-spacing:-1px}}
        .ring-label{{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:2px;text-transform:uppercase;margin-top:2px}}

        .usage-nums{{font-size:20px;font-weight:700;margin-bottom:4px}}
        .usage-nums span{{color:var(--text3);font-size:14px;font-weight:400}}
        .usage-sub{{font-size:11px;color:var(--text3)}}

        .info-row{{display:flex;gap:12px;margin-top:18px}}
        .info-box{{flex:1;background:rgba(200,30,45,0.05);border:1px solid rgba(200,30,45,0.1);
            border-radius:10px;padding:10px 12px;text-align:left}}
        .info-box-label{{font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px}}
        .info-box-val{{font-size:13px;font-weight:700}}
        .info-box-val.green{{color:var(--green)}}
        .info-box-val.red{{color:var(--red)}}
        .info-box-val.gold{{color:var(--gold)}}
        .info-box-sub{{font-size:10px;color:var(--text3);margin-top:1px}}

        /* QR card */
        .qr-card{{background:var(--surface2);border:1px solid var(--border);border-radius:20px;
            padding:24px;margin-bottom:14px;text-align:center;
            box-shadow:0 4px 24px rgba(0,0,0,0.4)}}
        .qr-wrap{{background:#fff;border-radius:12px;padding:12px;display:inline-block;
            box-shadow:0 0 24px rgba(200,30,45,0.2);margin-bottom:14px}}
        .qr-wrap img{{width:180px;height:180px;display:block;border-radius:4px}}
        .qr-label{{font-size:9px;letter-spacing:2px;color:var(--text3);text-transform:uppercase;margin-bottom:4px}}
        .sub-link-display{{font-size:11px;color:var(--gold);font-weight:600;
            background:var(--gold-dim);border:1px solid var(--border);border-radius:8px;
            padding:8px 12px;word-break:break-all;cursor:pointer;transition:all .2s}}
        .sub-link-display:hover{{background:rgba(200,30,45,0.15);border-color:var(--border2)}}
        .copy-sub-btn{{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;
            padding:12px;border-radius:10px;margin-top:10px;cursor:pointer;border:none;font-family:inherit;
            font-size:14px;font-weight:700;
            background:linear-gradient(135deg,var(--gold),var(--gold2));color:#000;
            box-shadow:0 0 20px rgba(200,30,45,0.25);transition:all .2s}}
        .copy-sub-btn:hover{{filter:brightness(1.1);box-shadow:0 0 30px rgba(200,30,45,0.4)}}

        /* Platform chips */
        .section-label{{font-size:9px;font-weight:800;letter-spacing:2px;color:var(--text3);
            text-transform:uppercase;margin:20px 0 10px}}
        .platform-chips{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}}
        .chip{{padding:7px 14px;border-radius:20px;border:1px solid var(--border);
            background:var(--surface2);color:var(--text3);font-size:11px;font-weight:600;
            cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:5px}}
        .chip:hover,.chip.active{{background:var(--gold-dim);border-color:var(--border2);color:var(--gold)}}

        /* App cards */
        .apps-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}}
        .app-card{{background:var(--surface2);border:1px solid var(--border);border-radius:14px;
            padding:14px;cursor:pointer;transition:all .2s;text-decoration:none;display:block}}
        .app-card:hover{{border-color:var(--border2);background:rgba(13,22,38,0.98);
            box-shadow:0 0 16px rgba(200,30,45,0.1);transform:translateY(-2px)}}
        .app-icon{{width:36px;height:36px;border-radius:8px;margin-bottom:8px;
            display:flex;align-items:center;justify-content:center;font-size:20px}}
        .app-name{{font-size:13px;font-weight:700;color:var(--text);margin-bottom:2px}}
        .app-action{{font-size:10.5px;color:var(--text3)}}

        /* Config list */
        .configs-card{{background:var(--surface2);border:1px solid var(--border);border-radius:20px;
            padding:18px;margin-bottom:14px}}
        .configs-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}}
        .configs-title{{font-size:12px;font-weight:700;color:var(--text);letter-spacing:.5px}}
        .configs-count{{font-size:10px;color:var(--text3);background:var(--gold-dim);
            border:1px solid var(--border);border-radius:6px;padding:2px 8px}}
        .config-item{{display:flex;align-items:center;justify-content:space-between;
            background:rgba(200,30,45,0.04);border:1px solid rgba(200,30,45,0.08);
            border-radius:10px;padding:11px 12px;margin-bottom:8px;gap:8px}}
        .config-icon{{width:32px;height:32px;border-radius:8px;background:var(--gold-dim);
            display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:14px}}
        .config-info{{flex:1;min-width:0}}
        .config-name{{font-size:12.5px;font-weight:600;color:var(--text);
            overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
        .config-type{{font-size:10px;color:var(--text3);margin-top:1px}}
        .ping-badge{{margin-left:8px;font-weight:700}}
        .config-actions{{display:flex;gap:5px;flex-shrink:0}}
        .btn-copy{{padding:5px 10px;border-radius:7px;border:1px solid rgba(200,30,45,0.2);
            background:var(--gold-dim);color:var(--gold);font-size:10.5px;font-weight:700;
            cursor:pointer;transition:all .2s;font-family:inherit}}
        .btn-copy:hover{{background:rgba(200,30,45,0.2)}}
        .btn-qr{{padding:5px 10px;border-radius:7px;border:1px solid rgba(167,139,250,0.2);
            background:rgba(167,139,250,0.08);color:#a78bfa;font-size:10.5px;font-weight:700;
            cursor:pointer;transition:all .2s;font-family:inherit}}
        .btn-qr:hover{{background:rgba(167,139,250,0.15)}}

        /* Ping all btn */
        .ping-btn{{width:100%;padding:14px;border-radius:12px;border:1px solid rgba(74,222,128,0.2);
            background:rgba(74,222,128,0.08);color:var(--green);font-size:14px;font-weight:700;
            cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px;
            font-family:inherit;transition:all .2s;margin-bottom:14px}}
        .ping-btn:hover{{background:rgba(74,222,128,0.15);box-shadow:0 0 20px rgba(74,222,128,0.1)}}
        .ping-btn:disabled{{opacity:0.6;cursor:wait}}

        /* QR modal */
        .mo{{position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:200;display:none;
            align-items:center;justify-content:center;backdrop-filter:blur(8px)}}
        .mo.show{{display:flex}}
        .mo-box{{background:var(--surface2);border:1px solid var(--border2);border-radius:20px;
            padding:24px;width:90%;max-width:300px;text-align:center;position:relative;
            box-shadow:var(--gold-glow)}}
        .mo-box img{{max-width:200px;border-radius:8px;border:3px solid var(--border);margin:12px 0}}
        .mo-close{{position:absolute;top:12px;right:12px;background:var(--surface2);
            border:1px solid var(--border);color:var(--text3);width:28px;height:28px;
            border-radius:6px;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:14px}}
        .mo-title{{font-size:12px;font-weight:700;color:var(--gold);letter-spacing:1px;margin-bottom:4px}}

        /* Toast */
        .toast{{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(16px);
            background:var(--bg2);color:var(--gold);border:1px solid var(--border2);
            border-radius:10px;padding:10px 18px;font-size:13px;font-weight:600;
            opacity:0;transition:all .3s;z-index:999;backdrop-filter:blur(20px);
            box-shadow:var(--gold-glow)}}
        .toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}

        /* IMMINENT footer links */
        .footer-links{{display:flex;justify-content:center;gap:16px;padding:20px 0 10px}}
        .footer-link{{display:flex;align-items:center;gap:5px;color:var(--text3);
            font-size:11px;font-weight:600;text-decoration:none;transition:color .2s}}
        .footer-link:hover{{color:var(--gold)}}
    
/* RGB Neon GitHub Edition */
@keyframes rgbShift{{0%{{filter:hue-rotate(0deg)}}100%{{filter:hue-rotate(360deg)}}}}
@keyframes borderRGB{{0%,100%{{box-shadow:0 0 14px rgba(0,245,255,.35),inset 0 0 12px rgba(255,0,170,.08)}}33%{{box-shadow:0 0 18px rgba(124,58,237,.45),inset 0 0 12px rgba(0,245,255,.08)}}66%{{box-shadow:0 0 18px rgba(255,0,170,.45),inset 0 0 12px rgba(124,58,237,.08)}}}}
body::before{{content:"";position:fixed;inset:0;pointer-events:none;z-index:0;background:linear-gradient(120deg,rgba(0,245,255,.04),rgba(124,58,237,.04),rgba(255,0,170,.04));animation:rgbShift 12s linear infinite}}
.sidebar,.topbar,.card,.modal,.panel-card,.stat-card{{border-color:var(--border)!important;animation:borderRGB 5s ease-in-out infinite}}
.logo,.brand,.page-title,.nav-item.active{{background:linear-gradient(90deg,#00f5ff,#7c3aed,#ff00aa,#00f5ff);background-size:300% 100%;-webkit-background-clip:text;background-clip:text;color:transparent;animation:rgbText 6s linear infinite}}
@keyframes rgbText{{0%{{background-position:0%}}100%{{background-position:300%}}}}
button,.btn,.nav-item.active{{box-shadow:0 0 12px rgba(0,245,255,.22)}}

/* ==================== IMMINENT RGB NEON v3 UI ==================== */
:root{{
  --gold:#00f5ff;--gold2:#8b5cf6;--gold3:#ff2bd6;
  --gold-dim:rgba(0,245,255,.10);--gold-dim2:rgba(139,92,246,.08);
  --black:#03040a;--black2:#070914;--black3:#0b1020;
  --surface:rgba(10,14,27,.72);--surface2:rgba(15,20,38,.82);--surface3:rgba(22,28,50,.88);
  --border:rgba(0,245,255,.16);--border2:rgba(255,43,214,.34);
  --text:rgba(248,250,252,.96);--text2:#67e8f9;--text3:rgba(203,213,225,.52);
  --gold-glow:0 0 22px rgba(0,245,255,.28),0 0 55px rgba(255,43,214,.14);
  --nav-w:82px;
}}
html,body{{background:#03040a!important}}
body{{background:
 radial-gradient(900px 520px at 15% -10%,rgba(0,245,255,.09),transparent 60%),
 radial-gradient(800px 500px at 100% 20%,rgba(139,92,246,.10),transparent 60%),
 radial-gradient(700px 500px at 55% 110%,rgba(255,43,214,.08),transparent 60%)!important}}
body:before{{content:"";position:fixed;inset:-30%;pointer-events:none;z-index:-1;background:conic-gradient(from 0deg,transparent 0 18%,rgba(0,245,255,.035) 25%,transparent 34%,rgba(255,43,214,.035) 52%,transparent 66%,rgba(139,92,246,.035) 82%,transparent 92%);animation:imAurora 16s linear infinite}}
@keyframes imAurora{{to{{transform:rotate(360deg)}}}}
.bg-fixed{{background:radial-gradient(ellipse 60% 45% at 50% 0,rgba(0,245,255,.08),transparent 62%),radial-gradient(ellipse 45% 35% at 100% 90%,rgba(255,43,214,.07),transparent 58%)!important}}
.grid-fixed{{background-image:linear-gradient(rgba(0,245,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(255,43,214,.028) 1px,transparent 1px)!important;background-size:42px 42px!important;mask-image:linear-gradient(to bottom,#000,transparent 92%)}}
::-webkit-scrollbar-thumb{{background:linear-gradient(180deg,#00f5ff,#8b5cf6,#ff2bd6)!important}}

.sidebar{{width:var(--nav-w)!important;background:rgba(5,8,18,.72)!important;border-right:1px solid rgba(0,245,255,.12)!important;backdrop-filter:blur(26px) saturate(150%)!important;box-shadow:10px 0 50px rgba(0,0,0,.28),inset -1px 0 rgba(255,43,214,.08)!important}}
.sidebar:before{{content:"";position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#00f5ff,#8b5cf6,#ff2bd6,#00f5ff);background-size:300% 100%;animation:rgbLine 5s linear infinite}}
.sidebar:after{{background:linear-gradient(180deg,#00f5ff,transparent 28%,#8b5cf6 55%,transparent 78%,#ff2bd6)!important;opacity:.7}}
@keyframes rgbLine{{to{{background-position:300% 0}}}}
.sb-social{{padding-top:18px!important}}.sb-social-btn{{border-color:rgba(0,245,255,.12)!important;background:rgba(255,255,255,.025)!important}}.sb-social-btn:hover{{color:#fff!important;border-color:#ff2bd6!important;box-shadow:0 0 20px rgba(255,43,214,.18)!important}}
.sb-brand{{border-bottom-color:rgba(0,245,255,.10)!important;padding:18px 0!important}}.sb-title{{color:#fff!important;text-shadow:0 0 12px rgba(0,245,255,.45)}}
.nav-item{{border-radius:16px!important;margin:2px 0;color:rgba(203,213,225,.5)!important}}
.nav-item:before{{background:linear-gradient(135deg,rgba(0,245,255,.13),rgba(139,92,246,.08),rgba(255,43,214,.10))!important}}
.nav-item:hover,.nav-item.active{{color:#fff!important;border-color:rgba(0,245,255,.22)!important}}
.nav-item.active{{background:linear-gradient(135deg,rgba(0,245,255,.10),rgba(139,92,246,.09),rgba(255,43,214,.08))!important;box-shadow:0 0 24px rgba(0,245,255,.12),inset 0 0 22px rgba(139,92,246,.06)!important}}
.nav-item.active:after{{content:"";position:absolute;left:4px;top:20%;bottom:20%;width:3px;border-radius:4px;background:linear-gradient(#00f5ff,#8b5cf6,#ff2bd6);box-shadow:0 0 12px #00f5ff}}
.nav-icon{{filter:drop-shadow(0 0 5px rgba(0,245,255,.12))}}
.nav-item.active .nav-icon{{filter:drop-shadow(0 0 8px rgba(0,245,255,.65)) drop-shadow(0 0 12px rgba(255,43,214,.3))}}

.main{{margin-left:var(--nav-w)!important;max-width:1500px;padding:30px 34px 70px!important}}
.page{{animation:imPage .42s cubic-bezier(.2,.8,.2,1)!important}}@keyframes imPage{{from{{opacity:0;transform:translateY(14px) scale(.99)}}to{{opacity:1;transform:none}}}}
.page-header{{padding:8px 2px 18px;margin-bottom:22px!important;border-bottom:1px solid rgba(0,245,255,.08)}}
.page-title{{font-size:26px!important;font-weight:900!important;letter-spacing:.02em!important;background:linear-gradient(90deg,#fff 5%,#67e8f9 42%,#a78bfa 70%,#ff8be9 100%);-webkit-background-clip:text;background-clip:text;color:transparent!important;text-shadow:none}}
.page-sub{{color:rgba(203,213,225,.45)!important;font-size:11px!important}}

.stats-row{{gap:16px!important}}.stat-card,.card{{background:linear-gradient(145deg,rgba(15,20,38,.86),rgba(8,12,24,.68))!important;border:1px solid transparent!important;background-clip:padding-box!important;box-shadow:0 18px 55px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.035)!important;backdrop-filter:blur(18px) saturate(140%)!important}}
.stat-card:before,.card:before{{height:2px!important;background:linear-gradient(90deg,transparent,#00f5ff 25%,#8b5cf6 50%,#ff2bd6 75%,transparent)!important;opacity:.7!important;background-size:200% 100%!important;animation:rgbLine 6s linear infinite!important}}
.stat-card:after,.card:after{{content:"";position:absolute;inset:0;border-radius:inherit;padding:1px;background:linear-gradient(120deg,rgba(0,245,255,.24),transparent 32%,rgba(139,92,246,.18) 55%,rgba(255,43,214,.24));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;opacity:.7}}
.stat-card{{border-radius:20px!important;padding:20px!important}}.stat-card:hover,.card:hover{{transform:translateY(-3px)!important;box-shadow:0 22px 60px rgba(0,0,0,.34),0 0 35px rgba(0,245,255,.08),0 0 45px rgba(255,43,214,.06)!important}}
.stat-label{{color:rgba(203,213,225,.45)!important}}.stat-val{{font-size:25px!important;color:#fff!important}}.stat-unit{{color:#67e8f9!important}}
.card{{border-radius:20px!important;padding:20px!important}}.card-title{{font-weight:800!important;color:#f8fafc!important}}.card-hd{{border-bottom-color:rgba(0,245,255,.08)!important}}

.btn,.lang-btn,.theme-toggle,.fi,.fs{{border-radius:12px!important}}.btn-gold{{background:linear-gradient(110deg,#00d9ff,#8b5cf6,#ff2bd6,#00d9ff)!important;background-size:300% 100%!important;color:#020617!important;border:0!important;box-shadow:0 0 24px rgba(0,245,255,.18)!important;animation:rgbLine 5s linear infinite!important}}.btn-gold:hover{{filter:brightness(1.12)!important;box-shadow:0 0 32px rgba(0,245,255,.28),0 0 35px rgba(255,43,214,.14)!important}}
.btn-ghost,.btn-danger{{background:rgba(255,255,255,.025)!important;border-color:rgba(0,245,255,.12)!important}}.btn-ghost:hover{{border-color:#00f5ff!important;color:#67e8f9!important}}.btn-danger:hover{{border-color:#ff2bd6!important}}
.fi,.fs{{background:rgba(2,6,23,.72)!important;border-color:rgba(0,245,255,.12)!important;color:#f8fafc!important;box-shadow:inset 0 0 20px rgba(0,0,0,.12)!important}}.fi:focus,.fs:focus{{border-color:#00f5ff!important;box-shadow:0 0 0 3px rgba(0,245,255,.07),0 0 24px rgba(0,245,255,.08)!important}}
.pill-fill-gold{{background:linear-gradient(90deg,#00f5ff,#8b5cf6,#ff2bd6)!important;background-size:200% 100%!important;animation:rgbLine 5s linear infinite}}

.mob-hd{{background:rgba(4,7,16,.72)!important;border-bottom:1px solid rgba(0,245,255,.10)!important;backdrop-filter:blur(24px)!important;position:fixed!important;top:0;left:0;right:0;z-index:120}}.mob-hd>span{{background:linear-gradient(90deg,#67e8f9,#a78bfa,#ff8be9);-webkit-background-clip:text;background-clip:text;color:transparent!important}}

.login-wrap{{background:radial-gradient(circle at 50% 20%,rgba(0,245,255,.10),transparent 30%),radial-gradient(circle at 80% 80%,rgba(255,43,214,.08),transparent 30%)!important}}
.login-box{{background:rgba(8,12,24,.72)!important;border:1px solid rgba(0,245,255,.16)!important;border-radius:28px!important;box-shadow:0 30px 100px rgba(0,0,0,.5),0 0 60px rgba(0,245,255,.06)!important;backdrop-filter:blur(25px)!important;position:relative;overflow:hidden}}
.login-box:before{{content:"";position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#00f5ff,#8b5cf6,#ff2bd6,#00f5ff);background-size:300% 100%;animation:rgbLine 5s linear infinite}}
.login-title{{color:#fff!important;background:linear-gradient(90deg,#67e8f9,#a78bfa,#ff8be9);-webkit-background-clip:text;background-clip:text;color:transparent!important}}.login-sub{{color:rgba(203,213,225,.48)!important}}

@media(max-width:768px){{
 .main{{padding:92px 16px 110px!important}}
 .page-header{{padding-top:0!important}}
 .page-title{{font-size:28px!important}}
 .stats-row{{grid-template-columns:1fr 1fr!important;gap:12px!important}}
 .stat-card{{padding:17px!important;min-height:112px}}
 .stat-val{{font-size:22px!important}}
 .card{{padding:18px!important}}
 .sidebar{{height:84px!important;background:rgba(4,7,16,.86)!important;backdrop-filter:blur(24px)!important;border-top:1px solid rgba(0,245,255,.13)!important}}
 .sidebar:before{{top:0;height:2px}}
 .sb-nav{{padding:5px 6px 7px!important}}
 .nav-item{{padding:8px 1px!important;border-radius:13px!important}}
 .nav-icon{{width:21px!important;height:21px!important;margin-bottom:3px!important}}
 .nav-label{{font-size:9px!important}}
 .nav-item.active:after{{left:50%;top:3px;bottom:auto;width:22px;height:2px;transform:translateX(-50%);background:linear-gradient(90deg,#00f5ff,#ff2bd6)!important}}
 .mob-hd{{height:76px!important;padding:0 14px!important}}
 .mob-hd span{{font-size:19px!important}}
 .mob-tl-group{{gap:6px!important}}
}}
@media(max-width:460px){{.stats-row{{grid-template-columns:1fr 1fr!important}}.stat-card{{min-height:105px}}}}

</style>
</head>
<body>
<div class="bg-glow"></div>
<div class="grid-bg"></div>
<div class="starfield" id="starfield"></div>
<div class="shooting-stars"><span class="star"></span><span class="star"></span><span class="star"></span><span class="star"></span><span class="star"></span></div>
<div class="toast" id="toast"></div>

<div class="container">

    <!-- Header -->
    <div class="music-dock" id="music-dock">
        <div class="music-orb">♫</div>
        <div class="music-copy"><b>غروب · سیاوش قمیشی</b><span>Background music</span></div>
        <button class="music-btn" id="music-btn" onclick="toggleMusic()">▶</button>
        <audio id="bg-audio" preload="auto" loop src="/client/audio/ghorob.mp3"></audio>
    </div>

    <div class="header">
        <div class="header-logo">
            <svg width="28" height="24" viewBox="0 0 84 68" fill="none">
                <ellipse cx="42" cy="52" rx="40" ry="11" fill="#7A0F1F" opacity=".85"/>
                <ellipse cx="42" cy="52" rx="40" ry="11" fill="none" stroke="#C81E2D" stroke-width="1.4" opacity=".6"/>
                <path d="M19 50 Q21 22 42 17 Q63 22 65 50" fill="#4a3a00" stroke="#C81E2D" stroke-width="1.4"/>
                <ellipse cx="42" cy="17" rx="23" ry="5.5" fill="#7A0F1F" stroke="#C81E2D" stroke-width="1"/>
                <path d="M20 45 Q21.5 41.5 42 39.5 Q62.5 41.5 64 45" fill="none" stroke="#CC2200" stroke-width="4.5" stroke-linecap="round" opacity=".92"/>
            </svg>
            <span class="header-title">AXIOM</span>
        </div>
        <div class="header-sub">{link['label']} · Connection Status</div>
    </div>

    <!-- Usage Ring Card -->
    <div class="ring-card">
        <div class="ring-wrap">
            <svg class="ring-svg" viewBox="0 0 160 160">
                <defs>
                    <linearGradient id="ringGrad" x1="0%" y1="0%" x2="100%" y2="0%">
                        <stop offset="0%" style="stop-color:{ring_color1}"/>
                        <stop offset="100%" style="stop-color:{ring_color2}"/>
                    </linearGradient>
                </defs>
                <circle class="ring-bg" cx="80" cy="80" r="70"/>
                <circle class="ring-fill" cx="80" cy="80" r="70"/>
            </svg>
            <div class="ring-center">
                <div class="ring-pct">{pct:.0f}%</div>
                <div class="ring-label">USED</div>
            </div>
        </div>

        <div class="usage-nums">
            {_fmt_bytes(used)} <span>/ {_fmt_bytes(limit) if limit > 0 else '∞'}</span>
        </div>
        <div class="usage-sub">{rem_str} remaining</div>

        <div class="info-row">
            <div class="info-box">
                <div class="info-box-label">Status</div>
                <div class="info-box-val {'green' if is_active else 'red'}">{status_text}</div>
            </div>
            <div class="info-box">
                <div class="info-box-label">Expires</div>
                <div class="info-box-val gold">{expiry_str}</div>
                <div class="info-box-sub">{expiry_date_str}</div>
            </div>
        </div>
    </div>

    <!-- QR Code Card -->
    <div class="qr-card">
        <div class="qr-label">Scan to Add</div>
        <div class="qr-wrap">
            <img src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&color=000000&bgcolor=ffffff&data={quote(sub_url)}" alt="QR">
        </div>
        <div class="qr-label">Subscription Link</div>
        <div class="sub-link-display" onclick="copySub()">{get_domain()}/sub/{uid}</div>
        <button class="copy-sub-btn" onclick="copySub()">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
            Copy Subscription Link
        </button>
    </div>

    <!-- Easy Import Section -->
    <div class="section-label">Easy Import</div>
    <div class="platform-chips" id="platform-chips">
        <div class="chip active" onclick="setPlatform('Android',this)"><svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor" fill-rule="evenodd" style="vertical-align:-2px;margin-right:3px"><path d="M7.2 8h9.6a5 5 0 0 0-2-3.5l1-1.7a.35.35 0 0 0-.6-.35l-1.05 1.8A5.6 5.6 0 0 0 12 3.7c-.78 0-1.5.15-2.15.4L8.8 2.3a.35.35 0 0 0-.6.35l1 1.7A5 5 0 0 0 7.2 8zm2.55-1.6a.8.8 0 1 1 0-1.6.8.8 0 0 1 0 1.6zm4.5 0a.8.8 0 1 1 0-1.6.8.8 0 0 1 0 1.6zM6.5 9.2h11v8.3a1 1 0 0 1-1 1h-1.2v2.8a1.3 1.3 0 0 1-2.6 0v-2.8h-1.4v2.8a1.3 1.3 0 0 1-2.6 0v-2.8H7.5a1 1 0 0 1-1-1V9.2zM4 9.2a1.3 1.3 0 0 1 1.3 1.3v4.8a1.3 1.3 0 0 1-2.6 0v-4.8A1.3 1.3 0 0 1 4 9.2zm16 0a1.3 1.3 0 0 1 1.3 1.3v4.8a1.3 1.3 0 0 1-2.6 0v-4.8A1.3 1.3 0 0 1 20 9.2z"/></svg> Android</div>
        <div class="chip" onclick="setPlatform('iOS',this)"><svg width="12" height="15" viewBox="0 0 384 512" fill="currentColor" style="vertical-align:-2px;margin-right:3px"><path d="M318.7 268.7c-.2-36.7 16.4-64.4 50-84.8-18.8-26.9-47.2-41.7-84.7-44.6-35.5-2.8-74.3 20.7-88.5 20.7-15 0-49.4-19.7-76.4-19.7C63.3 141.2 4 184.8 4 273.5q0 39.3 14.4 81.2c12.8 36.7 59 126.7 107.2 125.2 25.2-.6 43-17.9 75.8-17.9 31.8 0 48.3 17.9 76.4 17.9 48.6-.7 90.4-82.5 102.6-119.3-65.2-30.7-61.7-90-61.7-91.9zm-56.6-164.2c27.3-32.4 24.8-61.9 24-72.5-24.1 1.4-52 16.4-67.9 34.9-17.5 19.8-27.8 44.3-25.6 71.9 26.1 2 49.9-11.4 69.5-34.3z"/></svg> iOS</div>
        <div class="chip" onclick="setPlatform('Windows',this)"><svg width="14" height="14" viewBox="0 0 448 512" fill="currentColor" style="vertical-align:-2px;margin-right:3px"><path d="M0 93.7l183.6-25.3v177.4H0V93.7zm0 324.6l183.6 25.3V268.4H0v149.9zm203.8 28L448 480V268.4H203.8v177.9zm0-380.6v180.1H448V32L203.8 65.7z"/></svg> Windows</div>
        <div class="chip" onclick="setPlatform('macOS',this)"><svg width="12" height="15" viewBox="0 0 384 512" fill="currentColor" style="vertical-align:-2px;margin-right:3px"><path d="M318.7 268.7c-.2-36.7 16.4-64.4 50-84.8-18.8-26.9-47.2-41.7-84.7-44.6-35.5-2.8-74.3 20.7-88.5 20.7-15 0-49.4-19.7-76.4-19.7C63.3 141.2 4 184.8 4 273.5q0 39.3 14.4 81.2c12.8 36.7 59 126.7 107.2 125.2 25.2-.6 43-17.9 75.8-17.9 31.8 0 48.3 17.9 76.4 17.9 48.6-.7 90.4-82.5 102.6-119.3-65.2-30.7-61.7-90-61.7-91.9zm-56.6-164.2c27.3-32.4 24.8-61.9 24-72.5-24.1 1.4-52 16.4-67.9 34.9-17.5 19.8-27.8 44.3-25.6 71.9 26.1 2 49.9-11.4 69.5-34.3z"/></svg> macOS</div>
        <div class="chip" onclick="setPlatform('Linux',this)"><svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor" fill-rule="evenodd" style="vertical-align:-2px;margin-right:3px"><path d="M12 2c-2.6 0-4.3 2.1-4.3 4.8v4.4c0 1.3-.7 2.4-1.7 3.6-1.2 1.5-2.3 2.9-2.3 4.3 0 1.1.9 1.8 2 1.5l2.8-.8c.5 1.1 2 1.9 3.5 1.9s3-.8 3.5-1.9l2.8.8c1.1.3 2-.4 2-1.5 0-1.4-1.1-2.8-2.3-4.3-1-1.2-1.7-2.3-1.7-3.6V6.8C16.3 4.1 14.6 2 12 2zm-1.7 4.6a.9.9 0 1 1 0 1.8.9.9 0 0 1 0-1.8zm3.4 0a.9.9 0 1 1 0 1.8.9.9 0 0 1 0-1.8zM12 8.9l1.6 1.1c.3.2.3.6 0 .8L12 11.9l-1.6-1.1c-.3-.2-.3-.6 0-.8L12 8.9z"/></svg> Linux</div>
        <div class="chip" onclick="setPlatform('AndroidTV',this)"><svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor" style="vertical-align:-2px;margin-right:3px"><path d="M4 4h16a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-5v1.6l2.5 1.4v1h-11v-1L9 18.6V17H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2zm0 2v9h16V6H4z"/></svg> Android TV</div>
        <div class="chip" onclick="setPlatform('AppleTV',this)"><svg width="12" height="15" viewBox="0 0 384 512" fill="currentColor" style="vertical-align:-2px;margin-right:3px"><path d="M318.7 268.7c-.2-36.7 16.4-64.4 50-84.8-18.8-26.9-47.2-41.7-84.7-44.6-35.5-2.8-74.3 20.7-88.5 20.7-15 0-49.4-19.7-76.4-19.7C63.3 141.2 4 184.8 4 273.5q0 39.3 14.4 81.2c12.8 36.7 59 126.7 107.2 125.2 25.2-.6 43-17.9 75.8-17.9 31.8 0 48.3 17.9 76.4 17.9 48.6-.7 90.4-82.5 102.6-119.3-65.2-30.7-61.7-90-61.7-91.9zm-56.6-164.2c27.3-32.4 24.8-61.9 24-72.5-24.1 1.4-52 16.4-67.9 34.9-17.5 19.8-27.8 44.3-25.6 71.9 26.1 2 49.9-11.4 69.5-34.3z"/></svg> Apple TV</div>
    </div>

    <div id="apps-container" class="apps-grid"></div>

    <!-- Configs -->
    <div class="configs-card">
        <div class="configs-header">
            <div class="configs-title">CONFIGS</div>
            <div class="configs-count" id="configs-count">0 configs</div>
        </div>
        <button class="ping-btn" id="ping-all-btn" onclick="pingAll()">⚡ Ping test all</button>
        <div id="config-list"></div>
    </div>

    <!-- Footer links -->
    <div class="footer-links">
        <a href="https://t.me/IMMINENT_SHOP" target="_blank" class="footer-link">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.562 8.248l-2.032 9.57c-.148.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12l-6.871 4.326-2.962-.924c-.643-.204-.657-.643.136-.953l11.57-4.461c.537-.194 1.006.131.895.651z"/></svg>
            Telegram Channel
        </a>
    </div>

</div>

<!-- QR Modal -->
<div class="mo" id="qr-modal" onclick="if(event.target===this)this.classList.remove('show')">
    <div class="mo-box">
        <button class="mo-close" onclick="document.getElementById('qr-modal').classList.remove('show')">✕</button>
        <div class="mo-title">QR CODE</div>
        <img id="qr-modal-img" src="" alt="QR">
        <div id="qr-modal-name" style="font-size:11px;color:rgba(255,255,255,0.4);margin-bottom:8px"></div>
        <button onclick="downloadQR()" style="width:100%;padding:10px;border-radius:8px;background:linear-gradient(135deg,#C81E2D,#A8172A);border:none;color:#000;font-weight:700;font-size:13px;cursor:pointer;font-family:inherit">Download QR</button>
    </div>
</div>

<script>
    const configs = {configs_json};
    const subUrl = "https://{get_domain()}/sub/{uid}";
    (function(){{
        var sf=document.getElementById('starfield');
        if(!sf)return;
        var n=window.innerWidth<600?70:130;
        var h='';
        for(var i=0;i<n;i++){{
            var sz=(Math.random()*1.8+0.5).toFixed(2);
            h+='<span class="s" style="width:'+sz+'px;height:'+sz+'px;top:'+(Math.random()*100).toFixed(2)+'%;left:'+(Math.random()*100).toFixed(2)+'%;animation-duration:'+(Math.random()*3+1.8).toFixed(2)+'s;animation-delay:'+(Math.random()*4).toFixed(2)+'s;opacity:'+(Math.random()*0.5+0.3).toFixed(2)+'"></span>';
        }}
        sf.innerHTML=h;
    }})();
    // Hiddify's own URL Scheme spec is: hiddify://import/<sublink>#<name>
    // The #name fragment is what Hiddify shows as the profile name before
    // it even fetches the sublink, and is used as a fallback if the
    // content's own #profile-title header is missing or fails to parse.
    const hiddifyProfileName = encodeURIComponent("IMMINENT-{link['label']}");
    const hiddifyImportUrl = "hiddify://import/" + subUrl + "#" + hiddifyProfileName;

    // Returns URL to the PNG icon for the given app name.
    // Falls back to SVG initials if the PNG file does not exist.
    function appIcon(name, bg) {{
        const encoded = encodeURIComponent(name) + '.png';
        return '/client/' + encoded;
    }}

    function appIconFallback(name, bg) {{
        const initials = name.replace(/[^A-Za-z0-9 ]/g,'').trim().split(/\\s+/).map(w => w[0]).join('').substring(0,2).toUpperCase();
        const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="72" height="72">`
            + `<rect width="72" height="72" rx="18" fill="${{bg}}"/>`
            + `<text x="36" y="47" font-family="Arial,Helvetica,sans-serif" font-size="26" font-weight="700" fill="#fff" text-anchor="middle">${{initials}}</text>`
            + `</svg>`;
        return 'data:image/svg+xml;utf8,' + encodeURIComponent(svg);
    }}

    const APPS = {{
        Android: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
            {{name:"v2rayNG", color:"#16A34A", action:"Tap to open", url:"v2rayng://install-sub?url=" + encodeURIComponent(subUrl)}},
            {{name:"V2Box", color:"#F97316", action:"Tap to open", url:"v2box://install-sub?url=" + encodeURIComponent(subUrl)}},
            {{name:"Happ", color:"#7C3AED", action:"Tap to open", url:"happ://add/" + encodeURIComponent(subUrl)}},
            {{name:"NPV Tunnel", color:"#475569", action:"Tap to copy link", url:null}},
            {{name:"clash mi", color:"#DC2626", action:"Tap to open", url:"clash://install-config?url=" + encodeURIComponent(subUrl), fallbackUrl:"clashmeta://install-config?url=" + encodeURIComponent(subUrl)}},
        ],
        iOS: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
            {{name:"Happ", color:"#7C3AED", action:"Tap to open", url:"happ://add/" + encodeURIComponent(subUrl)}},
            {{name:"clash mi", color:"#DC2626", action:"Tap to open", url:"clash://install-config?url=" + encodeURIComponent(subUrl), fallbackUrl:"clashmeta://install-config?url=" + encodeURIComponent(subUrl)}},
        ],
        Windows: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
            {{name:"v2rayN", color:"#16A34A", action:"Tap to copy link", url:null}},
            {{name:"clash mi", color:"#DC2626", action:"Tap to open", url:"clash://install-config?url=" + encodeURIComponent(subUrl)}},
        ],
        macOS: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
        ],
        Linux: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
            {{name:"clash mi", color:"#DC2626", action:"Tap to open", url:"clash://install-config?url=" + encodeURIComponent(subUrl)}},
        ],
        AndroidTV: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
            {{name:"V2Box", color:"#F97316", action:"Tap to open", url:"v2box://install-sub?url=" + encodeURIComponent(subUrl)}},
        ],
        AppleTV: [
            {{name:"Hiddify", color:"#2F6FED", action:"Tap to open", url:hiddifyImportUrl}},
        ],
    }};

    let currentPlatform = 'Android';

    function setPlatform(p, el) {{
        currentPlatform = p;
        document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
        if(el) el.classList.add('active');
        renderApps();
    }}

    function renderApps() {{
        const apps = APPS[currentPlatform] || [];
        const container = document.getElementById('apps-container');
        container.innerHTML = apps.map(a => `
            <div class="app-card" onclick="openApp('${{a.url || ''}}', '${{a.name}}', '${{a.fallbackUrl || ''}}')">
                <img src="${{appIcon(a.name, a.color)}}" alt="app"
                    onerror="this.onerror=null;this.src=appIconFallback('${{a.name}}','${{a.color}}')"
                    style="width:36px;height:36px;border-radius:8px;margin-bottom:8px;display:block">
                <div class="app-name">${{a.name}}</div>
                <div class="app-action">${{a.action}}</div>
            </div>
        `).join('');
    }}

    // Tries to open an app via custom URL scheme. If the app doesn't take over
    // the page within a short window (meaning it isn't installed or the scheme
    // didn't register), tries a fallback scheme, and if that also fails,
    // copies the subscription link so the user can paste it manually.
    function tryOpenScheme(url, onFail) {{
        let didHide = false;
        const onVisibilityChange = () => {{ if (document.hidden) didHide = true; }};
        document.addEventListener('visibilitychange', onVisibilityChange);
        window.addEventListener('blur', onVisibilityChange, {{ once: true }});

        window.location.href = url;

        setTimeout(() => {{
            document.removeEventListener('visibilitychange', onVisibilityChange);
            if (!didHide) {{
                onFail();
            }}
        }}, 1500);
    }}

    function fallbackCopy(text) {{
        try {{
            var ta = document.createElement('textarea');
            ta.value = text; ta.style.position = 'fixed'; ta.style.left = '-9999px';
            document.body.appendChild(ta); ta.focus(); ta.select();
            document.execCommand('copy'); document.body.removeChild(ta);
        }} catch (e) {{}}
    }}
    function safeCopy(text) {{
        try {{
            if (navigator.clipboard && window.isSecureContext) {{
                navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
                return;
            }}
        }} catch (e) {{}}
        fallbackCopy(text);
    }}
    function openApp(url, name, fallbackUrl) {{
        if(!url) {{
            safeCopy(subUrl);
            showToast('لینک اشتراک کپی شد - ' + name + ' را باز کن و لینک را در آن پیست کن');
            return;
        }}

        safeCopy(subUrl);
        showToast('در حال باز کردن ' + name + ' - اگر خودکار اضافه نشد، لینک کپی شده؛ داخل اپ پیستش کن');

        tryOpenScheme(url, () => {{
            if (fallbackUrl) {{
                tryOpenScheme(fallbackUrl, () => {{
                    safeCopy(subUrl);
                    showToast(name + ' not detected - subscription link copied, paste it inside the app');
                }});
            }} else {{
                safeCopy(subUrl);
                showToast(name + ' not detected - subscription link copied, paste it inside the app');
            }}
        }});
    }}

    // از خودِ رشته‌ی share-link (vless:// یا trojan://) نوع پروتکل/ترابرد/امنیت واقعی رو تشخیص می‌ده
    function configBadge(cfg) {{
        try {{
            const scheme = cfg.split('://')[0].toUpperCase();
            const qIdx = cfg.indexOf('?');
            const hIdx = cfg.indexOf('#');
            const query = cfg.substring(qIdx + 1, hIdx === -1 ? undefined : hIdx);
            const params = new URLSearchParams(query);
            const type = (params.get('type') || 'ws').toLowerCase();
            const mode = (params.get('mode') || '').toLowerCase();
            const security = (params.get('security') || '').toLowerCase();
            let transportLabel = type === 'xhttp' ? ('XHTTP' + (mode ? ' (' + mode + ')' : '')) : type.toUpperCase();
            let secLabel = security === 'tls' ? 'TLS' : (security ? security.toUpperCase() : '');
            return [scheme, transportLabel, secLabel].filter(Boolean).join(' · ');
        }} catch (e) {{
            return 'VLESS · WS · TLS';
        }}
    }}

    // Render configs
    function renderConfigs() {{
        const list = document.getElementById('config-list');
        document.getElementById('configs-count').textContent = configs.length + ' config' + (configs.length !== 1 ? 's' : '');
        list.innerHTML = configs.map((cfg, i) => {{
            const parts = cfg.split('#');
            const remark = parts[1] ? decodeURIComponent(parts[1]) : 'Config ' + (i+1);
            return `
                <div class="config-item">
                    <div class="config-icon">🌐</div>
                    <div class="config-info">
                        <div class="config-name">${{remark}}</div>
                        <div class="config-type">${{configBadge(cfg)}} <span class="ping-badge" id="ping-badge-${{i}}">-</span></div>
                    </div>
                    <div class="config-actions">
                        <button class="btn-copy" onclick="copyConfig('${{cfg.replace(/'/g,"\\'")}}')" title="Copy">Copy</button>
                        <button class="btn-qr" onclick="showQR('${{cfg.replace(/'/g,"\\'")}}',' ${{remark}}')" title="QR">QR</button>
                    </div>
                </div>
            `;
        }}).join('');
    }}

    function copySub() {{
        safeCopy(subUrl);
        showToast('Subscription link copied!');
    }}

    function copyConfig(txt) {{
        safeCopy(txt);
        showToast('Config copied!');
    }}

    function showQR(txt, name) {{
        document.getElementById('qr-modal-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=250x250&data=' + encodeURIComponent(txt);
        document.getElementById('qr-modal-name').textContent = name || '';
        document.getElementById('qr-modal').classList.add('show');
    }}

    function downloadQR() {{
        const a = document.createElement('a');
        a.href = document.getElementById('qr-modal-img').src;
        a.download = 'imminent-config-qr.png';
        a.click();
    }}

    // Extracts host:port from a vless:// config link
    function parseHostPort(cfg) {{
        const m = cfg.match(/@([^:/?#]+):(\\d+)/);
        return m ? {{host: m[1], port: m[2]}} : null;
    }}

    // Asks the panel's own server to test connectivity to a config's host,
    // so the ping result reflects the server's real network path (and isn't
    // limited/blocked by the visitor's browser CORS rules).
    async function pingHost(host, port) {{
        try {{
            const r = await fetch('/api/ping-check?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port));
            if (!r.ok) return null;
            const d = await r.json();
            return d.reachable ? d.ms : null;
        }} catch (e) {{
            return null;
        }}
    }}

    async function pingAll() {{
        const btn = document.getElementById('ping-all-btn');
        if (btn) {{ btn.disabled = true; btn.textContent = '⏳ Testing...'; }}
        showToast('Testing ping for all configs...');

        await Promise.all(configs.map(async (cfg, i) => {{
            const badge = document.getElementById('ping-badge-' + i);
            if (badge) {{ badge.textContent = '...'; badge.style.color = 'var(--text3)'; }}
            const hp = parseHostPort(cfg);
            if (!hp) {{
                if (badge) {{ badge.textContent = 'N/A'; badge.style.color = 'var(--text3)'; }}
                return;
            }}
            const ms = await pingHost(hp.host, hp.port);
            if (!badge) return;
            if (ms === null) {{
                badge.textContent = 'Timeout';
                badge.style.color = 'var(--red)';
            }} else {{
                badge.textContent = ms + ' ms';
                badge.style.color = ms < 150 ? 'var(--green)' : ms < 400 ? 'var(--yellow)' : 'var(--red)';
            }}
        }}));

        if (btn) {{ btn.disabled = false; btn.textContent = '⚡ Ping test all'; }}
        showToast('Ping test complete');
    }}

    function showToast(msg) {{
        const t = document.getElementById('toast');
        t.textContent = msg;
        t.className = 'toast show';
        clearTimeout(t._t);
        t._t = setTimeout(() => t.className = 'toast', 2500);
    }}

    renderApps();
    const bgAudio = document.getElementById('bg-audio');
    const musicBtn = document.getElementById('music-btn');
    async function playMusic(){{
        try{{ bgAudio.volume = 0.34; await bgAudio.play(); musicBtn.textContent='❚❚'; musicBtn.classList.add('playing'); }}
        catch(e){{ musicBtn.textContent='▶'; }}
    }}
    function toggleMusic(){{ if(bgAudio.paused) playMusic(); else {{ bgAudio.pause(); musicBtn.textContent='▶'; musicBtn.classList.remove('playing'); }} }}
    // Music is manual-only; never autoplay on page load.

    renderConfigs();
</script>
</body>
</html>"""
    return html


def generate_subscription_content(link: dict, uid: str, addresses: list[str]) -> str:
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")
    usage_str = f"{_fmt_bytes(used)} / ∞" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "∞"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        expiry_str = f"{secs_left // 86400} Days Left"
    
    links_out = links_for_all_variants(link, uid)
    for addr in addresses:
        links_out.extend(links_for_all_variants(link, uid, address=addr))

    return "\n".join(links_out)


def generate_singbox_config(link: dict, uid: str, addresses: list[str]) -> str:
    """Hiddify's engine is sing-box, so give it sing-box's own native
    outbound JSON instead of the generic base64 vless list - removes any
    dependency on Hiddify's vless://-URL parser entirely."""
    domain = get_domain()

    def _vless_outbound(tag: str, server: str, port: int = DEFAULT_PORT) -> dict:
        return {
            "type": "vless",
            "tag": tag,
            "server": server,
            "server_port": port,
            "uuid": uid,
            "flow": "",
            "tls": {
                "enabled": True,
                "server_name": domain,
                "utls": {"enabled": True, "fingerprint": "chrome"},
            },
            "transport": {
                "type": "ws",
                "path": f"/ws/vless/{uid}?ed=2048",
                "headers": {"Host": domain},
            },
        }

    tags = [f"IMMINENT-{link['label']}"]
    outbounds = [_vless_outbound(tags[0], domain)]
    for i, addr in enumerate(addresses):
        tag = f"IMMINENT-{link['label']}-IP{i+1}"
        tags.append(tag)
        outbounds.append(_vless_outbound(tag, addr))

    outbounds.append({"type": "direct", "tag": "direct"})
    outbounds.append({"type": "block", "tag": "block"})
    outbounds.append({
        "type": "selector",
        "tag": "proxy",
        "outbounds": tags + ["direct"],
        "default": tags[0],
    })

    config = {
        "log": {"level": "warn"},
        "dns": {"servers": [{"tag": "dns-remote", "address": "https://1.1.1.1/dns-query"}]},
        "outbounds": outbounds,
        "route": {"final": "proxy", "auto_detect_interface": True},
    }
    return json.dumps(config, ensure_ascii=False, indent=2)


def generate_clash_config(link: dict, uid: str, addresses: list[str]) -> str:
    domain = get_domain()
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")
    usage_str = f"{_fmt_bytes(used)} / ∞" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "∞"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        expiry_str = f"{secs_left // 86400} Days Left"

    variants = sanitize_variants(link.get("variants"))

    def _proxy_entry(auth: str, fp: str, name: str, server: str, port: int = DEFAULT_PORT) -> str:
        # نکته: این خروجی کلش فقط ترابرد WS رو پوشش می‌ده؛ برای لینک‌های XHTTP از
        # همون لینک share (vless:// یا trojan://) استفاده کن.
        cred_line = f'    uuid: {uid}\n' if auth == "vless" else f'    password: {uid}\n'
        return (
            f'  - name: "{name}"\n'
            f'    type: {auth}\n'
            f'    server: {server}\n'
            f'    port: {port}\n'
            f'{cred_line}'
            f'    tls: true\n'
            f'    servername: {domain}\n'
            f'    client-fingerprint: {fp}\n'
            f'    network: ws\n'
            f'    ws-path: /ws/{auth}/{uid}\n'
            f'    ws-headers:\n'
            f'      Host: {domain}\n'
        )

    # فقط auth هایی که فعالن و ترابردشون ws هست رو کلش می‌سازیم (محدودیت خودِ این export)
    active_auths = [a for a in AUTH_TYPES if variants[a]["enabled"] and variants[a]["transport"] == "ws"]

    proxies = []
    proxy_name_list = []
    for auth in active_auths:
        fp = variants[auth]["fingerprint"]
        suffix = "" if len(active_auths) == 1 else f"-{auth.upper()}"
        name0 = f"IMMINENT-{link['label']}{suffix}"
        proxies.append(_proxy_entry(auth, fp, name0, domain))
        proxy_name_list.append(name0)
        for i, addr in enumerate(addresses):
            name_i = f"IMMINENT-{link['label']}{suffix}-IP{i+1}"
            proxies.append(_proxy_entry(auth, fp, name_i, addr))
            proxy_name_list.append(name_i)

    proxies_yaml = "\n".join(proxies)
    proxy_names = "\n".join(f'      - "{p}"' for p in proxy_name_list)

    return (
        f"# IMMINENT Panel - {link['label']}\n"
        f"# {usage_str} | {expiry_str}\n"
        f"port: 7890\n"
        f"socks-port: 7891\n"
        f"mode: rule\n"
        f"log-level: info\n"
        f"external-controller: 127.0.0.1:9090\n"
        f"ipv6: true\n"
        f"allow-lan: false\n"
        f"find-process-mode: strict\n"
        f"\n"
        f"proxies:\n"
        f"{proxies_yaml}\n"
        f"\n"
        f"proxy-groups:\n"
        f'  - name: Proxy\n'
        f'    type: select\n'
        f'    proxies:\n'
        f'{proxy_names}\n'
        f'  - name: Auto\n'
        f'    type: url-test\n'
        f'    url: http://www.gstatic.com/generate_204\n'
        f'    interval: 300\n'
        f'    tolerance: 50\n'
        f'    proxies:\n'
        f'{proxy_names}\n'
        f"\n"
        f"rules:\n"
        f"  - DOMAIN-SUFFIX,google.com,Proxy\n"
        f"  - DOMAIN-SUFFIX,youtube.com,Proxy\n"
        f"  - DOMAIN-SUFFIX,github.com,Proxy\n"
        f"  - DOMAIN-SUFFIX,telegram.org,Proxy\n"
        f"  - DOMAIN-KEYWORD,netflix,Proxy\n"
        f"  - GEOIP,IR,DIRECT\n"
        f"  - GEOSITE,cn,DIRECT\n"
        f"  - MATCH,DIRECT\n"
    )

@app.get("/sub/{uid}")
async def subscription_endpoint(uid: str, request: Request):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(status_code=404, detail="link not found")
        link = dict(link)
        
    if not link["active"]:
        raise HTTPException(status_code=403, detail="link disabled")
        
    expires_at = parse_expires_at(link.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=403, detail="link expired")

    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)

    ua = request.headers.get("user-agent", "").lower()
    accept = request.headers.get("accept", "").lower()
    requested_format = (request.query_params.get("format") or "").lower().strip()

    # Many VPN client apps (Hiddify, NapsternetV, v2rayNG, sing-box front-ends,
    # etc.) use HTTP libraries (Dio/OkHttp/etc.) that sometimes send a
    # browser-like User-Agent and/or a permissive Accept header for
    # compatibility with CDNs. If we only check for "mozilla"+"text/html" we
    # misclassify these real clients as browsers and hand them the HTML
    # landing page, which they can't parse ("unable to determine config
    # format"). So known client fingerprints are checked FIRST and always
    # win, regardless of what Accept/UA otherwise look like.
    known_client_markers = [
        "hiddify", "napsternet", "v2rayng", "v2box", "nekoray", "nekobox",
        "sing-box", "singbox", "streisand", "karing", "shadowrocket",
        "quantumult", "surge", "loon", "matsuri", "husi", "clash", "stash",
        "verge", "clashx", "clashmeta", "cfw", "dart", "okhttp",
    ]
    is_known_client = any(x in ua for x in known_client_markers)

    is_browser = (
        not is_known_client
        and any(x in ua for x in ["mozilla", "chrome", "safari", "opera", "edge"])
        and "text/html" in accept
    )

    if requested_format in {"html", "page", "landing"} or (is_browser and requested_format not in {"base64", "raw", "clash"}):
        return HTMLResponse(content=generate_landing_page(link, uid, addresses))

    is_clash = requested_format == "clash" or (("hiddify" not in ua) and any(x in ua for x in ["clash", "stash", "verge", "clashx", "clashmeta", "cfw"]))

    total_bytes = link["limit_bytes"] if link["limit_bytes"] > 0 else UNLIMITED_QUOTA_BYTES
    expire_ts = 0
    if expires_at is not None:
        expire_ts = int(expires_at.timestamp())

    if is_clash:
        clash_content = generate_clash_config(link, uid, addresses)
        headers = {
            "Content-Type": "text/yaml; charset=utf-8",
            "Content-Disposition": 'attachment; filename="clash.yaml"',
            "profile-update-interval": "6",
            "subscription-userinfo": f"upload={link['used_bytes']}; download=0; total={total_bytes}; expire={expire_ts}",
        }
        return Response(content=clash_content, headers=headers)

    sub_content = generate_subscription_content(link, uid, addresses)

    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "profile-update-interval": "6",
        "profile-title": "base64:" + base64.b64encode(f"IMMINENT-{link['label']}".encode()).decode(),
        "subscription-userinfo": f"upload={link['used_bytes']}; download=0; total={total_bytes}; expire={expire_ts}",
    }

    if requested_format == "raw":
        return Response(content=sub_content, headers=headers)

    # Default subscription format is base64, which is accepted by v2rayNG,
    # Hiddify and other subscription clients. Explicit ?format=base64 forces
    # this path even when a browser-like User-Agent is used.
    encoded = base64.b64encode(sub_content.encode()).decode()
    return Response(content=encoded, headers=headers)

RELAY_BUF = 128 * 1024

async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("chunk too small")
    pos = 1 + 16
    addon_len = first_chunk[pos]
    pos += 1 + addon_len
    command = first_chunk[pos]
    pos += 1
    port = int.from_bytes(first_chunk[pos:pos + 2], "big")
    pos += 2
    addr_type = first_chunk[pos]
    pos += 1
    if addr_type == 1:
        addr_bytes = first_chunk[pos:pos + 4]
        pos += 4
        address = ".".join(str(b) for b in addr_bytes)
    elif addr_type == 2:
        domain_len = first_chunk[pos]
        pos += 1
        address = first_chunk[pos:pos + domain_len].decode("utf-8", errors="ignore")
        pos += domain_len
    elif addr_type == 3:
        addr_bytes = first_chunk[pos:pos + 16]
        pos += 16
        address = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown address type: {addr_type}")
    return command, address, port, first_chunk[pos:]

async def parse_trojan_header(first_chunk: bytes):
    """پارس هدر Trojan: hex(SHA224(password))[56] + CRLF + (CMD+ATYP+DST.ADDR+DST.PORT) + CRLF + payload.
    مقدار hash پسورد اعتبارسنجی نمی‌شه چون احراز هویت واقعی همون uid مخفیِ توی
    مسیر URL هست (دقیقاً مثل رفتار فعلیِ پنل برای VLESS)."""
    if len(first_chunk) < 56 + 2 + 1 + 1 + 2 + 2:
        raise ValueError("chunk too small")
    pos = 56
    if first_chunk[pos:pos + 2] != b"\r\n":
        raise ValueError("invalid trojan header (missing CRLF after hash)")
    pos += 2
    command = first_chunk[pos]
    pos += 1
    addr_type = first_chunk[pos]
    pos += 1
    if addr_type == 1:
        addr_bytes = first_chunk[pos:pos + 4]
        pos += 4
        address = ".".join(str(b) for b in addr_bytes)
    elif addr_type == 3:
        domain_len = first_chunk[pos]
        pos += 1
        address = first_chunk[pos:pos + domain_len].decode("utf-8", errors="ignore")
        pos += domain_len
    elif addr_type == 4:
        addr_bytes = first_chunk[pos:pos + 16]
        pos += 16
        address = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown trojan address type: {addr_type}")
    port = int.from_bytes(first_chunk[pos:pos + 2], "big")
    pos += 2
    if first_chunk[pos:pos + 2] != b"\r\n":
        raise ValueError("invalid trojan header (missing trailing CRLF)")
    pos += 2
    return command, address, port, first_chunk[pos:]

async def parse_proxy_header(auth: str, first_chunk: bytes):
    """بر اساس auth گرفته‌شده از مسیر URL (vless یا trojan)، هدر رو با پارسر درست می‌خونه."""
    if auth == "trojan":
        return await parse_trojan_header(first_chunk)
    return await parse_vless_header(first_chunk)

def response_prefix_for_protocol(auth: str) -> bytes:
    """VLESS یک پاسخ ۲ بایتی (version=0 + no addons) قبل از اولین چانک دیتای برگشتی
    می‌فرسته؛ Trojan چنین چیزی نداره و کاملاً raw pass-through هست."""
    return b"" if auth == "trojan" else b"\x00\x00"

async def check_quota(uid: str, extra_bytes: int) -> bool:
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link["active"]:
            return False
        expires_at = parse_expires_at(link.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now(timezone.utc):
            return False
        if link["limit_bytes"] == 0:
            return True
        return (link["used_bytes"] + extra_bytes) <= link["limit_bytes"]

async def add_usage(uid: str, n: int):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n

async def check_and_add_usage(uid: str, extra_bytes: int) -> bool:
    """Atomically check quota/expiry/active state and commit usage in a
    single lock acquisition. Doing this as two separate locked calls
    (check_quota then add_usage) let concurrent chunks - e.g. the upload
    and download directions of the same connection racing each other -
    both pass the check before either had committed, which could push a
    link's used_bytes past its limit. It also doubled lock contention on
    every single packet relayed, which was a real throughput bottleneck
    under load. This does both in one step."""
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link["active"]:
            return False
        expires_at = parse_expires_at(link.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now(timezone.utc):
            return False
        if link["limit_bytes"] != 0 and (link["used_bytes"] + extra_bytes) > link["limit_bytes"]:
            return False
        link["used_bytes"] += extra_bytes
        return True

async def ws_to_tcp(websocket, writer, conn_id, link_uid):
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            size = len(data)
            if not await check_and_add_usage(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            stats["total_requests"] += 1
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            now = datetime.now(timezone.utc)
            hourly_traffic[now.strftime("%Y-%m-%d %H:00")] += size
            daily_traffic[now.strftime("%Y-%m-%d")] += size
            try:
                writer.write(data)
                await writer.drain()
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            if not writer.is_closing():
                writer.write_eof()
        except Exception:
            pass

async def tcp_to_ws(websocket, reader, conn_id, link_uid, resp_prefix: bytes = b"\x00\x00"):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            size = len(data)
            if not await check_and_add_usage(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            now = datetime.now(timezone.utc)
            hourly_traffic[now.strftime("%Y-%m-%d %H:00")] += size
            daily_traffic[now.strftime("%Y-%m-%d")] += size
            try:
                await websocket.send_bytes((resp_prefix + data) if (first and resp_prefix) else data)
                first = False
            except Exception:
                break
    except Exception:
        pass

@app.websocket("/ws/{auth}/{uuid}")
async def websocket_tunnel(websocket: WebSocket, auth: str, uuid: str):
    await ensure_default_link()

    if auth not in AUTH_TYPES:
        await websocket.close(code=1008)
        return

    async with LINKS_LOCK:
        link_data = LINKS.get(uuid)
        if link_data is None or not link_data["active"]:
            await websocket.close(code=1008)
            return
        variant = link_data.get("variants", {}).get(auth)
        if not variant or not variant.get("enabled") or variant.get("transport") != "ws":
            await websocket.close(code=1008)
            return
        max_conn = link_data.get("max_connections", 0)
        link_data_copy = dict(link_data)

    expires_at = parse_expires_at(link_data_copy.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        await websocket.close(code=1008)
        return

    if max_conn > 0:
        current_conns = await count_connections_for_link(uuid)
        if current_conns >= max_conn:
            await websocket.close(code=1008)
            return

    # Xray/V2Ray clients that request early data (ws ?ed=... in the link)
    # smuggle the first chunk of the VLESS request inside the
    # Sec-WebSocket-Protocol header of the upgrade request itself, so it
    # arrives in the same TCP packet as the handshake instead of a separate
    # round trip after accept(). Echoing the header back keeps the handshake
    # spec-compliant for clients that check it.
    early_data_hdr = websocket.headers.get("sec-websocket-protocol")
    early_data = b""
    if early_data_hdr:
        try:
            padded = early_data_hdr + "=" * (-len(early_data_hdr) % 4)
            early_data = base64.urlsafe_b64decode(padded)
        except Exception:
            early_data = b""
    await websocket.accept(subprotocol=early_data_hdr if early_data_hdr else None)
    writer = None
    conn_id = None
    client_ip = get_client_ip(websocket)
    try:
        if early_data:
            first_chunk = early_data
        else:
            first_msg = await asyncio.wait_for(websocket.receive(), timeout=15.0)
            if first_msg["type"] == "websocket.disconnect":
                return
            first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
            if not first_chunk:
                return

        try:
            command, address, port, initial_payload = await parse_proxy_header(auth, first_chunk)
        except ValueError as e:
            logger.warning(f"Invalid proxy header: {e}")
            await websocket.close(code=1008, reason="invalid header")
            return

        conn_id = secrets.token_urlsafe(8)
        async with connections_lock:
            connections[conn_id] = {
                "uuid": uuid, "ip": client_ip,
                "connected_at": datetime.now(timezone.utc).isoformat(),
                "bytes": 0,
            }
            connection_sockets[conn_id] = websocket
            link_ip_map[uuid].add(client_ip)

        await _log_connection_event("connect", link_data_copy.get("label", uuid), uuid, client_ip)

        size = len(first_chunk)
        if not await check_and_add_usage(uuid, size):
            await websocket.close(code=1008, reason="quota exceeded")
            return
        stats["total_bytes"] += size
        stats["total_requests"] += 1
        async with connections_lock:
            if conn_id in connections:
                connections[conn_id]["bytes"] += size
        now = datetime.now(timezone.utc)
        hourly_traffic[now.strftime("%Y-%m-%d %H:00")] += size
        daily_traffic[now.strftime("%Y-%m-%d")] += size

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port), timeout=10.0
        )
        # Disable Nagle's algorithm on the backend TCP socket. Without this,
        # small proxied packets (the common case for interactive/streaming
        # traffic) can sit buffered for up to ~40ms waiting to be coalesced,
        # which is felt as real added latency/slowness on every config.
        try:
            backend_sock = writer.get_extra_info("socket")
            if backend_sock is not None:
                backend_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, AttributeError):
            pass

        if initial_payload and not await check_and_add_usage(uuid, len(initial_payload)):
            await websocket.close(code=1008, reason="quota exceeded")
            return

        if initial_payload:
            p_size = len(initial_payload)
            stats["total_bytes"] += p_size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += p_size
            now = datetime.now(timezone.utc)
            hourly_traffic[now.strftime("%Y-%m-%d %H:00")] += p_size
            daily_traffic[now.strftime("%Y-%m-%d")] += p_size
            try:
                writer.write(initial_payload)
                await writer.drain()
            except Exception:
                pass

        task_up = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        task_down = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid, resp_prefix=response_prefix_for_protocol(auth)))
        done, pending = await asyncio.wait({task_up, task_down}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now(timezone.utc).isoformat()})
        logger.exception("WebSocket error")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if conn_id:
            async with connections_lock:
                info = connections.pop(conn_id, None)
                connection_sockets.pop(conn_id, None)
                if info:
                    uid = info.get("uuid")
                    ip = info.get("ip")
                    if uid and ip:
                        has_other = any(
                            c.get("uuid") == uid and c.get("ip") == ip
                            for c in connections.values()
                        )
                        if not has_other:
                            if uid in link_ip_map:
                                link_ip_map[uid].discard(ip)
                                if not link_ip_map[uid]:
                                    link_ip_map.pop(uid, None)
            if info:
                try:
                    connected_at = datetime.fromisoformat(info["connected_at"])
                    duration_s = max(0, int((datetime.now(timezone.utc) - connected_at).total_seconds()))
                except Exception:
                    duration_s = 0
                async with LINKS_LOCK:
                    label = LINKS.get(info.get("uuid"), {}).get("label", info.get("uuid", uuid))
                extra = f"duration {duration_s}s, {_fmt_bytes(info.get('bytes', 0))}"
                await _log_connection_event("disconnect", label, info.get("uuid", uuid), info.get("ip", client_ip), extra)

# ══════════════════════════════════════════════════════════════════════════════
# XHTTP transport (packet-up / stream-up) — جدا شده به xhttp_transport.py
# ══════════════════════════════════════════════════════════════════════════════
from xhttp_transport import router as xhttp_router
app.include_router(xhttp_router)

# ── HTML Panel (Gold/Neon Theme) ─────────────────────────────────────────
PANEL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>AXIOM • Command Center</title>
<link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@700;900&family=Inter:wght@300;400;500;600;700&family=Vazirmatn:wght@400;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --gold:#00f5ff;--gold2:#7c3aed;--gold3:#ff00aa;--gold-dim:rgba(0,245,255,0.12);
  --black:#060608;--black2:#0c0c10;--black3:#111118;
  --surface:rgba(12,12,18,0.97);--surface2:rgba(20,20,28,0.9);--surface3:rgba(28,28,40,0.8);
  --border:rgba(0,245,255,0.18);--border2:rgba(255,0,170,0.28);
  --text:rgba(255,255,255,0.92);--text2:rgba(0,245,255,0.8);--text3:rgba(255,255,255,0.4);
  --gold-glow:0 0 24px rgba(0,245,255,0.5),0 0 48px rgba(255,0,170,0.25);
  --green:#4ade80;--green-dim:rgba(74,222,128,0.1);
  --red:#f87171;--red-dim:rgba(248,113,113,0.1);
  --yellow:#fbbf24;
  --nav-w:64px;
}
body.light-mode{
  --black:#f0f4f8;--black2:#ffffff;--black3:#e8eef5;
  --surface:rgba(255,255,255,0.97);--surface2:#ffffff;--surface3:#f8fafc;
  --border:rgba(200,30,45,0.15);--border2:rgba(200,30,45,0.3);
  --text:#0f172a;--text2:#0891b2;--text3:#64748b;
  --gold-dim:rgba(200,30,45,0.1);--gold-dim2:rgba(200,30,45,0.06);
  --gold-glow:0 4px 14px rgba(0,0,0,0.08);
}
html,body{height:100%;background:var(--black);transition:background .3s,color .3s}
body{font-family:'Inter','Vazirmatn',sans-serif;color:var(--text);display:flex;min-height:100vh}
body[dir="rtl"]{direction:rtl;text-align:right}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:rgba(200,30,45,0.2);border-radius:4px}
.bg-fixed{position:fixed;inset:0;z-index:0;pointer-events:none;
  background:radial-gradient(ellipse 70% 50% at 50% -10%,rgba(200,30,45,0.07),transparent 60%),
             radial-gradient(ellipse 40% 30% at 90% 90%,rgba(200,30,45,0.04),transparent 50%)}
.light-mode .bg-fixed{background:none}
.grid-fixed{position:fixed;inset:0;z-index:0;pointer-events:none;
  background-image:linear-gradient(rgba(200,30,45,0.04) 1px,transparent 1px),
                   linear-gradient(90deg,rgba(200,30,45,0.04) 1px,transparent 1px);
  background-size:56px 56px}
.light-mode .grid-fixed{opacity:.4}

/* Sidebar */
.sidebar{position:fixed;left:0;top:0;bottom:0;width:var(--nav-w);background:var(--surface);
  border-right:1px solid var(--border);display:flex;flex-direction:column;z-index:100;
  transition:all .3s cubic-bezier(.4,0,.2,1);backdrop-filter:blur(20px)}
.sidebar::after{content:'';position:absolute;top:0;right:0;bottom:0;width:1px;
  background:linear-gradient(180deg,transparent,rgba(200,30,45,0.4) 30%,rgba(200,30,45,0.4) 70%,transparent)}
.light-mode .sidebar::after{display:none}
.sb-brand{padding:16px 0;display:flex;flex-direction:column;align-items:center;gap:2px;
  border-bottom:1px solid var(--border);flex-shrink:0}
.sb-hat{filter:drop-shadow(0 0 10px rgba(200,30,45,.5));transition:filter .3s}
.sb-hat:hover{filter:drop-shadow(0 0 18px rgba(200,30,45,.9))}
.sb-title{font-family:'Cinzel',serif;font-size:8px;letter-spacing:.18em;color:rgba(200,30,45,.6);
  text-transform:uppercase;white-space:nowrap;overflow:hidden}
.sb-nav{flex:1;display:flex;flex-direction:column;justify-content:flex-end;padding-bottom:12px;
  gap:2px;padding-left:8px;padding-right:8px}
.nav-item{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;
  padding:10px 6px;border-radius:12px;color:var(--text3);cursor:pointer;
  transition:all .2s cubic-bezier(.4,0,.2,1);border:1px solid transparent;position:relative;
  overflow:hidden;text-decoration:none;background:none;width:100%;font-family:inherit}
.nav-item::before{content:'';position:absolute;inset:0;border-radius:12px;
  background:linear-gradient(135deg,var(--gold-dim),transparent);opacity:0;transition:opacity .2s}
.nav-item:hover{color:var(--gold);border-color:rgba(200,30,45,.12)}
.nav-item:hover::before{opacity:1}
.nav-item.active{color:var(--gold);border-color:rgba(200,30,45,.22);background:var(--gold-dim);
  box-shadow:0 0 16px rgba(200,30,45,.1),inset 0 1px 0 rgba(200,30,45,.12)}
.nav-item.active::before{opacity:1}
.nav-icon{width:18px;height:18px;flex-shrink:0;transition:transform .2s}
.nav-item:hover .nav-icon,.nav-item.active .nav-icon{transform:scale(1.1)}
.nav-label{font-size:8.5px;font-weight:600;letter-spacing:.05em;white-space:nowrap;overflow:hidden}
.nav-badge{position:absolute;top:5px;right:5px;background:var(--gold);color:#000;font-size:8px;
  font-weight:800;min-width:14px;height:14px;border-radius:7px;display:flex;align-items:center;
  justify-content:center;padding:0 3px}
.sb-bottom{padding:8px;border-top:1px solid var(--border);display:flex;flex-direction:column;gap:6px;flex-shrink:0}
.lang-row{display:flex;gap:4px}
.lang-btn{flex:1;padding:5px 2px;border:1px solid var(--border);border-radius:7px;background:none;
  color:var(--text3);font-size:9px;font-weight:700;cursor:pointer;transition:all .2s;
  font-family:inherit;letter-spacing:.05em}
.lang-btn.active{background:var(--gold-dim);border-color:var(--gold);color:var(--gold)}
.lang-btn:hover:not(.active){border-color:rgba(200,30,45,.15);color:rgba(200,30,45,.5)}
.logout-btn{display:flex;align-items:center;justify-content:center;padding:7px;
  border:1px solid rgba(248,113,113,.15);border-radius:8px;background:rgba(248,113,113,.06);
  color:rgba(248,113,113,.6);cursor:pointer;transition:all .2s;font-size:10px;gap:4px;
  font-weight:600;font-family:inherit}
.logout-btn:hover{background:rgba(248,113,113,.12);border-color:rgba(248,113,113,.3);color:var(--red)}
.theme-toggle{background:transparent;border:1px solid var(--border);color:var(--text3);
  border-radius:7px;padding:4px;cursor:pointer;display:flex;align-items:center;justify-content:center;
  transition:all .2s}
.theme-toggle:hover{background:var(--surface3);color:var(--gold);border-color:var(--gold)}

/* Social links in sidebar */
.sb-social{display:flex;gap:4px;margin-bottom:2px}
.sb-social-btn{flex:1;display:flex;align-items:center;justify-content:center;padding:7px 4px;
  border:1px solid var(--border);border-radius:8px;color:var(--text3);cursor:pointer;
  transition:all .2s;text-decoration:none;background:none}
.sb-social-btn:hover{border-color:var(--border2);color:var(--gold);background:var(--gold-dim);
  box-shadow:0 0 10px rgba(200,30,45,0.1)}
.sb-social-btn svg{width:14px;height:14px}
.mob-social{display:none;gap:8px;align-items:center}
.mob-social .sb-social-btn{padding:7px}
.mob-social .sb-social-btn svg{width:16px;height:16px}

/* Main */
.main{margin-left:var(--nav-w);flex:1;padding:24px 28px 48px;min-height:100vh;position:relative;z-index:1}
.page{display:none;animation:pgIn .35s ease}
.page.active{display:block}
@keyframes pgIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.page-header{margin-bottom:20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
.page-title{font-family:'Cinzel',serif;font-size:16px;font-weight:700;color:var(--text);letter-spacing:.04em}
.page-sub{font-size:11px;color:var(--text3);margin-top:3px;letter-spacing:.02em}
.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.stat-card{background:var(--surface2);border:1px solid var(--border);border-radius:12px;
  padding:16px;position:relative;overflow:hidden;transition:all .25s;animation:cIn .5s ease both}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(200,30,45,0.4),transparent)}
.light-mode .stat-card::before{display:none}
.stat-card:hover{border-color:var(--border2);transform:translateY(-2px);box-shadow:var(--gold-glow)}
@keyframes cIn{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.stat-label{font-size:9.5px;color:var(--text3);font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}
.stat-val{font-size:20px;font-weight:700;color:var(--text);letter-spacing:-.02em}
.stat-unit{font-size:11px;font-weight:400;color:var(--text3)}
.card{background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:16px;
  margin-bottom:10px;position:relative;overflow:hidden;transition:all .25s;animation:cIn .5s ease both}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(200,30,45,0.2),transparent)}
.light-mode .card::before{display:none}
.card-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.card-title{font-size:12px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:6px}
.chart-container{height:170px;width:100%}
.btn{font-family:inherit;font-size:11.5px;font-weight:700;border-radius:8px;padding:7px 14px;
  cursor:pointer;display:inline-flex;align-items:center;gap:5px;border:none;transition:all .2s;letter-spacing:.03em}
.btn-gold{background:linear-gradient(135deg,#C81E2D,#A8172A);color:#000;box-shadow:0 0 16px rgba(200,30,45,.25)}
.btn-gold:hover{filter:brightness(1.1);transform:translateY(-1px);box-shadow:0 0 24px rgba(200,30,45,.4)}
.btn-ghost{background:var(--surface3);color:var(--text);border:1px solid var(--border)}
.btn-danger{background:var(--red-dim);color:var(--red);border:1px solid rgba(248,113,113,.15)}
.btn-sm{padding:4px 9px;font-size:10.5px}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.tbl-wrap{overflow-x:auto}
.tbl{width:100%;border-collapse:collapse}
.tbl th{text-align:left;font-size:9.5px;font-weight:700;color:var(--text3);padding:9px 11px;
  text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border);background:var(--surface3)}
.tbl td{padding:9px 11px;border-bottom:1px solid var(--border);font-size:12.5px;vertical-align:middle}
.tag{display:inline-flex;align-items:center;padding:2px 7px;border-radius:4px;font-size:9px;
  font-weight:800;letter-spacing:.05em;text-transform:uppercase}
.tag-vless{background:var(--gold-dim);color:var(--gold);border:1px solid var(--border)}
.tag-port{background:rgba(167,139,250,.1);color:#a78bfa;border:1px solid rgba(167,139,250,.2)}
.tag-on{background:var(--green-dim);color:var(--green);border:1px solid rgba(74,222,128,.2)}
.tag-off{background:var(--red-dim);color:var(--red);border:1px solid rgba(248,113,113,.2)}
.pill{display:flex;align-items:center;gap:7px;font-size:11px}
.pill-used{color:var(--text);font-weight:600}
.pill-bar{flex:1;height:4px;background:var(--border);border-radius:2px;min-width:40px}
.pill-fill{height:100%;border-radius:2px;transition:width .4s}
.pill-lim{color:var(--text3);font-size:10px}
.toggle{width:32px;height:17px;border-radius:9px;background:var(--surface3);position:relative;
  cursor:pointer;transition:all .28s;border:1px solid var(--border);flex-shrink:0}
.toggle::after{content:'';position:absolute;width:11px;height:11px;border-radius:50%;
  background:var(--text3);top:2px;left:2px;transition:all .28s cubic-bezier(.4,0,.2,1)}
.toggle.on{background:var(--green);border-color:var(--green);box-shadow:0 0 10px rgba(74,222,128,.3)}
.toggle.on::after{left:17px;background:#fff}
.sys-bar{height:6px;background:var(--border);border-radius:3px;overflow:hidden}
.sys-fill{height:100%;border-radius:3px;transition:width .4s}
.sl-item{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border)}
.sl-k{color:var(--text3);font-size:11.5px}
.sl-v{color:var(--text);font-weight:600;font-size:11.5px}
.fg{display:flex;flex-direction:column;gap:4px;margin-bottom:11px}
.fl{font-size:9.5px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.08em}
.fi,.fs{padding:8px 12px;border-radius:8px;border:1px solid var(--border);font-family:inherit;
  font-size:12.5px;outline:none;color:var(--text);background:var(--surface);transition:all .2s}
.fi:focus,.fs:focus{border-color:var(--gold);box-shadow:0 0 0 3px rgba(200,30,45,.08)}
.fr{display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end}
.fr .fg{margin-bottom:0;flex:1;min-width:90px}
.act-btn{font-family:inherit;font-size:9.5px;font-weight:700;border-radius:6px;padding:4px 8px;
  cursor:pointer;display:inline-flex;align-items:center;gap:3px;border:1px solid;transition:all .18s}
.act-copy{background:var(--gold-dim);color:var(--gold);border-color:var(--border)}
.act-sub{background:var(--green-dim);color:var(--green);border-color:rgba(74,222,128,.2)}
.act-qr{background:rgba(167,139,250,.1);color:#a78bfa;border-color:rgba(167,139,250,.2)}
.act-edit{background:rgba(251,191,36,.08);color:var(--yellow);border-color:rgba(251,191,36,.2)}
.act-del{background:var(--red-dim);color:var(--red);border-color:rgba(248,113,113,.18)}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(16px);
  background:var(--surface);color:var(--gold);border:1px solid var(--border2);border-radius:10px;
  padding:12px 20px;font-size:13px;font-weight:600;opacity:0;transition:all .3s;z-index:999;
  backdrop-filter:blur(24px);box-shadow:var(--gold-glow)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mo{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:200;display:none;
  align-items:center;justify-content:center;backdrop-filter:blur(8px)}
.mo.show{display:flex}
.mo-box{background:var(--surface2);border:1px solid var(--border2);border-radius:18px;padding:24px;
  width:100%;max-width:460px;position:relative;box-shadow:var(--gold-glow);
  transform:scale(.92);opacity:0;transition:all .38s cubic-bezier(.34,1.56,.64,1)}
.mo.show .mo-box{transform:scale(1);opacity:1}
.mo-title{font-family:'Cinzel',serif;font-size:14px;font-weight:700;margin-bottom:16px;
  color:var(--gold);letter-spacing:.06em}
.mo-close{position:absolute;top:14px;right:14px;background:var(--surface3);border:1px solid var(--border);
  color:var(--text3);width:30px;height:30px;border-radius:7px;cursor:pointer;display:flex;
  align-items:center;justify-content:center;font-size:14px}
.qr-box{text-align:center;padding:20px;background:var(--surface3);border-radius:12px;
  border:1px solid var(--border);margin-top:12px}
.qr-box img{max-width:200px;border-radius:8px;border:3px solid var(--border);box-shadow:var(--gold-glow)}
.tb{display:flex;align-items:center;gap:7px;margin-bottom:14px;flex-wrap:wrap}
.search-wrap{flex:1;min-width:160px;position:relative}
.search-wrap svg{position:absolute;left:12px;top:50%;transform:translateY(-50%);color:var(--text3)}
.search-wrap input{width:100%;padding:9px 12px 9px 34px;background:var(--surface2);
  border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:13px;
  font-family:inherit;outline:none}
.search-wrap input:focus{border-color:var(--gold)}
.filter-chips{display:flex;gap:3px;padding:3px;background:var(--surface2);border:1px solid var(--border);border-radius:8px}
.chip{padding:7px 12px;border-radius:6px;font-size:11.5px;font-weight:700;color:var(--text3);
  cursor:pointer;border:none;background:none;transition:all .18s;font-family:inherit}
.chip.active{background:var(--gold);color:#000}
.m-cards{display:none;flex-direction:column;gap:12px}
.m-card{border:1px solid var(--border);border-radius:12px;padding:16px;background:var(--surface2)}
.m-card-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.m-card-acts{display:flex;gap:6px;flex-wrap:wrap;margin-top:12px}
.empty{text-align:center;padding:36px;color:var(--text3)}
.mob-hd{display:none;position:fixed;top:0;left:0;right:0;background:var(--surface);
  border-bottom:1px solid var(--border);z-index:90;align-items:center;justify-content:space-between;
  backdrop-filter:blur(20px)}
.mob-tl-group{display:flex;gap:10px;align-items:center;flex-direction:row}
.logout-mob{display:none;color:var(--red) !important}
.logout-mob:hover{background:var(--red-dim) !important;border-color:rgba(248,113,113,.3) !important}
.alerts-box{background:rgba(248,113,113,.08);border:1px dashed rgba(248,113,113,.3);
  border-radius:12px;padding:14px;margin-bottom:14px;display:none}
.alerts-title{color:var(--red);font-size:12.5px;font-weight:700;margin-bottom:8px;
  display:flex;align-items:center;gap:6px}
.alert-item{font-size:12px;margin-bottom:4px;color:var(--text);display:flex;justify-content:space-between}
.live-logs-container{background:#000;border:1px solid var(--border);border-radius:8px;padding:12px;
  font-family:monospace;font-size:11px;color:#C81E2D;height:200px;overflow-y:auto;white-space:pre-wrap}
.login-wrap{display:flex;align-items:center;justify-content:center;min-height:100vh;width:100%}
.login-box{background:var(--surface2);border:1px solid var(--border2);border-radius:20px;
  padding:36px 32px;width:100%;max-width:360px;box-shadow:var(--gold-glow)}
.login-logo{text-align:center;margin-bottom:28px}
.login-title{font-family:'Cinzel',serif;font-size:22px;font-weight:900;color:var(--gold);letter-spacing:.1em}
.login-sub{font-size:11px;color:var(--text3);margin-top:6px}

/* Notification styles */
.notif-item{display:flex;align-items:flex-start;gap:12px;padding:14px 18px;border-bottom:1px solid var(--border);transition:all .2s}
.notif-item:last-child{border-bottom:none}
.notif-item:hover{background:var(--surface3)}
.notif-item.unseen{background:var(--gold-dim)}
.notif-icon{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:18px}
.notif-icon.update{background:rgba(56,189,248,.12);color:#38bdf8}
.notif-icon.quota{background:var(--red-dim);color:var(--red)}
.notif-icon.expiry{background:rgba(251,191,36,.12);color:var(--yellow)}
.notif-icon.info{background:rgba(74,222,128,.12);color:var(--green)}
.notif-body{flex:1;min-width:0}
.notif-title{font-size:13px;font-weight:700;color:var(--text);margin-bottom:2px}
.notif-msg{font-size:11px;color:var(--text3);line-height:1.4}
.notif-time{font-size:10px;color:var(--text3);margin-top:4px}
.notif-link{display:inline-flex;align-items:center;gap:4px;font-size:11px;font-weight:600;color:var(--gold);text-decoration:none;margin-top:4px}
.notif-link:hover{text-decoration:underline}
.notif-dot{width:8px;height:8px;border-radius:50%;background:var(--gold);flex-shrink:0;margin-top:10px}

/* Gold accent on progress fills */
.pill-fill-gold{background:linear-gradient(90deg,var(--gold),var(--gold2))}

@media(max-width:768px){
  .mob-hd{display:flex;height:65px;padding:0 20px}
  .mob-tl-group .lang-btn{font-size:13px;padding:7px 10px;border-radius:8px}
  .theme-toggle{font-size:18px;padding:7px 10px;border-radius:8px}
  .mob-hd span{font-size:22px !important}
  .sidebar{transform:none !important;width:100% !important;height:78px;top:auto;bottom:0;
    border-right:none;border-top:1px solid var(--border);flex-direction:row;padding:0;
    background:var(--surface);box-shadow:0 -4px 20px rgba(0,0,0,.5)}
  .light-mode .sidebar{box-shadow:0 -4px 20px rgba(0,0,0,.06)}
  .sb-brand,.sb-bottom{display:none !important}
  .sidebar .sb-social{display:none !important}
  .mob-social{display:flex !important}
  .sb-nav{flex-direction:row;width:100%;padding:0;align-items:center;justify-content:space-between;gap:0}
  .nav-item{flex:1;padding:12px 0;border-radius:0}
  .nav-icon{width:24px;height:24px;margin-bottom:5px}
  .nav-label{font-size:10px;letter-spacing:0}
  .nav-badge{top:6px;right:50%;transform:translateX(10px);min-width:18px;height:18px;font-size:10px}
  .logout-mob{display:flex}
  .main{margin-left:0;padding-top:85px;padding-left:18px;padding-right:18px;padding-bottom:100px}
  .page-title{font-size:24px}
  .page-sub{font-size:13px;margin-top:5px}
  .btn{font-size:14px;padding:10px 18px}
  .btn-sm{font-size:12px;padding:8px 14px}
  .stats-row{grid-template-columns:1fr 1fr;gap:14px;margin-bottom:18px}
  .stat-card{padding:22px;border-radius:16px}
  .stat-label{font-size:12px;margin-bottom:12px}
  .stat-val{font-size:26px}
  .stat-unit{font-size:14px}
  .grid-2{grid-template-columns:1fr;gap:14px;margin-bottom:14px}
  .card{padding:22px;border-radius:16px;margin-bottom:14px}
  .card-title{font-size:16px;margin-bottom:16px}
  .chart-container{height:220px;width:100%}
  #cpu-v,#mem-v{font-size:22px !important}
  .sl-k,.sl-v{font-size:14px;padding:14px 0}
  .tbl-wrap{display:none}
  .m-cards{display:flex}
  .m-card{padding:18px;border-radius:14px}
  .m-card-hd span{font-size:16px !important}
  .pill-used{font-size:13px}
  .pill-lim{font-size:12px}
  .m-card-acts .act-btn{font-size:12px;padding:8px 14px;border-radius:8px}
  .mo-box{padding:28px 24px;border-radius:20px}
  .fi,.fs{font-size:16px;padding:12px 16px}
  .fl{font-size:11px;margin-bottom:6px}
}
@media(max-width:460px){.stats-row{grid-template-columns:1fr;gap:14px}}
\n/* v3 NEON COMMAND CENTER — complete visual refresh */\nbody{background:\n radial-gradient(circle at 10% 10%,rgba(0,245,255,.10),transparent 26%),\n radial-gradient(circle at 90% 20%,rgba(255,0,170,.09),transparent 28%),\n radial-gradient(circle at 50% 100%,rgba(124,58,237,.10),transparent 35%),#05060a!important;}\nbody:before{content:"";position:fixed;inset:0;pointer-events:none;z-index:-1;background-image:linear-gradient(rgba(255,255,255,.018) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.018) 1px,transparent 1px);background-size:32px 32px;mask-image:linear-gradient(to bottom,black,transparent 90%)}\n.sidebar{width:78px!important;background:rgba(7,9,15,.72)!important;border-right:1px solid rgba(0,245,255,.14)!important;backdrop-filter:blur(24px);box-shadow:10px 0 50px rgba(0,0,0,.24)!important}\n.sb-brand{padding:18px 10px!important}.sb-brand .logo-mark{width:42px!important;height:42px!important;border-radius:14px!important;background:linear-gradient(135deg,#00f5ff,#7c3aed,#ff2bd6)!important;box-shadow:0 0 28px rgba(0,245,255,.25)!important}\n.nav-item{margin:5px 9px!important;border-radius:14px!important;border:1px solid transparent;transition:.25s!important}.nav-item:hover{background:rgba(0,245,255,.06)!important;border-color:rgba(0,245,255,.12)!important;transform:translateX(2px)}.nav-item.active{background:linear-gradient(135deg,rgba(0,245,255,.12),rgba(255,0,170,.09))!important;border-color:rgba(0,245,255,.25)!important;box-shadow:0 0 24px rgba(0,245,255,.08),inset 0 0 20px rgba(255,0,170,.03)}\n.main{margin-left:78px!important;padding:30px 34px!important;max-width:1680px!important}\n.page-header{padding:18px 0 22px!important}.page-title{font-size:34px!important;font-weight:900!important;letter-spacing:-.04em!important;background:linear-gradient(90deg,#fff,#67e8f9,#c4b5fd,#ff8be9);-webkit-background-clip:text;background-clip:text;color:transparent!important}.page-sub{color:rgba(203,213,225,.52)!important}\n.card,.stat-card{background:linear-gradient(145deg,rgba(15,20,31,.82),rgba(8,10,17,.78))!important;border:1px solid rgba(0,245,255,.11)!important;border-radius:22px!important;box-shadow:0 18px 60px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.035)!important;backdrop-filter:blur(18px)}\n.card:hover,.stat-card:hover{border-color:rgba(0,245,255,.2)!important;box-shadow:0 22px 70px rgba(0,0,0,.3),0 0 30px rgba(0,245,255,.04)!important}\n.stat-val{font-size:34px!important;font-weight:900!important}.stat-card{position:relative;overflow:hidden}.stat-card:after{content:"";position:absolute;right:-30px;top:-30px;width:100px;height:100px;border-radius:50%;background:radial-gradient(circle,rgba(0,245,255,.16),transparent 70%);pointer-events:none}\n.btn{border-radius:13px!important;border:1px solid rgba(0,245,255,.16)!important}.btn-gold{background:linear-gradient(110deg,#00d9ff,#7c3aed,#ff2bd6,#00d9ff)!important;background-size:300% 100%!important;animation:v3rgb 7s linear infinite;color:#020617!important;border:none!important;box-shadow:0 0 28px rgba(0,245,255,.13)!important}@keyframes v3rgb{to{background-position:300% 0}}\n.fi,.fs,.search-wrap input{background:rgba(4,7,13,.7)!important;border-color:rgba(0,245,255,.13)!important;border-radius:12px!important}.fi:focus,.fs:focus,.search-wrap input:focus{border-color:#00f5ff!important;box-shadow:0 0 0 3px rgba(0,245,255,.07),0 0 20px rgba(0,245,255,.06)!important}\n.tbl th{background:rgba(0,245,255,.035)!important;color:rgba(255,255,255,.48)!important}.tbl td{border-color:rgba(255,255,255,.045)!important}.m-card{background:rgba(9,13,22,.76)!important;border-color:rgba(0,245,255,.11)!important;border-radius:18px!important}\n.toast{border-color:rgba(0,245,255,.25)!important;background:rgba(8,12,20,.92)!important;box-shadow:0 0 32px rgba(0,245,255,.15)!important}\n.login-wrap{background:radial-gradient(circle at 50% 40%,rgba(0,245,255,.10),transparent 34%),radial-gradient(circle at 80% 80%,rgba(255,0,170,.08),transparent 30%)}.login-box{max-width:410px!important;border-radius:28px!important;background:rgba(9,13,22,.76)!important;border-color:rgba(0,245,255,.18)!important;box-shadow:0 30px 100px rgba(0,0,0,.4),0 0 60px rgba(0,245,255,.07)!important;backdrop-filter:blur(24px)}.login-title{font-size:28px!important;background:linear-gradient(90deg,#67e8f9,#c4b5fd,#ff8be9);-webkit-background-clip:text;background-clip:text;color:transparent!important}\n.panel-music{position:fixed;right:24px;bottom:24px;z-index:150;display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:18px;border:1px solid rgba(0,245,255,.18);background:rgba(5,8,14,.72);backdrop-filter:blur(20px);box-shadow:0 12px 50px rgba(0,0,0,.3),0 0 30px rgba(0,245,255,.06)}\n.pm-eq{display:flex;align-items:flex-end;gap:3px;width:24px;height:24px}.pm-eq i{display:block;width:4px;border-radius:3px;background:linear-gradient(#00f5ff,#ff2bd6);height:8px}.panel-music.playing .pm-eq i:nth-child(1){animation:eq .65s infinite alternate}.panel-music.playing .pm-eq i:nth-child(2){animation:eq .45s .12s infinite alternate}.panel-music.playing .pm-eq i:nth-child(3){animation:eq .55s .05s infinite alternate}.panel-music.playing .pm-eq i:nth-child(4){animation:eq .72s .2s infinite alternate}@keyframes eq{to{height:22px}}\n.pm-text{display:flex;flex-direction:column;line-height:1.15}.pm-text b{font-size:10px;color:#fff}.pm-text span{font-size:8px;color:rgba(255,255,255,.4);margin-top:3px}.panel-music button{width:34px;height:34px;border-radius:11px;border:1px solid rgba(0,245,255,.18);background:rgba(0,245,255,.07);color:#67e8f9;font-weight:800;cursor:pointer}.panel-music.playing button{background:linear-gradient(135deg,#00d9ff,#8b5cf6,#ff2bd6);color:#020617}\n@media(max-width:768px){.main{padding:82px 16px 110px!important}.page-title{font-size:28px!important}.panel-music{right:12px;bottom:88px}.pm-text{display:none}.sidebar{width:100%!important}}\n
/* AXIOM ULTRA // cinematic interface layer */
:root{--axi-cyan:#66f7ff;--axi-violet:#9b7bff;--axi-pink:#ff55c8;--axi-blue:#3b82f6}
body{isolation:isolate;overflow-x:hidden}
body:after{content:"AXIOM // PRIVATE INFRASTRUCTURE";position:fixed;left:96px;bottom:18px;z-index:2;font:700 8px/1 Inter,sans-serif;letter-spacing:.32em;color:rgba(102,247,255,.22);pointer-events:none}
.aurora-orb{position:fixed;z-index:-1;pointer-events:none;border-radius:50%;filter:blur(70px);mix-blend-mode:screen;opacity:.28;animation:axiFloat 16s ease-in-out infinite alternate}
.orb-a{width:360px;height:360px;left:8%;top:8%;background:#00d9ff}.orb-b{width:420px;height:420px;right:0;top:32%;background:#7c3aed;animation-delay:-5s}.orb-c{width:300px;height:300px;left:42%;bottom:-8%;background:#ff2bd6;animation-delay:-9s}
@keyframes axiFloat{from{transform:translate3d(-20px,-15px) scale(.92)}to{transform:translate3d(30px,25px) scale(1.08)}}
.noise-layer{position:fixed;inset:0;z-index:1;pointer-events:none;opacity:.025;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 180 180' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='.7'/%3E%3C/svg%3E")}
.scanline-layer{position:fixed;inset:0;z-index:1;pointer-events:none;opacity:.035;background:repeating-linear-gradient(0deg,transparent 0,transparent 3px,rgba(255,255,255,.08) 4px);mix-blend-mode:overlay}
.login-wrap{position:relative;overflow:hidden;background:radial-gradient(ellipse at 50% 20%,rgba(102,247,255,.13),transparent 32%),radial-gradient(ellipse at 20% 90%,rgba(155,123,255,.14),transparent 30%),radial-gradient(ellipse at 90% 80%,rgba(255,85,200,.11),transparent 28%)}
.login-wrap:before{content:"";position:absolute;width:620px;height:620px;border:1px solid rgba(102,247,255,.12);border-radius:50%;box-shadow:0 0 0 55px rgba(102,247,255,.018),0 0 0 110px rgba(155,123,255,.015),0 0 100px rgba(102,247,255,.08);animation:axiSpin 28s linear infinite}
.login-wrap:after{content:"";position:absolute;width:900px;height:1px;background:linear-gradient(90deg,transparent,rgba(102,247,255,.35),transparent);transform:rotate(-28deg);animation:axiSweep 8s ease-in-out infinite}
@keyframes axiSpin{to{transform:rotate(360deg)}}@keyframes axiSweep{0%,100%{opacity:0;translate:-30% 0}50%{opacity:1;translate:30% 0}}
.login-box{position:relative;z-index:3!important;border:1px solid rgba(102,247,255,.28)!important;background:linear-gradient(145deg,rgba(12,19,34,.84),rgba(8,8,18,.72))!important;box-shadow:0 0 0 1px rgba(255,255,255,.025),0 35px 120px rgba(0,0,0,.65),0 0 80px rgba(102,247,255,.09),inset 0 1px rgba(255,255,255,.09)!important}
.login-box:before{content:"";position:absolute;inset:0;border-radius:inherit;padding:1px;background:linear-gradient(130deg,rgba(102,247,255,.55),transparent 25%,transparent 70%,rgba(255,85,200,.45));-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none}
.login-logo svg{filter:drop-shadow(0 0 20px rgba(102,247,255,.35));transform:scale(1.12);margin-bottom:8px}.login-title{letter-spacing:.22em!important;font-size:36px!important;text-shadow:0 0 28px rgba(102,247,255,.16)}.login-sub{letter-spacing:.18em!important;font-size:9px!important}
.login-box .fl{letter-spacing:.18em;color:rgba(102,247,255,.65)}.login-box .fi{height:52px!important;background:rgba(2,5,12,.65)!important;border:1px solid rgba(102,247,255,.16)!important;box-shadow:inset 0 0 20px rgba(0,0,0,.25)}
.login-box .btn-gold{height:52px!important;font-size:12px!important;letter-spacing:.18em!important;position:relative;overflow:hidden}.login-box .btn-gold:after{content:"";position:absolute;inset:0;background:linear-gradient(110deg,transparent 20%,rgba(255,255,255,.35),transparent 80%);transform:translateX(-120%);animation:axiShine 3.5s infinite}@keyframes axiShine{55%,100%{transform:translateX(120%)}}
.sidebar{box-shadow:12px 0 70px rgba(0,0,0,.4),inset -1px 0 rgba(102,247,255,.08)!important}.sb-title{color:rgba(102,247,255,.72)!important;letter-spacing:.28em!important}.sb-brand{position:relative}.sb-brand:after{content:"ONLINE";font-size:6px;letter-spacing:.22em;color:#4ade80;margin-top:5px;opacity:.7}
.page-header{position:relative}.page-header:before{content:"";position:absolute;left:0;top:-8px;width:90px;height:2px;background:linear-gradient(90deg,var(--axi-cyan),transparent);box-shadow:0 0 18px var(--axi-cyan)}
.card,.stat-card{border-color:rgba(102,247,255,.13)!important}.card:after,.stat-card:after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(120deg,rgba(102,247,255,.025),transparent 35%,rgba(255,85,200,.025));opacity:.8}
.card-hd{position:relative;z-index:1}.card-title:before{content:"";width:5px;height:5px;border-radius:50%;background:var(--axi-cyan);box-shadow:0 0 10px var(--axi-cyan);display:inline-block}
.btn-gold{position:relative;overflow:hidden}.btn-gold:after{content:"";position:absolute;inset:0;background:linear-gradient(110deg,transparent 25%,rgba(255,255,255,.28),transparent 75%);transform:translateX(-130%);animation:axiShine 4s infinite}
.nav-item.active:after{content:"";position:absolute;right:5px;width:3px;height:24px;border-radius:4px;background:linear-gradient(#66f7ff,#ff55c8);box-shadow:0 0 12px #66f7ff}
@media(max-width:768px){body:after{left:16px;bottom:12px;font-size:7px}.aurora-orb{filter:blur(55px);opacity:.2}}

</style>
</head>
<body>
<div class="bg-fixed"></div>
<div class="grid-fixed"></div>
<div class="aurora-orb orb-a"></div><div class="aurora-orb orb-b"></div><div class="aurora-orb orb-c"></div>
<div class="noise-layer"></div><div class="scanline-layer"></div>
<div class="toast" id="toast"></div>

<!-- LOGIN PAGE -->
<div id="login-page" style="display:none;width:100%">
  <div class="login-wrap">
    <div class="login-box">
      <div class="login-logo">
        <svg width="52" height="44" viewBox="0 0 84 68" fill="none">
          <ellipse cx="42" cy="52" rx="40" ry="11" fill="#7A0F1F" opacity=".85"/>
          <ellipse cx="42" cy="52" rx="40" ry="11" fill="none" stroke="#C81E2D" stroke-width="1.4" opacity=".6"/>
          <path d="M19 50 Q21 22 42 17 Q63 22 65 50" fill="#4a3a00" stroke="#C81E2D" stroke-width="1.4"/>
          <ellipse cx="42" cy="17" rx="23" ry="5.5" fill="#7A0F1F" stroke="#C81E2D" stroke-width="1"/>
          <path d="M20 45 Q21.5 41.5 42 39.5 Q62.5 41.5 64 45" fill="none" stroke="#CC2200" stroke-width="4.5" stroke-linecap="round" opacity=".92"/>
        </svg>
        <div class="login-title">AXIOM</div>
        <div class="login-sub">SECURE COMMAND CENTER · PRIVATE ACCESS</div>
      </div>
      <div class="fg">
        <label class="fl">PASSWORD</label>
        <input class="fi" type="password" id="login-pw" placeholder="••••••••" onkeydown="if(event.key==='Enter')doLogin()">
      </div>
      <button class="btn btn-gold" onclick="doLogin()" style="width:100%;justify-content:center;padding:12px;margin-top:6px">LOGIN</button>
      <div id="login-err" style="color:var(--red);font-size:12px;margin-top:10px;text-align:center;display:none">Invalid password</div>
    </div>
  </div>
</div>

<!-- DASHBOARD -->
<div id="dashboard-page" style="display:none;width:100%">
  <div class="panel-music" id="panel-music">
    <div class="pm-eq"><i></i><i></i><i></i><i></i></div>
    <div class="pm-text"><b>غروب · سیاوش قمیشی</b><span>Panel ambience</span></div>
    <button id="pm-toggle" onclick="togglePanelMusic()">▶</button>
    <audio id="panel-audio" preload="auto" loop src="/client/audio/ghorob.mp3"></audio>
  </div>

  <!-- MOBILE HEADER -->
  <div class="mob-hd">
    <div class="mob-tl-group">
      <button class="theme-toggle" onclick="toggleTheme()" id="theme-btn-mob">🌙</button>
      <div class="lang-row">
        <button class="lang-btn lang-en active" onclick="setLang('en')">EN</button>
        <button class="lang-btn lang-fa" onclick="setLang('fa')">FA</button>
      </div>
      <div class="mob-social">
        <a href="https://t.me/IMMINENT_SHOP" target="_blank" class="sb-social-btn" title="Telegram Channel">
          <svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.562 8.248l-2.032 9.57c-.148.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12l-6.871 4.326-2.962-.924c-.643-.204-.657-.643.136-.953l11.57-4.461c.537-.194 1.006.131.895.651z"/></svg>
        </a>
      </div>
    </div>
    <span style="font-family:'Cinzel',serif;font-size:16px;font-weight:700;color:var(--gold);letter-spacing:2px">AXIOM</span>
  </div>

  <!-- SIDEBAR -->
  <aside class="sidebar" id="sb">
    <!-- Telegram channel link (above the IMMINENT logo) -->
    <div class="sb-social" style="padding:10px 8px 0">
      <a href="https://t.me/IMMINENT_SHOP" target="_blank" class="sb-social-btn" title="Telegram Channel">
        <svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.562 8.248l-2.032 9.57c-.148.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12l-6.871 4.326-2.962-.924c-.643-.204-.657-.643.136-.953l11.57-4.461c.537-.194 1.006.131.895.651z"/></svg>
      </a>
    </div>
    <div class="sb-brand">
      <div class="sb-hat">
        <svg width="36" height="30" viewBox="0 0 84 68" fill="none">
          <ellipse cx="42" cy="52" rx="40" ry="11" fill="#7A0F1F" opacity=".85"/>
          <ellipse cx="42" cy="52" rx="40" ry="11" fill="none" stroke="#C81E2D" stroke-width="1.4" opacity=".6"/>
          <path d="M19 50 Q21 22 42 17 Q63 22 65 50" fill="#4a3a00" stroke="#C81E2D" stroke-width="1.4"/>
          <ellipse cx="42" cy="17" rx="23" ry="5.5" fill="#7A0F1F" stroke="#C81E2D" stroke-width="1"/>
          <path d="M20 45 Q21.5 41.5 42 39.5 Q62.5 41.5 64 45" fill="none" stroke="#CC2200" stroke-width="4.5" stroke-linecap="round" opacity=".92"/>
          <ellipse cx="35" cy="24" rx="5" ry="3" fill="rgba(255,255,255,.1)" transform="rotate(-20 35 24)"/>
        </svg>
      </div>
      <div class="sb-title">AXIOM</div>
    </div>
    <nav class="sb-nav">
      <button class="nav-item active" data-page="dashboard">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
        <span class="nav-label" data-en="Dashboard" data-fa="داشبورد">Dashboard</span>
      </button>
      <button class="nav-item" data-page="inbounds">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><line x1="23" y1="11" x2="17" y2="11"/><line x1="20" y1="8" x2="20" y2="14"/></svg>
        <span class="nav-label" data-en="Inbounds" data-fa="اینباندها">Inbounds</span>
        <span class="nav-badge" id="nb">0</span>
      </button>
      <button class="nav-item" data-page="traffic">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
        <span class="nav-label" data-en="Traffic" data-fa="ترافیک">Traffic</span>
      </button>
      <button class="nav-item" data-page="addresses">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>
        <span class="nav-label" data-en="Clean IP" data-fa="آی‌پی تمیز">Clean IP</span>
      </button>
      <button class="nav-item" data-page="notifications">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/></svg>
        <span class="nav-label" data-en="Notifications" data-fa="اعلانات">Notifications</span>
        <span class="nav-badge" id="notif-badge" style="display:none">0</span>
      </button>
      <button class="nav-item" data-page="security">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
        <span class="nav-label" data-en="Security" data-fa="امنیت">Security</span>
      </button>
      <button class="nav-item" data-page="settings">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
        <span class="nav-label" data-en="Settings" data-fa="تنظیمات">Settings</span>
      </button>
      <button class="nav-item logout-mob" onclick="doLogout()">
        <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        <span class="nav-label" data-en="Logout" data-fa="خروج">Logout</span>
      </button>
    </nav>
    <div class="sb-bottom">
      <button class="theme-toggle" onclick="toggleTheme()" id="theme-btn-desk" style="margin-bottom:4px;font-size:12px">🌙 Theme</button>
      <div class="lang-row">
        <button class="lang-btn lang-en active" onclick="setLang('en')">EN</button>
        <button class="lang-btn lang-fa" onclick="setLang('fa')">FA</button>
      </div>
      <button class="logout-btn" onclick="doLogout()" style="margin-top:2px">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        <span data-en="Logout" data-fa="خروج">Logout</span>
      </button>
    </div>
  </aside>

  <!-- MAIN CONTENT -->
  <main class="main">

    <!-- Dashboard -->
    <section class="page active" id="page-dashboard">
      <div class="page-header">
        <div>
          <div class="page-title" data-en="Dashboard" data-fa="داشبورد">Dashboard</div>
          <div class="page-sub" id="last-up">-</div>
        </div>
      </div>

      <div class="alerts-box" id="alerts-box">
        <div class="alerts-title">
          <span>⚠️</span>
          <span data-en="SYSTEM WARNINGS" data-fa="هشدارهای سیستم">SYSTEM WARNINGS</span>
        </div>
        <div id="alerts-list"></div>
      </div>

      <div class="stats-row">
        <div class="stat-card" style="animation-delay:.08s"><div class="stat-label" data-en="Traffic" data-fa="ترافیک">Traffic</div><div class="stat-val" id="sv-traffic">-<span class="stat-unit"> MB</span></div></div>
        <div class="stat-card" style="animation-delay:.16s"><div class="stat-label" data-en="Inbounds" data-fa="اینباندها">Inbounds</div><div class="stat-val" id="sv-links">-</div></div>
        <div class="stat-card" style="animation-delay:.24s"><div class="stat-label" data-en="Uptime" data-fa="آپتایم">Uptime</div><div class="stat-val" id="sv-uptime" style="font-size:15px">-</div></div>
        <div class="stat-card" style="animation-delay:.32s"><div class="stat-label" data-en="Domain" data-fa="دامنه">Domain</div><div class="stat-val" id="sv-domain" style="font-size:10px;word-break:break-all;font-weight:500">-</div></div>
      </div>
      <div class="grid-2">
        <div class="card">
          <div class="card-hd"><div class="card-title" data-en="CPU" data-fa="پردازنده">CPU</div><span id="cpu-v" style="font-size:17px;font-weight:700;color:var(--gold)">-%</span></div>
          <div class="sys-bar"><div class="sys-fill" id="cpu-b" style="background:var(--gold)"></div></div>
        </div>
        <div class="card">
          <div class="card-hd"><div class="card-title" data-en="Memory" data-fa="حافظه">Memory</div><span id="mem-v" style="font-size:17px;font-weight:700;color:var(--green)">-%</span></div>
          <div class="sys-bar"><div class="sys-fill" id="mem-b" style="background:var(--green)"></div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-hd"><div class="card-title" data-en="Hourly Traffic" data-fa="ترافیک ساعتی">Hourly Traffic</div></div>
        <div class="chart-container"><canvas id="tc"></canvas></div>
      </div>
    </section>

    <!-- Inbounds -->
    <section class="page" id="page-inbounds">
      <div class="page-header">
        <div>
          <div class="page-title" data-en="Inbounds" data-fa="اینباندها">Inbounds</div>
          <div class="page-sub" data-en="VLESS over WebSocket · TLS" data-fa="VLESS روی WebSocket با TLS">VLESS over WebSocket · TLS</div>
        </div>
        <button class="btn btn-gold" onclick="showAddMo()" data-en="+ Add" data-fa="+ افزودن">+ Add</button>
      </div>
      <div class="tb">
        <div class="search-wrap">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input id="srch" data-ph-en="Search name…" data-ph-fa="جستجوی نام…" placeholder="Search name…" oninput="filterLinks()">
        </div>
        <div class="filter-chips">
          <button class="chip active" data-filter="all" onclick="setFilter('all',this)" data-en="All" data-fa="همه">All</button>
          <button class="chip" data-filter="active" onclick="setFilter('active',this)" data-en="Active" data-fa="فعال">Active</button>
          <button class="chip" data-filter="off" onclick="setFilter('off',this)" data-en="Off" data-fa="غیرفعال">Off</button>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:hidden">
        <div class="tbl-wrap">
          <table class="tbl">
            <thead><tr>
              <th>#</th>
              <th data-en="Name" data-fa="نام">Name</th>
              <th data-en="Type" data-fa="نوع">Type</th>
              <th data-en="Usage" data-fa="مصرف">Usage</th>
              <th data-en="IPs" data-fa="آی‌پی">IPs</th>
              <th data-en="Expiry" data-fa="انقضا">Expiry</th>
              <th data-en="Status" data-fa="وضعیت">Status</th>
              <th data-en="Actions" data-fa="عملیات">Actions</th>
            </tr></thead>
            <tbody id="ltb"></tbody>
          </table>
        </div>
        <div class="m-cards" id="mcards"></div>
        <div class="empty" id="lempty" style="display:none" data-en="No inbounds found" data-fa="هیچ اینباندی یافت نشد">No inbounds found</div>
      </div>
    </section>

    <!-- Traffic -->
    <section class="page" id="page-traffic">
      <div class="page-header"><div><div class="page-title" data-en="Traffic" data-fa="ترافیک">Traffic</div><div class="page-sub" data-en="Statistics & Inbound comparison" data-fa="آمار و مقایسه مصرف کاربران">Statistics & Inbound comparison</div></div></div>
      <div class="grid-2" style="margin-bottom:14px">
        <div class="card">
          <div class="sl-item"><span class="sl-k" data-en="Total Traffic" data-fa="کل ترافیک">Total Traffic</span><span class="sl-v" id="t-tr">-</span></div>
          <div class="sl-item"><span class="sl-k" data-en="Total Requests" data-fa="کل درخواست‌ها">Total Requests</span><span class="sl-v" id="t-rq">-</span></div>
          <div class="sl-item"><span class="sl-k" data-en="Uptime" data-fa="آپتایم">Uptime</span><span class="sl-v" id="t-up">-</span></div>
        </div>
        <div class="card">
          <div class="card-hd"><div class="card-title" data-en="Inbound Traffic Share" data-fa="سهم ترافیک کاربران">Inbound Traffic Share</div></div>
          <div class="chart-container"><canvas id="inbound-chart"></canvas></div>
        </div>
      </div>
    </section>

    <!-- Notifications -->
    <section class="page" id="page-notifications">
      <div class="page-header">
        <div><div class="page-title" data-en="Notifications" data-fa="اعلانات">Notifications</div><div class="page-sub" data-en="Updates, alerts & system messages" data-fa="بروزرسانی‌ها، هشدارها و پیام‌های سیستم">Updates, alerts & system messages</div></div>
        <div style="display:flex;gap:6px">
          <button class="btn btn-ghost btn-sm" onclick="markAllSeen()" data-en="Mark all read" data-fa="خوانده شدن همه">Mark all read</button>
          <button class="btn btn-danger btn-sm" onclick="clearNotifs()" data-en="Clear all" data-fa="حذف همه">Clear all</button>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:hidden">
        <div id="notif-list" style="padding:4px 0">
          <div class="empty" data-en="No notifications" data-fa="هیچ اعلانی وجود ندارد">No notifications</div>
        </div>
      </div>
    </section>

    <!-- Clean IP -->
    <section class="page" id="page-addresses">
      <div class="page-header">
        <div><div class="page-title" data-en="Clean IP" data-fa="آی‌پی تمیز">Clean IP</div><div class="page-sub" data-en="Subscription alternative addresses" data-fa="آدرس‌های جایگزین اشتراک">Subscription alternative addresses</div></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <button class="btn btn-ghost" onclick="importAddrs('railway')" data-en="🚄 Railway IP" data-fa="🚄 آی‌پی ریلوی">🚄 Railway IP</button>
          <button class="btn btn-danger" onclick="delAllAddrs()" data-en="Delete All" data-fa="پاک کردن همه">Delete All</button>
          <button class="btn btn-gold" onclick="showAddAddrMo()" data-en="+ Add" data-fa="+ افزودن">+ Add</button>
        </div>
      </div>
      <div class="card">
        <div style="font-size:12px;color:var(--text3);margin-bottom:12px" data-en="Add your own clean IPs or import from Railway/Cloudflare" data-fa="آی‌پی‌های تمیز خودت رو اضافه کن یا از Railway/Cloudflare ایمپورت کن">Add your own clean IPs or import from Railway/Cloudflare</div>
        <div id="addr-list"></div>
      </div>
    </section>

    <!-- Security & Settings -->
    <section class="page" id="page-security">
      <div class="page-header"><div><div class="page-title" data-en="Security & Settings" data-fa="امنیت و تنظیمات">Security & Settings</div><div class="page-sub" data-en="Settings, Password & Live logs" data-fa="تنظیمات، تغییر رمز پنل و لاگ‌های زنده">Settings, Password & Live logs</div></div></div>
      <div class="grid-2">
        <div class="card">
          <div class="card-hd"><div class="card-title" data-en="Telegram Bot Settings" data-fa="تنظیمات ربات تلگرام">Telegram Bot Settings</div></div>
          <div class="fg"><label class="fl" data-en="Bot Token" data-fa="توکن ربات">Bot Token</label><input class="fi" type="text" id="tg-token" placeholder="123456:ABC-DEF..."></div>
          <div class="fg"><label class="fl" data-en="Admin Chat ID" data-fa="شناسه ادمین">Admin Chat ID</label><input class="fi" type="text" id="tg-admin-id" placeholder="987654321"></div>
          <button class="btn btn-gold" onclick="saveSettings()" style="margin-top:10px;width:100%;justify-content:center" data-en="Save & Restart Bot" data-fa="ذخیره و ریستارت ربات">Save & Restart Bot</button>
        </div>
        <div class="card">
          <div class="card-hd"><div class="card-title" data-en="Change Password" data-fa="تغییر رمز عبور">Change Password</div></div>
          <div class="fg"><label class="fl" data-en="Current Password" data-fa="رمز فعلی">Current Password</label><input class="fi" type="password" id="cpw" placeholder="Current password"></div>
          <div class="fg"><label class="fl" data-en="New Password" data-fa="رمز جدید">New Password</label><input class="fi" type="password" id="npw" placeholder="Min 4 chars"></div>
          <button class="btn btn-gold" onclick="chgPw()" style="margin-top:10px;width:100%;justify-content:center" data-en="Update Password" data-fa="بروزرسانی رمز">Update Password</button>
        </div>
      </div>
      <div class="card" style="margin-top:14px">
        <div class="card-hd"><div class="card-title" data-en="Live Logs" data-fa="لاگ‌های زنده">Live Logs</div></div>
        <div class="live-logs-container" id="log-container">Connecting to live logs...</div>
      </div>
    </section>

    <!-- Settings -->
    <section class="page" id="page-settings">
      <div class="page-header"><div><div class="page-title" data-en="Settings" data-fa="تنظیمات">Settings</div><div class="page-sub" data-en="Railway Permanent Database & Preferences" data-fa="دیتابیس دائمی Railway و تنظیمات">Railway Permanent Database & Preferences</div></div></div>

      <!-- Permanent Database -->
      <div class="card" style="border:1px solid rgba(129,140,248,0.25)">
        <div class="card-hd">
          <div class="card-title" style="color:#818cf8">💾 <span data-en="Permanent Database" data-fa="دیتابیس دائمی">Permanent Database</span></div>
          <span id="rdb-status" style="font-size:11px;color:var(--text3)">-</span>
        </div>
        <div style="font-size:11px;color:var(--text3);margin-bottom:12px;line-height:1.5" data-en="Connect to Railway, select a project and ensure a persistent volume at /data exists for permanent storage." data-fa="به Railway متصل شوید، یک پروژه انتخاب کنید و مطمئن شوید یک volume پایدار در مسیر /data وجود دارد.">
          Connect to Railway, select a project and ensure a persistent volume at /data exists for permanent storage.
        </div>
        <div class="fg">
          <label class="fl" data-en="Railway Token" data-fa="توکن Railway">Railway Token</label>
          <div style="display:flex;gap:8px">
            <input class="fi" type="password" id="rw-token" placeholder="rly_..." style="flex:1">
            <button class="btn btn-ghost btn-sm" onclick="fetchRailwayProjects()" id="rw-fetch-btn" data-en="Fetch" data-fa="دریافت">Fetch</button>
          </div>
        </div>
        <div class="fg">
          <label class="fl" data-en="Project" data-fa="پروژه">Project</label>
          <select class="fs" id="rw-project" disabled>
            <option value="" data-en="-- Select a project --" data-fa="-- پروژه را انتخاب کنید --">-- Select a project --</option>
          </select>
        </div>
        <div class="fg" id="rw-volume-info" style="display:none">
          <div style="display:flex;align-items:center;gap:10px;padding:12px;border-radius:8px;border:1px solid var(--border)" id="rw-volume-box">
            <span id="rw-volume-icon" style="font-size:20px">❓</span>
            <div>
              <div id="rw-volume-title" style="font-weight:600;font-size:13px">-</div>
              <div id="rw-volume-desc" style="font-size:11px;color:var(--text3);margin-top:2px">-</div>
            </div>
            <button class="btn btn-gold btn-sm" id="rw-create-btn" style="margin-left:auto;display:none" onclick="createRailwayVolume()" data-en="Create Volume" data-fa="ایجاد Volume">Create Volume</button>
          </div>
        </div>
      </div>

      <!-- Bot Settings (moved here too) -->
      <div class="card">
        <div class="card-hd"><div class="card-title" data-en="Telegram Bot" data-fa="ربات تلگرام">Telegram Bot</div></div>
        <div class="fg"><label class="fl" data-en="Bot Token" data-fa="توکن ربات">Bot Token</label><input class="fi" type="text" id="rw-tg-token" placeholder="123456:ABC-DEF..."></div>
        <div class="fg"><label class="fl" data-en="Admin Chat ID" data-fa="شناسه ادمین">Admin Chat ID</label><input class="fi" type="text" id="rw-tg-admin" placeholder="987654321"></div>
        <div class="fg" style="display:flex;align-items:center;gap:8px;margin-top:4px">
          <input type="checkbox" id="rw-tg-notify-conn" style="width:16px;height:16px;accent-color:var(--gold)">
          <label for="rw-tg-notify-conn" style="font-size:12px;cursor:pointer" data-en="Notify on every connect / disconnect" data-fa="اعلان هر ورود و خروج (اتصال و قطع اتصال) کاربران">Notify on every connect / disconnect</label>
        </div>
        <button class="btn btn-gold" onclick="saveAllSettings()" style="margin-top:10px;width:100%;justify-content:center" data-en="Save All Settings" data-fa="ذخیره همه تنظیمات">Save All Settings</button>
      </div>
    </section>

  </main>
</div>

<!-- Modals -->
<div class="mo" id="mo-add" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-add').classList.remove('show')">✕</button>
    <div class="mo-title" data-en="ADD INBOUND" data-fa="افزودن اینباند">ADD INBOUND</div>
    <div class="fg"><label class="fl" data-en="Remark" data-fa="توضیح">Remark</label><input class="fi" id="nl" data-ph-en="e.g. User 1" data-ph-fa="مثلاً کاربر ۱" placeholder="e.g. User 1"></div>
    <div class="fr">
      <div class="fg"><label class="fl" data-en="Traffic Limit" data-fa="محدودیت ترافیک">Traffic Limit</label><input class="fi" id="nv" type="number" min="0" step=".1" placeholder="0 = ∞"></div>
      <div class="fg" style="max-width:100px"><label class="fl" data-en="Unit" data-fa="واحد">Unit</label><select class="fs" id="nu"><option>GB</option></select></div>
    </div>
    <div class="fg"><label class="fl" data-en="Max IPs" data-fa="حداکثر آی‌پی">Max IPs</label><input class="fi" id="nc" type="number" min="0" placeholder="0 = ∞"></div>
    <div class="fg"><label class="fl" data-en="Days Valid" data-fa="روزهای اعتبار">Days Valid</label><input class="fi" id="nd" type="number" min="0" placeholder="0 = No expiry"></div>
    <div class="fg" style="border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-top:4px">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
        <input type="checkbox" id="n_vless_enabled" checked style="width:16px;height:16px;accent-color:var(--gold)" onchange="toggleVariantBox('n','vless')">
        <label for="n_vless_enabled" style="font-weight:700;cursor:pointer">VLESS</label>
      </div>
      <div id="n_vless_box">
        <div class="fr">
          <div class="fg">
            <label class="fl" data-en="Transport" data-fa="ترابرد">Transport</label>
            <select class="fs" id="n_vless_transport" onchange="syncAlpnDefault('vless','n_vless_transport','n_vless_alpn')">
              <option value="ws">WebSocket</option>
              <option value="xhttp-packet-up">XHTTP (packet-up)</option>
              <option value="xhttp-stream-up">XHTTP (stream-up)</option>
            </select>
          </div>
          <div class="fg">
            <label class="fl" data-en="Fingerprint" data-fa="فینگرپرینت">Fingerprint</label>
            <select class="fs" id="n_vless_fp">
              <option value="chrome">chrome</option><option value="firefox">firefox</option><option value="safari">safari</option>
              <option value="ios">ios</option><option value="android">android</option><option value="edge">edge</option>
              <option value="360">360</option><option value="qq">qq</option><option value="random">random</option><option value="randomized">randomized</option>
            </select>
          </div>
        </div>
        <div class="fg">
          <label class="fl" data-en="ALPN" data-fa="ALPN">ALPN</label>
          <select class="fs" id="n_vless_alpn">
            <option value="h3">h3</option><option value="h2">h2</option><option value="http/1.1">http/1.1</option>
            <option value="h3,h2,http/1.1">h3,h2,http/1.1</option><option value="h3,h2">h3,h2</option><option value="h2,http/1.1">h2,http/1.1</option>
          </select>
        </div>
      </div>
    </div>
    <div class="fg" style="border:1px solid var(--border);border-radius:10px;padding:10px 12px">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
        <input type="checkbox" id="n_trojan_enabled" style="width:16px;height:16px;accent-color:var(--gold)" onchange="toggleVariantBox('n','trojan')">
        <label for="n_trojan_enabled" style="font-weight:700;cursor:pointer">Trojan</label>
      </div>
      <div id="n_trojan_box" style="display:none">
        <div class="fr">
          <div class="fg">
            <label class="fl" data-en="Transport" data-fa="ترابرد">Transport</label>
            <select class="fs" id="n_trojan_transport" onchange="syncAlpnDefault('trojan','n_trojan_transport','n_trojan_alpn')">
              <option value="ws">WebSocket</option>
              <option value="xhttp-packet-up">XHTTP (packet-up)</option>
              <option value="xhttp-stream-up">XHTTP (stream-up)</option>
            </select>
          </div>
          <div class="fg">
            <label class="fl" data-en="Fingerprint" data-fa="فینگرپرینت">Fingerprint</label>
            <select class="fs" id="n_trojan_fp">
              <option value="chrome">chrome</option><option value="firefox">firefox</option><option value="safari">safari</option>
              <option value="ios">ios</option><option value="android">android</option><option value="edge">edge</option>
              <option value="360">360</option><option value="qq">qq</option><option value="random">random</option><option value="randomized">randomized</option>
            </select>
          </div>
        </div>
        <div class="fg">
          <label class="fl" data-en="ALPN" data-fa="ALPN">ALPN</label>
          <select class="fs" id="n_trojan_alpn">
            <option value="h3">h3</option><option value="h2">h2</option><option value="http/1.1">http/1.1</option>
            <option value="h3,h2,http/1.1">h3,h2,http/1.1</option><option value="h3,h2">h3,h2</option><option value="h2,http/1.1">h2,http/1.1</option>
          </select>
        </div>
      </div>
    </div>
    <div class="fg" style="opacity:.6">
      <label class="fl" data-en="Port" data-fa="پورت">Port</label>
      <input class="fi" value="443" readonly style="cursor:not-allowed">
    </div>
    <button class="btn btn-gold" onclick="createLink()" style="width:100%;justify-content:center;margin-top:12px;padding:12px" data-en="CREATE" data-fa="ایجاد">CREATE</button>
  </div>
</div>

<div class="mo" id="mo-edit" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-edit').classList.remove('show')">✕</button>
    <div class="mo-title" id="et">EDIT INBOUND</div>
    <input type="hidden" id="eu">
    <div class="fg"><label class="fl" data-en="Name" data-fa="نام">Name</label><input class="fi" id="en2" readonly style="opacity:.5;cursor:not-allowed"></div>
    <div class="fr">
      <div class="fg"><label class="fl" data-en="Traffic Limit" data-fa="محدودیت ترافیک">Traffic Limit</label><input class="fi" id="el" type="number" min="0" step=".1" placeholder="0 = ∞"></div>
      <div class="fg" style="max-width:100px"><label class="fl" data-en="Unit" data-fa="واحد">Unit</label><select class="fs" id="eu2"><option>GB</option></select></div>
    </div>
    <div class="fg"><label class="fl" data-en="Max IPs" data-fa="حداکثر آی‌پی">Max IPs</label><input class="fi" id="ec" type="number" min="0" placeholder="0 = ∞"></div>
    <div class="fg"><label class="fl" data-en="Extend Days" data-fa="افزایش روزها">Extend Days</label><input class="fi" id="ed" type="number" min="0" placeholder="0 = no change"></div>
    <div class="fg" style="border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-top:4px">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
        <input type="checkbox" id="e_vless_enabled" style="width:16px;height:16px;accent-color:var(--gold)" onchange="toggleVariantBox('e','vless')">
        <label for="e_vless_enabled" style="font-weight:700;cursor:pointer">VLESS</label>
      </div>
      <div id="e_vless_box">
        <div class="fr">
          <div class="fg">
            <label class="fl" data-en="Transport" data-fa="ترابرد">Transport</label>
            <select class="fs" id="e_vless_transport" onchange="syncAlpnDefault('vless','e_vless_transport','e_vless_alpn')">
              <option value="ws">WebSocket</option>
              <option value="xhttp-packet-up">XHTTP (packet-up)</option>
              <option value="xhttp-stream-up">XHTTP (stream-up)</option>
            </select>
          </div>
          <div class="fg">
            <label class="fl" data-en="Fingerprint" data-fa="فینگرپرینت">Fingerprint</label>
            <select class="fs" id="e_vless_fp">
              <option value="chrome">chrome</option><option value="firefox">firefox</option><option value="safari">safari</option>
              <option value="ios">ios</option><option value="android">android</option><option value="edge">edge</option>
              <option value="360">360</option><option value="qq">qq</option><option value="random">random</option><option value="randomized">randomized</option>
            </select>
          </div>
        </div>
        <div class="fg">
          <label class="fl" data-en="ALPN" data-fa="ALPN">ALPN</label>
          <select class="fs" id="e_vless_alpn">
            <option value="h3">h3</option><option value="h2">h2</option><option value="http/1.1">http/1.1</option>
            <option value="h3,h2,http/1.1">h3,h2,http/1.1</option><option value="h3,h2">h3,h2</option><option value="h2,http/1.1">h2,http/1.1</option>
          </select>
        </div>
      </div>
    </div>
    <div class="fg" style="border:1px solid var(--border);border-radius:10px;padding:10px 12px">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
        <input type="checkbox" id="e_trojan_enabled" style="width:16px;height:16px;accent-color:var(--gold)" onchange="toggleVariantBox('e','trojan')">
        <label for="e_trojan_enabled" style="font-weight:700;cursor:pointer">Trojan</label>
      </div>
      <div id="e_trojan_box" style="display:none">
        <div class="fr">
          <div class="fg">
            <label class="fl" data-en="Transport" data-fa="ترابرد">Transport</label>
            <select class="fs" id="e_trojan_transport" onchange="syncAlpnDefault('trojan','e_trojan_transport','e_trojan_alpn')">
              <option value="ws">WebSocket</option>
              <option value="xhttp-packet-up">XHTTP (packet-up)</option>
              <option value="xhttp-stream-up">XHTTP (stream-up)</option>
            </select>
          </div>
          <div class="fg">
            <label class="fl" data-en="Fingerprint" data-fa="فینگرپرینت">Fingerprint</label>
            <select class="fs" id="e_trojan_fp">
              <option value="chrome">chrome</option><option value="firefox">firefox</option><option value="safari">safari</option>
              <option value="ios">ios</option><option value="android">android</option><option value="edge">edge</option>
              <option value="360">360</option><option value="qq">qq</option><option value="random">random</option><option value="randomized">randomized</option>
            </select>
          </div>
        </div>
        <div class="fg">
          <label class="fl" data-en="ALPN" data-fa="ALPN">ALPN</label>
          <select class="fs" id="e_trojan_alpn">
            <option value="h3">h3</option><option value="h2">h2</option><option value="http/1.1">http/1.1</option>
            <option value="h3,h2,http/1.1">h3,h2,http/1.1</option><option value="h3,h2">h3,h2</option><option value="h2,http/1.1">h2,http/1.1</option>
          </select>
        </div>
      </div>
    </div>
    <div class="fg" style="opacity:.6">
      <label class="fl" data-en="Port" data-fa="پورت">Port</label>
      <input class="fi" value="443" readonly style="cursor:not-allowed">
    </div>
    <div style="display:flex;gap:10px;margin-top:16px">
      <button class="btn btn-gold" onclick="saveEdit()" style="flex:1;justify-content:center;padding:12px" data-en="SAVE" data-fa="ذخیره">SAVE</button>
      <button class="btn btn-danger" onclick="resetTraf()" style="padding:12px" data-en="Reset" data-fa="بازنشانی">Reset</button>
    </div>
  </div>
</div>

<div class="mo" id="mo-qr" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box" style="max-width:340px">
    <button class="mo-close" onclick="document.getElementById('mo-qr').classList.remove('show')">✕</button>
    <div class="mo-title" data-en="QR CODE" data-fa="کد QR">QR CODE</div>
    <div class="qr-box"><img id="qr-img" src="" alt="QR"></div>
    <div style="display:flex;gap:10px;margin-top:16px;justify-content:center">
      <button class="btn btn-gold btn-sm" onclick="dlQR()" style="padding:10px 16px" data-en="Download" data-fa="دانلود">Download</button>
      <button class="btn btn-ghost btn-sm" onclick="document.getElementById('mo-qr').classList.remove('show')" style="padding:10px 16px" data-en="Close" data-fa="بستن">Close</button>
    </div>
  </div>
</div>

<div class="mo" id="mo-addr" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="mo-box">
    <button class="mo-close" onclick="document.getElementById('mo-addr').classList.remove('show')">✕</button>
    <div class="mo-title" data-en="ADD CLEAN IP" data-fa="افزودن آی‌پی تمیز">ADD CLEAN IP</div>
    <div class="fg"><label class="fl" data-en="IPs / Domains (one per line)" data-fa="آی‌پی‌ها (هر خط یک)">IPs / Domains</label><textarea class="fi" id="na" rows="5" placeholder="8.8.8.8&#10;example.com" style="resize:vertical;font-family:monospace"></textarea></div>
    <button class="btn btn-gold" onclick="addAddrs()" style="width:100%;justify-content:center;margin-top:12px;padding:12px" data-en="ADD ALL" data-fa="افزودن همه">ADD ALL</button>
  </div>
</div>

<script>
function $(s){return document.querySelector(s)}
function $m(id){return document.getElementById(id)}
function esc(s){return String(s).replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function protoBadge(variants){
  if(!variants)return 'VLESS';
  const on=[];
  if(variants.vless&&variants.vless.enabled)on.push('VLESS');
  if(variants.trojan&&variants.trojan.enabled)on.push('TROJAN');
  return on.length?on.join('+'):'VLESS';
}

const langMap={
  en:{edit:'Edit',copy:'Copy',sub:'Sub',qr:'QR',del:'Del',gh:'View on GitHub'},
  fa:{edit:'ویرایش',copy:'کپی',sub:'اشتراک',qr:'QR',del:'حذف',gh:'مشاهده در گیت‌هاب'}
};
function tr(key){return(langMap[lang]&&langMap[lang][key])||langMap['en'][key]||key}

let lang=localStorage.getItem('ll')||'en';
let theme=localStorage.getItem('theme')||'dark';
let allLinks=[];
let cf='all';
let sData={};
let tChart=null;
let iChart=null;

// Generates visually distinct colors using the golden-angle rotation so that
// adjacent chart segments never look alike, regardless of how many users exist.
function genDistinctColors(n){
  const colors=[];
  const GOLDEN_ANGLE=137.508;
  const startHue=355; // start near red to match theme, then spread out
  for(let i=0;i<n;i++){
    const hue=(startHue+i*GOLDEN_ANGLE)%360;
    const sat=70+((i*17)%20);   // 70-90%
    const light=48+((i*11)%16); // 48-64%
    colors.push(`hsl(${hue.toFixed(1)},${sat}%,${light}%)`);
  }
  return colors;
}
let allAddrs=[];
let isAuthenticated=false;
let logsWS=null;

function setTheme(t){
  theme=t;
  if(t==='light')document.body.classList.add('light-mode');
  else document.body.classList.remove('light-mode');
  localStorage.setItem('theme',t);
  const icon=t==='light'?'☀️':'🌙';
  const mb=$m('theme-btn-mob');
  const db=$m('theme-btn-desk');
  if(mb)mb.innerHTML=icon;
  if(db)db.innerHTML=icon+' Theme';
  updChartColors();
}
function toggleTheme(){setTheme(theme==='dark'?'light':'dark')}

function setLang(l){
  lang=l;
  document.querySelectorAll('.lang-en').forEach(e=>e.classList.toggle('active',l==='en'));
  document.querySelectorAll('.lang-fa').forEach(e=>e.classList.toggle('active',l==='fa'));
  document.body.dir=l==='fa'?'rtl':'ltr';
  document.querySelectorAll('[data-en]').forEach(el=>{
    const v=el.getAttribute('data-'+l);
    if(v)el.textContent=v;
  });
  document.querySelectorAll('[data-ph-en]').forEach(el=>{
    const v=el.getAttribute('data-ph-'+l);
    if(v)el.placeholder=v;
  });
  localStorage.setItem('ll',l);
  filterLinks();
}

function connectLogsWS(){
  if(logsWS){try{logsWS.close()}catch(e){}}
  const protocol=location.protocol==='https:'?'wss:':'ws:';
  const token=document.cookie.split('; ').find(r=>r.startsWith('ren_session='))?.split('=')[1];
  if(!token)return;
  logsWS=new WebSocket(`${protocol}//${location.host}/ws/live-logs?token=${token}`);
  logsWS.onmessage=function(e){
    const c=$m('log-container');
    if(c){c.textContent+=e.data+'\n';c.scrollTop=c.scrollHeight}
  };
  logsWS.onerror=function(){$m('log-container').textContent='Connection error. Reconnecting...'};
  logsWS.onclose=function(){setTimeout(connectLogsWS,5000)};
}

async function checkAuth(){
  try{
    const r=await fetch('/api/me');
    const d=await r.json();
    if(d.authenticated)showDashboard();
    else showLogin();
  }catch(e){showLogin()}
}

function showLogin(){
  isAuthenticated=false;
  $m('login-page').style.display='';
  $m('dashboard-page').style.display='none';
}

function showDashboard(){
  isAuthenticated=true;
  $m('login-page').style.display='none';
  $m('dashboard-page').style.display='';
  initChart();
  loadStats();
  loadLinks();
  loadAddrs();
  loadSettings();
  loadNotifs();
  updateNotifBadge();
  connectLogsWS();
}

async function doLogin(){
  const pw=$m('login-pw').value;
  $m('login-err').style.display='none';
  try{
    const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw})});
    if(r.ok){$m('login-pw').value='';showDashboard()}
    else $m('login-err').style.display='block';
  }catch(e){$m('login-err').style.display='block'}
}

async function doLogout(){
  await fetch('/api/logout',{method:'POST'});
  showLogin();
}

document.querySelectorAll('.nav-item[data-page]').forEach(el=>{
  el.addEventListener('click',()=>switchPage(el.dataset.page));
});

function switchPage(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const target=$m('page-'+id);
  if(target)target.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.toggle('active',n.dataset.page===id));
}

function toast(msg,err=false){
  const t=$m('toast');
  t.textContent=msg;
  t.className='toast'+(err?' err':'')+' show';
  clearTimeout(t._hide);
  t._hide=setTimeout(()=>t.classList.remove('show'),3000);
}

function fmtB(b){
  if(!b||b===0)return'0 B';
  return b>=1073741824?(b/1073741824).toFixed(2)+' GB':
         b>=1048576?(b/1048576).toFixed(2)+' MB':(b/1024).toFixed(1)+' KB';
}
function fmtLim(b){
  if(!b||b===0)return'∞';
  const g=b/1073741824;
  return(g%1===0?g.toFixed(0):g.toFixed(1))+' GB';
}
function fmtExp(ea){
  if(!ea||ea===0)return'∞';
  const d=new Date(ea)-new Date();
  if(d<=0)return'Expired';
  const days=Math.floor(d/86400000);
  if(days>0)return days+'d';
  const hours=Math.floor(d/3600000);
  if(hours>0)return hours+'h';
  return Math.floor(d/60000)+'m';
}

function setFilter(filter,el){
  cf=filter;
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('active'));
  if(el)el.classList.add('active');
  filterLinks();
}

function filterLinks(){
  const q=($m('srch')?.value||'').toLowerCase();
  let r=allLinks;
  if(cf==='active')r=r.filter(l=>l.active);
  else if(cf==='off')r=r.filter(l=>!l.active);
  if(q)r=r.filter(l=>l.label.toLowerCase().includes(q)||l.uuid.toLowerCase().includes(q));
  renderLinks(r);
}

function processAlertsAndCharts(){
  const alertsList=$m('alerts-list');
  const alertsBox=$m('alerts-box');
  alertsList.innerHTML='';
  let alertCount=0;

  allLinks.forEach(l=>{
    const u=l.used_bytes||0;
    const lim=l.limit_bytes||0;
    const pct=lim>0?(u/lim)*100:0;
    if(lim>0&&pct>=90){
      alertCount++;
      alertsList.innerHTML+=`<div class="alert-item"><span style="font-weight:600">🔴 '${esc(l.label)}' near limit:</span><span>${pct.toFixed(1)}% Used</span></div>`;
    }
    if(l.expires_at){
      const diff=new Date(l.expires_at)-new Date();
      const days=diff/86400000;
      if(days>0&&days<=3){
        alertCount++;
        alertsList.innerHTML+=`<div class="alert-item"><span style="font-weight:600">🟡 '${esc(l.label)}' expiring soon:</span><span>${days.toFixed(1)} Days</span></div>`;
      }
    }
  });
  alertsBox.style.display=alertCount>0?'block':'none';

  if(iChart){
    const sorted=[...allLinks].sort((a,b)=>(b.used_bytes||0)-(a.used_bytes||0)).slice(0,8);
    iChart.data.labels=sorted.map(x=>x.label);
    iChart.data.datasets[0].data=sorted.map(x=>Math.round((x.used_bytes||0)/(1024*1024)));
    iChart.data.datasets[0].backgroundColor=genDistinctColors(sorted.length);
    iChart.update();
  }
}

function renderLinks(links){
  const tb=$m('ltb');
  const em=$m('lempty');
  const mc=$m('mcards');
  if(!links||!links.length){
    tb.innerHTML='';mc.innerHTML='';em.style.display='block';
    em.textContent=em.getAttribute('data-'+lang)||'No inbounds found';
    return;
  }
  em.style.display='none';
  let idx=links.length;
  const rows=links.map(l=>{
    const u=l.used_bytes||0;
    const lim=l.limit_bytes||0;
    const pct=lim>0?Math.min(100,(u/lim)*100):0;
    const col=pct>90?'var(--red)':pct>70?'var(--yellow)':'var(--gold)';
    const ex=fmtExp(l.expires_at);
    const ec=ex==='Expired'?'var(--red)':ex==='∞'?'var(--text3)':'var(--text2)';
    const i=idx--;
    const cc=l.current_connections||0;
    const mc2=l.max_connections||0;
    return{l,pct,col,ex,ec,i,cc,mc2,u,lim};
  });

  const editText=tr('edit');
  const copyText=tr('copy');
  const subText=tr('sub');
  const qrText=tr('qr');
  const delText=tr('del');

  tb.innerHTML=rows.map(r=>`<tr>
    <td style="color:var(--text3);font-size:10.5px">${r.i}</td>
    <td style="font-weight:600">${esc(r.l.label)}</td>
    <td><span class="tag tag-vless">${protoBadge(r.l.variants)}</span></td>
    <td><div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div></td>
    <td style="font-size:11px;font-weight:600;color:${r.mc2>0&&r.cc>=r.mc2?'var(--red)':'var(--text2)'}">${r.cc}/${r.mc2||'∞'}</td>
    <td style="font-size:10.5px;font-weight:700;color:${r.ec}">${r.ex}</td>
    <td><span class="tag ${r.l.active?'tag-on':'tag-off'}">${r.l.active?'On':'Off'}</span></td>
    <td><div style="display:flex;gap:3px;align-items:center;flex-wrap:wrap">
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc((r.l.vless_links||[]).join(String.fromCharCode(10)))}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc((r.l.vless_links||[])[0]||'')}')">${qrText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div></td>
  </tr>`).join('');

  mc.innerHTML=rows.map(r=>`<div class="m-card">
    <div class="m-card-hd">
      <div style="display:flex;align-items:center;gap:7px">
        <span style="font-size:11px;color:var(--text3)">#${r.i}</span>
        <span style="font-weight:600;font-size:14px">${esc(r.l.label)}</span>
        <span class="tag tag-vless">${protoBadge(r.l.variants)}</span>
      </div>
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
    </div>
    <div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div>
    <div style="font-size:11.5px;color:${r.ec};margin-top:6px;font-weight:600">⏳ ${r.ex} · ${r.cc}/${r.mc2||'∞'} IPs</div>
    <div class="m-card-acts">
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc((r.l.vless_links||[]).join(String.fromCharCode(10)))}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc((r.l.vless_links||[])[0]||'')}')">${qrText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div>
  </div>`).join('');
  
  processAlertsAndCharts();
}

async function togLink(el){
  const uid=el.dataset.uid;
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  const na=!l.active;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:na})});
    if(!r.ok)throw new Error();
    l.active=na;filterLinks();loadStats();
  }catch(e){toast('Failed to toggle',true)}
}

function showAddMo(){$m('mo-add').classList.add('show')}

// وقتی transport یک بلاک (vless یا trojan) عوض شد، ALPN همون بلاک رو به پیش‌فرضش ببر
const ALPN_DEFAULTS={
  'vless-ws':'http/1.1','vless-xhttp-packet-up':'h2,http/1.1','vless-xhttp-stream-up':'h2,http/1.1',
  'trojan-ws':'http/1.1','trojan-xhttp-packet-up':'h2,http/1.1','trojan-xhttp-stream-up':'h2,http/1.1',
};
function syncAlpnDefault(auth,transportId,alpnId){
  const key=auth+'-'+$m(transportId).value;
  $m(alpnId).value=ALPN_DEFAULTS[key]||'http/1.1';
}
function toggleVariantBox(prefix,auth){
  $m(prefix+'_'+auth+'_box').style.display=$m(prefix+'_'+auth+'_enabled').checked?'':'none';
}
function readVariantFields(prefix,auth){
  return {
    [auth+'_enabled']: $m(prefix+'_'+auth+'_enabled').checked,
    [auth+'_transport']: $m(prefix+'_'+auth+'_transport').value,
    [auth+'_fingerprint']: $m(prefix+'_'+auth+'_fp').value,
    [auth+'_alpn']: $m(prefix+'_'+auth+'_alpn').value,
  };
}
function fillVariantFields(prefix,auth,variant){
  $m(prefix+'_'+auth+'_enabled').checked=!!(variant&&variant.enabled);
  $m(prefix+'_'+auth+'_transport').value=(variant&&variant.transport)||'ws';
  $m(prefix+'_'+auth+'_fp').value=(variant&&variant.fingerprint)||'chrome';
  $m(prefix+'_'+auth+'_alpn').value=(variant&&variant.alpn)||ALPN_DEFAULTS[auth+'-ws'];
  toggleVariantBox(prefix,auth);
}

async function createLink(){
  const label=$m('nl').value.trim()||'New Link';
  if(!/^[a-zA-Z0-9\-_. ]+$/.test(label)){toast('Only English letters allowed',true);return}
  if(!$m('n_vless_enabled').checked && !$m('n_trojan_enabled').checked){toast('Enable at least one protocol (VLESS or Trojan)',true);return}
  const v=parseFloat($m('nv').value)||0;
  const mc=parseInt($m('nc').value)||0;
  const days=parseInt($m('nd').value)||0;
  const body=Object.assign({label,limit_value:v,limit_unit:'GB',max_connections:mc,days_valid:days},readVariantFields('n','vless'),readVariantFields('n','trojan'));
  try{
    const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error();
    toast('Created');
    $m('nl').value='';$m('nv').value='';$m('nc').value='';$m('nd').value='';
    $m('mo-add').classList.remove('show');
    await loadLinks();await loadStats();
  }catch(e){toast('Error creating link',true)}
}

function showEditMo(uid){
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  $m('eu').value=uid;
  $m('en2').value=l.label;
  $m('el').value=l.limit_bytes>0?(l.limit_bytes/1073741824):'';
  $m('ec').value=l.max_connections>0?l.max_connections:'';
  $m('ed').value='';
  const variants=l.variants||{};
  fillVariantFields('e','vless',variants.vless);
  fillVariantFields('e','trojan',variants.trojan);
  $m('et').textContent=(lang==='fa'?'ویرایش: ':'EDIT: ')+l.label;
  $m('mo-edit').classList.add('show');
}

async function saveEdit(){
  const uid=$m('eu').value;
  if(!$m('e_vless_enabled').checked && !$m('e_trojan_enabled').checked){toast('Enable at least one protocol (VLESS or Trojan)',true);return}
  const v=parseFloat($m('el').value)||0;
  const mc=parseInt($m('ec').value)||0;
  const days=parseInt($m('ed').value)||0;
  const body=Object.assign({limit_value:v,limit_unit:'GB',max_connections:mc},readVariantFields('e','vless'),readVariantFields('e','trojan'));
  if(days>0)body.days_valid=days;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error();
    toast('Updated');$m('mo-edit').classList.remove('show');await loadLinks();
  }catch(e){toast('Error updating',true)}
}

async function resetTraf(){
  const uid=$m('eu').value;
  if(!confirm('Reset traffic for this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({reset_usage:true})});
    if(!r.ok)throw new Error();
    toast('Traffic reset');await loadLinks();
  }catch(e){toast('Error resetting',true)}
}

async function delLink(uid){
  if(!confirm('Delete this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');await loadLinks();await loadStats();
  }catch(e){toast('Error deleting',true)}
}

function cpLink(txt){
  if(!txt){toast('No link to copy',true);return}
  navigator.clipboard.writeText(txt).then(()=>toast('Copied!')).catch(()=>toast('Failed to copy',true));
}

async function cpSub(uid){
  try{
    await navigator.clipboard.writeText('https://'+location.host+'/sub/'+uid);
    toast('Sub URL copied!');
  }catch(e){toast('Failed to copy',true)}
}

function showQR(txt){
  if(!txt){toast('No QR data',true);return}
  $m('qr-img').src='https://api.qrserver.com/v1/create-qr-code/?size=280x280&data='+encodeURIComponent(txt);
  $m('mo-qr').classList.add('show');
}

function dlQR(){
  const a=document.createElement('a');
  a.href=$m('qr-img').src;a.download='imminent-qr.png';a.click();
}

async function loadSettings(){
  try{
    const r=await fetch('/api/settings');
    if(r.ok){const d=await r.json();
      $m('tg-token').value=d.telegram_token||'';
      $m('tg-admin-id').value=d.telegram_admin_id||'';
      if($m('rw-tg-token'))$m('rw-tg-token').value=d.telegram_token||'';
      if($m('rw-tg-admin'))$m('rw-tg-admin').value=d.telegram_admin_id||'';
      if($m('rw-token'))$m('rw-token').value=d.railway_token||'';
      if($m('rw-tg-notify-conn'))$m('rw-tg-notify-conn').checked=!!d.notify_connections;
    }
  }catch(e){}
}

async function saveSettings(){
  const tok=$m('tg-token').value.trim();
  const adm=$m('tg-admin-id').value.trim();
  try{
    const r=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({telegram_token:tok,telegram_admin_id:adm})});
    if(r.ok)toast('Bot settings saved & restarted');
    else toast('Failed to save settings',true);
  }catch(e){toast('Error saving settings',true)}
}

async function saveAllSettings(){
  const tok=($m('rw-tg-token')?.value||'').trim();
  const adm=($m('rw-tg-admin')?.value||'').trim();
  const rwt=($m('rw-token')?.value||'').trim();
  const notifyConn=!!($m('rw-tg-notify-conn')?.checked);
  try{
    const r=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({telegram_token:tok,telegram_admin_id:adm,railway_token:rwt,notify_connections:notifyConn})});
    if(r.ok)toast('All settings saved');
    else toast('Failed to save settings',true);
  }catch(e){toast('Error saving settings',true)}
}

// ── Railway / Permanent Database ──────────────────────────────────────────

async function fetchRailwayProjects(){
  const token=$m('rw-token').value.trim();
  if(!token){toast('Enter your Railway token first',true);return}
  const btn=$m('rw-fetch-btn');
  const sel=$m('rw-project');
  btn.disabled=true;btn.textContent='Loading...';
  sel.disabled=true;sel.innerHTML='<option>Loading...</option>';
  $m('rw-volume-info').style.display='none';
  try{
    const r=await fetch('/api/railway/projects',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    const d=await r.json();
    sel.innerHTML='<option value="">-- Select a project --</option>'+d.projects.map(p=>`<option value="${p.id}">${esc(p.name)}</option>`).join('');
    sel.disabled=false;
    toast('Found '+d.projects.length+' project(s)');
  }catch(e){toast(e.message||'Failed to fetch projects',true);sel.innerHTML='<option value="">Error loading</option>'}
  finally{btn.disabled=false;btn.textContent=btn.getAttribute('data-'+lang)||'Fetch'}
}

async function checkRailwayVolume(){
  const token=$m('rw-token').value.trim();
  const pid=$m('rw-project').value;
  if(!token||!pid){toast('Select a project first',true);return}
  const info=$m('rw-volume-info');
  const icon=$m('rw-volume-icon');
  const title=$m('rw-volume-title');
  const desc=$m('rw-volume-desc');
  const cbtn=$m('rw-create-btn');
  info.style.display='';icon.textContent='⏳';title.textContent='Checking...';desc.textContent='';cbtn.style.display='none';
  try{
    const r=await fetch('/api/railway/volume-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,project_id:pid})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    const d=await r.json();
    const hasData=d.has_data_volume;
    if(hasData){
      icon.textContent='✅';icon.style.color='var(--green)';
      title.textContent='Volume at /data exists!';
      const v=d.volumes.find(x=>x.path==='data'||x.path==='/data')||d.volumes[0];
      desc.textContent=(v?'ID: '+v.id+' | Name: '+v.name+' | State: '+v.state:'');
      cbtn.style.display='none';
      $m('rdb-status').textContent='✅ Active';$m('rdb-status').style.color='var(--green)';
    }else{
      // No volume found - create it automatically, no manual click needed.
      icon.textContent='⏳';title.textContent='No volume found, creating one automatically...';desc.textContent='';
      $m('rdb-status').textContent='⏳ Creating...';$m('rdb-status').style.color='var(--gold)';
      await createRailwayVolume(true);
    }
  }catch(e){toast(e.message||'Failed to check',true);info.style.display='none'}
}

async function createRailwayVolume(silent){
  const token=$m('rw-token').value.trim();
  const pid=$m('rw-project').value;
  if(!token||!pid){toast('Select a project first',true);return}
  const icon=$m('rw-volume-icon');
  const title=$m('rw-volume-title');
  const desc=$m('rw-volume-desc');
  const cbtn=$m('rw-create-btn');
  cbtn.disabled=true;cbtn.textContent='Creating...';
  try{
    const r=await fetch('/api/railway/create-volume',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,project_id:pid})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    if(!silent)toast('Volume created successfully!');
    else toast('/data volume created automatically');
    icon.textContent='✅';icon.style.color='var(--green)';
    title.textContent='Volume at /data created!';
    desc.textContent='It may take a few seconds to finish provisioning.';
    cbtn.style.display='none';
    $m('rdb-status').textContent='✅ Active';$m('rdb-status').style.color='var(--green)';
  }catch(e){
    icon.textContent='❌';icon.style.color='var(--red)';
    title.textContent='No volume at /data found';
    desc.textContent=e.message||'Failed to auto-create volume. Click below to retry.';
    cbtn.style.display='';
    $m('rdb-status').textContent='❌ Missing';$m('rdb-status').style.color='var(--red)';
    toast(e.message||'Failed to create volume',true);
  }
  finally{cbtn.disabled=false;cbtn.textContent=cbtn.getAttribute('data-'+lang)||'Create Volume'}
}

// Auto-check volume when project selection changes
document.addEventListener('change',function(e){
  if(e.target.id==='rw-project'&&e.target.value){
    checkRailwayVolume();
  }
});

async function loadStats(){
  try{
    const r=await fetch('/stats');
    if(r.status===401){showLogin();return}
    if(!r.ok)throw new Error();
    sData=await r.json();
    $m('sv-traffic').innerHTML=(sData.total_traffic_mb||0)+'<span class="stat-unit"> MB</span>';
    $m('sv-links').textContent=sData.links_count||0;
    $m('sv-uptime').textContent=sData.uptime||'-';
    $m('sv-domain').textContent=sData.domain||'-';
    $m('nb').textContent=sData.links_count||0;
    $m('last-up').textContent='Updated '+new Date().toLocaleTimeString();
    if($m('t-tr'))$m('t-tr').textContent=(sData.total_traffic_mb||0)+' MB';
    if($m('t-rq'))$m('t-rq').textContent=(sData.total_requests||0).toLocaleString();
    if($m('t-up'))$m('t-up').textContent=sData.uptime||'-';
    if(sData.cpu_percent!==undefined){
      const c=sData.cpu_percent;
      const cc=c>80?'var(--red)':c>50?'var(--yellow)':'var(--gold)';
      $m('cpu-v').textContent=c.toFixed(1)+'%';$m('cpu-v').style.color=cc;
      $m('cpu-b').style.width=c+'%';$m('cpu-b').style.background=cc;
    }
    if(sData.memory_percent!==undefined){
      const m=sData.memory_percent;
      const mc=m>80?'var(--red)':m>50?'var(--yellow)':'var(--green)';
      $m('mem-v').textContent=m.toFixed(1)+'%';$m('mem-v').style.color=mc;
      $m('mem-b').style.width=m+'%';$m('mem-b').style.background=mc;
    }
    updChart();
  }catch(e){}
}

async function loadLinks(){
  try{
    const r=await fetch('/api/links');
    if(r.status===401){showLogin();return}
    if(!r.ok)throw new Error();
    const d=await r.json();
    allLinks=d.links||[];filterLinks();
  }catch(e){}
}

async function chgPw(){
  const cur=$m('cpw').value;const nw=$m('npw').value;
  if(!cur||!nw){toast('Fill all fields',true);return}
  if(nw.length<4){toast('Password must be at least 4 characters',true);return}
  try{
    const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:cur,new_password:nw})});
    if(!r.ok){const d=await r.json().catch(()=>({}));throw new Error(d.detail||'Error')}
    toast('Password updated');$m('cpw').value='';$m('npw').value='';
  }catch(e){toast(e.message,true)}
}

function initChart(){
  const ctx=$m('tc');
  if(!ctx||tChart)return;
  tChart=new Chart(ctx,{
    type:'bar',
    data:{labels:[],datasets:[{label:'MB',data:[],backgroundColor:'rgba(200,30,45,0.4)',borderColor:'#C81E2D',borderWidth:1,borderRadius:4}]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{display:false},ticks:{color:'rgba(200,30,45,0.35)',font:{size:10}}},
        y:{grid:{color:'rgba(200,30,45,0.06)'},ticks:{color:'rgba(200,30,45,0.35)',font:{size:10},callback:v=>v+' MB'},beginAtZero:true}
      }
    }
  });

  const ctx2=$m('inbound-chart');
  if(ctx2&&!iChart){
    iChart=new Chart(ctx2,{
      type:'doughnut',
      data:{labels:[],datasets:[{data:[],
        backgroundColor:[],
        borderWidth:0}]},
      options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:true,position:'right',labels:{color:'rgba(255,255,255,0.6)',font:{size:10}}}}}
    });
  }
  updChartColors();
}

function updChartColors(){
  if(!tChart)return;
  const col=theme==='light'?'rgba(0,0,0,0.4)':'rgba(200,30,45,0.35)';
  const gridCol=theme==='light'?'rgba(0,0,0,0.06)':'rgba(200,30,45,0.06)';
  tChart.options.scales.x.ticks.color=col;
  tChart.options.scales.y.ticks.color=col;
  tChart.options.scales.y.grid.color=gridCol;
  tChart.update();
}

function updChart(){
  if(!tChart||!sData.hourly_traffic)return;
  const entries=Object.entries(sData.hourly_traffic).sort((a,b)=>a[0].localeCompare(b[0])).slice(-12);
  tChart.data.labels=entries.map(x=>{const p=x[0].split(' ');return p.length>1?p[1]:p[0]});
  tChart.data.datasets[0].data=entries.map(x=>Math.round(x[1]/1048576));
  tChart.update();
}

async function loadAddrs(){
  try{
    const r=await fetch('/api/addresses');
    if(!r.ok)throw new Error();
    const d=await r.json();allAddrs=d.addresses||[];renderAddrs();
  }catch(e){}
}

function renderAddrs(){
  const el=$m('addr-list');
  if(!el)return;
  if(!allAddrs||!allAddrs.length){el.innerHTML='<div style="color:var(--text3);font-size:12px">No addresses added</div>';return}
  el.innerHTML=allAddrs.map((a,i)=>`<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:var(--surface3);border:1px solid var(--border);border-radius:10px;margin-bottom:8px">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="color:var(--gold);font-size:16px">🌐</span>
      <div><div style="font-size:14px;font-weight:600">${esc(a)}</div><div style="font-size:11px;color:var(--text3);margin-top:2px">Address #${i+1}</div></div>
    </div>
    <button class="act-btn act-del" onclick="delAddr(${i})">${tr('del')}</button>
  </div>`).join('');
}

function showAddAddrMo(){$m('na').value='';$m('mo-addr').classList.add('show')}

// ── Notifications ────────────────────────────────────────────────────────
const NOTIF_ICONS = {update:'🔔',quota:'⚠️',expiry:'⏰',info:'ℹ️'};

async function loadNotifs(){
  try{
    const r=await fetch('/api/notifications');
    if(r.status===401)return;
    if(!r.ok)return;
    const d=await r.json();
    renderNotifs(d.notifications||[]);
  }catch(e){}
}

function renderNotifs(notifs){
  const el=$m('notif-list');
  if(!el)return;
  if(!notifs||!notifs.length){
    el.innerHTML='<div class="empty" style="padding:32px">'+(lang==='fa'?'هیچ اعلانی وجود ندارد':'No notifications')+'</div>';
    return;
  }
  el.innerHTML=notifs.map(n=>{
    const icon=NOTIF_ICONS[n.type]||'ℹ️';
    const cls=n.seen?'':'unseen';
    const time=new Date(n.created_at).toLocaleString();
    const linkHtml=n.link?`<a href="${esc(n.link)}" target="_blank" class="notif-link">${tr('gh')} ↗</a>`:'';
    return `<div class="notif-item ${cls}" onclick="markSeen(${n.id})">
      <div class="notif-icon ${n.type}">${icon}</div>
      <div class="notif-body">
        <div class="notif-title">${esc(n.title)}</div>
        <div class="notif-msg">${esc(n.message)}</div>
        <div class="notif-time">${time}</div>
        ${linkHtml}
      </div>
      ${n.seen?'':'<div class="notif-dot"></div>'}
    </div>`;
  }).join('');
}

async function markSeen(id){
  await fetch('/api/notifications/'+id+'/seen',{method:'POST'});
  await loadNotifs();
  await updateNotifBadge();
}

async function markAllSeen(){
  await fetch('/api/notifications/seen-all',{method:'POST'});
  await loadNotifs();
  await updateNotifBadge();
}

async function clearNotifs(){
  if(!confirm(lang==='fa'?'حذف همه اعلانات؟':'Clear all notifications?'))return;
  await fetch('/api/notifications',{method:'DELETE'});
  await loadNotifs();
  await updateNotifBadge();
}

async function updateNotifBadge(){
  try{
    const r=await fetch('/api/notifications/count');
    if(!r.ok)return;
    const d=await r.json();
    const badge=$m('notif-badge');
    if(badge){
      if(d.count>0){badge.style.display='';badge.textContent=d.count}
      else{badge.style.display='none'}
    }
  }catch(e){}
}

async function addAddrs(){
  const lines=($m('na').value||'').trim().split('\n').map(l=>l.trim()).filter(l=>l);
  let ok=0,fail=0;
  for(const a of lines){
    if(!/^[a-zA-Z0-9\-_. ]+$/.test(a)){fail++;continue}
    try{
      const r=await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});
      if(r.ok)ok++;else fail++;
    }catch(e){fail++}
  }
  if(ok)toast('Added '+ok);
  if(fail)toast(fail+' failed',true);
  if(ok){$m('mo-addr').classList.remove('show');await loadAddrs()}
}

async function delAddr(i){
  if(!confirm('Delete this address?'))return;
  try{
    const r=await fetch('/api/addresses/'+i,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');await loadAddrs();
  }catch(e){toast('Error deleting',true)}
}

async function delAllAddrs(){
  if(!allAddrs||!allAddrs.length){toast('No addresses to delete',true);return}
  if(!confirm('Delete ALL clean IP addresses?'))return;
  try{
    const r=await fetch('/api/addresses',{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('All addresses deleted');await loadAddrs();
  }catch(e){toast('Error deleting',true)}
}

// همه‌ی آی‌پی‌های railway_ips.txt رو یکجا (یک درخواست، بدون تاخیر
// به‌ازای هر آی‌پی) به لیست Clean IP اضافه می‌کنه.
async function importAddrs(source){
  try{
    const r=await fetch('/api/addresses/import/'+source,{method:'POST'});
    const d=await r.json().catch(()=>null);
    if(!r.ok){toast((d&&d.detail)||'Error importing',true);return}
    toast((d.added||0)+' address(es) added, '+((d.total_in_file||0)-(d.added||0))+' already existed');
    await loadAddrs();
  }catch(e){toast('Error importing',true)}
}

const panelAudio=document.getElementById('panel-audio');
const panelMusic=document.getElementById('panel-music');
const pmToggle=document.getElementById('pm-toggle');
async function startPanelMusic(){try{panelAudio.volume=.32;await panelAudio.play();panelMusic.classList.add('playing');pmToggle.textContent='❚❚';}catch(e){pmToggle.textContent='▶';}}
function togglePanelMusic(){if(panelAudio.paused)startPanelMusic();else{panelAudio.pause();panelMusic.classList.remove('playing');pmToggle.textContent='▶';}}
setTheme(theme);
setLang(lang);
checkAuth();
let statsInterval=null;
function startPolling(){
  if(statsInterval)clearInterval(statsInterval);
  statsInterval=setInterval(()=>{if(isAuthenticated){loadStats();loadLinks();updateNotifBadge()}},12000);
}
startPolling();

// ── Panel update notifications (checks GitHub for new releases) ────────
const PANEL_VERSION_KEY='imminent_panel_last_version';
const PANEL_GH_NOTIFIED_KEY='imminent_panel_last_notified_gh';
let loadedPanelVersion=null;

async function checkPanelVersion(isPeriodic){
  try{
    const r=await fetch('/api/version');
    if(!r.ok)return;
    const d=await r.json();
    const serverVersion=d.version;

    // Detect that this panel instance was updated since the last time we visited
    if(!loadedPanelVersion){
      loadedPanelVersion=serverVersion;
      const lastSeen=localStorage.getItem(PANEL_VERSION_KEY);
      if(lastSeen&&lastSeen!==serverVersion){
        toast('✅ Panel updated successfully to v'+serverVersion);
      }
      localStorage.setItem(PANEL_VERSION_KEY,serverVersion);
    }

    // Detect that GitHub has a newer release than what's currently running
    if(d.update_available&&d.latest_github_version){
      const alreadyNotified=localStorage.getItem(PANEL_GH_NOTIFIED_KEY);
      if(alreadyNotified!==d.latest_github_version){
        toast('🚀 New version available on GitHub: '+d.latest_github_version+' - pull the latest update');
        localStorage.setItem(PANEL_GH_NOTIFIED_KEY,d.latest_github_version);
      }
    }
  }catch(e){}
}
checkPanelVersion(false);
setInterval(()=>checkPanelVersion(true),5*60*1000);
</script>
</body>
</html>"""

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/panel", response_class=HTMLResponse)
async def panel_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
