import os
import re
import json
import html
import time
import asyncio
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sqlite3
import tempfile
import shutil
import urllib.parse
import csv
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    InputFile,
    MessageEntity,
)
from telegram.constants import ParseMode
from telegram.error import (
    TelegramError,
    BadRequest,
    Forbidden,
    RetryAfter,
    NetworkError,
    TimedOut,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ContextTypes,
    filters,
)


# ============================================================
# ENVIRONMENT
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

ADMIN_RAW = os.getenv(
    "ADMIN_IDS",
    os.getenv("ADMIN_ID", "")
).strip()

ADMIN_IDS = {
    int(x.strip())
    for x in ADMIN_RAW.split(",")
    if x.strip().lstrip("-").isdigit()
}

DB_PATH = Path(
    os.getenv("DATABASE_PATH", "bot_data.db")
)

BACKUP_DIR = Path(
    os.getenv("BACKUP_DIR", ".")
)

if not BOT_TOKEN:
    raise SystemExit(
        "ERROR: BOT_TOKEN environment variable is required."
    )

if not ADMIN_IDS:
    raise SystemExit(
        "ERROR: ADMIN_ID or ADMIN_IDS environment variable is required."
    )


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("telegram_bot")


# ============================================================
# DATABASE
# ============================================================

DB = None


def utc_now():
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def connect_database(path=DB_PATH):
    connection = sqlite3.connect(
        path,
        timeout=30,
        isolation_level=None,
        check_same_thread=False,
    )

    connection.row_factory = sqlite3.Row

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")

    return connection


SCHEMA = [

    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        join_date TEXT NOT NULL,
        last_activity TEXT NOT NULL,
        referrer_id INTEGER,
        referrals INTEGER NOT NULL DEFAULT 0,
        verified INTEGER NOT NULL DEFAULT 0,
        blocked INTEGER NOT NULL DEFAULT 0,
        assigned_number TEXT,
        claim_date TEXT,
        FOREIGN KEY(referrer_id)
            REFERENCES users(id)
            ON DELETE SET NULL
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS admins (
        user_id INTEGER PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 1,
        added_at TEXT NOT NULL
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        username TEXT,
        invite_link TEXT,
        join_text TEXT NOT NULL DEFAULT 'JOIN',
        style TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        position INTEGER NOT NULL DEFAULT 0
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS numbers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        number TEXT NOT NULL UNIQUE,
        active INTEGER NOT NULL DEFAULT 1,
        assigned_user_id INTEGER UNIQUE,
        assigned_at TEXT,
        FOREIGN KEY(assigned_user_id)
            REFERENCES users(id)
            ON DELETE SET NULL
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS claims (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        number_id INTEGER NOT NULL UNIQUE,
        claimed_at TEXT NOT NULL,
        FOREIGN KEY(user_id)
            REFERENCES users(id)
            ON DELETE CASCADE,
        FOREIGN KEY(number_id)
            REFERENCES numbers(id)
            ON DELETE CASCADE
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL,
        referred_id INTEGER NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        reward REAL NOT NULL DEFAULT 0,
        FOREIGN KEY(referrer_id)
            REFERENCES users(id)
            ON DELETE CASCADE,
        FOREIGN KEY(referred_id)
            REFERENCES users(id)
            ON DELETE CASCADE
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL DEFAULT ''
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS messages (
        key TEXT PRIMARY KEY,
        text TEXT NOT NULL DEFAULT '',
        entities_json TEXT NOT NULL DEFAULT '[]',
        media_type TEXT,
        media_file_id TEXT,
        updated_at TEXT NOT NULL
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS buttons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,
        text TEXT NOT NULL,
        action TEXT NOT NULL,
        url TEXT,
        callback TEXT,
        style TEXT,
        row INTEGER NOT NULL DEFAULT 0,
        position INTEGER NOT NULL DEFAULT 0,
        enabled INTEGER NOT NULL DEFAULT 1
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        media_type TEXT,
        text TEXT,
        entities_json TEXT NOT NULL DEFAULT '[]',
        buttons_json TEXT NOT NULL DEFAULT '[]',
        total INTEGER DEFAULT 0,
        sent INTEGER DEFAULT 0,
        failed INTEGER DEFAULT 0,
        blocked INTEGER DEFAULT 0,
        status TEXT DEFAULT 'created'
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_id INTEGER,
        action TEXT NOT NULL,
        details TEXT,
        created_at TEXT NOT NULL
    )
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_users_username
    ON users(username)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_users_name
    ON users(first_name, last_name)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_users_referrer
    ON users(referrer_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_numbers_assignment
    ON numbers(assigned_user_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_referrals_referrer
    ON referrals(referrer_id)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_logs_created
    ON logs(created_at)
    """,
]


DEFAULT_SETTINGS = {
    "bot_name": "Agent Bot",

    "maintenance_mode": "0",

    "claim_enabled": "1",
    "claim_requires_verification": "1",
    "claim_requires_referrals": "0",
    "minimum_referrals": "0",

    "referral_enabled": "1",
    "referral_reward": "0",

    "number_reuse": "0",

    "support_url": "",
    "support_text": "💬 Support",
    "support_style": "primary",

    "whatsapp_button_text": "💬 CONTACT ON WHATSAPP",
    "whatsapp_button_style": "success",

    "stats_show_id": "1",
    "stats_show_username": "1",
    "stats_show_referrals": "1",
    "stats_show_agent": "1",
    "stats_show_status": "1",
    "stats_show_dates": "1",

    "auto_accept_enabled": "1",
    "notify_channel_id": "",
}


DEFAULT_MESSAGES = {
    "start":
        "👋 Welcome to {bot_name}!\n\n"
        "🚀 Join all required channels below, then tap VERIFY.\n\n"
        "📌 You must join every enabled channel to continue.",

    "join":
        "🚀 Please join all required channels, then tap VERIFY.",

    "verify":
        "⏳ Checking your channel membership...",

    "verify_success":
        "✅ Verification successful! Welcome, {first_name}.",

    "main":
        "🏠 {bot_name}\n\n"
        "Choose an option below.",

    "claim":
        "🎯 Claim Agent\n\n"
        "Checking your eligibility...",

    "already_claimed":
        "ℹ️ You have already claimed this agent:\n\n"
        "📱 {agent_number}",

    "no_agent":
        "❌ No agent is currently available. Please try again later.",

    "assigned":
        "🎉 Agent Assigned\n\n"
        "📱 Number: {agent_number}\n\n"
        "Tap the button below to contact your agent.",

    "referral":
        "🤝 REFER & EARN\n\n"
        "Invite your friends using your referral link.\n\n"
        "🔗 Your Referral Link:\n"
        "{referral_link}\n\n"
        "👥 Your Referrals:\n"
        "{referrals}\n\n"
        "💰 Your Earnings:\n"
        "{earnings}",

    "stats":
        "📊 Statistics\n\n"
        "👤 ID: {user_id}\n"
        "👥 Referrals: {referrals}\n"
        "🎯 Agent: {agent_number}\n"
        "📅 Joined: {join_date}\n"
        "📌 Status: {status}",

    "whatsapp":
        "Hello 👋\n\n"
        "Your assigned agent is {agent_number}.\n\n"
        "Please contact them using the button below.",

    "referral_success":
        "🎉 Referral registered successfully!",

    "referral_error":
        "❌ This referral link is invalid or cannot be used.",

    "maintenance":
        "🔧 Maintenance Mode\n\n"
        "Please try again later.",

    "error":
        "⚠️ Something went wrong. Please try again later.",

    "need_referrals":
        "❌ You need {minimum_referrals} referrals before claiming an agent.",
}


# ============================================================
# DB HELPERS
# ============================================================

def initialize_database():
    global DB

    DB = connect_database()

    for query in SCHEMA:
        DB.execute(query)

    for key, value in DEFAULT_SETTINGS.items():
        DB.execute(
            """
            INSERT OR IGNORE INTO settings(key, value)
            VALUES(?, ?)
            """,
            (key, value),
        )

    for key, value in DEFAULT_MESSAGES.items():
        DB.execute(
            """
            INSERT OR IGNORE INTO messages
            (key, text, entities_json, updated_at)
            VALUES(?, ?, ?, ?)
            """,
            (
                key,
                value,
                "[]",
                utc_now(),
            ),
        )

    for admin_id in ADMIN_IDS:
        DB.execute(
            """
            INSERT OR IGNORE INTO admins
            (user_id, enabled, added_at)
            VALUES(?, 1, ?)
            """,
            (admin_id, utc_now()),
        )

    DB.commit()

    logger.info(
        "Database initialized: %s",
        DB_PATH,
    )


@contextmanager
def transaction():
    global DB

    try:
        DB.execute("BEGIN IMMEDIATE")
        yield DB
        DB.execute("COMMIT")
    except Exception:
        try:
            DB.execute("ROLLBACK")
        except Exception:
            pass
        raise


def db_all(query, params=()):
    return DB.execute(query, params).fetchall()


def db_one(query, params=()):
    return DB.execute(query, params).fetchone()


def get_setting(key, default=""):
    row = db_one(
        "SELECT value FROM settings WHERE key=?",
        (key,),
    )

    return row["value"] if row else default


def set_setting(key, value):
    DB.execute(
        """
        INSERT INTO settings(key, value)
        VALUES(?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
        """,
        (key, str(value)),
    )


def log_action(actor_id, action, details=""):
    try:
        DB.execute(
            """
            INSERT INTO logs
            (actor_id, action, details, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (
                actor_id,
                action,
                str(details)[:4000],
                utc_now(),
            ),
        )
        DB.commit()
    except Exception:
        logger.exception("Could not write log")


# ============================================================
# ADMIN
# ============================================================

def is_admin(user_id):
    if user_id in ADMIN_IDS:
        return True

    row = db_one(
        """
        SELECT enabled
        FROM admins
        WHERE user_id=?
        """,
        (user_id,),
    )

    return bool(row and row["enabled"])


# ============================================================
# USER MANAGEMENT
# ============================================================

def register_user(user, referral_id=None):
    user_id = user.id
    timestamp = utc_now()

    existing = db_one(
        "SELECT id FROM users WHERE id=?",
        (user_id,),
    )

    if existing:
        DB.execute(
            """
            UPDATE users
            SET username=?,
                first_name=?,
                last_name=?,
                last_activity=?
            WHERE id=?
            """,
            (
                user.username,
                user.first_name,
                user.last_name,
                timestamp,
                user_id,
            ),
        )
        DB.commit()
        return False

    valid_referrer = None

    if (
        referral_id
        and referral_id != user_id
        and get_setting("referral_enabled", "1") == "1"
    ):
        referrer = db_one(
            "SELECT id FROM users WHERE id=?",
            (referral_id,),
        )

        if referrer:
            valid_referrer = referral_id

    with transaction():

        DB.execute(
            """
            INSERT INTO users
            (
                id,
                username,
                first_name,
                last_name,
                join_date,
                last_activity,
                referrer_id
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                user.username,
                user.first_name,
                user.last_name,
                timestamp,
                timestamp,
                valid_referrer,
            ),
        )

        if valid_referrer:

            already_referred = db_one(
                """
                SELECT id
                FROM referrals
                WHERE referred_id=?
                """,
                (user_id,),
            )

            if not already_referred:

                reward = 0.0

                try:
                    reward = float(
                        get_setting(
                            "referral_reward",
                            "0",
                        )
                    )
                except Exception:
                    reward = 0.0

                DB.execute(
                    """
                    INSERT INTO referrals
                    (
                        referrer_id,
                        referred_id,
                        created_at,
                        reward
                    )
                    VALUES(?, ?, ?, ?)
                    """,
                    (
                        valid_referrer,
                        user_id,
                        timestamp,
                        reward,
                    ),
                )

                DB.execute(
                    """
                    UPDATE users
                    SET referrals=referrals+1
                    WHERE id=?
                    """,
                    (valid_referrer,),
                )

    log_action(
        user_id,
        "user_registered",
        f"referrer={valid_referrer or ''}",
    )

    return True


def get_user(user_id):
    return db_one(
        "SELECT * FROM users WHERE id=?",
        (user_id,),
    )


# ============================================================
# MESSAGE / ENTITY STORAGE
# ============================================================

def serialize_entities(entities):
    return json.dumps(
        [
            entity.to_dict()
            for entity in (entities or [])
        ],
        ensure_ascii=False,
    )


def deserialize_entities(raw, bot):
    try:
        data = json.loads(raw or "[]")

        return [
            MessageEntity.de_json(item, bot)
            for item in data
        ]

    except Exception:
        return []


def save_message(
    key,
    text,
    entities=None,
    media_type=None,
    media_file_id=None,
):
    DB.execute(
        """
        INSERT INTO messages
        (
            key,
            text,
            entities_json,
            media_type,
            media_file_id,
            updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?)

        ON CONFLICT(key)
        DO UPDATE SET
            text=excluded.text,
            entities_json=excluded.entities_json,
            media_type=excluded.media_type,
            media_file_id=excluded.media_file_id,
            updated_at=excluded.updated_at
        """,
        (
            key,
            text or "",
            serialize_entities(entities),
            media_type,
            media_file_id,
            utc_now(),
        ),
    )


def get_message(key):
    row = db_one(
        """
        SELECT *
        FROM messages
        WHERE key=?
        """,
        (key,),
    )

    if not row:
        return {
            "text": DEFAULT_MESSAGES.get(key, ""),
            "entities": "[]",
            "media_type": None,
            "media_file_id": None,
        }

    return {
        "text": row["text"],
        "entities": row["entities_json"],
        "media_type": row["media_type"],
        "media_file_id": row["media_file_id"],
    }


# ============================================================
# PLACEHOLDERS
# ============================================================


def _utf16_len(value):
    return len(value.encode("utf-16-le")) // 2


def render_template_with_entities(text, entities, user_id=None, extra=None):
    """Render placeholders while keeping Telegram UTF-16 entity offsets aligned."""
    text = text or ""
    entities = entities or []
    extra = extra or {}

    # Build the same values as render_template without formatting the text first.
    user = get_user(user_id) if user_id else None
    assigned_number = user["assigned_number"] if user else None
    referrals = user["referrals"] if user else 0
    earnings = 0.0
    if user_id:
        rows = db_all(
            "SELECT reward FROM referrals WHERE referrer_id=?",
            (user_id,),
        )
        earnings = sum(float(row["reward"] or 0) for row in rows)

    values = {
        "first_name": user["first_name"] if user else "",
        "last_name": user["last_name"] if user else "",
        "username": f"@{user['username']}" if user and user["username"] else "",
        "user_id": user_id or "",
        "referrals": referrals,
        "agent_number": assigned_number or "—",
        "total_users": db_one("SELECT COUNT(*) AS c FROM users")["c"],
        "total_verified": db_one("SELECT COUNT(*) AS c FROM users WHERE verified=1")["c"],
        "total_claims": db_one("SELECT COUNT(*) AS c FROM claims")["c"],
        "available_numbers": db_one("SELECT COUNT(*) AS c FROM numbers WHERE active=1 AND assigned_user_id IS NULL")["c"],
        "assigned_numbers": db_one("SELECT COUNT(*) AS c FROM numbers WHERE assigned_user_id IS NOT NULL")["c"],
        "bot_name": get_setting("bot_name", "Agent Bot"),
        "referral_link": extra.get("referral_link", ""),
        "support": get_setting("support_url", ""),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "join_date": user["join_date"] if user else "",
        "claim_date": user["claim_date"] if user and user["claim_date"] else "",
        "status": (
            "Claimed" if user and user["assigned_number"] else
            ("Verified" if user and user["verified"] else "Unverified")
        ),
        "minimum_referrals": get_setting("minimum_referrals", "0"),
        "earnings": earnings,
    }
    values.update(extra)

    class SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    rendered_parts = []
    boundary = [0] * (len(text) + 1)  # Python-character boundary -> rendered UTF-16 boundary
    old_pos = 0
    new_u16 = 0

    for match in re.finditer(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", text):
        start, end = match.span()
        literal = text[old_pos:start]
        rendered_parts.append(literal)
        for idx in range(old_pos, start):
            boundary[idx] = new_u16
            new_u16 += _utf16_len(text[idx])
        # new_u16 is now the rendered boundary immediately before the placeholder.
        boundary[start] = new_u16

        key = match.group(1)
        replacement = str(values.get(key, match.group(0)))
        rendered_parts.append(replacement)
        replacement_u16 = _utf16_len(replacement)
        boundary[end] = new_u16 + replacement_u16
        # Boundaries inside the placeholder are mapped to its rendered start.
        for idx in range(start + 1, end):
            boundary[idx] = new_u16
        new_u16 += replacement_u16
        old_pos = end

    tail = text[old_pos:]
    rendered_parts.append(tail)
    for idx in range(old_pos, len(text)):
        boundary[idx] = new_u16
        new_u16 += _utf16_len(text[idx])
    boundary[len(text)] = new_u16

    rendered = "".join(rendered_parts)

    # Convert Python character boundaries back to Telegram UTF-16 offsets.
    def py_index_from_u16(target):
        total = 0
        for idx, ch in enumerate(text):
            if total >= target:
                return idx
            total += _utf16_len(ch)
            if total >= target:
                return idx + 1
        return len(text)

    adjusted = []
    for entity in entities:
        try:
            old_start_u16 = entity.offset
            old_len_u16 = entity.length
            old_end_u16 = old_start_u16 + old_len_u16
            start_py = py_index_from_u16(old_start_u16)
            end_py = py_index_from_u16(old_end_u16)
            new_start = boundary[max(0, min(len(text), start_py))]
            new_end = boundary[max(0, min(len(text), end_py))]

            # Rebuild the entity directly instead of round-tripping through
            # to_dict()/de_json(). de_json(data, bot=None) is not guaranteed
            # across PTB versions to preserve fields like custom_emoji_id or
            # language once bot=None is passed, which was silently downgrading
            # premium/custom emoji to plain emoji on render. Copying every
            # field manually means nothing Telegram cares about can be lost.
            adjusted.append(
                MessageEntity(
                    type=entity.type,
                    offset=new_start,
                    length=max(0, new_end - new_start),
                    url=entity.url,
                    user=entity.user,
                    language=entity.language,
                    custom_emoji_id=entity.custom_emoji_id,
                )
            )
        except Exception:
            logger.exception("Failed to adjust message entity")
            adjusted.append(entity)

    return rendered, adjusted

def render_template(text, user_id=None, extra=None):
    extra = extra or {}

    user = (
        get_user(user_id)
        if user_id
        else None
    )

    assigned_number = (
        user["assigned_number"]
        if user
        else None
    )

    referrals = (
        user["referrals"]
        if user
        else 0
    )

    earnings = 0.0

    if user_id:
        rows = db_all(
            """
            SELECT reward
            FROM referrals
            WHERE referrer_id=?
            """,
            (user_id,),
        )

        earnings = sum(
            float(row["reward"] or 0)
            for row in rows
        )

    bot_name = get_setting(
        "bot_name",
        "Agent Bot",
    )

    values = {
        "first_name":
            user["first_name"]
            if user
            else "",

        "last_name":
            user["last_name"]
            if user
            else "",

        "username":
            (
                f"@{user['username']}"
                if user and user["username"]
                else ""
            ),

        "user_id":
            user_id or "",

        "referrals":
            referrals,

        "agent_number":
            assigned_number or "—",

        "total_users":
            db_one(
                "SELECT COUNT(*) AS c FROM users"
            )["c"],

        "total_verified":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM users
                WHERE verified=1
                """
            )["c"],

        "total_claims":
            db_one(
                "SELECT COUNT(*) AS c FROM claims"
            )["c"],

        "available_numbers":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM numbers
                WHERE active=1
                AND assigned_user_id IS NULL
                """
            )["c"],

        "assigned_numbers":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM numbers
                WHERE assigned_user_id IS NOT NULL
                """
            )["c"],

        "bot_name":
            bot_name,

        "referral_link":
            extra.get(
                "referral_link",
                "",
            ),

        "support":
            get_setting(
                "support_url",
                "",
            ),

        "date":
            datetime.now().strftime(
                "%Y-%m-%d"
            ),

        "time":
            datetime.now().strftime(
                "%H:%M:%S"
            ),

        "join_date":
            user["join_date"]
            if user
            else "",

        "claim_date":
            (
                user["claim_date"]
                if user and user["claim_date"]
                else ""
            ),

        "status":
            (
                "Claimed"
                if user and user["assigned_number"]
                else (
                    "Verified"
                    if user and user["verified"]
                    else "Unverified"
                )
            ),

        "minimum_referrals":
            get_setting(
                "minimum_referrals",
                "0",
            ),

        "earnings":
            earnings,
    }

    values.update(extra)

    class SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    try:
        return text.format_map(
            SafeDict(values)
        )
    except Exception:
        return text


# ============================================================
# BUTTONS
# ============================================================

VALID_STYLES = {
    "primary",
    "success",
    "danger",
}


def normalize_style(style):
    if style in VALID_STYLES:
        return style

    return None


def create_button(
    text,
    action,
    url=None,
    callback=None,
    style=None,
):
    kwargs = {
        "text": text,
    }

    action = (
        action or "CALLBACK"
    ).upper()

    if action == "URL" and url:
        kwargs["url"] = url

    else:
        kwargs["callback_data"] = (
            callback
            or action.lower()
        )

    normalized = normalize_style(style)

    if normalized:
        kwargs["style"] = normalized

    try:
        return InlineKeyboardButton(
            **kwargs
        )

    except (
        TypeError,
        ValueError,
    ):
        kwargs.pop(
            "style",
            None,
        )

        return InlineKeyboardButton(
            **kwargs
        )


def get_buttons(scope):
    return db_all(
        """
        SELECT *
        FROM buttons
        WHERE scope=?
        AND enabled=1
        ORDER BY row, position, id
        """,
        (scope,),
    )


def keyboard_from_scope(scope):
    buttons = get_buttons(scope)

    rows = {}

    for button in buttons:

        telegram_button = create_button(
            button["text"],
            button["action"],
            url=button["url"],
            callback=button["callback"],
            style=button["style"],
        )

        rows.setdefault(
            button["row"],
            [],
        ).append(
            telegram_button
        )

    return [
        rows[index]
        for index in sorted(rows)
    ]


# ============================================================
# CHANNEL SYSTEM
# ============================================================

VALID_BUTTON_ACTIONS = {"URL", "CALLBACK"}


def normalize_username(value):
    value = (value or "").strip()
    if not value:
        return None
    if value.startswith(("https://t.me/", "http://t.me/")):
        parsed = urllib.parse.urlparse(value)
        path = parsed.path.strip("/")
        if path.startswith("+") or path.startswith("joinchat/"):
            return None
        if path:
            return "@" + path.split("/")[0].lstrip("@")
    if value.startswith("@"):
        return "@" + value[1:].strip()
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        return "@" + value
    return None


def normalize_join_url(username=None, invite_link=None):
    invite_link = (invite_link or "").strip()
    if invite_link:
        if not re.match(r"^https?://t\.me/(?:\+[^\s]+|joinchat/[^\s]+)$", invite_link, re.I):
            raise ValueError("Invalid Telegram invite URL.")
        return invite_link
    normalized = normalize_username(username)
    if normalized:
        return f"https://t.me/{normalized.lstrip('@')}"
    return None


def validate_channel_input(title, channel_id, username=None, invite_link=None, join_text="JOIN", style=None):
    title = (title or "").strip()
    channel_id = (channel_id or "").strip()
    username = (username or "").strip() or None
    invite_link = (invite_link or "").strip() or None
    join_text = (join_text or "JOIN").strip()
    if not title:
        raise ValueError("Channel title is required.")
    if not re.fullmatch(r"-100\d{5,15}|-\d{4,20}", channel_id):
        raise ValueError("Invalid Channel ID. Example: -1001234567890")
    if style and style not in VALID_STYLES:
        raise ValueError("Invalid style. Allowed: primary, success, danger")
    if not join_text:
        raise ValueError("Join button text cannot be empty.")
    normalized_username = normalize_username(username) if username else None
    if username and not normalized_username:
        raise ValueError("Invalid username. Use @username, username, or https://t.me/username")
    if invite_link:
        normalize_join_url(None, invite_link)
    if not normalized_username and not invite_link:
        raise ValueError("A public username or private invite URL is required.")
    return {"title": title, "channel_id": channel_id, "username": normalized_username, "invite_link": invite_link, "join_text": join_text, "style": style or None}


async def validate_channel_with_telegram(bot, config):
    try:
        chat = await bot.get_chat(config["channel_id"])
    except TelegramError as first_exc:
        if config.get("username"):
            try:
                chat = await bot.get_chat(config["username"])
            except TelegramError as second_exc:
                raise ValueError(f"Channel not found or inaccessible: {second_exc}") from second_exc
        else:
            raise ValueError(f"Channel not found or inaccessible: {first_exc}") from first_exc
    if getattr(chat, "type", None) not in {"channel", "supergroup"}:
        raise ValueError("The supplied ID is not a Telegram channel/supergroup.")
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat.id, me.id)
    except TelegramError as exc:
        raise ValueError(f"Bot cannot verify membership in this channel: {exc}") from exc
    if getattr(bot_member, "status", None) not in {"administrator", "creator"}:
        raise ValueError("Bot must be an administrator in the channel to perform membership checks.")
    return chat


def channel_join_url(channel):
    if channel["invite_link"]:
        return channel["invite_link"]
    normalized = normalize_username(channel["username"])
    if normalized:
        return f"https://t.me/{normalized.lstrip('@')}"
    return None


def required_channels():
    return db_all(
        """
        SELECT *
        FROM channels
        WHERE enabled=1
        ORDER BY position, id
        """
    )


def channel_keyboard(channels=None):
    """Build the join/verify keyboard. When *channels* is supplied, show only those channels."""
    channels = list(channels) if channels is not None else list(required_channels())
    rows = []
    for channel in channels:
        url = channel_join_url(channel)
        if url:
            rows.append([
                create_button(
                    channel["join_text"],
                    "URL",
                    url=url,
                    style=normalize_style(channel["style"]),
                )
            ])
        else:
            logger.error("Enabled channel %s has no usable join URL", channel["id"])
            rows.append([
                create_button(
                    "⚠️ Channel unavailable",
                    "CALLBACK",
                    callback=f"channel_config_error:{channel['id']}",
                    style="danger",
                )
            ])
    rows.append([
        create_button("✅ VERIFY", "CALLBACK", callback="verify", style="success")
    ])
    return InlineKeyboardMarkup(rows)


def membership_is_confirmed(member):
    status = getattr(member, "status", "")
    is_member = getattr(member, "is_member", None)
    # Telegram may return restricted members. They are members unless is_member is explicitly False.
    return status in {"member", "administrator", "creator"} or (
        status == "restricted" and is_member is not False
    )


async def check_channel_membership(bot, user_id):
    """Return (missing_channels, check_errors). Errors never masquerade as missing membership."""
    missing = []
    errors = []
    for channel in required_channels():
        try:
            member = await bot.get_chat_member(
                chat_id=channel["channel_id"],
                user_id=user_id,
            )
            if not membership_is_confirmed(member):
                missing.append(channel)
        except RetryAfter as exc:
            errors.append((channel, f"RetryAfter: {exc.retry_after}"))
        except (Forbidden, BadRequest, NetworkError, TimedOut, TelegramError) as exc:
            errors.append((channel, str(exc)))
    return missing, errors


async def notify_admin_new_user(context, user):
    """Send a new-user alert to the configured notification channel, if set."""
    channel_id = get_setting("notify_channel_id", "").strip()
    if not channel_id:
        return

    username = f"@{user.username}" if user.username else "—"
    full_name = html.escape(
        " ".join(
            part
            for part in [user.first_name, user.last_name]
            if part
        )
        or "—"
    )

    text = (
        "🆕 One more user came on your bot!\n\n"
        f"👤 Name: {full_name}\n"
        f"🔖 Username: {html.escape(username)}\n"
        f"🆔 ID: <code>{user.id}</code>"
    )

    try:
        await context.bot.send_message(
            chat_id=channel_id,
            text=text,
            parse_mode=ParseMode.HTML,
        )
    except TelegramError as exc:
        logger.warning(
            "Could not send new-user notification to %s: %s",
            channel_id,
            exc,
        )


async def auto_approve_join_request(update, context):
    """Auto-approve required-channel join requests so users can verify immediately.

    Controlled by the admin panel's Auto-Accept toggle (settings.auto_accept_enabled).
    When off, join requests are left pending for manual admin approval.
    """
    if get_setting("auto_accept_enabled", "1") != "1":
        return
    request = update.chat_join_request
    chat_id = request.chat.id
    user_id = request.from_user.id
    row = db_one(
        "SELECT id FROM channels WHERE enabled=1 AND channel_id=?",
        (str(chat_id),),
    )
    if not row:
        return
    try:
        await context.bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
        logger.info("Auto-approved join request: user=%s channel=%s", user_id, chat_id)
    except (BadRequest, Forbidden, TelegramError) as exc:
        logger.warning("Could not auto-approve join request: user=%s channel=%s error=%s", user_id, chat_id, exc)


# ============================================================
# USER KEYBOARDS
# ============================================================

def main_keyboard():
    custom = keyboard_from_scope(
        "main"
    )

    if custom:
        return InlineKeyboardMarkup(
            custom
        )

    return InlineKeyboardMarkup(
        [
            [
                create_button(
                    "🎯 Claim Agent",
                    "CALLBACK",
                    callback="claim",
                    style="primary",
                ),
                create_button(
                    "📊 Statistics",
                    "CALLBACK",
                    callback="stats",
                    style="primary",
                ),
            ],
            [
                create_button(
                    "🤝 Refer & Earn",
                    "CALLBACK",
                    callback="referral",
                    style="success",
                )
            ],
        ]
    )


def referral_keyboard():
    custom = keyboard_from_scope(
        "referral"
    )

    if custom:
        return InlineKeyboardMarkup(
            custom
        )

    return InlineKeyboardMarkup(
        [
            [
                create_button(
                    "🏠 Main Menu",
                    "CALLBACK",
                    callback="main",
                    style="primary",
                )
            ]
        ]
    )


def claim_keyboard(number, user_id):
    whatsapp_message = render_template(
        get_message(
            "whatsapp"
        )["text"],
        user_id,
    )

    clean_number = re.sub(
        r"\D",
        "",
        number,
    )

    whatsapp_url = (
        "https://wa.me/"
        + clean_number
        + "?text="
        + urllib.parse.quote(
            whatsapp_message,
            safe="",
        )
    )

    custom = keyboard_from_scope(
        "claim"
    )

    if custom:
        return InlineKeyboardMarkup(
            custom
        )

    return InlineKeyboardMarkup(
        [
            [
                create_button(
                    get_setting(
                        "whatsapp_button_text",
                        "💬 CONTACT ON WHATSAPP",
                    ),
                    "URL",
                    url=whatsapp_url,
                    style=get_setting(
                        "whatsapp_button_style",
                        "success",
                    ),
                )
            ]
        ]
    )


# ============================================================
# SAFE TELEGRAM HELPERS
# ============================================================

async def answer_callback(
    callback_query,
    text=None,
    alert=False,
):
    try:
        await callback_query.answer(
            text=text,
            show_alert=alert,
        )
    except TelegramError:
        pass


async def send_configured_message(
    context,
    chat_id,
    key,
    reply_markup=None,
    extra=None,
):
    message = get_message(
        key
    )

    stored_entities = deserialize_entities(
        message["entities"],
        context.bot,
    )
    text, entities = render_template_with_entities(
        message["text"],
        stored_entities,
        chat_id,
        extra,
    )

    media_type = message[
        "media_type"
    ]

    media_file_id = message[
        "media_file_id"
    ]

    try:

        if (
            media_type == "photo"
            and media_file_id
        ):
            return await context.bot.send_photo(
                chat_id=chat_id,
                photo=media_file_id,
                caption=text,
                caption_entities=entities,
                reply_markup=reply_markup,
            )

        if (
            media_type == "video"
            and media_file_id
        ):
            return await context.bot.send_video(
                chat_id=chat_id,
                video=media_file_id,
                caption=text,
                caption_entities=entities,
                reply_markup=reply_markup,
            )

        return await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            entities=entities,
            reply_markup=reply_markup,
        )

    except (
        BadRequest,
        TelegramError,
    ) as exc:

        logger.warning(
            "Configured message failed [%s]: %s",
            key,
            exc,
        )

        try:
            return await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
            )
        except TelegramError:
            return None


async def edit_callback_message(
    callback_query,
    text,
    reply_markup=None,
):
    try:
        await callback_query.message.edit_text(
            text=text,
            reply_markup=reply_markup,
        )
    except TelegramError:
        try:
            await callback_query.message.reply_text(
                text=text,
                reply_markup=reply_markup,
            )
        except TelegramError:
            pass


# ============================================================
# MAINTENANCE
# ============================================================

def maintenance_enabled_for(user_id):
    return (
        get_setting(
            "maintenance_mode",
            "0",
        ) == "1"
        and not is_admin(user_id)
    )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.effective_user:
        return

    if (
        update.effective_chat
        and update.effective_chat.type != "private"
    ):
        return

    user_id = update.effective_user.id

    referral_id = None

    if context.args:
        parameter = context.args[0]

        if parameter.startswith("ref_"):
            referral_id = safe_int(
                parameter[4:],
                None,
            )

    is_new_user = register_user(
        update.effective_user,
        referral_id,
    )

    if is_new_user:
        await notify_admin_new_user(
            context,
            update.effective_user,
        )

    if maintenance_enabled_for(
        user_id
    ):
        await send_configured_message(
            context,
            user_id,
            "maintenance",
        )
        return

    channels = required_channels()

    if channels:

        missing, errors = (
            await check_channel_membership(
                context.bot,
                user_id,
            )
        )

        if missing or errors:

            await send_configured_message(
                context,
                user_id,
                "start",
                reply_markup=channel_keyboard(),
            )

            return

    DB.execute(
        """
        UPDATE users
        SET verified=1,
            last_activity=?
        WHERE id=?
        """,
        (
            utc_now(),
            user_id,
        ),
    )

    DB.commit()

    await send_configured_message(
        context,
        user_id,
        "main",
        reply_markup=main_keyboard(),
    )


# ============================================================
# VERIFY
# ============================================================

async def verify_callback(
    update,
    context,
):
    query = update.callback_query
    user_id = query.from_user.id

    await answer_callback(
        query,
        "Checking...",
    )

    if maintenance_enabled_for(
        user_id
    ):
        await answer_callback(
            query,
            "Maintenance mode",
            True,
        )
        return

    missing, errors = (
        await check_channel_membership(
            context.bot,
            user_id,
        )
    )

    if errors:
        # A transient/permission/network error is not proof that the user failed to join.
        DB.execute("UPDATE users SET verified=0, last_activity=? WHERE id=?", (utc_now(), user_id))
        DB.commit()
        await answer_callback(
            query,
            "Telegram could not check all channels. Please tap VERIFY again.",
            True,
        )
        return

    if missing:
        DB.execute("UPDATE users SET verified=0, last_activity=? WHERE id=?", (utc_now(), user_id))
        DB.commit()

        names = "\n".join(f"• {channel['title']}" for channel in missing)
        join_message = render_template(get_message("join")["text"], user_id)
        text = join_message + "\n\n❗ Still required:\n" + names

        try:
            await query.message.edit_text(
                text=text,
                reply_markup=channel_keyboard(missing),
            )
        except TelegramError:
            try:
                await context.bot.send_message(
                    user_id,
                    text,
                    reply_markup=channel_keyboard(missing),
                )
            except TelegramError:
                pass
        return

    DB.execute(
        """
        UPDATE users
        SET verified=1,
            last_activity=?
        WHERE id=?
        """,
        (
            utc_now(),
            user_id,
        ),
    )

    DB.commit()

    await send_configured_message(
        context,
        user_id,
        "verify_success",
        reply_markup=main_keyboard(),
    )


# ============================================================
# CLAIM AGENT
# ============================================================

async def claim_callback(
    update,
    context,
):
    query = update.callback_query
    user_id = query.from_user.id

    await answer_callback(
        query
    )

    if maintenance_enabled_for(
        user_id
    ):
        return

    user = get_user(
        user_id
    )

    if not user:
        register_user(
            query.from_user
        )
        user = get_user(
            user_id
        )

    if (
        get_setting(
            "claim_enabled",
            "1",
        )
        != "1"
    ):
        await send_configured_message(
            context,
            user_id,
            "error",
        )
        return

    if (
        get_setting(
            "claim_requires_verification",
            "1",
        )
        == "1"
        and not user["verified"]
    ):
        await send_configured_message(
            context,
            user_id,
            "join",
            reply_markup=channel_keyboard(),
        )
        return

    if (
        get_setting(
            "claim_requires_referrals",
            "0",
        )
        == "1"
    ):
        minimum = safe_int(
            get_setting(
                "minimum_referrals",
                "0",
            )
        )

        if user["referrals"] < minimum:
            await send_configured_message(
                context,
                user_id,
                "need_referrals",
            )
            return

    if user["assigned_number"]:

        await send_configured_message(
            context,
            user_id,
            "already_claimed",
            reply_markup=claim_keyboard(
                user["assigned_number"],
                user_id,
            ),
        )

        return

    try:

        with transaction():

            available = DB.execute(
                """
                SELECT id, number
                FROM numbers
                WHERE active=1
                AND assigned_user_id IS NULL
                ORDER BY id
                LIMIT 1
                """
            ).fetchone()

            if not available:
                raise LookupError(
                    "NO_NUMBER"
                )

            timestamp = utc_now()

            result = DB.execute(
                """
                UPDATE numbers
                SET assigned_user_id=?,
                    assigned_at=?
                WHERE id=?
                AND active=1
                AND assigned_user_id IS NULL
                """,
                (
                    user_id,
                    timestamp,
                    available["id"],
                ),
            )

            if result.rowcount != 1:
                raise LookupError(
                    "RETRY"
                )

            DB.execute(
                """
                INSERT INTO claims
                (
                    user_id,
                    number_id,
                    claimed_at
                )
                VALUES(?, ?, ?)
                """,
                (
                    user_id,
                    available["id"],
                    timestamp,
                ),
            )

            DB.execute(
                """
                UPDATE users
                SET assigned_number=?,
                    claim_date=?
                WHERE id=?
                """,
                (
                    available["number"],
                    timestamp,
                    user_id,
                ),
            )

        log_action(
            user_id,
            "claim",
            available["number"],
        )

    except LookupError as exc:

        if str(exc) == "NO_NUMBER":
            key = "no_agent"
        else:
            key = "error"

        await send_configured_message(
            context,
            user_id,
            key,
        )

        return

    except sqlite3.IntegrityError:

        refreshed = get_user(
            user_id
        )

        if (
            refreshed
            and refreshed["assigned_number"]
        ):
            await send_configured_message(
                context,
                user_id,
                "already_claimed",
                reply_markup=claim_keyboard(
                    refreshed["assigned_number"],
                    user_id,
                ),
            )
        else:
            await send_configured_message(
                context,
                user_id,
                "error",
            )

        return

    await send_configured_message(
        context,
        user_id,
        "assigned",
        reply_markup=claim_keyboard(
            available["number"],
            user_id,
        ),
    )


# ============================================================
# STATISTICS
# ============================================================

async def stats_callback(
    update,
    context,
):
    query = update.callback_query
    user_id = query.from_user.id

    await answer_callback(
        query
    )

    bot = await context.bot.get_me()

    referral_link = (
        f"https://t.me/"
        f"{bot.username}"
        f"?start=ref_{user_id}"
    )

    await send_configured_message(
        context,
        user_id,
        "stats",
        reply_markup=main_keyboard(),
        extra={
            "referral_link": referral_link
        },
    )


# ============================================================
# REFERRAL
# ============================================================

async def referral_callback(
    update,
    context,
):
    query = update.callback_query
    user_id = query.from_user.id

    await answer_callback(
        query
    )

    bot = await context.bot.get_me()

    referral_link = (
        f"https://t.me/"
        f"{bot.username}"
        f"?start=ref_{user_id}"
    )

    await send_configured_message(
        context,
        user_id,
        "referral",
        reply_markup=referral_keyboard(),
        extra={
            "referral_link": referral_link
        },
    )


# ============================================================
# MAIN MENU
# ============================================================

async def main_callback(
    update,
    context,
):
    query = update.callback_query

    await answer_callback(
        query
    )

    await send_configured_message(
        context,
        query.from_user.id,
        "main",
        reply_markup=main_keyboard(),
    )


# ============================================================
# ADMIN KEYBOARD
# ============================================================

def admin_keyboard():
    items = [
        ("📊 Dashboard", "adm:dash"),
        ("👥 Users", "adm:users:0"),

        ("📢 Broadcast", "adm:broadcast"),
        ("📣 Channels", "adm:channels:0"),

        ("📱 Numbers", "adm:numbers:0"),
        ("🎯 Claims", "adm:claims:0"),

        ("🤝 Referrals", "adm:refs:0"),
        ("✏️ Messages", "adm:messages"),

        ("🎨 Buttons", "adm:buttons"),
        ("🖼 Media", "adm:media"),

        ("💾 Database", "adm:db"),
        ("📈 Statistics", "adm:stats"),

        ("🔧 Maintenance", "adm:maintenance"),
        ("⚙️ Settings", "adm:settings"),

        ("📋 Logs", "adm:logs:0"),
        ("👑 Admins", "adm:admins"),

        ("🧹 Cleanup", "adm:cleanup"),
        ("✅ Auto-Accept", "adm:autoaccept"),

        ("🎨 Button Colors", "adm:colors"),
    ]

    rows = []

    for index in range(
        0,
        len(items),
        2,
    ):
        row = [
            create_button(
                items[index][0],
                "CALLBACK",
                callback=items[index][1],
                style="primary",
            )
        ]

        if index + 1 < len(items):
            row.append(
                create_button(
                    items[index + 1][0],
                    "CALLBACK",
                    callback=items[index + 1][1],
                    style="primary",
                )
            )

        rows.append(row)

    return InlineKeyboardMarkup(
        rows
    )


async def admin_command(
    update,
    context,
):
    if not is_admin(
        update.effective_user.id
    ):
        return

    await update.message.reply_text(
        "🔐 ADMIN PANEL\n\n"
        "Select an option:",
        reply_markup=admin_keyboard(),
    )


# ============================================================
# ADMIN CALLBACK ROUTER
# ============================================================

async def admin_callback_router(
    update,
    context,
):
    query = update.callback_query
    user_id = query.from_user.id

    if not is_admin(user_id):
        await answer_callback(
            query,
            "Unauthorized",
            True,
        )
        return

    await answer_callback(
        query
    )

    data = query.data or ""

    # Navigation callbacks must cancel stale admin input states.
    if not data.startswith("adm:confirm_broadcast:") and data != "adm:cancel_broadcast":
        context.user_data.clear()

    try:

        if data == "adm:dash":
            await admin_dashboard(
                update,
                context,
            )

        elif data.startswith(
            "adm:users:"
        ):
            await admin_users(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:user:"
        ):
            await admin_user_detail(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data == "adm:broadcast":
            await admin_broadcast_start(
                update,
                context,
            )

        elif data.startswith(
            "adm:channels:"
        ):
            await admin_channels(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:numbers:"
        ):
            await admin_numbers(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:claims:"
        ):
            await admin_claims(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:refs:"
        ):
            await admin_referrals(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data == "adm:messages":
            await admin_messages(
                update,
                context,
            )

        elif data.startswith("adm:editbtn:"):
            await admin_edit_button_flow(update, context, safe_int(data.split(":")[-1], 0))

        elif data.startswith("adm:editchannel:"):
            await admin_edit_channel_flow(update, context, safe_int(data.split(":")[-1], 0))

        elif data.startswith("adm:delch:"):
            channel_id = safe_int(data.split(":")[-1], 0)
            channel = db_one("SELECT * FROM channels WHERE id=?", (channel_id,))
            if not channel:
                await admin_channels(update, context, 0)
            else:
                await edit_admin_message(update, f"⚠️ DELETE CHANNEL?\n\nChannel: {channel['title']}\n\nAre you sure?", InlineKeyboardMarkup([[create_button("❌ DELETE", "CALLBACK", callback=f"adm:confirmdelch:{channel_id}", style="danger"), create_button("↩️ CANCEL", "CALLBACK", callback="adm:channels:0")]]))

        elif data.startswith("adm:confirmdelch:"):
            channel_id = safe_int(data.split(":")[-1], 0)
            with transaction():
                DB.execute("DELETE FROM channels WHERE id=?", (channel_id,))
            log_action(user_id, "delete_channel", str(channel_id))
            await admin_channels(update, context, 0)

        elif data == "adm:channel_health":
            await admin_channel_health(update, context)

        elif data.startswith("adm:chcolor:"):
            style = data.split(":", 2)[2] or None

            wizard = context.user_data.get("channel_wizard_data")

            if (
                context.user_data.get("state") != "channel_wizard"
                or not wizard
                or not wizard.get("channel_id")
            ):
                await edit_admin_message(
                    update,
                    "❌ Channel wizard session expired. Start again from Channels.",
                    admin_keyboard(),
                )
            else:
                try:
                    config = validate_channel_input(
                        wizard.get("title"),
                        wizard.get("channel_id"),
                        wizard.get("username"),
                        wizard.get("invite_link"),
                        wizard.get("join_text", "JOIN"),
                        style,
                    )

                    editing_id = context.user_data.get("channel_wizard_editing")

                    duplicate = db_one(
                        "SELECT id FROM channels WHERE id != ? AND "
                        "(channel_id=? OR (username IS NOT NULL AND username=?) "
                        "OR (invite_link IS NOT NULL AND invite_link=?))",
                        (
                            editing_id or 0,
                            config["channel_id"],
                            config["username"],
                            config["invite_link"],
                        ),
                    )

                    if duplicate:
                        await edit_admin_message(
                            update,
                            "❌ A channel with this ID, username or invite "
                            "link already exists.",
                            admin_keyboard(),
                        )
                    else:
                        await validate_channel_with_telegram(context.bot, config)

                        if editing_id:
                            row = db_one(
                                "SELECT position FROM channels WHERE id=?",
                                (editing_id,),
                            )
                            position = row["position"] if row else 0
                            with transaction():
                                DB.execute(
                                    "UPDATE channels SET title=?, channel_id=?, "
                                    "username=?, invite_link=?, join_text=?, "
                                    "style=?, position=? WHERE id=?",
                                    (
                                        config["title"],
                                        config["channel_id"],
                                        config["username"],
                                        config["invite_link"],
                                        config["join_text"],
                                        config["style"],
                                        position,
                                        editing_id,
                                    ),
                                )
                            log_action(user_id, "edit_channel", str(editing_id))
                            result_text = "✅ Channel updated."
                        else:
                            position = db_one(
                                "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM channels"
                            )["p"]
                            with transaction():
                                DB.execute(
                                    "INSERT INTO channels(title, channel_id, "
                                    "username, invite_link, join_text, style, "
                                    "position) VALUES(?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        config["title"],
                                        config["channel_id"],
                                        config["username"],
                                        config["invite_link"],
                                        config["join_text"],
                                        config["style"],
                                        position,
                                    ),
                                )
                            log_action(user_id, "add_channel", config["channel_id"])
                            result_text = "✅ Channel added."

                        context.user_data.clear()
                        await edit_admin_message(update, result_text, admin_keyboard())

                except ValueError as exc:
                    context.user_data.clear()
                    await edit_admin_message(
                        update,
                        f"❌ CHANNEL VALIDATION FAILED\n\n{exc}",
                        admin_keyboard(),
                    )
                except sqlite3.Error:
                    logger.exception("Channel wizard save failed")
                    context.user_data.clear()
                    await edit_admin_message(
                        update,
                        "❌ Channel could not be saved. No partial data was stored.",
                        admin_keyboard(),
                    )

        elif data == "adm:bcbtn_add":
            if context.user_data.get("broadcast") is None:
                await query.message.reply_text("❌ Broadcast session expired. /cancel and start again.")
            else:
                context.user_data["state"] = "broadcast_button_text"
                await query.message.reply_text(
                    "🔗 Send the button's label text (max 64 characters).\n\n"
                    "/cancel to cancel."
                )

        elif data == "adm:bcbtn_skip":
            if context.user_data.get("broadcast") is None:
                await query.message.reply_text("❌ Broadcast session expired. /cancel and start again.")
            else:
                context.user_data["state"] = "broadcast"
                await send_broadcast_preview(query, context)

        elif data.startswith(
            "adm:msgedit:"
        ):
            await admin_message_edit(
                update,
                context,
                data.split(
                    ":",
                    2,
                )[2],
            )

        elif data.startswith(
            "adm:msgpreview:"
        ):
            await admin_message_preview(
                update,
                context,
                data.split(
                    ":",
                    2,
                )[2],
            )

        elif data == "adm:buttons":
            await admin_buttons(
                update,
                context,
            )

        elif data == "adm:media":
            await admin_media(
                update,
                context,
            )

        elif data == "adm:db":
            await admin_database(
                update,
                context,
            )

        elif data == "adm:stats":
            await admin_dashboard(
                update,
                context,
            )

        elif data == "adm:maintenance":
            await admin_maintenance(
                update,
                context,
            )

        elif data == "adm:toggle_maintenance":
            current = get_setting(
                "maintenance_mode",
                "0",
            )

            set_setting(
                "maintenance_mode",
                "0"
                if current == "1"
                else "1",
            )

            DB.commit()

            log_action(
                user_id,
                "maintenance_toggle",
                get_setting(
                    "maintenance_mode"
                ),
            )

            await admin_maintenance(
                update,
                context,
            )

        elif data == "adm:autoaccept":
            await admin_autoaccept(
                update,
                context,
            )

        elif data == "adm:toggle_autoaccept":
            current = get_setting(
                "auto_accept_enabled",
                "1",
            )

            set_setting(
                "auto_accept_enabled",
                "0"
                if current == "1"
                else "1",
            )

            DB.commit()

            log_action(
                user_id,
                "autoaccept_toggle",
                get_setting(
                    "auto_accept_enabled"
                ),
            )

            await admin_autoaccept(
                update,
                context,
            )

        elif data == "adm:colors":
            await admin_colors(
                update,
                context,
            )

        elif data == "adm:noop":
            await answer_callback(query)

        elif data.startswith("adm:cyclechcolor:"):
            await admin_cycle_channel_color(
                update,
                context,
                safe_int(data.split(":")[-1], 0),
            )

        elif data.startswith("adm:cyclebtncolor:"):
            await admin_cycle_button_color(
                update,
                context,
                safe_int(data.split(":")[-1], 0),
            )

        elif data == "adm:settings":
            await admin_settings(
                update,
                context,
            )

        elif data == "adm:logs:0":
            await admin_logs(
                update,
                context,
                0,
            )

        elif data == "adm:admins":
            await admin_admins(
                update,
                context,
            )

        elif data == "adm:cleanup":
            await admin_cleanup(
                update,
                context,
            )

        elif data == "adm:cleanup_confirm":

            cutoff = (
                datetime.now(
                    timezone.utc
                ).timestamp()
                - (
                    180
                    * 24
                    * 60
                    * 60
                )
            )

            cutoff_date = datetime.fromtimestamp(
                cutoff,
                timezone.utc,
            ).isoformat()

            DB.execute(
                """
                DELETE FROM logs
                WHERE created_at < ?
                """,
                (cutoff_date,),
            )

            DB.commit()

            log_action(
                user_id,
                "cleanup_logs",
            )

            await admin_cleanup(
                update,
                context,
            )

        elif data == "adm:export_users":
            await export_users(
                update,
                context,
            )

        elif data == "adm:export_numbers":
            await export_numbers(
                update,
                context,
            )

        elif data == "adm:backup":
            await create_database_backup(
                update,
                context,
            )

        elif data == "adm:restore":
            context.user_data[
                "awaiting_restore"
            ] = True

            await query.message.reply_text(
                "♻️ Send the SQLite backup file "
                "as a document.\n\n"
                "Send /cancel to cancel."
            )

        elif data == "adm:remove_start_media":
            message = get_message(
                "start"
            )

            entities = deserialize_entities(
                message["entities"],
                context.bot,
            )

            save_message(
                "start",
                message["text"],
                entities,
                None,
                None,
            )

            DB.commit()

            await admin_media(
                update,
                context,
            )

        elif data.startswith(
            "adm:deluser:"
        ):
            await admin_delete_user(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:block:"
        ):
            await admin_block_user(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
                True,
            )

        elif data.startswith(
            "adm:unblock:"
        ):
            await admin_block_user(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
                False,
            )

        elif data.startswith(
            "adm:resetclaim:"
        ):
            await admin_reset_claim(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:togglebtn:"
        ):
            await admin_toggle_button(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:delbtn:"
        ):
            await admin_delete_button(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:delch:"
        ):
            await admin_delete_channel(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:togglech:"
        ):
            await admin_toggle_channel(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:delnum:"
        ):
            await admin_delete_number(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:togglenum:"
        ):
            await admin_toggle_number(
                update,
                context,
                safe_int(
                    data.split(":")[-1]
                ),
            )

        elif data.startswith(
            "adm:add:"
        ):
            await admin_add_flow(
                update,
                context,
                data.split(
                    ":",
                    2,
                )[2],
            )

        elif data.startswith(
            "adm:search:"
        ):
            search_type = data.split(
                ":",
                2,
            )[2]

            context.user_data[
                "admin_search_type"
            ] = search_type

            context.user_data[
                "state"
            ] = "search"

            await query.message.reply_text(
                "🔎 Send your search query.\n\n"
                "/cancel to cancel."
            )

        elif data.startswith(
            "adm:setting:"
        ):
            await admin_setting_edit(
                update,
                context,
                data.split(
                    ":",
                    2,
                )[2],
            )

        elif data == "adm:cancel":
            context.user_data.clear()
            await edit_admin_message(update, "❌ Cancelled.", admin_keyboard())

        elif data.startswith(
            "adm:confirm_broadcast:"
        ):
            await broadcast_confirm(
                update,
                context,
            )

        elif data == "adm:cancel_broadcast":
            context.user_data.clear()

            await query.message.reply_text(
                "❌ Broadcast cancelled.",
                reply_markup=admin_keyboard(),
            )

    except Exception as exc:
        logger.exception(
            "Admin callback error"
        )

        try:
            await query.message.reply_text(
                "⚠️ Admin action failed safely. Check the bot logs for details."
            )
        except TelegramError:
            pass


# ============================================================
# ADMIN DASHBOARD
# ============================================================

async def admin_dashboard(
    update,
    context,
):
    values = {
        "users":
            db_one(
                "SELECT COUNT(*) AS c FROM users"
            )["c"],

        "verified":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM users
                WHERE verified=1
                """
            )["c"],

        "claims":
            db_one(
                "SELECT COUNT(*) AS c FROM claims"
            )["c"],

        "available":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM numbers
                WHERE active=1
                AND assigned_user_id IS NULL
                """
            )["c"],

        "assigned":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM numbers
                WHERE assigned_user_id IS NOT NULL
                """
            )["c"],

        "referrals":
            db_one(
                "SELECT COUNT(*) AS c FROM referrals"
            )["c"],

        "blocked":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM users
                WHERE blocked=1
                """
            )["c"],

        "broadcasts":
            db_one(
                """
                SELECT COUNT(*) AS c
                FROM broadcasts
                WHERE status='completed'
                """
            )["c"],
    }

    text = (
        "📊 DASHBOARD\n\n"
        f"👥 Total Users: {values['users']}\n"
        f"✅ Verified Users: {values['verified']}\n"
        f"🎯 Total Claims: {values['claims']}\n"
        f"📱 Available Numbers: {values['available']}\n"
        f"📱 Assigned Numbers: {values['assigned']}\n"
        f"🤝 Total Referrals: {values['referrals']}\n"
        f"🚫 Blocked Users: {values['blocked']}\n"
        f"📢 Broadcasts: {values['broadcasts']}"
    )

    await edit_admin_message(
        update,
        text,
        admin_keyboard(),
    )


# ============================================================
# ADMIN USERS
# ============================================================

async def admin_users(
    update,
    context,
    page=0,
):
    per_page = 8

    total = db_one(
        "SELECT COUNT(*) AS c FROM users"
    )["c"]

    users = db_all(
        """
        SELECT *
        FROM users
        ORDER BY join_date DESC
        LIMIT ?
        OFFSET ?
        """,
        (
            per_page,
            page * per_page,
        ),
    )

    if users:

        lines = []

        for user in users:

            name = html.escape(
                user["first_name"] or ""
            )

            lines.append(
                f"• <code>{user['id']}</code> "
                f"{name} "
                f"@{user['username'] or '—'} "
                f"→ "
                f"{user['assigned_number'] or '—'}"
            )

        text = (
            f"👥 USERS — {total}\n\n"
            + "\n".join(lines)
        )

    else:
        text = "👥 No users."

    rows = [
        [
            create_button(
                "🔎 Search",
                "CALLBACK",
                callback="adm:search:users",
                style="primary",
            )
        ]
    ]

    for user in users:

        rows.append(
            [
                create_button(
                    str(user["id"]),
                    "CALLBACK",
                    callback=(
                        f"adm:user:"
                        f"{user['id']}"
                    ),
                ),
                create_button(
                    "🔓"
                    if user["blocked"]
                    else "🚫",
                    "CALLBACK",
                    callback=(
                        "adm:unblock:"
                        if user["blocked"]
                        else "adm:block:"
                    )
                    + str(user["id"]),
                ),
            ]
        )

    navigation = []

    if page > 0:
        navigation.append(
            create_button(
                "⬅️",
                "CALLBACK",
                callback=(
                    f"adm:users:"
                    f"{page - 1}"
                ),
            )
        )

    navigation.append(
        create_button(
            (
                f"📄 {page + 1}/"
                f"{max(1, (total + per_page - 1) // per_page)}"
            ),
            "CALLBACK",
            callback="noop",
        )
    )

    if (
        (page + 1)
        * per_page
        < total
    ):
        navigation.append(
            create_button(
                "➡️",
                "CALLBACK",
                callback=(
                    f"adm:users:"
                    f"{page + 1}"
                ),
            )
        )

    rows.append(
        navigation
    )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


async def admin_user_detail(
    update,
    context,
    user_id,
):
    user = get_user(
        user_id
    )

    if not user:
        await edit_admin_message(
            update,
            "❌ User not found.",
            InlineKeyboardMarkup(
                [
                    [
                        create_button(
                            "⬅️ Users",
                            "CALLBACK",
                            callback="adm:users:0",
                        )
                    ]
                ]
            ),
        )
        return

    text = (
        "👤 USER DETAILS\n\n"
        f"🆔 ID: <code>{user['id']}</code>\n"
        f"👤 Username: @{html.escape(user['username'] or '—')}\n"
        f"📛 Name: {html.escape((user['first_name'] or '') + ' ' + (user['last_name'] or ''))}\n"
        f"🤝 Referrals: {user['referrals']}\n"
        f"👤 Referrer: {user['referrer_id'] or '—'}\n"
        f"✅ Verified: {'Yes' if user['verified'] else 'No'}\n"
        f"🎯 Agent: {user['assigned_number'] or '—'}\n"
        f"📅 Joined: {user['join_date']}\n"
        f"🎯 Claim Date: {user['claim_date'] or '—'}\n"
        f"🚫 Blocked: {'Yes' if user['blocked'] else 'No'}"
    )

    rows = [
        [
            create_button(
                "🔓 Unblock"
                if user["blocked"]
                else "🚫 Block",
                "CALLBACK",
                callback=(
                    "adm:unblock:"
                    if user["blocked"]
                    else "adm:block:"
                )
                + str(user_id),
            ),
            create_button(
                "♻️ Reset Claim",
                "CALLBACK",
                callback=(
                    f"adm:resetclaim:{user_id}"
                ),
            ),
        ],
        [
            create_button(
                "🗑 Delete",
                "CALLBACK",
                callback=(
                    f"adm:deluser:{user_id}"
                ),
                style="danger",
            ),
            create_button(
                "⬅️ Users",
                "CALLBACK",
                callback="adm:users:0",
            ),
        ],
    ]

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# ADMIN CHANNELS
# ============================================================

async def admin_channels(
    update,
    context,
    page=0,
):
    channels = db_all(
        """
        SELECT *
        FROM channels
        ORDER BY position, id
        """
    )

    lines = []

    for index, channel in enumerate(
        channels,
        start=1,
    ):
        lines.append(
            f"{index}. "
            f"{'✅' if channel['enabled'] else '❌'} "
            f"{html.escape(channel['title'])} "
            f"| {channel['channel_id']}"
        )

    text = (
        "📣 CHANNEL MANAGEMENT\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No channels configured."
        )
    )

    rows = [
        [
            create_button(
                "➕ Add Channel",
                "CALLBACK",
                callback="adm:add:channel",
                style="success",
            )
        ]
    ]

    for channel in channels:

        rows.append(
            [
                create_button(
                    "✏️ "
                    + channel["title"][:18],
                    "CALLBACK",
                    callback=(
                        f"adm:editchannel:"
                        f"{channel['id']}"
                    ),
                ),
                create_button(
                    "❌"
                    if channel["enabled"]
                    else "✅",
                    "CALLBACK",
                    callback=(
                        f"adm:togglech:"
                        f"{channel['id']}"
                    ),
                ),
                create_button(
                    "🗑",
                    "CALLBACK",
                    callback=(
                        f"adm:delch:"
                        f"{channel['id']}"
                    ),
                    style="danger",
                ),
            ]
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


async def admin_channel_health(update, context):
    channels = db_all("SELECT * FROM channels ORDER BY position, id")
    lines = ["🩺 CHANNEL HEALTH\n"]
    for channel in channels:
        try:
            config = {"channel_id": channel["channel_id"], "username": channel["username"]}
            await validate_channel_with_telegram(context.bot, config)
            lines.append(f"✅ {channel['title']} — Working")
        except Exception as exc:
            lines.append(f"❌ {channel['title']} — {str(exc)[:180]}")
    await edit_admin_message(update, "\n".join(lines), InlineKeyboardMarkup([[create_button("⬅️ Channels", "CALLBACK", callback="adm:channels:0")]]))


# ============================================================
# ADMIN NUMBERS
# ============================================================

async def admin_numbers(
    update,
    context,
    page=0,
):
    per_page = 10

    total = db_one(
        "SELECT COUNT(*) AS c FROM numbers"
    )["c"]

    numbers = db_all(
        """
        SELECT *
        FROM numbers
        ORDER BY id DESC
        LIMIT ?
        OFFSET ?
        """,
        (
            per_page,
            page * per_page,
        ),
    )

    lines = []

    for number in numbers:

        lines.append(
            f"{number['id']}. "
            f"{'✅' if number['active'] else '❌'} "
            f"<code>{number['number']}</code> "
            f"→ "
            f"{number['assigned_user_id'] or 'available'}"
        )

    text = (
        f"📱 NUMBERS — {total}\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No numbers."
        )
    )

    rows = [
        [
            create_button(
                "➕ Add",
                "CALLBACK",
                callback="adm:add:number",
                style="success",
            ),
            create_button(
                "📥 Bulk Add",
                "CALLBACK",
                callback="adm:add:bulk_number",
                style="success",
            ),
        ],
        [
            create_button(
                "🔎 Search",
                "CALLBACK",
                callback="adm:search:numbers",
            )
        ],
    ]

    for number in numbers:

        rows.append(
            [
                create_button(
                    number["number"][:18],
                    "CALLBACK",
                    callback="noop",
                ),
                create_button(
                    "❌"
                    if number["active"]
                    else "✅",
                    "CALLBACK",
                    callback=(
                        f"adm:togglenum:"
                        f"{number['id']}"
                    ),
                ),
                create_button(
                    "🗑",
                    "CALLBACK",
                    callback=(
                        f"adm:delnum:"
                        f"{number['id']}"
                    ),
                    style="danger",
                ),
            ]
        )

    navigation = []

    if page > 0:
        navigation.append(
            create_button(
                "⬅️",
                "CALLBACK",
                callback=(
                    f"adm:numbers:"
                    f"{page - 1}"
                ),
            )
        )

    if (
        (page + 1)
        * per_page
        < total
    ):
        navigation.append(
            create_button(
                "➡️",
                "CALLBACK",
                callback=(
                    f"adm:numbers:"
                    f"{page + 1}"
                ),
            )
        )

    if navigation:
        rows.append(
            navigation
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# ADMIN CLAIMS
# ============================================================

async def admin_claims(
    update,
    context,
    page=0,
):
    per_page = 10

    total = db_one(
        "SELECT COUNT(*) AS c FROM claims"
    )["c"]

    claims = db_all(
        """
        SELECT
            c.*,
            u.username,
            n.number
        FROM claims c
        JOIN users u
            ON u.id=c.user_id
        JOIN numbers n
            ON n.id=c.number_id
        ORDER BY c.id DESC
        LIMIT ?
        OFFSET ?
        """,
        (
            per_page,
            page * per_page,
        ),
    )

    lines = []

    for claim in claims:
        lines.append(
            f"• {claim['user_id']} "
            f"@{claim['username'] or '—'} "
            f"→ <code>{claim['number']}</code>\n"
            f"  {claim['claimed_at']}"
        )

    text = (
        f"🎯 CLAIMS — {total}\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No claims."
        )
    )

    rows = []

    navigation = []

    if page > 0:
        navigation.append(
            create_button(
                "⬅️",
                "CALLBACK",
                callback=(
                    f"adm:claims:"
                    f"{page - 1}"
                ),
            )
        )

    if (
        (page + 1)
        * per_page
        < total
    ):
        navigation.append(
            create_button(
                "➡️",
                "CALLBACK",
                callback=(
                    f"adm:claims:"
                    f"{page + 1}"
                ),
            )
        )

    if navigation:
        rows.append(
            navigation
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# ADMIN REFERRALS
# ============================================================

async def admin_referrals(
    update,
    context,
    page=0,
):
    referrals = db_all(
        """
        SELECT
            r.*,
            u1.username AS referrer_username,
            u2.username AS referred_username
        FROM referrals r
        LEFT JOIN users u1
            ON u1.id=r.referrer_id
        LEFT JOIN users u2
            ON u2.id=r.referred_id
        ORDER BY r.id DESC
        LIMIT 15
        """
    )

    lines = []

    for referral in referrals:
        lines.append(
            f"• "
            f"{referral['referrer_id']} "
            f"@{referral['referrer_username'] or '—'} "
            f"← "
            f"{referral['referred_id']} "
            f"@{referral['referred_username'] or '—'}"
        )

    text = (
        "🤝 REFERRALS\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No referrals."
        )
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "⬅️ Admin",
                        "CALLBACK",
                        callback="adm:dash",
                    )
                ]
            ]
        ),
    )


# ============================================================
# MESSAGE EDITOR
# ============================================================

async def admin_messages(
    update,
    context,
):
    rows = []

    for key in DEFAULT_MESSAGES:

        label = key.replace(
            "_",
            " ",
        ).title()

        rows.append(
            [
                create_button(
                    "✏️ " + label,
                    "CALLBACK",
                    callback=(
                        f"adm:msgedit:{key}"
                    ),
                    style="success",
                ),
                create_button(
                    "👁",
                    "CALLBACK",
                    callback=(
                        f"adm:msgpreview:{key}"
                    ),
                ),
            ]
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        "✏️ MESSAGE EDITOR\n\n"
        "Select a user-facing message:",
        InlineKeyboardMarkup(
            rows
        ),
    )


async def admin_message_edit(
    update,
    context,
    key,
):
    context.user_data[
        "state"
    ] = "message"

    context.user_data[
        "editing_message"
    ] = key

    await edit_admin_message(
        update,
        "✏️ EDIT MESSAGE\n\n"
        f"Message: {key}\n\n"
        "Send the new formatted Telegram message.\n\n"
        "You can send:\n"
        "• formatted text\n"
        "• custom/Premium emojis\n"
        "• photo + caption\n"
        "• video + caption\n\n"
        "/cancel to cancel.",
        InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "❌ Cancel",
                        "CALLBACK",
                        callback="adm:cancel",
                        style="danger",
                    )
                ]
            ]
        ),
    )


async def admin_message_preview(
    update,
    context,
    key,
):
    await send_configured_message(
        context,
        update.effective_user.id,
        key,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "⬅️ Admin",
                        "CALLBACK",
                        callback="adm:dash",
                    )
                ]
            ]
        ),
    )


# ============================================================
# BUTTON MANAGER
# ============================================================

async def admin_buttons(
    update,
    context,
):
    buttons = db_all(
        """
        SELECT *
        FROM buttons
        ORDER BY scope, row, position, id
        """
    )

    if buttons:

        lines = []

        for button in buttons:
            lines.append(
                f"#{button['id']} "
                f"[{button['scope']}] "
                f"{'✅' if button['enabled'] else '❌'} "
                f"{html.escape(button['text'])} "
                f"→ {button['action']} "
                f"| {button['style'] or 'default'} "
                f"| row {button['row']}"
            )

        text = (
            "🎨 BUTTON MANAGER\n\n"
            + "\n".join(lines)
        )

    else:
        text = (
            "🎨 BUTTON MANAGER\n\n"
            "No custom buttons configured."
        )

    rows = [
        [
            create_button(
                "➕ Add Button",
                "CALLBACK",
                callback="adm:add:button",
                style="success",
            )
        ]
    ]

    for button in buttons:

        rows.append(
            [
                create_button(
                    "✏️ "
                    + button["text"][:16],
                    "CALLBACK",
                    callback=(
                        f"adm:editbtn:"
                        f"{button['id']}"
                    ),
                ),
                create_button(
                    "❌"
                    if button["enabled"]
                    else "✅",
                    "CALLBACK",
                    callback=(
                        f"adm:togglebtn:"
                        f"{button['id']}"
                    ),
                ),
                create_button(
                    "🗑",
                    "CALLBACK",
                    callback=(
                        f"adm:delbtn:"
                        f"{button['id']}"
                    ),
                    style="danger",
                ),
            ]
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# MEDIA MANAGER
# ============================================================

async def admin_media(
    update,
    context,
):
    message = get_message(
        "start"
    )

    media = (
        message["media_type"]
        or "None"
    )

    file_id = (
        message["media_file_id"]
        or "—"
    )

    text = (
        "🖼 MEDIA MANAGER\n\n"
        f"Start Media: {media}\n"
        f"File ID: <code>{html.escape(file_id)}</code>"
    )

    rows = [
        [
            create_button(
                "🔄 Replace Start Media",
                "CALLBACK",
                callback="adm:add:start_media",
                style="primary",
            ),
            create_button(
                "🗑 Remove",
                "CALLBACK",
                callback="adm:remove_start_media",
                style="danger",
            ),
        ],
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ],
    ]

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# DATABASE MANAGEMENT
# ============================================================

async def admin_database(
    update,
    context,
):
    text = (
        "💾 DATABASE\n\n"
        "Create complete SQLite backups, "
        "export data, or restore a validated "
        "previous database."
    )

    rows = [
        [
            create_button(
                "💾 Backup",
                "CALLBACK",
                callback="adm:backup",
                style="success",
            ),
            create_button(
                "♻️ Restore",
                "CALLBACK",
                callback="adm:restore",
                style="danger",
            ),
        ],
        [
            create_button(
                "📤 Export Users",
                "CALLBACK",
                callback="adm:export_users",
            )
        ],
        [
            create_button(
                "📤 Export Numbers",
                "CALLBACK",
                callback="adm:export_numbers",
            )
        ],
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ],
    ]

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


async def create_database_backup(
    update,
    context,
):
    user_id = update.effective_user.id

    BACKUP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    backup_path = (
        BACKUP_DIR
        / (
            "bot_backup_"
            + datetime.now().strftime(
                "%Y%m%d_%H%M%S"
            )
            + ".db"
        )
    )

    destination = sqlite3.connect(
        backup_path
    )

    try:
        DB.backup(
            destination
        )
    finally:
        destination.close()

    log_action(
        user_id,
        "database_backup",
    )

    try:
        await context.bot.send_document(
            chat_id=user_id,
            document=InputFile(
                backup_path
            ),
            caption=(
                "💾 Complete SQLite "
                "database backup."
            ),
        )
    finally:
        try:
            backup_path.unlink()
        except OSError:
            pass


async def restore_database_document(
    update,
    context,
):
    global DB

    user_id = update.effective_user.id
    document = update.message.document

    filename = (
        document.file_name
        or ""
    ).lower()

    if not filename.endswith(
        (
            ".db",
            ".sqlite",
            ".sqlite3",
        )
    ):
        await update.message.reply_text(
            "❌ Only SQLite database "
            "files are accepted."
        )
        return

    temporary_path = (
        Path(
            tempfile.gettempdir()
        )
        / (
            f"restore_"
            f"{user_id}_"
            f"{int(time.time())}.db"
        )
    )

    telegram_file = (
        await document.get_file()
    )

    await telegram_file.download_to_drive(
        temporary_path
    )

    try:

        test_db = connect_database(
            temporary_path
        )

        integrity = test_db.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        tables = {
            row[0]
            for row in test_db.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                """
            )
        }

        required_tables = {
            "users",
            "admins",
            "channels",
            "numbers",
            "claims",
            "referrals",
            "settings",
            "messages",
            "buttons",
            "broadcasts",
            "logs",
        }

        if integrity != "ok":
            raise ValueError(
                "SQLite integrity check failed."
            )

        if not required_tables.issubset(
            tables
        ):
            raise ValueError(
                "Database schema is incompatible."
            )

        test_db.close()

        BACKUP_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        pre_restore = (
            BACKUP_DIR
            / (
                "pre_restore_"
                + datetime.now().strftime(
                    "%Y%m%d_%H%M%S"
                )
                + ".db"
            )
        )

        destination = sqlite3.connect(
            pre_restore
        )

        try:
            DB.backup(
                destination
            )
        finally:
            destination.close()

        DB.close()

        shutil.copy2(
            temporary_path,
            DB_PATH,
        )

        initialize_database()

        log_action(
            user_id,
            "database_restore",
            document.file_name,
        )

        await update.message.reply_text(
            "✅ Database restored successfully.\n\n"
            "• SQLite integrity verified\n"
            "• Required tables verified\n"
            "• Pre-restore backup created\n"
            "• Database connection reopened"
        )

    except Exception as exc:

        logger.exception(
            "Database restore failed"
        )

        await update.message.reply_text(
            "❌ Restore rejected.\n\n"
            "The current database was kept untouched "
            "where possible.\n\n"
            f"{str(exc)[:500]}"
        )

    finally:

        context.user_data.clear()

        try:
            temporary_path.unlink()
        except OSError:
            pass


# ============================================================
# EXPORT
# ============================================================

async def export_users(
    update,
    context,
):
    path = (
        Path(
            tempfile.gettempdir()
        )
        / (
            f"users_"
            f"{int(time.time())}.csv"
        )
    )

    try:

        with path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:

            writer = csv.writer(
                file
            )

            writer.writerow(
                [
                    "id",
                    "username",
                    "first_name",
                    "last_name",
                    "join_date",
                    "last_activity",
                    "referrer_id",
                    "referrals",
                    "verified",
                    "blocked",
                    "assigned_number",
                    "claim_date",
                ]
            )

            rows = db_all(
                """
                SELECT
                    id,
                    username,
                    first_name,
                    last_name,
                    join_date,
                    last_activity,
                    referrer_id,
                    referrals,
                    verified,
                    blocked,
                    assigned_number,
                    claim_date
                FROM users
                ORDER BY id
                """
            )

            for row in rows:
                writer.writerow(
                    list(row)
                )

        await context.bot.send_document(
            update.effective_user.id,
            InputFile(path),
            caption="📤 Users export",
        )

    finally:

        try:
            path.unlink()
        except OSError:
            pass


async def export_numbers(
    update,
    context,
):
    path = (
        Path(
            tempfile.gettempdir()
        )
        / (
            f"numbers_"
            f"{int(time.time())}.csv"
        )
    )

    try:

        with path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:

            writer = csv.writer(
                file
            )

            writer.writerow(
                [
                    "id",
                    "number",
                    "active",
                    "assigned_user_id",
                    "assigned_at",
                ]
            )

            rows = db_all(
                """
                SELECT
                    id,
                    number,
                    active,
                    assigned_user_id,
                    assigned_at
                FROM numbers
                ORDER BY id
                """
            )

            for row in rows:
                writer.writerow(
                    list(row)
                )

        await context.bot.send_document(
            update.effective_user.id,
            InputFile(path),
            caption="📤 Numbers export",
        )

    finally:

        try:
            path.unlink()
        except OSError:
            pass


# ============================================================
# SETTINGS
# ============================================================

async def admin_settings(
    update,
    context,
):
    keys = [
        "bot_name",
        "claim_enabled",
        "claim_requires_verification",
        "claim_requires_referrals",
        "minimum_referrals",
        "referral_enabled",
        "referral_reward",
        "number_reuse",
        "support_url",
        "support_text",
        "support_style",
        "whatsapp_button_text",
        "whatsapp_button_style",
    ]

    rows = []

    for key in keys:

        value = get_setting(
            key,
            "",
        )

        display = (
            value
            if len(value) < 35
            else value[:32] + "..."
        )

        rows.append(
            [
                create_button(
                    f"{key}: {display}",
                    "CALLBACK",
                    callback=(
                        f"adm:setting:{key}"
                    ),
                )
            ]
        )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        "⚙️ SETTINGS\n\n"
        "Tap a setting to edit:",
        InlineKeyboardMarkup(
            rows
        ),
    )


async def admin_setting_edit(
    update,
    context,
    key,
):
    context.user_data[
        "state"
    ] = "setting"

    context.user_data[
        "editing_setting"
    ] = key

    await edit_admin_message(
        update,
        f"⚙️ EDIT SETTING\n\n"
        f"Key: {key}\n\n"
        "Send the new value.\n\n"
        "/cancel to cancel.",
    )


# ============================================================
# AUTO-ACCEPT (JOIN REQUEST AUTO-APPROVAL)
# ============================================================

async def admin_autoaccept(
    update,
    context,
):
    enabled = (
        get_setting(
            "auto_accept_enabled",
            "1",
        )
        == "1"
    )

    status = (
        "ON 🟢"
        if enabled
        else "OFF 🔴"
    )

    notify_channel = get_setting(
        "notify_channel_id",
        "",
    ).strip()

    await edit_admin_message(
        update,
        "✅ AUTO-ACCEPT JOIN REQUESTS\n\n"
        f"Status: {status}\n\n"
        "When ON, users who send a join request to any "
        "enabled required channel are approved instantly "
        "so they can verify right away.\n\n"
        "When OFF, join requests stay pending until an "
        "admin approves them manually inside Telegram.\n\n"
        f"📢 New-user alert channel: "
        f"{notify_channel or 'not set'}",
        InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "Toggle Auto-Accept",
                        "CALLBACK",
                        callback="adm:toggle_autoaccept",
                        style="danger" if enabled else "success",
                    )
                ],
                [
                    create_button(
                        "📢 Set Notify Channel",
                        "CALLBACK",
                        callback="adm:setting:notify_channel_id",
                    )
                ],
                [
                    create_button(
                        "⬅️ Admin",
                        "CALLBACK",
                        callback="adm:dash",
                    )
                ],
            ]
        ),
    )


# ============================================================
# MAINTENANCE
# ============================================================

async def admin_maintenance(
    update,
    context,
):
    enabled = (
        get_setting(
            "maintenance_mode",
            "0",
        )
        == "1"
    )

    status = (
        "ON 🔴"
        if enabled
        else "OFF 🟢"
    )

    rows = [
        [
            create_button(
                "Toggle",
                "CALLBACK",
                callback="adm:toggle_maintenance",
                style="danger",
            )
        ],
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ],
    ]

    await edit_admin_message(
        update,
        "🔧 MAINTENANCE MODE\n\n"
        f"Status: {status}\n\n"
        "Admins can continue using the bot.",
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# ADMIN LOGS
# ============================================================

async def admin_logs(
    update,
    context,
    page=0,
):
    rows = db_all(
        """
        SELECT *
        FROM logs
        ORDER BY id DESC
        LIMIT 15
        OFFSET ?
        """,
        (page * 15,),
    )

    lines = []

    for row in rows:
        lines.append(
            f"{row['created_at']} | "
            f"{row['actor_id'] or 'system'} | "
            f"{row['action']} | "
            f"{row['details'] or ''}"
        )

    text = (
        "📋 LOGS\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No logs."
        )
    )

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "⬅️ Admin",
                        "CALLBACK",
                        callback="adm:dash",
                    )
                ]
            ]
        ),
    )


# ============================================================
# ADMIN MANAGEMENT
# ============================================================

async def admin_admins(
    update,
    context,
):
    admins = db_all(
        """
        SELECT *
        FROM admins
        ORDER BY user_id
        """
    )

    lines = []

    for admin in admins:
        lines.append(
            f"• {admin['user_id']} "
            f"— "
            f"{'enabled' if admin['enabled'] else 'disabled'}"
        )

    text = (
        "👑 ADMINS\n\n"
        + (
            "\n".join(lines)
            if lines
            else "No database admins."
        )
    )

    rows = [
        [
            create_button(
                "➕ Add Admin",
                "CALLBACK",
                callback="adm:add:admin",
                style="success",
            )
        ],
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ],
    ]

    await edit_admin_message(
        update,
        text,
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# CLEANUP
# ============================================================

async def admin_cleanup(
    update,
    context,
):
    rows = [
        [
            create_button(
                "🧹 Clean Logs Older Than 180 Days",
                "CALLBACK",
                callback="adm:cleanup_confirm",
                style="danger",
            )
        ],
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ],
    ]

    await edit_admin_message(
        update,
        "🧹 CLEANUP\n\n"
        "Only old logs are removed.\n"
        "Users, claims, referrals and numbers "
        "are never automatically deleted.",
        InlineKeyboardMarkup(
            rows
        ),
    )


# ============================================================
# ADMIN ADD FLOWS
# ============================================================

async def start_channel_wizard(
    update,
    context,
    channel_row_id=None,
):
    """Begin the step-by-step channel add/edit wizard.

    channel_row_id is None for a brand new channel, or the DB row id when
    editing an existing one (pre-fills fields, wizard still walks through
    each step so the color picker stays available).
    """
    context.user_data.clear()

    context.user_data["state"] = "channel_wizard"
    context.user_data["channel_wizard_step"] = "id"
    context.user_data["channel_wizard_editing"] = channel_row_id
    context.user_data["channel_wizard_data"] = {}

    await edit_admin_message(
        update,
        "📣 ADD CHANNEL — Step 1/4\n\n"
        "Send the Channel ID.\n\n"
        "This looks like -1001234567890. Forward any "
        "message from the channel to @userinfobot or "
        "@RawDataBot to find it if you don't have it.\n\n"
        "/cancel to cancel.",
    )


CHANNEL_COLOR_OPTIONS = [
    ("🔵 Primary", "primary"),
    ("🟢 Success", "success"),
    ("🔴 Danger", "danger"),
    ("⚪ Default", ""),
]


def channel_wizard_color_keyboard():
    rows = []
    for label, style in CHANNEL_COLOR_OPTIONS:
        rows.append(
            [
                create_button(
                    label,
                    "CALLBACK",
                    callback=f"adm:chcolor:{style}",
                    style=style or None,
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


async def admin_add_flow(
    update,
    context,
    kind,
):
    if kind == "channel":
        await start_channel_wizard(
            update,
            context,
        )
        return

    context.user_data[
        "state"
    ] = "add"

    context.user_data[
        "add_kind"
    ] = kind

    prompts = {

        "number":
            "📱 Send one WhatsApp number.\n\n"
            "Digits only, preferably including country code.",

        "bulk_number":
            "📥 Send numbers one per line.\n\n"
            "Example:\n"
            "919876543210\n"
            "918765432109\n"
            "917654321098",

        "button":
            "🎨 Add Button\n\n"
            "Send:\n"
            "TEXT|SCOPE|ACTION|URL|CALLBACK|STYLE|ROW|POSITION\n\n"
            "Example:\n"
            "🌐 Website|main|URL|https://example.com||primary|0|0\n\n"
            "Styles:\n"
            "primary\n"
            "success\n"
            "danger\n"
            "blank for default",

        "admin":
            "👑 Send the Telegram numeric user ID.",

        "start_media":
            "🖼 Send a photo or video.\n\n"
            "Its caption becomes the Start message.\n\n"
            "Custom emoji entities and formatting are preserved.",
    }

    await edit_admin_message(
        update,
        prompts.get(
            kind,
            "Send the requested information.",
        ),
    )


# ============================================================
# ADMIN CRUD
# ============================================================

async def admin_delete_user(
    update,
    context,
    user_id,
):
    if user_id in ADMIN_IDS:
        return

    with transaction():
        DB.execute(
            """
            DELETE FROM users
            WHERE id=?
            """,
            (user_id,),
        )

    log_action(
        update.effective_user.id,
        "delete_user",
        str(user_id),
    )

    await admin_users(
        update,
        context,
        0,
    )


async def admin_block_user(
    update,
    context,
    user_id,
    block,
):
    DB.execute(
        """
        UPDATE users
        SET blocked=?
        WHERE id=?
        """,
        (
            1 if block else 0,
            user_id,
        ),
    )

    DB.commit()

    log_action(
        update.effective_user.id,
        "block_user"
        if block
        else "unblock_user",
        str(user_id),
    )

    await admin_user_detail(
        update,
        context,
        user_id,
    )


async def admin_reset_claim(
    update,
    context,
    user_id,
):
    with transaction():

        number = DB.execute(
            """
            SELECT id
            FROM numbers
            WHERE assigned_user_id=?
            """,
            (user_id,),
        ).fetchone()

        if number:

            DB.execute(
                """
                UPDATE numbers
                SET assigned_user_id=NULL,
                    assigned_at=NULL
                WHERE id=?
                """,
                (number["id"],),
            )

        DB.execute(
            """
            DELETE FROM claims
            WHERE user_id=?
            """,
            (user_id,),
        )

        DB.execute(
            """
            UPDATE users
            SET assigned_number=NULL,
                claim_date=NULL
            WHERE id=?
            """,
            (user_id,),
        )

    log_action(
        update.effective_user.id,
        "reset_claim",
        str(user_id),
    )

    await admin_user_detail(
        update,
        context,
        user_id,
    )


async def admin_toggle_button(
    update,
    context,
    button_id,
):
    DB.execute(
        """
        UPDATE buttons
        SET enabled=1-enabled
        WHERE id=?
        """,
        (button_id,),
    )

    DB.commit()

    await admin_buttons(
        update,
        context,
    )


async def admin_delete_button(
    update,
    context,
    button_id,
):
    DB.execute(
        """
        DELETE FROM buttons
        WHERE id=?
        """,
        (button_id,),
    )

    DB.commit()

    await admin_buttons(
        update,
        context,
    )


async def admin_delete_channel(
    update,
    context,
    channel_id,
):
    DB.execute(
        """
        DELETE FROM channels
        WHERE id=?
        """,
        (channel_id,),
    )

    DB.commit()

    await admin_channels(
        update,
        context,
        0,
    )


async def admin_toggle_channel(
    update,
    context,
    channel_id,
):
    DB.execute(
        """
        UPDATE channels
        SET enabled=1-enabled
        WHERE id=?
        """,
        (channel_id,),
    )

    DB.commit()

    await admin_channels(
        update,
        context,
        0,
    )


# ============================================================
# UNIFIED BUTTON COLOR EDITOR
# ============================================================

COLOR_CYCLE = ["primary", "success", "danger", ""]


def _next_color(current):
    current = current or ""
    try:
        idx = COLOR_CYCLE.index(current)
    except ValueError:
        idx = -1
    return COLOR_CYCLE[(idx + 1) % len(COLOR_CYCLE)]


def _color_label(style):
    return {
        "primary": "🔵 Primary",
        "success": "🟢 Success",
        "danger": "🔴 Danger",
        "": "⚪ Default",
    }.get(style or "", "⚪ Default")


async def admin_colors(
    update,
    context,
):
    channels = db_all(
        "SELECT id, title, join_text, style FROM channels ORDER BY position, id"
    )
    buttons = db_all(
        "SELECT id, text, scope, style FROM buttons ORDER BY scope, row, position, id"
    )

    rows = []

    if channels:
        rows.append(
            [
                create_button(
                    "— CHANNEL JOIN BUTTONS —",
                    "CALLBACK",
                    callback="adm:noop",
                )
            ]
        )
        for channel in channels:
            rows.append(
                [
                    create_button(
                        f"{channel['title'][:18]} ({channel['join_text']})",
                        "CALLBACK",
                        callback="adm:noop",
                    ),
                    create_button(
                        _color_label(channel["style"]),
                        "CALLBACK",
                        callback=f"adm:cyclechcolor:{channel['id']}",
                        style=channel["style"] or None,
                    ),
                ]
            )

    if buttons:
        rows.append(
            [
                create_button(
                    "— CUSTOM BUTTONS —",
                    "CALLBACK",
                    callback="adm:noop",
                )
            ]
        )
        for button in buttons:
            rows.append(
                [
                    create_button(
                        f"[{button['scope']}] {button['text'][:16]}",
                        "CALLBACK",
                        callback="adm:noop",
                    ),
                    create_button(
                        _color_label(button["style"]),
                        "CALLBACK",
                        callback=f"adm:cyclebtncolor:{button['id']}",
                        style=button["style"] or None,
                    ),
                ]
            )

    rows.append(
        [
            create_button(
                "⬅️ Admin",
                "CALLBACK",
                callback="adm:dash",
            )
        ]
    )

    await edit_admin_message(
        update,
        "🎨 BUTTON COLORS\n\n"
        "Tap a color chip to cycle it: "
        "Primary → Success → Danger → Default.\n\n"
        + (
            "No channels or custom buttons configured yet."
            if not channels and not buttons
            else ""
        ),
        InlineKeyboardMarkup(rows),
    )


async def admin_cycle_channel_color(update, context, channel_id):
    channel = db_one("SELECT style FROM channels WHERE id=?", (channel_id,))
    if not channel:
        await admin_colors(update, context)
        return
    new_style = _next_color(channel["style"])
    DB.execute(
        "UPDATE channels SET style=? WHERE id=?",
        (new_style or None, channel_id),
    )
    DB.commit()
    await admin_colors(update, context)


async def admin_cycle_button_color(update, context, button_id):
    button = db_one("SELECT style FROM buttons WHERE id=?", (button_id,))
    if not button:
        await admin_colors(update, context)
        return
    new_style = _next_color(button["style"])
    DB.execute(
        "UPDATE buttons SET style=? WHERE id=?",
        (new_style or None, button_id),
    )
    DB.commit()
    await admin_colors(update, context)


async def admin_delete_number(
    update,
    context,
    number_id,
):
    number = db_one(
        """
        SELECT assigned_user_id
        FROM numbers
        WHERE id=?
        """,
        (number_id,),
    )

    if (
        number
        and number["assigned_user_id"]
        is not None
    ):
        await edit_admin_message(
            update,
            "❌ This number is assigned.\n\n"
            "Reset its claim before deleting it.",
            InlineKeyboardMarkup(
                [
                    [
                        create_button(
                            "⬅️ Numbers",
                            "CALLBACK",
                            callback="adm:numbers:0",
                        )
                    ]
                ]
            ),
        )
        return

    DB.execute(
        """
        DELETE FROM numbers
        WHERE id=?
        """,
        (number_id,),
    )

    DB.commit()

    await admin_numbers(
        update,
        context,
        0,
    )


async def admin_toggle_number(
    update,
    context,
    number_id,
):
    DB.execute(
        """
        UPDATE numbers
        SET active=1-active
        WHERE id=?
        """,
        (number_id,),
    )

    DB.commit()

    await admin_numbers(
        update,
        context,
        0,
    )


# ============================================================
# BROADCAST
# ============================================================

async def admin_broadcast_start(
    update,
    context,
):
    context.user_data[
        "state"
    ] = "broadcast"

    context.user_data[
        "broadcast"
    ] = None

    await edit_admin_message(
        update,
        "📢 BROADCAST\n\n"
        "Send the message you want to broadcast.\n\n"
        "Supported:\n"
        "• formatted text\n"
        "• Premium/custom emoji entities\n"
        "• photo + caption\n"
        "• video + caption\n"
        "• configured broadcast buttons\n\n"
        "/cancel to cancel.",
    )


async def handle_broadcast_message(
    update,
    context,
):
    if not is_admin(
        update.effective_user.id
    ):
        return False

    if (
        context.user_data.get(
            "state"
        )
        != "broadcast"
    ):
        return False

    message = update.message

    text = (
        message.caption
        if (
            message.photo
            or message.video
        )
        else message.text
    )

    if text is None:
        await message.reply_text(
            "Send text, photo or video."
        )
        return True

    entities = (
        message.caption_entities
        if (
            message.photo
            or message.video
        )
        else message.entities
    )

    context.user_data[
        "broadcast"
    ] = {
        "text": text,
        "entities": [
            entity.to_dict()
            for entity in (
                entities or []
            )
        ],
        "media_type":
            (
                "photo"
                if message.photo
                else (
                    "video"
                    if message.video
                    else None
                )
            ),
        "file_id":
            (
                message.photo[-1].file_id
                if message.photo
                else (
                    message.video.file_id
                    if message.video
                    else None
                )
            ),
        "buttons": [],
    }

    context.user_data["state"] = "broadcast_button_prompt"

    await message.reply_text(
        "🔗 Add an inline button under this broadcast?\n\n"
        "This is separate from the permanent buttons configured "
        "in Buttons → scope 'broadcast' — it's a one-off link "
        "just for this message (e.g. a promo or channel link).",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    create_button(
                        "➕ Add Button",
                        "CALLBACK",
                        callback="adm:bcbtn_add",
                        style="success",
                    ),
                    create_button(
                        "⏭ Skip",
                        "CALLBACK",
                        callback="adm:bcbtn_skip",
                    ),
                ]
            ]
        ),
    )

    return True


async def send_broadcast_preview(update_or_message, context):
    """Render the broadcast preview screen with confirm/cancel controls.

    Called once the admin has either added their one-off buttons or
    skipped that step. Accepts either an Update (from a callback) or a
    Message (from the original text/media capture) so both entry points
    can share the same preview renderer.
    """
    data = context.user_data.get("broadcast") or {}
    text = data.get("text", "")

    total = db_one(
        """
        SELECT COUNT(*) AS c
        FROM users
        WHERE blocked=0
        """
    )["c"]

    one_off_rows = []
    for button in data.get("buttons", []):
        one_off_rows.append(
            [
                create_button(
                    button["text"],
                    "URL",
                    url=button["url"],
                    style="primary",
                )
            ]
        )

    scope_rows = keyboard_from_scope("broadcast")

    control_row = [
        create_button(
            "✅ CONFIRM BROADCAST",
            "CALLBACK",
            callback="adm:confirm_broadcast:0",
            style="success",
        ),
        create_button(
            "❌ CANCEL",
            "CALLBACK",
            callback="adm:cancel_broadcast",
            style="danger",
        ),
    ]

    preview_markup = InlineKeyboardMarkup(
        one_off_rows + scope_rows + [control_row]
    )

    reply_target = (
        update_or_message.message
        if hasattr(update_or_message, "message")
        and update_or_message.message
        else update_or_message
    )

    try:
        await reply_target.reply_text(
            "👁 BROADCAST PREVIEW\n\n"
            f"{text[:3500]}\n\n"
            f"👥 Recipients: {total}",
            reply_markup=preview_markup,
        )
    except TelegramError:
        await reply_target.reply_text(
            "👁 Broadcast preview prepared.\n\n"
            f"👥 Recipients: {total}",
            reply_markup=preview_markup,
        )


async def broadcast_confirm(
    update,
    context,
):
    data = context.user_data.get(
        "broadcast"
    )

    if not data:
        await edit_admin_message(
            update,
            "❌ Broadcast session expired.",
            admin_keyboard(),
        )
        return

    admin_id = (
        update.effective_user.id
    )

    users = db_all(
        """
        SELECT id
        FROM users
        WHERE blocked=0
        ORDER BY id
        """
    )

    cursor = DB.execute(
        """
        INSERT INTO broadcasts
        (
            admin_id,
            created_at,
            media_type,
            text,
            entities_json,
            buttons_json,
            total,
            status
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            admin_id,
            utc_now(),
            data["media_type"],
            data["text"],
            json.dumps(
                data["entities"],
                ensure_ascii=False,
            ),
            json.dumps(
                data.get("buttons", []),
                ensure_ascii=False,
            ),
            len(users),
            "running",
        ),
    )

    broadcast_id = cursor.lastrowid

    DB.commit()

    await edit_admin_message(
        update,
        (
            f"📢 Broadcasting to "
            f"{len(users)} users..."
        ),
    )

    sent = 0
    failed = 0
    blocked = 0

    entities = [
        MessageEntity.de_json(
            entity,
            context.bot,
        )
        for entity in data["entities"]
    ]

    one_off_rows = [
        [
            create_button(
                button["text"],
                "URL",
                url=button["url"],
                style="primary",
            )
        ]
        for button in data.get("buttons", [])
    ]

    broadcast_rows = keyboard_from_scope(
        "broadcast"
    )

    combined_rows = one_off_rows + broadcast_rows

    broadcast_markup = (
        InlineKeyboardMarkup(
            combined_rows
        )
        if combined_rows
        else None
    )

    for user in users:

        recipient_id = user["id"]

        try:

            if data["media_type"] == "photo":

                await context.bot.send_photo(
                    chat_id=recipient_id,
                    photo=data["file_id"],
                    caption=data["text"],
                    caption_entities=entities,
                    reply_markup=broadcast_markup,
                )

            elif data["media_type"] == "video":

                await context.bot.send_video(
                    chat_id=recipient_id,
                    video=data["file_id"],
                    caption=data["text"],
                    caption_entities=entities,
                    reply_markup=broadcast_markup,
                )

            else:

                await context.bot.send_message(
                    chat_id=recipient_id,
                    text=data["text"],
                    entities=entities,
                    reply_markup=broadcast_markup,
                )

            sent += 1

            await asyncio.sleep(
                0.04
            )

        except RetryAfter as exc:

            await asyncio.sleep(
                float(
                    exc.retry_after
                )
                + 0.5
            )

            try:

                if data["media_type"] == "photo":

                    await context.bot.send_photo(
                        chat_id=recipient_id,
                        photo=data["file_id"],
                        caption=data["text"],
                        caption_entities=entities,
                        reply_markup=broadcast_markup,
                    )

                elif data["media_type"] == "video":

                    await context.bot.send_video(
                        chat_id=recipient_id,
                        video=data["file_id"],
                        caption=data["text"],
                        caption_entities=entities,
                        reply_markup=broadcast_markup,
                    )

                else:

                    await context.bot.send_message(
                        chat_id=recipient_id,
                        text=data["text"],
                        entities=entities,
                        reply_markup=broadcast_markup,
                    )

                sent += 1

            except Forbidden:

                blocked += 1

                DB.execute(
                    """
                    UPDATE users
                    SET blocked=1
                    WHERE id=?
                    """,
                    (recipient_id,),
                )

            except TelegramError:
                failed += 1

        except Forbidden:

            blocked += 1

            DB.execute(
                """
                UPDATE users
                SET blocked=1
                WHERE id=?
                """,
                (recipient_id,),
            )

        except (
            NetworkError,
            TimedOut,
        ):
            failed += 1

        except TelegramError:
            failed += 1

    DB.execute(
        """
        UPDATE broadcasts
        SET sent=?,
            failed=?,
            blocked=?,
            status='completed'
        WHERE id=?
        """,
        (
            sent,
            failed,
            blocked,
            broadcast_id,
        ),
    )

    DB.commit()

    log_action(
        admin_id,
        "broadcast",
        (
            f"total={len(users)},"
            f"sent={sent},"
            f"failed={failed},"
            f"blocked={blocked}"
        ),
    )

    context.user_data.clear()

    await context.bot.send_message(
        chat_id=admin_id,
        text=(
            "📢 BROADCAST COMPLETED\n\n"
            f"👥 Total: {len(users)}\n"
            f"✅ Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"🚫 Blocked: {blocked}"
        ),
        reply_markup=admin_keyboard(),
    )


# ============================================================
# ADMIN MESSAGE HANDLER
# ============================================================

async def handle_admin_message(
    update,
    context,
):
    user_id = (
        update.effective_user.id
    )

    if not is_admin(user_id):
        return False

    message = update.message

    if (
        message.text
        and message.text == "/cancel"
    ):
        context.user_data.clear()

        await message.reply_text(
            "❌ Cancelled.",
            reply_markup=admin_keyboard(),
        )

        return True

    if (
        context.user_data.get(
            "awaiting_restore"
        )
        and message.document
    ):
        context.user_data[
            "awaiting_restore"
        ] = False

        await restore_database_document(
            update,
            context,
        )

        return True

    state = context.user_data.get(
        "state"
    )

    # --------------------------------------------------------
    # MESSAGE EDIT
    # --------------------------------------------------------

    if state == "message":

        key = context.user_data.get(
            "editing_message"
        )

        has_media = bool(
            message.photo
            or message.video
        )

        text = (
            message.caption
            if has_media
            else message.text
        )

        if text is None and not has_media:
            await message.reply_text(
                "❌ Send text, photo or video."
            )
            return True

        if text is None and has_media:
            # Photo/video sent with no caption at all — perfectly valid,
            # this is exactly how an admin adds a bare image with no text.
            # Keep whatever text was previously saved for this key instead
            # of wiping it out or rejecting the upload.
            existing = get_message(key)
            text = existing["text"]

        media_type = None
        media_file_id = None

        if message.photo:
            media_type = "photo"
            media_file_id = (
                message.photo[-1].file_id
            )

        elif message.video:
            media_type = "video"
            media_file_id = (
                message.video.file_id
            )

        entities = (
            message.caption_entities
            if (
                message.photo
                or message.video
            )
            else message.entities
        )

        if has_media and message.caption is None:
            # No fresh caption entities to apply since there's no caption;
            # keep whatever formatting was already stored for this key.
            existing = get_message(key)
            entities = deserialize_entities(
                existing["entities"],
                context.bot,
            )

        save_message(
            key,
            text,
            entities,
            media_type,
            media_file_id,
        )

        DB.commit()

        log_action(
            user_id,
            "edit_message",
            key,
        )

        context.user_data.clear()

        await message.reply_text(
            "✅ Message updated successfully.",
            reply_markup=admin_keyboard(),
        )

        return True

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if state == "setting":

        key = context.user_data.get(
            "editing_setting"
        )

        set_setting(
            key,
            message.text or "",
        )

        DB.commit()

        log_action(
            user_id,
            "edit_setting",
            key,
        )

        context.user_data.clear()

        await message.reply_text(
            "✅ Setting updated.",
            reply_markup=admin_keyboard(),
        )

        return True

    # --------------------------------------------------------
    # ADD FLOWS
    # --------------------------------------------------------

    if state == "add":

        kind = context.user_data.get(
            "add_kind"
        )

        text = (
            message.text
            or ""
        )

        # Number
        if kind == "number":

            number = re.sub(
                r"\s+",
                "",
                text,
            )

            if not re.fullmatch(
                r"\d{7,15}",
                number,
            ):
                await message.reply_text(
                    "❌ Invalid number.\n\n"
                    "Use digits only with country code."
                )
                return True

            try:

                DB.execute(
                    """
                    INSERT INTO numbers(number)
                    VALUES(?)
                    """,
                    (number,),
                )

                DB.commit()

                await message.reply_text(
                    "✅ Number added."
                )

            except sqlite3.IntegrityError:

                await message.reply_text(
                    "ℹ️ Duplicate number ignored."
                )

        # Bulk numbers
        elif kind == "bulk_number":

            added = 0
            duplicate = 0
            invalid = 0

            with transaction():

                for line in text.splitlines():

                    number = re.sub(
                        r"\s+",
                        "",
                        line,
                    )

                    if not re.fullmatch(
                        r"\d{7,15}",
                        number,
                    ):
                        invalid += 1
                        continue

                    try:

                        DB.execute(
                            """
                            INSERT INTO numbers(number)
                            VALUES(?)
                            """,
                            (number,),
                        )

                        added += 1

                    except sqlite3.IntegrityError:
                        duplicate += 1

            await message.reply_text(
                "📥 Bulk import completed.\n\n"
                f"✅ Added: {added}\n"
                f"♻️ Duplicates: {duplicate}\n"
                f"❌ Invalid: {invalid}"
            )

        # Channel
        # Button
        elif kind == "button":
            parts = [x.strip() for x in text.split("|")]
            if len(parts) < 8:
                await message.reply_text("❌ Invalid button format.\n\nTEXT|SCOPE|ACTION|URL|CALLBACK|STYLE|ROW|POSITION")
                return True
            button_text, scope, action = parts[0], parts[1], parts[2].upper()
            url, callback, style = parts[3] or None, parts[4] or None, parts[5].lower() or None
            row, position = safe_int(parts[6], -1), safe_int(parts[7], -1)
            if not button_text or not scope or action not in VALID_BUTTON_ACTIONS:
                await message.reply_text("❌ Invalid button TEXT, SCOPE or ACTION.")
                return True
            if style and style not in VALID_STYLES:
                await message.reply_text("❌ Invalid style. Allowed: primary, success, danger")
                return True
            if row < 0 or position < 0:
                await message.reply_text("❌ ROW and POSITION must be non-negative integers.")
                return True
            if action == "URL":
                if not url or not re.match(r"^https?://[^\s]+$", url, re.I):
                    await message.reply_text("❌ URL button requires a valid http(s) URL.")
                    return True
                callback = None
            else:
                if not callback or not (1 <= len(callback.encode("utf-8")) <= 64):
                    await message.reply_text("❌ Callback button requires callback data of 1-64 UTF-8 bytes.")
                    return True
                url = None
                if db_one("SELECT id FROM buttons WHERE callback=?", (callback,)):
                    await message.reply_text("❌ Callback identifier already exists.")
                    return True
            with transaction():
                DB.execute("INSERT INTO buttons(scope, text, action, url, callback, style, row, position) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", (scope, button_text, action, url, callback, style, row, position))
            await message.reply_text("✅ Button added.", reply_markup=admin_keyboard())

        # Admin
        elif kind == "admin":

            admin_id = safe_int(
                text,
                None,
            )

            if admin_id is None:
                await message.reply_text(
                    "❌ Invalid Telegram ID."
                )
                return True

            DB.execute(
                """
                INSERT OR REPLACE INTO admins
                (
                    user_id,
                    enabled,
                    added_at
                )
                VALUES(?, 1, ?)
                """,
                (
                    admin_id,
                    utc_now(),
                ),
            )

            DB.commit()

            await message.reply_text(
                "✅ Admin added."
            )

        # Start media
        elif kind == "start_media":

            if message.photo:

                current_start = get_message("start")
                caption = message.caption or current_start["text"]
                caption_entities = message.caption_entities or (deserialize_entities(current_start["entities"], context.bot) if not message.caption else [])

                save_message(
                    "start",
                    caption,
                    caption_entities,
                    "photo",
                    message.photo[-1].file_id,
                )

                DB.commit()

                await message.reply_text(
                    "✅ Start image saved."
                )

            elif message.video:

                current_start = get_message("start")
                caption = message.caption or current_start["text"]
                caption_entities = message.caption_entities or (deserialize_entities(current_start["entities"], context.bot) if not message.caption else [])

                save_message(
                    "start",
                    caption,
                    caption_entities,
                    "video",
                    message.video.file_id,
                )

                DB.commit()

                await message.reply_text(
                    "✅ Start video saved."
                )

            else:
                await message.reply_text(
                    "❌ Send a photo or video."
                )
                return True

        context.user_data.clear()

        return True

    # --------------------------------------------------------
    # SEARCH
    # --------------------------------------------------------

    if state == "search":

        search_type = context.user_data.get(
            "admin_search_type"
        )

        query = (
            message.text
            or ""
        ).strip()

        context.user_data.clear()

        if search_type == "users":

            rows = db_all(
                """
                SELECT *
                FROM users
                WHERE CAST(id AS TEXT)=?
                   OR username LIKE ?
                   OR first_name LIKE ?
                   OR last_name LIKE ?
                LIMIT 20
                """,
                (
                    query,
                    query.lstrip("@") + "%",
                    query + "%",
                    query + "%",
                ),
            )

            if rows:

                output = "\n".join(
                    (
                        f"{row['id']} "
                        f"@{row['username'] or '—'} "
                        f"{row['first_name'] or ''} "
                        f"→ "
                        f"{row['assigned_number'] or '—'}"
                    )
                    for row in rows
                )

            else:
                output = "No matches."

        else:

            rows = db_all(
                """
                SELECT *
                FROM numbers
                WHERE number LIKE ?
                LIMIT 20
                """,
                (
                    query + "%",
                ),
            )

            if rows:

                output = "\n".join(
                    (
                        f"{row['id']} "
                        f"{row['number']} "
                        f"→ "
                        f"{row['assigned_user_id'] or 'available'}"
                    )
                    for row in rows
                )

            else:
                output = "No matches."

        await message.reply_text(
            "🔎 SEARCH RESULTS\n\n"
            + output,
            reply_markup=admin_keyboard(),
        )

        return True

    return False


# ============================================================
# ADMIN BUTTON EDIT / CHANNEL EDIT
# ============================================================

async def admin_edit_button_flow(
    update,
    context,
    button_id,
):
    button = db_one(
        """
        SELECT *
        FROM buttons
        WHERE id=?
        """,
        (button_id,),
    )

    if not button:
        return

    context.user_data[
        "state"
    ] = "edit_button"

    context.user_data[
        "editing_button"
    ] = button_id

    await edit_admin_message(
        update,
        "✏️ EDIT BUTTON\n\n"
        "Send:\n"
        "TEXT|SCOPE|ACTION|URL|CALLBACK|STYLE|ROW|POSITION\n\n"
        f"Current:\n"
        f"{button['text']}|"
        f"{button['scope']}|"
        f"{button['action']}|"
        f"{button['url'] or ''}|"
        f"{button['callback'] or ''}|"
        f"{button['style'] or ''}|"
        f"{button['row']}|"
        f"{button['position']}\n\n"
        "/cancel to cancel.",
    )


async def admin_edit_channel_flow(
    update,
    context,
    channel_id,
):
    channel = db_one(
        """
        SELECT *
        FROM channels
        WHERE id=?
        """,
        (channel_id,),
    )

    if not channel:
        return

    context.user_data.clear()

    context.user_data["state"] = "channel_wizard"
    context.user_data["channel_wizard_step"] = "id"
    context.user_data["channel_wizard_editing"] = channel_id
    context.user_data["channel_wizard_data"] = {
        "channel_id": channel["channel_id"],
        "username": channel["username"],
        "invite_link": channel["invite_link"],
        "join_text": channel["join_text"],
        "title": channel["title"],
    }

    await edit_admin_message(
        update,
        "✏️ EDIT CHANNEL — Step 1/4\n\n"
        f"Current Channel ID: {channel['channel_id']}\n\n"
        "Send a new Channel ID, or /skip to keep the current one.\n\n"
        "/cancel to cancel.",
    )


# ============================================================
# ADMIN TEXT EDITOR EXTENSION
# ============================================================

async def handle_extended_admin_state(
    update,
    context,
):
    if not is_admin(
        update.effective_user.id
    ):
        return False

    state = context.user_data.get(
        "state"
    )

    message = update.message

    if state == "broadcast_button_text":
        text_in = (message.text or "").strip()
        if not text_in:
            await message.reply_text("❌ Button text cannot be empty. Send it again, or /cancel.")
            return True
        if len(text_in) > 64:
            await message.reply_text("❌ Button text is too long (max 64 characters). Send it again.")
            return True

        context.user_data["broadcast_pending_button_text"] = text_in
        context.user_data["state"] = "broadcast_button_url"

        await message.reply_text(
            "🔗 Now send the URL for this button.\n\n"
            "Must start with http:// or https://\n\n"
            "/cancel to cancel."
        )
        return True

    if state == "broadcast_button_url":
        url_in = (message.text or "").strip()
        if not re.match(r"^https?://[^\s]+$", url_in, re.I):
            await message.reply_text("❌ Invalid URL. It must start with http:// or https://. Send it again.")
            return True

        button_text = context.user_data.pop("broadcast_pending_button_text", None)
        if not button_text:
            await message.reply_text("❌ Session expired. /cancel and start the broadcast again.")
            return True

        broadcast_data = context.user_data.setdefault("broadcast", {"buttons": []})
        broadcast_data.setdefault("buttons", []).append(
            {"text": button_text, "url": url_in}
        )

        context.user_data["state"] = "broadcast_button_prompt"

        await message.reply_text(
            f"✅ Button added: {button_text} → {url_in}\n\n"
            "Add another button, or continue to preview?",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        create_button(
                            "➕ Add Another",
                            "CALLBACK",
                            callback="adm:bcbtn_add",
                            style="success",
                        ),
                        create_button(
                            "▶️ Continue",
                            "CALLBACK",
                            callback="adm:bcbtn_skip",
                        ),
                    ]
                ]
            ),
        )
        return True

    if state == "channel_wizard":
        step = context.user_data.get("channel_wizard_step")
        wizard = context.user_data.setdefault("channel_wizard_data", {})
        is_editing = bool(context.user_data.get("channel_wizard_editing"))
        heading = "EDIT CHANNEL" if is_editing else "ADD CHANNEL"
        text_in = (message.text or "").strip()
        is_skip = text_in.lower() == "/skip"

        if step == "id":
            if is_skip and is_editing and wizard.get("channel_id"):
                pass  # keep existing value already pre-filled in wizard
            elif not re.fullmatch(r"-100\d{5,15}|-\d{4,20}", text_in):
                await message.reply_text(
                    "❌ Invalid Channel ID. It should look like "
                    "-1001234567890.\n\nSend it again"
                    + (", /skip to keep current, " if is_editing else " ")
                    + "or /cancel."
                )
                return True
            else:
                wizard["channel_id"] = text_in

            context.user_data["channel_wizard_step"] = "link"

            current_link = wizard.get("invite_link") or wizard.get("username") or ""
            await message.reply_text(
                f"📣 {heading} — Step 2/4\n\n"
                "Send the invite link or @username of the channel.\n\n"
                "Accepted:\n"
                "@channelusername\n"
                "https://t.me/channelusername\n"
                "https://t.me/+xxxxxxxxxxxx (private invite link)\n\n"
                "If the channel has no public username, send the "
                "private invite link instead — one is required."
                + (f"\n\nCurrent: {current_link}\n/skip to keep it." if is_editing and current_link else "")
                + "\n\n/cancel to cancel."
            )
            return True

        if step == "link":
            if is_skip and is_editing and (wizard.get("username") or wizard.get("invite_link")):
                pass  # keep existing username/invite_link already pre-filled
            else:
                username = normalize_username(text_in)
                invite_link = (
                    text_in
                    if re.match(r"^https?://t\.me/(?:\+|joinchat/)", text_in, re.I)
                    else None
                )

                if not username and not invite_link:
                    await message.reply_text(
                        "❌ That doesn't look like a valid @username or "
                        "invite link. Send it again"
                        + (", /skip to keep current, " if is_editing else " ")
                        + "or /cancel."
                    )
                    return True

                wizard["username"] = username
                wizard["invite_link"] = invite_link

            context.user_data["channel_wizard_step"] = "title"

            current_title = wizard.get("join_text") or ""
            await message.reply_text(
                f"📣 {heading} — Step 3/4\n\n"
                "What should the join button say?\n\n"
                "Example: JOIN"
                + (f"\n\nCurrent: {current_title}\n/skip to keep it." if is_editing and current_title else "")
                + "\n\n/cancel to cancel."
            )
            return True

        if step == "title":
            if is_skip and is_editing and wizard.get("join_text"):
                pass  # keep existing title/button text already pre-filled
            elif not text_in:
                await message.reply_text("❌ Title/button text cannot be empty. Send it again.")
                return True
            else:
                wizard["join_text"] = text_in
                # "title" here is the internal channel label shown in the
                # admin panel list; kept in sync with the button text.
                wizard["title"] = text_in

            context.user_data["channel_wizard_step"] = "color"

            await message.reply_text(
                f"📣 {heading} — Step 4/4\n\n"
                "Pick a color for the join button:",
                reply_markup=channel_wizard_color_keyboard(),
            )
            return True

        # step == "color" is handled entirely via callback buttons
        # (adm:chcolor:<style>), not free text.
        await message.reply_text(
            "Please tap one of the color buttons above, or /cancel."
        )
        return True

    if state == "edit_button":
        button_id = context.user_data.get("editing_button")
        parts = (message.text or "").split("|")
        if len(parts) < 8:
            await message.reply_text("❌ Invalid button format. Use TEXT|SCOPE|ACTION|URL|CALLBACK|STYLE|ROW|POSITION")
            return True
        text_value, scope, action = parts[0].strip(), parts[1].strip(), parts[2].strip().upper()
        url, callback, style = parts[3].strip() or None, parts[4].strip() or None, parts[5].strip().lower() or None
        row, position = safe_int(parts[6], -1), safe_int(parts[7], -1)
        if not button_id or not text_value or not scope or action not in VALID_BUTTON_ACTIONS:
            await message.reply_text("❌ Invalid TEXT, SCOPE or ACTION.")
            return True
        if style and style not in VALID_STYLES:
            await message.reply_text("❌ Invalid style. Allowed: primary, success, danger")
            return True
        if row < 0 or position < 0:
            await message.reply_text("❌ ROW and POSITION must be non-negative integers.")
            return True
        if action == "URL":
            if not url or not re.match(r"^https?://[^\s]+$", url, re.I):
                await message.reply_text("❌ URL button requires a valid http(s) URL.")
                return True
            callback = None
        else:
            if not callback or not (1 <= len(callback.encode("utf-8")) <= 64):
                await message.reply_text("❌ Callback button requires callback data of 1-64 UTF-8 bytes.")
                return True
            url = None
            duplicate = db_one("SELECT id FROM buttons WHERE callback=? AND id != ?", (callback, button_id))
            if duplicate:
                await message.reply_text("❌ Callback identifier already exists.")
                return True
        DB.execute("UPDATE buttons SET text=?, scope=?, action=?, url=?, callback=?, style=?, row=?, position=? WHERE id=?", (text_value, scope, action, url, callback, style, row, position, button_id))
        DB.commit()
        context.user_data.clear()
        await message.reply_text("✅ Button updated.", reply_markup=admin_keyboard())
        return True

    return False


# ============================================================
# ADMIN EDIT MESSAGE
# ============================================================

async def edit_admin_message(
    update,
    text,
    reply_markup=None,
):
    try:
        await update.callback_query.message.edit_text(
            text=text,
            reply_markup=reply_markup,
        )

    except TelegramError:

        try:
            await update.callback_query.message.reply_text(
                text=text,
                reply_markup=reply_markup,
            )
        except TelegramError:
            pass


# ============================================================
# CUSTOM CALLBACK BUTTONS
# ============================================================

async def custom_callback_router(
    update,
    context,
):
    query = update.callback_query
    callback_data = (
        query.data or ""
    )

    await answer_callback(
        query
    )

    if callback_data == "noop":
        return
    if callback_data.startswith("channel_config_error:"):
        await answer_callback(query, "This required channel is misconfigured. Please contact the bot administrator.", True)
        return

    button = db_one(
        """
        SELECT *
        FROM buttons
        WHERE enabled=1
        AND callback=?
        """,
        (callback_data,),
    )

    if not button:
        return

    action = (
        button["action"]
        or ""
    ).upper()

    if action == "SUPPORT":

        support_url = get_setting(
            "support_url",
            "",
        )

        if support_url:

            try:
                await query.message.reply_text(
                    get_setting(
                        "support_text",
                        "💬 Support",
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                create_button(
                                    get_setting(
                                        "support_text",
                                        "💬 Support",
                                    ),
                                    "URL",
                                    url=support_url,
                                    style=get_setting(
                                        "support_style",
                                        "primary",
                                    ),
                                )
                            ]
                        ]
                    ),
                )
            except TelegramError:
                pass


# ============================================================
# GENERAL USER MESSAGE
# ============================================================

async def general_message(
    update,
    context,
):
    if not update.effective_user:
        return

    if is_admin(
        update.effective_user.id
    ):

        if await handle_extended_admin_state(
            update,
            context,
        ):
            return

        if await handle_admin_message(
            update,
            context,
        ):
            return

        if await handle_broadcast_message(
            update,
            context,
        ):
            return

    if (
        update.effective_chat
        and update.effective_chat.type
        != "private"
    ):
        return

    user_id = (
        update.effective_user.id
    )

    is_new_user = register_user(
        update.effective_user
    )

    if is_new_user:
        await notify_admin_new_user(
            context,
            update.effective_user,
        )

    user = get_user(
        user_id
    )

    if (
        user
        and user["blocked"]
        and not is_admin(user_id)
    ):
        return

    if maintenance_enabled_for(
        user_id
    ):
        await send_configured_message(
            context,
            user_id,
            "maintenance",
        )
        return

    if update.message.text:

        await send_configured_message(
            context,
            user_id,
            "main",
            reply_markup=main_keyboard(),
        )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update,
    context,
):
    error = context.error

    if isinstance(
        error,
        RetryAfter,
    ):
        logger.warning(
            "Telegram rate limit: %s",
            error,
        )
        return

    logger.exception(
        "Unhandled update error",
        exc_info=error,
    )

    if (
        update
        and update.effective_chat
    ):

        try:

            await context.bot.send_message(
                update.effective_chat.id,
                get_message(
                    "error"
                )["text"],
            )

        except TelegramError:
            pass


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def post_init(
    application,
):
    """Run non-fatal Telegram startup configuration.

    IMPORTANT: application.bot is a telegram.Bot.
    bot.get_me() returns telegram.User and must NEVER be used for Bot API
    methods such as set_my_commands().
    """
    telegram_bot = application.bot

    try:
        me = await telegram_bot.get_me()
        logger.info(
            "Bot connected successfully as @%s (id=%s)",
            me.username,
            me.id,
        )
    except Exception:
        logger.exception("Telegram getMe failed during startup")

    # Keep the visible Telegram command menu limited to /start.
    # /admin and /cancel remain registered handlers but are intentionally
    # not exposed in the normal command menu. This call is non-fatal.
    try:
        await telegram_bot.set_my_commands(
            commands=[BotCommand("start", "Start the bot")]
        )
        logger.info("Telegram command menu configured: /start only")
    except Exception:
        logger.exception(
            "Could not configure Telegram command menu; continuing startup"
        )


async def post_shutdown(
    application,
):
    global DB

    try:
        if DB:
            DB.commit()
            DB.close()
    except Exception:
        logger.exception(
            "Database shutdown error"
        )

    logger.info(
        "Bot shutdown completed."
    )


# ============================================================
# APPLICATION
# ============================================================

def build_application():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "admin",
            admin_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_command,
        )
    )

    # Required for channels/groups that use join requests.
    application.add_handler(
        ChatJoinRequestHandler(
            auto_approve_join_request,
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_callback_router,
            pattern=r"^adm:",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            verify_callback,
            pattern=r"^verify$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            claim_callback,
            pattern=r"^claim$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            stats_callback,
            pattern=r"^stats$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            referral_callback,
            pattern=r"^referral$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            main_callback,
            pattern=r"^main$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            custom_callback_router,
            pattern=(
                r"^(?!adm:)"
                r"(?!verify$)"
                r"(?!claim$)"
                r"(?!stats$)"
                r"(?!referral$)"
                r"(?!main$)"
                r".+"
            ),
        )
    )

    application.add_handler(
        MessageHandler(
            filters.ALL
            & ~filters.COMMAND,
            general_message,
        )
    )

    application.add_error_handler(
        error_handler
    )

    return application


# ============================================================
# CANCEL
# ============================================================

async def cancel_command(
    update,
    context,
):
    if is_admin(
        update.effective_user.id
    ):

        context.user_data.clear()

        await update.message.reply_text(
            "❌ Cancelled.",
            reply_markup=admin_keyboard(),
        )



# ============================================================
# RAILWAY / WEB-SERVICE ENTRYPOINT
# ============================================================

def run_bot():
    """Start the Telegram polling loop in the current thread."""
    initialize_database()
    application = build_application()
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
        close_loop=True,
    )


# ============================================================
# RENDER WEB SERVICE COMPATIBILITY
# ============================================================

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    thread.start()
    logger.info("Render health server listening on 0.0.0.0:%s", port)
    return server


# MAIN
# ============================================================

if __name__ == "__main__":

    initialize_database()
    health_server = start_health_server()

    application = (
        build_application()
    )

    try:

        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
            close_loop=True,
        )

    except KeyboardInterrupt:

        logger.info(
            "Bot stopped by administrator."
        )

    except Exception:

        logger.exception(
            "Fatal startup/runtime error."
        )

        try:
            if DB:
                DB.close()
        except Exception:
            pass

        try:
            health_server.shutdown()
            health_server.server_close()
        except Exception:
            pass

        raise
