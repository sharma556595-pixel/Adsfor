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

def required_channels():
    return db_all(
        """
        SELECT *
        FROM channels
        WHERE enabled=1
        ORDER BY position, id
        """
    )


def channel_keyboard():
    rows = []

    for channel in required_channels():

        url = (
            channel["invite_link"]
            or (
                f"https://t.me/"
                f"{channel['username'].lstrip('@')}"
                if channel["username"]
                else None
            )
        )

        if url:
            rows.append(
                [
                    create_button(
                        channel["join_text"],
                        "URL",
                        url=url,
                        style=channel["style"],
                    )
                ]
            )

    custom_start_buttons = keyboard_from_scope(
        "start"
    )

    rows.extend(
        custom_start_buttons
    )

    rows.append(
        [
            create_button(
                "✅ VERIFY",
                "CALLBACK",
                callback="verify",
                style="success",
            )
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


async def check_channel_membership(
    bot,
    user_id,
):
    missing = []
    errors = []

    for channel in required_channels():

        try:

            member = await bot.get_chat_member(
                chat_id=channel["channel_id"],
                user_id=user_id,
            )

            if member.status in (
                "left",
                "kicked",
            ):
                missing.append(
                    channel
                )

        except TelegramError as exc:
            errors.append(
                (
                    channel,
                    str(exc),
                )
            )

    return missing, errors


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

    text = render_template(
        message["text"],
        chat_id,
        extra,
    )

    entities = deserialize_entities(
        message["entities"],
        context.bot,
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

    register_user(
        update.effective_user,
        referral_id,
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

    if missing or errors:

        names = "\n".join(
            f"• {channel['title']}"
            for channel in missing
        )

        if not names:
            names = (
                "• Some channels could not "
                "be verified."
            )

        join_message = render_template(
            get_message("join")["text"],
            user_id,
        )

        text = (
            join_message
            + "\n\n"
            + names
        )

        try:
            await query.message.edit_text(
                text=text,
                reply_markup=channel_keyboard(),
            )
        except TelegramError:
            try:
                await context.bot.send_message(
                    user_id,
                    text,
                    reply_markup=channel_keyboard(),
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
                "⚠️ Admin action failed safely.\n\n"
                + str(exc)[:500]
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

async def admin_add_flow(
    update,
    context,
    kind,
):
    context.user_data[
        "state"
    ] = "add"

    context.user_data[
        "add_kind"
    ] = kind

    prompts = {

        "channel":
            "📣 Add Channel\n\n"
            "Send:\n"
            "Title | Channel ID | @username | invite URL\n\n"
            "Example:\n"
            "Official Channel | -100123456789 | @example | https://t.me/example",

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
    }

    total = db_one(
        """
        SELECT COUNT(*) AS c
        FROM users
        WHERE blocked=0
        """
    )["c"]

    preview_markup = None

    broadcast_buttons = keyboard_from_scope(
        "broadcast"
    )

    if broadcast_buttons:
        preview_markup = (
            InlineKeyboardMarkup(
                broadcast_buttons
            )
        )

    if preview_markup is None:
        preview_markup = (
            InlineKeyboardMarkup(
                [
                    [
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
                ]
            )
        )
    else:
        extra_rows = [
            [
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
        ]

        preview_markup = (
            InlineKeyboardMarkup(
                broadcast_buttons
                + extra_rows
            )
        )

    try:

        await message.reply_text(
            "👁 BROADCAST PREVIEW\n\n"
            f"{text[:3500]}\n\n"
            f"👥 Recipients: {total}",
            reply_markup=preview_markup,
        )

    except TelegramError:
        await message.reply_text(
            "👁 Broadcast preview prepared.\n\n"
            f"👥 Recipients: {total}",
            reply_markup=preview_markup,
        )

    return True


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
            json.dumps([]),
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

    broadcast_rows = keyboard_from_scope(
        "broadcast"
    )

    broadcast_markup = (
        InlineKeyboardMarkup(
            broadcast_rows
        )
        if broadcast_rows
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
                "❌ Send text, photo or video."
            )
            return True

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
        elif kind == "channel":

            parts = [
                item.strip()
                for item in text.split("|")
            ]

            if len(parts) < 2:
                await message.reply_text(
                    "❌ Invalid format.\n\n"
                    "Title | Channel ID | @username | invite URL"
                )
                return True

            title = parts[0]
            channel_id = parts[1]

            username = (
                parts[2]
                if len(parts) > 2
                and parts[2]
                else None
            )

            invite_link = (
                parts[3]
                if len(parts) > 3
                and parts[3]
                else None
            )

            position = db_one(
                """
                SELECT
                    COALESCE(
                        MAX(position),
                        -1
                    ) + 1 AS p
                FROM channels
                """
            )["p"]

            DB.execute(
                """
                INSERT INTO channels
                (
                    title,
                    channel_id,
                    username,
                    invite_link,
                    position
                )
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    title,
                    channel_id,
                    username,
                    invite_link,
                    position,
                ),
            )

            DB.commit()

            await message.reply_text(
                "✅ Channel added."
            )

        # Button
        elif kind == "button":

            parts = text.split("|")

            if len(parts) < 8:
                await message.reply_text(
                    "❌ Invalid button format.\n\n"
                    "TEXT|SCOPE|ACTION|URL|CALLBACK|STYLE|ROW|POSITION"
                )
                return True

            button_text = parts[0]
            scope = parts[1]
            action = parts[2]
            url = parts[3] or None
            callback = parts[4] or None
            style = (
                parts[5]
                if parts[5] in VALID_STYLES
                else None
            )
            row = safe_int(
                parts[6]
            )
            position = safe_int(
                parts[7]
            )

            DB.execute(
                """
                INSERT INTO buttons
                (
                    scope,
                    text,
                    action,
                    url,
                    callback,
                    style,
                    row,
                    position
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scope,
                    button_text,
                    action,
                    url,
                    callback,
                    style,
                    row,
                    position,
                ),
            )

            DB.commit()

            await message.reply_text(
                "✅ Button added."
            )

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

                caption = (
                    message.caption
                    or get_message(
                        "start"
                    )["text"]
                )

                save_message(
                    "start",
                    caption,
                    message.caption_entities
                    or [],
                    "photo",
                    message.photo[-1].file_id,
                )

                DB.commit()

                await message.reply_text(
                    "✅ Start image saved."
                )

            elif message.video:

                caption = (
                    message.caption
                    or get_message(
                        "start"
                    )["text"]
                )

                save_message(
                    "start",
                    caption,
                    message.caption_entities
                    or [],
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

    context.user_data[
        "state"
    ] = "edit_channel"

    context.user_data[
        "editing_channel"
    ] = channel_id

    await edit_admin_message(
        update,
        "✏️ EDIT CHANNEL\n\n"
        "Send:\n"
        "Title | Channel ID | @username | invite URL\n\n"
        f"Current:\n"
        f"{channel['title']} | "
        f"{channel['channel_id']} | "
        f"{channel['username'] or ''} | "
        f"{channel['invite_link'] or ''}\n\n"
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

    if state == "edit_button":

        button_id = context.user_data.get(
            "editing_button"
        )

        parts = (
            message.text or ""
        ).split("|")

        if len(parts) < 8:
            await message.reply_text(
                "❌ Invalid format."
            )
            return True

        DB.execute(
            """
            UPDATE buttons
            SET text=?,
                scope=?,
                action=?,
                url=?,
                callback=?,
                style=?,
                row=?,
                position=?
            WHERE id=?
            """,
            (
                parts[0],
                parts[1],
                parts[2],
                parts[3] or None,
                parts[4] or None,
                (
                    parts[5]
                    if parts[5]
                    in VALID_STYLES
                    else None
                ),
                safe_int(parts[6]),
                safe_int(parts[7]),
                button_id,
            ),
        )

        DB.commit()

        context.user_data.clear()

        await message.reply_text(
            "✅ Button updated.",
            reply_markup=admin_keyboard(),
        )

        return True

    if state == "edit_channel":

        channel_id = context.user_data.get(
            "editing_channel"
        )

        parts = [
            x.strip()
            for x in (
                message.text or ""
            ).split("|")
        ]

        if len(parts) < 2:
            await message.reply_text(
                "❌ Invalid format."
            )
            return True

        DB.execute(
            """
            UPDATE channels
            SET title=?,
                channel_id=?,
                username=?,
                invite_link=?
            WHERE id=?
            """,
            (
                parts[0],
                parts[1],
                (
                    parts[2]
                    if len(parts) > 2
                    and parts[2]
                    else None
                ),
                (
                    parts[3]
                    if len(parts) > 3
                    and parts[3]
                    else None
                ),
                channel_id,
            ),
        )

        DB.commit()

        context.user_data.clear()

        await message.reply_text(
            "✅ Channel updated.",
            reply_markup=admin_keyboard(),
        )

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

    register_user(
        update.effective_user
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
    bot = await application.bot.get_me()

    logger.info(
        "Bot started successfully as @%s",
        bot.username,
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
