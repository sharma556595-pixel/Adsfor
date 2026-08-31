"""Monolithic Telegram bot: all application code lives in this single file."""
from __future__ import annotations
import asyncio
import csv
from dataclasses import dataclass
import functools
import html
import io
import json
import logging
import math
import os
import platform
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Sequence

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BufferedInputFile, CallbackQuery, ErrorEvent, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message, TelegramObject)
from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# MERGED FROM telegram_admin_bot/config.py
# ==============================================================================



BASE_DIR = Path(__file__).resolve().parent


def _int_list(raw: str) -> tuple[int, ...]:
    out: list[int] = []
    for chunk in raw.replace(";", ",").split(","):
        chunk = chunk.strip()
        if chunk.lstrip("-").isdigit():
            out.append(int(chunk))
    return tuple(out)


@dataclass(frozen=True)
class Config:
    bot_token: str
    owner_ids: tuple[int, ...]
    db_path: Path
    backup_dir: Path
    health_port: int
    broadcast_rate: int   # messages per second, Telegram tolerates ~30
    state_ttl: int        # seconds an idle admin wizard state survives
    timezone: str

    @classmethod
    def load(cls) -> "Config":
        token = os.getenv("BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError(
                "BOT_TOKEN is not set. Copy .env.example to .env and fill it in."
            )
        owners = _int_list(os.getenv("OWNER_IDS", ""))
        if not owners:
            raise RuntimeError(
                "OWNER_IDS is not set. At least one Telegram ID must own the bot."
            )
        db_path = Path(os.getenv("DB_PATH", BASE_DIR / "data" / "bot.db"))
        backup_dir = Path(os.getenv("BACKUP_DIR", BASE_DIR / "data" / "backups"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        backup_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            bot_token=token,
            owner_ids=owners,
            db_path=db_path,
            backup_dir=backup_dir,
            health_port=int(os.getenv("HEALTH_PORT", "8080")),
            broadcast_rate=int(os.getenv("BROADCAST_RATE", "25")),
            state_ttl=int(os.getenv("ADMIN_STATE_TTL", "600")),
            timezone=os.getenv("TIMEZONE", "UTC"),
        )

    def safe_dict(self) -> dict[str, str]:
        """Loggable view of the configuration - token deliberately omitted."""
        return {
            "owners": str(len(self.owner_ids)),
            "db_path": str(self.db_path),
            "health_port": str(self.health_port),
            "broadcast_rate": str(self.broadcast_rate),
            "timezone": self.timezone,
        }


config = Config.load()


# ==============================================================================
# MERGED FROM telegram_admin_bot/core/db.py
# ==============================================================================




UTC = timezone.utc

# --------------------------------------------------------------------------
# time helpers - all timestamps are UTC strings, which sort lexicographically
# --------------------------------------------------------------------------

TS = "%Y-%m-%d %H:%M:%S"


def now() -> str:
    return datetime.now(UTC).strftime(TS)


def now_dt() -> datetime:
    return datetime.now(UTC)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, TS).replace(tzinfo=UTC)
    except ValueError:
        return None


def shift(days: int = 0, hours: int = 0) -> str:
    return (now_dt() - timedelta(days=days, hours=hours)).strftime(TS)


def day_start(offset_days: int = 0) -> str:
    d = (now_dt() - timedelta(days=offset_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return d.strftime(TS)


PERIODS = {
    "today": (lambda: day_start(0), lambda: now()),
    "yesterday": (lambda: day_start(1), lambda: day_start(0)),
    "7d": (lambda: day_start(6), lambda: now()),
    "30d": (lambda: day_start(29), lambda: now()),
    "all": (lambda: "1970-01-01 00:00:00", lambda: now()),
}

PERIOD_LABELS = {
    "today": "Today",
    "yesterday": "Yesterday",
    "7d": "7 Days",
    "30d": "30 Days",
    "all": "All Time",
}


def period_bounds(period: str) -> tuple[str, str]:
    start, end = PERIODS.get(period, PERIODS["today"])
    return start(), end()


# --------------------------------------------------------------------------
# schema
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    tg_id              INTEGER PRIMARY KEY,
    username           TEXT,
    first_name         TEXT,
    last_name          TEXT,
    joined_at          TEXT NOT NULL,
    last_seen_at       TEXT NOT NULL,
    is_blocked         INTEGER NOT NULL DEFAULT 0,
    blocked_reason     TEXT,
    force_join_ok      INTEGER NOT NULL DEFAULT 0,
    force_join_at      TEXT,
    referred_by        INTEGER,
    referral_count     INTEGER NOT NULL DEFAULT 0,
    claim_count        INTEGER NOT NULL DEFAULT 0,
    deleted            INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_users_joined  ON users(joined_at);
CREATE INDEX IF NOT EXISTS idx_users_seen    ON users(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_users_ref     ON users(referred_by);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id      INTEGER,
    event      TEXT NOT NULL,
    meta       TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(event, created_at);

CREATE TABLE IF NOT EXISTS channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     TEXT NOT NULL UNIQUE,
    title       TEXT NOT NULL,
    invite_link TEXT,
    enabled     INTEGER NOT NULL DEFAULT 1,
    position    INTEGER NOT NULL DEFAULT 0,
    added_by    INTEGER,
    added_at    TEXT NOT NULL,
    last_check  TEXT,
    last_result TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    key         TEXT PRIMARY KEY,
    category    TEXT NOT NULL,
    name        TEXT NOT NULL,
    content     TEXT NOT NULL,
    default_content TEXT NOT NULL,
    parse_mode  TEXT NOT NULL DEFAULT 'HTML',
    enabled     INTEGER NOT NULL DEFAULT 1,
    updated_at  TEXT,
    updated_by  INTEGER
);

CREATE TABLE IF NOT EXISTS buttons (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    screen    TEXT NOT NULL,
    text      TEXT NOT NULL,
    emoji     TEXT,
    kind      TEXT NOT NULL DEFAULT 'callback',   -- callback | url
    url       TEXT,
    callback  TEXT,
    style     TEXT NOT NULL DEFAULT 'DEFAULT',    -- PRIMARY|SUCCESS|DANGER|DEFAULT
    row       INTEGER NOT NULL DEFAULT 0,
    position  INTEGER NOT NULL DEFAULT 0,
    enabled   INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_buttons_screen ON buttons(screen, row, position);

CREATE TABLE IF NOT EXISTS broadcasts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL DEFAULT 'text',
    body         TEXT,
    file_id      TEXT,
    from_chat_id TEXT,
    from_msg_id  INTEGER,
    parse_mode   TEXT NOT NULL DEFAULT 'HTML',
    markup_json  TEXT,
    audience     TEXT NOT NULL DEFAULT 'all',
    custom_ids   TEXT,
    status       TEXT NOT NULL DEFAULT 'DRAFT',
    scheduled_at TEXT,
    started_at   TEXT,
    finished_at  TEXT,
    total        INTEGER NOT NULL DEFAULT 0,
    sent         INTEGER NOT NULL DEFAULT 0,
    failed       INTEGER NOT NULL DEFAULT 0,
    blocked      INTEGER NOT NULL DEFAULT 0,
    created_by   INTEGER,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    body        TEXT,
    file_id     TEXT,
    markup_json TEXT,
    audience    TEXT NOT NULL DEFAULT 'all',
    start_at    TEXT,
    end_at      TEXT,
    status      TEXT NOT NULL DEFAULT 'PAUSED',   -- ACTIVE|PAUSED|ENDED
    views       INTEGER NOT NULL DEFAULT 0,
    clicks      INTEGER NOT NULL DEFAULT 0,
    claims      INTEGER NOT NULL DEFAULT 0,
    joins       INTEGER NOT NULL DEFAULT 0,
    created_by  INTEGER,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS referrals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id  INTEGER NOT NULL,
    referred_id  INTEGER NOT NULL UNIQUE,
    valid        INTEGER NOT NULL DEFAULT 1,
    reason       TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admins (
    tg_id       INTEGER PRIMARY KEY,
    name        TEXT,
    role        TEXT NOT NULL DEFAULT 'ADMIN',
    permissions TEXT,                              -- JSON list, NULL = role preset
    added_by    INTEGER,
    added_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id   INTEGER NOT NULL,
    action     TEXT NOT NULL,
    target     TEXT,
    result     TEXT NOT NULL DEFAULT 'OK',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_logs(created_at);

CREATE TABLE IF NOT EXISTS error_logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,
    message    TEXT NOT NULL,
    operation  TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    section    TEXT NOT NULL DEFAULT 'general',
    updated_at TEXT,
    updated_by INTEGER
);

CREATE TABLE IF NOT EXISTS schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,                     -- broadcast | campaign | announcement
    ref_id      INTEGER,
    repeat      TEXT NOT NULL DEFAULT 'once',      -- once | daily | weekly
    run_at      TEXT,
    time_of_day TEXT,
    weekday     INTEGER,
    status      TEXT NOT NULL DEFAULT 'SCHEDULED', -- SCHEDULED|PAUSED|DONE|CANCELLED
    last_run_at TEXT,
    created_by  INTEGER,
    created_at  TEXT NOT NULL
);
"""

# --------------------------------------------------------------------------
# seed data
# --------------------------------------------------------------------------

DEFAULT_MESSAGES: list[tuple[str, str, str, str]] = [
    ("start", "START", "Welcome",
     "👋 <b>Welcome, {name}!</b>\n\nTap the button below to get started."),
    ("force_join", "FORCE JOIN", "Force Join Prompt",
     "🔒 <b>One step left</b>\n\nJoin the channels below, then press "
     "<b>✅ I Joined</b> to continue."),
    ("force_join_failed", "FORCE JOIN", "Verification Failed",
     "❌ You are still missing some channels. Join them all and try again."),
    ("claim", "CLAIM", "Claim Screen",
     "🎁 <b>Your reward is ready</b>\n\nPress the claim button below."),
    ("success", "SUCCESS", "Claim Success",
     "✅ <b>Done!</b>\n\nYour reward has been claimed successfully."),
    ("error", "ERROR", "Generic Error",
     "⚠️ Something went wrong. Please try again in a moment."),
    ("help", "HELP", "Help",
     "ℹ️ <b>Help</b>\n\nUse /start to begin. Contact support if you get stuck."),
    ("about", "ABOUT", "About",
     "🤖 <b>About</b>\n\nThis bot is operated by our team."),
    ("maintenance", "MAINTENANCE", "Maintenance Notice",
     "🛠 <b>Maintenance</b>\n\nThe bot is briefly unavailable. Please come back soon."),
    ("followup", "START", "Follow-up",
     "👀 <b>Still there?</b>\n\nYou have not finished your claim yet."),
    ("referral", "START", "Referral Info",
     "🎁 <b>Invite friends</b>\n\nYour link: {ref_link}\nInvited: {ref_count}"),
    ("broadcast_footer", "BROADCAST", "Broadcast Footer", ""),
    ("admin_denied", "ADMIN", "Access Denied",
     "⛔️ You do not have permission for this section."),
]

DEFAULT_BUTTONS: list[dict[str, Any]] = [
    dict(screen="start", text="Get Started", emoji="🚀", kind="callback",
         callback="flow:claim", style="PRIMARY", row=0, position=0),
    dict(screen="start", text="Help", emoji="ℹ️", kind="callback",
         callback="flow:help", style="DEFAULT", row=1, position=0),
    dict(screen="start", text="Invite Friends", emoji="🎁", kind="callback",
         callback="flow:referral", style="SUCCESS", row=1, position=1),
    dict(screen="force_join", text="I Joined", emoji="✅", kind="callback",
         callback="flow:verify", style="SUCCESS", row=99, position=0),
    dict(screen="claim", text="Claim Now", emoji="🎁", kind="callback",
         callback="flow:claim_confirm", style="PRIMARY", row=0, position=0),
    dict(screen="success", text="Invite Friends", emoji="🎁", kind="callback",
         callback="flow:referral", style="SUCCESS", row=0, position=0),
]

DEFAULT_SETTINGS: list[tuple[str, str, str]] = [
    # key, value, section
    ("bot_enabled", "1", "general"),
    ("maintenance_mode", "0", "general"),
    ("active_window_days", "7", "general"),
    ("force_join_enabled", "1", "force_join"),
    ("force_join_recheck", "1", "force_join"),
    ("stage_welcome", "1", "journey"),
    ("stage_force_join", "1", "journey"),
    ("stage_claim", "1", "journey"),
    ("stage_success", "1", "journey"),
    ("stage_followup", "0", "journey"),
    ("followup_delay_min", "60", "journey"),
    ("broadcast_rate", "25", "broadcast"),
    ("broadcast_retry", "1", "broadcast"),
    ("referral_enabled", "1", "referral"),
    ("referral_reward", "1", "referral"),
    ("referral_min_joins", "1", "referral"),
    ("referral_self_block", "1", "referral"),
    ("scheduler_enabled", "1", "scheduler"),
    ("log_retention_days", "30", "logging"),
    ("audit_enabled", "1", "logging"),
    ("notify_new_user", "0", "notifications"),
    ("notify_new_referral", "0", "notifications"),
    ("notify_broadcast_done", "1", "notifications"),
    ("notify_broadcast_failed", "1", "notifications"),
    ("notify_channel_error", "1", "notifications"),
    ("notify_db_error", "1", "notifications"),
    ("notify_bot_error", "1", "notifications"),
]


# --------------------------------------------------------------------------
# connection wrapper
# --------------------------------------------------------------------------

class DB:
    def __init__(self) -> None:
        self._conn: aiosqlite.Connection | None = None
        self.path: Path | None = None
        self.started_at = now_dt()

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected yet")
        return self._conn

    async def connect(self, path: Path) -> None:
        self.path = Path(path)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        await self.seed()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        cur = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cur.lastrowid or 0

    async def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        await self.conn.executemany(sql, rows)
        await self.conn.commit()

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        cur = await self.conn.execute(sql, params)
        return list(await cur.fetchall())

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> aiosqlite.Row | None:
        cur = await self.conn.execute(sql, params)
        return await cur.fetchone()

    async def scalar(self, sql: str, params: Sequence[Any] = (), default: Any = 0) -> Any:
        row = await self.fetchone(sql, params)
        if row is None or row[0] is None:
            return default
        return row[0]

    async def seed(self) -> None:
        stamp = now()
        for key, category, name, content in DEFAULT_MESSAGES:
            await self.conn.execute(
                "INSERT OR IGNORE INTO messages"
                " (key, category, name, content, default_content, updated_at)"
                " VALUES (?,?,?,?,?,?)",
                (key, category, name, content, content, stamp),
            )
        for key, value, section in DEFAULT_SETTINGS:
            await self.conn.execute(
                "INSERT OR IGNORE INTO settings (key, value, section, updated_at)"
                " VALUES (?,?,?,?)",
                (key, value, section, stamp),
            )
        existing = await self.scalar("SELECT COUNT(*) FROM buttons")
        if not existing:
            for b in DEFAULT_BUTTONS:
                await self.conn.execute(
                    "INSERT INTO buttons (screen, text, emoji, kind, url, callback,"
                    " style, row, position) VALUES (?,?,?,?,?,?,?,?,?)",
                    (b["screen"], b["text"], b["emoji"], b["kind"], b.get("url"),
                     b.get("callback"), b["style"], b["row"], b["position"]),
                )
        await self.conn.commit()

    async def size_bytes(self) -> int:
        page_size = await self.scalar("PRAGMA page_size")
        page_count = await self.scalar("PRAGMA page_count")
        return int(page_size) * int(page_count)

    async def ping(self) -> bool:
        try:
            await self.scalar("SELECT 1")
            return True
        except Exception:
            return False


db = DB()


# --------------------------------------------------------------------------
# repositories
# --------------------------------------------------------------------------

class Users:
    @staticmethod
    async def touch(tg_id: int, username: str | None, first: str | None,
                    last: str | None) -> bool:
        """Upsert the user, return True when this is a brand new record."""
        stamp = now()
        row = await db.fetchone("SELECT tg_id FROM users WHERE tg_id=?", (tg_id,))
        if row is None:
            await db.execute(
                "INSERT INTO users (tg_id, username, first_name, last_name,"
                " joined_at, last_seen_at) VALUES (?,?,?,?,?,?)",
                (tg_id, username, first, last, stamp, stamp),
            )
            return True
        await db.execute(
            "UPDATE users SET username=?, first_name=?, last_name=?, last_seen_at=?,"
            " deleted=0 WHERE tg_id=?",
            (username, first, last, stamp, tg_id),
        )
        return False

    @staticmethod
    async def get(tg_id: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM users WHERE tg_id=?", (tg_id,))

    @staticmethod
    async def set_blocked(tg_id: int, blocked: bool, reason: str | None = None) -> None:
        await db.execute(
            "UPDATE users SET is_blocked=?, blocked_reason=? WHERE tg_id=?",
            (1 if blocked else 0, reason, tg_id),
        )

    @staticmethod
    async def delete(tg_id: int) -> None:
        await db.execute("UPDATE users SET deleted=1 WHERE tg_id=?", (tg_id,))

    @staticmethod
    async def set_force_join(tg_id: int, ok: bool) -> None:
        await db.execute(
            "UPDATE users SET force_join_ok=?, force_join_at=? WHERE tg_id=?",
            (1 if ok else 0, now(), tg_id),
        )

    @staticmethod
    async def add_claim(tg_id: int) -> None:
        await db.execute(
            "UPDATE users SET claim_count=claim_count+1 WHERE tg_id=?", (tg_id,)
        )

    @staticmethod
    async def search(query: str, limit: int = 25) -> list[aiosqlite.Row]:
        q = query.strip().lstrip("@")
        if q.isdigit():
            return await db.fetchall(
                "SELECT * FROM users WHERE tg_id=? AND deleted=0", (int(q),)
            )
        like = f"%{q.lower()}%"
        return await db.fetchall(
            "SELECT * FROM users WHERE deleted=0 AND ("
            " LOWER(IFNULL(username,'')) LIKE ?"
            " OR LOWER(IFNULL(first_name,'')) LIKE ?"
            " OR LOWER(IFNULL(last_name,'')) LIKE ?)"
            " ORDER BY last_seen_at DESC LIMIT ?",
            (like, like, like, limit),
        )

    @staticmethod
    def _filter_sql(kind: str) -> tuple[str, list[Any]]:
        active_cut = shift(days=7)
        if kind == "active":
            return "deleted=0 AND is_blocked=0 AND last_seen_at>=?", [active_cut]
        if kind == "blocked":
            return "deleted=0 AND is_blocked=1", []
        if kind == "new":
            return "deleted=0 AND joined_at>=?", [day_start(0)]
        return "deleted=0", []

    @staticmethod
    async def page(kind: str, page: int, per_page: int = 8
                   ) -> tuple[list[aiosqlite.Row], int]:
        where, params = Users._filter_sql(kind)
        total = await db.scalar(f"SELECT COUNT(*) FROM users WHERE {where}", params)
        rows = await db.fetchall(
            f"SELECT * FROM users WHERE {where} ORDER BY joined_at DESC LIMIT ? OFFSET ?",
            [*params, per_page, page * per_page],
        )
        return rows, int(total)

    @staticmethod
    async def audience(audience: str, custom: str | None = None) -> list[int]:
        if audience == "custom" and custom:
            ids = [int(x) for x in custom.replace(";", ",").split(",")
                   if x.strip().lstrip("-").isdigit()]
            if not ids:
                return []
            marks = ",".join("?" * len(ids))
            rows = await db.fetchall(
                f"SELECT tg_id FROM users WHERE deleted=0 AND tg_id IN ({marks})", ids
            )
            return [r["tg_id"] for r in rows]
        where, params = {
            "all": ("deleted=0", []),
            "not_blocked": ("deleted=0 AND is_blocked=0", []),
            "active": ("deleted=0 AND is_blocked=0 AND last_seen_at>=?", [shift(days=7)]),
            "new": ("deleted=0 AND is_blocked=0 AND joined_at>=?", [day_start(6)]),
        }.get(audience, ("deleted=0 AND is_blocked=0", []))
        rows = await db.fetchall(f"SELECT tg_id FROM users WHERE {where}", params)
        return [r["tg_id"] for r in rows]

    @staticmethod
    async def export_rows() -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT tg_id, username, first_name, last_name, joined_at, last_seen_at,"
            " is_blocked, force_join_ok, referral_count, claim_count"
            " FROM users WHERE deleted=0 ORDER BY joined_at"
        )


class Events:
    @staticmethod
    async def log(tg_id: int | None, event: str, meta: str | None = None) -> None:
        await db.execute(
            "INSERT INTO events (tg_id, event, meta, created_at) VALUES (?,?,?,?)",
            (tg_id, event, meta, now()),
        )

    @staticmethod
    async def count(event: str, period: str = "all") -> int:
        start, end = period_bounds(period)
        return int(await db.scalar(
            "SELECT COUNT(*) FROM events WHERE event=? AND created_at BETWEEN ? AND ?",
            (event, start, end),
        ))


class Channels:
    @staticmethod
    async def all(enabled_only: bool = False) -> list[aiosqlite.Row]:
        sql = "SELECT * FROM channels"
        if enabled_only:
            sql += " WHERE enabled=1"
        sql += " ORDER BY position, id"
        return await db.fetchall(sql)

    @staticmethod
    async def get(cid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM channels WHERE id=?", (cid,))

    @staticmethod
    async def add(chat_id: str, title: str, link: str | None, admin_id: int) -> int:
        pos = int(await db.scalar("SELECT IFNULL(MAX(position),0)+1 FROM channels"))
        return await db.execute(
            "INSERT INTO channels (chat_id, title, invite_link, position, added_by,"
            " added_at) VALUES (?,?,?,?,?,?)",
            (str(chat_id), title, link, pos, admin_id, now()),
        )

    @staticmethod
    async def update(cid: int, **fields: Any) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE channels SET {sets} WHERE id=?",
                         [*fields.values(), cid])

    @staticmethod
    async def delete(cid: int) -> None:
        await db.execute("DELETE FROM channels WHERE id=?", (cid,))

    @staticmethod
    async def move(cid: int, direction: int) -> None:
        rows = await Channels.all()
        order = [r["id"] for r in rows]
        if cid not in order:
            return
        i = order.index(cid)
        j = i + direction
        if not 0 <= j < len(order):
            return
        order[i], order[j] = order[j], order[i]
        for pos, item in enumerate(order):
            await db.execute("UPDATE channels SET position=? WHERE id=?", (pos, item))

    @staticmethod
    async def counts() -> tuple[int, int, int]:
        total = int(await db.scalar("SELECT COUNT(*) FROM channels"))
        on = int(await db.scalar("SELECT COUNT(*) FROM channels WHERE enabled=1"))
        return total, on, total - on


class Messages:
    @staticmethod
    async def get(key: str) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM messages WHERE key=?", (key,))

    @staticmethod
    async def by_category(category: str) -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT * FROM messages WHERE category=? ORDER BY name", (category,)
        )

    @staticmethod
    async def categories() -> list[str]:
        rows = await db.fetchall(
            "SELECT DISTINCT category FROM messages ORDER BY category"
        )
        return [r["category"] for r in rows]

    @staticmethod
    async def update(key: str, admin_id: int, **fields: Any) -> None:
        fields["updated_at"] = now()
        fields["updated_by"] = admin_id
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE messages SET {sets} WHERE key=?",
                         [*fields.values(), key])

    @staticmethod
    async def reset(key: str, admin_id: int) -> None:
        row = await Messages.get(key)
        if row:
            await Messages.update(key, admin_id, content=row["default_content"])


class Buttons:
    @staticmethod
    async def for_screen(screen: str, enabled_only: bool = True) -> list[aiosqlite.Row]:
        sql = "SELECT * FROM buttons WHERE screen=?"
        if enabled_only:
            sql += " AND enabled=1"
        sql += " ORDER BY row, position, id"
        return await db.fetchall(sql, (screen,))

    @staticmethod
    async def get(bid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM buttons WHERE id=?", (bid,))

    @staticmethod
    async def add(**fields: Any) -> int:
        keys = ", ".join(fields)
        marks = ", ".join("?" * len(fields))
        return await db.execute(
            f"INSERT INTO buttons ({keys}) VALUES ({marks})", list(fields.values())
        )

    @staticmethod
    async def update(bid: int, **fields: Any) -> None:
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE buttons SET {sets} WHERE id=?",
                         [*fields.values(), bid])

    @staticmethod
    async def delete(bid: int) -> None:
        await db.execute("DELETE FROM buttons WHERE id=?", (bid,))

    @staticmethod
    async def duplicate(bid: int) -> int | None:
        row = await Buttons.get(bid)
        if not row:
            return None
        data = {k: row[k] for k in row.keys() if k != "id"}
        data["position"] = int(data["position"]) + 1
        return await Buttons.add(**data)

    @staticmethod
    async def screens() -> list[str]:
        rows = await db.fetchall("SELECT DISTINCT screen FROM buttons ORDER BY screen")
        known = {r["screen"] for r in rows}
        for extra in ("start", "force_join", "claim", "success"):
            known.add(extra)
        return sorted(known)


class Broadcasts:
    @staticmethod
    async def create(admin_id: int, **fields: Any) -> int:
        fields.setdefault("created_at", now())
        fields["created_by"] = admin_id
        keys = ", ".join(fields)
        marks = ", ".join("?" * len(fields))
        return await db.execute(
            f"INSERT INTO broadcasts ({keys}) VALUES ({marks})", list(fields.values())
        )

    @staticmethod
    async def get(bid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM broadcasts WHERE id=?", (bid,))

    @staticmethod
    async def update(bid: int, **fields: Any) -> None:
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE broadcasts SET {sets} WHERE id=?",
                         [*fields.values(), bid])

    @staticmethod
    async def delete(bid: int) -> None:
        await db.execute("DELETE FROM broadcasts WHERE id=?", (bid,))

    @staticmethod
    async def page(page: int, per_page: int = 6) -> tuple[list[aiosqlite.Row], int]:
        total = int(await db.scalar("SELECT COUNT(*) FROM broadcasts"))
        rows = await db.fetchall(
            "SELECT * FROM broadcasts ORDER BY id DESC LIMIT ? OFFSET ?",
            (per_page, page * per_page),
        )
        return rows, total

    @staticmethod
    async def due() -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT * FROM broadcasts WHERE status='SCHEDULED' AND scheduled_at<=?",
            (now(),),
        )

    @staticmethod
    async def delivery_rate() -> float:
        row = await db.fetchone(
            "SELECT IFNULL(SUM(sent),0) s, IFNULL(SUM(total),0) t FROM broadcasts"
            " WHERE status='COMPLETED'"
        )
        if not row or not row["t"]:
            return 0.0
        return row["s"] * 100.0 / row["t"]


class Campaigns:
    @staticmethod
    async def all() -> list[aiosqlite.Row]:
        return await db.fetchall("SELECT * FROM campaigns ORDER BY id DESC")

    @staticmethod
    async def get(cid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM campaigns WHERE id=?", (cid,))

    @staticmethod
    async def create(admin_id: int, **fields: Any) -> int:
        fields.setdefault("created_at", now())
        fields["created_by"] = admin_id
        keys = ", ".join(fields)
        marks = ", ".join("?" * len(fields))
        return await db.execute(
            f"INSERT INTO campaigns ({keys}) VALUES ({marks})", list(fields.values())
        )

    @staticmethod
    async def update(cid: int, **fields: Any) -> None:
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE campaigns SET {sets} WHERE id=?",
                         [*fields.values(), cid])

    @staticmethod
    async def delete(cid: int) -> None:
        await db.execute("DELETE FROM campaigns WHERE id=?", (cid,))

    @staticmethod
    async def bump(cid: int, field: str) -> None:
        if field not in {"views", "clicks", "claims", "joins"}:
            return
        await db.execute(f"UPDATE campaigns SET {field}={field}+1 WHERE id=?", (cid,))

    @staticmethod
    async def active() -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT * FROM campaigns WHERE status='ACTIVE'"
            " AND (start_at IS NULL OR start_at<=?)"
            " AND (end_at IS NULL OR end_at>=?)",
            (now(), now()),
        )


class Referrals:
    @staticmethod
    async def record(referrer: int, referred: int) -> tuple[bool, str]:
        if referrer == referred:
            return False, "self-referral"
        existing = await db.fetchone(
            "SELECT id FROM referrals WHERE referred_id=?", (referred,)
        )
        if existing:
            return False, "already referred"
        await db.execute(
            "INSERT INTO referrals (referrer_id, referred_id, created_at)"
            " VALUES (?,?,?)",
            (referrer, referred, now()),
        )
        await db.execute(
            "UPDATE users SET referral_count=referral_count+1 WHERE tg_id=?", (referrer,)
        )
        await db.execute(
            "UPDATE users SET referred_by=? WHERE tg_id=?", (referrer, referred)
        )
        return True, "ok"

    @staticmethod
    async def top(limit: int = 10) -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT u.tg_id, u.username, u.first_name, u.referral_count FROM users u"
            " WHERE u.referral_count>0 ORDER BY u.referral_count DESC, u.joined_at"
            " LIMIT ?",
            (limit,),
        )

    @staticmethod
    async def stats() -> dict[str, int]:
        total = int(await db.scalar("SELECT COUNT(*) FROM referrals"))
        valid = int(await db.scalar("SELECT COUNT(*) FROM referrals WHERE valid=1"))
        referrers = int(await db.scalar(
            "SELECT COUNT(DISTINCT referrer_id) FROM referrals"
        ))
        return {"total": total, "valid": valid, "referrers": referrers,
                "blocked": total - valid}


class Admins:
    @staticmethod
    async def all() -> list[aiosqlite.Row]:
        return await db.fetchall("SELECT * FROM admins ORDER BY role, tg_id")

    @staticmethod
    async def get(tg_id: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM admins WHERE tg_id=?", (tg_id,))

    @staticmethod
    async def upsert(tg_id: int, role: str, added_by: int,
                     name: str | None = None) -> None:
        await db.execute(
            "INSERT INTO admins (tg_id, name, role, added_by, added_at)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(tg_id) DO UPDATE SET role=excluded.role, name=excluded.name",
            (tg_id, name, role, added_by, now()),
        )

    @staticmethod
    async def set_permissions(tg_id: int, perms: list[str] | None) -> None:
        await db.execute(
            "UPDATE admins SET permissions=? WHERE tg_id=?",
            (json.dumps(perms) if perms is not None else None, tg_id),
        )

    @staticmethod
    async def remove(tg_id: int) -> None:
        await db.execute("DELETE FROM admins WHERE tg_id=?", (tg_id,))


class Audit:
    @staticmethod
    async def log(admin_id: int, action: str, target: str | None = None,
                  result: str = "OK") -> None:
        await db.execute(
            "INSERT INTO audit_logs (admin_id, action, target, result, created_at)"
            " VALUES (?,?,?,?,?)",
            (admin_id, action, target, result, now()),
        )

    @staticmethod
    async def page(page: int, per_page: int = 10) -> tuple[list[aiosqlite.Row], int]:
        total = int(await db.scalar("SELECT COUNT(*) FROM audit_logs"))
        rows = await db.fetchall(
            "SELECT * FROM audit_logs ORDER BY id DESC LIMIT ? OFFSET ?",
            (per_page, page * per_page),
        )
        return rows, total

    @staticmethod
    async def cleanup(days: int) -> int:
        cut = shift(days=days)
        before = int(await db.scalar("SELECT COUNT(*) FROM audit_logs"))
        await db.execute("DELETE FROM audit_logs WHERE created_at<?", (cut,))
        await db.execute("DELETE FROM error_logs WHERE created_at<?", (cut,))
        await db.execute("DELETE FROM events WHERE created_at<?", (cut,))
        after = int(await db.scalar("SELECT COUNT(*) FROM audit_logs"))
        return before - after


class Errors:
    @staticmethod
    async def log(kind: str, message: str, operation: str | None = None) -> int:
        return await db.execute(
            "INSERT INTO error_logs (kind, message, operation, created_at)"
            " VALUES (?,?,?,?)",
            (kind, message[:900], operation, now()),
        )

    @staticmethod
    async def page(page: int, per_page: int = 6) -> tuple[list[aiosqlite.Row], int]:
        total = int(await db.scalar("SELECT COUNT(*) FROM error_logs"))
        rows = await db.fetchall(
            "SELECT * FROM error_logs ORDER BY id DESC LIMIT ? OFFSET ?",
            (per_page, page * per_page),
        )
        return rows, total

    @staticmethod
    async def get(eid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM error_logs WHERE id=?", (eid,))

    @staticmethod
    async def clear(eid: int | None = None) -> None:
        if eid is None:
            await db.execute("DELETE FROM error_logs")
        else:
            await db.execute("DELETE FROM error_logs WHERE id=?", (eid,))

    @staticmethod
    async def last() -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM error_logs ORDER BY id DESC LIMIT 1")


class Settings:
    @staticmethod
    async def all() -> dict[str, str]:
        rows = await db.fetchall("SELECT key, value FROM settings")
        return {r["key"]: r["value"] for r in rows}

    @staticmethod
    async def section(name: str) -> list[aiosqlite.Row]:
        return await db.fetchall(
            "SELECT * FROM settings WHERE section=? ORDER BY key", (name,)
        )

    @staticmethod
    async def sections() -> list[str]:
        rows = await db.fetchall("SELECT DISTINCT section FROM settings ORDER BY section")
        return [r["section"] for r in rows]

    @staticmethod
    async def set(key: str, value: str, admin_id: int | None = None,
                  section: str = "general") -> None:
        await db.execute(
            "INSERT INTO settings (key, value, section, updated_at, updated_by)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
            " updated_at=excluded.updated_at, updated_by=excluded.updated_by",
            (key, value, section, now(), admin_id),
        )


class Schedules:
    @staticmethod
    async def all(active_only: bool = False) -> list[aiosqlite.Row]:
        sql = "SELECT * FROM schedules"
        if active_only:
            sql += " WHERE status='SCHEDULED'"
        sql += " ORDER BY id DESC"
        return await db.fetchall(sql)

    @staticmethod
    async def get(sid: int) -> aiosqlite.Row | None:
        return await db.fetchone("SELECT * FROM schedules WHERE id=?", (sid,))

    @staticmethod
    async def create(admin_id: int, **fields: Any) -> int:
        fields.setdefault("created_at", now())
        fields["created_by"] = admin_id
        keys = ", ".join(fields)
        marks = ", ".join("?" * len(fields))
        return await db.execute(
            f"INSERT INTO schedules ({keys}) VALUES ({marks})", list(fields.values())
        )

    @staticmethod
    async def update(sid: int, **fields: Any) -> None:
        sets = ", ".join(f"{k}=?" for k in fields)
        await db.execute(f"UPDATE schedules SET {sets} WHERE id=?",
                         [*fields.values(), sid])

    @staticmethod
    async def delete(sid: int) -> None:
        await db.execute("DELETE FROM schedules WHERE id=?", (sid,))


class Stats:
    """Every number the dashboard and the analytics screens display."""

    @staticmethod
    async def new_users(period: str) -> int:
        start, end = period_bounds(period)
        return int(await db.scalar(
            "SELECT COUNT(*) FROM users WHERE deleted=0 AND joined_at BETWEEN ? AND ?",
            (start, end),
        ))

    @staticmethod
    async def overview() -> dict[str, Any]:
        window = int((await Settings.all()).get("active_window_days", "7"))
        total = int(await db.scalar("SELECT COUNT(*) FROM users WHERE deleted=0"))
        active = int(await db.scalar(
            "SELECT COUNT(*) FROM users WHERE deleted=0 AND is_blocked=0"
            " AND last_seen_at>=?", (shift(days=window),)
        ))
        blocked = int(await db.scalar(
            "SELECT COUNT(*) FROM users WHERE deleted=0 AND is_blocked=1"
        ))
        returning = int(await db.scalar(
            "SELECT COUNT(*) FROM users WHERE deleted=0 AND last_seen_at>=?"
            " AND joined_at<?", (shift(days=window), day_start(0))
        ))
        channels_total, channels_on, _ = await Channels.counts()
        broadcasts = int(await db.scalar("SELECT COUNT(*) FROM broadcasts"))
        return {
            "total_users": total,
            "active_users": active,
            "blocked_users": blocked,
            "returning_users": returning,
            "channels": channels_total,
            "channels_enabled": channels_on,
            "broadcasts": broadcasts,
            "new_today": await Stats.new_users("today"),
            "new_yesterday": await Stats.new_users("yesterday"),
            "new_week": await Stats.new_users("7d"),
            "new_month": await Stats.new_users("30d"),
        }

    @staticmethod
    async def analytics(period: str) -> dict[str, Any]:
        base = await Stats.overview()
        fj_ok = int(await db.scalar(
            "SELECT COUNT(*) FROM users WHERE deleted=0 AND force_join_ok=1"
        ))
        data = {
            **base,
            "period": period,
            "period_users": await Stats.new_users(period),
            "force_join_ok": fj_ok,
            "force_join_fail": await Events.count("fj_fail", period),
            "claim_clicks": await Events.count("claim_click", period),
            "claim_success": await Events.count("claim_ok", period),
            "referral_users": (await Referrals.stats())["valid"],
            "delivery_rate": await Broadcasts.delivery_rate(),
        }
        return data

    @staticmethod
    async def table_counts() -> dict[str, int]:
        out = {}
        for table in ("users", "channels", "messages", "buttons", "broadcasts",
                      "campaigns", "referrals", "audit_logs", "error_logs",
                      "events", "schedules"):
            out[table] = int(await db.scalar(f"SELECT COUNT(*) FROM {table}"))
        return out


# ==============================================================================
# MERGED FROM telegram_admin_bot/core/ui.py
# ==============================================================================




DIV = "━━━━━━━━━━━━━━━━━━━━"

# Telegram inline keyboards have no colour API, so a button "style" is
# expressed with a leading marker. The vocabulary stays consistent panel-wide.
STYLE_MARK = {
    "PRIMARY": "🔵",
    "SUCCESS": "🟢",
    "DANGER": "🔴",
    "DEFAULT": "",
}


# --------------------------------------------------------------------------
# callback data
# --------------------------------------------------------------------------

def cb(section: str, action: str = "open", *args: Any) -> str:
    """Build admin callback data: a:<section>:<action>[:<arg>...]."""
    return ":".join(["a", section, action, *[str(a) for a in args]])


def parse_cb(data: str) -> tuple[str, str, list[str]]:
    parts = data.split(":")
    while len(parts) < 3:
        parts.append("")
    return parts[1], parts[2], parts[3:]


def arg(args: Sequence[str], index: int, default: Any = None) -> Any:
    return args[index] if len(args) > index else default


def int_arg(args: Sequence[str], index: int, default: int = 0) -> int:
    value = arg(args, index)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# --------------------------------------------------------------------------
# text helpers
# --------------------------------------------------------------------------

def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def num(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def pct(part: float, whole: float) -> str:
    if not whole:
        return "0%"
    return f"{part * 100.0 / whole:.1f}%"


def growth(current: float, previous: float) -> str:
    if previous == 0:
        return "+100%" if current else "0%"
    change = (current - previous) * 100.0 / previous
    icon = "📈" if change > 0 else ("📉" if change < 0 else "➖")
    return f"{icon} {change:+.1f}%"


def dot(flag: bool) -> str:
    return "🟢" if flag else "🔴"


def bar(part: float, whole: float, width: int = 10) -> str:
    if not whole:
        return "▱" * width
    filled = max(0, min(width, math.ceil(part * width / whole)))
    return "▰" * filled + "▱" * (width - filled)


def uptime(seconds: float) -> str:
    seconds = int(max(0, seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def ago(stamp: str | None) -> str:
    from core.db import now_dt, parse_ts

    dt = parse_ts(stamp)
    if dt is None:
        return "never"
    delta = (now_dt() - dt).total_seconds()
    if delta < 60:
        return "just now"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


def size(bytes_: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if bytes_ < 1024 or unit == "GB":
            return f"{bytes_:.1f} {unit}"
        bytes_ /= 1024
    return f"{bytes_:.1f} GB"


def screen(title: str, lines: Iterable[str], footer: str | None = None) -> str:
    """Standard section layout: rule, title, rule, body, rule."""
    body = "\n".join(lines)
    out = f"{DIV}\n<b>{title}</b>\n{DIV}\n\n{body}\n\n{DIV}"
    if footer:
        out += f"\n<i>{footer}</i>"
    return out


def user_label(row: Any) -> str:
    name = " ".join(x for x in [row["first_name"], row["last_name"]] if x) or "Unknown"
    if row["username"]:
        return f"{esc(name)} (@{esc(row['username'])})"
    return esc(name)


# --------------------------------------------------------------------------
# keyboards
# --------------------------------------------------------------------------

def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def url_btn(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, url=url)


def keyboard(rows: Sequence[Sequence[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[r for r in rows if r])


def nav_row(back: str | None = None, home: bool = True
            ) -> list[InlineKeyboardButton]:
    row: list[InlineKeyboardButton] = []
    if back:
        row.append(btn("🔙 Back", back))
    if home:
        row.append(btn("🏠 Dashboard", cb("dash", "open")))
    return row


def cancel_row(back: str | None = None) -> list[InlineKeyboardButton]:
    row = [btn("❌ Cancel", cb("wizard", "cancel"))]
    if back:
        row.append(btn("🔙 Back", back))
    return row


def pager(prefix_cb: str, page: int, total: int, per_page: int
          ) -> list[InlineKeyboardButton]:
    """prefix_cb must be a callback string ending right before the page number."""
    pages = max(1, math.ceil(total / per_page)) if per_page else 1
    if pages <= 1:
        return []
    row = []
    if page > 0:
        row.append(btn("◀️", f"{prefix_cb}:{page - 1}"))
    row.append(btn(f"{page + 1}/{pages}", cb("noop", "x")))
    if page < pages - 1:
        row.append(btn("▶️", f"{prefix_cb}:{page + 1}"))
    return row


def confirm_screen(question: str, detail: str, confirm_cb: str, cancel_cb: str
                   ) -> tuple[str, InlineKeyboardMarkup]:
    text = screen(
        "⚠️ CONFIRMATION REQUIRED",
        [f"<b>{esc(question)}</b>", "", esc(detail), "", "This cannot be undone."],
    )
    kb = keyboard([
        [btn("✅ YES, CONTINUE", confirm_cb)],
        [btn("❌ CANCEL", cancel_cb)],
    ])
    return text, kb


def grid(items: Sequence[tuple[str, str]], columns: int = 2
         ) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(items), columns):
        rows.append([btn(t, d) for t, d in items[i:i + columns]])
    return rows


def toggle_label(text: str, on: bool) -> str:
    return f"{'🟢' if on else '🔴'} {text}"


# --------------------------------------------------------------------------
# admin wizard states
# --------------------------------------------------------------------------

class Wizard(StatesGroup):
    """One state per free-text step. Every one of them is cancellable."""

    user_search = State()
    user_message = State()

    channel_chat_id = State()
    channel_title = State()
    channel_link = State()

    message_edit = State()

    button_text = State()
    button_target = State()

    broadcast_content = State()
    broadcast_buttons = State()
    broadcast_custom_ids = State()
    broadcast_schedule = State()

    campaign_name = State()
    campaign_body = State()
    campaign_dates = State()

    admin_add = State()
    setting_value = State()
    schedule_time = State()


# ==============================================================================
# MERGED FROM telegram_admin_bot/core/security.py
# ==============================================================================





PERMISSIONS: tuple[str, ...] = (
    "dashboard", "analytics", "users", "channels", "messages", "buttons",
    "broadcast", "campaigns", "referrals", "database", "logs", "settings",
    "admins",
)

ROLES: dict[str, tuple[str, ...]] = {
    "OWNER": PERMISSIONS,
    "SUPER_ADMIN": tuple(p for p in PERMISSIONS if p != "admins") + ("admins",),
    "ADMIN": ("dashboard", "analytics", "users", "channels", "messages",
              "buttons", "broadcast", "campaigns", "referrals", "logs"),
    "MODERATOR": ("dashboard", "analytics", "users", "logs"),
}

ROLE_ICONS = {"OWNER": "👑", "SUPER_ADMIN": "🛡", "ADMIN": "🔧", "MODERATOR": "🧹"}


class Actor:
    """The admin behind the current update."""

    def __init__(self, tg_id: int, role: str, perms: frozenset[str]) -> None:
        self.tg_id = tg_id
        self.role = role
        self.perms = perms

    @property
    def is_owner(self) -> bool:
        return self.role == "OWNER"

    def can(self, perm: str) -> bool:
        return self.is_owner or perm in self.perms

    def icon(self) -> str:
        return ROLE_ICONS.get(self.role, "🔧")


async def resolve(tg_id: int) -> Actor | None:
    if tg_id in config.owner_ids:
        return Actor(tg_id, "OWNER", frozenset(PERMISSIONS))
    row = await Admins.get(tg_id)
    if row is None:
        return None
    role = row["role"] if row["role"] in ROLES else "MODERATOR"
    if row["permissions"]:
        try:
            perms = frozenset(json.loads(row["permissions"]))
        except (TypeError, ValueError):
            perms = frozenset(ROLES[role])
    else:
        perms = frozenset(ROLES[role])
    return Actor(tg_id, role, perms)


async def is_admin(tg_id: int) -> bool:
    return await resolve(tg_id) is not None


# --------------------------------------------------------------------------
# handler guard
# --------------------------------------------------------------------------

Handler = Callable[..., Awaitable[Any]]


def requires(permission: str) -> Callable[[Handler], Handler]:
    """Guard an admin handler. The event is always answered, never ignored."""

    def decorator(handler: Handler) -> Handler:
        @functools.wraps(handler)
        async def wrapper(event: Message | CallbackQuery, *args: Any, **kwargs: Any):
            user = event.from_user
            actor = await resolve(user.id) if user else None
            if actor is None or not actor.can(permission):
                if isinstance(event, CallbackQuery):
                    await event.answer("⛔️ No permission for this section.",
                                       show_alert=True)
                else:
                    await event.answer("⛔️ You do not have access to this panel.")
                return None
            kwargs["actor"] = actor
            return await handler(event, *args, **kwargs)

        return wrapper

    return decorator


# --------------------------------------------------------------------------
# audit + error capture
# --------------------------------------------------------------------------

async def audit(actor: Actor | int, action: str, target: str | None = None,
                result: str = "OK") -> None:
    """Record an admin action. Never receives a token or any secret."""
    if (await Settings.all()).get("audit_enabled", "1") != "1":
        return
    admin_id = actor.tg_id if isinstance(actor, Actor) else int(actor)
    await Audit.log(admin_id, action, target, result)


SECRET_HINTS = ("bot_token", "token=", "api_key", "authorization", "password")


def scrub(text: str) -> str:
    """Strip anything that smells like a credential before it is stored."""
    out = str(text)
    low = out.lower()
    for hint in SECRET_HINTS:
        idx = low.find(hint)
        while idx != -1:
            end = idx + len(hint)
            while end < len(out) and out[end] not in " \n\t,;)]}":
                end += 1
            out = out[:idx + len(hint)] + "***" + out[end:]
            low = out.lower()
            idx = low.find(hint, idx + len(hint) + 3)
    if config.bot_token:
        out = out.replace(config.bot_token, "***")
    return out


async def capture(kind: str, exc: BaseException | str, operation: str | None = None
                  ) -> int:
    message = exc if isinstance(exc, str) else f"{type(exc).__name__}: {exc}"
    return await Errors.log(kind, scrub(message), operation)


# --------------------------------------------------------------------------
# idle wizard expiry
# --------------------------------------------------------------------------

class StateClock:
    """Expires an abandoned wizard so nobody is stuck inside a conversation."""

    def __init__(self, ttl: int) -> None:
        self.ttl = ttl
        self._seen: dict[int, float] = {}

    def touch(self, tg_id: int) -> None:
        self._seen[tg_id] = time.monotonic()

    def expired(self, tg_id: int) -> bool:
        seen = self._seen.get(tg_id)
        return seen is not None and (time.monotonic() - seen) > self.ttl

    def clear(self, tg_id: int) -> None:
        self._seen.pop(tg_id, None)


state_clock = StateClock(config.state_ttl)


# ==============================================================================
# MERGED FROM telegram_admin_bot/services/content.py
# ==============================================================================






# --------------------------------------------------------------------------
# settings cache
# --------------------------------------------------------------------------

class SettingsCache:
    """Read-through cache. Any write invalidates it, so the panel and the user
    flow can never disagree about the current configuration."""

    def __init__(self) -> None:
        self._data: dict[str, str] | None = None
        self._lock = asyncio.Lock()

    async def load(self) -> dict[str, str]:
        if self._data is None:
            async with self._lock:
                if self._data is None:
                    self._data = await Settings.all()
        return self._data

    def invalidate(self) -> None:
        self._data = None

    async def get(self, key: str, default: str = "") -> str:
        return (await self.load()).get(key, default)

    async def flag(self, key: str, default: bool = False) -> bool:
        return (await self.get(key, "1" if default else "0")) == "1"

    async def number(self, key: str, default: int = 0) -> int:
        try:
            return int(await self.get(key, str(default)))
        except ValueError:
            return default

    async def set(self, key: str, value: str, admin_id: int | None = None,
                  section: str = "general") -> None:
        await Settings.set(key, value, admin_id, section)
        self.invalidate()

    async def toggle(self, key: str, admin_id: int | None = None) -> bool:
        new = "0" if await self.flag(key, True) else "1"
        await self.set(key, new, admin_id)
        return new == "1"


settings = SettingsCache()


# --------------------------------------------------------------------------
# message CMS
# --------------------------------------------------------------------------

async def render(key: str, **context: Any) -> tuple[str, str] | None:
    """Return (text, parse_mode) for a CMS message, or None when disabled."""
    row = await Messages.get(key)
    if row is None or not row["enabled"]:
        return None
    text = row["content"]
    for name, value in context.items():
        text = text.replace("{" + name + "}", str(value))
    return text, row["parse_mode"] or "HTML"


async def preview(key: str) -> tuple[str, str]:
    row = await Messages.get(key)
    if row is None:
        return "(missing)", "HTML"
    return row["content"], row["parse_mode"] or "HTML"


# --------------------------------------------------------------------------
# keyboard builder
# --------------------------------------------------------------------------

def decorate(row: Any) -> str:
    mark = STYLE_MARK.get(row["style"], "")
    parts = [p for p in (mark, row["emoji"], row["text"]) if p]
    return " ".join(parts)


async def build_keyboard(screen: str, extra_rows: list[list[InlineKeyboardButton]]
                         | None = None) -> InlineKeyboardMarkup | None:
    """Compose a user-facing keyboard from the buttons table."""
    rows = await Buttons.for_screen(screen)
    grouped: dict[int, list[InlineKeyboardButton]] = {}
    for row in rows:
        label = decorate(row)
        if row["kind"] == "url" and row["url"]:
            button = url_btn(label, row["url"])
        else:
            button = InlineKeyboardButton(
                text=label, callback_data=row["callback"] or "flow:noop"
            )
        grouped.setdefault(int(row["row"]), []).append(button)
    keyboard = [grouped[k] for k in sorted(grouped)]
    if extra_rows:
        keyboard.extend(extra_rows)
    if not keyboard:
        return None
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# --------------------------------------------------------------------------
# force join
# --------------------------------------------------------------------------

MEMBER_OK = {"member", "administrator", "creator"}


async def missing_channels(bot: Bot, tg_id: int) -> list[Any]:
    """Channels the user has not joined. An unreachable channel is reported
    as an error and skipped, so a misconfigured channel never blocks users."""
    if not await settings.flag("force_join_enabled", True):
        return []
    out = []
    for channel in await Channels.all(enabled_only=True):
        try:
            member = await bot.get_chat_member(channel["chat_id"], tg_id)
            joined = member.status in MEMBER_OK
        except TelegramAPIError as exc:
            await capture("CHANNEL", exc, f"get_chat_member {channel['chat_id']}")
            await Channels.update(channel["id"], last_result="unreachable")
            continue
        await Channels.update(channel["id"], last_result="ok")
        if not joined:
            out.append(channel)
    return out


async def verify(bot: Bot, tg_id: int) -> bool:
    missing = await missing_channels(bot, tg_id)
    ok = not missing
    await Users.set_force_join(tg_id, ok)
    await Events.log(tg_id, "fj_pass" if ok else "fj_fail")
    return ok


async def join_keyboard(channels: list[Any], verify_cb: str = "flow:verify"
                        ) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for channel in channels:
        link = channel["invite_link"] or f"https://t.me/{str(channel['chat_id']).lstrip('@')}"
        rows.append([url_btn(f"📢 {channel['title']}", link)])
    rows.append([InlineKeyboardButton(text="✅ I Joined", callback_data=verify_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def test_channel(bot: Bot, chat_id: str, bot_id: int) -> tuple[bool, str]:
    """Membership-verification self test used by the Channel Manager."""
    try:
        chat = await bot.get_chat(chat_id)
    except TelegramAPIError as exc:
        return False, f"Cannot read chat: {exc.message if hasattr(exc, 'message') else exc}"
    try:
        me = await bot.get_chat_member(chat_id, bot_id)
    except TelegramAPIError as exc:
        return False, f"Cannot query members: {exc}"
    if me.status not in {"administrator", "creator"}:
        return False, f"Bot is '{me.status}' in {chat.title} - promote it to admin."
    return True, f"OK - membership checks work for {chat.title}."


# ==============================================================================
# MERGED FROM telegram_admin_bot/services/engine.py
# ==============================================================================





try:  # optional - the panel degrades gracefully without it
    import psutil
except ModuleNotFoundError:  # pragma: no cover
    psutil = None


STATUS_ICON = {
    "DRAFT": "📝", "SCHEDULED": "⏰", "RUNNING": "🚀",
    "COMPLETED": "✅", "FAILED": "❌", "CANCELLED": "🚫",
}


def markup_from_json(raw: str | None) -> InlineKeyboardMarkup | None:
    """Rebuild an inline keyboard stored with a broadcast or campaign.

    Stored shape: [[{"text": "...", "url": "..."}, ...], ...]
    """
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    rows: list[list[InlineKeyboardButton]] = []
    for row in data:
        built = []
        for item in row:
            text = item.get("text") or "Open"
            if item.get("url"):
                built.append(InlineKeyboardButton(text=text, url=item["url"]))
            elif item.get("callback"):
                built.append(InlineKeyboardButton(text=text,
                                                  callback_data=item["callback"]))
        if built:
            rows.append(built)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


def parse_button_spec(raw: str) -> str:
    """Admin types 'Text | https://... ; Text2 | https://...' per line.

    Each line becomes one keyboard row. Returns JSON for storage.
    """
    rows: list[list[dict[str, str]]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        row = []
        for chunk in line.split(";"):
            if "|" not in chunk:
                continue
            text, target = chunk.split("|", 1)
            text, target = text.strip(), target.strip()
            if not text or not target:
                continue
            if target.startswith(("http://", "https://", "tg://")):
                row.append({"text": text, "url": target})
            else:
                row.append({"text": text, "callback": target})
        if row:
            rows.append(row)
    return json.dumps(rows)


# --------------------------------------------------------------------------
# broadcast engine
# --------------------------------------------------------------------------

class BroadcastEngine:
    """Sends one broadcast at a time, respecting Telegram rate limits and
    recording per-recipient outcomes back into the broadcasts row."""

    def __init__(self) -> None:
        self.bot: Bot | None = None
        self.current: int | None = None
        self._cancel = False
        self._task: asyncio.Task | None = None
        self.on_finished = None  # optional async callback(broadcast_row)

    def bind(self, bot: Bot) -> None:
        self.bot = bot

    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    def cancel(self) -> None:
        self._cancel = True

    async def start(self, broadcast_id: int) -> bool:
        if self.busy:
            return False
        self._cancel = False
        self._task = asyncio.create_task(self._run(broadcast_id))
        return True

    async def _deliver(self, tg_id: int, row: Any,
                       markup: InlineKeyboardMarkup | None) -> str:
        bot = self.bot
        assert bot is not None
        kind = row["kind"]
        text = row["body"] or ""
        mode = row["parse_mode"] or "HTML"
        try:
            if kind == "text":
                await bot.send_message(tg_id, text, parse_mode=mode,
                                       reply_markup=markup,
                                       disable_web_page_preview=True)
            elif kind == "forward":
                await bot.copy_message(tg_id, row["from_chat_id"], row["from_msg_id"],
                                       reply_markup=markup)
            elif kind == "photo":
                await bot.send_photo(tg_id, row["file_id"], caption=text,
                                     parse_mode=mode, reply_markup=markup)
            elif kind == "video":
                await bot.send_video(tg_id, row["file_id"], caption=text,
                                     parse_mode=mode, reply_markup=markup)
            elif kind == "document":
                await bot.send_document(tg_id, row["file_id"], caption=text,
                                        parse_mode=mode, reply_markup=markup)
            elif kind == "audio":
                await bot.send_audio(tg_id, row["file_id"], caption=text,
                                     parse_mode=mode, reply_markup=markup)
            elif kind == "voice":
                await bot.send_voice(tg_id, row["file_id"], caption=text,
                                     reply_markup=markup)
            else:
                return "failed"
            return "sent"
        except TelegramForbiddenError:
            await Users.set_blocked(tg_id, True, "blocked the bot")
            return "blocked"
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after + 1)
            return await self._deliver(tg_id, row, markup)
        except TelegramAPIError as exc:
            await capture("BROADCAST", exc, f"send to {tg_id}")
            return "failed"

    async def _run(self, broadcast_id: int) -> None:
        row = await Broadcasts.get(broadcast_id)
        if row is None:
            return
        self.current = broadcast_id
        targets = await Users.audience(row["audience"], row["custom_ids"])
        markup = markup_from_json(row["markup_json"])
        await Broadcasts.update(broadcast_id, status="RUNNING", started_at=now(),
                                total=len(targets), sent=0, failed=0, blocked=0)
        rate = max(1, await settings.number("broadcast_rate", config.broadcast_rate))
        delay = 1.0 / rate
        sent = failed = blocked = 0
        try:
            for index, tg_id in enumerate(targets, start=1):
                if self._cancel:
                    await Broadcasts.update(broadcast_id, status="CANCELLED",
                                            finished_at=now())
                    break
                result = await self._deliver(tg_id, row, markup)
                sent += result == "sent"
                failed += result == "failed"
                blocked += result == "blocked"
                if index % 25 == 0:
                    await Broadcasts.update(broadcast_id, sent=sent, failed=failed,
                                            blocked=blocked)
                await asyncio.sleep(delay)
            else:
                await Broadcasts.update(broadcast_id, status="COMPLETED",
                                        finished_at=now())
        except Exception as exc:  # noqa: BLE001 - the run must never kill the bot
            await capture("BROADCAST", exc, f"broadcast {broadcast_id}")
            await Broadcasts.update(broadcast_id, status="FAILED", finished_at=now())
        finally:
            await Broadcasts.update(broadcast_id, sent=sent, failed=failed,
                                    blocked=blocked)
            self.current = None
            if self.on_finished:
                final = await Broadcasts.get(broadcast_id)
                try:
                    await self.on_finished(final)
                except Exception as exc:  # noqa: BLE001
                    await capture("NOTIFY", exc, "broadcast finished hook")


engine = BroadcastEngine()


# --------------------------------------------------------------------------
# scheduler
# --------------------------------------------------------------------------

class Scheduler:
    """One lightweight loop, checked every 30 seconds. Handles scheduled
    broadcasts, recurring jobs and campaign expiry."""

    def __init__(self) -> None:
        self.bot: Bot | None = None
        self.task: asyncio.Task | None = None
        self.last_tick: float = 0.0
        self.running = False

    def start(self, bot: Bot) -> None:
        self.bot = bot
        self.running = True
        self.task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self.running = False
        if self.task:
            self.task.cancel()

    async def _loop(self) -> None:
        while self.running:
            try:
                await self.tick()
            except Exception as exc:  # noqa: BLE001
                await capture("SCHEDULER", exc, "tick")
            self.last_tick = time.time()
            await asyncio.sleep(30)

    async def tick(self) -> None:
        if not await settings.flag("scheduler_enabled", True):
            return
        for row in await Broadcasts.due():
            if not engine.busy:
                await engine.start(row["id"])
        for job in await Schedules.all(active_only=True):
            if await self._is_due(job):
                await self._fire(job)
        for campaign in await Campaigns.all():
            if campaign["status"] == "ACTIVE" and campaign["end_at"]:
                end = parse_ts(campaign["end_at"])
                if end and end < now_dt():
                    await Campaigns.update(campaign["id"], status="ENDED")

    async def _is_due(self, job: Any) -> bool:
        current = now_dt()
        last = parse_ts(job["last_run_at"])
        if job["repeat"] == "once":
            run_at = parse_ts(job["run_at"])
            return bool(run_at and run_at <= current and last is None)
        if not job["time_of_day"]:
            return False
        hour, _, minute = job["time_of_day"].partition(":")
        try:
            target = current.replace(hour=int(hour), minute=int(minute or 0),
                                     second=0, microsecond=0)
        except ValueError:
            return False
        if current < target:
            return False
        if job["repeat"] == "weekly" and job["weekday"] is not None:
            if current.weekday() != int(job["weekday"]):
                return False
        return last is None or last < target

    async def _fire(self, job: Any) -> None:
        await Schedules.update(job["id"], last_run_at=now())
        if job["kind"] in {"broadcast", "announcement"} and job["ref_id"]:
            await Broadcasts.update(job["ref_id"], status="SCHEDULED",
                                    scheduled_at=now())
        elif job["kind"] == "campaign" and job["ref_id"]:
            await Campaigns.update(job["ref_id"], status="ACTIVE")
        if job["repeat"] == "once":
            await Schedules.update(job["id"], status="DONE")


scheduler = Scheduler()


# --------------------------------------------------------------------------
# health
# --------------------------------------------------------------------------

class Health:
    def __init__(self) -> None:
        self.started = time.time()
        self.api_ok = True
        self.http_ok = False
        self.last_db_ok: str | None = None

    @property
    def uptime(self) -> float:
        return time.time() - self.started

    async def probe(self, bot: Bot) -> dict[str, Any]:
        data: dict[str, Any] = {
            "uptime": self.uptime,
            "python": platform.python_version(),
            "platform": platform.system(),
            "scheduler": scheduler.running,
            "http": self.http_ok,
            "broadcast_busy": engine.busy,
        }
        try:
            await bot.get_me()
            data["api"] = True
            self.api_ok = True
        except Exception as exc:  # noqa: BLE001
            data["api"] = False
            self.api_ok = False
            await capture("API", exc, "get_me")
        data["db"] = await db.ping()
        if data["db"]:
            self.last_db_ok = now()
        data["last_db_ok"] = self.last_db_ok
        try:
            data["db_size"] = await db.size_bytes()
        except Exception:  # noqa: BLE001
            data["db_size"] = 0
        if psutil is not None:
            try:
                proc = psutil.Process()
                data["cpu"] = psutil.cpu_percent(interval=None)
                data["memory"] = proc.memory_info().rss
                data["memory_pct"] = psutil.virtual_memory().percent
            except Exception:  # noqa: BLE001
                pass
        return data


health = Health()


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/admin_dashboard.py
# ==============================================================================




admin_dashboard_router = Router(name="admin_dashboard")


# --------------------------------------------------------------------------
# shared screen renderer, used by every admin module
# --------------------------------------------------------------------------

async def show(event: Message | CallbackQuery, text: str,
               markup: InlineKeyboardMarkup | None = None,
               alert: str | None = None) -> None:
    """Render an admin screen in place, tolerating identical repaints."""
    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, reply_markup=markup,
                                          parse_mode="HTML",
                                          disable_web_page_preview=True)
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc):
                await event.message.answer(text, reply_markup=markup,
                                           parse_mode="HTML",
                                           disable_web_page_preview=True)
        await event.answer(alert or "")
    else:
        await event.answer(text, reply_markup=markup, parse_mode="HTML",
                           disable_web_page_preview=True)


# --------------------------------------------------------------------------
# dashboard
# --------------------------------------------------------------------------

QUICK_ACTIONS = [
    ("📊 Analytics", cb("stats", "open", "today")),
    ("👥 Users", cb("users", "open", "all", 0)),
    ("📢 Broadcast", cb("bc", "open")),
    ("📡 Channels", cb("chan", "open")),
    ("✏️ Messages", cb("msg", "open")),
    ("🔘 Buttons", cb("btn", "open")),
    ("🎯 Campaigns", cb("camp", "open")),
    ("🎁 Referrals", cb("ref", "open")),
    ("👑 Admins", cb("adm", "open")),
    ("⚙️ Settings", cb("set", "open")),
    ("💾 Database", cb("dbm", "open")),
    ("📋 Logs", cb("log", "open", 0)),
]

PERMISSION_OF = {
    "stats": "analytics", "users": "users", "bc": "broadcast", "chan": "channels",
    "msg": "messages", "btn": "buttons", "camp": "campaigns", "ref": "referrals",
    "adm": "admins", "set": "settings", "dbm": "database", "log": "logs",
}


async def dashboard_screen(actor: Actor) -> tuple[str, InlineKeyboardMarkup]:
    stats = await Stats.overview()
    enabled = await settings.flag("bot_enabled", True)
    maintenance = await settings.flag("maintenance_mode", False)
    status = "🟠 Maintenance" if maintenance else (
        "🟢 Online" if enabled else "🔴 Disabled")

    lines = [
        f"🤖 <b>Bot Status:</b> {status}",
        f"⏱ <b>Uptime:</b> {uptime(health.uptime)}",
        "",
        f"👥 <b>Total Users:</b> {num(stats['total_users'])}",
        f"🟢 <b>Active Users:</b> {num(stats['active_users'])}",
        f"🚫 <b>Blocked Users:</b> {num(stats['blocked_users'])}",
        f"📢 <b>Channels:</b> {num(stats['channels'])} "
        f"({num(stats['channels_enabled'])} enabled)",
        f"📨 <b>Broadcasts:</b> {num(stats['broadcasts'])}",
        f"📈 <b>New Today:</b> {num(stats['new_today'])} "
        f"({growth(stats['new_today'], stats['new_yesterday'])})",
    ]
    text = screen("🛡 ADMIN DASHBOARD", lines,
                  footer=f"Signed in as {actor.icon()} {actor.role}")

    allowed = [(label, data) for label, data in QUICK_ACTIONS
               if actor.can(PERMISSION_OF[parse_cb(data)[0]])]
    rows = grid(allowed, columns=2)
    rows.append([btn("🔧 Bot Controls", cb("ctl", "open")),
                 btn("🩺 Health", cb("hp", "open"))])
    rows.append([btn("🔄 Refresh Dashboard", cb("dash", "open"))])
    return text, keyboard(rows)


@admin_dashboard_router.message(Command("admin"))
@requires("dashboard")
async def cmd_admin(message: Message, actor: Actor, **_) -> None:
    text, markup = await dashboard_screen(actor)
    await show(message, text, markup)
    await audit(actor, "Opened dashboard")


@admin_dashboard_router.callback_query(F.data.startswith("a:dash:"))
@requires("dashboard")
async def cb_dashboard(call: CallbackQuery, actor: Actor, **_) -> None:
    text, markup = await dashboard_screen(actor)
    await show(call, text, markup, alert="🔄 Refreshed")


@admin_dashboard_router.callback_query(F.data.startswith("a:noop:"))
async def cb_noop(call: CallbackQuery) -> None:
    await call.answer()


# --------------------------------------------------------------------------
# analytics
# --------------------------------------------------------------------------

@admin_dashboard_router.callback_query(F.data.startswith("a:stats:"))
@requires("analytics")
async def cb_stats(call: CallbackQuery, actor: Actor, **_) -> None:
    _, _, args = parse_cb(call.data)
    period = args[0] if args else "today"
    data = await Stats.analytics(period)
    from core.db import Referrals

    top = await Referrals.top(5)
    clicks, claims = data["claim_clicks"], data["claim_success"]

    lines = [
        f"<b>📈 User Growth</b>  ·  {PERIOD_LABELS.get(period, period)}",
        f"Selected period: <b>+{num(data['period_users'])}</b>",
        f"Today: +{num(data['new_today'])}   Yesterday: +{num(data['new_yesterday'])}",
        f"Growth: {growth(data['new_today'], data['new_yesterday'])}",
        f"This week: +{num(data['new_week'])}   This month: +{num(data['new_month'])}",
        "",
        "<b>👥 Audience</b>",
        f"Total: {num(data['total_users'])}   Active: {num(data['active_users'])}",
        f"Returning: {num(data['returning_users'])}   "
        f"Blocked: {num(data['blocked_users'])}",
        "",
        "<b>📢 Force Join</b>",
        f"Completed: {num(data['force_join_ok'])}  "
        f"({pct(data['force_join_ok'], data['total_users'])})",
        f"Failed checks: {num(data['force_join_fail'])}",
        "",
        "<b>🎁 Conversion</b>",
        f"Claim clicks: {num(clicks)}   Successful: {num(claims)}",
        f"Conversion: {bar(claims, clicks or 1)} {pct(claims, clicks or 1)}",
        f"Referred users: {num(data['referral_users'])}",
        "",
        "<b>📨 Delivery</b>",
        f"Broadcast delivery rate: {data['delivery_rate']:.1f}%",
    ]
    if top:
        lines += ["", "<b>🏆 Top Referrers</b>"]
        for i, row in enumerate(top, start=1):
            handle = f"@{row['username']}" if row["username"] else str(row["tg_id"])
            lines.append(f"{i}. {esc(handle)} — {num(row['referral_count'])}")

    periods = [(("✅ " if p == period else "") + label, cb("stats", "open", p))
               for p, label in PERIOD_LABELS.items()]
    rows = grid(periods, columns=3)
    rows.append([btn("📤 Export Users", cb("users", "export")),
                 btn("🔄 Refresh", cb("stats", "open", period))])
    rows.append(nav_row(back=cb("dash", "open"), home=False))
    await show(call, screen("📊 ANALYTICS", lines), keyboard(rows))


# --------------------------------------------------------------------------
# system health
# --------------------------------------------------------------------------

@admin_dashboard_router.callback_query(F.data.startswith("a:hp:"))
@requires("dashboard")
async def cb_health(call: CallbackQuery, bot: Bot, actor: Actor, **_) -> None:
    data = await health.probe(bot)
    last_error = await Errors.last()
    lines = [
        f"{dot(data['api'])} <b>Telegram API</b>",
        f"{dot(data['db'])} <b>Database</b>",
        f"{dot(True)} <b>Bot Process</b>",
        f"{dot(data['http'])} <b>Health Server</b>",
        f"{dot(data['scheduler'])} <b>Scheduler</b>",
        "",
        f"⏱ Uptime: {uptime(data['uptime'])}",
        f"💾 Database size: {size(data['db_size'])}",
        f"🐍 Python {data['python']} on {data['platform']}",
    ]
    if "cpu" in data:
        lines.append(f"🧮 CPU: {data['cpu']:.0f}%   "
                     f"RAM: {size(data['memory'])} ({data['memory_pct']:.0f}% system)")
    else:
        lines.append("🧮 CPU/RAM: not available on this host")
    lines.append(f"🚀 Broadcast worker: {'running' if data['broadcast_busy'] else 'idle'}")
    lines.append(f"✅ Last DB operation: {data['last_db_ok'] or 'n/a'}")
    if last_error:
        lines += ["", f"🚨 Last error: <code>{esc(last_error['kind'])}</code> — "
                      f"{esc(last_error['message'][:120])}"]
    rows = [
        [btn("🚨 Error Center", cb("err", "open", 0)),
         btn("🔄 Refresh", cb("hp", "open"))],
        nav_row(back=cb("dash", "open"), home=False),
    ]
    await show(call, screen("🩺 SYSTEM HEALTH", lines), keyboard(rows))


# --------------------------------------------------------------------------
# bot controls
# --------------------------------------------------------------------------

@admin_dashboard_router.callback_query(F.data.startswith("a:ctl:"))
@requires("settings")
async def cb_controls(call: CallbackQuery, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)
    note = None

    if action == "enable":
        await settings.set("bot_enabled", "1", actor.tg_id)
        await audit(actor, "Enabled bot")
        note = "🟢 Bot enabled"
    elif action == "confirm_disable":
        from core.ui import confirm_screen

        text, markup = confirm_screen(
            "Disable the bot?",
            "Users will stop receiving replies. Admins keep full access.",
            cb("ctl", "disable"), cb("ctl", "open"))
        await show(call, text, markup)
        return
    elif action == "disable":
        await settings.set("bot_enabled", "0", actor.tg_id)
        await audit(actor, "Disabled bot")
        note = "🔴 Bot disabled"
    elif action == "maint":
        on = await settings.toggle("maintenance_mode", actor.tg_id)
        await audit(actor, f"Maintenance mode {'on' if on else 'off'}")
        note = f"🛠 Maintenance {'enabled' if on else 'disabled'}"
    elif action == "reload":
        settings.invalidate()
        await audit(actor, "Reloaded configuration")
        note = "🔄 Configuration reloaded"
    elif action == "cleartmp":
        from core.db import Audit

        removed = await Audit.cleanup(await settings.number("log_retention_days", 30))
        await audit(actor, "Cleared temporary data", f"{removed} rows")
        note = f"🧹 Removed {removed} old rows"

    enabled = await settings.flag("bot_enabled", True)
    maintenance = await settings.flag("maintenance_mode", False)
    lines = [
        f"Bot: {'🟢 enabled' if enabled else '🔴 disabled'}",
        f"Maintenance: {'🛠 on' if maintenance else '⚪️ off'}",
        "",
        "Maintenance mode pauses the public flow but never blocks admins.",
    ]
    rows = [
        [btn("🟢 Enable Bot", cb("ctl", "enable")),
         btn("🔴 Disable Bot", cb("ctl", "confirm_disable"))],
        [btn(("🛠 Maintenance: ON" if maintenance else "🛠 Maintenance: OFF"),
             cb("ctl", "maint"))],
        [btn("🔄 Reload Config", cb("ctl", "reload")),
         btn("📊 Refresh Stats", cb("dash", "open"))],
        [btn("🧹 Clear Temporary Data", cb("ctl", "cleartmp"))],
        nav_row(back=cb("dash", "open"), home=False),
    ]
    await show(call, screen("🔧 BOT CONTROL CENTER", lines), keyboard(rows), note)


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/admin_users.py
# ==============================================================================





admin_users_router = Router(name="admin_users")

LISTS = {
    "all": "📋 All Users",
    "active": "🟢 Active Users",
    "blocked": "🚫 Blocked Users",
    "new": "🆕 New Today",
}
PER_PAGE = 8


# --------------------------------------------------------------------------
# user list + search
# --------------------------------------------------------------------------

async def user_list_screen(kind: str, page: int) -> tuple[str, object]:
    rows, total = await Users.page(kind, page, PER_PAGE)
    lines = [f"{LISTS.get(kind, kind)} — <b>{num(total)}</b> found", ""]
    if not rows:
        lines.append("<i>No users match this filter yet.</i>")
    for row in rows:
        flags = "".join([
            "🚫" if row["is_blocked"] else "",
            "✅" if row["force_join_ok"] else "",
        ])
        lines.append(f"<code>{row['tg_id']}</code> · {user_label(row)} {flags}")
        lines.append(f"    joined {ago(row['joined_at'])} · seen {ago(row['last_seen_at'])}")

    buttons = [(f"👤 {row['tg_id']}", cb("users", "view", row["tg_id"]))
               for row in rows]
    kb = grid(buttons, columns=3)
    page_row = pager(cb("users", "open", kind), page, total, PER_PAGE)
    if page_row:
        kb.append(page_row)
    kb.append([btn("🔍 Search", cb("users", "search")),
               btn("📤 Export", cb("users", "export"))])
    kb.append([btn(LISTS[k], cb("users", "open", k, 0))
               for k in ("all", "active") if k != kind][:2])
    kb.append([btn(LISTS[k], cb("users", "open", k, 0))
               for k in ("blocked", "new") if k != kind][:2])
    kb.append(nav_row(back=cb("dash", "open"), home=False))
    return screen("👥 USER MANAGEMENT", lines), keyboard(kb)


async def user_card(tg_id: int) -> tuple[str, object] | None:
    row = await Users.get(tg_id)
    if row is None:
        return None
    referrals = int(row["referral_count"])
    lines = [
        f"👤 <b>Name:</b> {user_label(row)}",
        f"🔹 <b>Username:</b> "
        f"{'@' + esc(row['username']) if row['username'] else '—'}",
        f"🆔 <b>Telegram ID:</b> <code>{row['tg_id']}</code>",
        f"📅 <b>Joined:</b> {esc(row['joined_at'])} ({ago(row['joined_at'])})",
        f"🕐 <b>Last activity:</b> {ago(row['last_seen_at'])}",
        f"📊 <b>Referrals:</b> {num(referrals)}",
        f"🎁 <b>Claims:</b> {num(row['claim_count'])}",
        f"📢 <b>Force join:</b> "
        f"{'✅ completed' if row['force_join_ok'] else '❌ not completed'}",
        f"🚫 <b>Status:</b> "
        f"{'blocked — ' + esc(row['blocked_reason'] or 'no reason') if row['is_blocked'] else '🟢 active'}",
    ]
    if row["referred_by"]:
        lines.append(f"↩️ <b>Referred by:</b> <code>{row['referred_by']}</code>")
    kb = [
        [btn("✅ Unblock" if row["is_blocked"] else "🚫 Block",
             cb("users", "unblock" if row["is_blocked"] else "block", tg_id))],
        [btn("📨 Send Message", cb("users", "msg", tg_id)),
         btn("🔎 Activity", cb("users", "activity", tg_id))],
        [btn("🗑 Delete", cb("users", "confirm_del", tg_id))],
        nav_row(back=cb("users", "open", "all", 0)),
    ]
    return screen("👤 USER DETAILS", lines), keyboard(kb)


@admin_users_router.callback_query(F.data.startswith("a:users:"))
@requires("users")
async def cb_users(call: CallbackQuery, state: FSMContext, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        kind = args[0] if args else "all"
        text, markup = await user_list_screen(kind, int_arg(args, 1))
        await show(call, text, markup)
        return

    if action == "search":
        await state.set_state(Wizard.user_search)
        await show(call, screen("🔍 SEARCH USER", [
            "Send a Telegram ID, a @username or part of a name.",
        ]), keyboard([cancel_row(cb("users", "open", "all", 0))]))
        return

    if action == "export":
        rows = await Users.export_rows()
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["tg_id", "username", "first_name", "last_name", "joined_at",
                         "last_seen_at", "is_blocked", "force_join_ok",
                         "referral_count", "claim_count"])
        for row in rows:
            writer.writerow([row[k] for k in row.keys()])
        file = BufferedInputFile(buffer.getvalue().encode("utf-8"),
                                 filename=f"users-{now()[:10]}.csv")
        await call.message.answer_document(file, caption=f"📤 {num(len(rows))} users")
        await audit(actor, "Exported users", f"{len(rows)} rows")
        await call.answer("📤 Exported")
        return

    tg_id = int_arg(args, 0)

    if action == "view":
        card = await user_card(tg_id)
        if card is None:
            await call.answer("User not found.", show_alert=True)
            return
        await show(call, *card)
        return

    if action in {"block", "unblock"}:
        await Users.set_blocked(tg_id, action == "block", "blocked by admin")
        await audit(actor, f"{action.capitalize()}ed user", str(tg_id))
        card = await user_card(tg_id)
        await show(call, *card, alert="✅ Updated")
        return

    if action == "confirm_del":
        text, markup = confirm_screen(
            "Delete this user?", f"User {tg_id} will be removed from the database.",
            cb("users", "delete", tg_id), cb("users", "view", tg_id))
        await show(call, text, markup)
        return

    if action == "delete":
        await Users.delete(tg_id)
        await audit(actor, "Deleted user", str(tg_id))
        text, markup = await user_list_screen("all", 0)
        await show(call, text, markup, alert="🗑 Deleted")
        return

    if action == "msg":
        await state.set_state(Wizard.user_message)
        await state.update_data(target=tg_id)
        await show(call, screen("📨 SEND MESSAGE", [
            f"Recipient: <code>{tg_id}</code>", "",
            "Send the text you want delivered. HTML formatting is supported.",
        ]), keyboard([cancel_row(cb("users", "view", tg_id))]))
        return

    if action == "activity":
        from core.db import db

        rows = await db.fetchall(
            "SELECT event, created_at FROM events WHERE tg_id=?"
            " ORDER BY id DESC LIMIT 15", (tg_id,))
        lines = [f"Last {len(rows)} events for <code>{tg_id}</code>", ""]
        lines += [f"• <code>{esc(r['event'])}</code> — {ago(r['created_at'])}"
                  for r in rows] or ["<i>No activity recorded.</i>"]
        await show(call, screen("🔎 USER ACTIVITY", lines),
                   keyboard([nav_row(back=cb("users", "view", tg_id))]))


@admin_users_router.message(Wizard.user_search)
@requires("users")
async def do_search(message: Message, state: FSMContext, actor: Actor, **_) -> None:
    await state.clear()
    rows = await Users.search(message.text or "")
    if not rows:
        await show(message, screen("🔍 SEARCH RESULT", ["<i>Nothing found.</i>"]),
                   keyboard([nav_row(back=cb("users", "open", "all", 0))]))
        return
    if len(rows) == 1:
        await show(message, *(await user_card(rows[0]["tg_id"])))
        return
    lines = [f"<b>{len(rows)}</b> matches", ""]
    lines += [f"<code>{r['tg_id']}</code> · {user_label(r)}" for r in rows[:20]]
    kb = grid([(f"👤 {r['tg_id']}", cb("users", "view", r["tg_id"]))
               for r in rows[:12]], columns=3)
    kb.append(nav_row(back=cb("users", "open", "all", 0)))
    await show(message, screen("🔍 SEARCH RESULT", lines), keyboard(kb))


@admin_users_router.message(Wizard.user_message)
@requires("users")
async def do_direct_message(message: Message, state: FSMContext, bot: Bot,
                            actor: Actor, **_) -> None:
    data = await state.get_data()
    await state.clear()
    target = int(data.get("target", 0))
    try:
        await bot.send_message(target, message.html_text, parse_mode="HTML")
        await audit(actor, "Messaged user", str(target))
        note = f"✅ Delivered to <code>{target}</code>"
    except Exception as exc:  # noqa: BLE001
        from core.security import capture

        await capture("ADMIN_DM", exc, f"send to {target}")
        note = f"❌ Could not deliver to <code>{target}</code>"
    await show(message, screen("📨 SEND MESSAGE", [note]),
               keyboard([nav_row(back=cb("users", "view", target))]))


# --------------------------------------------------------------------------
# admin & role management
# --------------------------------------------------------------------------

async def admins_screen() -> tuple[str, object]:
    rows = await Admins.all()
    lines = ["Roles: OWNER · SUPER_ADMIN · ADMIN · MODERATOR", ""]
    if not rows:
        lines.append("<i>No extra admins. Owners come from the environment.</i>")
    for row in rows:
        icon = ROLE_ICONS.get(row["role"], "🔧")
        custom = " (custom permissions)" if row["permissions"] else ""
        lines.append(f"{icon} <code>{row['tg_id']}</code> — {esc(row['role'])}{custom}")
    kb = grid([(f"{ROLE_ICONS.get(r['role'], '🔧')} {r['tg_id']}",
                cb("adm", "view", r["tg_id"])) for r in rows], columns=2)
    kb.append([btn("➕ Add Admin", cb("adm", "add"))])
    kb.append(nav_row(back=cb("dash", "open"), home=False))
    return screen("👑 ADMIN MANAGER", lines), keyboard(kb)


async def admin_card(tg_id: int) -> tuple[str, object] | None:
    row = await Admins.get(tg_id)
    if row is None:
        return None
    actor = await resolve(tg_id)
    perms = sorted(actor.perms) if actor else []
    lines = [
        f"🆔 <code>{tg_id}</code>",
        f"{ROLE_ICONS.get(row['role'], '🔧')} <b>Role:</b> {esc(row['role'])}",
        f"📅 <b>Added:</b> {esc(row['added_at'])}",
        "",
        "<b>Permissions</b> (tap to toggle)",
        ", ".join(perms) or "none",
    ]
    kb = [[btn(f"{ROLE_ICONS[r]} {r}", cb("adm", "role", tg_id, r))]
          for r in ROLES if r != "OWNER"]
    kb += grid([(toggle_label(p, p in perms), cb("adm", "perm", tg_id, p))
                for p in PERMISSIONS], columns=2)
    kb.append([btn("🗑 Remove Admin", cb("adm", "confirm_rm", tg_id))])
    kb.append(nav_row(back=cb("adm", "open")))
    return screen("👑 ADMIN DETAILS", lines), keyboard(kb)


@admin_users_router.callback_query(F.data.startswith("a:adm:"))
@requires("admins")
async def cb_admins(call: CallbackQuery, state: FSMContext, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        await show(call, *(await admins_screen()))
        return

    if action == "add":
        await state.set_state(Wizard.admin_add)
        await show(call, screen("➕ ADD ADMIN", [
            "Send the Telegram ID of the new admin.",
            "They start as <b>MODERATOR</b>; change the role afterwards.",
        ]), keyboard([cancel_row(cb("adm", "open"))]))
        return

    tg_id = int_arg(args, 0)

    if action == "view":
        card = await admin_card(tg_id)
        if card is None:
            await call.answer("Not an admin.", show_alert=True)
            return
        await show(call, *card)
        return

    if action == "role":
        role = args[1] if len(args) > 1 else "MODERATOR"
        if role in ROLES:
            await Admins.upsert(tg_id, role, actor.tg_id)
            await Admins.set_permissions(tg_id, None)
            await audit(actor, "Changed admin role", f"{tg_id} → {role}")
        await show(call, *(await admin_card(tg_id)), alert="✅ Role updated")
        return

    if action == "perm":
        perm = args[1] if len(args) > 1 else ""
        target = await resolve(tg_id)
        if target and perm in PERMISSIONS:
            perms = set(target.perms)
            perms.symmetric_difference_update({perm})
            await Admins.set_permissions(tg_id, sorted(perms))
            await audit(actor, "Changed admin permissions", f"{tg_id}:{perm}")
        await show(call, *(await admin_card(tg_id)), alert="✅ Permissions updated")
        return

    if action == "confirm_rm":
        text, markup = confirm_screen(
            "Remove this admin?", f"Admin {tg_id} loses all panel access.",
            cb("adm", "remove", tg_id), cb("adm", "view", tg_id))
        await show(call, text, markup)
        return

    if action == "remove":
        await Admins.remove(tg_id)
        await audit(actor, "Removed admin", str(tg_id))
        await show(call, *(await admins_screen()), alert="🗑 Removed")


@admin_users_router.message(Wizard.admin_add)
@requires("admins")
async def do_add_admin(message: Message, state: FSMContext, actor: Actor, **_) -> None:
    await state.clear()
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await show(message, screen("➕ ADD ADMIN", ["❌ That is not a Telegram ID."]),
                   keyboard([nav_row(back=cb("adm", "open"))]))
        return
    await Admins.upsert(int(raw), "MODERATOR", actor.tg_id)
    await audit(actor, "Added admin", raw)
    await show(message, *(await admin_card(int(raw))))


# --------------------------------------------------------------------------
# referral manager
# --------------------------------------------------------------------------

@admin_users_router.callback_query(F.data.startswith("a:ref:"))
@requires("referrals")
async def cb_referrals(call: CallbackQuery, bot: Bot, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "toggle":
        on = await settings.toggle("referral_enabled", actor.tg_id)
        await audit(actor, f"Referrals {'enabled' if on else 'disabled'}")
    elif action == "selfblock":
        on = await settings.toggle("referral_self_block", actor.tg_id)
        await audit(actor, f"Anti-self-referral {'on' if on else 'off'}")

    stats = await Referrals.stats()
    top = await Referrals.top(10)
    me = await bot.get_me()
    enabled = await settings.flag("referral_enabled", True)
    lines = [
        f"Status: {'🟢 enabled' if enabled else '🔴 disabled'}",
        f"Reward: {esc(await settings.get('referral_reward', '1'))}",
        f"Minimum requirement: {esc(await settings.get('referral_min_joins', '1'))}"
        " completed join(s)",
        "",
        f"Total referrals: <b>{num(stats['total'])}</b>",
        f"Valid: {num(stats['valid'])}   Rejected: {num(stats['blocked'])}",
        f"Distinct referrers: {num(stats['referrers'])}",
        "",
        f"Link format: <code>https://t.me/{me.username}?start=ref_&lt;user_id&gt;</code>",
        "",
        "<b>🏆 Leaderboard</b>",
    ]
    if top:
        for i, row in enumerate(top, start=1):
            handle = f"@{row['username']}" if row["username"] else str(row["tg_id"])
            medal = ["🥇", "🥈", "🥉"][i - 1] if i <= 3 else f"{i}."
            lines.append(f"{medal} {esc(handle)} — {num(row['referral_count'])}")
    else:
        lines.append("<i>No referrals recorded yet.</i>")

    kb = [
        [btn(toggle_label("Referrals", enabled), cb("ref", "toggle"))],
        [btn(toggle_label("Anti-self-referral",
                          await settings.flag("referral_self_block", True)),
             cb("ref", "selfblock"))],
        [btn("🎁 Reward", cb("set", "edit", "referral_reward")),
         btn("📏 Minimum", cb("set", "edit", "referral_min_joins"))],
        nav_row(back=cb("dash", "open"), home=False),
    ]
    await show(call, screen("🎁 REFERRAL MANAGER", lines), keyboard(kb))


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/admin_content.py
# ==============================================================================




admin_content_router = Router(name="admin_content")


# --------------------------------------------------------------------------
# channel manager
# --------------------------------------------------------------------------

async def channels_screen() -> tuple[str, object]:
    rows = await Channels.all()
    total, on, off = await Channels.counts()
    lines = [f"Total: <b>{num(total)}</b>   Enabled: {num(on)}   Disabled: {num(off)}",
             ""]
    if not rows:
        lines.append("<i>No channels yet. Add one to enable force join.</i>")
    for row in rows:
        state = "🟢" if row["enabled"] else "🔴"
        link = row["invite_link"] or f"https://t.me/{str(row['chat_id']).lstrip('@')}"
        lines += [
            f"{state} <b>{esc(row['title'])}</b>",
            f"    🆔 <code>{esc(row['chat_id'])}</code>",
            f"    🔗 {esc(link)}",
            f"    🔍 last check: {esc(row['last_result'] or 'never')}"
            f" ({ago(row['last_check'])})",
        ]
    kb = grid([(f"📢 {r['title'][:18]}", cb("chan", "view", r["id"])) for r in rows],
              columns=2)
    kb.append([btn("➕ Add Channel", cb("chan", "add")),
               btn("📊 Statistics", cb("chan", "stats"))])
    kb.append(nav_row(back=cb("dash", "open"), home=False))
    return screen("📢 CHANNEL MANAGER", lines), keyboard(kb)


async def channel_card(cid: int) -> tuple[str, object] | None:
    row = await Channels.get(cid)
    if row is None:
        return None
    lines = [
        f"📢 <b>{esc(row['title'])}</b>",
        f"🆔 <code>{esc(row['chat_id'])}</code>",
        f"🔗 {esc(row['invite_link'] or '—')}",
        f"🟢 <b>Status:</b> {'enabled' if row['enabled'] else 'disabled'}",
        f"↕️ <b>Order:</b> {row['position']}",
        f"🔍 <b>Last verification:</b> {esc(row['last_result'] or 'never')}"
        f" ({ago(row['last_check'])})",
    ]
    kb = [
        [btn("🔄 Enable" if not row["enabled"] else "⏸ Disable",
             cb("chan", "toggle", cid)),
         btn("🔍 Test Membership", cb("chan", "test", cid))],
        [btn("✏️ Title", cb("chan", "edit", cid, "title")),
         btn("🔗 Link", cb("chan", "edit", cid, "link"))],
        [btn("⬆️ Move Up", cb("chan", "up", cid)),
         btn("⬇️ Move Down", cb("chan", "down", cid))],
        [btn("🗑 Delete", cb("chan", "confirm_del", cid))],
        nav_row(back=cb("chan", "open")),
    ]
    return screen("📢 CHANNEL", lines), keyboard(kb)


@admin_content_router.callback_query(F.data.startswith("a:chan:"))
@requires("channels")
async def cb_channels(call: CallbackQuery, state: FSMContext, bot: Bot,
                      actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        await show(call, *(await channels_screen()))
        return

    if action == "add":
        await state.set_state(Wizard.channel_chat_id)
        await show(call, screen("➕ ADD CHANNEL", [
            "Send the channel <b>@username</b> or numeric chat id "
            "(for example <code>-1001234567890</code>).",
            "",
            "The bot must already be an administrator there.",
        ]), keyboard([cancel_row(cb("chan", "open"))]))
        return

    if action == "stats":
        rows = await Channels.all()
        lines = ["Verification results from the most recent membership checks.", ""]
        for row in rows:
            lines.append(f"{'🟢' if row['enabled'] else '🔴'} {esc(row['title'])} — "
                         f"{esc(row['last_result'] or 'never checked')}")
        await show(call, screen("📊 CHANNEL STATISTICS", lines or ["<i>Empty.</i>"]),
                   keyboard([nav_row(back=cb("chan", "open"))]))
        return

    cid = int_arg(args, 0)

    if action == "view":
        card = await channel_card(cid)
        if card is None:
            await call.answer("Channel not found.", show_alert=True)
            return
        await show(call, *card)
        return

    if action == "toggle":
        row = await Channels.get(cid)
        await Channels.update(cid, enabled=0 if row["enabled"] else 1)
        await audit(actor, "Toggled channel", row["chat_id"])
        await show(call, *(await channel_card(cid)), alert="✅ Updated")
        return

    if action == "test":
        row = await Channels.get(cid)
        me = await bot.get_me()
        ok, detail = await test_channel(bot, row["chat_id"], me.id)
        from core.db import now

        await Channels.update(cid, last_check=now(),
                              last_result="ok" if ok else "failed")
        await audit(actor, "Tested channel", row["chat_id"], "OK" if ok else "FAIL")
        await show(call, screen("🔍 MEMBERSHIP TEST", [
            f"{'✅' if ok else '❌'} {esc(detail)}",
        ]), keyboard([nav_row(back=cb("chan", "view", cid))]))
        return

    if action in {"up", "down"}:
        await Channels.move(cid, -1 if action == "up" else 1)
        await show(call, *(await channels_screen()), alert="↕️ Reordered")
        return

    if action == "edit":
        field = args[1] if len(args) > 1 else "title"
        await state.set_state(Wizard.channel_title if field == "title"
                              else Wizard.channel_link)
        await state.update_data(channel_id=cid)
        await show(call, screen("✏️ EDIT CHANNEL", [
            f"Send the new {field}.",
        ]), keyboard([cancel_row(cb("chan", "view", cid))]))
        return

    if action == "confirm_del":
        row = await Channels.get(cid)
        text, markup = confirm_screen(
            "Delete this channel?",
            f"{row['title']} will no longer be required for force join.",
            cb("chan", "delete", cid), cb("chan", "view", cid))
        await show(call, text, markup)
        return

    if action == "delete":
        row = await Channels.get(cid)
        await Channels.delete(cid)
        await audit(actor, "Deleted channel", row["chat_id"] if row else str(cid))
        await show(call, *(await channels_screen()), alert="🗑 Deleted")


@admin_content_router.message(Wizard.channel_chat_id)
@requires("channels")
async def do_add_channel(message: Message, state: FSMContext, bot: Bot,
                         actor: Actor, **_) -> None:
    raw = (message.text or "").strip()
    try:
        chat = await bot.get_chat(raw)
    except Exception as exc:  # noqa: BLE001
        from core.security import capture

        await capture("CHANNEL", exc, f"get_chat {raw}")
        await show(message, screen("➕ ADD CHANNEL", [
            "❌ The bot cannot see that chat.",
            "Add the bot as an administrator first, then try again.",
        ]), keyboard([cancel_row(cb("chan", "open"))]))
        return
    await state.clear()
    link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username
                                else None)
    cid = await Channels.add(str(chat.id), chat.title or raw, link, actor.tg_id)
    await audit(actor, "Added channel", str(chat.id))
    await show(message, *(await channel_card(cid)))


@admin_content_router.message(Wizard.channel_title)
@requires("channels")
async def do_edit_title(message: Message, state: FSMContext, actor: Actor, **_) -> None:
    data = await state.get_data()
    await state.clear()
    cid = int(data.get("channel_id", 0))
    await Channels.update(cid, title=(message.text or "").strip())
    await audit(actor, "Edited channel title", str(cid))
    await show(message, *(await channel_card(cid)))


@admin_content_router.message(Wizard.channel_link)
@requires("channels")
async def do_edit_link(message: Message, state: FSMContext, actor: Actor, **_) -> None:
    data = await state.get_data()
    await state.clear()
    cid = int(data.get("channel_id", 0))
    await Channels.update(cid, invite_link=(message.text or "").strip())
    await audit(actor, "Edited channel link", str(cid))
    await show(message, *(await channel_card(cid)))


# --------------------------------------------------------------------------
# message CMS
# --------------------------------------------------------------------------

@admin_content_router.callback_query(F.data.startswith("a:msg:"))
@requires("messages")
async def cb_messages(call: CallbackQuery, state: FSMContext, actor: Actor,
                      **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        categories = await Messages.categories()
        lines = ["Every text the bot sends lives here.",
                 "Edits apply to the next message the bot sends — no restart.", ""]
        for category in categories:
            rows = await Messages.by_category(category)
            enabled = sum(1 for r in rows if r["enabled"])
            lines.append(f"• <b>{esc(category)}</b> — {len(rows)} message(s), "
                         f"{enabled} enabled")
        kb = grid([(f"📁 {c}", cb("msg", "cat", c)) for c in categories], columns=2)
        kb.append(nav_row(back=cb("dash", "open"), home=False))
        await show(call, screen("✏️ MESSAGE MANAGER", lines), keyboard(kb))
        return

    if action == "cat":
        category = args[0] if args else "START"
        rows = await Messages.by_category(category)
        lines = [f"Category: <b>{esc(category)}</b>", ""]
        for row in rows:
            lines.append(f"{'🟢' if row['enabled'] else '🔴'} <b>{esc(row['name'])}</b> "
                         f"— <code>{esc(row['key'])}</code>")
            lines.append(f"    edited {ago(row['updated_at'])}"
                         + (f" by <code>{row['updated_by']}</code>"
                            if row["updated_by"] else ""))
        kb = grid([(f"✏️ {r['name'][:18]}", cb("msg", "view", r["key"]))
                   for r in rows], columns=2)
        kb.append(nav_row(back=cb("msg", "open")))
        await show(call, screen("✏️ MESSAGES", lines), keyboard(kb))
        return

    key = args[0] if args else ""
    row = await Messages.get(key)
    if row is None:
        await call.answer("Message not found.", show_alert=True)
        return

    if action == "toggle":
        await Messages.update(key, actor.tg_id, enabled=0 if row["enabled"] else 1)
        await audit(actor, "Toggled message", key)
        row = await Messages.get(key)
    elif action == "mode":
        modes = ["HTML", "Markdown", "None"]
        nxt = modes[(modes.index(row["parse_mode"]) + 1) % len(modes)] \
            if row["parse_mode"] in modes else "HTML"
        await Messages.update(key, actor.tg_id, parse_mode=nxt)
        await audit(actor, "Changed parse mode", f"{key} → {nxt}")
        row = await Messages.get(key)
    elif action == "reset":
        await Messages.reset(key, actor.tg_id)
        await audit(actor, "Reset message", key)
        row = await Messages.get(key)
    elif action == "edit":
        await state.set_state(Wizard.message_edit)
        await state.update_data(message_key=key)
        await show(call, screen("✏️ EDIT MESSAGE", [
            f"<b>{esc(row['name'])}</b> — <code>{esc(key)}</code>", "",
            "Send the new content. Placeholders you can use:",
            "<code>{name}</code> <code>{id}</code> <code>{ref_link}</code> "
            "<code>{ref_count}</code> <code>{reward}</code>",
        ]), keyboard([cancel_row(cb("msg", "view", key))]))
        return
    elif action == "preview":
        text, mode = await preview(key)
        try:
            await call.message.answer(text, parse_mode=(mode if mode != "None" else None))
        except Exception:  # noqa: BLE001
            await call.answer("⚠️ The content is not valid for this parse mode.",
                              show_alert=True)
            return
        await call.answer("👁 Preview sent")
        return

    lines = [
        f"🆔 <code>{esc(row['key'])}</code>",
        f"📛 <b>{esc(row['name'])}</b> · {esc(row['category'])}",
        f"🔤 <b>Parse mode:</b> {esc(row['parse_mode'])}",
        f"⚡️ <b>Status:</b> {'enabled' if row['enabled'] else 'disabled'}",
        f"🕐 <b>Last edited:</b> {ago(row['updated_at'])}"
        + (f" by <code>{row['updated_by']}</code>" if row["updated_by"] else ""),
        "",
        "<b>Content</b>",
        f"<code>{esc(row['content'][:600])}</code>" if row["content"]
        else "<i>(empty)</i>",
    ]
    kb = [
        [btn("✏️ Edit", cb("msg", "edit", key)),
         btn("👁 Preview", cb("msg", "preview", key))],
        [btn("🔴 Disable" if row["enabled"] else "🟢 Enable", cb("msg", "toggle", key)),
         btn(f"🔤 {row['parse_mode']}", cb("msg", "mode", key))],
        [btn("🔄 Reset to default", cb("msg", "reset", key))],
        nav_row(back=cb("msg", "cat", row["category"])),
    ]
    await show(call, screen("✏️ MESSAGE", lines), keyboard(kb))


@admin_content_router.message(Wizard.message_edit)
@requires("messages")
async def do_edit_message(message: Message, state: FSMContext, actor: Actor,
                          **_) -> None:
    data = await state.get_data()
    await state.clear()
    key = data.get("message_key", "")
    await Messages.update(key, actor.tg_id, content=message.html_text)
    await audit(actor, "Edited message", key)
    row = await Messages.get(key)
    await show(message, screen("✅ SAVED", [
        f"<b>{esc(row['name'])}</b> is live from the next send.", "",
        f"<code>{esc(row['content'][:400])}</code>",
    ]), keyboard([[btn("👁 Preview", cb("msg", "preview", key))],
                  nav_row(back=cb("msg", "view", key))]))


# --------------------------------------------------------------------------
# visual button builder
# --------------------------------------------------------------------------

@admin_content_router.callback_query(F.data.startswith("a:btn:"))
@requires("buttons")
async def cb_buttons(call: CallbackQuery, state: FSMContext, actor: Actor,
                     **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        screens = await Buttons.screens()
        lines = ["Pick the screen whose keyboard you want to design.", ""]
        for name in screens:
            rows = await Buttons.for_screen(name, enabled_only=False)
            lines.append(f"• <b>{esc(name)}</b> — {len(rows)} button(s)")
        kb = grid([(f"🔘 {s}", cb("btn", "screen", s)) for s in screens], columns=2)
        kb.append(nav_row(back=cb("dash", "open"), home=False))
        await show(call, screen("🔘 BUTTON BUILDER", lines), keyboard(kb))
        return

    if action == "screen":
        name = args[0] if args else "start"
        rows = await Buttons.for_screen(name, enabled_only=False)
        lines = [f"Screen: <b>{esc(name)}</b>", ""]
        if not rows:
            lines.append("<i>No buttons yet.</i>")
        for row in rows:
            lines.append(
                f"{'🟢' if row['enabled'] else '🔴'} row {row['row']}·"
                f"pos {row['position']} — {esc(decorate(row))}"
                f" → <code>{esc(row['url'] or row['callback'] or '')}</code>")
        kb = grid([(f"✏️ {decorate(r)[:18]}", cb("btn", "view", r["id"]))
                   for r in rows], columns=2)
        kb.append([btn("➕ Add Button", cb("btn", "add", name)),
                   btn("👁 Live Preview", cb("btn", "preview", name))])
        kb.append(nav_row(back=cb("btn", "open")))
        await show(call, screen("🔘 BUTTON BUILDER", lines), keyboard(kb))
        return

    if action == "add":
        name = args[0] if args else "start"
        await state.set_state(Wizard.button_text)
        await state.update_data(screen=name)
        await show(call, screen("➕ ADD BUTTON", [
            f"Screen: <b>{esc(name)}</b>", "",
            "Send the button label. Include an emoji if you want one:",
            "<code>🎁 Claim Now</code>",
        ]), keyboard([cancel_row(cb("btn", "screen", name))]))
        return

    if action == "preview":
        name = args[0] if args else "start"
        markup = await build_keyboard(name)
        await call.message.answer(
            f"👁 <b>Live preview — {esc(name)}</b>\n"
            "This is exactly what users see right now.",
            parse_mode="HTML", reply_markup=markup)
        await call.answer("👁 Preview sent")
        return

    bid = int_arg(args, 0)
    row = await Buttons.get(bid)
    if row is None:
        await call.answer("Button not found.", show_alert=True)
        return

    if action == "toggle":
        await Buttons.update(bid, enabled=0 if row["enabled"] else 1)
    elif action == "style":
        styles = list(STYLE_MARK)
        nxt = styles[(styles.index(row["style"]) + 1) % len(styles)] \
            if row["style"] in styles else "DEFAULT"
        await Buttons.update(bid, style=nxt)
    elif action == "row":
        await Buttons.update(bid, row=max(0, int(row["row"]) + int_arg(args, 1)))
    elif action == "pos":
        await Buttons.update(bid, position=max(0, int(row["position"])
                                               + int_arg(args, 1)))
    elif action == "dup":
        new_id = await Buttons.duplicate(bid)
        await audit(actor, "Duplicated button", str(bid))
        bid = new_id or bid
    elif action == "text":
        await state.set_state(Wizard.button_text)
        await state.update_data(button_id=bid, screen=row["screen"])
        await show(call, screen("✏️ EDIT BUTTON", ["Send the new label."]),
                   keyboard([cancel_row(cb("btn", "view", bid))]))
        return
    elif action == "target":
        await state.set_state(Wizard.button_target)
        await state.update_data(button_id=bid)
        await show(call, screen("🔗 BUTTON TARGET", [
            "Send a URL (<code>https://…</code>) for a link button,",
            "or a callback name such as <code>flow:claim</code>.",
        ]), keyboard([cancel_row(cb("btn", "view", bid))]))
        return
    elif action == "confirm_del":
        text, markup = confirm_screen(
            "Delete this button?", f"'{decorate(row)}' will disappear from the keyboard.",
            cb("btn", "delete", bid), cb("btn", "view", bid))
        await show(call, text, markup)
        return
    elif action == "delete":
        await Buttons.delete(bid)
        await audit(actor, "Deleted button", str(bid))
        await show(call, screen("🗑 BUTTON DELETED", [
            f"{esc(decorate(row))} has been removed from "
            f"<b>{esc(row['screen'])}</b>.",
            "",
            "The user keyboard already reflects this.",
        ]), keyboard([
            [btn("👁 Live Preview", cb("btn", "preview", row["screen"]))],
            nav_row(back=cb("btn", "screen", row["screen"])),
        ]), "🗑 Deleted")
        return

    row = await Buttons.get(bid)
    lines = [
        f"🔘 <b>Preview:</b> {esc(decorate(row))}",
        f"📛 <b>Text:</b> {esc(row['text'])}",
        f"😀 <b>Emoji:</b> {esc(row['emoji'] or '—')}",
        f"🧭 <b>Type:</b> {esc(row['kind'])}",
        f"🔗 <b>Target:</b> <code>{esc(row['url'] or row['callback'] or '—')}</code>",
        f"🎨 <b>Style:</b> {esc(row['style'])} {STYLE_MARK.get(row['style'], '')}",
        f"📐 <b>Row:</b> {row['row']}   <b>Position:</b> {row['position']}",
        f"⚡️ <b>Status:</b> {'enabled' if row['enabled'] else 'disabled'}",
    ]
    kb = [
        [btn("✏️ Text", cb("btn", "text", bid)),
         btn("🔗 Target", cb("btn", "target", bid))],
        [btn("🎨 Style", cb("btn", "style", bid)),
         btn("📋 Duplicate", cb("btn", "dup", bid))],
        [btn("⬆️ Row -", cb("btn", "row", bid, -1)),
         btn("⬇️ Row +", cb("btn", "row", bid, 1))],
        [btn("◀️ Pos -", cb("btn", "pos", bid, -1)),
         btn("▶️ Pos +", cb("btn", "pos", bid, 1))],
        [btn("🔴 Disable" if row["enabled"] else "🟢 Enable", cb("btn", "toggle", bid)),
         btn("👁 Preview", cb("btn", "preview", row["screen"]))],
        [btn("🗑 Delete", cb("btn", "confirm_del", bid))],
        nav_row(back=cb("btn", "screen", row["screen"])),
    ]
    await show(call, screen("🔘 BUTTON", lines), keyboard(kb))


@admin_content_router.message(Wizard.button_text)
@requires("buttons")
async def do_button_text(message: Message, state: FSMContext, actor: Actor,
                         **_) -> None:
    data = await state.get_data()
    raw = (message.text or "").strip()
    emoji, _, rest = raw.partition(" ")
    has_emoji = bool(emoji) and not emoji[0].isalnum()
    label = rest.strip() if has_emoji else raw

    if data.get("button_id"):
        await state.clear()
        bid = int(data["button_id"])
        await Buttons.update(bid, text=label, emoji=emoji if has_emoji else None)
        await audit(actor, "Edited button", str(bid))
        row = await Buttons.get(bid)
        await show(message, screen("✅ SAVED", [
            f"Button is now {esc(decorate(row))}",
        ]), keyboard([nav_row(back=cb("btn", "view", bid))]))
        return

    screen_name = data.get("screen", "start")
    rows = await Buttons.for_screen(screen_name, enabled_only=False)
    next_row = max([int(r["row"]) for r in rows], default=-1) + 1
    bid = await Buttons.add(screen=screen_name, text=label,
                            emoji=emoji if has_emoji else None,
                            kind="callback", callback="flow:noop",
                            style="DEFAULT", row=next_row, position=0)
    await state.set_state(Wizard.button_target)
    await state.update_data(button_id=bid)
    await audit(actor, "Added button", f"{screen_name}:{label}")
    await show(message, screen("🔗 BUTTON TARGET", [
        f"Added {esc(label)}. Now send its target:", "",
        "a URL (<code>https://…</code>) or a callback such as "
        "<code>flow:claim</code>.",
    ]), keyboard([cancel_row(cb("btn", "view", bid))]))


@admin_content_router.message(Wizard.button_target)
@requires("buttons")
async def do_button_target(message: Message, state: FSMContext, actor: Actor,
                           **_) -> None:
    data = await state.get_data()
    await state.clear()
    bid = int(data.get("button_id", 0))
    target = (message.text or "").strip()
    if target.startswith(("http://", "https://", "tg://")):
        await Buttons.update(bid, kind="url", url=target, callback=None)
    else:
        await Buttons.update(bid, kind="callback", callback=target, url=None)
    await audit(actor, "Set button target", str(bid))
    row = await Buttons.get(bid)
    await show(message, screen("✅ SAVED", [
        f"{esc(decorate(row))} → <code>{esc(target)}</code>", "",
        "The user keyboard already reflects this.",
    ]), keyboard([[btn("👁 Live Preview", cb("btn", "preview", row["screen"]))],
                  nav_row(back=cb("btn", "view", bid))]))


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/admin_broadcast.py
# ==============================================================================





admin_broadcast_router = Router(name="admin_broadcast")

KINDS = [("💬 Text", "text"), ("🖼 Photo", "photo"), ("🎥 Video", "video"),
         ("📄 Document", "document"), ("🎵 Audio", "audio"), ("🎤 Voice", "voice"),
         ("🔁 Forward", "forward")]

AUDIENCES = [("all", "👥 All Users"), ("active", "🟢 Active Users"),
             ("new", "🆕 New Users"), ("not_blocked", "🚫 Exclude Blocked"),
             ("custom", "🎯 Custom IDs")]
AUDIENCE_LABEL = dict(AUDIENCES)
PER_PAGE = 6


# --------------------------------------------------------------------------
# studio
# --------------------------------------------------------------------------

async def studio_screen() -> tuple[str, object]:
    rows, total = await Broadcasts.page(0, 5)
    rate = await Broadcasts.delivery_rate()
    lines = [f"Stored broadcasts: <b>{num(total)}</b>",
             f"Delivery rate: <b>{rate:.1f}%</b>",
             f"Worker: {'🚀 sending' if engine.busy else '💤 idle'}", ""]
    if rows:
        lines.append("<b>Recent</b>")
        for row in rows:
            lines.append(
                f"{STATUS_ICON.get(row['status'], '•')} #{row['id']} "
                f"{esc(row['kind'])} · {num(row['sent'])}/{num(row['total'])} · "
                f"{esc(row['status'])} · {ago(row['created_at'])}")
    else:
        lines.append("<i>Nothing sent yet.</i>")
    kb = [
        [btn("🆕 New Broadcast", cb("bc", "new"))],
        [btn("📜 History", cb("bc", "hist", 0)),
         btn("⏰ Scheduler", cb("sch", "open"))],
    ]
    if engine.busy:
        kb.append([btn("🛑 Stop Current Send", cb("bc", "stop"))])
    kb.append(nav_row(back=cb("dash", "open"), home=False))
    return screen("📢 BROADCAST STUDIO", lines), keyboard(kb)


async def draft_screen(bid: int) -> tuple[str, object] | None:
    row = await Broadcasts.get(bid)
    if row is None:
        return None
    targets = await Users.audience(row["audience"], row["custom_ids"])
    buttons = json.loads(row["markup_json"] or "[]")
    body = (row["body"] or "")[:400]
    lines = [
        f"🆔 Draft <b>#{row['id']}</b> · {esc(row['kind'])}",
        f"🎯 <b>Audience:</b> {AUDIENCE_LABEL.get(row['audience'], row['audience'])}"
        f" — <b>{num(len(targets))}</b> recipients",
        f"🔘 <b>Buttons:</b> {sum(len(r) for r in buttons)} in {len(buttons)} row(s)",
        f"⏰ <b>Scheduled:</b> {esc(row['scheduled_at'] or '—')}",
        "",
        "<b>Content</b>",
        f"<code>{esc(body) or '(media only)'}</code>",
    ]
    if row["file_id"]:
        lines.append("📎 media attached")
    kb = [
        [btn("🎯 Audience", cb("bc", "aud", bid)),
         btn("🔘 Buttons", cb("bc", "btns", bid))],
        [btn("✏️ Content", cb("bc", "content", bid)),
         btn("👁 Preview", cb("bc", "preview", bid))],
        [btn("🚀 SEND NOW", cb("bc", "confirm_send", bid))],
        [btn("⏰ SCHEDULE", cb("bc", "sched", bid)),
         btn("💾 SAVE DRAFT", cb("bc", "save", bid))],
        [btn("❌ Discard", cb("bc", "confirm_del", bid))],
        nav_row(back=cb("bc", "open")),
    ]
    return screen("📢 BROADCAST COMPOSER", lines), keyboard(kb)


@admin_broadcast_router.callback_query(F.data.startswith("a:bc:"))
@requires("broadcast")
async def cb_broadcast(call: CallbackQuery, state: FSMContext, bot: Bot,
                       actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        await show(call, *(await studio_screen()))
        return

    if action == "new":
        kb = grid([(label, cb("bc", "type", kind)) for label, kind in KINDS],
                  columns=2)
        kb.append(nav_row(back=cb("bc", "open")))
        await show(call, screen("🆕 NEW BROADCAST",
                                ["Pick the message type you want to send."]),
                   keyboard(kb))
        return

    if action == "type":
        kind = args[0] if args else "text"
        bid = await Broadcasts.create(actor.tg_id, kind=kind, status="DRAFT")
        await state.set_state(Wizard.broadcast_content)
        await state.update_data(broadcast_id=bid, kind=kind)
        prompt = {
            "text": "Send the message text. HTML formatting is kept.",
            "forward": "Forward me the message you want to broadcast.",
        }.get(kind, "Send the media, with a caption if you want one.")
        await show(call, screen("✏️ BROADCAST CONTENT", [prompt]),
                   keyboard([cancel_row(cb("bc", "open"))]))
        return

    if action == "stop":
        engine.cancel()
        await audit(actor, "Stopped broadcast", str(engine.current))
        await show(call, *(await studio_screen()), alert="🛑 Stopping")
        return

    if action == "hist":
        page = int_arg(args, 0)
        rows, total = await Broadcasts.page(page, PER_PAGE)
        lines = [f"<b>{num(total)}</b> broadcasts stored", ""]
        for row in rows:
            lines += [
                f"{STATUS_ICON.get(row['status'], '•')} <b>#{row['id']}</b> "
                f"· {esc(row['kind'])} · {esc(row['status'])}",
                f"    {esc(row['created_at'])} · recipients {num(row['total'])}"
                f" · ✅ {num(row['sent'])} · ❌ {num(row['failed'])}"
                f" · 🚫 {num(row['blocked'])}",
            ]
        kb = grid([(f"#{r['id']}", cb("bc", "view", r["id"])) for r in rows],
                  columns=4)
        page_row = pager(cb("bc", "hist"), page, total, PER_PAGE)
        if page_row:
            kb.append(page_row)
        kb.append(nav_row(back=cb("bc", "open")))
        await show(call, screen("📜 BROADCAST HISTORY",
                                lines or ["<i>Empty.</i>"]), keyboard(kb))
        return

    bid = int_arg(args, 0)
    row = await Broadcasts.get(bid)
    if row is None:
        await call.answer("Broadcast not found.", show_alert=True)
        return

    if action == "view":
        if row["status"] == "DRAFT":
            await show(call, *(await draft_screen(bid)))
            return
        lines = [
            f"{STATUS_ICON.get(row['status'], '•')} <b>#{row['id']}</b> "
            f"— {esc(row['status'])}",
            f"📅 Created: {esc(row['created_at'])}",
            f"🚀 Started: {esc(row['started_at'] or '—')}",
            f"🏁 Finished: {esc(row['finished_at'] or '—')}",
            f"🎯 Audience: {AUDIENCE_LABEL.get(row['audience'], row['audience'])}",
            "",
            f"👥 Recipients: {num(row['total'])}",
            f"✅ Successful: {num(row['sent'])} ({pct(row['sent'], row['total'] or 1)})",
            f"❌ Failed: {num(row['failed'])}",
            f"🚫 Blocked: {num(row['blocked'])}",
            "",
            f"<code>{esc((row['body'] or '(media)')[:300])}</code>",
        ]
        kb = [
            [btn("🔁 Duplicate", cb("bc", "dup", bid)),
             btn("👁 Preview", cb("bc", "preview", bid))],
            [btn("🗑 Delete", cb("bc", "confirm_del", bid))],
            nav_row(back=cb("bc", "hist", 0)),
        ]
        await show(call, screen("📊 BROADCAST DETAILS", lines), keyboard(kb))
        return

    if action == "aud":
        order = [a for a, _ in AUDIENCES]
        nxt = order[(order.index(row["audience"]) + 1) % len(order)] \
            if row["audience"] in order else "all"
        await Broadcasts.update(bid, audience=nxt)
        if nxt == "custom":
            await state.set_state(Wizard.broadcast_custom_ids)
            await state.update_data(broadcast_id=bid)
            await show(call, screen("🎯 CUSTOM AUDIENCE", [
                "Send the Telegram IDs, separated by commas.",
            ]), keyboard([cancel_row(cb("bc", "view", bid))]))
            return
        await show(call, *(await draft_screen(bid)), alert="🎯 Audience updated")
        return

    if action == "btns":
        await state.set_state(Wizard.broadcast_buttons)
        await state.update_data(broadcast_id=bid)
        await show(call, screen("🔘 BROADCAST BUTTONS", [
            "Send one line per keyboard row:",
            "<code>Open Site | https://example.com</code>",
            "<code>Left | https://a.com ; Right | https://b.com</code>",
            "",
            "Send <b>-</b> to remove all buttons.",
        ]), keyboard([cancel_row(cb("bc", "view", bid))]))
        return

    if action == "content":
        await state.set_state(Wizard.broadcast_content)
        await state.update_data(broadcast_id=bid, kind=row["kind"])
        await show(call, screen("✏️ BROADCAST CONTENT", ["Send the new content."]),
                   keyboard([cancel_row(cb("bc", "view", bid))]))
        return

    if action == "preview":
        markup = markup_from_json(row["markup_json"])
        try:
            await engine._deliver(actor.tg_id, row, markup) if False else None
            if row["kind"] == "text":
                await call.message.answer(row["body"] or "(empty)", parse_mode="HTML",
                                          reply_markup=markup)
            elif row["kind"] == "forward" and row["from_chat_id"]:
                await bot.copy_message(actor.tg_id, row["from_chat_id"],
                                       row["from_msg_id"], reply_markup=markup)
            elif row["file_id"]:
                sender = {"photo": bot.send_photo, "video": bot.send_video,
                          "document": bot.send_document, "audio": bot.send_audio,
                          "voice": bot.send_voice}[row["kind"]]
                await sender(actor.tg_id, row["file_id"], reply_markup=markup)
            await call.answer("👁 Preview sent")
        except Exception as exc:  # noqa: BLE001
            await capture("BROADCAST", exc, f"preview {bid}")
            await call.answer("⚠️ Preview failed — check the content.", show_alert=True)
        return

    if action == "confirm_send":
        targets = await Users.audience(row["audience"], row["custom_ids"])
        text, markup = confirm_screen(
            f"Send broadcast #{bid} now?",
            f"{len(targets)} recipients · "
            f"{AUDIENCE_LABEL.get(row['audience'], row['audience'])}",
            cb("bc", "send", bid), cb("bc", "view", bid))
        await show(call, text, markup)
        return

    if action == "send":
        if engine.busy:
            await call.answer("A broadcast is already running.", show_alert=True)
            return
        await engine.start(bid)
        await audit(actor, "Started broadcast", f"#{bid}")
        await show(call, *(await studio_screen()), alert="🚀 Sending")
        return

    if action == "sched":
        await state.set_state(Wizard.broadcast_schedule)
        await state.update_data(broadcast_id=bid)
        await show(call, screen("⏰ SCHEDULE BROADCAST", [
            "Send the send time in UTC:",
            "<code>2026-09-01 20:00</code>",
        ]), keyboard([cancel_row(cb("bc", "view", bid))]))
        return

    if action == "save":
        await Broadcasts.update(bid, status="DRAFT")
        await audit(actor, "Saved broadcast draft", f"#{bid}")
        await show(call, *(await studio_screen()), alert="💾 Draft saved")
        return

    if action == "dup":
        fields = {k: row[k] for k in ("kind", "body", "file_id", "from_chat_id",
                                      "from_msg_id", "parse_mode", "markup_json",
                                      "audience", "custom_ids")}
        new_id = await Broadcasts.create(actor.tg_id, status="DRAFT", **fields)
        await audit(actor, "Duplicated broadcast", f"#{bid} → #{new_id}")
        await show(call, *(await draft_screen(new_id)), alert="🔁 Duplicated")
        return

    if action == "confirm_del":
        text, markup = confirm_screen(
            f"Delete broadcast #{bid}?", "The record and its statistics are removed.",
            cb("bc", "del", bid), cb("bc", "view", bid))
        await show(call, text, markup)
        return

    if action == "del":
        await Broadcasts.delete(bid)
        await audit(actor, "Deleted broadcast", f"#{bid}")
        await show(call, *(await studio_screen()), alert="🗑 Deleted")


@admin_broadcast_router.message(Wizard.broadcast_content)
@requires("broadcast")
async def do_broadcast_content(message: Message, state: FSMContext,
                               actor: Actor, **_) -> None:
    data = await state.get_data()
    bid = int(data.get("broadcast_id", 0))
    kind = data.get("kind", "text")
    fields: dict[str, object] = {}

    if kind == "forward" and (message.forward_from_chat or message.forward_from):
        fields = {"from_chat_id": str(message.chat.id), "from_msg_id": message.message_id}
    elif kind == "text":
        fields = {"body": message.html_text}
    elif kind == "photo" and message.photo:
        fields = {"file_id": message.photo[-1].file_id,
                  "body": message.html_text if message.caption else None}
    elif kind == "video" and message.video:
        fields = {"file_id": message.video.file_id,
                  "body": message.html_text if message.caption else None}
    elif kind == "document" and message.document:
        fields = {"file_id": message.document.file_id,
                  "body": message.html_text if message.caption else None}
    elif kind == "audio" and message.audio:
        fields = {"file_id": message.audio.file_id,
                  "body": message.html_text if message.caption else None}
    elif kind == "voice" and message.voice:
        fields = {"file_id": message.voice.file_id}
    else:
        await show(message, screen("✏️ BROADCAST CONTENT", [
            f"❌ That is not a <b>{esc(kind)}</b> message. Try again, or cancel.",
        ]), keyboard([cancel_row(cb("bc", "view", bid))]))
        return

    await state.clear()
    await Broadcasts.update(bid, **fields)
    await audit(actor, "Edited broadcast content", f"#{bid}")
    await show(message, *(await draft_screen(bid)))


@admin_broadcast_router.message(Wizard.broadcast_buttons)
@requires("broadcast")
async def do_broadcast_buttons(message: Message, state: FSMContext,
                               actor: Actor, **_) -> None:
    data = await state.get_data()
    await state.clear()
    bid = int(data.get("broadcast_id", 0))
    raw = (message.text or "").strip()
    await Broadcasts.update(bid, markup_json=None if raw == "-"
                            else parse_button_spec(raw))
    await show(message, *(await draft_screen(bid)))


@admin_broadcast_router.message(Wizard.broadcast_custom_ids)
@requires("broadcast")
async def do_broadcast_ids(message: Message, state: FSMContext, actor: Actor,
                           **_) -> None:
    data = await state.get_data()
    await state.clear()
    bid = int(data.get("broadcast_id", 0))
    await Broadcasts.update(bid, custom_ids=(message.text or "").strip())
    await show(message, *(await draft_screen(bid)))


@admin_broadcast_router.message(Wizard.broadcast_schedule)
@requires("broadcast")
async def do_broadcast_schedule(message: Message, state: FSMContext, actor: Actor,
                                **_) -> None:
    data = await state.get_data()
    bid = int(data.get("broadcast_id", 0))
    raw = (message.text or "").strip()
    stamp = raw if len(raw) == 16 else raw[:16]
    try:
        from datetime import datetime

        datetime.strptime(stamp, "%Y-%m-%d %H:%M")
    except ValueError:
        await show(message, screen("⏰ SCHEDULE BROADCAST", [
            "❌ Use the format <code>2026-09-01 20:00</code>.",
        ]), keyboard([cancel_row(cb("bc", "view", bid))]))
        return
    await state.clear()
    await Broadcasts.update(bid, status="SCHEDULED", scheduled_at=f"{stamp}:00")
    await Schedules.create(actor.tg_id, kind="broadcast", ref_id=bid, repeat="once",
                           run_at=f"{stamp}:00")
    await audit(actor, "Scheduled broadcast", f"#{bid} at {stamp}")
    await show(message, screen("⏰ SCHEDULED", [
        f"Broadcast #{bid} will start at <b>{esc(stamp)} UTC</b>.",
    ]), keyboard([nav_row(back=cb("bc", "open"))]))


# --------------------------------------------------------------------------
# campaigns
# --------------------------------------------------------------------------

CAMPAIGN_ICON = {"ACTIVE": "🟢", "PAUSED": "⏸", "ENDED": "🔴"}


@admin_broadcast_router.callback_query(F.data.startswith("a:camp:"))
@requires("campaigns")
async def cb_campaigns(call: CallbackQuery, state: FSMContext, actor: Actor,
                       **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        rows = await Campaigns.all()
        lines = [f"Campaigns: <b>{num(len(rows))}</b>", ""]
        for row in rows:
            lines += [
                f"{CAMPAIGN_ICON.get(row['status'], '•')} <b>{esc(row['name'])}</b>"
                f" · #{row['id']}",
                f"    👁 {num(row['views'])} · 🔘 {num(row['clicks'])}"
                f" · 🎁 {num(row['claims'])} · 📢 {num(row['joins'])}"
                f" · conv {pct(row['claims'], row['views'] or 1)}",
            ]
        if not rows:
            lines.append("<i>No campaigns yet.</i>")
        kb = grid([(f"🎯 {r['name'][:16]}", cb("camp", "view", r["id"]))
                   for r in rows], columns=2)
        kb.append([btn("➕ New Campaign", cb("camp", "new"))])
        kb.append(nav_row(back=cb("dash", "open"), home=False))
        await show(call, screen("🎯 CAMPAIGN MANAGER", lines), keyboard(kb))
        return

    if action == "new":
        await state.set_state(Wizard.campaign_name)
        await show(call, screen("➕ NEW CAMPAIGN", ["Send the campaign name."]),
                   keyboard([cancel_row(cb("camp", "open"))]))
        return

    cid = int_arg(args, 0)
    row = await Campaigns.get(cid)
    if row is None:
        await call.answer("Campaign not found.", show_alert=True)
        return

    if action == "status":
        order = ["ACTIVE", "PAUSED", "ENDED"]
        nxt = order[(order.index(row["status"]) + 1) % len(order)] \
            if row["status"] in order else "PAUSED"
        await Campaigns.update(cid, status=nxt)
        await audit(actor, "Changed campaign status", f"#{cid} → {nxt}")
        row = await Campaigns.get(cid)
    elif action == "dates":
        await state.set_state(Wizard.campaign_dates)
        await state.update_data(campaign_id=cid)
        await show(call, screen("📅 CAMPAIGN DATES", [
            "Send start and end, one per line:",
            "<code>2026-09-01 00:00</code>",
            "<code>2026-09-30 23:59</code>",
        ]), keyboard([cancel_row(cb("camp", "view", cid))]))
        return
    elif action == "body":
        await state.set_state(Wizard.campaign_body)
        await state.update_data(campaign_id=cid)
        await show(call, screen("✏️ CAMPAIGN MESSAGE", [
            "Send the campaign text (HTML supported).",
        ]), keyboard([cancel_row(cb("camp", "view", cid))]))
        return
    elif action == "confirm_del":
        text, markup = confirm_screen(
            "Delete this campaign?", f"'{row['name']}' and its metrics are removed.",
            cb("camp", "del", cid), cb("camp", "view", cid))
        await show(call, text, markup)
        return
    elif action == "del":
        await Campaigns.delete(cid)
        await audit(actor, "Deleted campaign", f"#{cid}")
        await cb_campaigns(call.model_copy(update={"data": cb("camp", "open")}),
                           state, actor=actor)
        return

    views = row["views"] or 0
    lines = [
        f"{CAMPAIGN_ICON.get(row['status'], '•')} <b>{esc(row['name'])}</b>",
        f"📅 {esc(row['start_at'] or '—')} → {esc(row['end_at'] or '—')}",
        f"🎯 Audience: {AUDIENCE_LABEL.get(row['audience'], row['audience'])}",
        "",
        "<b>Performance</b>",
        f"👁 Views: {num(views)}",
        f"🔘 Button clicks: {num(row['clicks'])} ({pct(row['clicks'], views or 1)})",
        f"📢 Join completions: {num(row['joins'])}",
        f"🎁 Claims: {num(row['claims'])}",
        f"📈 Conversion: {pct(row['claims'], views or 1)}",
        "",
        f"<code>{esc((row['body'] or '(no message yet)')[:300])}</code>",
    ]
    kb = [
        [btn(f"{CAMPAIGN_ICON.get(row['status'], '•')} Status: {row['status']}",
             cb("camp", "status", cid))],
        [btn("✏️ Message", cb("camp", "body", cid)),
         btn("📅 Dates", cb("camp", "dates", cid))],
        [btn("🗑 Delete", cb("camp", "confirm_del", cid))],
        nav_row(back=cb("camp", "open")),
    ]
    await show(call, screen("🎯 CAMPAIGN", lines), keyboard(kb))


@admin_broadcast_router.message(Wizard.campaign_name)
@requires("campaigns")
async def do_campaign_name(message: Message, state: FSMContext, actor: Actor,
                           **_) -> None:
    await state.clear()
    cid = await Campaigns.create(actor.tg_id, name=(message.text or "Campaign").strip())
    await audit(actor, "Created campaign", f"#{cid}")
    await state.set_state(Wizard.campaign_body)
    await state.update_data(campaign_id=cid)
    await show(message, screen("✏️ CAMPAIGN MESSAGE", [
        "Campaign created. Now send its message text.",
    ]), keyboard([cancel_row(cb("camp", "view", cid))]))


@admin_broadcast_router.message(Wizard.campaign_body)
@requires("campaigns")
async def do_campaign_body(message: Message, state: FSMContext, actor: Actor,
                           **_) -> None:
    data = await state.get_data()
    await state.clear()
    cid = int(data.get("campaign_id", 0))
    await Campaigns.update(cid, body=message.html_text)
    await show(message, screen("✅ SAVED", ["Campaign message updated."]),
               keyboard([nav_row(back=cb("camp", "view", cid))]))


@admin_broadcast_router.message(Wizard.campaign_dates)
@requires("campaigns")
async def do_campaign_dates(message: Message, state: FSMContext, actor: Actor,
                            **_) -> None:
    data = await state.get_data()
    await state.clear()
    cid = int(data.get("campaign_id", 0))
    parts = [p.strip() for p in (message.text or "").splitlines() if p.strip()]
    start = f"{parts[0]}:00" if parts else None
    end = f"{parts[1]}:00" if len(parts) > 1 else None
    await Campaigns.update(cid, start_at=start, end_at=end)
    await show(message, screen("✅ SAVED", ["Campaign window updated."]),
               keyboard([nav_row(back=cb("camp", "view", cid))]))


# --------------------------------------------------------------------------
# scheduler
# --------------------------------------------------------------------------

JOB_ICON = {"SCHEDULED": "🟢", "PAUSED": "⏸", "DONE": "✅", "CANCELLED": "🚫"}


@admin_broadcast_router.callback_query(F.data.startswith("a:sch:"))
@requires("broadcast")
async def cb_scheduler(call: CallbackQuery, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)
    sid = int_arg(args, 0)

    if action == "pause":
        await Schedules.update(sid, status="PAUSED")
        await audit(actor, "Paused job", f"#{sid}")
    elif action == "resume":
        await Schedules.update(sid, status="SCHEDULED")
        await audit(actor, "Resumed job", f"#{sid}")
    elif action == "del":
        await Schedules.delete(sid)
        await audit(actor, "Deleted job", f"#{sid}")

    rows = await Schedules.all()
    lines = [f"Jobs: <b>{num(len(rows))}</b>", ""]
    for row in rows:
        when = row["run_at"] or f"{row['time_of_day']} ({row['repeat']})"
        lines += [
            f"[ Job #{row['id']} ] {JOB_ICON.get(row['status'], '•')} "
            f"{esc(row['status'])}",
            f"    {esc(row['kind'])} → ref #{row['ref_id']} · ⏰ {esc(when)}"
            f" · last run {ago(row['last_run_at'])}",
        ]
    if not rows:
        lines.append("<i>No scheduled jobs. Schedule one from the Broadcast Studio.</i>")
    kb = []
    for row in rows[:8]:
        toggle = ("▶️ Resume", cb("sch", "resume", row["id"])) \
            if row["status"] == "PAUSED" else ("⏸ Pause", cb("sch", "pause", row["id"]))
        kb.append([btn(f"#{row['id']}", cb("sch", "open", row["id"])),
                   btn(toggle[0], toggle[1]),
                   btn("🗑", cb("sch", "del", row["id"]))])
    kb.append(nav_row(back=cb("bc", "open")))
    await show(call, screen("⏰ SCHEDULER", lines), keyboard(kb))


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/admin_system.py
# ==============================================================================





admin_system_router = Router(name="admin_system")

SECTION_ICONS = {
    "general": "⚙️", "force_join": "📢", "journey": "🧭", "broadcast": "📨",
    "referral": "🎁", "scheduler": "⏰", "logging": "📋", "notifications": "🔔",
    "security": "🔐", "database": "💾", "messages": "✏️", "buttons": "🔘",
}
BOOLEAN_KEYS = {"0", "1"}
PER_PAGE = 10


# --------------------------------------------------------------------------
# settings
# --------------------------------------------------------------------------

@admin_system_router.callback_query(F.data.startswith("a:set:"))
@requires("settings")
async def cb_settings(call: CallbackQuery, state: FSMContext, actor: Actor,
                      **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "open":
        sections = await Settings.sections()
        lines = ["Every value here is stored in the database and applied live.", ""]
        for name in sections:
            rows = await Settings.section(name)
            lines.append(f"{SECTION_ICONS.get(name, '•')} <b>{esc(name)}</b>"
                         f" — {len(rows)} setting(s)")
        kb = grid([(f"{SECTION_ICONS.get(s, '•')} {s}", cb("set", "sec", s))
                   for s in sections], columns=2)
        kb.append([btn("🔔 Notifications", cb("set", "sec", "notifications"))])
        kb.append(nav_row(back=cb("dash", "open"), home=False))
        await show(call, screen("⚙️ SETTINGS", lines), keyboard(kb))
        return

    if action == "sec":
        name = args[0] if args else "general"
        rows = await Settings.section(name)
        lines = [f"Section: <b>{esc(name)}</b>", ""]
        for row in rows:
            lines.append(f"• <code>{esc(row['key'])}</code> = "
                         f"<b>{esc(row['value'])}</b>"
                         + (f"  ({ago(row['updated_at'])})" if row["updated_at"] else ""))
        kb = []
        for row in rows:
            if row["value"] in BOOLEAN_KEYS:
                kb.append([btn(toggle_label(row["key"], row["value"] == "1"),
                               cb("set", "toggle", row["key"]))])
            else:
                kb.append([btn(f"✏️ {row['key']} = {row['value']}",
                               cb("set", "edit", row["key"]))])
        kb.append(nav_row(back=cb("set", "open")))
        await show(call, screen(f"{SECTION_ICONS.get(name, '⚙️')} {name.upper()}",
                                lines), keyboard(kb))
        return

    key = args[0] if args else ""
    row = await db.fetchone("SELECT * FROM settings WHERE key=?", (key,))
    if row is None:
        await call.answer("Unknown setting.", show_alert=True)
        return

    if action == "toggle":
        new = await settings.toggle(key, actor.tg_id)
        await audit(actor, "Changed setting", f"{key} → {'1' if new else '0'}")
        await cb_settings(
            call.model_copy(update={"data": cb("set", "sec", row["section"])}),
            state, actor=actor)
        return

    if action == "edit":
        await state.set_state(Wizard.setting_value)
        await state.update_data(setting_key=key, section=row["section"])
        state_clock.touch(actor.tg_id)
        await show(call, screen("✏️ EDIT SETTING", [
            f"<code>{esc(key)}</code>",
            f"Current value: <b>{esc(row['value'])}</b>", "",
            "Send the new value.",
        ]), keyboard([cancel_row(cb("set", "sec", row["section"]))]))


@admin_system_router.message(Wizard.setting_value)
@requires("settings")
async def do_setting(message: Message, state: FSMContext, actor: Actor, **_) -> None:
    data = await state.get_data()
    await state.clear()
    key = data.get("setting_key", "")
    section = data.get("section", "general")
    value = (message.text or "").strip()
    await settings.set(key, value, actor.tg_id, section)
    await audit(actor, "Changed setting", f"{key} → {value}")
    await show(message, screen("✅ SAVED", [
        f"<code>{esc(key)}</code> is now <b>{esc(value)}</b>.",
        "The change is already in effect.",
    ]), keyboard([nav_row(back=cb("set", "sec", section))]))


# --------------------------------------------------------------------------
# database manager
# --------------------------------------------------------------------------

@admin_system_router.callback_query(F.data.startswith("a:dbm:"))
@requires("database")
async def cb_database(call: CallbackQuery, state: FSMContext, actor: Actor,
                      **_) -> None:
    _, action, args = parse_cb(call.data)
    note = None

    if action == "backup":
        stamp = now().replace(":", "-").replace(" ", "_")
        target = Path(config.backup_dir) / f"backup-{stamp}.db"
        await db.conn.commit()
        shutil.copyfile(config.db_path, target)
        await audit(actor, "Created backup", target.name)
        await call.message.answer_document(
            FSInputFile(target), caption=f"💾 Backup · {size(target.stat().st_size)}")
        note = "💾 Backup created"

    elif action == "restore":
        await state.set_state(Wizard.setting_value if False else None)
        text, markup = confirm_screen(
            "Restore a database backup?",
            "The current database is replaced. Send the .db file after confirming.",
            cb("dbm", "restore_go"), cb("dbm", "open"))
        await show(call, text, markup)
        return

    elif action == "restore_go":
        await state.set_state(Wizard.setting_value)
        await state.update_data(restore=True)
        await show(call, screen("📥 RESTORE DATABASE", [
            "Send the <code>.db</code> backup file now.",
        ]), keyboard([cancel_row(cb("dbm", "open"))]))
        return

    elif action == "confirm_cleanup":
        days = await settings.number("log_retention_days", 30)
        text, markup = confirm_screen(
            "Clean up old logs?",
            f"Audit entries, errors and events older than {days} days are deleted.",
            cb("dbm", "cleanup"), cb("dbm", "open"))
        await show(call, text, markup)
        return

    elif action == "cleanup":
        days = await settings.number("log_retention_days", 30)
        removed = await Audit.cleanup(days)
        await audit(actor, "Cleaned old logs", f"{removed} audit rows")
        note = f"🧹 Removed {removed} audit rows"

    counts = await Stats.table_counts()
    lines = [
        f"🗄 <b>Type:</b> SQLite (WAL)",
        f"📦 <b>Size:</b> {size(await db.size_bytes())}",
        f"📁 <b>File:</b> <code>{esc(Path(config.db_path).name)}</code>",
        "",
        "<b>Records</b>",
        f"👥 Users: {num(counts['users'])}   📢 Channels: {num(counts['channels'])}",
        f"✏️ Messages: {num(counts['messages'])}   🔘 Buttons: {num(counts['buttons'])}",
        f"📨 Broadcasts: {num(counts['broadcasts'])}   "
        f"🎯 Campaigns: {num(counts['campaigns'])}",
        f"🎁 Referrals: {num(counts['referrals'])}   ⏰ Jobs: {num(counts['schedules'])}",
        f"📋 Audit rows: {num(counts['audit_logs'])}   "
        f"🚨 Errors: {num(counts['error_logs'])}",
        f"📈 Events: {num(counts['events'])}",
    ]
    kb = [
        [btn("📤 Backup", cb("dbm", "backup")), btn("📥 Restore", cb("dbm", "restore"))],
        [btn("🧹 Cleanup Old Logs", cb("dbm", "confirm_cleanup"))],
        [btn("🔄 Refresh", cb("dbm", "open"))],
        nav_row(back=cb("dash", "open"), home=False),
    ]
    await show(call, screen("💾 DATABASE MANAGER", lines), keyboard(kb), note)


@admin_system_router.message(F.document)
@requires("database")
async def do_restore(message: Message, state: FSMContext, bot: Bot, actor: Actor,
                     **_) -> None:
    data = await state.get_data()
    if not data.get("restore"):
        return
    await state.clear()
    target = Path(config.backup_dir) / f"restore-{message.document.file_unique_id}.db"
    await bot.download(message.document, destination=target)
    await db.close()
    shutil.copyfile(target, config.db_path)
    await db.connect(config.db_path)
    settings.invalidate()
    await audit(actor, "Restored database", message.document.file_name or "upload")
    await show(message, screen("📥 RESTORE COMPLETE", [
        "The database has been replaced and reconnected.",
        "All panels now read the restored data.",
    ]), keyboard([nav_row(back=cb("dbm", "open"))]))


# --------------------------------------------------------------------------
# audit logs
# --------------------------------------------------------------------------

@admin_system_router.callback_query(F.data.startswith("a:log:"))
@requires("logs")
async def cb_logs(call: CallbackQuery, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)
    page = int_arg(args, 0)

    if action == "export":
        rows, _ = await Audit.page(0, 1000)
        text = "\n".join(
            f"{r['created_at']} | ADMIN {r['admin_id']} | {r['action']} | "
            f"{r['target'] or '-'} | {r['result']}" for r in rows)
        file = BufferedInputFile(text.encode("utf-8"),
                                 filename=f"audit-{now()[:10]}.txt")
        await call.message.answer_document(file, caption="📋 Audit log export")
        await call.answer("📤 Exported")
        return

    rows, total = await Audit.page(page, PER_PAGE)
    lines = [f"<b>{num(total)}</b> recorded actions", ""]
    for row in rows:
        lines += [
            f"👤 <b>ADMIN {row['admin_id']}</b> · {esc(row['result'])}",
            f"    {esc(row['action'])}"
            + (f" — <code>{esc(row['target'])}</code>" if row["target"] else ""),
            f"    <i>{esc(row['created_at'])}</i>",
        ]
    if not rows:
        lines.append("<i>No actions recorded yet.</i>")
    kb = []
    page_row = pager(cb("log", "open"), page, total, PER_PAGE)
    if page_row:
        kb.append(page_row)
    kb.append([btn("📤 Export", cb("log", "export")),
               btn("🚨 Error Center", cb("err", "open", 0))])
    kb.append(nav_row(back=cb("dash", "open"), home=False))
    await show(call, screen("📋 AUDIT LOGS", lines), keyboard(kb))


# --------------------------------------------------------------------------
# error center
# --------------------------------------------------------------------------

@admin_system_router.callback_query(F.data.startswith("a:err:"))
@requires("logs")
async def cb_errors(call: CallbackQuery, actor: Actor, **_) -> None:
    _, action, args = parse_cb(call.data)

    if action == "view":
        row = await Errors.get(int_arg(args, 0))
        if row is None:
            await call.answer("Error not found.", show_alert=True)
            return
        lines = [
            f"🆔 <b>#{row['id']}</b>",
            f"🕐 {esc(row['created_at'])} ({ago(row['created_at'])})",
            f"🏷 <b>Type:</b> {esc(row['kind'])}",
            f"⚙️ <b>Operation:</b> {esc(row['operation'] or '—')}",
            "",
            f"<code>{esc(row['message'])}</code>",
        ]
        kb = [[btn("🗑 Clear this error", cb("err", "clear", row["id"]))],
              nav_row(back=cb("err", "open", 0))]
        await show(call, screen("🚨 ERROR DETAILS", lines), keyboard(kb))
        return

    if action == "clear":
        await Errors.clear(int_arg(args, 0))
        await audit(actor, "Cleared error", str(int_arg(args, 0)))
    elif action == "confirm_clear_all":
        text, markup = confirm_screen(
            "Clear every stored error?", "The error history will be empty.",
            cb("err", "clear_all"), cb("err", "open", 0))
        await show(call, text, markup)
        return
    elif action == "clear_all":
        await Errors.clear()
        await audit(actor, "Cleared all errors")

    page = int_arg(args, 0)
    rows, total = await Errors.page(page, 6)
    lines = [f"<b>{num(total)}</b> stored errors", ""]
    for row in rows:
        lines += [
            f"🚨 <b>#{row['id']}</b> · {esc(row['kind'])} · {ago(row['created_at'])}",
            f"    <code>{esc(row['message'][:110])}</code>",
            f"    ⚙️ {esc(row['operation'] or '—')}",
        ]
    if not rows:
        lines.append("<i>No errors recorded. 🎉</i>")
    kb = grid([(f"👁 #{r['id']}", cb("err", "view", r["id"])) for r in rows],
              columns=3)
    page_row = pager(cb("err", "open"), page, total, 6)
    if page_row:
        kb.append(page_row)
    if rows:
        kb.append([btn("🧹 Clear All", cb("err", "confirm_clear_all"))])
    kb.append(nav_row(back=cb("hp", "open"), home=True))
    await show(call, screen("🚨 ERROR CENTER", lines), keyboard(kb))


# --------------------------------------------------------------------------
# wizard safety net - never leave an admin stuck inside a conversation
# --------------------------------------------------------------------------

@admin_system_router.callback_query(F.data.startswith("a:wizard:cancel"))
async def cb_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    state_clock.clear(call.from_user.id)
    from handlers.admin_dashboard import dashboard_screen
    from core.security import resolve

    actor = await resolve(call.from_user.id)
    if actor is None:
        await call.answer("Cancelled.")
        return
    text, markup = await dashboard_screen(actor)
    await show(call, text, markup, alert="❌ Cancelled — temporary data cleared")


# ==============================================================================
# MERGED FROM telegram_admin_bot/handlers/user_flow.py
# ==============================================================================





user_flow_router = Router(name="user_flow")


async def _bot_available(tg_id: int) -> bool:
    """Maintenance and the kill switch never lock an admin out."""
    if await is_admin(tg_id):
        return True
    return (await settings.flag("bot_enabled", True)
            and not await settings.flag("maintenance_mode", False))


async def _maintenance_notice(target: Message) -> None:
    rendered = await render("maintenance")
    if rendered:
        text, mode = rendered
        await target.answer(text, parse_mode=mode)


async def _send_start(message: Message, tg_id: int) -> None:
    if not await settings.flag("stage_welcome", True):
        await _send_force_join(message, message.bot, tg_id)
        return
    user = await Users.get(tg_id)
    rendered = await render(
        "start",
        name=(user["first_name"] if user and user["first_name"] else "there"),
        id=tg_id,
    )
    if not rendered:
        return
    text, mode = rendered
    await message.answer(text, parse_mode=mode,
                         reply_markup=await build_keyboard("start"))
    for campaign in await Campaigns.active():
        await Campaigns.bump(campaign["id"], "views")


async def _send_force_join(message: Message, bot: Bot, tg_id: int) -> bool:
    """Returns True when the user is already cleared to continue."""
    if not await settings.flag("stage_force_join", True):
        return True
    missing = await missing_channels(bot, tg_id)
    if not missing:
        await Users.set_force_join(tg_id, True)
        return True
    rendered = await render("force_join")
    if rendered:
        text, mode = rendered
        await message.answer(text, parse_mode=mode,
                             reply_markup=await join_keyboard(missing))
    return False


async def _send_claim(message: Message) -> None:
    if not await settings.flag("stage_claim", True):
        return
    rendered = await render("claim")
    if rendered:
        text, mode = rendered
        await message.answer(text, parse_mode=mode,
                             reply_markup=await build_keyboard("claim"))


@user_flow_router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot) -> None:
    user = message.from_user
    if user is None:
        return
    created = await Users.touch(user.id, user.username, user.first_name,
                                user.last_name)
    await Events.log(user.id, "start")

    if not await _bot_available(user.id):
        await _maintenance_notice(message)
        return

    # referral payload: /start ref_<id>
    payload = (message.text or "").partition(" ")[2].strip()
    if created and payload.startswith("ref_") and await settings.flag(
            "referral_enabled", True):
        raw = payload[4:]
        if raw.isdigit():
            ok, reason = await Referrals.record(int(raw), user.id)
            await Events.log(user.id, "referral" if ok else "referral_blocked", reason)

    await _send_start(message, user.id)
    if await _send_force_join(message, bot, user.id):
        await _send_claim(message)
    elif await settings.flag("stage_followup", False):
        asyncio.create_task(_followup(bot, user.id))


async def _followup(bot: Bot, tg_id: int) -> None:
    delay = await settings.number("followup_delay_min", 60)
    await asyncio.sleep(max(1, delay) * 60)
    user = await Users.get(tg_id)
    if not user or user["claim_count"] or user["is_blocked"]:
        return
    rendered = await render("followup")
    if not rendered:
        return
    text, mode = rendered
    try:
        await bot.send_message(tg_id, text, parse_mode=mode,
                               reply_markup=await build_keyboard("start"))
    except Exception as exc:  # noqa: BLE001
        await capture("FOLLOWUP", exc, f"follow-up to {tg_id}")


@user_flow_router.callback_query(F.data == "flow:verify")
async def cb_verify(call: CallbackQuery, bot: Bot) -> None:
    tg_id = call.from_user.id
    await Users.touch(tg_id, call.from_user.username, call.from_user.first_name,
                      call.from_user.last_name)
    if await verify(bot, tg_id):
        await call.answer("✅ Verified")
        for campaign in await Campaigns.active():
            await Campaigns.bump(campaign["id"], "joins")
        rendered = await render("claim")
        if rendered:
            text, mode = rendered
            await call.message.edit_text(text, parse_mode=mode,
                                         reply_markup=await build_keyboard("claim"))
        return
    rendered = await render("force_join_failed")
    if rendered:
        await call.answer(rendered[0].replace("<b>", "").replace("</b>", ""),
                          show_alert=True)
    missing = await missing_channels(bot, tg_id)
    if missing:
        await call.message.edit_reply_markup(reply_markup=await join_keyboard(missing))


@user_flow_router.callback_query(F.data.in_({"flow:claim", "flow:claim_confirm"}))
async def cb_claim(call: CallbackQuery, bot: Bot) -> None:
    tg_id = call.from_user.id
    await Events.log(tg_id, "claim_click")
    for campaign in await Campaigns.active():
        await Campaigns.bump(campaign["id"], "clicks")

    if not await _bot_available(tg_id):
        await call.answer("🛠 Maintenance in progress.", show_alert=True)
        return

    missing = await missing_channels(bot, tg_id)
    if missing:
        rendered = await render("force_join")
        if rendered:
            text, mode = rendered
            await call.message.edit_text(text, parse_mode=mode,
                                         reply_markup=await join_keyboard(missing))
        await call.answer("🔒 Join the channels first.", show_alert=True)
        return

    if call.data == "flow:claim":
        rendered = await render("claim")
        if rendered:
            text, mode = rendered
            await call.message.edit_text(text, parse_mode=mode,
                                         reply_markup=await build_keyboard("claim"))
        await call.answer()
        return

    await Users.add_claim(tg_id)
    await Events.log(tg_id, "claim_ok")
    for campaign in await Campaigns.active():
        await Campaigns.bump(campaign["id"], "claims")
    rendered = await render("success") if await settings.flag("stage_success", True) \
        else None
    if rendered:
        text, mode = rendered
        await call.message.edit_text(text, parse_mode=mode,
                                     reply_markup=await build_keyboard("success"))
    await call.answer("✅ Claimed")


@user_flow_router.callback_query(F.data == "flow:referral")
async def cb_referral(call: CallbackQuery, bot: Bot) -> None:
    if not await settings.flag("referral_enabled", True):
        await call.answer("Referrals are currently disabled.", show_alert=True)
        return
    me = await bot.get_me()
    user = await Users.get(call.from_user.id)
    rendered = await render(
        "referral",
        ref_link=f"https://t.me/{me.username}?start=ref_{call.from_user.id}",
        ref_count=(user["referral_count"] if user else 0),
        reward=await settings.get("referral_reward", "1"),
    )
    if rendered:
        text, mode = rendered
        await call.message.answer(text, parse_mode=mode,
                                  disable_web_page_preview=True)
    await call.answer()


@user_flow_router.callback_query(F.data == "flow:help")
async def cb_help(call: CallbackQuery) -> None:
    rendered = await render("help")
    if rendered:
        await call.message.answer(rendered[0], parse_mode=rendered[1])
    await call.answer()


@user_flow_router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    rendered = await render("help")
    if rendered:
        await message.answer(rendered[0], parse_mode=rendered[1])


@user_flow_router.message(Command("about"))
async def cmd_about(message: Message) -> None:
    rendered = await render("about")
    if rendered:
        await message.answer(rendered[0], parse_mode=rendered[1])


@user_flow_router.message(F.chat.type == "private")
async def track_activity(message: Message) -> None:
    """Keeps last-seen fresh so 'active users' means something real."""
    user = message.from_user
    if user:
        await Users.touch(user.id, user.username, user.first_name, user.last_name)


# ==============================================================================
# MERGED FROM telegram_admin_bot/bot.py
# ==============================================================================





logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("bot")


# --------------------------------------------------------------------------
# middlewares
# --------------------------------------------------------------------------

async def track_users(handler, event: TelegramObject, data: dict):
    """Keep the users table current on every single update."""
    user = data.get("event_from_user")
    if user is not None and not user.is_bot:
        try:
            created = await Users.touch(user.id, user.username, user.first_name,
                                        user.last_name)
            if created and await settings.flag("notify_new_user", False):
                await notify_admins(data["bot"],
                                    f"🔔 New user: <code>{user.id}</code>")
        except Exception as exc:  # noqa: BLE001
            await capture("DB", exc, "track user")
    return await handler(event, data)


async def expire_idle_states(handler, event: TelegramObject, data: dict):
    """Drop an abandoned admin wizard instead of trapping the admin in it."""
    user = data.get("event_from_user")
    state = data.get("state")
    if user is not None and state is not None:
        if await state.get_state() is not None:
            if state_clock.expired(user.id):
                await state.clear()
                state_clock.clear(user.id)
                if isinstance(event, Message):
                    await event.answer(
                        "⌛️ That admin action expired and was cancelled.",
                        reply_markup=keyboard([[btn("🏠 Dashboard",
                                                    cb("dash", "open"))]]))
            else:
                state_clock.touch(user.id)
        else:
            state_clock.touch(user.id)
    return await handler(event, data)


# --------------------------------------------------------------------------
# admin notifications
# --------------------------------------------------------------------------

async def notify_admins(bot: Bot, text: str) -> None:
    recipients = set(config.owner_ids)
    for row in await Admins.all():
        recipients.add(int(row["tg_id"]))
    for tg_id in recipients:
        try:
            await bot.send_message(tg_id, text, parse_mode="HTML")
        except Exception:  # noqa: BLE001 - a silent admin is not an incident
            continue


def build_broadcast_hook(bot: Bot):
    async def on_finished(row) -> None:
        if row is None:
            return
        done = row["status"] == "COMPLETED"
        if done and not await settings.flag("notify_broadcast_done", True):
            return
        if not done and not await settings.flag("notify_broadcast_failed", True):
            return
        await notify_admins(
            bot,
            f"{'✅' if done else '⚠️'} <b>Broadcast #{row['id']} {row['status']}</b>\n"
            f"Recipients: {row['total']}\n"
            f"Sent: {row['sent']} · Failed: {row['failed']} · Blocked: {row['blocked']}",
        )

    return on_finished


# --------------------------------------------------------------------------
# health endpoint (optional)
# --------------------------------------------------------------------------

async def start_health_server() -> None:
    try:
        from aiohttp import web
    except ModuleNotFoundError:
        log.info("aiohttp not installed - health endpoint disabled")
        return

    async def handle(_request):
        return web.json_response({
            "status": "ok" if await db.ping() else "degraded",
            "uptime_seconds": int(health.uptime),
            "broadcast_running": engine.busy,
        })

    app = web.Application()
    app.router.add_get("/health", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.health_port)
    try:
        await site.start()
        health.http_ok = True
        log.info("Health endpoint listening on :%s/health", config.health_port)
    except OSError as exc:
        await capture("HEALTH", exc, "bind health port")


# --------------------------------------------------------------------------
# startup
# --------------------------------------------------------------------------

def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())

    dp.update.outer_middleware(track_users)
    dp.update.outer_middleware(expire_idle_states)

    # Admin routers come first: their FSM handlers must win over the
    # catch-all activity tracker at the end of the user flow.
    dp.include_router(admin_dashboard_router)
    dp.include_router(admin_users_router)
    dp.include_router(admin_content_router)
    dp.include_router(admin_broadcast_router)
    dp.include_router(admin_system_router)
    dp.include_router(user_flow_router)

    @dp.error()
    async def on_error(event: ErrorEvent) -> bool:
        """Nothing reaches the console unscrubbed, and nothing crashes the bot."""
        error_id = await capture("BOT", event.exception, "update handling")
        log.error("Handled error #%s: %s", error_id, type(event.exception).__name__)
        if await settings.flag("notify_bot_error", True):
            bot = event.update.bot if hasattr(event.update, "bot") else None
            if bot:
                await notify_admins(bot, f"🚨 Bot error #{error_id} "
                                         f"({type(event.exception).__name__})")
        return True

    return dp


async def main() -> None:
    await db.connect(config.db_path)
    log.info("Database ready: %s", config.safe_dict())

    bot = Bot(config.bot_token,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    me = await bot.get_me()
    log.info("Starting @%s", me.username)

    engine.bind(bot)
    engine.on_finished = build_broadcast_hook(bot)
    scheduler.start(bot)
    await start_health_server()

    dp = build_dispatcher()
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await scheduler.stop()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Stopped")
