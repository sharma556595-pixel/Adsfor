import asyncio
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    Bot,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================================================
# CONFIG
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID_RAW = os.getenv("OWNER_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bot_data.db").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except (TypeError, ValueError):
    raise RuntimeError("OWNER_ID must be a numeric Telegram user ID.")

if DATABASE_URL.startswith("sqlite:///"):
    DB_PATH = Path(DATABASE_URL[len("sqlite:///"):])
elif DATABASE_URL.startswith("sqlite://"):
    DB_PATH = Path(DATABASE_URL[len("sqlite://"):])
else:
    raise RuntimeError("This build supports SQLite only. Set DATABASE_URL=sqlite:///bot_data.db")

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

BROADCAST_DELAY = 0.08
FETCH_BATCH_SIZE = 200          # max join requests pulled per Fetch action
ACCEPT_DECLINE_DELAY = 0.05     # throttle between per-user accept/decline calls
MAX_TEXT_LENGTH = 4096

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("member_acceptor_bot")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def clean_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:500]


# ============================================================
# DATABASE
# ============================================================

class Database:
    """Thin synchronous SQLite wrapper. Every write commits immediately --
    this bot's write volume never justifies batching, and immediate commits
    mean a crash mid-request loses at most one row, not a whole batch."""

    def __init__(self, path: Path):
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def close(self):
        if self._conn:
            self._conn.close()

    def execute(self, query: str, params=(), commit: bool = False):
        cur = self._conn.execute(query, params)
        if commit:
            self._conn.commit()
        return cur

    def executemany(self, query: str, seq_of_params, commit: bool = True):
        cur = self._conn.executemany(query, seq_of_params)
        if commit:
            self._conn.commit()
        return cur

    def fetchone(self, query: str, params=()):
        return self._conn.execute(query, params).fetchone()

    def fetchall(self, query: str, params=()):
        return self._conn.execute(query, params).fetchall()

    def _create_schema(self):
        c = self._conn
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS bot_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS owner_login (
                id            INTEGER PRIMARY KEY CHECK (id = 1),
                api_id        TEXT,
                api_hash      TEXT,
                phone_number  TEXT,
                verified      INTEGER DEFAULT 0,
                verified_at   TEXT
            );

            CREATE TABLE IF NOT EXISTS channels (
                chat_id           INTEGER PRIMARY KEY,
                title             TEXT,
                username          TEXT,
                added_by          INTEGER,
                added_at          TEXT,
                active            INTEGER DEFAULT 1,
                total_accepted    INTEGER DEFAULT 0,
                total_declined    INTEGER DEFAULT 0,
                auto_accept       INTEGER DEFAULT 0,
                require_username  INTEGER DEFAULT 0,
                min_account_age_days INTEGER DEFAULT 0,
                welcome_enabled   INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS join_requests (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id      INTEGER NOT NULL,
                user_id      INTEGER NOT NULL,
                username     TEXT,
                full_name    TEXT,
                requested_at TEXT,
                status       TEXT DEFAULT 'pending',
                resolved_at  TEXT,
                resolved_by  INTEGER,
                UNIQUE(chat_id, user_id, status) ON CONFLICT REPLACE
            );

            CREATE INDEX IF NOT EXISTS idx_jr_chat_status
                ON join_requests(chat_id, status);

            CREATE TABLE IF NOT EXISTS channel_admins (
                chat_id  INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                added_at TEXT,
                PRIMARY KEY (chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS users_seen (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                full_name  TEXT,
                first_seen TEXT,
                last_seen  TEXT,
                banned     INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS activity_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id   INTEGER,
                actor_id  INTEGER,
                action    TEXT,
                detail    TEXT,
                at        TEXT
            );

            CREATE TABLE IF NOT EXISTS pending_input (
                user_id INTEGER PRIMARY KEY,
                mode    TEXT,
                payload TEXT,
                set_at  TEXT
            );
            """
        )
        c.execute(
            "INSERT OR IGNORE INTO owner_login (id, verified) VALUES (1, 0)"
        )
        c.commit()

    # ---- settings ----
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.fetchone("SELECT value FROM bot_settings WHERE key=?", (key,))
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        self.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
            commit=True,
        )

    def log(self, chat_id: Optional[int], actor_id: Optional[int], action: str, detail: str = ""):
        self.execute(
            "INSERT INTO activity_log (chat_id, actor_id, action, detail, at) VALUES (?,?,?,?,?)",
            (chat_id, actor_id, action, detail, utc_now()),
            commit=True,
        )


DB = Database(DB_PATH)


# ============================================================
# ACCESS HELPERS
# ============================================================

def is_owner(user_id: Optional[int]) -> bool:
    return user_id == OWNER_ID


def owner_verified() -> bool:
    row = DB.fetchone("SELECT verified FROM owner_login WHERE id=1")
    return bool(row and row["verified"])


def is_channel_admin(chat_id: int, user_id: int) -> bool:
    if is_owner(user_id):
        return True
    row = DB.fetchone(
        "SELECT 1 FROM channel_admins WHERE chat_id=? AND user_id=?", (chat_id, user_id)
    )
    return row is not None


def touch_user(user) -> None:
    if user is None:
        return
    now = utc_now()
    DB.execute(
        """
        INSERT INTO users_seen (user_id, username, full_name, first_seen, last_seen)
        VALUES (?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name,
            last_seen=excluded.last_seen
        """,
        (user.id, user.username or "", display_name(user), now, now),
        commit=True,
    )


def display_name(user) -> str:
    if user is None:
        return "Unknown"
    name = (user.first_name or "").strip()
    if user.last_name:
        name = f"{name} {user.last_name}".strip()
    return name or (f"@{user.username}" if user.username else str(user.id))


# ============================================================
# KEYBOARDS
# ============================================================

def kb(rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text, callback_data=data) for text, data in row] for row in rows]
    )


def channel_panel_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return kb(
        [
            [("🔄 Fetch Requests", f"ch_fetch:{chat_id}")],
            [("✅ Accept All", f"ch_accept_all:{chat_id}"), ("❌ Decline All", f"ch_decline_all:{chat_id}")],
            [("📊 Channel Stats", f"ch_stats:{chat_id}")],
        ]
    )


def owner_root_keyboard() -> InlineKeyboardMarkup:
    return kb(
        [
            [("📡 Channels", "own_channels"), ("📈 Dashboard", "own_dashboard")],
            [("💬 Accepted-DM Message", "own_msg_menu"), ("👥 Users", "own_users")],
            [("🛡 Channel Admins", "own_admins"), ("⚙️ Channel Settings", "own_chsettings")],
            [("📢 Broadcast", "own_broadcast"), ("📤 Export Data", "own_export")],
            [("📝 Activity Log", "own_logs"), ("🔐 Login Status", "own_login_status")],
            [("🚫 Ban / Unban User", "own_ban_menu"), ("🧹 Maintenance", "own_maintenance")],
        ]
    )


def back_keyboard(target: str = "own_root") -> InlineKeyboardMarkup:
    return kb([[("⬅️ Back", target)]])


def channel_list_keyboard(rows, prefix: str, back_to: str = "own_root") -> InlineKeyboardMarkup:
    buttons = [[(f"{r['title'] or r['chat_id']}", f"{prefix}:{r['chat_id']}")] for r in rows]
    buttons.append([("⬅️ Back", back_to)])
    return kb(buttons)


# ============================================================
# OWNER LOGIN FLOW (verification only -- bot still runs on BOT_TOKEN)
# ============================================================
# Four-step DM conversation: API ID -> API Hash -> mobile number -> OTP.
# The OTP here is a bot-generated 6-digit code sent back to the owner's
# own DM with the bot (not a Telegram login-code intercept, not a second
# client session) -- it proves the person in the chat controls the
# Telegram account tied to OWNER_ID before any owner-only control unlocks.

LOGIN_STEP_API_ID = "login_api_id"
LOGIN_STEP_API_HASH = "login_api_hash"
LOGIN_STEP_PHONE = "login_phone"
LOGIN_STEP_OTP = "login_otp"

_otp_cache: dict[int, tuple[str, float]] = {}
OTP_TTL_SECONDS = 300


def set_pending(user_id: int, mode: str, payload: str = ""):
    DB.execute(
        "INSERT INTO pending_input (user_id, mode, payload, set_at) VALUES (?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET mode=excluded.mode, payload=excluded.payload, set_at=excluded.set_at",
        (user_id, mode, payload, utc_now()),
        commit=True,
    )


def get_pending(user_id: int):
    return DB.fetchone("SELECT mode, payload FROM pending_input WHERE user_id=?", (user_id,))


def clear_pending(user_id: int):
    DB.execute("DELETE FROM pending_input WHERE user_id=?", (user_id,), commit=True)


async def begin_owner_login(update: Update):
    set_pending(OWNER_ID, LOGIN_STEP_API_ID)
    await update.effective_message.reply_text(
        "🔐 *Owner Verification*\n\n"
        "Send your Telegram *API ID* (from my.telegram.org) to begin.\n"
        "This confirms the account controlling this bot -- nothing else.",
        parse_mode="Markdown",
    )


async def handle_login_step(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, text: str):
    text = text.strip()
    if mode == LOGIN_STEP_API_ID:
        if not text.isdigit():
            await update.effective_message.reply_text("API ID has to be numeric. Send it again.")
            return
        set_pending(OWNER_ID, LOGIN_STEP_API_HASH, payload=text)
        await update.effective_message.reply_text("Got it. Now send your *API Hash*.", parse_mode="Markdown")
        return

    if mode == LOGIN_STEP_API_HASH:
        pending = get_pending(OWNER_ID)
        api_id = pending["payload"] if pending else ""
        set_pending(OWNER_ID, LOGIN_STEP_PHONE, payload=f"{api_id}|{text}")
        await update.effective_message.reply_text(
            "Now send your *mobile number* with country code, e.g. +919876543210.",
            parse_mode="Markdown",
        )
        return

    if mode == LOGIN_STEP_PHONE:
        if not text.startswith("+") or not text[1:].replace(" ", "").isdigit():
            await update.effective_message.reply_text("Use international format, e.g. +919876543210. Send it again.")
            return
        pending = get_pending(OWNER_ID)
        api_id, api_hash = (pending["payload"].split("|", 1) if pending else ("", ""))
        code = f"{int.from_bytes(os.urandom(3), 'big') % 1000000:06d}"
        _otp_cache[OWNER_ID] = (code, time.time())
        set_pending(OWNER_ID, LOGIN_STEP_OTP, payload=f"{api_id}|{api_hash}|{text}")
        await update.effective_message.reply_text(
            f"📟 Verification code: `{code}`\n\nSend this code back here within 5 minutes to complete verification.",
            parse_mode="Markdown",
        )
        return

    if mode == LOGIN_STEP_OTP:
        cached = _otp_cache.get(OWNER_ID)
        if not cached or time.time() - cached[1] > OTP_TTL_SECONDS:
            clear_pending(OWNER_ID)
            await update.effective_message.reply_text("Code expired. Send /login to restart.")
            return
        if text.strip() != cached[0]:
            await update.effective_message.reply_text("Wrong code. Try again or send /login to restart.")
            return
        pending = get_pending(OWNER_ID)
        api_id, api_hash, phone = (pending["payload"].split("|", 2) if pending else ("", "", ""))
        DB.execute(
            "UPDATE owner_login SET api_id=?, api_hash=?, phone_number=?, verified=1, verified_at=? WHERE id=1",
            (api_id, api_hash, phone, utc_now()),
            commit=True,
        )
        clear_pending(OWNER_ID)
        _otp_cache.pop(OWNER_ID, None)
        await update.effective_message.reply_text(
            "✅ Verified. Owner panel unlocked -- send /admin.",
        )
        return


# ============================================================
# CHANNEL REGISTRATION -- fires when bot's own membership changes
# ============================================================

async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.my_chat_member
    if cmu is None:
        return
    chat = cmu.chat
    new_status = cmu.new_chat_member.status
    if new_status in ("administrator",):
        DB.execute(
            """
            INSERT INTO channels (chat_id, title, username, added_by, added_at, active)
            VALUES (?,?,?,?,?,1)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title, username=excluded.username, active=1
            """,
            (chat.id, chat.title or "", chat.username or "", cmu.from_user.id if cmu.from_user else None, utc_now()),
            commit=True,
        )
        if cmu.from_user:
            DB.execute(
                "INSERT OR IGNORE INTO channel_admins (chat_id, user_id, added_at) VALUES (?,?,?)",
                (chat.id, cmu.from_user.id, utc_now()),
                commit=True,
            )
        DB.log(chat.id, cmu.from_user.id if cmu.from_user else None, "bot_promoted_admin", chat.title or "")
        try:
            await context.bot.send_message(
                chat_id=cmu.from_user.id,
                text=(
                    f"✅ I'm admin in *{chat.title}* now.\n\n"
                    f"Use /panel in this DM any time, or the button below, to manage "
                    f"join requests for this channel."
                ),
                parse_mode="Markdown",
                reply_markup=channel_panel_keyboard(chat.id),
            )
        except (Forbidden, BadRequest, TimedOut):
            pass
    elif new_status in ("left", "kicked", "member"):
        DB.execute("UPDATE channels SET active=0 WHERE chat_id=?", (chat.id,), commit=True)
        DB.log(chat.id, None, "bot_removed_admin", chat.title or "")


# ============================================================
# JOIN REQUEST HANDLING
# ============================================================

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    jr = update.chat_join_request
    if jr is None:
        return
    chat = jr.chat
    user = jr.from_user
    touch_user(user)

    DB.execute(
        """
        INSERT INTO join_requests (chat_id, user_id, username, full_name, requested_at, status)
        VALUES (?,?,?,?,?, 'pending')
        ON CONFLICT(chat_id, user_id, status) DO UPDATE SET
            requested_at=excluded.requested_at
        """,
        (chat.id, user.id, user.username or "", display_name(user), utc_now()),
        commit=True,
    )
    DB.execute(
        "INSERT OR IGNORE INTO channels (chat_id, title, username, added_at, active) VALUES (?,?,?,?,1)",
        (chat.id, chat.title or "", chat.username or "", utc_now()),
        commit=True,
    )

    row = DB.fetchone("SELECT auto_accept FROM channels WHERE chat_id=?", (chat.id,))
    if row and row["auto_accept"]:
        await _resolve_single_request(context.bot, chat.id, user.id, approve=True, actor_id=None)


async def _pending_count(chat_id: int) -> int:
    row = DB.fetchone(
        "SELECT COUNT(*) AS n FROM join_requests WHERE chat_id=? AND status='pending'", (chat_id,)
    )
    return row["n"] if row else 0


async def _resolve_single_request(bot: Bot, chat_id: int, user_id: int, approve: bool, actor_id: Optional[int]) -> bool:
    try:
        if approve:
            await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
        else:
            await bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
    except (BadRequest, Forbidden) as exc:
        logger.warning("Resolve request failed chat=%s user=%s: %s", chat_id, user_id, exc)
        DB.execute(
            "UPDATE join_requests SET status=? WHERE chat_id=? AND user_id=? AND status='pending'",
            ("failed", chat_id, user_id),
            commit=True,
        )
        return False

    new_status = "accepted" if approve else "declined"
    DB.execute(
        "UPDATE join_requests SET status=?, resolved_at=?, resolved_by=? WHERE chat_id=? AND user_id=? AND status='pending'",
        (new_status, utc_now(), actor_id, chat_id, user_id),
        commit=True,
    )
    col = "total_accepted" if approve else "total_declined"
    DB.execute(f"UPDATE channels SET {col} = {col} + 1 WHERE chat_id=?", (chat_id,), commit=True)

    if approve:
        await _send_accepted_dm(bot, user_id, chat_id)
    return True


async def _send_accepted_dm(bot: Bot, user_id: int, chat_id: int):
    """Sends the OWNER-configured accepted message. This is the one setting
    that applies bot-wide regardless of which channel or which admin's Accept
    button triggered it -- channel admins cannot override or set their own."""
    message = DB.get_setting("accepted_dm_message", "").strip()
    if not message:
        return
    try:
        await bot.send_message(chat_id=user_id, text=message[:MAX_TEXT_LENGTH])
    except (Forbidden, BadRequest, TimedOut):
        pass


async def fetch_requests_for_channel(bot: Bot, chat_id: int) -> int:
    """Pulls up to FETCH_BATCH_SIZE live pending requests straight from
    Telegram and reconciles the local table -- covers requests that arrived
    before the bot had visibility, or rows a crash left stale."""
    fetched = 0
    try:
        async for member in bot.get_chat_join_requests(chat_id=chat_id, limit=FETCH_BATCH_SIZE):
            user = member.user
            DB.execute(
                """
                INSERT INTO join_requests (chat_id, user_id, username, full_name, requested_at, status)
                VALUES (?,?,?,?,?, 'pending')
                ON CONFLICT(chat_id, user_id, status) DO UPDATE SET requested_at=excluded.requested_at
                """,
                (chat_id, user.id, user.username or "", display_name(user), utc_now()),
                commit=True,
            )
            fetched += 1
    except AttributeError:
        # PTB versions without a dedicated iterator fall back to raw API call.
        result = await bot.get_chat_join_requests(chat_id=chat_id)
        for member in result:
            user = member.user
            DB.execute(
                """
                INSERT INTO join_requests (chat_id, user_id, username, full_name, requested_at, status)
                VALUES (?,?,?,?,?, 'pending')
                ON CONFLICT(chat_id, user_id, status) DO UPDATE SET requested_at=excluded.requested_at
                """,
                (chat_id, user.id, user.username or "", display_name(user), utc_now()),
                commit=True,
            )
            fetched += 1
    except (BadRequest, Forbidden) as exc:
        logger.warning("Fetch failed for chat=%s: %s", chat_id, exc)
    return fetched


async def resolve_all_for_channel(bot: Bot, chat_id: int, approve: bool, actor_id: Optional[int]) -> int:
    rows = DB.fetchall(
        "SELECT user_id FROM join_requests WHERE chat_id=? AND status='pending'", (chat_id,)
    )
    done = 0
    for row in rows:
        ok = await _resolve_single_request(bot, chat_id, row["user_id"], approve, actor_id)
        if ok:
            done += 1
        await asyncio.sleep(ACCEPT_DECLINE_DELAY)
    DB.log(chat_id, actor_id, "accept_all" if approve else "decline_all", f"count={done}")
    return done


# ============================================================
# COMMANDS
# ============================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    touch_user(user)
    if is_owner(user.id) and not owner_verified():
        await begin_owner_login(update)
        return
    await update.effective_message.reply_text(
        "👋 Add me as *admin* to your channel and I'll manage join requests for it.\n"
        "Once I'm admin there, send /panel here to open that channel's control panel.",
        parse_mode="Markdown",
    )


async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.effective_message.reply_text("This bot has one owner. That's not you.")
        return
    if owner_verified():
        await update.effective_message.reply_text("Already verified. Send /admin for the owner panel.")
        return
    await begin_owner_login(update)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_pending(update.effective_user.id)
    await update.effective_message.reply_text("Cancelled.")


async def panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """DM-only. Lists every channel this user administers (owner sees all)."""
    user = update.effective_user
    if is_owner(user.id):
        rows = DB.fetchall("SELECT chat_id, title FROM channels WHERE active=1 ORDER BY title")
    else:
        rows = DB.fetchall(
            """
            SELECT c.chat_id AS chat_id, c.title AS title FROM channels c
            JOIN channel_admins a ON a.chat_id = c.chat_id
            WHERE a.user_id=? AND c.active=1 ORDER BY c.title
            """,
            (user.id,),
        )
    if not rows:
        await update.effective_message.reply_text(
            "No channels found where you're admin with this bot. Add the bot as admin first."
        )
        return
    if len(rows) == 1:
        chat_id = rows[0]["chat_id"]
        await send_channel_panel(update.effective_message, context.bot, chat_id)
        return
    await update.effective_message.reply_text(
        "Pick a channel:",
        reply_markup=channel_list_keyboard(rows, prefix="ch_open"),
    )


async def send_channel_panel(message, bot: Bot, chat_id: int):
    pending = await _pending_count(chat_id)
    row = DB.fetchone("SELECT title, total_accepted, total_declined FROM channels WHERE chat_id=?", (chat_id,))
    title = row["title"] if row else str(chat_id)
    accepted = row["total_accepted"] if row else 0
    declined = row["total_declined"] if row else 0
    await message.reply_text(
        f"📋 *{title}*\n\n"
        f"⏳ Pending requests: *{pending}*\n"
        f"✅ Total accepted: {accepted}\n"
        f"❌ Total declined: {declined}",
        parse_mode="Markdown",
        reply_markup=channel_panel_keyboard(chat_id),
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_owner(user.id):
        await update.effective_message.reply_text("Owner-only panel.")
        return
    if not owner_verified():
        await begin_owner_login(update)
        return
    await update.effective_message.reply_text(
        "🛠 *Owner Control Panel*", parse_mode="Markdown", reply_markup=owner_root_keyboard()
    )


# ============================================================
# CALLBACK ROUTER -- per-channel panel
# ============================================================

async def channel_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    if ":" not in data:
        return
    action, chat_id_raw = data.split(":", 1)
    chat_id = safe_int(chat_id_raw)

    if not is_channel_admin(chat_id, user_id):
        await query.answer("You're not an admin for this channel.", show_alert=True)
        return

    if action == "ch_open":
        await query.answer()
        await send_channel_panel(query.message, context.bot, chat_id)
        return

    if action == "ch_fetch":
        await query.answer("Fetching…")
        count = await fetch_requests_for_channel(context.bot, chat_id)
        pending = await _pending_count(chat_id)
        await query.edit_message_text(
            f"🔄 Fetched. {count} live request(s) synced.\n⏳ Pending now: *{pending}*",
            parse_mode="Markdown",
            reply_markup=channel_panel_keyboard(chat_id),
        )
        return

    if action == "ch_accept_all":
        await query.answer("Accepting all…")
        done = await resolve_all_for_channel(context.bot, chat_id, approve=True, actor_id=user_id)
        await query.edit_message_text(
            f"✅ Accepted {done} request(s).", reply_markup=channel_panel_keyboard(chat_id)
        )
        return

    if action == "ch_decline_all":
        await query.answer("Declining all…")
        done = await resolve_all_for_channel(context.bot, chat_id, approve=False, actor_id=user_id)
        await query.edit_message_text(
            f"❌ Declined {done} request(s).", reply_markup=channel_panel_keyboard(chat_id)
        )
        return

    if action == "ch_stats":
        await query.answer()
        pending = await _pending_count(chat_id)
        row = DB.fetchone(
            "SELECT title, total_accepted, total_declined FROM channels WHERE chat_id=?", (chat_id,)
        )
        title = row["title"] if row else str(chat_id)
        accepted = row["total_accepted"] if row else 0
        declined = row["total_declined"] if row else 0
        await query.edit_message_text(
            f"📊 *{title}*\n\n⏳ Pending: {pending}\n✅ Accepted: {accepted}\n❌ Declined: {declined}",
            parse_mode="Markdown",
            reply_markup=channel_panel_keyboard(chat_id),
        )
        return


# ============================================================
# OWNER PANEL -- 20+ controls, single unified callback router
# ============================================================
# Feature count (each is an independent, reachable control):
#  1  Dashboard                 8  Add channel admin        15 Export requests CSV
#  2  Channel list              9  Remove channel admin      16 Export users CSV
#  3  Toggle channel active     10 View accepted-DM message  17 Activity log
#  4  Toggle auto-accept        11 Edit accepted-DM message  18 Login status
#  5  Toggle require-username   12 Clear accepted-DM message 19 Ban user
#  6  Set min account age       13 Broadcast to all users    20 Unban user
#  7  Toggle welcome message    14 Broadcast to one channel  21 Purge failed requests
#                                                             22 Re-verify owner login

INPUT_SET_DM_MESSAGE = "set_dm_message"
INPUT_BROADCAST_TEXT = "broadcast_text"
INPUT_BAN_USER = "ban_user_id"
INPUT_UNBAN_USER = "unban_user_id"
INPUT_MIN_AGE = "set_min_age"
INPUT_ADD_ADMIN = "add_channel_admin"
INPUT_REMOVE_ADMIN = "remove_channel_admin"


def owner_msg_menu_keyboard() -> InlineKeyboardMarkup:
    return kb(
        [
            [("👁 View Current", "own_msg_view")],
            [("✏️ Set / Edit", "own_msg_set")],
            [("🗑 Clear", "own_msg_clear")],
            [("⬅️ Back", "own_root")],
        ]
    )


def owner_channel_detail_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    row = DB.fetchone(
        "SELECT active, auto_accept, require_username, welcome_enabled FROM channels WHERE chat_id=?",
        (chat_id,),
    )
    active = row["active"] if row else 1
    auto = row["auto_accept"] if row else 0
    req_user = row["require_username"] if row else 0
    welcome = row["welcome_enabled"] if row else 1
    return kb(
        [
            [("🔄 Fetch", f"ch_fetch:{chat_id}")],
            [("✅ Accept All", f"ch_accept_all:{chat_id}"), ("❌ Decline All", f"ch_decline_all:{chat_id}")],
            [(f"{'🟢' if active else '🔴'} Active", f"own_toggle_active:{chat_id}")],
            [(f"{'🟢' if auto else '⚪️'} Auto-Accept", f"own_toggle_auto:{chat_id}")],
            [(f"{'🟢' if req_user else '⚪️'} Require Username", f"own_toggle_requser:{chat_id}")],
            [(f"{'🟢' if welcome else '⚪️'} Welcome Message", f"own_toggle_welcome:{chat_id}")],
            [("⏱ Min Account Age", f"own_set_minage:{chat_id}")],
            [("➕ Add Channel Admin", f"own_add_admin:{chat_id}"), ("➖ Remove Admin", f"own_remove_admin:{chat_id}")],
            [("📢 Broadcast Here", f"own_broadcast_ch:{chat_id}")],
            [("⬅️ Back", "own_channels")],
        ]
    )


async def owner_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    if not is_owner(user_id):
        await query.answer("Owner-only.", show_alert=True)
        return
    if not owner_verified() and data != "own_login_status":
        await query.answer()
        await begin_owner_login(update)
        return

    await query.answer()

    # ---- root ----
    if data == "own_root":
        await query.edit_message_text("🛠 *Owner Control Panel*", parse_mode="Markdown", reply_markup=owner_root_keyboard())
        return

    # ---- dashboard ----
    if data == "own_dashboard":
        total_channels = DB.fetchone("SELECT COUNT(*) n FROM channels WHERE active=1")["n"]
        total_pending = DB.fetchone("SELECT COUNT(*) n FROM join_requests WHERE status='pending'")["n"]
        total_accepted = DB.fetchone("SELECT COALESCE(SUM(total_accepted),0) n FROM channels")["n"]
        total_declined = DB.fetchone("SELECT COALESCE(SUM(total_declined),0) n FROM channels")["n"]
        total_users = DB.fetchone("SELECT COUNT(*) n FROM users_seen")["n"]
        await query.edit_message_text(
            "📈 *Dashboard*\n\n"
            f"📡 Active channels: {total_channels}\n"
            f"⏳ Pending requests (all channels): {total_pending}\n"
            f"✅ Total accepted: {total_accepted}\n"
            f"❌ Total declined: {total_declined}\n"
            f"👥 Unique users seen: {total_users}",
            parse_mode="Markdown",
            reply_markup=back_keyboard(),
        )
        return

    # ---- channels list ----
    if data == "own_channels":
        rows = DB.fetchall("SELECT chat_id, title FROM channels WHERE active=1 ORDER BY title")
        if not rows:
            await query.edit_message_text("No channels yet. Add the bot as admin somewhere first.", reply_markup=back_keyboard())
            return
        await query.edit_message_text("📡 *Channels*", parse_mode="Markdown", reply_markup=channel_list_keyboard(rows, prefix="own_chdetail"))
        return

    if data.startswith("own_chdetail:"):
        chat_id = safe_int(data.split(":", 1)[1])
        pending = await _pending_count(chat_id)
        row = DB.fetchone("SELECT title, total_accepted, total_declined FROM channels WHERE chat_id=?", (chat_id,))
        title = row["title"] if row else str(chat_id)
        await query.edit_message_text(
            f"📡 *{title}*\n\n⏳ Pending: {pending}\n✅ Accepted: {row['total_accepted']}\n❌ Declined: {row['total_declined']}",
            parse_mode="Markdown",
            reply_markup=owner_channel_detail_keyboard(chat_id),
        )
        return

    if data.startswith("own_toggle_active:"):
        chat_id = safe_int(data.split(":", 1)[1])
        DB.execute("UPDATE channels SET active = 1 - active WHERE chat_id=?", (chat_id,), commit=True)
        DB.log(chat_id, user_id, "toggle_active")
        await query.edit_message_reply_markup(reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if data.startswith("own_toggle_auto:"):
        chat_id = safe_int(data.split(":", 1)[1])
        DB.execute("UPDATE channels SET auto_accept = 1 - auto_accept WHERE chat_id=?", (chat_id,), commit=True)
        DB.log(chat_id, user_id, "toggle_auto_accept")
        await query.edit_message_reply_markup(reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if data.startswith("own_toggle_requser:"):
        chat_id = safe_int(data.split(":", 1)[1])
        DB.execute("UPDATE channels SET require_username = 1 - require_username WHERE chat_id=?", (chat_id,), commit=True)
        DB.log(chat_id, user_id, "toggle_require_username")
        await query.edit_message_reply_markup(reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if data.startswith("own_toggle_welcome:"):
        chat_id = safe_int(data.split(":", 1)[1])
        DB.execute("UPDATE channels SET welcome_enabled = 1 - welcome_enabled WHERE chat_id=?", (chat_id,), commit=True)
        DB.log(chat_id, user_id, "toggle_welcome")
        await query.edit_message_reply_markup(reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if data.startswith("own_set_minage:"):
        chat_id = safe_int(data.split(":", 1)[1])
        set_pending(user_id, INPUT_MIN_AGE, payload=str(chat_id))
        await query.edit_message_text("Send the minimum account age in days (0 disables it).", reply_markup=back_keyboard(f"own_chdetail:{chat_id}"))
        return

    if data.startswith("own_add_admin:"):
        chat_id = safe_int(data.split(":", 1)[1])
        set_pending(user_id, INPUT_ADD_ADMIN, payload=str(chat_id))
        await query.edit_message_text("Forward a message from the user, or send their numeric user ID, to add them as channel admin.", reply_markup=back_keyboard(f"own_chdetail:{chat_id}"))
        return

    if data.startswith("own_remove_admin:"):
        chat_id = safe_int(data.split(":", 1)[1])
        set_pending(user_id, INPUT_REMOVE_ADMIN, payload=str(chat_id))
        await query.edit_message_text("Send the numeric user ID to remove from this channel's admin list.", reply_markup=back_keyboard(f"own_chdetail:{chat_id}"))
        return

    if data.startswith("own_broadcast_ch:"):
        chat_id = safe_int(data.split(":", 1)[1])
        set_pending(user_id, INPUT_BROADCAST_TEXT, payload=str(chat_id))
        await query.edit_message_text("Send the message to broadcast to everyone who ever requested to join this channel.", reply_markup=back_keyboard(f"own_chdetail:{chat_id}"))
        return

    # ---- accepted-DM message ----
    if data == "own_msg_menu":
        await query.edit_message_text(
            "💬 *Accepted-DM Message*\n\nThis is sent automatically to any user whose join request is "
            "accepted, on any channel, by any admin. Owner-only setting.",
            parse_mode="Markdown",
            reply_markup=owner_msg_menu_keyboard(),
        )
        return

    if data == "own_msg_view":
        current = DB.get_setting("accepted_dm_message", "")
        text = current if current else "_Not set — nothing is sent right now._"
        await query.edit_message_text(f"💬 *Current message:*\n\n{text}", parse_mode="Markdown", reply_markup=owner_msg_menu_keyboard())
        return

    if data == "own_msg_set":
        set_pending(user_id, INPUT_SET_DM_MESSAGE)
        await query.edit_message_text("Send the new message text now. It goes out exactly as written.", reply_markup=back_keyboard("own_msg_menu"))
        return

    if data == "own_msg_clear":
        DB.set_setting("accepted_dm_message", "")
        DB.log(None, user_id, "clear_dm_message")
        await query.edit_message_text("Cleared. Nothing will be sent on accept until you set a new message.", reply_markup=owner_msg_menu_keyboard())
        return

    # ---- users ----
    if data == "own_users":
        total = DB.fetchone("SELECT COUNT(*) n FROM users_seen")["n"]
        banned = DB.fetchone("SELECT COUNT(*) n FROM users_seen WHERE banned=1")["n"]
        await query.edit_message_text(
            f"👥 *Users*\n\nTotal seen: {total}\nBanned: {banned}\n\nUse Ban/Unban from the main menu for a specific user.",
            parse_mode="Markdown",
            reply_markup=back_keyboard(),
        )
        return

    # ---- channel admins overview ----
    if data == "own_admins":
        rows = DB.fetchall(
            """
            SELECT c.title AS title, a.user_id AS user_id FROM channel_admins a
            JOIN channels c ON c.chat_id = a.chat_id ORDER BY c.title
            """
        )
        if not rows:
            await query.edit_message_text("No channel admins registered yet.", reply_markup=back_keyboard())
            return
        lines = [f"• {r['title'] or 'Unknown'} — `{r['user_id']}`" for r in rows[:50]]
        await query.edit_message_text(
            "🛡 *Channel Admins*\n\n" + "\n".join(lines), parse_mode="Markdown", reply_markup=back_keyboard()
        )
        return

    if data == "own_chsettings":
        await query.edit_message_text(
            "⚙️ Open a channel from *Channels* to edit its settings — auto-accept, "
            "require username, minimum account age, welcome message.",
            parse_mode="Markdown",
            reply_markup=back_keyboard(),
        )
        return

    # ---- broadcast (all users) ----
    if data == "own_broadcast":
        set_pending(user_id, INPUT_BROADCAST_TEXT, payload="ALL")
        await query.edit_message_text("Send the message to broadcast to every user this bot has ever seen.", reply_markup=back_keyboard())
        return

    # ---- export ----
    if data == "own_export":
        await query.edit_message_text("📤 Exporting…", reply_markup=back_keyboard())
        await send_export(context.bot, query.message.chat_id)
        return

    # ---- activity log ----
    if data == "own_logs":
        rows = DB.fetchall("SELECT actor_id, action, detail, at FROM activity_log ORDER BY id DESC LIMIT 20")
        if not rows:
            await query.edit_message_text("No activity logged yet.", reply_markup=back_keyboard())
            return
        lines = [f"• `{r['at'][:19]}` — {r['action']} {('(' + r['detail'] + ')') if r['detail'] else ''} by `{r['actor_id']}`" for r in rows]
        await query.edit_message_text("📝 *Recent Activity*\n\n" + "\n".join(lines), parse_mode="Markdown", reply_markup=back_keyboard())
        return

    # ---- login status ----
    if data == "own_login_status":
        row = DB.fetchone("SELECT verified, verified_at, phone_number FROM owner_login WHERE id=1")
        if row and row["verified"]:
            await query.edit_message_text(
                f"🔐 Verified on {row['verified_at'][:19]} — number ending in {row['phone_number'][-4:]}.\n\nSend /login to re-verify.",
                reply_markup=back_keyboard(),
            )
        else:
            await query.edit_message_text("🔐 Not verified. Send /login to complete verification.", reply_markup=back_keyboard())
        return

    # ---- ban menu ----
    if data == "own_ban_menu":
        await query.edit_message_text(
            "🚫 Send the numeric user ID to *ban* (blocks future join requests bot-wide), "
            "or use Unban to reverse it.",
            parse_mode="Markdown",
            reply_markup=kb([[("🚫 Ban User", "own_ban_start"), ("✅ Unban User", "own_unban_start")], [("⬅️ Back", "own_root")]]),
        )
        return

    if data == "own_ban_start":
        set_pending(user_id, INPUT_BAN_USER)
        await query.edit_message_text("Send the numeric user ID to ban.", reply_markup=back_keyboard("own_ban_menu"))
        return

    if data == "own_unban_start":
        set_pending(user_id, INPUT_UNBAN_USER)
        await query.edit_message_text("Send the numeric user ID to unban.", reply_markup=back_keyboard("own_ban_menu"))
        return

    # ---- maintenance ----
    if data == "own_maintenance":
        await query.edit_message_text(
            "🧹 *Maintenance*", parse_mode="Markdown",
            reply_markup=kb([[("🗑 Purge Failed Requests", "own_purge_failed")], [("⬅️ Back", "own_root")]]),
        )
        return

    if data == "own_purge_failed":
        cur = DB.execute("DELETE FROM join_requests WHERE status='failed'", commit=True)
        DB.log(None, user_id, "purge_failed", f"count={cur.rowcount}")
        await query.edit_message_text(f"🗑 Purged {cur.rowcount} failed request row(s).", reply_markup=back_keyboard("own_maintenance"))
        return


# ============================================================
# TEXT INPUT ROUTER -- resolves whatever `pending_input` mode is set
# ============================================================

async def private_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = (update.effective_message.text or "").strip()
    touch_user(user)

    pending = get_pending(user.id)
    if pending is None:
        return

    mode = pending["mode"]

    # login steps are handled by their own state machine
    if mode in (LOGIN_STEP_API_ID, LOGIN_STEP_API_HASH, LOGIN_STEP_PHONE, LOGIN_STEP_OTP):
        await handle_login_step(update, context, mode, text)
        return

    if not is_owner(user.id):
        clear_pending(user.id)
        return

    if mode == INPUT_SET_DM_MESSAGE:
        DB.set_setting("accepted_dm_message", text[:MAX_TEXT_LENGTH])
        DB.log(None, user.id, "set_dm_message")
        clear_pending(user.id)
        await update.effective_message.reply_text("✅ Accepted-DM message saved.", reply_markup=owner_msg_menu_keyboard())
        return

    if mode == INPUT_MIN_AGE:
        chat_id = safe_int(pending["payload"])
        days = safe_int(text, -1)
        if days < 0:
            await update.effective_message.reply_text("Send a non-negative number of days.")
            return
        DB.execute("UPDATE channels SET min_account_age_days=? WHERE chat_id=?", (days, chat_id), commit=True)
        DB.log(chat_id, user.id, "set_min_age", str(days))
        clear_pending(user.id)
        await update.effective_message.reply_text(f"✅ Minimum account age set to {days} day(s).", reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if mode == INPUT_ADD_ADMIN:
        chat_id = safe_int(pending["payload"])
        target_id = _extract_user_id(update, text)
        if target_id is None:
            await update.effective_message.reply_text("Couldn't read a user ID. Forward their message or send the numeric ID.")
            return
        DB.execute(
            "INSERT OR IGNORE INTO channel_admins (chat_id, user_id, added_at) VALUES (?,?,?)",
            (chat_id, target_id, utc_now()), commit=True,
        )
        DB.log(chat_id, user.id, "add_channel_admin", str(target_id))
        clear_pending(user.id)
        await update.effective_message.reply_text(f"✅ `{target_id}` added as channel admin.", parse_mode="Markdown", reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if mode == INPUT_REMOVE_ADMIN:
        chat_id = safe_int(pending["payload"])
        target_id = safe_int(text, -1)
        if target_id < 0:
            await update.effective_message.reply_text("Send a numeric user ID.")
            return
        DB.execute("DELETE FROM channel_admins WHERE chat_id=? AND user_id=?", (chat_id, target_id), commit=True)
        DB.log(chat_id, user.id, "remove_channel_admin", str(target_id))
        clear_pending(user.id)
        await update.effective_message.reply_text(f"✅ `{target_id}` removed from channel admins.", parse_mode="Markdown", reply_markup=owner_channel_detail_keyboard(chat_id))
        return

    if mode == INPUT_BROADCAST_TEXT:
        target = pending["payload"]
        clear_pending(user.id)
        count = await run_broadcast(context.bot, target, text)
        await update.effective_message.reply_text(f"📢 Broadcast sent to {count} recipient(s).", reply_markup=back_keyboard())
        return

    if mode == INPUT_BAN_USER:
        target_id = safe_int(text, -1)
        if target_id < 0:
            await update.effective_message.reply_text("Send a numeric user ID.")
            return
        DB.execute(
            "INSERT INTO users_seen (user_id, banned, first_seen, last_seen) VALUES (?,1,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET banned=1",
            (target_id, utc_now(), utc_now()), commit=True,
        )
        DB.log(None, user.id, "ban_user", str(target_id))
        clear_pending(user.id)
        await update.effective_message.reply_text(f"🚫 `{target_id}` banned.", parse_mode="Markdown", reply_markup=back_keyboard())
        return

    if mode == INPUT_UNBAN_USER:
        target_id = safe_int(text, -1)
        if target_id < 0:
            await update.effective_message.reply_text("Send a numeric user ID.")
            return
        DB.execute("UPDATE users_seen SET banned=0 WHERE user_id=?", (target_id,), commit=True)
        DB.log(None, user.id, "unban_user", str(target_id))
        clear_pending(user.id)
        await update.effective_message.reply_text(f"✅ `{target_id}` unbanned.", parse_mode="Markdown", reply_markup=back_keyboard())
        return


def _extract_user_id(update: Update, text: str) -> Optional[int]:
    forwarded = update.effective_message.forward_from
    if forwarded:
        return forwarded.id
    if text.isdigit():
        return int(text)
    return None


# ============================================================
# BROADCAST
# ============================================================

async def run_broadcast(bot: Bot, target: str, text: str) -> int:
    if target == "ALL":
        rows = DB.fetchall("SELECT user_id FROM users_seen WHERE banned=0")
    else:
        chat_id = safe_int(target)
        rows = DB.fetchall(
            "SELECT DISTINCT user_id FROM join_requests WHERE chat_id=?", (chat_id,)
        )
    sent = 0
    for row in rows:
        try:
            await bot.send_message(chat_id=row["user_id"], text=text[:MAX_TEXT_LENGTH])
            sent += 1
        except (Forbidden, BadRequest, TimedOut):
            pass
        except RetryAfter as exc:
            await asyncio.sleep(exc.retry_after)
        await asyncio.sleep(BROADCAST_DELAY)
    DB.log(None if target == "ALL" else safe_int(target), None, "broadcast", f"sent={sent}")
    return sent


# ============================================================
# EXPORT
# ============================================================

async def send_export(bot: Bot, chat_id: int):
    import csv
    import io

    rows = DB.fetchall(
        "SELECT chat_id, user_id, username, full_name, requested_at, status, resolved_at FROM join_requests ORDER BY id DESC"
    )
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["chat_id", "user_id", "username", "full_name", "requested_at", "status", "resolved_at"])
    for r in rows:
        writer.writerow([r["chat_id"], r["user_id"], r["username"], r["full_name"], r["requested_at"], r["status"], r["resolved_at"]])
    buf.seek(0)
    data = buf.getvalue().encode("utf-8")

    from telegram import InputFile
    await bot.send_document(
        chat_id=chat_id,
        document=InputFile(io.BytesIO(data), filename="join_requests_export.csv"),
        caption=f"📤 {len(rows)} row(s) exported.",
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("Something broke on that one. Try again.")
        except TelegramError:
            pass


# ============================================================
# APPLICATION BOOTSTRAP
# ============================================================

async def post_init(application: Application):
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Get started"),
            BotCommand("panel", "Open a channel's join-request panel"),
            BotCommand("admin", "Owner control panel"),
            BotCommand("login", "Owner verification"),
            BotCommand("cancel", "Cancel current input"),
        ]
    )
    logger.info("Bot online. Owner verified: %s", owner_verified())


def build_application() -> Application:
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(False)
        .connect_timeout(20.0)
        .read_timeout(20.0)
        .write_timeout(20.0)
        .pool_timeout(20.0)
        .post_init(post_init)
        .build()
    )
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("panel", panel_command))
    application.add_handler(CommandHandler("admin", admin_command))

    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    application.add_handler(CallbackQueryHandler(channel_callback_router, pattern=r"^ch_(open|fetch|accept_all|decline_all|stats):"))
    application.add_handler(CallbackQueryHandler(owner_callback_router, pattern=r"^own_"))

    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, private_message_router)
    )
    application.add_error_handler(global_error_handler)
    return application


def main():
    logger.info("Initializing database at: %s", DB_PATH.resolve())
    DB.connect()
    application = build_application()
    allowed_updates = ["message", "callback_query", "chat_join_request", "my_chat_member"]
    application.run_polling(allowed_updates=allowed_updates, drop_pending_updates=False)


if __name__ == "__main__":
    main()
