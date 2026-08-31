import asyncio
import json
import logging
import os
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import (
    GetChatInviteImportersRequest,
    HideAllChatJoinRequestsRequest,
)
from telethon.tl.types import InputUserEmpty
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UserAlreadyParticipantError,
)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "8753914631"))
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///member_acceptor.db").strip()
DB_PATH = Path(DATABASE_URL.replace("sqlite:///", "", 1))
PORT = int(os.getenv("PORT", "10000"))
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "telegram-webhook").strip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
POLL_SECONDS = max(10, int(os.getenv("POLL_SECONDS", "30")))
MAX_LOGIN_WAIT = 300

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not OWNER_ID:
    raise RuntimeError("OWNER_ID must be a numeric Telegram user ID")

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("member_acceptor")


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class DB:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.init()

    def init(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_seen TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            blocked INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS telegram_sessions (
            user_id INTEGER PRIMARY KEY,
            api_id INTEGER NOT NULL,
            api_hash TEXT NOT NULL,
            phone TEXT NOT NULL,
            session TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            username TEXT,
            title TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(owner_id, channel_id)
        );
        CREATE TABLE IF NOT EXISTS settings (
            owner_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY(owner_id, key)
        );
        CREATE TABLE IF NOT EXISTS request_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_request_contacts (
            owner_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            user_chat_id INTEGER,
            created_at TEXT NOT NULL,
            PRIMARY KEY(owner_id, channel_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER,
            kind TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL
        );
        """)
        self.conn.execute("INSERT OR IGNORE INTO users(user_id,last_seen,role) VALUES(?,?,?)", (OWNER_ID, now(), "owner"))
        self.conn.commit()

    def execute(self, q, p=(), commit=True):
        cur = self.conn.execute(q, p)
        if commit:
            self.conn.commit()
        return cur

    def one(self, q, p=()):
        return self.execute(q, p, False).fetchone()

    def all(self, q, p=()):
        return self.execute(q, p, False).fetchall()

    def user(self, u):
        self.execute("""INSERT INTO users(user_id,username,first_name,last_seen) VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name,last_seen=excluded.last_seen""",
        (u.id, u.username, u.first_name, now()))

    def setting(self, uid, key, default=""):
        r = self.one("SELECT value FROM settings WHERE owner_id=? AND key=?", (uid, key))
        return r["value"] if r else default

    def set_setting(self, uid, key, value):
        self.execute("INSERT INTO settings(owner_id,key,value) VALUES(?,?,?) ON CONFLICT(owner_id,key) DO UPDATE SET value=excluded.value", (uid,key,str(value)))

    def log(self, owner_id, kind, details=""):
        self.execute("INSERT INTO events(owner_id,kind,details,created_at) VALUES(?,?,?,?)", (owner_id,kind,str(details)[:4000],now()))

    def session(self, uid):
        return self.one("SELECT * FROM telegram_sessions WHERE user_id=?", (uid,))

    def save_session(self, uid, api_id, api_hash, phone, session):
        self.execute("""INSERT INTO telegram_sessions(user_id,api_id,api_hash,phone,session,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id,api_hash=excluded.api_hash,phone=excluded.phone,session=excluded.session,updated_at=excluded.updated_at""",
        (uid,api_id,api_hash,phone,session,now(),now()))

    def delete_session(self, uid):
        self.execute("DELETE FROM telegram_sessions WHERE user_id=?", (uid,))

    def add_channel(self, owner, channel_id, username, title):
        self.execute("""INSERT INTO channels(owner_id,channel_id,username,title,created_at,updated_at)
        VALUES(?,?,?,?,?,?) ON CONFLICT(owner_id,channel_id) DO UPDATE SET username=excluded.username,title=excluded.title,enabled=1,updated_at=excluded.updated_at""",
        (owner,channel_id,username,title,now(),now()))

    def channels(self, owner):
        return self.all("SELECT * FROM channels WHERE owner_id=? AND enabled=1 ORDER BY title", (owner,))

    def channel(self, owner, cid):
        return self.one("SELECT * FROM channels WHERE owner_id=? AND channel_id=?", (owner,cid))

    def remove_channel(self, owner, cid):
        self.execute("DELETE FROM channels WHERE owner_id=? AND channel_id=?", (owner,cid))

    def log_request(self, owner, cid, uid, action):
        self.execute("INSERT INTO request_log(owner_id,channel_id,user_id,action,created_at) VALUES(?,?,?,?,?)", (owner,cid,uid,action,now()))

    def owner_count(self):
        return self.one("SELECT COUNT(*) c FROM users WHERE role='user'")["c"]


db = DB(DB_PATH)

# One Telethon client per bot user. A client is built only after the user has
# explicitly supplied their own API credentials and completed Telegram login.
clients: dict[int, TelegramClient] = {}
login_flows: dict[int, dict] = {}


def is_owner(uid: int) -> bool:
    return uid == OWNER_ID


def is_user(uid: int) -> bool:
    return bool(db.one("SELECT user_id FROM users WHERE user_id=?", (uid,)))


def btn(text, data=None, url=None):
    return InlineKeyboardButton(text, callback_data=data, url=url)


def back():
    return InlineKeyboardMarkup([[btn("⬅️ Back", "home")]])


def user_home_kb():
    return InlineKeyboardMarkup([
        [btn("📊 My Dashboard", "dashboard")],
        [btn("🔐 Telegram Login", "login"), btn("🚪 Logout", "logout")],
        [btn("📢 My Channels", "channels")],
        [btn("📩 Fetch Requests", "fetch")],
        [btn("👥 Approve All", "approve_all"), btn("🚫 Decline All", "decline_all")],
        [btn("⚙️ Settings", "settings")],
    ])


def owner_kb():
    return InlineKeyboardMarkup([
        [btn("📊 Dashboard", "dashboard"), btn("👤 Users", "owner_users")],
        [btn("📢 Channels", "owner_channels"), btn("📩 Requests", "owner_requests")],
        [btn("📣 Broadcast", "owner_broadcast"), btn("💬 Accepted Msg", "set_message")],
        [btn("📝 Preview Msg", "preview_message"), btn("📈 Statistics", "owner_stats")],
        [btn("🧾 Logs", "owner_logs"), btn("🔐 Sessions", "owner_sessions")],
        [btn("🔎 User Search", "owner_user_search"), btn("🔎 Channel Search", "owner_channel_search")],
        [btn("📋 Request Audit", "owner_request_audit"), btn("📤 Export Users", "owner_export_users")],
        [btn("📤 Export Channels", "owner_export_channels"), btn("📤 Export Logs", "owner_export_logs")],
        [btn("🛡 Security", "owner_security"), btn("⚙️ Permissions", "owner_permissions")],
        [btn("🤖 Bot Info", "owner_bot_info"), btn("🌐 Runtime", "owner_runtime")],
        [btn("💾 Backup", "owner_backup"), btn("🧹 Maintenance", "owner_maintenance")],
        [btn("🗑 Clear Logs", "owner_clear_logs"), btn("🔄 Sync Channels", "owner_sync")],
        [btn("⏱ Rate Limits", "owner_rate_limits"), btn("📦 Database", "owner_database")],
        [btn("🧪 Health Check", "owner_health"), btn("❓ Help", "owner_help")],
        [btn("🔄 Refresh", "dashboard")],
    ])


def owner_only_message():
    return "🔒 This section is Owner-only."


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u or not update.message:
        return
    db.user(u)
    text = (
        "🛡 MEMBER ACCEPTOR BOT\n\n"
        "Manage Telegram channel join requests from one clean panel.\n\n"
        "👤 User: Login with your own Telegram API ID/API Hash + phone.\n"
        "📢 Channel: Add your channel after giving the bot admin rights.\n"
        "📩 Requests: Fetch pending count and process requests.\n\n"
        "⚠️ Never send your Telegram password or OTP to anyone outside this bot."
    )
    await update.message.reply_text(text, reply_markup=user_home_kb())


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u or not update.message:
        return
    db.user(u)
    if is_owner(u.id):
        await update.message.reply_text("👑 OWNER CONTROL PANEL", reply_markup=owner_kb())
    else:
        await update.message.reply_text("📋 MEMBER ACCEPTOR", reply_markup=user_home_kb())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    login_flows.pop(uid, None)
    context.user_data.clear()
    await update.message.reply_text("❌ Cancelled.", reply_markup=user_home_kb())


async def ensure_client(uid: int) -> Optional[TelegramClient]:
    if uid in clients and clients[uid].is_connected() and await clients[uid].is_user_authorized():
        return clients[uid]
    row = db.session(uid)
    if not row:
        return None
    client = TelegramClient(StringSession(row["session"]), int(row["api_id"]), row["api_hash"])
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            return None
        clients[uid] = client
        return client
    except Exception:
        log.exception("Telethon connect failed for %s", uid)
        return None


async def begin_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_owner(uid):
        await update.callback_query.answer("Owner login is optional; you can use the same account flow.", show_alert=True)
    q = update.callback_query
    await q.answer()
    login_flows[uid] = {"step": "api_id"}
    await q.message.reply_text(
        "🔐 Telegram Login — Step 1/4\n\nSend your Telegram API ID.\nExample: 12345678\n\nUse /cancel to stop."
    )


async def process_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    flow = login_flows.get(uid)
    if not flow:
        return False
    text = (update.message.text or "").strip()
    if not text:
        return True
    try:
        if flow["step"] == "api_id":
            api_id = int(text)
            if api_id <= 0:
                raise ValueError
            flow.update(api_id=api_id, step="api_hash")
            await update.message.reply_text("Step 2/4 — Send your Telegram API Hash.")
            return True
        if flow["step"] == "api_hash":
            if len(text) < 20:
                await update.message.reply_text("API Hash looks invalid. Send the full API Hash.")
                return True
            flow.update(api_hash=text, step="phone")
            await update.message.reply_text("Step 3/4 — Send your phone number in international format.\nExample: +919876543210")
            return True
        if flow["step"] == "phone":
            api_id, api_hash = flow["api_id"], flow["api_hash"]
            phone = text.replace(" ", "")
            client = TelegramClient(StringSession(), api_id, api_hash)
            await client.connect()
            sent = await client.send_code_request(phone)
            flow.update(client=client, phone=phone, phone_code_hash=sent.phone_code_hash, step="otp")
            await update.message.reply_text("Step 4/4 — Send the Telegram login OTP.\n\nDo not share this OTP anywhere else. If 2-step verification is enabled, the bot will ask for the password next.")
            return True
        if flow["step"] == "otp":
            client = flow["client"]
            try:
                await client.sign_in(phone=flow["phone"], code=text, phone_code_hash=flow["phone_code_hash"])
            except SessionPasswordNeededError:
                flow["step"] = "password"
                await update.message.reply_text("🔑 Two-step verification is enabled. Send your Telegram 2FA password.")
                return True
            except (PhoneCodeInvalidError, PhoneCodeExpiredError):
                await update.message.reply_text("❌ Invalid/expired OTP. Send the latest OTP, or /cancel.")
                return True
            await finish_login(uid, flow, client, update.message)
            return True
        if flow["step"] == "password":
            client = flow["client"]
            try:
                await client.sign_in(password=text)
            except PasswordHashInvalidError:
                await update.message.reply_text("❌ Wrong 2FA password. Try again or /cancel.")
                return True
            await finish_login(uid, flow, client, update.message)
            return True
    except (ApiIdInvalidError, PhoneNumberInvalidError) as exc:
        await update.message.reply_text(f"❌ Telegram rejected the login details: {type(exc).__name__}. Start again with /cancel then Login.")
        login_flows.pop(uid, None)
    except FloodWaitError as exc:
        await update.message.reply_text(f"⏳ Telegram rate limit. Try again after {exc.seconds} seconds.")
        login_flows.pop(uid, None)
    except Exception as exc:
        log.exception("Login flow failed")
        await update.message.reply_text(f"❌ Login failed: {str(exc)[:500]}\nUse /cancel and try again.")
        login_flows.pop(uid, None)
    return True


async def finish_login(uid, flow, client, message):
    me = await client.get_me()
    session = client.session.save()
    db.save_session(uid, flow["api_id"], flow["api_hash"], flow["phone"], session)
    db.log(uid, "telegram_login", f"account_id={me.id}")
    clients[uid] = client
    login_flows.pop(uid, None)
    await message.reply_text(
        "✅ Telegram account connected.\n\n"
        f"👤 {me.first_name or ''} {('@' + me.username) if me.username else ''}\n"
        "Now add a channel. The logged-in account must be an admin with permission to manage join requests, and the bot must also be an admin in that channel.\n\n"
        "Your OTP/password is not stored; only the Telegram authorization session is stored for future reconnects.",
        reply_markup=user_home_kb(),
    )


async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    client = clients.pop(uid, None)
    if client:
        try:
            await client.log_out()
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            pass
    db.delete_session(uid)
    await q.message.reply_text("🚪 Telegram account disconnected and local authorization session removed.", reply_markup=user_home_kb())


async def add_channel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    if not await ensure_client(uid):
        await q.message.reply_text("❌ Login first.", reply_markup=user_home_kb())
        return
    context.user_data["state"] = "add_channel"
    await q.message.reply_text("📢 Send the channel @username, public link, or numeric channel ID.\n\nThe bot and your logged-in Telegram account must have admin rights.\nUse /cancel to stop.")


async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    client = await ensure_client(uid)
    raw = (update.message.text or "").strip()
    if not client:
        await update.message.reply_text("❌ Telegram account is not connected.")
        context.user_data.pop("state", None)
        return True
    try:
        entity = await client.get_entity(raw)
        cid = int(entity.id)
        if getattr(entity, "broadcast", False) or getattr(entity, "megagroup", False):
            title = getattr(entity, "title", "Channel")
            username = getattr(entity, "username", None)
            # Verify logged-in account has enough admin rights.
            me = await client.get_me()
            perms = await client.get_permissions(entity, me)
            if not getattr(perms, "is_admin", False):
                await update.message.reply_text("❌ Your logged-in Telegram account is not an admin in this channel.")
                return True
            if not (getattr(perms, "is_creator", False) or getattr(perms, "invite_users", False)):
                await update.message.reply_text("❌ Your account needs admin rights that allow managing/inviting members (join requests).")
                return True
            # Verify the bot is an administrator through Bot API.
            member = await context.bot.get_chat_member(cid, context.bot.id)
            if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                await update.message.reply_text("❌ Add this bot as a channel administrator first.")
                return True
            if member.status == ChatMemberStatus.ADMINISTRATOR and getattr(member, "can_invite_users", True) is False:
                await update.message.reply_text("❌ Bot admin needs 'Invite Users / Add Subscribers' permission.")
                return True
            db.add_channel(uid, cid, username, title)
            db.log(uid, "channel_added", cid)
            context.user_data.pop("state", None)
            await update.message.reply_text(f"✅ Channel added.\n\n📢 {title}\n🆔 {cid}\n👤 Logged-in account + 🤖 bot admin verified.", reply_markup=user_home_kb())
            return True
        await update.message.reply_text("❌ That is not a channel/supergroup supported by this manager.")
    except Exception as exc:
        await update.message.reply_text(f"❌ Could not add channel: {str(exc)[:600]}")
    return True


async def list_channels(update, context):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    rows = db.channels(uid)
    if not rows:
        await q.message.reply_text("📢 No channels added yet.\n\nAdd a channel from the button below.", reply_markup=InlineKeyboardMarkup([[btn("➕ Add Channel", "add_channel")],[btn("⬅️ Back", "home")]]))
        return
    keys = []
    for r in rows:
        keys.append([btn(f"📢 {r['title'][:35]}", f"channel:{r['channel_id']}")])
    keys.append([btn("➕ Add Channel", "add_channel")])
    keys.append([btn("⬅️ Back", "home")])
    await q.message.reply_text("📢 YOUR CHANNELS", reply_markup=InlineKeyboardMarkup(keys))


async def channel_page(update, context, cid=None):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    if cid is None:
        cid = int(q.data.split(":",1)[1])
    row = db.channel(uid,cid)
    if not row:
        await q.message.reply_text("Channel not found.")
        return
    client = await ensure_client(uid)
    if not client:
        await q.message.reply_text("❌ Login first.", reply_markup=user_home_kb())
        return
    try:
        entity = await client.get_entity(cid)
        full = await client.get_entity(entity)
        members = getattr(full, "participants_count", None)
        pending = await get_pending(client, entity)
        text = f"📢 {row['title']}\n🆔 {cid}\n\n👥 Members: {members if members is not None else 'N/A'}\n📩 Pending Requests: {len(pending)}\n\nChoose an action:"
        kb = InlineKeyboardMarkup([
            [btn("🔄 Fetch Requests", f"fetch:{cid}")],
            [btn("✅ Approve All", f"approve_all:{cid}"), btn("🚫 Decline All", f"decline_all:{cid}")],
            [btn("🗑 Remove Channel", f"remove:{cid}")],
            [btn("⬅️ Channels", "channels")],
        ])
        await q.message.reply_text(text, reply_markup=kb)
    except Exception as exc:
        await q.message.reply_text(f"❌ Channel check failed: {str(exc)[:700]}")


async def get_pending(client, entity, limit=10000):
    result = []
    offset_date = None
    offset_user = InputUserEmpty()
    while len(result) < limit:
        resp = await client(GetChatInviteImportersRequest(
            peer=entity,
            requested=True,
            subscription_expired=False,
            link=None,
            q=None,
            offset_date=offset_date or 0,
            offset_user=offset_user,
            limit=min(100, limit-len(result)),
        ))
        users = {u.id: u for u in resp.users}
        batch = list(getattr(resp, "importers", []))
        if not batch:
            break
        for item in batch:
            user = users.get(item.user_id)
            if user:
                result.append((item, user))
        if len(batch) < 100:
            break
        last = batch[-1]
        offset_date = getattr(last, "date", None)
        offset_user = users.get(last.user_id)
        if not offset_user:
            break
    return result


async def fetch_requests(update, context, cid=None):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer("Fetching…")
    if cid is None:
        cid = int(q.data.split(":",1)[1]) if ":" in q.data else None
    if cid is None:
        rows = db.channels(uid)
        if len(rows) != 1:
            await q.message.reply_text("Select a channel first.", reply_markup=user_home_kb())
            return
        cid = rows[0]["channel_id"]
    client = await ensure_client(uid)
    if not client:
        await q.message.reply_text("❌ Login first.", reply_markup=user_home_kb())
        return
    try:
        entity = await client.get_entity(cid)
        pending = await get_pending(client, entity)
        title = getattr(entity, "title", str(cid))
        member_count = getattr(entity, "participants_count", None)
        text = f"📊 {title}\n\n👥 Members: {member_count if member_count is not None else 'N/A'}\n📩 Pending: {len(pending)}\n\n"
        if pending:
            preview=[]
            for _, u in pending[:15]:
                name=(getattr(u,'first_name','') or '') + (' '+(getattr(u,'last_name','') or '') if getattr(u,'last_name',None) else '')
                preview.append(f"• {name or 'Unknown'} — @{u.username}" if u.username else f"• {name or 'Unknown'} — {u.id}")
            text += "\n".join(preview)
            if len(pending)>15: text += f"\n… and {len(pending)-15} more"
        else:
            text += "No pending join requests."
        await q.message.reply_text(text[:4000], reply_markup=InlineKeyboardMarkup([
            [btn("🔄 Refresh", f"fetch:{cid}")],
            [btn("✅ Approve All", f"approve_all:{cid}"), btn("🚫 Decline All", f"decline_all:{cid}")],
            [btn("⬅️ Channel", f"channel:{cid}")],
        ]))
    except Exception as exc:
        await q.message.reply_text(f"❌ Fetch failed: {str(exc)[:800]}")


async def process_all(update, context, approved: bool, cid=None):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer("Processing…")
    if cid is None:
        cid = int(q.data.split(":", 1)[1]) if ":" in q.data else None
    if cid is None:
        rows = db.channels(uid)
        if len(rows) != 1:
            await q.message.reply_text("Select a channel first.", reply_markup=user_home_kb())
            return
        cid = rows[0]["channel_id"]
    client = await ensure_client(uid)
    if not client:
        await q.message.reply_text("❌ Login first.")
        return
    try:
        entity = await client.get_entity(cid)
        pending = await get_pending(client, entity) if approved else []
        action = "approve" if approved else "decline"
        await client(HideAllChatJoinRequestsRequest(peer=entity, approved=approved, link=None))
        db.log(uid, f"{action}_all", f"channel={cid},count={len(pending)}")
        # The bulk MTProto method does not return the affected user list. We
        # fetched it immediately before approval so the Owner-configured
        # accepted-member message can still be delivered to every approved user.
        notified = 0
        if approved:
            for _, user in pending:
                try:
                    if await notify_accepted_contact(context.bot, uid, cid, user.id, user):
                        notified += 1
                except Exception:
                    pass
                db.log_request(uid, cid, user.id, "approved")
                db.execute("DELETE FROM pending_request_contacts WHERE owner_id=? AND channel_id=? AND user_id=?", (uid,cid,user.id))
                await asyncio.sleep(0.05)
        if not approved:
            db.execute("DELETE FROM pending_request_contacts WHERE owner_id=? AND channel_id=?", (uid,cid))
        await q.message.reply_text(
            f"✅ {action.title()} All completed for {getattr(entity, 'title', cid)}.\n\n"
            f"📌 Processed: {len(pending) if approved else 'all pending'}\n"
            f"💬 Accepted-message sent: {notified if approved else 0}",
            reply_markup=InlineKeyboardMarkup([
                [btn("🔄 Fetch Requests", f"fetch:{cid}")],
                [btn("⬅️ Channel", f"channel:{cid}")],
            ]),
        )
    except FloodWaitError as exc:
        await q.message.reply_text(f"⏳ Telegram rate limit. Retry after {exc.seconds} seconds.")
    except Exception as exc:
        await q.message.reply_text(f"❌ {action.title()} All failed: {str(exc)[:800]}")


async def settings(update, context):
    q=update.callback_query; uid=q.from_user.id; await q.answer()
    msg = db.setting(uid,"accepted_message","")
    await q.message.reply_text(
        "⚙️ USER SETTINGS\n\n"
        "Accepted-member message is controlled by the Owner.\n"
        f"Owner message status: {'SET' if msg else 'NOT SET'}\n\n"
        "Your account is only used to manage channels you add.", reply_markup=back())


async def dashboard(update, context):
    q=update.callback_query; uid=q.from_user.id; await q.answer()
    rows=db.channels(uid)
    session=await ensure_client(uid)
    lines=["📊 MEMBER ACCEPTOR DASHBOARD","",f"🔐 Telegram Login: {'CONNECTED' if session else 'NOT CONNECTED'}",f"📢 Channels: {len(rows)}"]
    total_pending=0
    for r in rows:
        if session:
            try:
                e=await session.get_entity(r['channel_id']); p=await get_pending(session,e,1000); total_pending+=len(p)
            except Exception: pass
    lines.append(f"📩 Pending Requests: {total_pending}")
    lines.append(f"🕒 Server UTC: {now()}")
    await q.message.reply_text("\n".join(lines), reply_markup=home_kb(uid))


async def remove_channel(update, context):
    q=update.callback_query; uid=q.from_user.id; await q.answer()
    cid=int(q.data.split(":",1)[1])
    db.remove_channel(uid,cid); db.log(uid,"channel_removed",cid)
    await q.message.reply_text("🗑 Channel removed from this bot.", reply_markup=user_home_kb())


async def owner_callback(update, context):
    q=update.callback_query; uid=q.from_user.id
    if not is_owner(uid):
        await q.answer(owner_only_message(), show_alert=True); return
    data=q.data
    await q.answer()
    if data=="owner_users":
        rows=db.all("SELECT user_id,username,first_name,last_seen FROM users WHERE role='user' ORDER BY last_seen DESC LIMIT 100")
        text="👤 USERS\n\n"+"\n".join(f"• {r['first_name'] or 'User'} | @{r['username']} | {r['user_id']}" if r['username'] else f"• {r['first_name'] or 'User'} | {r['user_id']}" for r in rows)
        await q.message.reply_text(text[:4000] or "No users.", reply_markup=back()); return
    if data=="owner_channels":
        rows=db.all("SELECT owner_id,channel_id,title,username FROM channels ORDER BY title LIMIT 100")
        text="📢 ALL CONNECTED CHANNELS\n\n"+"\n".join(f"• {r['title']} | owner {r['owner_id']} | {r['channel_id']}" for r in rows)
        await q.message.reply_text(text[:4000] or "No channels.", reply_markup=back()); return
    if data=="owner_requests":
        rows=db.all("SELECT action,COUNT(*) c FROM request_log GROUP BY action ORDER BY c DESC")
        text="📩 REQUEST ACTION AUDIT\n\n"+"\n".join(f"• {r['action']}: {r['c']}" for r in rows)
        await q.message.reply_text(text[:4000] or "No request actions yet.",reply_markup=back()); return
    if data=="owner_sessions":
        n=db.one("SELECT COUNT(*) c FROM telegram_sessions")["c"]
        await q.message.reply_text(f"🔐 ACTIVE STORED SESSIONS\n\nConnected authorization records: {n}\n\nSessions belong to users who explicitly logged in.",reply_markup=back()); return
    if data=="owner_user_search":
        context.user_data["state"]="owner_user_search"
        await q.message.reply_text("Send a user ID or @username to search. /cancel to stop."); return
    if data=="owner_channel_search":
        context.user_data["state"]="owner_channel_search"
        await q.message.reply_text("Send a channel ID or title keyword. /cancel to stop."); return
    if data=="owner_request_audit":
        rows=db.all("SELECT owner_id,channel_id,user_id,action,created_at FROM request_log ORDER BY id DESC LIMIT 30")
        text="📋 REQUEST AUDIT\n\n"+"\n".join(f"{r['created_at']} | {r['action']} | ch {r['channel_id']} | user {r['user_id']}" for r in rows)
        await q.message.reply_text(text[:4000] or "No audit records.",reply_markup=back()); return
    if data=="owner_export_users":
        rows=db.all("SELECT user_id,username,first_name,last_seen,role,blocked FROM users ORDER BY user_id")
        text="user_id,username,first_name,last_seen,role,blocked\n"+"\n".join(','.join(str(r[k] or '').replace(',',' ') for k in ('user_id','username','first_name','last_seen','role','blocked')) for r in rows)
        await q.message.reply_document(document=__import__('io').BytesIO(text.encode()),filename="users.csv",caption="👤 Users export"); return
    if data=="owner_export_channels":
        rows=db.all("SELECT owner_id,channel_id,username,title,created_at FROM channels ORDER BY owner_id")
        text="owner_id,channel_id,username,title,created_at\n"+"\n".join(','.join(str(r[k] or '').replace(',',' ') for k in ('owner_id','channel_id','username','title','created_at')) for r in rows)
        await q.message.reply_document(document=__import__('io').BytesIO(text.encode()),filename="channels.csv",caption="📢 Channels export"); return
    if data=="owner_export_logs":
        rows=db.all("SELECT owner_id,kind,details,created_at FROM events ORDER BY id DESC")
        text="owner_id,kind,details,created_at\n"+"\n".join(','.join(str(r[k] or '').replace(',',' ') for k in ('owner_id','kind','details','created_at')) for r in rows)
        await q.message.reply_document(document=__import__('io').BytesIO(text.encode()),filename="events.csv",caption="🧾 Logs export"); return
    if data=="owner_permissions":
        await q.message.reply_text("⚙️ PERMISSIONS\n\nOnly OWNER_ID can change global settings. Regular users can manage only channels they have added. A channel is accepted only after both the bot and the user's logged-in Telegram account pass admin checks.",reply_markup=back()); return
    if data=="owner_bot_info":
        me=await context.bot.get_me()
        await q.message.reply_text(f"🤖 BOT INFO\n\nName: {me.first_name}\nUsername: @{me.username}\nID: {me.id}\nPTB: 22.7\nTelethon: 1.43.0",reply_markup=back()); return
    if data=="owner_runtime":
        await q.message.reply_text(f"🌐 RUNTIME\n\nUTC: {now()}\nRender URL: {'configured' if RENDER_EXTERNAL_URL else 'not configured'}\nDB: {DB_PATH.name}\nPID: {os.getpid()}",reply_markup=back()); return
    if data=="owner_clear_logs":
        db.execute("DELETE FROM events"); db.execute("DELETE FROM request_log")
        await q.message.reply_text("🗑 Event and request logs cleared.",reply_markup=owner_kb()); return
    if data=="owner_sync":
        await q.message.reply_text("🔄 Channel sync is performed live when each user opens/fetches a channel. No stale member/request count is stored.",reply_markup=back()); return
    if data=="owner_rate_limits":
        await q.message.reply_text("⏱ RATE LIMITS\n\nBulk approve/decline uses Telegram's official bulk operation. Accepted-member messages are throttled at 20/sec in this build to reduce Bot API pressure.",reply_markup=back()); return
    if data=="owner_database":
        await q.message.reply_text(f"📦 DATABASE\n\nPath: {DB_PATH}\nSize: {DB_PATH.stat().st_size if DB_PATH.exists() else 0:,} bytes\nWAL: enabled",reply_markup=back()); return
    if data=="owner_health":
        await q.message.reply_text("🧪 HEALTH CHECK\n\nBot process: OK\nSQLite: OK\nTelethon clients: %d\nHTTP health endpoint: /health" % len(clients),reply_markup=back()); return
    if data=="owner_help":
        await q.message.reply_text("❓ OWNER HELP\n\nThe owner controls the accepted-member message, broadcasts, security, exports, logs and global monitoring. Users cannot change the owner message.",reply_markup=back()); return

    if data=="owner_stats":
        users=db.one("SELECT COUNT(*) c FROM users WHERE role='user'")["c"]
        channels=db.one("SELECT COUNT(*) c FROM channels")["c"]
        logs=db.one("SELECT COUNT(*) c FROM request_log")["c"]
        await q.message.reply_text(f"📈 OWNER STATISTICS\n\n👤 Users: {users}\n📢 Channels: {channels}\n🧾 Actions logged: {logs}", reply_markup=back()); return
    if data=="owner_logs":
        rows=db.all("SELECT owner_id,kind,details,created_at FROM events ORDER BY id DESC LIMIT 20")
        text="🧾 RECENT LOGS\n\n"+"\n".join(f"[{r['created_at']}] {r['kind']} | {r['owner_id']} | {r['details']}" for r in rows)
        await q.message.reply_text(text[:4000] or "No logs.", reply_markup=back()); return
    if data=="owner_security":
        await q.message.reply_text("🛡 SECURITY\n\n• Owner-only configuration\n• Per-user Telegram sessions\n• OTP/2FA values are not written to DB\n• Logout deletes the stored authorization session\n• Channel access is verified before adding\n• Every approve/decline action is logged\n• Do not expose member_acceptor.db or .session data", reply_markup=back()); return
    if data=="owner_backup":
        await q.message.reply_text("💾 BACKUP\n\nSQLite database: member_acceptor.db\n\nFor Render, use a persistent disk or external database/storage. The Telegram authorization sessions are sensitive and must be protected.", reply_markup=back()); return
    if data=="owner_maintenance":
        await q.message.reply_text("🧹 MAINTENANCE\n\nCurrent DB size: %s bytes\nWAL mode: enabled\nPolling interval: %ss" % (DB_PATH.stat().st_size if DB_PATH.exists() else 0, POLL_SECONDS), reply_markup=back()); return
    if data=="set_message":
        context.user_data["state"]="owner_message"
        await q.message.reply_text("💬 Send the message that the bot should send to users after their channel request is approved.\n\nUse {Username} to insert the user's name.\n\nThis setting is Owner-only. Use /cancel to stop.")
        return
    if data=="preview_message":
        msg=db.setting(uid,"accepted_message","")
        if not msg: await q.message.reply_text("No accepted-member message is configured.",reply_markup=back()); return
        await q.message.reply_text(msg.replace("{Username}",q.from_user.first_name or "there"),reply_markup=back()); return
    if data=="owner_broadcast":
        context.user_data["state"]="broadcast"
        await q.message.reply_text("📣 Owner Broadcast\n\nSend the broadcast text now. It will go to bot users who have started the bot. Use /cancel to stop.")
        return


async def owner_text(update, context):
    uid=update.effective_user.id
    state=context.user_data.get("state")
    if not is_owner(uid) or not state:
        return False
    text=(update.message.text or "").strip()
    if state=="owner_message":
        db.set_setting(OWNER_ID,"accepted_message",text[:4096]); db.log(uid,"accepted_message_updated")
        context.user_data.pop("state",None)
        await update.message.reply_text("✅ Accepted-member message saved.",reply_markup=owner_kb()); return True
    if state=="owner_user_search":
        context.user_data.pop("state",None)
        raw=text.lstrip("@").lower()
        rows=db.all("SELECT user_id,username,first_name,last_seen,role,blocked FROM users WHERE CAST(user_id AS TEXT)=? OR LOWER(username)=? LIMIT 10",(raw,raw))
        out="🔎 USER SEARCH\n\n"+"\n".join(f"{r['user_id']} | @{r['username']} | {r['first_name']} | {r['role']}" for r in rows)
        await update.message.reply_text(out if rows else "No matching user.",reply_markup=owner_kb()); return True
    if state=="owner_channel_search":
        context.user_data.pop("state",None)
        raw=text.lower()
        rows=db.all("SELECT owner_id,channel_id,title,username FROM channels WHERE LOWER(title) LIKE ? OR LOWER(username) LIKE ? OR CAST(channel_id AS TEXT)=? LIMIT 20",(f"%{raw}%",f"%{raw}%",raw))
        out="🔎 CHANNEL SEARCH\n\n"+"\n".join(f"{r['title']} | {r['channel_id']} | owner {r['owner_id']}" for r in rows)
        await update.message.reply_text(out if rows else "No matching channel.",reply_markup=owner_kb()); return True
    if state=="broadcast":
        context.user_data.pop("state",None)
        users=db.all("SELECT user_id FROM users WHERE role='user' AND blocked=0")
        sent=failed=0
        for r in users:
            try:
                await context.bot.send_message(r["user_id"], text[:4096], disable_web_page_preview=True)
                sent+=1
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after)+1)
                try: await context.bot.send_message(r["user_id"], text[:4096]); sent+=1
                except Exception: failed+=1
            except Forbidden:
                db.execute("UPDATE users SET blocked=1 WHERE user_id=?",(r["user_id"],)); failed+=1
            except Exception: failed+=1
            await asyncio.sleep(0.05)
        await update.message.reply_text(f"📣 Broadcast finished.\n\n✅ Sent: {sent}\n❌ Failed/blocked: {failed}",reply_markup=owner_kb()); return True
    return False


async def send_accepted_message(bot, uid, user):
    msg=db.setting(OWNER_ID,"accepted_message","")
    if not msg:
        return False
    name=(user.first_name or "there")
    msg=msg.replace("{Username}",name).replace("{username}",name)
    try:
        await bot.send_message(uid,msg[:4096],disable_web_page_preview=True)
        return True
    except Exception as exc:
        log.warning("accepted message failed for %s: %s",uid,exc)
        return False


async def notify_accepted_contact(bot, owner_id, channel_id, user_id, user_obj):
    # Prefer the temporary Bot API user_chat_id captured when the join request
    # arrived. If it has expired, fall back to the normal user ID (works when
    # the user has already started this bot).
    row = db.one("SELECT user_chat_id FROM pending_request_contacts WHERE owner_id=? AND channel_id=? AND user_id=?", (owner_id, channel_id, user_id))
    target = row["user_chat_id"] if row and row["user_chat_id"] else user_id
    return await send_accepted_message(bot, target, user_obj)


async def handle_bot_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req=update.chat_join_request
    if not req: return
    # Bot receives these updates when it is an admin with invite permission.
    owner_rows=db.all("SELECT owner_id FROM channels WHERE channel_id=? AND enabled=1",(req.chat.id,))
    for r in owner_rows:
        owner_id = r["owner_id"]
        db.log(owner_id,"join_request_received",f"channel={req.chat.id},user={req.from_user.id}")
        db.execute(
            "INSERT INTO pending_request_contacts(owner_id,channel_id,user_id,user_chat_id,created_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(owner_id,channel_id,user_id) DO UPDATE SET user_chat_id=excluded.user_chat_id,created_at=excluded.created_at",
            (owner_id, req.chat.id, req.from_user.id, getattr(req, "user_chat_id", None), now()),
        )
    # This bot intentionally does not auto-approve. Approval is controlled by the user panel.


async def generic_callback(update, context):
    q=update.callback_query; data=q.data or ""; uid=q.from_user.id
    if data=="home":
        await q.answer(); await q.message.reply_text("📋 MEMBER ACCEPTOR",reply_markup=home_kb(uid)); return
    if data=="dashboard": await dashboard(update,context); return
    if data=="login": await begin_login(update,context); return
    if data=="logout": await logout(update,context); return
    if data=="channels": await list_channels(update,context); return
    if data=="add_channel": await add_channel_start(update,context); return
    if data.startswith("channel:"): await channel_page(update,context); return
    if data=="fetch": await fetch_requests(update,context); return
    if data.startswith("fetch:"): await fetch_requests(update,context); return
    if data=="approve_all": await process_all(update,context,True); return
    if data.startswith("approve_all:"): await process_all(update,context,True); return
    if data=="decline_all": await process_all(update,context,False); return
    if data.startswith("decline_all:"): await process_all(update,context,False); return
    if data.startswith("remove:"): await remove_channel(update,context); return
    if data=="settings": await settings(update,context); return
    if data.startswith("owner_") or data in ("set_message","preview_message"):
        await owner_callback(update,context); return
    await q.answer("Unknown action",show_alert=True)


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    uid=update.effective_user.id
    db.user(update.effective_user)
    if await process_login_text(update,context):
        return
    if is_owner(uid) and await owner_text(update,context):
        return
    if context.user_data.get("state")=="add_channel":
        await add_channel(update,context); return


async def error_handler(update, context):
    log.exception("Unhandled update error", exc_info=context.error)


async def run_http_webhook(application):
    from aiohttp import web
    webapp = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    async def webhook(request):
        if WEBHOOK_SECRET:
            supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if not secrets.compare_digest(supplied, WEBHOOK_SECRET):
                return web.Response(status=403, text="forbidden")
        try:
            payload = await request.json()
            upd = Update.de_json(payload, application.bot)
            await application.update_queue.put(upd)
            return web.Response(text="ok")
        except Exception as exc:
            log.exception("Webhook update failed")
            return web.Response(status=400, text=str(exc)[:200])

    webapp.router.add_get("/", health)
    webapp.router.add_get("/health", health)
    webapp.router.add_post(f"/{WEBHOOK_PATH}", webhook)
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner


async def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(ChatJoinRequestHandler(handle_bot_join_request))
    application.add_handler(CallbackQueryHandler(generic_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    application.add_error_handler(error_handler)

    await application.initialize()
    await application.start()
    if RENDER_EXTERNAL_URL:
        await application.bot.set_webhook(
            url=f"{RENDER_EXTERNAL_URL}/{WEBHOOK_PATH}",
            secret_token=WEBHOOK_SECRET or None,
            drop_pending_updates=False,
            allowed_updates=Update.ALL_TYPES,
        )
        await run_http_webhook(application)
        log.info("Webhook + /health listening on port %s", PORT)
    else:
        await application.updater.start_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)
        log.info("Polling mode enabled")
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
