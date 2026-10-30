import os
import re
import asyncio
import sqlite3
import logging
from contextlib import closing
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

# Load environment variables
load_dotenv()

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Kolkata"))
DB_PATH = os.getenv("DB_PATH", "scheduler.db")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("scheduler")

# ================= REGEX =================
SCHEDULE_RE = re.compile(r"^\.schedule\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\s+(\S+)\s+([\s\S]+)$")
SCHEDULECH_RE = re.compile(r"^\.schedulech\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\s+(\S+)\s+([\s\S]+)$")
CANCEL_RE = re.compile(r"^\.cancelschedule\s+(\d+)\s*$")
SCHEDULEME_RE = re.compile(r"^\.scheduleme\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\s+([\s\S]+)$")

# ================= DB =================
def db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as conn:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER NOT NULL,
                    chat_id INTEGER,
                    kind TEXT NOT NULL,
                    target TEXT NOT NULL,
                    reply_to INTEGER,
                    message TEXT NOT NULL,
                    run_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

def add_schedule(owner_id, chat_id, kind, target, reply_to, message, run_at):
    with closing(db()) as conn:
        with conn:
            cur = conn.execute("""
                INSERT INTO schedules (owner_id, chat_id, kind, target, reply_to, message, run_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (owner_id, chat_id, kind, str(target), reply_to, message, run_at))
            return cur.lastrowid

def get_schedule(sid):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM schedules WHERE id = ?", (sid,)).fetchone()

def set_status(sid, status, error=None):
    with closing(db()) as conn:
        with conn:
            conn.execute("UPDATE schedules SET status = ?, error = ? WHERE id = ?", (status, error, sid))

def list_pending(owner_id):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM schedules WHERE owner_id = ? AND status='pending' ORDER BY run_at ASC", (owner_id,)).fetchall()

def get_pending_all():
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM schedules WHERE status='pending' ORDER BY run_at ASC").fetchall()

# ================= HELPERS =================
def parse_when(date_s, time_s):
    fmt = "%Y-%m-%d %H:%M:%S" if time_s.count(":") == 2 else "%Y-%m-%d %H:%M"
    dt = datetime.strptime(f"{date_s} {time_s}", fmt)
    return dt.replace(tzinfo=TZ)

def normalize_target(target):
    target = target.strip()
    if re.fullmatch(r"-?\d+", target):
        return int(target)
    return target

def parse_tme_link(link):
    link = link.strip()
    m = re.match(r"^https?://t\.me/c/(\d+)/(\d+)(?:\?.*)?$", link)
    if m: return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.match(r"^https?://t\.me/([A-Za-z0-9_]+)/(\d+)(?:\?.*)?$", link)
    if m: return "@" + m.group(1), int(m.group(2))
    m = re.match(r"^https?://t\.me/([A-Za-z0-9_]+)/?$", link)
    if m: return "@" + m.group(1), None
    m = re.match(r"^@?([A-Za-z0-9_]+)/(\d+)$", link)
    if m: return "@" + m.group(1), int(m.group(2))
    raise ValueError("Invalid post link")

def fmt_when(iso_str):
    return datetime.fromisoformat(iso_str).astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")

async def require_owner(update: Update):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        if update.effective_message:
            await update.effective_message.reply_text("Not authorized.")
        return False
    return True

# ================= SCHEDULER =================
class PreciseScheduler:
    def __init__(self, app: Application):
        self.app = app
        self.tasks: dict[int, asyncio.Task] = {}

    def schedule(self, sid: int, when: datetime, payload: dict):
        self.cancel(sid)
        self.tasks[sid] = asyncio.create_task(self._wait_and_send(sid, when, payload))

    def cancel(self, sid: int):
        t = self.tasks.pop(sid, None)
        if t and not t.done(): t.cancel()

    def cancel_all(self):
        for t in list(self.tasks.values()):
            if not t.done(): t.cancel()
        self.tasks.clear()

    async def _wait_and_send(self, sid: int, when: datetime, payload: dict):
        try:
            delay = (when - datetime.now(TZ)).total_seconds()
            if delay > 0.2:
                await asyncio.sleep(delay - 0.1)

            target_wall = when.timestamp()
            while True:
                remaining = target_wall - datetime.now(TZ).timestamp()
                if remaining <= 0: break
                await asyncio.sleep(min(remaining, 0.005))

            bot = self.app.bot
            if payload.get("reply_to"):
                await bot.send_message(chat_id=payload["chat_id"], text=payload["text"], reply_to_message_id=payload["reply_to"])
            else:
                await bot.send_message(chat_id=payload["chat_id"], text=payload["text"])
            set_status(sid, "sent")
            log.info("Schedule #%s sent", sid)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            set_status(sid, "failed", str(e)[:500])
            log.exception("Schedule #%s failed", sid)
        finally:
            self.tasks.pop(sid, None)

scheduler: PreciseScheduler | None = None

# ================= COMMANDS =================
async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    u = update.effective_user
    c = update.effective_chat
    await update.effective_message.reply_text(f"Your user id: `{u.id}`\nThis chat id: `{c.id}`\nChat type: `{c.type}`", parse_mode="Markdown")

async def scheduleme_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    m = SCHEDULEME_RE.match(update.effective_message.text or "")
    if not m:
        await update.effective_message.reply_text("Usage:\n.scheduleme YYYY-MM-DD HH:MM[:SS] message")
        return
    date_s, time_s, msg = m.groups()
    try: when = parse_when(date_s, time_s)
    except: return await update.effective_message.reply_text("Date/time galat.")
    if when <= datetime.now(TZ): return await update.effective_message.reply_text("Past time hai.")

    target = OWNER_ID
    sid = add_schedule(OWNER_ID, update.effective_chat.id, "msg", target, None, msg, when.isoformat())
    scheduler.schedule(sid, when, {"owner_id": OWNER_ID, "chat_id": target, "text": msg, "reply_to": None})
    await update.effective_message.reply_text(f"Scheduled to your DM #{sid}\nTime: {when.strftime('%Y-%m-%d %H:%M:%S')}")

async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    m = SCHEDULE_RE.match(update.effective_message.text or "")
    if not m: return await update.effective_message.reply_text("Usage:\n.schedule YYYY-MM-DD HH:MM[:SS] target message")
    date_s, time_s, target_s, msg = m.groups()
    try: when = parse_when(date_s, time_s)
    except: return await update.effective_message.reply_text("Date/time galat.")
    if when <= datetime.now(TZ): return await update.effective_message.reply_text("Past time hai.")

    target = normalize_target(target_s)
    sid = add_schedule(OWNER_ID, update.effective_chat.id, "msg", target, None, msg, when.isoformat())
    try: await context.bot.get_chat(target)
    except: pass
    scheduler.schedule(sid, when, {"owner_id": OWNER_ID, "chat_id": target, "text": msg, "reply_to": None})
    await update.effective_message.reply_text(f"Scheduled #{sid}\nTime: {when.strftime('%Y-%m-%d %H:%M:%S')}\nTarget: {target}")

async def schedulech_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    m = SCHEDULECH_RE.match(update.effective_message.text or "")
    if not m: return await update.effective_message.reply_text("Usage:\n.schedulech YYYY-MM-DD HH:MM[:SS] post_link message")
    date_s, time_s, link, msg = m.groups()
    try: when = parse_when(date_s, time_s)
    except: return await update.effective_message.reply_text("Date/time galat.")
    if when <= datetime.now(TZ): return await update.effective_message.reply_text("Past time hai.")
    try: target, reply_to = parse_tme_link(link)
    except: return await update.effective_message.reply_text("Post link galat hai.")

    sid = add_schedule(OWNER_ID, update.effective_chat.id, "reply", target, reply_to, msg, when.isoformat())
    try: await context.bot.get_chat(target)
    except: pass
    scheduler.schedule(sid, when, {"owner_id": OWNER_ID, "chat_id": target, "text": msg, "reply_to": reply_to})
    await update.effective_message.reply_text(f"Scheduled ch #{sid}\nTime: {when.strftime('%Y-%m-%d %H:%M:%S')}\nTarget: {target}")

async def schedules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    rows = list_pending(OWNER_ID)
    if not rows: return await update.effective_message.reply_text("No pending schedules.")
    lines = []
    for r in rows:
        msg = (r["message"] or "").replace("\n", " ")[:60]
        kind = "reply" if r["reply_to"] else "msg"
        lines.append(f"#{r['id']} | {fmt_when(r['run_at'])} | {kind} | {r['target']} | {msg}")
    await update.effective_message.reply_text("Pending:\n\n" + "\n".join(lines))

async def cancelschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update): return
    m = CANCEL_RE.match(update.effective_message.text or "")
    if not m: return await update.effective_message.reply_text("Usage: .cancelschedule <id>")
    sid = int(m.group(1))
    row = get_schedule(sid)
    if not row or row["owner_id"] != OWNER_ID: return await update.effective_message.reply_text("Schedule nahi mila.")
    if row["status"] != "pending": return await update.effective_message.reply_text(f"Already {row['status']} hai.")
    set_status(sid, "cancelled")
    scheduler.cancel(sid)
    await update.effective_message.reply_text(f"Schedule #{sid} cancelled.")

# ================= STARTUP =================
async def post_init(application: Application):
    global scheduler
    scheduler = PreciseScheduler(application)
    try: await application.bot.get_me()
    except: pass
    
    now = datetime.now(TZ)
    for row in get_pending_all():
        when = datetime.fromisoformat(row["run_at"])
        if when <= now: when = now
        target_raw = row["target"]
        target_val = int(target_raw) if re.fullmatch(r"-?\d+", str(target_raw)) else target_raw
        payload = {"owner_id": row["owner_id"], "chat_id": target_val, "text": row["message"], "reply_to": int(row["reply_to"]) if row["reply_to"] else None}
        scheduler.schedule(row["id"], when, payload)

async def post_shutdown(application: Application):
    if scheduler: scheduler.cancel_all()

def main():
    if not BOT_TOKEN or not OWNER_ID:
        raise SystemExit("BOT_TOKEN ya OWNER_ID missing hai!")
    init_db()
    app = Application.builder().token(BOT_TOKEN).connection_pool_size(32).post_init(post_init).post_shutdown(post_shutdown).build()
    
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.id\b"), id_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.scheduleme\b"), scheduleme_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.schedulech\b"), schedulech_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.schedule\b"), schedule_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.schedules\b"), schedules_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^\.cancelschedule\b"), cancelschedule_cmd))
    
    log.info("Scheduler bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
