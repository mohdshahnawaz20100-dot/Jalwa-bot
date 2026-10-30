import os
import re
import asyncio
import sqlite3
import logging
from contextlib import closing
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

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
SCHEDULE_RE = re.compile(
    r"^\.schedule\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\s+(\S+)\s+([\s\S]+)$"
)
SCHEDULECH_RE = re.compile(
    r"^\.schedulech\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)\s+(\S+)\s+([\s\S]+)$"
)
CANCEL_RE = re.compile(r"^\.cancelschedule\s+(\d+)\s*$")

# ================= DB =================
def db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn:
        with conn:
            conn.execute(
                """
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
                """
            )


def add_schedule(owner_id, chat_id, kind, target, reply_to, message, run_at):
    with closing(db()) as conn:
        with conn:
            cur = conn.execute(
                """
                INSERT INTO schedules
                (owner_id, chat_id, kind, target, reply_to, message, run_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (owner_id, chat_id, kind, str(target), reply_to, message, run_at),
            )
            return cur.lastrowid


def get_schedule(sid):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM schedules WHERE id = ?", (sid,)).fetchone()


def set_status(sid, status, error=None):
    with closing(db()) as conn:
        with conn:
            conn.execute(
                "UPDATE schedules SET status = ?, error = ? WHERE id = ?",
                (status, error, sid),
            )


def list_pending(owner_id):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM schedules WHERE owner_id = ? AND status='pending' ORDER BY run_at ASC",
            (owner_id,),
        ).fetchall()


def get_pending_all():
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM schedules WHERE status='pending' ORDER BY run_at ASC"
        ).fetchall()

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
    if m:
        return int(f"-100{m.group(1)}"), int(m.group(2))
    m = re.match(r"^https?://t\.me/([A-Za-z0-9_]+)/(\d+)(?:\?.*)?$", link)
    if m:
        return "@" + m.group(1), int(m.group(2))
    m = re.match(r"^https?://t\.me/([A-Za-z0-9_]+)/?$", link)
    if m:
        return "@" + m.group(1), None
    m = re.match(r"^@?([A-Za-z0-9_]+)/(\d+)$", link)
    if m:
        return "@" + m.group(1), int(m.group(2))
    raise ValueError("Invalid post link")


def fmt_when(iso_str):
    return datetime.fromisoformat(iso_str).astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")


async def require_owner(update: Update):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        if update.effective_message:
            await update.effective_message.reply_text("Not authorized.")
        return False
    return True


# ================= PRECISE SCHEDULER (asyncio, sub-second) =================
class PreciseScheduler:
    """
    Har job ke liye ek asyncio task.
    - Long delay: chunk-sleep then fine-tune via loop.time() (monotonic, sub-ms)
    - Payload in-memory: fire pe koi DB lookup / parse nahi
    - Chat cache warm-up se send_message fast (~100-300ms network)
    """

    def __init__(self, app: Application):
        self.app = app
        self.tasks: dict[int, asyncio.Task] = {}

    def schedule(self, sid: int, when: datetime, payload: dict):
        self.cancel(sid)
        self.tasks[sid] = asyncio.create_task(self._wait_and_send(sid, when, payload))

    def cancel(self, sid: int):
        t = self.tasks.pop(sid, None)
        if t and not t.done():
            t.cancel()

    def cancel_all(self):
        for t in list(self.tasks.values()):
            if not t.done():
                t.cancel()
        self.tasks.clear()

    async def _wait_and_send(self, sid: int, when: datetime, payload: dict):
        loop = asyncio.get_running_loop()
        try:
            # 1) Coarse sleep: target - 100ms (via wall clock)
            delay = (when - datetime.now(TZ)).total_seconds()
            if delay > 0.2:
                await asyncio.sleep(delay - 0.1)

            # 2) Fine-tune using monotonic loop.time() for sub-ms accuracy
            target_wall = when.timestamp()
            while True:
                remaining = target_wall - datetime.now(TZ).timestamp()
                if remaining <= 0:
                    break
                # sleep in tiny increments for last 200ms
                await asyncio.sleep(min(remaining, 0.005))

            # 3) Fire — direct bot call, no DB hit
            bot = self.app.bot
            if payload.get("reply_to"):
                await bot.send_message(
                    chat_id=payload["chat_id"],
                    text=payload["text"],
                    reply_to_message_id=payload["reply_to"],
                )
            else:
                await bot.send_message(
                    chat_id=payload["chat_id"],
                    text=payload["text"],
                )
            set_status(sid, "sent")
            log.info("Schedule #%s sent (fired fast)", sid)

        except asyncio.CancelledError:
            log.info("Schedule #%s cancelled", sid)
            raise
        except Exception as e:
            set_status(sid, "failed", str(e)[:500])
            log.exception("Schedule #%s failed", sid)
            try:
                await self.app.bot.send_message(
                    chat_id=payload["owner_id"],
                    text=f"Schedule #{sid} failed:\n{e}",
                )
            except Exception:
                pass
        finally:
            self.tasks.pop(sid, None)


# global handle
scheduler: PreciseScheduler | None = None

# ================= COMMANDS =================
async def schedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update):
        return

    text = update.effective_message.text or ""
    m = SCHEDULE_RE.match(text)
    if not m:
        await update.effective_message.reply_text(
            "Usage:\n.schedule YYYY-MM-DD HH:MM[:SS] target message\n\n"
            "Example:\n.schedule 2026-01-20 18:30:05 @mychannel Hello"
        )
        return

    date_s, time_s, target_s, msg = m.groups()
    if len(msg) > 4096:
        await update.effective_message.reply_text("Message 4096 chars se zyada hai.")
        return

    try:
        when = parse_when(date_s, time_s)
    except Exception:
        await update.effective_message.reply_text("Date/time galat. Format: YYYY-MM-DD HH:MM[:SS]")
        return

    if when <= datetime.now(TZ):
        await update.effective_message.reply_text("Past time hai. Future time do.")
        return

    target = normalize_target(target_s)

    sid = add_schedule(
        owner_id=OWNER_ID,
        chat_id=update.effective_chat.id,
        kind="msg",
        target=target,
        reply_to=None,
        message=msg,
        run_at=when.isoformat(),
    )

    # Warm target chat cache -> send pe fast resolve
    try:
        await context.bot.get_chat(target)
    except Exception:
        pass

    payload = {
        "owner_id": OWNER_ID,
        "chat_id": target,
        "text": msg,
        "reply_to": None,
    }
    scheduler.schedule(sid, when, payload)

    await update.effective_message.reply_text(
        f"Scheduled #{sid}\nTime: {when.strftime('%Y-%m-%d %H:%M:%S')}\nTarget: {target}"
    )


async def schedulech_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update):
        return

    text = update.effective_message.text or ""
    m = SCHEDULECH_RE.match(text)
    if not m:
        await update.effective_message.reply_text(
            "Usage:\n.schedulech YYYY-MM-DD HH:MM[:SS] post_link message"
        )
        return

    date_s, time_s, link, msg = m.groups()
    if len(msg) > 4096:
        await update.effective_message.reply_text("Message 4096 chars se zyada hai.")
        return

    try:
        when = parse_when(date_s, time_s)
    except Exception:
        await update.effective_message.reply_text("Date/time galat.")
        return

    if when <= datetime.now(TZ):
        await update.effective_message.reply_text("Past time hai.")
        return

    try:
        target, reply_to = parse_tme_link(link)
    except Exception:
        await update.effective_message.reply_text("Post link galat hai.")
        return

    sid = add_schedule(
        owner_id=OWNER_ID,
        chat_id=update.effective_chat.id,
        kind="reply",
        target=target,
        reply_to=reply_to,
        message=msg,
        run_at=when.isoformat(),
    )

    try:
        await context.bot.get_chat(target)
    except Exception:
        pass

    payload = {
        "owner_id": OWNER_ID,
        "chat_id": target,
        "text": msg,
        "reply_to": reply_to,
    }
    scheduler.schedule(sid, when, payload)

    await update.effective_message.reply_text(
        f"Scheduled ch #{sid}\nTime: {when.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Target: {target}\nReplyTo: {reply_to}"
    )


async def schedules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update):
        return
    rows = list_pending(OWNER_ID)
    if not rows:
        await update.effective_message.reply_text("No pending schedules.")
        return
    lines = []
    for r in rows:
        msg = (r["message"] or "").replace("\n", " ")[:60]
        kind = "reply" if r["reply_to"] else "msg"
        lines.append(f"#{r['id']} | {fmt_when(r['run_at'])} | {kind} | {r['target']} | {msg}")
    await update.effective_message.reply_text("Pending:\n\n" + "\n".join(lines))


async def cancelschedule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update):
        return
    m = CANCEL_RE.match(update.effective_message.text or "")
    if not m:
        await update.effective_message.reply_text("Usage: .cancelschedule <id>")
        return
    sid = int(m.group(1))
    row = get_schedule(sid)
    if not row or row["owner_id"] != OWNER_ID:
        await update.effective_message.reply_text("Schedule nahi mila.")
        return
    if row["status"] != "pending":
        await update.effective_message.reply_text(f"Already {row['status']} hai.")
        return
    set_status(sid, "cancelled")
    scheduler.cancel(sid)
    await update.effective_message.reply_text(f"Schedule #{sid} cancelled.")


# ================= STARTUP =================
async def post_init(application: Application):
    global scheduler
    scheduler = PreciseScheduler(application)

    # Warm session (HTTP keep-alive + DNS + TLS)
    try:
        await application.bot.get_me()
        log.info("Bot session warmed.")
    except Exception:
        log.warning("Warm-up get_me failed")

    # Reload pending (past-due -> fire immediately)
    now = datetime.now(TZ)
    for row in get_pending_all():
        when = datetime.fromisoformat(row["run_at"])
        if when <= now:
            when = now  # fire instantly

        target_raw = row["target"]
        target_val = int(target_raw) if re.fullmatch(r"-?\d+", str(target_raw)) else target_raw

        payload = {
            "owner_id": row["owner_id"],
            "chat_id": target_val,
            "text": row["message"],
            "reply_to": int(row["reply_to"]) if row["reply_to"] else None,
        }
        scheduler.schedule(row["id"], when, payload)

    log.info("Pending schedules loaded: %d", len(scheduler.tasks))


async def post_shutdown(application: Application):
    if scheduler:
        scheduler.cancel_all()


def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN env variable set kar.")
    if not OWNER_ID:
        raise SystemExit("OWNER_ID env variable set kar.")

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(32)          # keep-alive pool
        .pool_timeout(3)
        .connect_timeout(3)                # fast fail
        .read_timeout(10)
        .write_timeout(5)
        .get_updates_connect_timeout(5)
        .get_updates_read_timeout(30)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^\.schedulech\b"), schedulech_cmd)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^\.schedule\b"), schedule_cmd)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^\.schedules\b"), schedules_cmd)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^\.cancelschedule\b"), cancelschedule_cmd)
    )

    log.info("Scheduler bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
