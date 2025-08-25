import os
import json
import re
import html
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from zoneinfo import ZoneInfo
import requests

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("tehillim-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATA_PATH = os.environ.get("DATA_PATH", "data/tehillim.json")
PS119_PARTS_PATH = os.environ.get("PS119_PARTS_PATH", "data/psalm119_parts.json")
DB_PATH = os.environ.get("DB_PATH", "bookmarks.db")
TZ_NAME = os.environ.get("TZ_NAME", "Asia/Jerusalem")
MAX_CHAPTER = 150
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID", "")

_tehillim_cache: Dict[str, str] = {}
_ps119_parts: Dict[str, str] = {}

def load_tehillim(path: str) -> Dict[str, str]:
    global _tehillim_cache
    if _tehillim_cache:
        return _tehillim_cache
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _tehillim_cache = {str(k): str(v) for k, v in data.items()}
        logger.info("Loaded Tehillim (%d chapters) from %s", len(_tehillim_cache), path)
    except FileNotFoundError:
        logger.warning("%s not found. Using sample for chapters 1–2.", path)
        _tehillim_cache = {
            "1": "דוגמה: פרק א׳ — מלא כאן טקסט מלא.",
            "2": "דוגמה: פרק ב׳ — מלא כאן טקסט מלא.",
        }
    return _tehillim_cache

def load_ps119_parts(path: str) -> Dict[str, str]:
    global _ps119_parts
    if _ps119_parts:
        return _ps119_parts
    try:
        with open(path, "r", encoding="utf-8") as f:
            _ps119_parts = {str(k): str(v) for k, v in json.load(f).items()}
        logger.info("Loaded Psalm 119 parts from %s", path)
    except FileNotFoundError:
        _ps119_parts = {}
        logger.info("No psalm119_parts.json found — will fallback to full 119 on days 25–28.")
    return _ps119_parts

_conn: Optional[sqlite3.Connection] = None

def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS bookmarks (
                user_id INTEGER PRIMARY KEY,
                chapter INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            )
            '''
        )
        _conn.commit()
    return _conn

def get_chapter(user_id: int) -> int:
    conn = get_conn()
    cur = conn.execute("SELECT chapter FROM bookmarks WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    return int(row[0]) if row else 1

def set_chapter(user_id: int, chapter: int) -> None:
    chapter = max(1, min(MAX_CHAPTER, chapter))
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute(
        "INSERT INTO bookmarks(user_id, chapter, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET chapter=excluded.chapter, updated_at=excluded.updated_at",
        (user_id, chapter, now),
    )
    conn.commit()

def build_nav_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("◀️ הקודם", callback_data="prev"),
            InlineKeyboardButton("▶️ הבא", callback_data="next"),
        ],
        [
            InlineKeyboardButton("🔢 קפיצה לפרק", callback_data="goto"),
            InlineKeyboardButton("♻️ איפוס", callback_data="reset"),
        ],
        [
            InlineKeyboardButton("🗓️ יומי", callback_data="daily"),
            InlineKeyboardButton("📅 שבועי", callback_data="weekly"),
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def split_to_chunks(text: str, limit: int = 3500) -> List[str]:
    chunks, buf = [], []
    count = 0
    for line in text.splitlines(keepends=True):
        if count + len(line) > limit and buf:
            chunks.append("".join(buf))
            buf, count = [line], len(line)
        else:
            buf.append(line)
            count += len(line)
    if buf:
        chunks.append("".join(buf))
    return chunks or [text]

def normalize_chapter(n: int) -> int:
    if n < 1:
        return MAX_CHAPTER
    if n > MAX_CHAPTER:
        return 1
    return n

async def send_text_with_nav(update: Update, full_text: str) -> None:
    chunks = split_to_chunks(full_text)
    for i, ch in enumerate(chunks, start=1):
        if i == len(chunks):
            await update.effective_chat.send_message(ch, reply_markup=build_nav_keyboard())
        else:
            await update.effective_chat.send_message(ch)

async def send_chapter(update: Update, context: ContextTypes.DEFAULT_TYPE, chapter: int) -> None:
    data = load_tehillim(DATA_PATH)
    text = data.get(str(chapter))
    if not text:
        msg = (
            f"פרק {chapter} חסר בקובץ הנתונים.\n"
            "אנא מלא/י את data/tehillim.json בכל הפרקים."
        )
        await update.effective_chat.send_message(msg)
        return
    set_chapter(update.effective_user.id, chapter)
    header = f"תהילים — פרק {chapter}\n\n"
    await send_text_with_nav(update, header + text)

WEEKLY_SPLIT = {
    1: (1, 29),
    2: (30, 50),
    3: (51, 72),
    4: (73, 89),
    5: (90, 106),
    6: (107, 119),
    7: (120, 150),
}

DAILY_SPLIT = {
    1: (1, 9),
    2: (10, 17),
    3: (18, 22),
    4: (23, 28),
    5: (29, 34),
    6: (35, 38),
    7: (39, 43),
    8: (44, 48),
    9: (49, 54),
    10: (55, 59),
    11: (60, 65),
    12: (66, 68),
    13: (69, 71),
    14: (72, 76),
    15: (77, 78),
    16: (79, 82),
    17: (83, 87),
    18: (88, 89),
    19: (90, 96),
    20: (97, 103),
    21: (104, 105),
    22: (106, 107),
    23: (108, 112),
    24: (113, 118),
    25: (119, 119),
    26: (119, 119),
    27: (119, 119),
    28: (119, 119),
    29: (120, 134),
    30: (135, 150),
}

def tznow() -> datetime:
    return datetime.now(ZoneInfo(TZ_NAME))

def render_range(ch_from: int, ch_to: int) -> str:
    return f"פרק {ch_from}" if ch_from == ch_to else f"פרקים {ch_from}–{ch_to}"

API = "https://www.sefaria.org/api/texts/Psalms.{n}?lang=he"

def clean_sefaria_text(raw: str) -> str:
    # Convert line-break tags to newlines
    text = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities (&thinsp;, &nbsp;, etc.)
    text = html.unescape(text)
    # Remove cantillation marks (Ta'amei haMikra) and similar diacritics
    text = re.sub(r"[\u0591-\u05AF\u05BD\u05BF]", "", text)
    # Normalize special spaces to regular spaces
    text = (
        text
        .replace("\u2009", " ")  # thin space
        .replace("\u200a", " ")  # hair space
        .replace("\u202f", " ")  # narrow no-break space
        .replace("\xa0", " ")   # non-breaking space
    )
    # Collapse repeated spaces (not across newlines)
    text = re.sub(r"[ \t]+", " ", text)
    # Trim trailing spaces per line and overall
    text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    return text

def to_hebrew_numeral(n: int) -> str:
    units = ["", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט"]
    tens = ["", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ"]
    hundreds = ["", "ק", "ר", "ש", "ת"]
    if n <= 0:
        return str(n)
    parts = []
    # Hundreds (up to 400)
    h = n // 100
    if h:
        parts.append(hundreds[h])
    n = n % 100
    # Special cases 15 and 16 to avoid forming divine name
    if n == 15:
        parts.append("ט" + "ו")
        n = 0
    elif n == 16:
        parts.append("ט" + "ז")
        n = 0
    # Tens
    t = n // 10
    if t:
        parts.append(tens[t])
    # Units
    u = n % 10
    if u:
        parts.append(units[u])
    return "".join(parts) or "א"

def fetch_psalm(n: int) -> str:
    url = API.format(n=n)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    js = r.json()
    verses = js.get("he") or []
    lines = []
    for i, v in enumerate(verses, start=1):
        cleaned = clean_sefaria_text(v)
        numeral = to_hebrew_numeral(i)
        lines.append(f"{numeral}. {cleaned}")
    return "\n".join(lines).strip()

def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID and str(user_id) == str(ADMIN_USER_ID)

async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = tznow()
    day = now.day if now.day <= 30 else 30
    ch_from, ch_to = DAILY_SPLIT[day]
    header = f"חלוקה יומית (ל' בחודש) — יום {day}: {render_range(ch_from, ch_to)}\n\n"

    if ch_from == 119 and ch_to == 119 and os.path.exists(PS119_PARTS_PATH):
        parts = load_ps119_parts(PS119_PARTS_PATH)
        if str(day) in parts:
            full = f"{header}פרק קי\"ט - חלק יום {day}\n\n{parts[str(day)]}"
            await send_text_with_nav(update, full)
            return

    data = load_tehillim(DATA_PATH)
    texts = []
    for ch in range(ch_from, ch_to + 1):
        t = data.get(str(ch), f"[חסר טקסט לפרק {ch}]")
        texts.append(f"— פרק {ch} —\n{t}\n")
    await send_text_with_nav(update, header + "\n".join(texts))

async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = tznow()
    weekday = now.isoweekday()  # Mon=1..Sun=7
    ch_from, ch_to = WEEKLY_SPLIT[weekday]
    header = f"חלוקה שבועית — יום {weekday}: {render_range(ch_from, ch_to)}\n\n"
    data = load_tehillim(DATA_PATH)
    texts = []
    for ch in range(ch_from, ch_to + 1):
        t = data.get(str(ch), f"[חסר טקסט לפרק {ch}]")
        texts.append(f"— פרק {ch} —\n{t}\n")
    await send_text_with_nav(update, header + "\n".join(texts))

async def cmd_load_texts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _tehillim_cache, _ps119_parts
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("פקודה זו מיועדת למנהל בלבד.")
        return
    await update.message.reply_text("מתחיל למשוך טקסטים (1–150)...")
    data_dir = os.path.dirname(DATA_PATH) or "."
    os.makedirs(data_dir, exist_ok=True)
    out = {}
    try:
        for n in range(1, 151):
            out[str(n)] = fetch_psalm(n)
        with open(DATA_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception as e:
        await update.message.reply_text(f"שגיאה במשיכה: {e}")
        return
    if not os.path.exists(PS119_PARTS_PATH):
        parts = {
            "25": "הדבק כאן חלק 1 של קי\"ט.",
            "26": "הדבק כאן חלק 2 של קי\"ט.",
            "27": "הדבק כאן חלק 3 של קי\"ט.",
            "28": "הדבק כאן חלק 4 של קי\"ט.",
        }
        with open(PS119_PARTS_PATH, "w", encoding="utf-8") as f:
            json.dump(parts, f, ensure_ascii=False, indent=2)
    # Invalidate in-memory caches so subsequent calls reload from disk
    _tehillim_cache = {}
    _ps119_parts = {}
    await update.message.reply_text("הטקסטים נשמרו בהצלחה.")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chapter = get_chapter(user_id)
    await send_chapter(update, context, chapter)

async def cmd_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chapter = normalize_chapter(get_chapter(user_id) + 1)
    await send_chapter(update, context, chapter)

async def cmd_prev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chapter = normalize_chapter(get_chapter(user_id) - 1)
    await send_chapter(update, context, chapter)

async def cmd_where(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chapter = get_chapter(update.effective_user.id)
    await update.message.reply_text(f"הסימנייה שלך נמצאת בפרק {chapter}.")

async def cmd_goto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("הקלד/י מספר פרק (1–150):")
    context.user_data["awaiting_goto"] = True

async def on_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("awaiting_goto"):
        try:
            n = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("אנא הזן/י מספר תקין בין 1 ל־150.")
            return
        if not (1 <= n <= MAX_CHAPTER):
            await update.message.reply_text("טווח חוקי: 1–150.")
            return
        context.user_data["awaiting_goto"] = False
        await send_chapter(update, context, n)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    data = q.data

    if data == "next":
        chapter = normalize_chapter(get_chapter(user_id) + 1)
        await send_chapter(update, context, chapter)
    elif data == "prev":
        chapter = normalize_chapter(get_chapter(user_id) - 1)
        await send_chapter(update, context, chapter)
    elif data == "reset":
        set_chapter(user_id, 1)
        await q.edit_message_reply_markup(reply_markup=None)
        await q.message.reply_text("הסימנייה אופסה לפרק 1.")
        await send_chapter(update, context, 1)
    elif data == "goto":
        await q.message.reply_text("הקלד/י מספר פרק (1–150):")
        context.user_data["awaiting_goto"] = True
    elif data == "daily":
        await cmd_daily(update, context)
    elif data == "weekly":
        await cmd_weekly(update, context)

def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Missing BOT_TOKEN env var")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("next", cmd_next))
    app.add_handler(CommandHandler("prev", cmd_prev))
    app.add_handler(CommandHandler("where", cmd_where))
    app.add_handler(CommandHandler("goto", cmd_goto))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("weekly", cmd_weekly))
    app.add_handler(CommandHandler("load_texts", cmd_load_texts))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_free_text))

    logger.info("Starting polling...")
    app.run_polling(drop_pending_updates=True, allowed_updates=None)

if __name__ == "__main__":
    main()
