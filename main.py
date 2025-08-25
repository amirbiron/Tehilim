import os
import json
import re
import html
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Literal
from zoneinfo import ZoneInfo
import requests
from convertdate import hebrew

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)
from telegram.error import BadRequest

from activity_reporter import create_reporter

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

# Activity reporter (keep near top after env vars)
reporter = create_reporter(
    mongodb_uri="mongodb+srv://mumin:M43M2TFgLfGvhBwY@muminai.tm6x81b.mongodb.net/?retryWrites=true&w=majority&appName=muminAI",
    service_id="srv-d2lu1cjipnbc738l72q0",
    service_name="Tehilim"
)

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


# Database and bookmarks with per-mode support
_conn: Optional[sqlite3.Connection] = None
Mode = Literal['regular', 'monthly', 'weekly']


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
                chapter_regular INTEGER NOT NULL DEFAULT 1,
                chapter_monthly INTEGER NOT NULL DEFAULT 1,
                chapter_weekly INTEGER NOT NULL DEFAULT 1,
                current_mode TEXT NOT NULL DEFAULT 'regular',
                updated_at TEXT NOT NULL
            )
            '''
        )
        _migrate_bookmarks_schema(_conn)
        _conn.commit()
    return _conn


def _migrate_bookmarks_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(bookmarks)")
    cols = {row[1] for row in cur.fetchall()}
    try:
        if 'chapter_regular' not in cols:
            conn.execute("ALTER TABLE bookmarks ADD COLUMN chapter_regular INTEGER NOT NULL DEFAULT 1")
            conn.execute("UPDATE bookmarks SET chapter_regular = chapter")
        if 'chapter_monthly' not in cols:
            conn.execute("ALTER TABLE bookmarks ADD COLUMN chapter_monthly INTEGER NOT NULL DEFAULT 1")
        if 'chapter_weekly' not in cols:
            conn.execute("ALTER TABLE bookmarks ADD COLUMN chapter_weekly INTEGER NOT NULL DEFAULT 1")
        if 'current_mode' not in cols:
            conn.execute("ALTER TABLE bookmarks ADD COLUMN current_mode TEXT NOT NULL DEFAULT 'regular'")
    except sqlite3.OperationalError:
        # Ignore duplicate column errors if they happen due to races
        pass


def get_current_mode(user_id: int) -> Mode:
    conn = get_conn()
    cur = conn.execute("SELECT current_mode FROM bookmarks WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        return 'regular'
    val = str(row[0])
    return 'regular' if val not in ('regular', 'monthly', 'weekly') else val  # type: ignore[return-value]


def set_current_mode(user_id: int, mode: Mode) -> None:
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO bookmarks(user_id, chapter, current_mode, updated_at)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            current_mode=excluded.current_mode,
            updated_at=excluded.updated_at
        """,
        (user_id, 1, mode, now),
    )
    conn.commit()


def get_chapter(user_id: int, mode: Mode = 'regular') -> int:
    conn = get_conn()
    column = {
        'regular': 'chapter_regular',
        'monthly': 'chapter_monthly',
        'weekly': 'chapter_weekly',
    }[mode]
    cur = conn.execute(f"SELECT {column} FROM bookmarks WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 1


def set_chapter(user_id: int, chapter: int, mode: Mode = 'regular') -> None:
    chapter = max(1, min(MAX_CHAPTER, chapter))
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    if mode == 'regular':
        conn.execute(
            """
            INSERT INTO bookmarks(user_id, chapter, chapter_regular, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                chapter=excluded.chapter,
                chapter_regular=excluded.chapter_regular,
                updated_at=excluded.updated_at
            """,
            (user_id, chapter, chapter, now),
        )
    elif mode == 'monthly':
        conn.execute(
            """
            INSERT INTO bookmarks(user_id, chapter, chapter_monthly, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                chapter_monthly=excluded.chapter_monthly,
                updated_at=excluded.updated_at
            """,
            (user_id, 1, chapter, now),
        )
    else:  # weekly
        conn.execute(
            """
            INSERT INTO bookmarks(user_id, chapter, chapter_weekly, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                chapter_weekly=excluded.chapter_weekly,
                updated_at=excluded.updated_at
            """,
            (user_id, 1, chapter, now),
        )
    conn.commit()


# Splits
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


# Utils

def tznow() -> datetime:
    return datetime.now(ZoneInfo(TZ_NAME))


def render_range(ch_from: int, ch_to: int) -> str:
    return f"פרק {ch_from}" if ch_from == ch_to else f"פרקים {ch_from}–{ch_to}"


def to_hebrew_date_str(dt: datetime) -> str:
    y, m, d = hebrew.from_gregorian(dt.year, dt.month, dt.day)
    months = [
        "", "תשרי", "חשוון", "כסלו", "טבת", "שבט", "אדר א'", "אדר", "ניסן",
        "אייר", "סיוון", "תמוז", "אב", "אלול"
    ]
    is_leap = hebrew.leap(y)
    name = months[m]
    if not is_leap and m == 7:
        name = "אדר"
    return f"{d} ב{name} {y}"


API = "https://www.sefaria.org/api/texts/Psalms.{n}?lang=he"


def clean_sefaria_text(raw: str) -> str:
    text = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[\u0591-\u05AF\u05BD\u05BF]", "", text)
    text = text.replace("\u05C0", "")  # paseq ׀
    text = text.replace("\u05BE", " ")  # maqaf -> space
    text = text.replace("|", "")
    text = (
        text
        .replace("\u2009", " ")
        .replace("\u200a", " ")
        .replace("\u202f", " ")
        .replace("\xa0", " ")
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
    return text


def to_hebrew_numeral(n: int) -> str:
    units = ["", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט"]
    tens = ["", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ"]
    hundreds = ["", "ק", "ר", "ש", "ת"]
    if n <= 0:
        return str(n)
    parts: List[str] = []
    h = n // 100
    if h:
        parts.append(hundreds[h])
    n = n % 100
    if n == 15:
        parts.append("ט" + "ו")
        n = 0
    elif n == 16:
        parts.append("ט" + "ז")
        n = 0
    t = n // 10
    if t:
        parts.append(tens[t])
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
    lines: List[str] = []
    for i, v in enumerate(verses, start=1):
        cleaned = clean_sefaria_text(v)
        numeral = to_hebrew_numeral(i)
        lines.append(f"{numeral}. {cleaned}")
    return "\n".join(lines).strip()


def download_all_texts(data_path: str, ps119_parts_path: str) -> None:
    data_dir = os.path.dirname(data_path) or "."
    os.makedirs(data_dir, exist_ok=True)
    out: Dict[str, str] = {}
    for n in range(1, MAX_CHAPTER + 1):
        out[str(n)] = fetch_psalm(n)
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    parts_dir = os.path.dirname(ps119_parts_path) or "."
    os.makedirs(parts_dir, exist_ok=True)
    if not os.path.exists(ps119_parts_path):
        parts = {
            "25": "הדבק כאן חלק 1 של קי\"ט.",
            "26": "הדבק כאן חלק 2 של קי\"ט.",
            "27": "הדבק כאן חלק 3 של קי\"ט.",
            "28": "הדבק כאן חלק 4 של קי\"ט.",
        }
        with open(ps119_parts_path, "w", encoding="utf-8") as f:
            json.dump(parts, f, ensure_ascii=False, indent=2)


def ensure_texts_present() -> None:
    if os.path.exists(DATA_PATH):
        return
    logger.info("Data file %s not found; downloading Psalms...", DATA_PATH)
    download_all_texts(DATA_PATH, PS119_PARTS_PATH)
    global _tehillim_cache, _ps119_parts
    _tehillim_cache = {}
    _ps119_parts = {}
    logger.info("Downloaded Tehillim texts to %s", DATA_PATH)


def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID and str(user_id) == str(ADMIN_USER_ID)


# UI

def build_nav_keyboard() -> InlineKeyboardMarkup:
    # Compute today's monthly range (Hebrew calendar) for dynamic button label
    now = tznow()
    hy, hm, hd = hebrew.from_gregorian(now.year, now.month, now.day)
    day = hd if hd <= 30 else 30
    ch_from, ch_to = DAILY_SPLIT[day]
    monthly_label = f"🗓️ חודשי ({render_range(ch_from, ch_to)})"
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
            InlineKeyboardButton("📅 שבועי", callback_data="weekly"),
            InlineKeyboardButton("📖 רגיל", callback_data="regular"),
        ],
        [
            InlineKeyboardButton(monthly_label, callback_data="daily"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def split_to_chunks(text: str, limit: int = 3500) -> List[str]:
    chunks: List[str] = []
    buf: List[str] = []
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


def build_mode_label(mode: Mode) -> str:
    return {
        'regular': 'רגיל',
        'monthly': 'חודשי',
        'weekly': 'שבועי',
    }[mode]


def build_chapter_message(user_id: int, mode: Mode, chapter: int) -> str:
    data = load_tehillim(DATA_PATH)
    text = data.get(str(chapter))
    set_chapter(user_id, chapter, mode)
    if not text:
        return (
            f"פרק {chapter} חסר בקובץ הנתונים.\n"
            "אנא מלא/י את data/tehillim.json בכל הפרקים."
        )
    header = f"תהילים — פרק {chapter} (מצב: {build_mode_label(mode)})\n\n"
    return header + text


def build_daily_message_for_user(user_id: int) -> str:
    now = tznow()
    hy, hm, hd = hebrew.from_gregorian(now.year, now.month, now.day)
    day = hd if hd <= 30 else 30
    ch_from, ch_to = DAILY_SPLIT[day]
    heb_date = to_hebrew_date_str(now)
    header = f"חלוקה חודשית — {heb_date} (יום {day}): {render_range(ch_from, ch_to)}\n\n"
    set_current_mode(user_id, 'monthly')
    if ch_from == 119 and ch_to == 119 and os.path.exists(PS119_PARTS_PATH):
        parts = load_ps119_parts(PS119_PARTS_PATH)
        part_text = parts.get(str(day))
        if part_text:
            set_chapter(user_id, 119, 'monthly')
            return f"{header}פרק קי\"ט - חלק יום {day}\n\n{part_text}"
    set_chapter(user_id, ch_from, 'monthly')
    data = load_tehillim(DATA_PATH)
    txt = data.get(str(ch_from))
    if not txt:
        try:
            txt = fetch_psalm(ch_from)
            data[str(ch_from)] = txt
            os.makedirs(os.path.dirname(DATA_PATH) or ".", exist_ok=True)
            with open(DATA_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            txt = f"[חסר טקסט לפרק {ch_from}]"
    return f"{header}— פרק {ch_from} —\n{txt}\n"


def build_weekly_message_for_user(user_id: int) -> str:
    now = tznow()
    weekday = now.isoweekday()
    ch_from, ch_to = WEEKLY_SPLIT[weekday]
    header = f"חלוקה שבועית — יום {weekday}: {render_range(ch_from, ch_to)}\n\n"
    set_current_mode(user_id, 'weekly')
    set_chapter(user_id, ch_from, 'weekly')
    data = load_tehillim(DATA_PATH)
    t = data.get(str(ch_from), f"[חסר טקסט לפרק {ch_from}]")
    return f"{header}— פרק {ch_from} —\n{t}\n"


def trim_for_edit(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n\n[הטקסט קוצר לתצוגה]"
    return text[: max(0, limit - len(suffix))].rstrip() + suffix


async def edit_nav_message(q, text: str) -> None:
    try:
        await q.message.edit_text(text, reply_markup=build_nav_keyboard())
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if "message is too long" in msg:
            trimmed = trim_for_edit(text)
            await q.message.edit_text(trimmed, reply_markup=build_nav_keyboard())
            return
        raise


def normalize_chapter(n: int) -> int:
    if n < 1:
        return MAX_CHAPTER
    if n > MAX_CHAPTER:
        return 1
    return n


# Messaging
async def send_text_with_nav(update: Update, full_text: str) -> None:
    # For initial sends (commands like /start), we still send a new message.
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
    user_id = update.effective_user.id
    mode = get_current_mode(user_id)
    set_chapter(user_id, chapter, mode)
    mode_label = {
        'regular': 'רגיל',
        'monthly': 'חודשי',
        'weekly': 'שבועי',
    }[mode]
    header = f"תהילים — פרק {chapter} (מצב: {mode_label})\n\n"
    await send_text_with_nav(update, header + text)


# Handlers
async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    now = tznow()
    hy, hm, hd = hebrew.from_gregorian(now.year, now.month, now.day)
    day = hd if hd <= 30 else 30
    ch_from, ch_to = DAILY_SPLIT[day]
    heb_date = to_hebrew_date_str(now)
    header = f"חלוקה חודשית — {heb_date} (יום {day}): {render_range(ch_from, ch_to)}\n\n"
    user_id = update.effective_user.id
    set_current_mode(user_id, 'monthly')
    # If it's a Psalm 119 day and parts exist, show only the relevant part text
    if ch_from == 119 and ch_to == 119 and os.path.exists(PS119_PARTS_PATH):
        parts = load_ps119_parts(PS119_PARTS_PATH)
        part_text = parts.get(str(day))
        if part_text:
            set_chapter(user_id, 119, 'monthly')
            full = f"{header}פרק קי\"ט - חלק יום {day}\n\n{part_text}"
            await send_text_with_nav(update, full)
            return
    # Otherwise, send only the first chapter in the day's monthly range and set bookmark.
    set_chapter(user_id, ch_from, 'monthly')
    data = load_tehillim(DATA_PATH)
    txt = data.get(str(ch_from))
    if not txt:
        try:
            txt = fetch_psalm(ch_from)
            data[str(ch_from)] = txt
            # Persist back to file so next time it's available
            os.makedirs(os.path.dirname(DATA_PATH) or ".", exist_ok=True)
            with open(DATA_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            txt = f"[חסר טקסט לפרק {ch_from}]"
    full = f"{header}— פרק {ch_from} —\n{txt}\n"
    await send_text_with_nav(update, full)


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    now = tznow()
    weekday = now.isoweekday()  # Mon=1..Sun=7
    ch_from, ch_to = WEEKLY_SPLIT[weekday]
    header = f"חלוקה שבועית — יום {weekday}: {render_range(ch_from, ch_to)}\n\n"
    # Send only the first chapter of the weekly split, and set bookmark so nav works
    user_id = update.effective_user.id
    set_current_mode(user_id, 'weekly')
    set_chapter(user_id, ch_from, 'weekly')
    data = load_tehillim(DATA_PATH)
    t = data.get(str(ch_from), f"[חסר טקסט לפרק {ch_from}]")
    full = f"{header}— פרק {ch_from} —\n{t}\n"
    await send_text_with_nav(update, full)


async def cmd_load_texts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    global _tehillim_cache, _ps119_parts
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("פקודה זו מיועדת למנהל בלבד.")
        return
    await update.message.reply_text("מתחיל למשוך טקסטים (1–150)...")
    try:
        download_all_texts(DATA_PATH, PS119_PARTS_PATH)
    except Exception as e:
        await update.message.reply_text(f"שגיאה במשיכה: {e}")
        return
    # Invalidate in-memory caches so subsequent calls reload from disk
    _tehillim_cache = {}
    _ps119_parts = {}
    await update.message.reply_text("הטקסטים נשמרו בהצלחה.")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    user_id = update.effective_user.id
    mode = get_current_mode(user_id)
    chapter = get_chapter(user_id, mode)
    await send_chapter(update, context, chapter)


async def cmd_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    user_id = update.effective_user.id
    mode = get_current_mode(user_id)
    chapter = normalize_chapter(get_chapter(user_id, mode) + 1)
    await send_chapter(update, context, chapter)


async def cmd_prev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    user_id = update.effective_user.id
    mode = get_current_mode(user_id)
    chapter = normalize_chapter(get_chapter(user_id, mode) - 1)
    await send_chapter(update, context, chapter)


async def cmd_where(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    user_id = update.effective_user.id
    conn = get_conn()
    cur = conn.execute(
        "SELECT chapter_regular, chapter_monthly, chapter_weekly, current_mode FROM bookmarks WHERE user_id=?",
        (user_id,),
    )
    row = cur.fetchone()
    if row:
        reg, mon, wk, mode = row
    else:
        reg, mon, wk, mode = 1, 1, 1, 'regular'
    mode_label = {'regular': 'רגיל', 'monthly': 'חודשי', 'weekly': 'שבועי'}.get(str(mode), 'רגיל')
    msg = (
        f"מצב נוכחי: {mode_label}\n"
        f"סימניות — רגיל: {reg}, חודשי: {mon}, שבועי: {wk}"
    )
    await update.message.reply_text(msg)


async def cmd_goto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
    await update.message.reply_text("הקלד/י מספר פרק (1–150):")
    context.user_data["awaiting_goto"] = True


async def on_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reporter.report_activity(update.effective_user.id)
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
    reporter.report_activity(update.effective_user.id)
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    data = q.data

    if data == "next":
        mode = get_current_mode(user_id)
        chapter = normalize_chapter(get_chapter(user_id, mode) + 1)
        text = build_chapter_message(user_id, mode, chapter)
        await edit_nav_message(q, text)
    elif data == "prev":
        mode = get_current_mode(user_id)
        chapter = normalize_chapter(get_chapter(user_id, mode) - 1)
        text = build_chapter_message(user_id, mode, chapter)
        await edit_nav_message(q, text)
    elif data == "reset":
        mode = get_current_mode(user_id)
        set_chapter(user_id, 1, mode)
        await q.answer("הסימנייה אופסה לפרק 1.")
        text = build_chapter_message(user_id, mode, 1)
        await edit_nav_message(q, text)
    elif data == "goto":
        await q.message.reply_text("הקלד/י מספר פרק (1–150):")
        context.user_data["awaiting_goto"] = True
    elif data == "daily":
        text = build_daily_message_for_user(user_id)
        await edit_nav_message(q, text)
    elif data == "weekly":
        text = build_weekly_message_for_user(user_id)
        await edit_nav_message(q, text)
    elif data == "regular":
        set_current_mode(user_id, 'regular')
        chapter = get_chapter(user_id, 'regular')
        text = build_chapter_message(user_id, 'regular', chapter)
        await edit_nav_message(q, text)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Missing BOT_TOKEN env var")

    # Ensure texts exist on disk (first run on a new disk will auto-download)
    try:
        ensure_texts_present()
    except Exception as e:
        logger.exception("Failed ensuring texts present: %s", e)

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