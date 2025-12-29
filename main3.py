# uwallet_full_final.py
# ============================================================
# UWallet (UWT) — SINGLE-FILE TELEGRAM BOT (aiogram 3)
# UI: Inline menu (no reply keyboard)
# Checks/Bills: URL buttons (deep-link /start payload) like CryptoBot
# Checks: multi-use (type 1: max claims) + password + description
# Optional: required channel subscriptions for claiming checks
# Exchange: automatic at admin-defined rate (RUB <-> UWT)
# P2P: user-to-user transfers (UWT and RUB)
# Биржа: simple order book + matching for UWT/RUB
# Розыгрыши: inline create/join + background финализация
# Каналы: пользователи могут добавлять свои каналы и продавать подписку на месяц за UWT
#
# Install:
#   pip install -U aiogram==3.* python-dotenv
# .env:
#   BOT_TOKEN=123:ABC
#   DB_PATH=uwallet.db
#
# Run:
#   python3 uwallet_full_final.py
# ============================================================

import os
import re
import shlex
import uuid
import sqlite3
import hashlib
import asyncio
import secrets
from datetime import datetime, timedelta

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InlineQuery, InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.exceptions import TelegramBadRequest

# -------------------- CONFIG --------------------
load_dotenv()
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in .env (BOT_TOKEN=...)")

DB_PATH = (os.getenv("DB_PATH") or "uwallet.db").strip() or "uwallet.db"

# Админы по username (без @)
DEFAULT_ADMINS = {"enzekoin", "motidevch"}

DEFAULT_RATE_RUB_PER_UWT = 10.0
MAX_DESC_LEN = 180
MAX_PASS_LEN = 32

# Background
GIVEAWAY_POLL_SEC = 20
SUBS_POLL_SEC = 120

# Business rules
CHECK_REQUIRE_SUBS = True  # обязательные подписки для получения чеков (админы добавляют в список)

BOT_USERNAME: str | None = None  # set in main()

# -------------------- HELPERS --------------------
def utcnow() -> datetime:
    return datetime.utcnow()

def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()

def now_iso() -> str:
    return iso(utcnow())

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def fmt_num(x: float) -> str:
    s = f"{x:.8f}".rstrip("0").rstrip(".")
    return s if s else "0"

def clean_username(u: str) -> str:
    return (u or "").strip().lstrip("@").lower()

def safe_desc(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if not s or s == "-":
        return None
    return s[:MAX_DESC_LEN]

def safe_pass(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if not s or s == "-":
        return None
    return s[:MAX_PASS_LEN]

def require_username_text() -> str:
    return (
        "⚠️ У вас не установлен username в Telegram.\n\n"
        "Telegram → Настройки → Имя пользователя (Username)\n"
        "Потом вернитесь и нажмите /start"
    )

async def safe_edit(message, text: str, **kwargs):
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise

# -------------------- DB --------------------
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS users(
        tg_id INTEGER PRIMARY KEY,
        username TEXT UNIQUE,
        uwt REAL NOT NULL DEFAULT 0,
        rub REAL NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS admins(
        username TEXT PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS settings(
        k TEXT PRIMARY KEY,
        v TEXT
    );

    CREATE TABLE IF NOT EXISTS tx(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER NOT NULL,
        asset TEXT NOT NULL,
        delta REAL NOT NULL,
        kind TEXT NOT NULL,
        meta TEXT,
        created_at TEXT NOT NULL
    );

    -- Checks (multi-use type1)
    CREATE TABLE IF NOT EXISTS checks(
        id TEXT PRIMARY KEY,
        token TEXT UNIQUE,
        creator_tg_id INTEGER NOT NULL,
        total_amount REAL NOT NULL,
        per_claim REAL NOT NULL,
        max_claims INTEGER NOT NULL,
        claimed_count INTEGER NOT NULL DEFAULT 0,
        description TEXT,
        passhash TEXT,
        status TEXT NOT NULL,      -- active / finished / cancelled
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS check_claims(
        check_id TEXT NOT NULL,
        user_tg_id INTEGER NOT NULL,
        claimed_at TEXT NOT NULL,
        PRIMARY KEY(check_id, user_tg_id)
    );

    -- Bills in UWT
    CREATE TABLE IF NOT EXISTS bills_uwt(
        id TEXT PRIMARY KEY,
        token TEXT UNIQUE,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        description TEXT,
        status TEXT NOT NULL,      -- active / paid / cancelled
        paid_by_tg_id INTEGER,
        created_at TEXT NOT NULL,
        paid_at TEXT
    );

    -- Giveaways
    CREATE TABLE IF NOT EXISTS giveaways(
        id TEXT PRIMARY KEY,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        status TEXT NOT NULL,      -- active / finished
        end_at TEXT NOT NULL,
        winner_tg_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS giveaway_participants(
        giveaway_id TEXT NOT NULL,
        user_tg_id INTEGER NOT NULL,
        PRIMARY KEY(giveaway_id, user_tg_id)
    );

    -- Биржа UWT/RUB
    CREATE TABLE IF NOT EXISTS orders(
        id TEXT PRIMARY KEY,
        user_tg_id INTEGER NOT NULL,
        side TEXT NOT NULL,           -- buy / sell
        price REAL NOT NULL,          -- RUB per 1 UWT
        amount REAL NOT NULL,         -- total amount UWT
        remaining REAL NOT NULL,
        status TEXT NOT NULL,         -- open / filled / cancelled
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS trades(
        id TEXT PRIMARY KEY,
        buy_order_id TEXT NOT NULL,
        sell_order_id TEXT NOT NULL,
        price REAL NOT NULL,
        amount REAL NOT NULL,
        created_at TEXT NOT NULL
    );

    -- Channels marketplace
    CREATE TABLE IF NOT EXISTS channels(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_tg_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL UNIQUE,
        title TEXT,
        username TEXT,
        price_uwt REAL NOT NULL,
        invite_link TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS channel_subs(
        id TEXT PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        user_tg_id INTEGER NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(channel_id, user_tg_id)
    );

    -- Required channels (for claiming checks)
    CREATE TABLE IF NOT EXISTS required_channels(
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        username TEXT,
        added_at TEXT NOT NULL
    );
    """)

    cur.execute("INSERT OR IGNORE INTO settings(k,v) VALUES('rate_rub_per_uwt', ?)", (str(DEFAULT_RATE_RUB_PER_UWT),))
    for a in DEFAULT_ADMINS:
        cur.execute("INSERT OR IGNORE INTO admins(username) VALUES(?)", (a,))
    con.commit()
    con.close()

def ensure_user(tg_id: int, username: str):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO users(tg_id, username, uwt, rub, created_at) VALUES(?,?,?,?,?)",
        (tg_id, username.lower(), 0.0, 0.0, now_iso())
    )
    cur.execute("UPDATE users SET username=? WHERE tg_id=?", (username.lower(), tg_id))
    con.commit()
    con.close()

def is_admin(username: str | None) -> bool:
    u = clean_username(username or "")
    if not u:
        return False
    con = db()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM admins WHERE username=?", (u,))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def get_rate() -> float:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT v FROM settings WHERE k='rate_rub_per_uwt'")
    row = cur.fetchone()
    con.close()
    try:
        return float(row["v"]) if row else DEFAULT_RATE_RUB_PER_UWT
    except Exception:
        return DEFAULT_RATE_RUB_PER_UWT

def set_rate(v: float):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE settings SET v=? WHERE k='rate_rub_per_uwt'", (str(v),))
    con.commit()
    con.close()

def get_balances(tg_id: int) -> tuple[float, float]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT uwt, rub FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return (0.0, 0.0)
    return (float(row["uwt"]), float(row["rub"]))

def add_asset(tg_id: int, asset: str, delta: float, kind: str, meta: str = ""):
    con = db()
    cur = con.cursor()
    if asset == "UWT":
        cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (delta, tg_id))
    elif asset == "RUB":
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (delta, tg_id))
    else:
        con.close()
        raise ValueError("Bad asset")

    cur.execute(
        "INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
        (tg_id, asset, float(delta), kind, meta, now_iso())
    )
    con.commit()
    con.close()

def last_txs(tg_id: int, limit: int = 15):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM tx WHERE tg_id=? ORDER BY id DESC LIMIT ?", (tg_id, limit))
    rows = cur.fetchall()
    con.close()
    return rows

# -------------------- REQUIRED CHANNELS (checks gate) --------------------
def req_channels_list():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM required_channels ORDER BY added_at DESC")
    rows = cur.fetchall()
    con.close()
    return rows

def req_channels_add(chat_id: int, title: str | None, username: str | None):
    con = db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO required_channels(chat_id, title, username, added_at) VALUES(?,?,?,?)",
                (chat_id, title, username, now_iso()))
    con.commit()
    con.close()

def req_channels_remove(chat_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM required_channels WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()

async def user_in_required_channels(bot: Bot, user_id: int) -> tuple[bool, list[str]]:
    """
    Returns (ok, missing_titles)
    """
    if not CHECK_REQUIRE_SUBS:
        return True, []
    rows = req_channels_list()
    missing = []
    for r in rows:
        chat_id = int(r["chat_id"])
        title = r["title"] or (f"@{r['username']}" if r["username"] else str(chat_id))
        try:
            cm = await bot.get_chat_member(chat_id, user_id)
            # statuses: creator, administrator, member, restricted, left, kicked
            if cm.status in ("left", "kicked"):
                missing.append(title)
        except Exception:
            # if bot can't check -> treat as missing
            missing.append(title)
    return (len(missing) == 0), missing

# -------------------- CHECKS --------------------
def create_check_multi(creator_id: int, total_amount: float, per_claim: float, max_claims: int,
                       desc: str | None, password: str | None) -> tuple[bool, str]:
    if total_amount <= 0 or per_claim <= 0 or max_claims <= 0:
        return (False, "Суммы и количество должны быть > 0")
    required = per_claim * max_claims
    if total_amount + 1e-12 < required:
        return (False, f"❌ Общая сумма меньше чем per_claim*max_claims ({fmt_num(required)})")

    uwt, _ = get_balances(creator_id)
    if uwt + 1e-12 < total_amount:
        return (False, "❌ Недостаточно UWT")

    token = secrets.token_urlsafe(8)
    check_id = str(uuid.uuid4())
    ph = sha256(password) if password else None

    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (total_amount, creator_id))
    cur.execute(
        "INSERT INTO checks(id, token, creator_tg_id, total_amount, per_claim, max_claims, claimed_count, description, passhash, status, created_at) "
        "VALUES(?,?,?,?,?,?,0,?,?,'active',?)",
        (check_id, token, creator_id, total_amount, per_claim, int(max_claims), desc, ph, now_iso())
    )
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (creator_id, "UWT", -total_amount, "check_create", f"token={token};total={total_amount};per={per_claim};max={max_claims}", now_iso()))
    con.commit()
    con.close()
    return (True, token)

def check_info(token: str):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM checks WHERE token=?", (token,))
    row = cur.fetchone()
    con.close()
    return row

def claim_check_by_token(token: str, user_id: int, password: str | None) -> tuple[bool, str, dict | None]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM checks WHERE token=?", (token,))
    row = cur.fetchone()
    if not row:
        con.close()
        return (False, "❌ Чек не найден", None)
    if row["status"] != "active":
        con.close()
        return (False, "❌ Чек недоступен", None)

    if row["passhash"]:
        if not password:
            con.close()
            return (False, "__NEED_PASS__", {"need_pass": True, "token": token})
        if sha256(password) != row["passhash"]:
            con.close()
            return (False, "❌ Неверный пароль", None)

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT 1 FROM check_claims WHERE check_id=? AND user_tg_id=?", (row["id"], user_id))
    if cur.fetchone():
        con.rollback(); con.close()
        return (False, "⚠️ Вы уже получали из этого чека", None)

    cur.execute("SELECT claimed_count, max_claims, per_claim FROM checks WHERE token=? AND status='active'", (token,))
    r2 = cur.fetchone()
    if not r2:
        con.rollback(); con.close()
        return (False, "❌ Чек недоступен", None)

    claimed = int(r2["claimed_count"])
    maxc = int(r2["max_claims"])
    if claimed >= maxc:
        cur.execute("UPDATE checks SET status='finished' WHERE token=?", (token,))
        con.commit(); con.close()
        return (False, "❌ Чек закончился", None)

    per = float(r2["per_claim"])
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (per, user_id))
    cur.execute("INSERT INTO check_claims(check_id, user_tg_id, claimed_at) VALUES(?,?,?)",
                (row["id"], user_id, now_iso()))
    cur.execute("UPDATE checks SET claimed_count=claimed_count+1 WHERE token=?", (token,))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, "UWT", per, "check_claim", f"token={token}", now_iso()))

    cur.execute("SELECT claimed_count, max_claims FROM checks WHERE token=?", (token,))
    rr = cur.fetchone()
    left = 0
    if rr:
        left = int(rr["max_claims"]) - int(rr["claimed_count"])
        if left <= 0:
            cur.execute("UPDATE checks SET status='finished' WHERE token=?", (token,))
    con.commit()
    con.close()
    return (True, f"✅ Вы получили {fmt_num(per)} UWT. Осталось получений: {left}", None)

# -------------------- BILLS --------------------
def create_bill_uwt_by_token(creator_id: int, amount: float, desc: str | None) -> tuple[bool, str]:
    if amount <= 0:
        return (False, "Сумма должна быть > 0")
    token = secrets.token_urlsafe(8)
    bill_id = str(uuid.uuid4())
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO bills_uwt(id, token, creator_tg_id, amount, description, status, created_at) "
        "VALUES(?,?,?,?,?,'active',?)",
        (bill_id, token, creator_id, amount, desc, now_iso())
    )
    con.commit()
    con.close()
    return (True, token)

def bill_info(token: str):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM bills_uwt WHERE token=?", (token,))
    row = cur.fetchone()
    con.close()
    return row

def pay_bill_by_token(token: str, payer_id: int) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM bills_uwt WHERE token=?", (token,))
    b = cur.fetchone()
    if not b:
        con.close()
        return (False, "❌ Счёт не найден")
    if b["status"] != "active":
        con.close()
        return (False, "❌ Счёт недоступен")
    creator = int(b["creator_tg_id"])
    if creator == payer_id:
        con.close()
        return (False, "❌ Нельзя оплатить самому себе")

    amount = float(b["amount"])
    uwt, _ = get_balances(payer_id)
    if uwt + 1e-12 < amount:
        con.close()
        return (False, "❌ Недостаточно UWT")

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE bills_uwt SET status='paid', paid_by_tg_id=?, paid_at=? WHERE token=? AND status='active'",
                (payer_id, now_iso(), token))
    if cur.rowcount != 1:
        con.rollback(); con.close()
        return (False, "❌ Уже оплачено/недоступно")

    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, payer_id))
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, creator))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (payer_id, "UWT", -amount, "bill_pay", f"token={token}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (creator, "UWT", amount, "bill_receive", f"token={token}", now_iso()))
    con.commit()
    con.close()
    return (True, f"✅ Оплачено {fmt_num(amount)} UWT")

# -------------------- EXCHANGE (AUTO) --------------------
def exchange_buy(uid: int, rub_amount: float) -> tuple[bool, str]:
    if rub_amount <= 0:
        return False, "Сумма должна быть > 0"
    rate = get_rate()
    uwt, rub = get_balances(uid)
    if rub + 1e-12 < rub_amount:
        return False, "❌ Недостаточно RUB"
    uwt_get = rub_amount / rate
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE users SET rub=rub-?, uwt=uwt+? WHERE tg_id=?", (rub_amount, uwt_get, uid))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (uid, "RUB", -rub_amount, "exchange_buy", f"rate={rate}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (uid, "UWT", uwt_get, "exchange_buy", f"rate={rate}", now_iso()))
    con.commit(); con.close()
    return True, f"✅ Куплено {fmt_num(uwt_get)} UWT за {rub_amount:g} ₽ (курс {rate:g} ₽/UWT)"

def exchange_sell(uid: int, uwt_amount: float) -> tuple[bool, str]:
    if uwt_amount <= 0:
        return False, "Сумма должна быть > 0"
    rate = get_rate()
    uwt, rub = get_balances(uid)
    if uwt + 1e-12 < uwt_amount:
        return False, "❌ Недостаточно UWT"
    rub_get = uwt_amount * rate
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE users SET uwt=uwt-?, rub=rub+? WHERE tg_id=?", (uwt_amount, rub_get, uid))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (uid, "UWT", -uwt_amount, "exchange_sell", f"rate={rate}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (uid, "RUB", rub_get, "exchange_sell", f"rate={rate}", now_iso()))
    con.commit(); con.close()
    return True, f"✅ Продано {fmt_num(uwt_amount)} UWT за {rub_get:g} ₽ (курс {rate:g} ₽/UWT)"

# -------------------- P2P TRANSFER --------------------
def p2p_transfer(from_id: int, to_username: str, asset: str, amount: float) -> tuple[bool, str, int | None]:
    asset = asset.upper()
    if asset not in ("UWT", "RUB"):
        return False, "Актив должен быть UWT или RUB", None
    if amount <= 0:
        return False, "Сумма должна быть > 0", None

    to_u = clean_username(to_username)
    con = db()
    cur = con.cursor()
    cur.execute("SELECT tg_id FROM users WHERE username=?", (to_u,))
    row = cur.fetchone()
    if not row:
        con.close()
        return False, "❌ Пользователь не найден (он должен хоть раз нажать /start у бота)", None
    to_id = int(row["tg_id"])

    uwt, rub = get_balances(from_id)
    bal = uwt if asset == "UWT" else rub
    if bal + 1e-12 < amount:
        con.close()
        return False, f"❌ Недостаточно {asset}", None

    cur.execute("BEGIN IMMEDIATE")
    if asset == "UWT":
        cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, from_id))
        cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, to_id))
    else:
        cur.execute("UPDATE users SET rub=rub-? WHERE tg_id=?", (amount, from_id))
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (amount, to_id))

    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (from_id, asset, -amount, "p2p_send", f"to={to_u}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (to_id, asset, amount, "p2p_recv", f"from={from_id}", now_iso()))
    con.commit(); con.close()
    return True, f"✅ Отправлено {fmt_num(amount)} {asset} пользователю @{to_u}", to_id

# -------------------- BIRZA (ORDERBOOK + MATCH) --------------------
def _order_lock_funds(cur: sqlite3.Cursor, uid: int, side: str, price: float, amount: float):
    if side == "buy":
        cost = price * amount
        cur.execute("SELECT rub FROM users WHERE tg_id=?", (uid,))
        rub = float(cur.fetchone()["rub"])
        if rub + 1e-12 < cost:
            raise ValueError("Недостаточно RUB")
        cur.execute("UPDATE users SET rub=rub-? WHERE tg_id=?", (cost, uid))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (uid, "RUB", -cost, "order_lock", f"buy cost={cost:g}", now_iso()))
    else:
        cur.execute("SELECT uwt FROM users WHERE tg_id=?", (uid,))
        uwt = float(cur.fetchone()["uwt"])
        if uwt + 1e-12 < amount:
            raise ValueError("Недостаточно UWT")
        cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, uid))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (uid, "UWT", -amount, "order_lock", f"sell amt={amount:g}", now_iso()))

def _order_refund(cur: sqlite3.Cursor, uid: int, side: str, price: float, remaining: float):
    if remaining <= 0:
        return
    if side == "buy":
        refund = price * remaining
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (refund, uid))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (uid, "RUB", refund, "order_refund", "", now_iso()))
    else:
        cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (remaining, uid))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (uid, "UWT", remaining, "order_refund", "", now_iso()))

def place_order(uid: int, side: str, price: float, amount: float) -> tuple[bool, str]:
    side = side.lower()
    if side not in ("buy", "sell"):
        return False, "side должен быть buy/sell"
    if price <= 0 or amount <= 0:
        return False, "Цена и количество должны быть > 0"

    oid = str(uuid.uuid4())
    con = db()
    cur = con.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _order_lock_funds(cur, uid, side, price, amount)
        cur.execute("INSERT INTO orders(id,user_tg_id,side,price,amount,remaining,status,created_at) VALUES(?,?,?,?,?,?, 'open', ?)",
                    (oid, uid, side, price, amount, amount, now_iso()))
        con.commit()
    except Exception as e:
        con.rollback(); con.close()
        return False, f"❌ {e}"
    con.close()

    # Match immediately
    match_orders()
    return True, f"✅ Ордер создан: {side.upper()} {fmt_num(amount)} UWT по {price:g} ₽"

def match_orders():
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")

    # Buy orders: highest price first
    cur.execute("SELECT * FROM orders WHERE status='open' AND side='buy' ORDER BY price DESC, created_at ASC")
    buys = cur.fetchall()
    # Sell orders: lowest price first
    cur.execute("SELECT * FROM orders WHERE status='open' AND side='sell' ORDER BY price ASC, created_at ASC")
    sells = cur.fetchall()

    def refresh_order(oid: str):
        cur.execute("SELECT * FROM orders WHERE id=?", (oid,))
        return cur.fetchone()

    for b in buys:
        b = refresh_order(b["id"])
        if not b or b["status"] != "open" or float(b["remaining"]) <= 1e-12:
            continue
        for s in sells:
            s = refresh_order(s["id"])
            if not s or s["status"] != "open" or float(s["remaining"]) <= 1e-12:
                continue
            buy_price = float(b["price"])
            sell_price = float(s["price"])
            if buy_price + 1e-12 < sell_price:
                break  # no more matches for this buy (since sells sorted ascending)
            # trade price = sell_price (maker = sell), simple rule
            trade_price = sell_price
            qty = min(float(b["remaining"]), float(s["remaining"]))
            if qty <= 1e-12:
                continue

            buy_uid = int(b["user_tg_id"])
            sell_uid = int(s["user_tg_id"])

            # Buyer gets UWT, Seller gets RUB
            rub_amount = qty * trade_price
            cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (qty, buy_uid))
            cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (rub_amount, sell_uid))

            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (buy_uid, "UWT", qty, "trade_buy", f"price={trade_price:g}", now_iso()))
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (sell_uid, "RUB", rub_amount, "trade_sell", f"price={trade_price:g}", now_iso()))

            cur.execute("UPDATE orders SET remaining=remaining-? WHERE id=?", (qty, b["id"]))
            cur.execute("UPDATE orders SET remaining=remaining-? WHERE id=?", (qty, s["id"]))

            tid = str(uuid.uuid4())
            cur.execute("INSERT INTO trades(id,buy_order_id,sell_order_id,price,amount,created_at) VALUES(?,?,?,?,?,?)",
                        (tid, b["id"], s["id"], trade_price, qty, now_iso()))

            # if filled, update status and refund remainder for BUY if trade executed at lower than buy price
            b2 = refresh_order(b["id"])
            s2 = refresh_order(s["id"])
            if b2 and float(b2["remaining"]) <= 1e-12:
                cur.execute("UPDATE orders SET status='filled', remaining=0 WHERE id=?", (b["id"],))
                # Buyer locked RUB at buy_price; actual spent at trade_price. Refund difference for executed qty:
                # Total lock = buy_price*amount; actual spent = sum(trade_price*qty). We don't track sum.
                # Simplified: on each trade refund (buy_price - trade_price)*qty if positive.
                diff = (buy_price - trade_price) * qty
                if diff > 1e-12:
                    cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (diff, buy_uid))
                    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                                (buy_uid, "RUB", diff, "order_price_refund", "", now_iso()))
            if s2 and float(s2["remaining"]) <= 1e-12:
                cur.execute("UPDATE orders SET status='filled', remaining=0 WHERE id=?", (s["id"],))

    con.commit()
    con.close()

def cancel_order(uid: int, oid: str) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM orders WHERE id=?", (oid,))
    o = cur.fetchone()
    if not o:
        con.close()
        return False, "❌ Ордер не найден"
    if int(o["user_tg_id"]) != uid:
        con.close()
        return False, "❌ Это не ваш ордер"
    if o["status"] != "open":
        con.close()
        return False, "❌ Ордер уже не активен"

    side = o["side"]
    price = float(o["price"])
    remaining = float(o["remaining"])

    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE orders SET status='cancelled' WHERE id=? AND status='open'", (oid,))
    if cur.rowcount != 1:
        con.rollback(); con.close()
        return False, "❌ Не удалось отменить"

    _order_refund(cur, uid, side, price, remaining)
    con.commit()
    con.close()
    return True, "✅ Ордер отменён (остаток возвращён)"

def my_orders(uid: int, limit: int = 10):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM orders WHERE user_tg_id=? ORDER BY created_at DESC LIMIT ?", (uid, limit))
    rows = cur.fetchall()
    con.close()
    return rows

def top_book(limit: int = 5):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT price, SUM(remaining) AS qty FROM orders WHERE status='open' AND side='buy' GROUP BY price ORDER BY price DESC LIMIT ?", (limit,))
    buys = cur.fetchall()
    cur.execute("SELECT price, SUM(remaining) AS qty FROM orders WHERE status='open' AND side='sell' GROUP BY price ORDER BY price ASC LIMIT ?", (limit,))
    sells = cur.fetchall()
    con.close()
    return buys, sells

# -------------------- CHANNELS MARKET --------------------
def channels_list(limit: int = 20):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return rows

def channel_get(cid: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels WHERE id=?", (cid,))
    r = cur.fetchone()
    con.close()
    return r

def channel_by_chat(chat_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels WHERE chat_id=?", (chat_id,))
    r = cur.fetchone()
    con.close()
    return r

def channel_upsert(owner_id: int, chat_id: int, title: str | None, username: str | None, price_uwt: float, invite_link: str | None):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO channels(owner_tg_id,chat_id,title,username,price_uwt,invite_link,created_at) VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET owner_tg_id=excluded.owner_tg_id, title=excluded.title, username=excluded.username, price_uwt=excluded.price_uwt, invite_link=excluded.invite_link",
        (owner_id, chat_id, title, username, price_uwt, invite_link, now_iso())
    )
    con.commit(); con.close()

def sub_get(channel_id: int, user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channel_subs WHERE channel_id=? AND user_tg_id=?", (channel_id, user_id))
    r = cur.fetchone()
    con.close()
    return r

def sub_upsert(channel_id: int, user_id: int, expires_at: str):
    con = db()
    cur = con.cursor()
    sid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO channel_subs(id,channel_id,user_tg_id,expires_at,created_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(channel_id,user_tg_id) DO UPDATE SET expires_at=excluded.expires_at",
        (sid, channel_id, user_id, expires_at, now_iso())
    )
    con.commit(); con.close()

def due_subs():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT cs.*, c.chat_id FROM channel_subs cs JOIN channels c ON c.id=cs.channel_id WHERE cs.expires_at<=?", (now_iso(),))
    rows = cur.fetchall()
    con.close()
    return rows

# -------------------- GIVEAWAYS --------------------
def finish_due_giveaways() -> list[tuple[str, int | None, float, int]]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM giveaways WHERE status='active'")
    rows = cur.fetchall()
    finished = []
    for g in rows:
        try:
            end_at = datetime.fromisoformat(g["end_at"])
        except Exception:
            continue
        if end_at > utcnow():
            continue

        gid = g["id"]
        amount = float(g["amount"])
        creator = int(g["creator_tg_id"])

        cur.execute("SELECT user_tg_id FROM giveaway_participants WHERE giveaway_id=?", (gid,))
        ps = [int(r["user_tg_id"]) for r in cur.fetchall()]
        winner = secrets.choice(ps) if ps else None

        if winner is None:
            cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, creator))
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (creator, "UWT", amount, "giveaway_refund", f"gid={gid}", now_iso()))
        else:
            cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, winner))
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (winner, "UWT", amount, "giveaway_win", f"gid={gid}", now_iso()))

        cur.execute("UPDATE giveaways SET status='finished', winner_tg_id=? WHERE id=?", (winner, gid))
        con.commit()
        finished.append((gid, winner, amount, creator))
    con.close()
    return finished

# -------------------- INLINE UI --------------------
def main_menu_kb() -> InlineKeyboardMarkup:
    def b(text, key): 
        return InlineKeyboardButton(text=text, callback_data=f"nav:{key}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [b("👛 Кошелёк", "wallet"), b("🔄 Обмен", "exchange")],
        [b("🤝 P2P", "p2p"), b("🐬 Биржа", "birza")],
        [b("🎁 Чеки", "checks"), b("📩 Счета", "bills")],
        [b("🎁 Розыгрыши", "giveaways"), b("📣 Каналы", "channels")],
        [b("🧾 История", "history"), b("⚙️ Помощь", "help")],
    ])

def home_text(uid: int) -> str:
    uwt, rub = get_balances(uid)
    return (
        "👛 *UWallet*\n\n"
        f"Баланс:\n"
        f"• UWT: *{fmt_num(uwt)}*\n"
        f"• RUB: *{rub:g}*\n\n"
        "Выберите действие 👇"
    )

def back_home_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]])

def exchange_kb(is_admin_user: bool) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="Купить UWT (за RUB)", callback_data="ex:buy")],
        [InlineKeyboardButton(text="Продать UWT (за RUB)", callback_data="ex:sell")],
    ]
    if is_admin_user:
        buttons.append([InlineKeyboardButton(text="⚙️ Установить курс", callback_data="ex:setrate")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def p2p_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отправить UWT", callback_data="p2p:send:UWT")],
        [InlineKeyboardButton(text="Отправить RUB", callback_data="p2p:send:RUB")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
    ])

def birza_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Buy", callback_data="ob:new:buy"),
         InlineKeyboardButton(text="➕ Sell", callback_data="ob:new:sell")],
        [InlineKeyboardButton(text="📊 Стакан", callback_data="ob:book"),
         InlineKeyboardButton(text="🧾 Мои ордера", callback_data="ob:mine")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
    ])

def giveaways_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать", callback_data="gw:new"),
         InlineKeyboardButton(text="📄 Активные", callback_data="gw:active")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
    ])

def channels_menu_kb(is_admin_user: bool):
    rows = [
        [InlineKeyboardButton(text="📣 Список каналов", callback_data="ch:list")],
        [InlineKeyboardButton(text="➕ Добавить мой канал", callback_data="ch:add")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
    ]
    if is_admin_user:
        rows.insert(0, [InlineKeyboardButton(text="⚙️ Обяз. подписки (чеки)", callback_data="rch:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# -------------------- FSM --------------------
class ClaimPassFlow(StatesGroup):
    waiting_pass = State()

class ExchangeAmountFlow(StatesGroup):
    kind = State()
    amount = State()

class AdminSetRateFlow(StatesGroup):
    rate = State()

class P2PSendFlow(StatesGroup):
    asset = State()
    to_user = State()
    amount = State()

class OrderFlow(StatesGroup):
    side = State()
    price = State()
    amount = State()

class ChannelAddFlow(StatesGroup):
    chat = State()
    price = State()

class ReqChAddFlow(StatesGroup):
    chat = State()

# -------------------- BOT --------------------
router = Router()

@router.message(F.text.startswith("/start"))
async def cmd_start(m: Message, state: FSMContext):
    global BOT_USERNAME
    if not m.from_user.username:
        await m.answer(require_username_text())
        return
    ensure_user(m.from_user.id, m.from_user.username)

    parts = m.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""

    # deep link: check claim
    if payload.startswith("c_"):
        token = payload[2:]
        info = check_info(token)
        if not info:
            await m.answer("❌ Чек не найден.")
            return
        # required channels gate
        ok_subs, missing = await user_in_required_channels(m.bot, m.from_user.id)
        if not ok_subs:
            txt = "❗ Чтобы получить чек, подпишитесь на каналы:\n\n" + "\n".join([f"• {x}" for x in missing])
            await m.answer(txt)
            return
        if info["passhash"]:
            await state.set_state(ClaimPassFlow.waiting_pass)
            await state.update_data(token=token, tries=0)
            await m.answer("🔐 Этот чек защищён паролем.\nВведите пароль сообщением:")
            return
        ok, msg, _ = claim_check_by_token(token, m.from_user.id, None)
        await m.answer(msg)
        return

    # deep link: bill pay
    if payload.startswith("b_"):
        token = payload[2:]
        ok, msg = pay_bill_by_token(token, m.from_user.id)
        await m.answer(msg)
        return

    BOT_USERNAME = BOT_USERNAME or (await m.bot.me()).username
    await m.answer(home_text(m.from_user.id), parse_mode="Markdown", reply_markup=main_menu_kb())

@router.message(ClaimPassFlow.waiting_pass)
async def claim_pass(m: Message, state: FSMContext):
    data = await state.get_data()
    token = data.get("token")
    tries = int(data.get("tries", 0))
    pwd = (m.text or "").strip()

    ok_subs, missing = await user_in_required_channels(m.bot, m.from_user.id)
    if not ok_subs:
        await m.answer("❗ Сначала подпишитесь:\n" + "\n".join([f"• {x}" for x in missing]))
        await state.clear()
        return

    ok, msg, _ = claim_check_by_token(token, m.from_user.id, pwd)
    if ok:
        await m.answer(msg)
        await state.clear()
        return

    tries += 1
    await state.update_data(tries=tries)
    if tries >= 3:
        await m.answer("⛔ Слишком много попыток. Попробуйте позже.")
        await state.clear()
        return
    await m.answer(msg)

# -------------------- NAVIGATION --------------------
@router.callback_query(F.data.startswith("nav:"))
async def nav(cb: CallbackQuery, state: FSMContext):
    global BOT_USERNAME
    BOT_USERNAME = BOT_USERNAME or (await cb.bot.me()).username

    key = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    is_admin_user = is_admin(cb.from_user.username)

    if key == "home":
        await safe_edit(cb.message, home_text(uid), parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    if key == "wallet":
        uwt, rub = get_balances(uid)
        text = (
            "👛 *Кошелёк*\n\n"
            f"• UWT: *{fmt_num(uwt)}*\n"
            f"• RUB: *{rub:g}*\n"
        )
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    if key == "exchange":
        rate = get_rate()
        uwt, rub = get_balances(uid)
        text = (
            "🔄 *Обмен*\n\n"
            f"Курс: *1 UWT = {rate:g} ₽*\n\n"
            f"Баланс: {fmt_num(uwt)} UWT | {rub:g} ₽\n"
        )
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=exchange_kb(is_admin_user))
        await cb.answer(); return

    if key == "p2p":
        await safe_edit(cb.message, "🤝 *P2P*\n\nОтправляйте активы другим пользователям.", parse_mode="Markdown", reply_markup=p2p_kb())
        await cb.answer(); return

    if key == "birza":
        await safe_edit(cb.message, "🐬 *Биржа UWT/RUB*\n\nЛимитные ордера и стакан.", parse_mode="Markdown", reply_markup=birza_kb())
        await cb.answer(); return

    if key == "checks":
        un = BOT_USERNAME
        text = (
            "🎁 *Чеки*\n\n"
            "Создание через inline-режим (в любом чате):\n"
            f"• `@{un} 100` → быстрый чек и счёт\n"
            f"• `@{un} check 100 \"описание\" пароль` → чек\n"
            f"• `@{un} mcheck 1000 100 10 \"описание\" пароль` → многоразовый чек\n\n"
            "Получение — по кнопке (URL deep-link)."
        )
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    if key == "bills":
        un = BOT_USERNAME
        text = (
            "📩 *Счета*\n\n"
            "Создание через inline:\n"
            f"• `@{un} bill 250 \"описание\"`\n\n"
            "Оплата — по кнопке (URL deep-link)."
        )
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    if key == "giveaways":
        await safe_edit(cb.message, "🎁 *Розыгрыши*\n\nВсе действия — кнопками.", parse_mode="Markdown", reply_markup=giveaways_menu_kb())
        await cb.answer(); return

    if key == "channels":
        await safe_edit(cb.message, "📣 *Каналы*\n\nПодписки на каналы пользователей на 30 дней (оплата UWT).",
                        parse_mode="Markdown", reply_markup=channels_menu_kb(is_admin_user))
        await cb.answer(); return

    if key == "history":
        rows = last_txs(uid, 15)
        if not rows:
            text = "🧾 История пуста."
        else:
            text = "🧾 *Последние операции:*\n\n"
            for r in rows:
                text += f"{r['created_at']} | {r['asset']} {float(r['delta']):+g} | {r['kind']}\n"
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    if key == "help":
        un = BOT_USERNAME
        text = (
            "⚙️ *Помощь*\n\n"
            "*Inline команды:*\n"
            f"• `@{un} 100`\n"
            f"• `@{un} check 100 \"описание\" пароль`\n"
            f"• `@{un} mcheck 1000 100 10 \"описание\" пароль`\n"
            f"• `@{un} bill 250 \"описание\"`\n\n"
            "*Обмен:* по курсу в разделе 🔄\n"
            "*Биржа:* лимитные ордера в разделе 🐬\n"
            "*Каналы:* добавьте свой канал и выставьте цену."
        )
        await safe_edit(cb.message, text, parse_mode="Markdown", reply_markup=main_menu_kb())
        await cb.answer(); return

    await cb.answer()

# -------------------- EXCHANGE FLOW --------------------
@router.callback_query(F.data == "ex:buy")
async def ex_buy(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ExchangeAmountFlow.amount)
    await state.update_data(kind="buy")
    await cb.message.answer("Введите сумму RUB для покупки UWT (например 500):")
    await cb.answer()

@router.callback_query(F.data == "ex:sell")
async def ex_sell(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ExchangeAmountFlow.amount)
    await state.update_data(kind="sell")
    await cb.message.answer("Введите сумму UWT для продажи (например 25):")
    await cb.answer()

@router.callback_query(F.data == "ex:setrate")
async def ex_setrate(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    await state.set_state(AdminSetRateFlow.rate)
    await cb.message.answer(f"Текущий курс: {get_rate():g} ₽ за 1 UWT.\nВведите новый курс числом:")
    await cb.answer()

@router.message(AdminSetRateFlow.rate)
async def admin_rate(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await m.answer("Нет прав"); await state.clear(); return
    raw = (m.text or "").strip().replace(",", ".")
    try:
        v = float(raw)
        if v <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число > 0"); return
    set_rate(v)
    await m.answer(f"✅ Курс установлен: 1 UWT = {v:g} ₽")
    await state.clear()

@router.message(ExchangeAmountFlow.amount)
async def ex_amount(m: Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("kind")
    raw = (m.text or "").strip().replace(",", ".")
    try:
        val = float(raw)
        if val <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число > 0"); return

    if kind == "buy":
        ok, msg = exchange_buy(m.from_user.id, val)
    else:
        ok, msg = exchange_sell(m.from_user.id, val)
    await m.answer(msg)
    await state.clear()

# -------------------- P2P FLOW --------------------
@router.callback_query(F.data.startswith("p2p:send:"))
async def p2p_send(cb: CallbackQuery, state: FSMContext):
    asset = cb.data.split(":")[2]
    await state.set_state(P2PSendFlow.to_user)
    await state.update_data(asset=asset)
    await cb.message.answer(f"Введите @username получателя для отправки {asset}:")
    await cb.answer()

@router.message(P2PSendFlow.to_user)
async def p2p_to(m: Message, state: FSMContext):
    to_u = clean_username(m.text or "")
    if not to_u:
        await m.answer("❌ Введите @username"); return
    await state.update_data(to_user=to_u)
    await state.set_state(P2PSendFlow.amount)
    await m.answer("Введите сумму:")

@router.message(P2PSendFlow.amount)
async def p2p_amount(m: Message, state: FSMContext):
    data = await state.get_data()
    asset = data.get("asset", "UWT")
    to_u = data.get("to_user", "")
    raw = (m.text or "").strip().replace(",", ".")
    try:
        amt = float(raw)
        if amt <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число > 0"); return
    ok, msg, to_id = p2p_transfer(m.from_user.id, to_u, asset, amt)
    await m.answer(msg)
    if ok and to_id:
        try:
            await m.bot.send_message(to_id, f"📩 Вам пришло {fmt_num(amt)} {asset} от @{clean_username(m.from_user.username)}")
        except Exception:
            pass
    await state.clear()

# -------------------- BIRZA FLOW --------------------
@router.callback_query(F.data.startswith("ob:new:"))
async def ob_new(cb: CallbackQuery, state: FSMContext):
    side = cb.data.split(":")[2]
    await state.set_state(OrderFlow.price)
    await state.update_data(side=side)
    await cb.message.answer(f"Введите цену (₽ за 1 UWT) для {side.upper()}:")
    await cb.answer()

@router.message(OrderFlow.price)
async def ob_price(m: Message, state: FSMContext):
    raw = (m.text or "").strip().replace(",", ".")
    try:
        price = float(raw)
        if price <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите цену > 0"); return
    await state.update_data(price=price)
    await state.set_state(OrderFlow.amount)
    await m.answer("Введите количество UWT:")

@router.message(OrderFlow.amount)
async def ob_amount(m: Message, state: FSMContext):
    data = await state.get_data()
    side = data.get("side")
    price = float(data.get("price"))
    raw = (m.text or "").strip().replace(",", ".")
    try:
        amt = float(raw)
        if amt <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите количество > 0"); return
    ok, msg = place_order(m.from_user.id, side, price, amt)
    await m.answer(msg)
    await state.clear()

@router.callback_query(F.data == "ob:book")
async def ob_book(cb: CallbackQuery):
    buys, sells = top_book()
    txt = "📊 *Стакан UWT/RUB*\n\n*BUY:*\n"
    if buys:
        for r in buys:
            txt += f"• {float(r['price']):g} ₽  |  {fmt_num(float(r['qty'] or 0))} UWT\n"
    else:
        txt += "—\n"
    txt += "\n*SELL:*\n"
    if sells:
        for r in sells:
            txt += f"• {float(r['price']):g} ₽  |  {fmt_num(float(r['qty'] or 0))} UWT\n"
    else:
        txt += "—\n"
    await cb.message.answer(txt, parse_mode="Markdown")
    await cb.answer()

@router.callback_query(F.data == "ob:mine")
async def ob_mine(cb: CallbackQuery):
    rows = my_orders(cb.from_user.id, 10)
    if not rows:
        await cb.message.answer("У вас нет ордеров.")
        await cb.answer(); return
    for o in rows:
        kb = None
        if o["status"] == "open":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data=f"ob:cancel:{o['id']}")]
            ])
        await cb.message.answer(
            f"🧾 Ордер\nID: {o['id']}\n{str(o['side']).upper()} | цена {float(o['price']):g} ₽ | остаток {fmt_num(float(o['remaining']))} UWT | статус {o['status']}",
            reply_markup=kb
        )
    await cb.answer()

@router.callback_query(F.data.startswith("ob:cancel:"))
async def ob_cancel(cb: CallbackQuery):
    oid = cb.data.split(":")[2]
    ok, msg = cancel_order(cb.from_user.id, oid)
    await cb.message.answer(msg)
    await cb.answer()

# -------------------- GIVEAWAYS --------------------
def gw_prize_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="50 UWT", callback_data="gw:p:50"),
         InlineKeyboardButton(text="100 UWT", callback_data="gw:p:100"),
         InlineKeyboardButton(text="500 UWT", callback_data="gw:p:500")],
        [InlineKeyboardButton(text="1000 UWT", callback_data="gw:p:1000"),
         InlineKeyboardButton(text="✍️ Другая сумма", callback_data="gw:p:custom")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:giveaways")],
    ])

def gw_time_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="30 минут", callback_data="gw:t:30"),
         InlineKeyboardButton(text="1 час", callback_data="gw:t:60"),
         InlineKeyboardButton(text="6 часов", callback_data="gw:t:360")],
        [InlineKeyboardButton(text="24 часа", callback_data="gw:t:1440")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:giveaways")],
    ])

class GiveawayCreateFlow(StatesGroup):
    prize_custom = State()

@router.callback_query(F.data == "gw:new")
async def gw_new(cb: CallbackQuery, state: FSMContext):
    await state.update_data(gw_prize=None)
    await cb.message.answer("🎁 Создание розыгрыша\nВыберите приз:", reply_markup=gw_prize_kb())
    await cb.answer()

@router.callback_query(F.data.startswith("gw:p:"))
async def gw_pick_prize(cb: CallbackQuery, state: FSMContext):
    val = cb.data.split(":")[2]
    if val == "custom":
        await state.set_state(GiveawayCreateFlow.prize_custom)
        await cb.message.answer("Введите приз в UWT (число):")
        await cb.answer()
        return
    prize = float(val)
    await state.update_data(gw_prize=prize)
    await cb.message.answer(f"🎁 Приз: {fmt_num(prize)} UWT\nВыберите длительность:", reply_markup=gw_time_kb())
    await cb.answer()

@router.message(GiveawayCreateFlow.prize_custom)
async def gw_custom_prize(m: Message, state: FSMContext):
    raw = (m.text or "").strip().replace(",", ".")
    try:
        prize = float(raw)
        if prize <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число > 0"); return
    await state.update_data(gw_prize=prize)
    await state.clear()
    await m.answer(f"🎁 Приз: {fmt_num(prize)} UWT\nВыберите длительность:", reply_markup=gw_time_kb())

@router.callback_query(F.data.startswith("gw:t:"))
async def gw_pick_time(cb: CallbackQuery, state: FSMContext):
    minutes = int(cb.data.split(":")[2])
    data = await state.get_data()
    prize = float(data.get("gw_prize") or 0)
    if prize <= 0:
        await cb.answer("Сначала выберите приз", show_alert=True); return
    uwt, _ = get_balances(cb.from_user.id)
    if uwt + 1e-12 < prize:
        await cb.answer("Недостаточно UWT", show_alert=True); return

    gid = str(uuid.uuid4())
    end_at = iso(utcnow() + timedelta(minutes=minutes))

    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (prize, cb.from_user.id))
    cur.execute("INSERT INTO giveaways(id, creator_tg_id, amount, status, end_at, created_at) VALUES(?,?,?,?,?,?)",
                (gid, cb.from_user.id, prize, "active", end_at, now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (cb.from_user.id, "UWT", -prize, "giveaway_create", f"gid={gid}", now_iso()))
    con.commit(); con.close()

    join_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Участвовать", callback_data=f"gw:join:{gid}")],
    ])
    await cb.message.answer(
        f"🎁 *Розыгрыш*\n\nПриз: *{fmt_num(prize)} UWT*\nДо: `{end_at}`\nID: `{gid}`",
        parse_mode="Markdown",
        reply_markup=join_kb
    )
    await cb.answer()

@router.callback_query(F.data == "gw:active")
async def gw_active(cb: CallbackQuery):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM giveaways WHERE status='active' ORDER BY created_at DESC LIMIT 10")
    rows = cur.fetchall()
    con.close()
    if not rows:
        await cb.message.answer("Активных розыгрышей нет.")
        await cb.answer(); return
    for g in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Участвовать", callback_data=f"gw:join:{g['id']}")]
        ])
        await cb.message.answer(
            f"🎁 Розыгрыш\nПриз: {fmt_num(float(g['amount']))} UWT\nДо: {g['end_at']}\nID: {g['id']}",
            reply_markup=kb
        )
    await cb.answer()

@router.callback_query(F.data.startswith("gw:join:"))
async def gw_join(cb: CallbackQuery):
    gid = cb.data.split(":", 2)[2]
    con = db()
    cur = con.cursor()
    cur.execute("SELECT status FROM giveaways WHERE id=?", (gid,))
    g = cur.fetchone()
    if not g or g["status"] != "active":
        con.close()
        await cb.answer("Розыгрыш недоступен", show_alert=True)
        return
    try:
        cur.execute("INSERT INTO giveaway_participants(giveaway_id, user_tg_id) VALUES(?,?)", (gid, cb.from_user.id))
        con.commit(); con.close()
        await cb.answer("✅ Участвуете!", show_alert=True)
    except sqlite3.IntegrityError:
        con.close()
        await cb.answer("⚠️ Уже участвуете", show_alert=True)

# -------------------- CHANNELS --------------------
@router.callback_query(F.data == "ch:list")
async def ch_list(cb: CallbackQuery):
    rows = channels_list(20)
    if not rows:
        await cb.message.answer("Каналов пока нет. Добавьте свой через меню.")
        await cb.answer(); return
    for c in rows:
        title = c["title"] or (f"@{c['username']}" if c["username"] else str(c["chat_id"]))
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"✅ Подписаться (30 дней) за {fmt_num(float(c['price_uwt']))} UWT", callback_data=f"ch:sub:{c['id']}")]
        ])
        await cb.message.answer(f"📣 {title}\nЦена: {fmt_num(float(c['price_uwt']))} UWT / 30 дней", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "ch:add")
async def ch_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ChannelAddFlow.chat)
    await cb.message.answer(
        "➕ Добавление канала\n\n"
        "Отправьте @username канала *или* numeric chat_id.\n"
        "Важно: бот должен быть *админом* в канале.",
        parse_mode="Markdown"
    )
    await cb.answer()

@router.message(ChannelAddFlow.chat)
async def ch_add_chat(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    chat_id = None
    chat_username = None
    if raw.startswith("@"):
        chat_username = raw
    else:
        try:
            chat_id = int(raw)
        except Exception:
            await m.answer("❌ Введите @username канала или chat_id"); return

    await state.update_data(chat_id=chat_id, chat_username=chat_username)
    await state.set_state(ChannelAddFlow.price)
    await m.answer("Введите цену подписки за 30 дней в UWT (например 100):")

@router.message(ChannelAddFlow.price)
async def ch_add_price(m: Message, state: FSMContext):
    data = await state.get_data()
    raw = (m.text or "").strip().replace(",", ".")
    try:
        price = float(raw)
        if price <= 0:
            raise ValueError
    except Exception:
        await m.answer("❌ Введите число > 0"); return

    chat_id = data.get("chat_id")
    chat_username = data.get("chat_username")

    # Resolve chat
    try:
        chat = await m.bot.get_chat(chat_username if chat_username else chat_id)
        cid = int(chat.id)
        title = chat.title
        username = chat.username
    except Exception:
        await m.answer("❌ Не смог получить чат. Проверьте @username/chat_id и что бот имеет доступ.")
        await state.clear(); return

    # Check bot admin
    try:
        me = await m.bot.me()
        cm = await m.bot.get_chat_member(cid, me.id)
        if cm.status not in ("administrator", "creator"):
            await m.answer("❌ Бот не админ в канале. Дайте права администратора и повторите.")
            await state.clear(); return
    except Exception:
        await m.answer("❌ Не смог проверить админку. Добавьте бота админом и повторите.")
        await state.clear(); return

    # Create invite link if possible
    invite = None
    try:
        invite_obj = await m.bot.create_chat_invite_link(cid, name="UWallet subscription", creates_join_request=False)
        invite = invite_obj.invite_link
    except Exception:
        # fallback: public username link
        if username:
            invite = f"https://t.me/{username}"

    channel_upsert(m.from_user.id, cid, title, username, price, invite)
    await m.answer(f"✅ Канал добавлен!\n{title or cid}\nЦена: {fmt_num(price)} UWT / 30 дней")
    await state.clear()

@router.callback_query(F.data.startswith("ch:sub:"))
async def ch_sub(cb: CallbackQuery):
    cid = int(cb.data.split(":")[2])
    c = channel_get(cid)
    if not c:
        await cb.answer("Канал не найден", show_alert=True); return
    price = float(c["price_uwt"])
    uwt, _ = get_balances(cb.from_user.id)
    if uwt + 1e-12 < price:
        await cb.answer("Недостаточно UWT", show_alert=True); return

    # pay owner
    owner = int(c["owner_tg_id"])
    con = db()
    cur = con.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (price, cb.from_user.id))
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (price, owner))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (cb.from_user.id, "UWT", -price, "channel_sub_pay", f"channel_id={cid}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (owner, "UWT", price, "channel_sub_recv", f"channel_id={cid}", now_iso()))
    con.commit(); con.close()

    expires = iso(utcnow() + timedelta(days=30))
    sub_upsert(cid, cb.from_user.id, expires)

    invite = c["invite_link"]
    title = c["title"] or (f"@{c['username']}" if c["username"] else str(c["chat_id"]))
    await cb.message.answer(
        f"✅ Подписка оформлена!\nКанал: {title}\nДо: {expires}\n\nСсылка:\n{invite}"
    )
    await cb.answer()

# -------------------- REQUIRED CHANNELS ADMIN UI --------------------
def rch_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📃 Список", callback_data="rch:list"),
         InlineKeyboardButton(text="➕ Добавить", callback_data="rch:add")],
        [InlineKeyboardButton(text="➖ Удалить", callback_data="rch:del")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:channels")],
    ])

@router.callback_query(F.data == "rch:menu")
async def rch_menu(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    await cb.message.answer("⚙️ Обязательные подписки для получения чеков:", reply_markup=rch_menu_kb())
    await cb.answer()

@router.callback_query(F.data == "rch:list")
async def rch_list(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    rows = req_channels_list()
    if not rows:
        await cb.message.answer("Список пуст."); await cb.answer(); return
    txt = "📃 Обязательные каналы:\n\n"
    for r in rows:
        name = r["title"] or (f"@{r['username']}" if r["username"] else str(r["chat_id"]))
        txt += f"• {name} ({r['chat_id']})\n"
    await cb.message.answer(txt)
    await cb.answer()

@router.callback_query(F.data == "rch:add")
async def rch_add(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    await state.set_state(ReqChAddFlow.chat)
    await cb.message.answer("Отправьте @username канала или chat_id (бот должен иметь доступ для проверки).")
    await cb.answer()

@router.message(ReqChAddFlow.chat)
async def rch_add_chat(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await m.answer("Нет прав"); await state.clear(); return
    raw = (m.text or "").strip()
    target = raw
    if not raw.startswith("@"):
        try:
            target = int(raw)
        except Exception:
            await m.answer("❌ Введите @username или chat_id"); return
    try:
        chat = await m.bot.get_chat(target)
        req_channels_add(int(chat.id), chat.title, chat.username)
        await m.answer(f"✅ Добавлено: {chat.title or chat.id}")
    except Exception:
        await m.answer("❌ Не удалось получить чат. Проверьте доступ и данные.")
    await state.clear()

@router.callback_query(F.data == "rch:del")
async def rch_del(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    rows = req_channels_list()
    if not rows:
        await cb.message.answer("Список пуст."); await cb.answer(); return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {r['title'] or r['chat_id']}", callback_data=f"rch:del1:{r['chat_id']}")]
        for r in rows[:25]
    ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="rch:menu")]])
    await cb.message.answer("Выберите канал для удаления:", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("rch:del1:"))
async def rch_del1(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("Нет прав", show_alert=True); return
    chat_id = int(cb.data.split(":")[2])
    req_channels_remove(chat_id)
    await cb.message.answer("✅ Удалено")
    await cb.answer()

# -------------------- INLINE MODE (Checks/Bills with URL buttons) --------------------
def parse_inline_query(q: str):
    q = q.strip()
    if not q:
        return None
    if re.fullmatch(r"\d+([.,]\d+)?", q):
        return {"kind": "simple", "amount": float(q.replace(",", "."))}
    try:
        parts = shlex.split(q)
    except Exception:
        return None
    if not parts:
        return None
    cmd = parts[0].lower()

    if cmd == "bill":
        if len(parts) < 2 or not re.fullmatch(r"\d+([.,]\d+)?", parts[1]): return None
        amount = float(parts[1].replace(",", "."))
        desc = safe_desc(parts[2]) if len(parts) >= 3 else None
        return {"kind": "bill", "amount": amount, "desc": desc}

    # multi-use check: mcheck total per_claim max_claims "desc" pass
    if cmd == "mcheck":
        if len(parts) < 4: return None
        if not re.fullmatch(r"\d+([.,]\d+)?", parts[1]): return None
        if not re.fullmatch(r"\d+([.,]\d+)?", parts[2]): return None
        if not re.fullmatch(r"\d+", parts[3]): return None
        total = float(parts[1].replace(",", "."))
        per = float(parts[2].replace(",", "."))
        maxc = int(parts[3])
        desc = safe_desc(parts[4]) if len(parts) >= 5 else None
        pwd = safe_pass(parts[5]) if len(parts) >= 6 else None
        return {"kind": "mcheck", "total": total, "per": per, "maxc": maxc, "desc": desc, "pwd": pwd}

    # single-use check: check amount "desc" pass
    if cmd == "check":
        if len(parts) < 2 or not re.fullmatch(r"\d+([.,]\d+)?", parts[1]): return None
        amount = float(parts[1].replace(",", "."))
        desc = safe_desc(parts[2]) if len(parts) >= 3 else None
        pwd = safe_pass(parts[3]) if len(parts) >= 4 else None
        return {"kind": "mcheck", "total": amount, "per": amount, "maxc": 1, "desc": desc, "pwd": pwd}

    return None

def make_check_text(total: float, per: float, maxc: int, desc: str | None, has_pass: bool) -> str:
    text = "🎁 *Чек UWT*\n\n"
    if maxc > 1:
        text += f"💰 За раз: *{fmt_num(per)} UWT*\n"
        text += f"👥 Лимит получений: *{maxc}*\n"
        text += f"📦 Общая сумма: *{fmt_num(total)} UWT*\n"
    else:
        text += f"💰 Сумма: *{fmt_num(per)} UWT*\n"
    if desc:
        text += f"\n📝 {desc}\n"
    if has_pass:
        text += "\n🔐 Защищён паролем\n"
    if CHECK_REQUIRE_SUBS and req_channels_list():
        text += "\n📣 Требуются подписки (для получения)\n"
    text += "\nНажмите кнопку ниже 👇"
    return text

def make_bill_text(amount: float, desc: str | None) -> str:
    text = "📩 *Счёт UWT*\n\n"
    text += f"💰 Сумма: *{fmt_num(amount)} UWT*\n"
    if desc:
        text += f"\n📝 {desc}\n"
    text += "\nНажмите кнопку ниже 👇"
    return text

@router.inline_query()
async def inline_handler(i: InlineQuery):
    global BOT_USERNAME
    if not i.from_user.username:
        await i.answer([], cache_time=1)
        return
    ensure_user(i.from_user.id, i.from_user.username)

    parsed = parse_inline_query(i.query)
    if not parsed:
        await i.answer([], cache_time=1)
        return

    bot_user = BOT_USERNAME or (await i.bot.me()).username
    BOT_USERNAME = bot_user

    results = []
    kind = parsed["kind"]

    if kind == "simple":
        amount = float(parsed["amount"])
        # Single check
        ok, token = create_check_multi(i.from_user.id, amount, amount, 1, None, None)
        if ok:
            url = f"https://t.me/{bot_user}?start=c_{token}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Забрать чек", url=url)]])
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"🎁 Чек на {fmt_num(amount)} UWT",
                input_message_content=InputTextMessageContent(
                    message_text=make_check_text(amount, amount, 1, None, False),
                    parse_mode="Markdown"
                ),
                reply_markup=kb
            ))
        # Bill
        ok, tokenb = create_bill_uwt_by_token(i.from_user.id, amount, None)
        if ok:
            url = f"https://t.me/{bot_user}?start=b_{tokenb}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить", url=url)]])
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"📩 Счёт на {fmt_num(amount)} UWT",
                input_message_content=InputTextMessageContent(
                    message_text=make_bill_text(amount, None),
                    parse_mode="Markdown"
                ),
                reply_markup=kb
            ))

    elif kind == "mcheck":
        total = float(parsed["total"])
        per = float(parsed["per"])
        maxc = int(parsed["maxc"])
        desc = parsed.get("desc")
        pwd = parsed.get("pwd")
        ok, token = create_check_multi(i.from_user.id, total, per, maxc, desc, pwd)
        if ok:
            url = f"https://t.me/{bot_user}?start=c_{token}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Забрать чек", url=url)]])
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=("🎁 Многоразовый чек" if maxc > 1 else "🎁 Чек") + f" ({fmt_num(per)} UWT)",
                description=(desc or "UWallet чек")[:60],
                input_message_content=InputTextMessageContent(
                    message_text=make_check_text(total, per, maxc, desc, bool(pwd)),
                    parse_mode="Markdown"
                ),
                reply_markup=kb
            ))
        else:
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="❌ Нельзя создать чек",
                description=token[:80],
                input_message_content=InputTextMessageContent(message_text=f"❌ {token}")
            ))

    elif kind == "bill":
        amount = float(parsed["amount"])
        desc = parsed.get("desc")
        ok, token = create_bill_uwt_by_token(i.from_user.id, amount, desc)
        if ok:
            url = f"https://t.me/{bot_user}?start=b_{token}"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить", url=url)]])
            results.append(InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title=f"📩 Счёт на {fmt_num(amount)} UWT",
                description=(desc or "Оплата")[:60],
                input_message_content=InputTextMessageContent(
                    message_text=make_bill_text(amount, desc),
                    parse_mode="Markdown"
                ),
                reply_markup=kb
            ))

    await i.answer(results, cache_time=0, is_personal=True)

# -------------------- BACKGROUND WORKERS --------------------
async def giveaways_worker(bot: Bot):
    while True:
        finished = finish_due_giveaways()
        for gid, winner, amount, creator in finished:
            msg = f"🎁 Розыгрыш {gid} завершён. "
            if winner is None:
                msg += "Участников не было. Приз возвращён создателю."
            else:
                msg += f"Победитель: {winner}. Приз: {fmt_num(amount)} UWT"
            # notify creator and winner (and participants if possible)
            try:
                await bot.send_message(creator, msg)
            except Exception:
                pass
            if winner:
                try:
                    await bot.send_message(winner, msg)
                except Exception:
                    pass
        await asyncio.sleep(GIVEAWAY_POLL_SEC)

async def subs_worker(bot: Bot):
    """
    Если бот админ в канале и имеет права ban, попробует кикнуть просроченных.
    Если нет — просто оставляет запись (можно чистить вручную).
    """
    while True:
        rows = due_subs()
        for r in rows:
            chat_id = int(r["chat_id"])
            user_id = int(r["user_tg_id"])
            try:
                # kick: ban then unban to remove
                await bot.ban_chat_member(chat_id, user_id)
                await bot.unban_chat_member(chat_id, user_id)
            except Exception:
                pass
            # delete subscription row (stop repeating)
            con = db()
            cur = con.cursor()
            cur.execute("DELETE FROM channel_subs WHERE id=?", (r["id"],))
            con.commit(); con.close()
        await asyncio.sleep(SUBS_POLL_SEC)


# -------------------- ADMIN GIVE COMMANDS --------------------
def get_user_by_username(username: str):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT tg_id FROM users WHERE username=?", (clean_username(username),))
    row = cur.fetchone()
    con.close()
    return int(row["tg_id"]) if row else None

@router.message(F.text.startswith("/give "))
async def cmd_give(m: Message):
    if not is_admin(m.from_user.username):
        await m.answer("❌ Нет прав")
        return
    parts = m.text.split()
    if len(parts) != 3:
        await m.answer("Использование: /give @username сумма")
        return
    uid = get_user_by_username(parts[1])
    if not uid:
        await m.answer("❌ Пользователь не найден")
        return
    try:
        amt = float(parts[2])
        if amt <= 0: raise ValueError
    except:
        await m.answer("❌ Сумма должна быть > 0")
        return
    add_asset(uid, "UWT", amt, "admin_give", f"by @{clean_username(m.from_user.username)}")
    await m.answer(f"✅ Начислено {fmt_num(amt)} UWT пользователю {parts[1]}")
    try:
        await m.bot.send_message(uid, f"💸 Вам начислено {fmt_num(amt)} UWT от администратора")
    except:
        pass

@router.message(F.text.startswith("/giverub "))
async def cmd_giverub(m: Message):
    if not is_admin(m.from_user.username):
        await m.answer("❌ Нет прав")
        return
    parts = m.text.split()
    if len(parts) != 3:
        await m.answer("Использование: /giverub @username сумма")
        return
    uid = get_user_by_username(parts[1])
    if not uid:
        await m.answer("❌ Пользователь не найден")
        return
    try:
        amt = float(parts[2])
        if amt <= 0: raise ValueError
    except:
        await m.answer("❌ Сумма должна быть > 0")
        return
    add_asset(uid, "RUB", amt, "admin_give", f"by @{clean_username(m.from_user.username)}")
    await m.answer(f"✅ Начислено {amt:g} RUB пользователю {parts[1]}")
    try:
        await m.bot.send_message(uid, f"💸 Вам начислено {amt:g} RUB от администратора")
    except:
        pass

# -------------------- RUN --------------------
async def main():
    global BOT_USERNAME
    init_db()
    bot = Bot(BOT_TOKEN)

    me = await bot.me()
    BOT_USERNAME = me.username

    dp = Dispatcher()

    # Global handler for "message is not modified"
    @dp.errors()
    async def ignore_not_modified(update, exception):
        if isinstance(exception, TelegramBadRequest) and "message is not modified" in str(exception):
            return True
        return False

    dp.include_router(router)

    asyncio.create_task(giveaways_worker(bot))
    asyncio.create_task(subs_worker(bot))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
