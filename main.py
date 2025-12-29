# uwallet_full.py
# ============================================================
# UWallet (UWT) — FULL SINGLE-FILE TELEGRAM BOT (FREE BOT)
# ============================================================
# No paid tiers for bot features. Bot is free.
# Paid subscriptions exist ONLY as "user pays UWT to subscribe to other users' channels" (monthly),
# with user-chosen duration (months).
#
# Features:
# - UWT wallet + TX history (UWT + RUB balance supported for exchange/birza)
# - P2P transfers by @username
# - Inline mode like @send:
#     @bot 100               -> offer "Check" or "Bill"
#     @bot check 100 "desc" pass
#     @bot bill  200 "desc"
# - Checks: optional password + optional description
# - UWT Bills: payer pays UWT to bill creator
# - RUB deposits: user creates deposit request, pays admin, admin approves => credits RUB
# - Fixed-rate exchange (RUB <-> UWT) (admin sets rate)
# - Birza (orderbook) UWT/RUB: BUY/SELL limit orders, matching, partial fills, refunds
# - Giveaways: creator deposits UWT prize, users join, auto-finish + winner
# - Channel monetization: channel owner registers channel (price UWT per month),
#     subscriber chooses months, pays UWT, gets one-time invite link; bot auto-removes on expiry
# - Admin panel (admins by username): adjust balances, set rate, approve deposits, manage admins
#
# Requirements:
#   Python 3.10+
#   pip install aiogram==3.* python-dotenv
#
# .env:
#   BOT_TOKEN=xxxx
#   ADMIN_CARD=....   (optional)
#   ADMIN_BANK=....   (optional)
#   ADMIN_NAME=....   (optional)
#   DB_PATH=uwallet.db (optional)
#
# IMPORTANT for channel subscriptions:
# - Add bot as ADMIN in the channel (Invite Users required; Ban/Restrict recommended for auto-remove)
#
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
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InlineQuery, InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

# -------------------- CONFIG --------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in .env")

DB_PATH = os.getenv("DB_PATH", "uwallet.db").strip() or "uwallet.db"

ADMIN_CARD = os.getenv("ADMIN_CARD", "0000 0000 0000 0000")
ADMIN_BANK = os.getenv("ADMIN_BANK", "Bank")
ADMIN_NAME = os.getenv("ADMIN_NAME", "Admin")

# Admins by username (lowercase, without @)
DEFAULT_ADMINS = {"enzekoin", "motidevch"}

DEFAULT_RATE_RUB_PER_UWT = 10.0

MAX_DESC_LEN = 140
MAX_PASS_LEN = 32

# Background polling intervals
GIVEAWAY_POLL_SEC = 30
CHANNEL_POLL_SEC = 60

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
    return u.strip().lstrip("@").lower()

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
        "Откройте Telegram → Настройки → Имя пользователя (Username) и установите его.\n"
        "Потом вернитесь и нажмите /start"
    )

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
        asset TEXT NOT NULL,       -- UWT / RUB
        delta REAL NOT NULL,
        kind TEXT NOT NULL,
        meta TEXT,
        created_at TEXT NOT NULL
    );

    -- Checks: UWT deducted at creation
    CREATE TABLE IF NOT EXISTS checks(
        id TEXT PRIMARY KEY,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        description TEXT,
        passhash TEXT,
        status TEXT NOT NULL,      -- active / claimed / cancelled
        claimed_by_tg_id INTEGER,
        created_at TEXT NOT NULL,
        claimed_at TEXT
    );

    -- Bills in UWT
    CREATE TABLE IF NOT EXISTS bills_uwt(
        id TEXT PRIMARY KEY,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        description TEXT,
        status TEXT NOT NULL,      -- active / paid / cancelled
        paid_by_tg_id INTEGER,
        created_at TEXT NOT NULL,
        paid_at TEXT
    );

    -- RUB deposits (manual admin approve)
    CREATE TABLE IF NOT EXISTS rub_deposits(
        id TEXT PRIMARY KEY,
        user_tg_id INTEGER NOT NULL,
        rub_amount REAL NOT NULL,
        status TEXT NOT NULL,      -- pending / approved / rejected
        created_at TEXT NOT NULL,
        decided_at TEXT,
        admin_username TEXT
    );

    -- Fixed exchange log
    CREATE TABLE IF NOT EXISTS exchange_log(
        id TEXT PRIMARY KEY,
        user_tg_id INTEGER NOT NULL,
        kind TEXT NOT NULL,        -- buy_uwt / sell_uwt
        rub REAL NOT NULL,
        uwt REAL NOT NULL,
        created_at TEXT NOT NULL
    );

    -- Orderbook exchange UWT/RUB
    CREATE TABLE IF NOT EXISTS orders(
        id TEXT PRIMARY KEY,
        user_tg_id INTEGER NOT NULL,
        side TEXT NOT NULL,        -- buy / sell
        price REAL NOT NULL,       -- RUB per UWT
        amount REAL NOT NULL,      -- remaining UWT
        status TEXT NOT NULL,      -- open / filled / cancelled
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

    -- Giveaways
    CREATE TABLE IF NOT EXISTS giveaways(
        id TEXT PRIMARY KEY,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        status TEXT NOT NULL,      -- active / finished / cancelled
        end_at TEXT NOT NULL,
        winner_tg_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS giveaway_participants(
        giveaway_id TEXT NOT NULL,
        user_tg_id INTEGER NOT NULL,
        PRIMARY KEY(giveaway_id, user_tg_id)
    );

    -- Channel monetization (subscriptions)
    CREATE TABLE IF NOT EXISTS channels(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_tg_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL UNIQUE,
        title TEXT,
        username TEXT,
        price_uwt REAL NOT NULL,
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
    if not username:
        return False
    u = clean_username(username)
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
    except:
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

def last_txs(tg_id: int, limit: int = 10):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM tx WHERE tg_id=? ORDER BY id DESC LIMIT ?", (tg_id, limit))
    rows = cur.fetchall()
    con.close()
    return rows

# -------------------- CORE: Checks & Bills --------------------
def create_check(creator_id: int, amount: float, desc: str | None, password: str | None) -> tuple[bool, str]:
    if amount <= 0:
        return (False, "Сумма должна быть > 0")
    uwt, _ = get_balances(creator_id)
    if uwt < amount:
        return (False, "❌ Недостаточно UWT")
    chk_id = str(uuid.uuid4())
    ph = sha256(password) if password else None

    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, creator_id))
    cur.execute(
        "INSERT INTO checks(id, creator_tg_id, amount, description, passhash, status, created_at) "
        "VALUES(?,?,?,?,?,'active',?)",
        (chk_id, creator_id, amount, desc, ph, now_iso())
    )
    cur.execute(
        "INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
        (creator_id, "UWT", -amount, "check_create", f"check_id={chk_id}", now_iso())
    )
    con.commit()
    con.close()
    return (True, chk_id)

def claim_check(chk_id: str, claimer_id: int, password: str | None) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM checks WHERE id=?", (chk_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return (False, "❌ Чек не найден")
    if row["status"] != "active":
        con.close()
        return (False, "❌ Чек уже использован/недоступен")

    if row["passhash"]:
        if not password:
            con.close()
            return (False, "__NEED_PASS__")
        if sha256(password) != row["passhash"]:
            con.close()
            return (False, "❌ Неверный пароль")

    amount = float(row["amount"])
    cur.execute(
        "UPDATE checks SET status='claimed', claimed_by_tg_id=?, claimed_at=? "
        "WHERE id=? AND status='active'",
        (claimer_id, now_iso(), chk_id)
    )
    if cur.rowcount != 1:
        con.close()
        return (False, "❌ Не удалось забрать (кто-то опередил)")
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, claimer_id))
    cur.execute(
        "INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
        (claimer_id, "UWT", amount, "check_claim", f"check_id={chk_id}", now_iso())
    )
    con.commit()
    con.close()
    return (True, f"✅ Вы получили {fmt_num(amount)} UWT")

def create_bill_uwt(creator_id: int, amount: float, desc: str | None) -> tuple[bool, str]:
    if amount <= 0:
        return (False, "Сумма должна быть > 0")
    bill_id = str(uuid.uuid4())
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO bills_uwt(id, creator_tg_id, amount, description, status, created_at) "
        "VALUES(?,?,?,?, 'active', ?)",
        (bill_id, creator_id, amount, desc, now_iso())
    )
    con.commit()
    con.close()
    return (True, bill_id)

def pay_bill_uwt(bill_id: str, payer_id: int) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM bills_uwt WHERE id=?", (bill_id,))
    b = cur.fetchone()
    if not b:
        con.close()
        return (False, "❌ Счёт не найден")
    if b["status"] != "active":
        con.close()
        return (False, "❌ Счёт уже оплачен/недоступен")
    creator = int(b["creator_tg_id"])
    if creator == payer_id:
        con.close()
        return (False, "❌ Нельзя оплатить самому себе")

    amount = float(b["amount"])
    uwt, _ = get_balances(payer_id)
    if uwt < amount:
        con.close()
        return (False, "❌ Недостаточно UWT")

    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, payer_id))
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, creator))
    cur.execute(
        "UPDATE bills_uwt SET status='paid', paid_by_tg_id=?, paid_at=? WHERE id=? AND status='active'",
        (payer_id, now_iso(), bill_id)
    )
    if cur.rowcount != 1:
        con.close()
        return (False, "❌ Не удалось оплатить (возможно, уже оплатили)")

    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (payer_id, "UWT", -amount, "bill_pay", f"bill_id={bill_id}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (creator, "UWT", amount, "bill_receive", f"bill_id={bill_id}", now_iso()))
    con.commit()
    con.close()
    return (True, f"✅ Оплачено {fmt_num(amount)} UWT")

# -------------------- RUB Deposits --------------------
def create_rub_deposit(user_id: int, rub_amount: float) -> tuple[bool, str]:
    if rub_amount <= 0:
        return (False, "Сумма должна быть > 0")
    dep_id = str(uuid.uuid4())
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO rub_deposits(id, user_tg_id, rub_amount, status, created_at) VALUES(?,?,?,?,?)",
        (dep_id, user_id, rub_amount, "pending", now_iso())
    )
    con.commit()
    con.close()
    return (True, dep_id)

def decide_rub_deposit(dep_id: str, admin_username: str, approve: bool) -> tuple[bool, str, int | None, float | None]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM rub_deposits WHERE id=?", (dep_id,))
    d = cur.fetchone()
    if not d:
        con.close()
        return (False, "❌ Заявка не найдена", None, None)
    if d["status"] != "pending":
        con.close()
        return (False, "❌ Заявка уже обработана", None, None)

    status = "approved" if approve else "rejected"
    cur.execute(
        "UPDATE rub_deposits SET status=?, decided_at=?, admin_username=? WHERE id=? AND status='pending'",
        (status, now_iso(), clean_username(admin_username), dep_id)
    )
    if cur.rowcount != 1:
        con.close()
        return (False, "❌ Не удалось обработать", None, None)

    user_id = int(d["user_tg_id"])
    rub_amount = float(d["rub_amount"])
    if approve:
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (rub_amount, user_id))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (user_id, "RUB", rub_amount, "rub_deposit_approved", f"dep_id={dep_id}", now_iso()))
    con.commit()
    con.close()
    return (True, f"✅ {'Подтверждено' if approve else 'Отклонено'}: {rub_amount:g} ₽", user_id, rub_amount)

# -------------------- Fixed-rate Exchange --------------------
def exchange_buy_uwt(user_id: int, rub_to_spend: float) -> tuple[bool, str]:
    if rub_to_spend <= 0:
        return (False, "Сумма должна быть > 0")
    rate = get_rate()
    uwt_amount = rub_to_spend / rate
    uwt, rub = get_balances(user_id)
    if rub < rub_to_spend:
        return (False, "❌ Недостаточно RUB")

    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET rub=rub-?, uwt=uwt+? WHERE tg_id=?", (rub_to_spend, uwt_amount, user_id))
    xid = str(uuid.uuid4())
    cur.execute("INSERT INTO exchange_log(id, user_tg_id, kind, rub, uwt, created_at) VALUES(?,?,?,?,?,?)",
                (xid, user_id, "buy_uwt", rub_to_spend, uwt_amount, now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, "RUB", -rub_to_spend, "exchange_buy", f"xid={xid}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, "UWT", uwt_amount, "exchange_buy", f"xid={xid}", now_iso()))
    con.commit()
    con.close()
    return (True, f"✅ Куплено {fmt_num(uwt_amount)} UWT за {rub_to_spend:g} ₽ (курс {rate:g} ₽/UWT)")

def exchange_sell_uwt(user_id: int, uwt_to_sell: float) -> tuple[bool, str]:
    if uwt_to_sell <= 0:
        return (False, "Сумма должна быть > 0")
    rate = get_rate()
    rub_amount = uwt_to_sell * rate
    uwt, rub = get_balances(user_id)
    if uwt < uwt_to_sell:
        return (False, "❌ Недостаточно UWT")

    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET uwt=uwt-?, rub=rub+? WHERE tg_id=?", (uwt_to_sell, rub_amount, user_id))
    xid = str(uuid.uuid4())
    cur.execute("INSERT INTO exchange_log(id, user_tg_id, kind, rub, uwt, created_at) VALUES(?,?,?,?,?,?)",
                (xid, user_id, "sell_uwt", rub_amount, uwt_to_sell, now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, "UWT", -uwt_to_sell, "exchange_sell", f"xid={xid}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, "RUB", rub_amount, "exchange_sell", f"xid={xid}", now_iso()))
    con.commit()
    con.close()
    return (True, f"✅ Продано {fmt_num(uwt_to_sell)} UWT за {rub_amount:g} ₽ (курс {rate:g} ₽/UWT)")

# -------------------- Orderbook Exchange --------------------
def place_order(user_id: int, side: str, price: float, amount_uwt: float) -> tuple[bool, str]:
    if side not in ("buy", "sell"):
        return (False, "Bad side")
    if price <= 0 or amount_uwt <= 0:
        return (False, "Цена и сумма должны быть > 0")

    con = db()
    cur = con.cursor()
    cur.execute("SELECT uwt, rub FROM users WHERE tg_id=?", (user_id,))
    r = cur.fetchone()
    if not r:
        con.close()
        return (False, "No user")

    uwt = float(r["uwt"])
    rub = float(r["rub"])

    need_rub = price * amount_uwt
    if side == "buy":
        if rub < need_rub:
            con.close()
            return (False, "❌ Недостаточно RUB для BUY ордера")
        cur.execute("UPDATE users SET rub=rub-? WHERE tg_id=?", (need_rub, user_id))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (user_id, "RUB", -need_rub, "order_reserve", f"side=buy;price={price};amt={amount_uwt}", now_iso()))
    else:
        if uwt < amount_uwt:
            con.close()
            return (False, "❌ Недостаточно UWT для SELL ордера")
        cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount_uwt, user_id))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (user_id, "UWT", -amount_uwt, "order_reserve", f"side=sell;price={price};amt={amount_uwt}", now_iso()))

    oid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO orders(id, user_tg_id, side, price, amount, status, created_at) VALUES(?,?,?,?,?,'open',?)",
        (oid, user_id, side, price, amount_uwt, now_iso())
    )
    con.commit()
    con.close()

    match_orders()
    return (True, f"✅ Ордер создан: {side.upper()} {fmt_num(amount_uwt)} UWT по {price:g} ₽")

def cancel_order(user_id: int, order_id: str) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM orders WHERE id=?", (order_id,))
    o = cur.fetchone()
    if not o:
        con.close()
        return (False, "❌ Ордер не найден")
    if o["status"] != "open":
        con.close()
        return (False, "❌ Ордер уже не активен")
    if int(o["user_tg_id"]) != user_id:
        con.close()
        return (False, "❌ Это не ваш ордер")

    side = o["side"]
    price = float(o["price"])
    amt = float(o["amount"])

    if side == "buy":
        refund = price * amt
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (refund, user_id))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (user_id, "RUB", refund, "order_cancel_refund", f"order_id={order_id}", now_iso()))
    else:
        cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amt, user_id))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (user_id, "UWT", amt, "order_cancel_refund", f"order_id={order_id}", now_iso()))

    cur.execute("UPDATE orders SET status='cancelled' WHERE id=? AND status='open'", (order_id,))
    con.commit()
    con.close()
    return (True, "✅ Ордер отменён")

def match_orders():
    """
    Match open BUY and SELL orders:
      - best buy: highest price
      - best sell: lowest price
      - match if buy_price >= sell_price
    Trade price = sell_price.
    Handles partial fills. Refunds buyer difference if buy limit > trade price.
    """
    con = db()
    cur = con.cursor()

    while True:
        cur.execute("SELECT * FROM orders WHERE status='open' AND side='buy' ORDER BY price DESC, created_at ASC LIMIT 1")
        buy = cur.fetchone()
        cur.execute("SELECT * FROM orders WHERE status='open' AND side='sell' ORDER BY price ASC, created_at ASC LIMIT 1")
        sell = cur.fetchone()
        if not buy or not sell:
            break

        buy_price = float(buy["price"])
        sell_price = float(sell["price"])
        if buy_price < sell_price:
            break

        buy_amt = float(buy["amount"])
        sell_amt = float(sell["amount"])
        trade_amt = min(buy_amt, sell_amt)
        trade_price = sell_price

        buyer = int(buy["user_tg_id"])
        seller = int(sell["user_tg_id"])

        # Settlement:
        # buyer gets UWT
        cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (trade_amt, buyer))
        # seller gets RUB
        rub_gain = trade_amt * trade_price
        cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (rub_gain, seller))

        # refund buyer if reserved > actual
        reserved = trade_amt * buy_price
        actual = trade_amt * trade_price
        refund = reserved - actual
        if refund > 1e-12:
            cur.execute("UPDATE users SET rub=rub+? WHERE tg_id=?", (refund, buyer))

        # Update orders remaining
        new_buy = buy_amt - trade_amt
        new_sell = sell_amt - trade_amt

        if new_buy <= 1e-12:
            cur.execute("UPDATE orders SET amount=0, status='filled' WHERE id=?", (buy["id"],))
        else:
            cur.execute("UPDATE orders SET amount=? WHERE id=?", (new_buy, buy["id"]))

        if new_sell <= 1e-12:
            cur.execute("UPDATE orders SET amount=0, status='filled' WHERE id=?", (sell["id"],))
        else:
            cur.execute("UPDATE orders SET amount=? WHERE id=?", (new_sell, sell["id"]))

        tid = str(uuid.uuid4())
        cur.execute("INSERT INTO trades(id, buy_order_id, sell_order_id, price, amount, created_at) VALUES(?,?,?,?,?,?)",
                    (tid, buy["id"], sell["id"], trade_price, trade_amt, now_iso()))

        # tx logs
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (buyer, "UWT", trade_amt, "trade_fill", f"tid={tid};price={trade_price}", now_iso()))
        cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                    (seller, "RUB", rub_gain, "trade_fill", f"tid={tid};price={trade_price}", now_iso()))
        if refund > 1e-12:
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (buyer, "RUB", refund, "trade_refund", f"tid={tid}", now_iso()))

        con.commit()

    con.close()

# -------------------- Giveaways --------------------
def create_giveaway(creator_id: int, amount: float, minutes: int) -> tuple[bool, str]:
    if amount <= 0:
        return (False, "Сумма должна быть > 0")
    if minutes <= 0 or minutes > 60 * 24 * 14:
        return (False, "Длительность: 1..20160 минут")
    uwt, _ = get_balances(creator_id)
    if uwt < amount:
        return (False, "❌ Недостаточно UWT")
    gid = str(uuid.uuid4())
    end_at = iso(utcnow() + timedelta(minutes=minutes))

    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, creator_id))
    cur.execute("INSERT INTO giveaways(id, creator_tg_id, amount, status, end_at, created_at) VALUES(?,?,?,?,?,?)",
                (gid, creator_id, amount, "active", end_at, now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (creator_id, "UWT", -amount, "giveaway_create", f"gid={gid}", now_iso()))
    con.commit()
    con.close()
    return (True, gid)

def join_giveaway(gid: str, user_id: int) -> tuple[bool, str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT status FROM giveaways WHERE id=?", (gid,))
    g = cur.fetchone()
    if not g:
        con.close()
        return (False, "❌ Розыгрыш не найден")
    if g["status"] != "active":
        con.close()
        return (False, "❌ Розыгрыш не активен")
    try:
        cur.execute("INSERT INTO giveaway_participants(giveaway_id, user_tg_id) VALUES(?,?)", (gid, user_id))
    except sqlite3.IntegrityError:
        con.close()
        return (False, "⚠️ Вы уже участвуете")
    con.commit()
    con.close()
    return (True, "✅ Вы участвуете!")

def finish_due_giveaways() -> list[tuple[str, int | None, float]]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM giveaways WHERE status='active'")
    rows = cur.fetchall()
    finished: list[tuple[str, int | None, float]] = []
    for g in rows:
        try:
            end_at = datetime.fromisoformat(g["end_at"])
        except:
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
            # refund to creator
            cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, creator))
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (creator, "UWT", amount, "giveaway_refund", f"gid={gid}", now_iso()))
        else:
            cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, winner))
            cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                        (winner, "UWT", amount, "giveaway_win", f"gid={gid}", now_iso()))

        cur.execute("UPDATE giveaways SET status='finished', winner_tg_id=? WHERE id=?", (winner, gid))
        con.commit()
        finished.append((gid, winner, amount))

    con.close()
    return finished

# -------------------- Channel subscriptions --------------------
async def bot_has_channel_rights(bot: Bot, chat_id: int) -> tuple[bool, str]:
    try:
        me = await bot.me()
        member = await bot.get_chat_member(chat_id, me.id)
        if member.status not in ("administrator", "creator"):
            return (False, "Бот не админ в канале")
        can_invite = getattr(member, "can_invite_users", False)
        can_restrict = getattr(member, "can_restrict_members", False) or getattr(member, "can_ban_users", False)
        if not can_invite:
            return (False, "Дайте боту право создавать invite links (Invite Users)")
        if not can_restrict:
            return (True, "OK (но лучше дать право удалять участников для авто-отписки)")
        return (True, "OK")
    except Exception as e:
        return (False, f"Ошибка проверки прав: {e}")

async def create_invite_link(bot: Bot, chat_id: int, expire_dt: datetime) -> str:
    link = await bot.create_chat_invite_link(
        chat_id=chat_id,
        expire_date=int(expire_dt.timestamp()),
        member_limit=1
    )
    return link.invite_link

def channel_add(owner_id: int, chat_id: int, title: str | None, username: str | None, price_uwt: float) -> tuple[bool, str]:
    if price_uwt <= 0:
        return (False, "Цена должна быть > 0")
    con = db()
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO channels(owner_tg_id, chat_id, title, username, price_uwt, created_at) VALUES(?,?,?,?,?,?)",
            (owner_id, chat_id, title, username, price_uwt, now_iso())
        )
        con.commit()
    except sqlite3.IntegrityError:
        con.close()
        return (False, "Этот канал уже добавлен")
    con.close()
    return (True, "✅ Канал добавлен в маркет")

def channel_list_owner(owner_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels WHERE owner_tg_id=? ORDER BY id DESC", (owner_id,))
    rows = cur.fetchall()
    con.close()
    return rows

def channel_all(limit: int = 50):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    con.close()
    return rows

def channel_get(channel_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM channels WHERE id=?", (channel_id,))
    row = cur.fetchone()
    con.close()
    return row

def channel_sub_extend(channel_id: int, user_id: int, months: int) -> str:
    if months <= 0:
        months = 1
    base = utcnow()
    con = db()
    cur = con.cursor()
    cur.execute("SELECT expires_at FROM channel_subs WHERE channel_id=? AND user_tg_id=?", (channel_id, user_id))
    row = cur.fetchone()
    if row:
        try:
            exp = datetime.fromisoformat(row["expires_at"])
            if exp > base:
                base = exp
        except:
            pass
    new_exp = iso(base + timedelta(days=30 * months))
    sid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO channel_subs(id, channel_id, user_tg_id, expires_at, created_at) "
        "VALUES(?,?,?,?,?) "
        "ON CONFLICT(channel_id, user_tg_id) DO UPDATE SET expires_at=excluded.expires_at",
        (sid, channel_id, user_id, new_exp, now_iso())
    )
    con.commit()
    con.close()
    return new_exp

def channel_subs_due():
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT channel_subs.channel_id, channel_subs.user_tg_id, channel_subs.expires_at,
               channels.chat_id, channels.title, channels.username
        FROM channel_subs
        JOIN channels ON channels.id = channel_subs.channel_id
    """)
    rows = cur.fetchall()
    con.close()
    due = []
    for r in rows:
        try:
            exp = datetime.fromisoformat(r["expires_at"])
        except:
            continue
        if exp <= utcnow():
            due.append(r)
    return due

def channel_sub_remove(channel_id: int, user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM channel_subs WHERE channel_id=? AND user_tg_id=?", (channel_id, user_id))
    con.commit()
    con.close()

def channel_user_subs(user_id: int):
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT channel_subs.expires_at, channels.title, channels.username, channels.chat_id
        FROM channel_subs JOIN channels ON channels.id=channel_subs.channel_id
        WHERE channel_subs.user_tg_id=?
        ORDER BY channel_subs.expires_at DESC
    """, (user_id,))
    rows = cur.fetchall()
    con.close()
    return rows

# -------------------- UI --------------------
menu_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👛 Кошелёк"), KeyboardButton(text="🔄 Обмен")],
        [KeyboardButton(text="🤝 P2P"), KeyboardButton(text="🐬 Биржа")],
        [KeyboardButton(text="🦋 Чеки"), KeyboardButton(text="📩 Счета")],
        [KeyboardButton(text="🎁 Розыгрыши"), KeyboardButton(text="📣 Каналы")],
        [KeyboardButton(text="🧾 История"), KeyboardButton(text="⚙️ Помощь")],
    ],
    resize_keyboard=True
)

def check_message_text(amount: float, desc: str | None, has_pass: bool) -> str:
    t = f"🎁 Чек на {fmt_num(amount)} UWT"
    if desc:
        t += f"\n📝 {desc}"
    if has_pass:
        t += "\n🔐 Защищён паролем"
    t += "\n\nНажмите кнопку ниже 👇"
    return t

def bill_message_text(amount: float, desc: str | None) -> str:
    t = f"📩 Счёт на {fmt_num(amount)} UWT"
    if desc:
        t += f"\n📝 {desc}"
    t += "\n\nНажмите «Оплатить» 👇"
    return t

# -------------------- FSM --------------------
class P2PFlow(StatesGroup):
    to_user = State()
    amount = State()

class ClaimPassFlow(StatesGroup):
    waiting_pass = State()

class DepositRubFlow(StatesGroup):
    amount = State()

class ExchangeFlow(StatesGroup):
    kind = State()
    amount = State()

class OrderFlow(StatesGroup):
    side = State()
    price = State()
    amount = State()

class GiveawayFlow(StatesGroup):
    amount = State()
    minutes = State()

class ChannelAddFlow(StatesGroup):
    chat = State()
    price = State()

class ChannelBuyFlow(StatesGroup):
    months = State()

class AdminBalFlow(StatesGroup):
    who = State()
    asset = State()
    amount = State()

class AdminRateFlow(StatesGroup):
    rate = State()

# -------------------- BOT --------------------
router = Router()

@router.message(F.text == "/start")
async def cmd_start(m: Message):
    if not m.from_user.username:
        await m.answer(require_username_text())
        return
    ensure_user(m.from_user.id, m.from_user.username)
    await m.answer("✅ UWallet запущен. Выберите действие:", reply_markup=menu_kb)

@router.message(F.text == "⚙️ Помощь")
async def help_msg(m: Message):
    me = await m.bot.me()
    await m.answer(
        "ℹ️ Помощь\n\n"
        "Inline:\n"
        f"• @{me.username} 100  → чек/счёт\n"
        f"• @{me.username} check 100 \"описание\" пароль\n"
        f"• @{me.username} bill  200 \"описание\"\n\n"
        "RUB депозит: Счета → Пополнить RUB\n"
        "Каналы: добавьте бота админом в канал (Invite Users, желательно Ban Users)\n"
        "Админка: /admin",
    )

@router.message(F.text == "/admin")
async def admin_entry(m: Message):
    if not is_admin(m.from_user.username):
        await m.answer("⛔ Доступ запрещён")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕/➖ Баланс", callback_data="adm:bal")],
        [InlineKeyboardButton(text="💱 Курс ₽/UWT", callback_data="adm:rate")],
        [InlineKeyboardButton(text="✅ RUB депозиты", callback_data="adm:deps")],
        [InlineKeyboardButton(text="👑 Админы", callback_data="adm:admins")],
    ])
    await m.answer("👑 Админ-панель", reply_markup=kb)

# -------- Wallet & History --------
@router.message(F.text == "👛 Кошелёк")
async def wallet(m: Message):
    if not m.from_user.username:
        await m.answer(require_username_text()); return
    ensure_user(m.from_user.id, m.from_user.username)
    uwt, rub = get_balances(m.from_user.id)
    await m.answer(
        f"👛 Кошелёк\n\n"
        f"UWT: *{fmt_num(uwt)}*\n"
        f"RUB: *{rub:g}*\n",
        parse_mode="Markdown"
    )

@router.message(F.text == "🧾 История")
async def history(m: Message):
    rows = last_txs(m.from_user.id, 12)
    if not rows:
        await m.answer("История пуста.")
        return
    text = "🧾 Последние операции:\n\n"
    for r in rows:
        text += f"{r['created_at']} | {r['asset']} {r['delta']:+g} | {r['kind']}\n"
    await m.answer(text)

# -------- P2P --------
@router.message(F.text == "🤝 P2P")
async def p2p_start(m: Message, state: FSMContext):
    if not m.from_user.username:
        await m.answer(require_username_text()); return
    ensure_user(m.from_user.id, m.from_user.username)
    await state.set_state(P2PFlow.to_user)
    await m.answer("Введите username получателя (например @user):")

@router.message(P2PFlow.to_user)
async def p2p_to(m: Message, state: FSMContext):
    to = m.text.strip()
    if not to.startswith("@") or len(to) < 3:
        await m.answer("❌ Нужно @username")
        return
    await state.update_data(to_username=clean_username(to))
    await state.set_state(P2PFlow.amount)
    await m.answer("Введите сумму UWT:")

@router.message(P2PFlow.amount)
async def p2p_amount(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        amount = float(raw)
        if amount <= 0:
            raise ValueError
    except:
        await m.answer("❌ Сумма должна быть > 0"); return

    data = await state.get_data()
    to_username = data["to_username"]

    con = db()
    cur = con.cursor()
    cur.execute("SELECT tg_id FROM users WHERE username=?", (to_username,))
    r = cur.fetchone()
    if not r:
        con.close()
        await m.answer("❌ Получатель не найден (пусть нажмёт /start)")
        await state.clear()
        return
    to_id = int(r["tg_id"])

    cur.execute("SELECT uwt FROM users WHERE tg_id=?", (m.from_user.id,))
    bal = float(cur.fetchone()["uwt"])
    if bal < amount:
        con.close()
        await m.answer("❌ Недостаточно UWT")
        await state.clear()
        return

    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (amount, m.from_user.id))
    cur.execute("UPDATE users SET uwt=uwt+? WHERE tg_id=?", (amount, to_id))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (m.from_user.id, "UWT", -amount, "p2p_send", f"to=@{to_username}", now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (to_id, "UWT", amount, "p2p_receive", f"from=@{clean_username(m.from_user.username)}", now_iso()))
    con.commit()
    con.close()

    await m.answer(f"✅ Отправлено {fmt_num(amount)} UWT пользователю @{to_username}")
    await state.clear()

# -------- Checks & Bills info --------
@router.message(F.text == "🦋 Чеки")
async def checks_info(m: Message):
    me = await m.bot.me()
    await m.answer(
        "🦋 Чеки (inline)\n\n"
        "Примеры:\n"
        f"• `@{me.username} 100`\n"
        f"• `@{me.username} check 100 \"описание\" пароль`\n",
        parse_mode="Markdown"
    )

@router.message(F.text == "📩 Счета")
async def bills_info(m: Message):
    me = await m.bot.me()
    await m.answer(
        "📩 Счета\n\n"
        "UWT счёт (inline):\n"
        f"• `@{me.username} bill 250 \"описание\"`\n\n"
        "RUB пополнение:\n"
        "• Нажмите кнопку ниже",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить RUB", callback_data="rub:deposit")]
        ])
    )

# -------- RUB deposit flow --------
@router.callback_query(F.data == "rub:deposit")
async def rub_deposit_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(DepositRubFlow.amount)
    await cb.message.answer("Введите сумму пополнения в рублях (например 1500):")
    await cb.answer()

@router.message(DepositRubFlow.amount)
async def rub_deposit_amount(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        rub = float(raw)
        if rub <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите число > 0")
        return

    ok, dep_id = create_rub_deposit(m.from_user.id, rub)
    if not ok:
        await m.answer(dep_id)
        await state.clear()
        return

    await m.answer(
        f"💳 Переведите *{rub:g} ₽* админу:\n\n"
        f"Карта: `{ADMIN_CARD}`\n"
        f"Банк: *{ADMIN_BANK}*\n"
        f"Получатель: *{ADMIN_NAME}*\n\n"
        f"После перевода нажмите «Я оплатил».\n"
        f"ID заявки: `{dep_id}`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"rub:paid:{dep_id}")]
        ])
    )
    await state.clear()

@router.callback_query(F.data.startswith("rub:paid:"))
async def rub_paid(cb: CallbackQuery):
    dep_id = cb.data.split(":", 2)[2]
    await cb.answer("✅ Заявка отправлена админу", show_alert=False)

    # Notify admins who have started the bot (stored in users by username)
    con = db()
    cur = con.cursor()
    cur.execute("SELECT username FROM admins")
    admins = [r["username"] for r in cur.fetchall()]
    cur.execute("SELECT user_tg_id, rub_amount FROM rub_deposits WHERE id=?", (dep_id,))
    d = cur.fetchone()
    con.close()
    if not d:
        return

    user_id = int(d["user_tg_id"])
    rub_amount = float(d["rub_amount"])
    uname = clean_username(cb.from_user.username)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"adm:dep:ok:{dep_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm:dep:no:{dep_id}")],
    ])
    msg = (
        f"💳 RUB депозит\n"
        f"Пользователь: @{uname} (id {user_id})\n"
        f"Сумма: {rub_amount:g} ₽\n"
        f"ID: {dep_id}"
    )

    con = db()
    cur = con.cursor()
    for a in admins:
        cur.execute("SELECT tg_id FROM users WHERE username=?", (a,))
        r = cur.fetchone()
        if r:
            try:
                await cb.bot.send_message(int(r["tg_id"]), msg, reply_markup=kb)
            except:
                pass
    con.close()

# -------- Exchange (fixed) --------
@router.message(F.text == "🔄 Обмен")
async def exchange_menu(m: Message):
    rate = get_rate()
    uwt, rub = get_balances(m.from_user.id)
    await m.answer(
        f"🔄 Обмен (фикс курс)\n\n"
        f"Курс: *1 UWT = {rate:g} ₽*\n"
        f"Ваши балансы: {fmt_num(uwt)} UWT | {rub:g} ₽\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Купить UWT за RUB", callback_data="ex:buy")],
            [InlineKeyboardButton(text="Продать UWT за RUB", callback_data="ex:sell")],
        ])
    )

@router.callback_query(F.data == "ex:buy")
async def ex_buy(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ExchangeFlow.amount)
    await state.update_data(kind="buy")
    await cb.message.answer("Введите сумму RUB для покупки UWT:")
    await cb.answer()

@router.callback_query(F.data == "ex:sell")
async def ex_sell(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ExchangeFlow.amount)
    await state.update_data(kind="sell")
    await cb.message.answer("Введите сумму UWT для продажи:")
    await cb.answer()

@router.message(ExchangeFlow.amount)
async def ex_amount(m: Message, state: FSMContext):
    data = await state.get_data()
    kind = data.get("kind")
    raw = m.text.strip().replace(",", ".")
    try:
        val = float(raw)
        if val <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите число > 0")
        return

    if kind == "buy":
        ok, msg = exchange_buy_uwt(m.from_user.id, val)
    else:
        ok, msg = exchange_sell_uwt(m.from_user.id, val)

    await m.answer(msg)
    await state.clear()

# -------- Birza --------
@router.message(F.text == "🐬 Биржа")
async def birza(m: Message):
    uwt, rub = get_balances(m.from_user.id)

    con = db()
    cur = con.cursor()
    cur.execute("SELECT price, amount FROM orders WHERE status='open' AND side='buy' ORDER BY price DESC LIMIT 5")
    buys = cur.fetchall()
    cur.execute("SELECT price, amount FROM orders WHERE status='open' AND side='sell' ORDER BY price ASC LIMIT 5")
    sells = cur.fetchall()
    con.close()

    book = "📈 Стакан (топ)\n\nBUY:\n"
    book += "\n".join([f"{float(r['price']):g} ₽ | {fmt_num(float(r['amount']))} UWT" for r in buys]) if buys else "—"
    book += "\n\nSELL:\n"
    book += "\n".join([f"{float(r['price']):g} ₽ | {fmt_num(float(r['amount']))} UWT" for r in sells]) if sells else "—"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новый ордер", callback_data="ord:new")],
        [InlineKeyboardButton(text="📄 Мои ордера", callback_data="ord:mine")],
        [InlineKeyboardButton(text="🧾 Последние сделки", callback_data="ord:trades")],
    ])

    await m.answer(
        f"🐬 Биржа UWT/RUB\n\n"
        f"Баланс: {fmt_num(uwt)} UWT | {rub:g} ₽\n\n"
        f"{book}",
        reply_markup=kb
    )

@router.callback_query(F.data == "ord:new")
async def ord_new(cb: CallbackQuery, state: FSMContext):
    await state.set_state(OrderFlow.side)
    await cb.message.answer("Сторона ордера: напишите `buy` или `sell`")
    await cb.answer()

@router.message(OrderFlow.side)
async def ord_side(m: Message, state: FSMContext):
    side = m.text.strip().lower()
    if side not in ("buy", "sell"):
        await m.answer("❌ Напишите buy или sell"); return
    await state.update_data(side=side)
    await state.set_state(OrderFlow.price)
    await m.answer("Введите цену (₽ за 1 UWT), например 12.5:")

@router.message(OrderFlow.price)
async def ord_price(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        price = float(raw)
        if price <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите цену > 0"); return
    await state.update_data(price=price)
    await state.set_state(OrderFlow.amount)
    await m.answer("Введите количество UWT (например 100):")

@router.message(OrderFlow.amount)
async def ord_amount(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        amount = float(raw)
        if amount <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите количество > 0"); return
    data = await state.get_data()
    side = data["side"]
    price = float(data["price"])
    ok, msg = place_order(m.from_user.id, side, price, amount)
    await m.answer(msg)
    await state.clear()

@router.callback_query(F.data == "ord:mine")
async def ord_mine(cb: CallbackQuery):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM orders WHERE user_tg_id=? ORDER BY created_at DESC LIMIT 10", (cb.from_user.id,))
    rows = cur.fetchall()
    con.close()
    if not rows:
        await cb.message.answer("У вас нет ордеров.")
        await cb.answer()
        return
    for o in rows:
        kb = None
        if o["status"] == "open":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data=f"ord:cancel:{o['id']}")]
            ])
        await cb.message.answer(
            f"Ордер {o['id']}\n{o['side'].upper()} {fmt_num(float(o['amount']))} UWT по {float(o['price']):g} ₽\nСтатус: {o['status']}",
            reply_markup=kb
        )
    await cb.answer()

@router.callback_query(F.data.startswith("ord:cancel:"))
async def ord_cancel(cb: CallbackQuery):
    oid = cb.data.split(":", 2)[2]
    ok, msg = cancel_order(cb.from_user.id, oid)
    await cb.answer(msg, show_alert=True)

@router.callback_query(F.data == "ord:trades")
async def ord_trades(cb: CallbackQuery):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM trades ORDER BY created_at DESC LIMIT 12")
    rows = cur.fetchall()
    con.close()
    if not rows:
        await cb.message.answer("Сделок пока нет.")
        await cb.answer()
        return
    text = "🧾 Последние сделки:\n\n"
    for t in rows:
        text += f"{t['created_at']} | {float(t['price']):g} ₽ | {fmt_num(float(t['amount']))} UWT\n"
    await cb.message.answer(text)
    await cb.answer()

# -------- Giveaways --------
@router.message(F.text == "🎁 Розыгрыши")
async def giveaways(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать розыгрыш", callback_data="gw:new")],
        [InlineKeyboardButton(text="📄 Активные", callback_data="gw:active")],
    ])
    await m.answer("🎁 Розыгрыши", reply_markup=kb)

@router.callback_query(F.data == "gw:new")
async def gw_new(cb: CallbackQuery, state: FSMContext):
    await state.set_state(GiveawayFlow.amount)
    await cb.message.answer("Введите приз в UWT:")
    await cb.answer()

@router.message(GiveawayFlow.amount)
async def gw_amount(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        amount = float(raw)
        if amount <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите число > 0"); return
    await state.update_data(amount=amount)
    await state.set_state(GiveawayFlow.minutes)
    await m.answer("Введите длительность в минутах (например 60):")

@router.message(GiveawayFlow.minutes)
async def gw_minutes(m: Message, state: FSMContext):
    try:
        minutes = int(m.text.strip())
        if minutes <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите целое число минут > 0"); return
    data = await state.get_data()
    amount = float(data["amount"])
    ok, gid = create_giveaway(m.from_user.id, amount, minutes)
    if not ok:
        await m.answer(gid)
        await state.clear()
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Участвовать", callback_data=f"gw:join:{gid}")],
    ])
    await m.answer(
        f"🎁 Розыгрыш создан!\nПриз: {fmt_num(amount)} UWT\nЗакончится через {minutes} мин.\nID: {gid}",
        reply_markup=kb
    )
    await state.clear()

@router.callback_query(F.data.startswith("gw:join:"))
async def gw_join(cb: CallbackQuery):
    gid = cb.data.split(":", 2)[2]
    ok, msg = join_giveaway(gid, cb.from_user.id)
    await cb.answer(msg, show_alert=True)

@router.callback_query(F.data == "gw:active")
async def gw_active(cb: CallbackQuery):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM giveaways WHERE status='active' ORDER BY created_at DESC LIMIT 10")
    rows = cur.fetchall()
    con.close()
    if not rows:
        await cb.message.answer("Активных розыгрышей нет.")
        await cb.answer()
        return
    for g in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Участвовать", callback_data=f"gw:join:{g['id']}")]
        ])
        await cb.message.answer(
            f"🎁 Розыгрыш\nПриз: {fmt_num(float(g['amount']))} UWT\nДо: {g['end_at']}\nID: {g['id']}",
            reply_markup=kb
        )
    await cb.answer()

# -------- Channels --------
@router.message(F.text == "📣 Каналы")
async def channels_menu(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить мой канал", callback_data="ch:add")],
        [InlineKeyboardButton(text="📄 Мои каналы", callback_data="ch:mine")],
        [InlineKeyboardButton(text="🛒 Маркет каналов", callback_data="ch:market")],
        [InlineKeyboardButton(text="📌 Мои подписки", callback_data="ch:mysubs")],
    ])
    await m.answer("📣 Каналы (подписки UWT/мес)", reply_markup=kb)

@router.callback_query(F.data == "ch:add")
async def ch_add(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ChannelAddFlow.chat)
    await cb.message.answer(
        "Добавление канала:\n\n"
        "1) Перешлите сюда любое сообщение из вашего канала\n"
        "или отправьте @username канала (если публичный)\n\n"
        "⚠️ Бот должен быть админом канала."
    )
    await cb.answer()

@router.message(ChannelAddFlow.chat)
async def ch_add_chat(m: Message, state: FSMContext):
    bot = m.bot
    chat_id = None
    title = None
    username = None

    if m.forward_from_chat:
        chat_id = m.forward_from_chat.id
        title = m.forward_from_chat.title
        username = m.forward_from_chat.username
    else:
        txt = m.text.strip()
        if txt.startswith("@"):
            try:
                ch = await bot.get_chat(txt)
                chat_id = ch.id
                title = ch.title
                username = ch.username
            except Exception as e:
                await m.answer(f"❌ Не удалось найти канал: {e}")
                return
        else:
            await m.answer("❌ Перешлите сообщение из канала или отправьте @username канала")
            return

    ok, msg = await bot_has_channel_rights(bot, chat_id)
    if not ok:
        await m.answer(f"❌ {msg}\nДобавьте бота админом и попробуйте снова.")
        return

    await state.update_data(chat_id=chat_id, title=title, username=username)
    await state.set_state(ChannelAddFlow.price)
    await m.answer("Введите цену подписки в UWT за 1 месяц (например 300):")

@router.message(ChannelAddFlow.price)
async def ch_add_price(m: Message, state: FSMContext):
    raw = m.text.strip().replace(",", ".")
    try:
        price = float(raw)
        if price <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите число > 0"); return
    data = await state.get_data()
    ok, msg = channel_add(
        owner_id=m.from_user.id,
        chat_id=int(data["chat_id"]),
        title=data.get("title"),
        username=data.get("username"),
        price_uwt=price
    )
    await m.answer(msg)
    await state.clear()

@router.callback_query(F.data == "ch:mine")
async def ch_mine(cb: CallbackQuery):
    rows = channel_list_owner(cb.from_user.id)
    if not rows:
        await cb.message.answer("У вас пока нет добавленных каналов.")
        await cb.answer()
        return
    for r in rows:
        label = r["title"] or (f"@{r['username']}" if r["username"] else str(r["chat_id"]))
        await cb.message.answer(
            f"Канал #{r['id']}: {label}\n"
            f"Цена: {fmt_num(float(r['price_uwt']))} UWT/мес\n"
            f"chat_id: {r['chat_id']}"
        )
    await cb.answer()

@router.callback_query(F.data == "ch:market")
async def ch_market(cb: CallbackQuery):
    rows = channel_all()
    if not rows:
        await cb.message.answer("Пока нет каналов в маркете.")
        await cb.answer()
        return
    for r in rows:
        label = r["title"] or (f"@{r['username']}" if r["username"] else str(r["chat_id"]))
        price = float(r["price_uwt"])
        await cb.message.answer(
            f"🛒 Канал #{r['id']}\n{label}\nЦена: {fmt_num(price)} UWT/мес",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подписаться", callback_data=f"ch:buy:{r['id']}")]
            ])
        )
    await cb.answer()

@router.callback_query(F.data.startswith("ch:buy:"))
async def ch_buy(cb: CallbackQuery, state: FSMContext):
    channel_id = int(cb.data.split(":")[2])
    ch = channel_get(channel_id)
    if not ch:
        await cb.answer("Канал не найден", show_alert=True); return
    await state.set_state(ChannelBuyFlow.months)
    await state.update_data(channel_id=channel_id)
    await cb.message.answer("Введите количество месяцев (любое число 1,2,3...):")
    await cb.answer()

@router.message(ChannelBuyFlow.months)
async def ch_buy_months(m: Message, state: FSMContext):
    data = await state.get_data()
    channel_id = int(data["channel_id"])
    ch = channel_get(channel_id)
    if not ch:
        await m.answer("❌ Канал не найден")
        await state.clear()
        return

    try:
        months = int(m.text.strip())
        if months <= 0:
            raise ValueError
        if months > 120:
            # safety cap 10 years
            months = 120
    except:
        await m.answer("❌ Введите целое число месяцев (1..)")
        return

    price = float(ch["price_uwt"])
    total = price * months
    uwt, _ = get_balances(m.from_user.id)
    if uwt < total:
        await m.answer(f"❌ Недостаточно UWT. Нужно {fmt_num(total)} UWT")
        await state.clear()
        return

    # Pay: subscriber -> owner
    add_asset(m.from_user.id, "UWT", -total, "channel_sub_buy", f"channel_id={channel_id};months={months}")
    add_asset(int(ch["owner_tg_id"]), "UWT", total, "channel_sub_income", f"channel_id={channel_id};months={months}")

    # Extend subscription record
    new_exp = channel_sub_extend(channel_id, m.from_user.id, months)

    # Invite link valid 24h, 1 use
    chat_id = int(ch["chat_id"])
    try:
        invite = await create_invite_link(m.bot, chat_id, utcnow() + timedelta(days=1))
    except Exception as e:
        await m.answer(
            "⚠️ Оплата прошла, но не удалось создать invite ссылку.\n"
            "Проверьте, что бот админ в канале и имеет Invite Users.\n"
            f"Ошибка: {e}"
        )
        await state.clear()
        return

    label = ch["title"] or (f"@{ch['username']}" if ch["username"] else str(chat_id))
    await m.answer(
        f"✅ Подписка оформлена!\n\n"
        f"Канал: {label}\n"
        f"Срок: {months} мес.\n"
        f"До: {new_exp}\n"
        f"Списано: {fmt_num(total)} UWT\n\n"
        f"🔗 Ссылка для входа (одноразовая, действует 24ч):\n{invite}"
    )
    await state.clear()

@router.callback_query(F.data == "ch:mysubs")
async def ch_mysubs(cb: CallbackQuery):
    rows = channel_user_subs(cb.from_user.id)
    if not rows:
        await cb.message.answer("У вас нет подписок на каналы.")
        await cb.answer()
        return
    text = "📌 Ваши подписки:\n\n"
    for r in rows:
        label = r["title"] or (f"@{r['username']}" if r["username"] else str(r["chat_id"]))
        text += f"{label} — до {r['expires_at']}\n"
    await cb.message.answer(text)
    await cb.answer()

# -------------------- INLINE MODE --------------------
def parse_inline_query(q: str):
    q = q.strip()
    if not q:
        return None

    # Just number => offer both
    if re.fullmatch(r"\d+([.,]\d+)?", q):
        return {"kind": "simple", "amount": float(q.replace(",", "."))}

    try:
        parts = shlex.split(q)
    except:
        return None
    if not parts:
        return None

    cmd = parts[0].lower()
    if cmd == "check":
        if len(parts) < 2 or not re.fullmatch(r"\d+([.,]\d+)?", parts[1]):
            return None
        amount = float(parts[1].replace(",", "."))
        desc = safe_desc(parts[2]) if len(parts) >= 3 else None
        pwd = safe_pass(parts[3]) if len(parts) >= 4 else None
        return {"kind": "check", "amount": amount, "desc": desc, "pwd": pwd}

    if cmd == "bill":
        if len(parts) < 2 or not re.fullmatch(r"\d+([.,]\d+)?", parts[1]):
            return None
        amount = float(parts[1].replace(",", "."))
        desc = safe_desc(parts[2]) if len(parts) >= 3 else None
        return {"kind": "bill", "amount": amount, "desc": desc}

    return None

@router.inline_query()
async def inline_handler(i: InlineQuery):
    if not i.from_user.username:
        await i.answer([], cache_time=1); return
    ensure_user(i.from_user.id, i.from_user.username)

    parsed = parse_inline_query(i.query)
    if not parsed:
        await i.answer([], cache_time=1); return

    def mk_article_check(amount: float, desc: str | None, pwd: str | None):
        text = check_message_text(amount, desc, bool(pwd))
        # callback_data length cap => keep short
        cd = (desc or "-")[:40].replace(":", ";")
        cp = (pwd or "-")[:20].replace(":", ";")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Создать чек", callback_data=f"mkc:{amount}:{cd}:{cp}")],
        ])
        return InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=f"🎁 Чек на {fmt_num(amount)} UWT",
            description=(desc or "Передать UWT")[:60],
            input_message_content=InputTextMessageContent(message_text=text),
            reply_markup=kb
        )

    def mk_article_bill(amount: float, desc: str | None):
        text = bill_message_text(amount, desc)
        cd = (desc or "-")[:40].replace(":", ";")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Создать счёт", callback_data=f"mkb:{amount}:{cd}")],
        ])
        return InlineQueryResultArticle(
            id=str(uuid.uuid4()),
            title=f"📩 Счёт на {fmt_num(amount)} UWT",
            description=(desc or "Запросить оплату")[:60],
            input_message_content=InputTextMessageContent(message_text=text),
            reply_markup=kb
        )

    amount = float(parsed["amount"])
    if amount <= 0:
        await i.answer([], cache_time=1); return

    results = []
    if parsed["kind"] == "simple":
        results.append(mk_article_check(amount, None, None))
        results.append(mk_article_bill(amount, None))
    elif parsed["kind"] == "check":
        results.append(mk_article_check(amount, parsed.get("desc"), parsed.get("pwd")))
    elif parsed["kind"] == "bill":
        results.append(mk_article_bill(amount, parsed.get("desc")))

    await i.answer(results, cache_time=0, is_personal=True)

@router.callback_query(F.data.startswith("mkc:"))
async def mk_check_cb(cb: CallbackQuery):
    # mkc:amount:desc:pwd
    try:
        _, amount_s, desc_s, pwd_s = cb.data.split(":", 3)
        amount = float(amount_s)
        desc = safe_desc(desc_s.replace(";", ":") if desc_s != "-" else None)
        pwd = safe_pass(pwd_s.replace(";", ":") if pwd_s != "-" else None)
    except:
        await cb.answer("Ошибка данных", show_alert=True); return

    ok, res = create_check(cb.from_user.id, amount, desc, pwd)
    if not ok:
        await cb.answer(res, show_alert=True); return

    chk_id = res
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Забрать чек", callback_data=f"clm:{chk_id}")],
    ])
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer("✅ Чек создан", show_alert=False)

@router.callback_query(F.data.startswith("clm:"))
async def claim_check_cb(cb: CallbackQuery, state: FSMContext):
    chk_id = cb.data.split(":", 1)[1]
    con = db()
    cur = con.cursor()
    cur.execute("SELECT passhash, description, amount FROM checks WHERE id=?", (chk_id,))
    r = cur.fetchone()
    con.close()
    if not r:
        await cb.answer("Чек не найден", show_alert=True); return

    if r["passhash"]:
        await state.set_state(ClaimPassFlow.waiting_pass)
        await state.update_data(chk_id=chk_id, tries=0)
        await cb.answer("🔐 Нужен пароль — отправьте пароль боту в личку", show_alert=True)
        try:
            await cb.message.reply(
                f"🔐 Этот чек защищён паролем.\n"
                f"Сумма: {fmt_num(float(r['amount']))} UWT\n"
                f"{('📝 ' + r['description']) if r['description'] else ''}\n\n"
                f"Введите пароль сообщением в личке боту."
            )
        except:
            pass
        return

    ok, msg = claim_check(chk_id, cb.from_user.id, None)
    await cb.answer("✅" if ok else "❌", show_alert=not ok)
    if ok:
        try:
            await cb.message.reply(msg)
        except:
            pass

@router.message(ClaimPassFlow.waiting_pass)
async def claim_pass_msg(m: Message, state: FSMContext):
    data = await state.get_data()
    chk_id = data.get("chk_id")
    tries = int(data.get("tries", 0))
    pwd = m.text.strip()

    ok, msg = claim_check(chk_id, m.from_user.id, pwd)
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

@router.callback_query(F.data.startswith("mkb:"))
async def mk_bill_cb(cb: CallbackQuery):
    try:
        _, amount_s, desc_s = cb.data.split(":", 2)
        amount = float(amount_s)
        desc = safe_desc(desc_s.replace(";", ":") if desc_s != "-" else None)
    except:
        await cb.answer("Ошибка данных", show_alert=True); return

    ok, res = create_bill_uwt(cb.from_user.id, amount, desc)
    if not ok:
        await cb.answer(res, show_alert=True); return
    bill_id = res
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Оплатить", callback_data=f"pay:{bill_id}")],
    ])
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer("✅ Счёт создан", show_alert=False)

@router.callback_query(F.data.startswith("pay:"))
async def pay_bill_cb(cb: CallbackQuery):
    bill_id = cb.data.split(":", 1)[1]
    ok, msg = pay_bill_uwt(bill_id, cb.from_user.id)
    await cb.answer("✅" if ok else "❌", show_alert=not ok)
    if ok:
        try:
            await cb.message.reply(msg)
        except:
            pass

# -------------------- ADMIN FLOWS --------------------
@router.callback_query(F.data == "adm:bal")
async def adm_bal_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    await state.set_state(AdminBalFlow.who)
    await cb.message.answer("Введите @username пользователя:")
    await cb.answer()

@router.message(AdminBalFlow.who)
async def adm_bal_who(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await state.clear(); return
    u = m.text.strip()
    if not u.startswith("@"):
        await m.answer("❌ Нужно @username"); return
    await state.update_data(username=clean_username(u))
    await state.set_state(AdminBalFlow.asset)
    await m.answer("Актив: UWT или RUB?")

@router.message(AdminBalFlow.asset)
async def adm_bal_asset(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await state.clear(); return
    asset = m.text.strip().upper()
    if asset not in ("UWT", "RUB"):
        await m.answer("❌ Только UWT или RUB"); return
    await state.update_data(asset=asset)
    await state.set_state(AdminBalFlow.amount)
    await m.answer("Введите сумму (можно отрицательную):")

@router.message(AdminBalFlow.amount)
async def adm_bal_amount(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await state.clear(); return
    raw = m.text.strip().replace(",", ".")
    try:
        amt = float(raw)
    except:
        await m.answer("❌ Введите число"); return
    data = await state.get_data()
    username = data["username"]
    asset = data["asset"]

    con = db()
    cur = con.cursor()
    cur.execute("SELECT tg_id FROM users WHERE username=?", (username,))
    r = cur.fetchone()
    con.close()
    if not r:
        await m.answer("❌ Пользователь не найден (пусть нажмёт /start)")
        await state.clear()
        return
    uid = int(r["tg_id"])
    add_asset(uid, asset, amt, "admin_adjust", f"by=@{clean_username(m.from_user.username)}")
    await m.answer(f"✅ Готово: @{username} {asset} {amt:+g}")
    await state.clear()

@router.callback_query(F.data == "adm:rate")
async def adm_rate_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    await state.set_state(AdminRateFlow.rate)
    await cb.message.answer(f"Текущий курс: {get_rate():g} ₽/UWT\nВведите новый курс:")
    await cb.answer()

@router.message(AdminRateFlow.rate)
async def adm_rate_set(m: Message, state: FSMContext):
    if not is_admin(m.from_user.username):
        await state.clear(); return
    raw = m.text.strip().replace(",", ".")
    try:
        v = float(raw)
        if v <= 0:
            raise ValueError
    except:
        await m.answer("❌ Введите число > 0"); return
    set_rate(v)
    await m.answer(f"✅ Курс обновлён: {v:g} ₽/UWT")
    await state.clear()

@router.callback_query(F.data == "adm:deps")
async def adm_deps(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM rub_deposits WHERE status='pending' ORDER BY created_at DESC LIMIT 10")
    rows = cur.fetchall()
    con.close()
    if not rows:
        await cb.message.answer("Нет заявок.")
        await cb.answer()
        return
    for d in rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"adm:dep:ok:{d['id']}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm:dep:no:{d['id']}")],
        ])
        await cb.message.answer(
            f"Заявка {d['id']}\nuser_id {d['user_tg_id']}\nсумма {float(d['rub_amount']):g} ₽\nсоздано {d['created_at']}",
            reply_markup=kb
        )
    await cb.answer()

@router.callback_query(F.data.startswith("adm:dep:ok:"))
async def adm_dep_ok(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    dep_id = cb.data.split(":")[3]
    ok, msg, user_id, rub_amount = decide_rub_deposit(dep_id, cb.from_user.username, True)
    await cb.answer(msg, show_alert=True)
    if ok and user_id is not None:
        try:
            await cb.bot.send_message(user_id, f"✅ Ваш RUB депозит подтверждён: {rub_amount:g} ₽")
        except:
            pass

@router.callback_query(F.data.startswith("adm:dep:no:"))
async def adm_dep_no(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    dep_id = cb.data.split(":")[3]
    ok, msg, _, _ = decide_rub_deposit(dep_id, cb.from_user.username, False)
    await cb.answer(msg, show_alert=True)

@router.callback_query(F.data == "adm:admins")
async def adm_admins(cb: CallbackQuery):
    if not is_admin(cb.from_user.username):
        await cb.answer("⛔", show_alert=True); return
    con = db()
    cur = con.cursor()
    cur.execute("SELECT username FROM admins ORDER BY username ASC")
    rows = cur.fetchall()
    con.close()
    txt = "👑 Админы:\n\n" + "\n".join([f"@{r['username']}" for r in rows])
    await cb.message.answer(txt + "\n\nКоманды:\n/addadmin @user\n/deladmin @user")
    await cb.answer()

@router.message(F.text.regexp(r"^/addadmin\s+@"))
async def add_admin_cmd(m: Message):
    if not is_admin(m.from_user.username):
        await m.answer("⛔"); return
    u = clean_username(m.text.split(None, 1)[1])
    con = db()
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO admins(username) VALUES(?)", (u,))
    con.commit()
    con.close()
    await m.answer(f"✅ Добавлен админ @{u}")

@router.message(F.text.regexp(r"^/deladmin\s+@"))
async def del_admin_cmd(m: Message):
    if not is_admin(m.from_user.username):
        await m.answer("⛔"); return
    u = clean_username(m.text.split(None, 1)[1])
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM admins WHERE username=?", (u,))
    con.commit()
    con.close()
    await m.answer(f"✅ Удалён админ @{u}")

# -------------------- BACKGROUND WORKERS --------------------
async def giveaways_worker(bot: Bot):
    while True:
        finished = finish_due_giveaways()
        for gid, winner, amount in finished:
            con = db()
            cur = con.cursor()
            cur.execute("SELECT creator_tg_id FROM giveaways WHERE id=?", (gid,))
            g = cur.fetchone()
            cur.execute("SELECT user_tg_id FROM giveaway_participants WHERE giveaway_id=?", (gid,))
            ps = [int(r["user_tg_id"]) for r in cur.fetchall()]
            con.close()
            creator = int(g["creator_tg_id"]) if g else None

            msg = f"🎁 Розыгрыш {gid} завершён. "
            if winner is None:
                msg += "Участников не было. Приз возвращён создателю."
            else:
                msg += f"Победитель: {winner}. Приз: {fmt_num(amount)} UWT"
            for uid in set(ps + ([creator] if creator else [])):
                try:
                    await bot.send_message(uid, msg)
                except:
                    pass
        await asyncio.sleep(GIVEAWAY_POLL_SEC)

async def channel_subs_worker(bot: Bot):
    while True:
        due = channel_subs_due()
        for r in due:
            channel_id = int(r["channel_id"])
            user_id = int(r["user_tg_id"])
            chat_id = int(r["chat_id"])

            removed = False
            try:
                # Kick: ban then unban
                await bot.ban_chat_member(chat_id, user_id)
                await bot.unban_chat_member(chat_id, user_id)
                removed = True
            except:
                removed = False

            channel_sub_remove(channel_id, user_id)

            try:
                label = r["title"] or (f"@{r['username']}" if r["username"] else str(chat_id))
                await bot.send_message(
                    user_id,
                    f"📌 Подписка на канал {label} закончилась. "
                    f"{'Вы удалены из канала.' if removed else 'Бот не смог удалить автоматически (нет прав).'}"
                )
            except:
                pass

        await asyncio.sleep(CHANNEL_POLL_SEC)

# -------------------- RUN --------------------
async def main():
    init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(giveaways_worker(bot))
    asyncio.create_task(channel_subs_worker(bot))

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
