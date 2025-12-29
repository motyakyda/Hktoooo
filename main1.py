# uwallet_full_v5.py
# ============================================================
# UWallet (UWT) — SINGLE-FILE TELEGRAM BOT
# Menu: INLINE buttons (no reply keyboard)
# Checks/Bills: URL buttons (deep-link /start payload) like CryptoBot
# Checks: multi-use (type 1: max claims) + password + description
# Giveaways: inline UI (buttons)
#
# pip install aiogram==3.* python-dotenv
# .env: BOT_TOKEN=...
# ============================================================

import os, re, shlex, uuid, sqlite3, hashlib, asyncio, secrets
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

# -------------------- CONFIG --------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in .env")

DB_PATH = os.getenv("DB_PATH", "uwallet.db").strip() or "uwallet.db"

ADMIN_CARD = os.getenv("ADMIN_CARD", "0000 0000 0000 0000")
ADMIN_BANK = os.getenv("ADMIN_BANK", "Bank")
ADMIN_NAME = os.getenv("ADMIN_NAME", "Admin")

DEFAULT_ADMINS = {"enzekoin", "motidevch"}  # usernames without @
DEFAULT_RATE_RUB_PER_UWT = 10.0

MAX_DESC_LEN = 140
MAX_PASS_LEN = 32

GIVEAWAY_POLL_SEC = 20

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
        "Telegram → Настройки → Имя пользователя (Username)\n"
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
        asset TEXT NOT NULL,
        delta REAL NOT NULL,
        kind TEXT NOT NULL,
        meta TEXT,
        created_at TEXT NOT NULL
    );

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

    CREATE TABLE IF NOT EXISTS giveaways(
        id TEXT PRIMARY KEY,
        creator_tg_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        status TEXT NOT NULL,
        end_at TEXT NOT NULL,
        winner_tg_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS giveaway_participants(
        giveaway_id TEXT NOT NULL,
        user_tg_id INTEGER NOT NULL,
        PRIMARY KEY(giveaway_id, user_tg_id)
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

# -------------------- Giveaways --------------------
def finish_due_giveaways() -> list[tuple[str, int | None, float]]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM giveaways WHERE status='active'")
    rows = cur.fetchall()
    finished = []
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

# -------------------- INLINE MENU UI --------------------
def nav_kb() -> InlineKeyboardMarkup:
    def b(text, key): return InlineKeyboardButton(text=text, callback_data=f"nav:{key}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [b("👛 Кошелёк", "wallet"), b("🧾 История", "history")],
        [b("🎁 Розыгрыши", "giveaways"), b("⚙️ Помощь", "help")],
    ])

def home_text(uid: int) -> str:
    uwt, rub = get_balances(uid)
    return (
        "👛 *UWallet*\n\n"
        f"Баланс:\n"
        f"• UWT: *{fmt_num(uwt)}*\n"
        f"• RUB: *{rub:g}*\n\n"
        "Меню ниже 👇"
    )

# Giveaways inline UI
def gw_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать", callback_data="gw:new"),
         InlineKeyboardButton(text="📄 Активные", callback_data="gw:active")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
    ])

def gw_prize_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="50", callback_data="gw:p:50"),
         InlineKeyboardButton(text="100", callback_data="gw:p:100"),
         InlineKeyboardButton(text="500", callback_data="gw:p:500")],
        [InlineKeyboardButton(text="1000", callback_data="gw:p:1000")],
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

# -------------------- FSM --------------------
class ClaimPassFlow(StatesGroup):
    waiting_pass = State()

class GiveawayState(StatesGroup):
    pick_time = State()

# -------------------- BOT --------------------
router = Router()

@router.message(F.text.startswith("/start"))
async def cmd_start(m: Message, state: FSMContext):
    if not m.from_user.username:
        await m.answer(require_username_text()); return
    ensure_user(m.from_user.id, m.from_user.username)

    parts = m.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""

    if payload.startswith("c_"):
        token = payload[2:]
        info = check_info(token)
        if not info:
            await m.answer("❌ Чек не найден."); return
        if info["passhash"]:
            await state.set_state(ClaimPassFlow.waiting_pass)
            await state.update_data(token=token, tries=0)
            await m.answer("🔐 Этот чек защищён паролем. Введите пароль сообщением:")
            return
        ok, msg, _ = claim_check_by_token(token, m.from_user.id, None)
        await m.answer(msg); return

    if payload.startswith("b_"):
        token = payload[2:]
        ok, msg = pay_bill_by_token(token, m.from_user.id)
        await m.answer(msg); return

    await m.answer(home_text(m.from_user.id), parse_mode="Markdown", reply_markup=nav_kb())

@router.message(ClaimPassFlow.waiting_pass)
async def claim_pass(m: Message, state: FSMContext):
    data = await state.get_data()
    token = data.get("token")
    tries = int(data.get("tries", 0))
    pwd = m.text.strip()

    ok, msg, _ = claim_check_by_token(token, m.from_user.id, pwd)
    if ok:
        await m.answer(msg); await state.clear(); return

    tries += 1
    await state.update_data(tries=tries)
    if tries >= 3:
        await m.answer("⛔ Слишком много попыток. Попробуйте позже.")
        await state.clear(); return
    await m.answer(msg)

@router.callback_query(F.data.startswith("nav:"))
async def nav(cb: CallbackQuery):
    key = cb.data.split(":", 1)[1]
    uid = cb.from_user.id

    if key == "home":
        await cb.message.edit_text(home_text(uid), parse_mode="Markdown", reply_markup=nav_kb())
        await cb.answer(); return

    if key == "wallet":
        uwt, rub = get_balances(uid)
        await cb.message.edit_text(
            f"👛 *Кошелёк*\n\n• UWT: *{fmt_num(uwt)}*\n• RUB: *{rub:g}*",
            parse_mode="Markdown",
            reply_markup=nav_kb()
        )
        await cb.answer(); return

    if key == "history":
        rows = last_txs(uid, 12)
        if not rows:
            txt = "🧾 История пуста."
        else:
            txt = "🧾 *Последние операции:*\n\n"
            for r in rows:
                txt += f"{r['created_at']} | {r['asset']} {float(r['delta']):+g} | {r['kind']}\n"
        await cb.message.edit_text(txt, parse_mode="Markdown", reply_markup=nav_kb())
        await cb.answer(); return

    if key == "help":
        un = BOT_USERNAME or (await cb.bot.me()).username
        await cb.message.edit_text(
            "⚙️ *Помощь*\n\n"
            "*Inline команды:*\n"
            f"• `@{un} 100` → чек/счёт\n"
            f"• `@{un} check 100 \"описание\" пароль` → одноразовый чек\n"
            f"• `@{un} mcheck 1000 100 10 \"описание\" пароль` → многоразовый чек\n"
            f"• `@{un} bill 250 \"описание\"` → счёт\n\n"
            "Чеки/счета публикуются с *URL-кнопками*.\n"
            "Розыгрыши — через inline-меню.",
            parse_mode="Markdown",
            reply_markup=nav_kb()
        )
        await cb.answer(); return

    if key == "giveaways":
        await cb.message.edit_text("🎁 *Розыгрыши*", parse_mode="Markdown", reply_markup=gw_menu_kb())
        await cb.answer(); return

    await cb.answer()

# -------------------- Giveaways callbacks --------------------
@router.callback_query(F.data == "gw:new")
async def gw_new(cb: CallbackQuery, state: FSMContext):
    await state.update_data(gw_prize=None)
    await cb.message.edit_text("🎁 Создание розыгрыша\n\nВыберите приз (UWT):", reply_markup=gw_prize_kb())
    await cb.answer()

@router.callback_query(F.data.startswith("gw:p:"))
async def gw_pick_prize(cb: CallbackQuery, state: FSMContext):
    prize = float(cb.data.split(":")[2])
    await state.update_data(gw_prize=prize)
    await cb.message.edit_text(f"🎁 Приз: {fmt_num(prize)} UWT\n\nВыберите длительность:", reply_markup=gw_time_kb())
    await cb.answer()

@router.callback_query(F.data.startswith("gw:t:"))
async def gw_pick_time(cb: CallbackQuery, state: FSMContext):
    minutes = int(cb.data.split(":")[2])
    data = await state.get_data()
    prize = float(data.get("gw_prize") or 0)
    if prize <= 0:
        await cb.answer("Сначала выберите приз", show_alert=True); return
    uwt, _ = get_balances(cb.from_user.id)
    if uwt < prize:
        await cb.answer("Недостаточно UWT", show_alert=True); return

    gid = str(uuid.uuid4())
    end_at = iso(utcnow() + timedelta(minutes=minutes))

    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET uwt=uwt-? WHERE tg_id=?", (prize, cb.from_user.id))
    cur.execute("INSERT INTO giveaways(id, creator_tg_id, amount, status, end_at, created_at) VALUES(?,?,?,?,?,?)",
                (gid, cb.from_user.id, prize, "active", end_at, now_iso()))
    cur.execute("INSERT INTO tx(tg_id, asset, delta, kind, meta, created_at) VALUES(?,?,?,?,?,?)",
                (cb.from_user.id, "UWT", -prize, "giveaway_create", f"gid={gid}", now_iso()))
    con.commit(); con.close()

    join_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Участвовать", callback_data=f"gw:join:{gid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:giveaways")],
    ])
    await cb.message.edit_text(
        f"🎁 Розыгрыш создан!\n\nПриз: {fmt_num(prize)} UWT\nДо: {end_at}\nID: {gid}",
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

# -------------------- Inline Mode --------------------
def parse_inline_query(q: str):
    q = q.strip()
    if not q:
        return None
    if re.fullmatch(r"\d+([.,]\d+)?", q):
        return {"kind": "simple", "amount": float(q.replace(",", "."))}
    try:
        parts = shlex.split(q)
    except:
        return None
    if not parts:
        return None
    cmd = parts[0].lower()

    if cmd == "bill":
        if len(parts) < 2 or not re.fullmatch(r"\d+([.,]\d+)?", parts[1]): return None
        amount = float(parts[1].replace(",", "."))
        desc = safe_desc(parts[2]) if len(parts) >= 3 else None
        return {"kind": "bill", "amount": amount, "desc": desc}

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
        text += f"👥 Лимит: *{maxc}*\n"
        text += f"📦 Общая сумма: *{fmt_num(total)} UWT*\n"
    else:
        text += f"💰 Сумма: *{fmt_num(per)} UWT*\n"
    if desc:
        text += f"\n📝 {desc}\n"
    if has_pass:
        text += "\n🔐 Защищён паролем\n"
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
        await i.answer([], cache_time=1); return
    ensure_user(i.from_user.id, i.from_user.username)

    parsed = parse_inline_query(i.query)
    if not parsed:
        await i.answer([], cache_time=1); return

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
                input_message_content=InputTextMessageContent(message_text=make_check_text(amount, amount, 1, None, False), parse_mode="Markdown"),
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
                input_message_content=InputTextMessageContent(message_text=make_bill_text(amount, None), parse_mode="Markdown"),
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
                input_message_content=InputTextMessageContent(message_text=make_check_text(total, per, maxc, desc, bool(pwd)), parse_mode="Markdown"),
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
                input_message_content=InputTextMessageContent(message_text=make_bill_text(amount, desc), parse_mode="Markdown"),
                reply_markup=kb
            ))

    await i.answer(results, cache_time=0, is_personal=True)

# -------------------- WORKER --------------------
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
            msg += "Участников не было. Приз возвращён создателю." if winner is None else f"Победитель: {winner}. Приз: {fmt_num(amount)} UWT"

            for uid in set(ps + ([creator] if creator else [])):
                try:
                    await bot.send_message(uid, msg)
                except:
                    pass
        await asyncio.sleep(GIVEAWAY_POLL_SEC)

# -------------------- RUN --------------------
async def main():
    global BOT_USERNAME
    init_db()
    bot = Bot(BOT_TOKEN)
    me = await bot.me()
    BOT_USERNAME = me.username

    dp = Dispatcher()
    dp.include_router(router)

    asyncio.create_task(giveaways_worker(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
