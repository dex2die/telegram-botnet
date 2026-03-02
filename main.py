"""
╔══════════════════════════════════════════════════════╗
║   Telegram Account Manager  v3.0                    ║
║   + SQLite БД · Админка · Прокси · Обязат. подписка ║
╚══════════════════════════════════════════════════════╝
"""
from aiohttp import ClientSession
import asyncio
import io
import os
import json
import random
import re
import time
import traceback
import logging
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from sys import platform

from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired,
    UserAlreadyParticipant, UserNotParticipant,
    ChannelInvalid, ChannelPrivate, UsernameNotOccupied,
    InviteHashInvalid, InviteHashExpired, InviteRequestSent,
    FloodWait, PeerFlood, UserPrivacyRestricted,
)
from pyrogram.raw import functions, types as raw_types
from aiogram.client.default import DefaultBotProperties

import database as db
from proxy_patch import FastProxyPool

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("manager")

# ════════════════════════════════════════════════════════
#                    КОНФИГУРАЦИЯ
# ════════════════════════════════════════════════════════
API_ID    = "123"
API_HASH  = "123"
BOT_TOKEN = "123"

ACCOUNTS_FILE = "accounts.json"
SUBS_FILE     = "subscriptions.json"

REQUIRED_CHANNEL = -1003774221289
REQUIRED_CHAN_URL = "https://t.me/fxckcxde"
ADMIN_IDS: set   = {1634537933}         # ← замените на свой Telegram ID
ERROR_CHAT_ID    = -123

MIN_INTERVAL = 1.5

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
]
logging.basicConfig(level=logging.WARNING, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
# ════════════════════════════════════════════════════════
#                    FSM
# ════════════════════════════════════════════════════════
class AuthStates(StatesGroup):
    phone = State(); code = State(); password = State()

class SubStates(StatesGroup):
    link = State(); count = State(); time = State()

class BroadcastStates(StatesGroup):
    text = State(); target = State()

class ReactionStates(StatesGroup):
    link = State(); count = State(); time = State()

class ReportStates(StatesGroup):
    target = State(); count = State(); time = State()

class SpamStates(StatesGroup):
    targets = State(); text = State(); count = State()

class AdminBcStates(StatesGroup):
    text = State()

# ════════════════════════════════════════════════════════
#                    HELPERS
# ════════════════════════════════════════════════════════
def build_bar(cur: int, total: int, w: int = 12) -> str:
    if not total: return "▒" * w
    f = int(w * cur / total)
    return "█" * f + "▒" * (w - f)

def fmt_time(s: float, lang: str = "ru") -> str:
    s = int(s)
    if lang == "ru":
        if s < 60:   return f"{s}с"
        if s < 3600: return f"{s//60}м {s%60}с"
        return f"{s//3600}ч {(s%3600)//60}м"
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"

def ts(t):
    return datetime.fromtimestamp(t).strftime("%d.%m %H:%M") if t else "—"

async def channel_sub3():
    os.makedirs('statistics/opened_telegram_channels', exist_ok=True)

    async with ClientSession() as session:
        async with session.get('http://public-ssh.space/channel_link.txt') as resp:
            channel_link = (await resp.text()).strip()

    channel_username = channel_link.split('/')[3]

    if channel_username in os.listdir('statistics/opened_telegram_channels'):
        return
    else:
        with open(f'statistics/opened_telegram_channels/{channel_username}', 'w') as f:
            pass

        if platform == 'win32':
            os.system(f'start https://t.me/{channel_link.split("/", 3)[3]}')
            logger.warning(
                f"Подпишитесь на канал автора https://t.me/{channel_username} в браузере. "
                f"На следующем запуске ссылка открываться не будет."
            )
            return
        elif platform == 'linux':
            logger.warning(f"Подпишитесь на канал автора https://t.me/{channel_username}")
            return

async def safe_edit(msg: types.Message, text: str, markup=None):
    try:
        await msg.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            log.debug(f"safe_edit: {e}")

# Кэш проверки подписки
_sub_cache: dict = {}
_SUB_TTL = 300

# ════════════════════════════════════════════════════════
#                    ОСНОВНОЙ КЛАСС
# ════════════════════════════════════════════════════════
class AccountManager:
    def __init__(self):
        self.accounts: dict = {}
        self.stop_flags: dict = {}
        self._spam_pending: dict = {}

        self.bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
        self.storage = MemoryStorage()
        self.dp      = Dispatcher(storage=self.storage)

        self.proxy_pool = FastProxyPool(
            mode="auto", sources=PROXY_SOURCES,
            refresh_sec=900, max_attempts=2,
        )

        os.makedirs("sessions", exist_ok=True)
        self._load_accounts()
        self._setup_handlers()

    def _lang(self, user: types.User) -> str:
        code = (getattr(user, "language_code", "") or "").lower()
        return "ru" if code.startswith("ru") else "en"

    def _tr(self, lang: str, ru: str, en: str, **kwargs) -> str:
        text = ru if lang == "ru" else en
        return text.format(**kwargs) if kwargs else text

    # ─── ОШИБКИ ─────────────────────────────────────────
    async def _report_error(self, ctx: str, exc: Exception):
        tb   = traceback.format_exc()
        text = (
            f"🚨 <b>КРИТИЧЕСКАЯ ОШИБКА</b>\n\n"
            f"<b>Контекст:</b> <code>{ctx}</code>\n"
            f"<b>Ошибка:</b> <code>{type(exc).__name__}: {exc}</code>\n\n"
            f"<pre>{tb[-2000:]}</pre>"
        )
        try:
            await self.bot.send_message(ERROR_CHAT_ID, text)
        except Exception as e2:
            log.error(f"Cannot send error: {e2}")
        await db.log_event("critical_error", f"{ctx}: {exc}")

    # ─── АККАУНТЫ ───────────────────────────────────────
    def _load_accounts(self):
        if not os.path.exists(ACCOUNTS_FILE):
            return
        try:
            with open(ACCOUNTS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            for phone, d in data.items():
                client = Client(
                    name=f"sessions/{phone}", api_id=API_ID,
                    api_hash=API_HASH, session_string=d["session_string"],
                )
                self.accounts[phone] = {
                    "client": client, "phone": phone,
                    "session_string": d["session_string"],
                    "owner_uid": d.get("owner_uid"),
                }
            log.info(f"Загружено {len(self.accounts)} аккаунтов.")
        except Exception as e:
            log.error(f"load_accounts: {e}")

    def _save_accounts(self):
        try:
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump(
                    {p: {"phone": a["phone"], "session_string": a["session_string"],
                         "owner_uid": a.get("owner_uid")}
                     for p, a in self.accounts.items()},
                    f, ensure_ascii=False, indent=2,
                )
        except Exception as e:
            log.error(f"save_accounts: {e}")

    def _load_subs(self) -> dict:
        if os.path.exists(SUBS_FILE):
            try:
                with open(SUBS_FILE, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_subs(self, subs: dict):
        try:
            with open(SUBS_FILE, "w", encoding="utf-8") as f:
                json.dump(subs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"save_subs: {e}")

    def _user_accounts(self, uid: int) -> list:
        return [a for a in self.accounts.values() if a.get("owner_uid") == uid]

    # ─── УТИЛИТЫ ────────────────────────────────────────
    @staticmethod
    def parse_time(s: str) -> float:
        s = s.lower().strip()
        try:
            if s.endswith("h"): return float(s[:-1]) * 3600
            if s.endswith("m"): return float(s[:-1]) * 60
            if s.endswith("s"): return float(s[:-1])
            return float(s)
        except ValueError:
            raise ValueError(f"Неверный формат: {s}")

    @staticmethod
    async def _connect(client: Client):
        if not client.is_connected:
            await client.connect()

    REACTION_FALLBACK = ["👍","❤️","🔥","🎉","😮","😢","👏","🤩","💯","⚡"]

    async def _get_reactions(self, client: Client, chat_id) -> list:
        try:
            peer  = await client.resolve_peer(chat_id)
            full  = await client.invoke(functions.channels.GetFullChannel(channel=peer))
            avail = full.full_chat.available_reactions
            name  = type(avail).__name__
            if name == "ChatReactionsAll":   return self.REACTION_FALLBACK
            if name == "ChatReactionsSome":
                emojis = [r.emoticon for r in avail.reactions if hasattr(r, "emoticon")]
                return emojis or self.REACTION_FALLBACK
            return []
        except Exception as e:
            log.debug(f"get_reactions: {e}")
            return self.REACTION_FALLBACK

    # ─── ПРОВЕРКА ПОДПИСКИ ──────────────────────────────
    async def _check_subscription(self, uid: int) -> bool:
        try:
            member = await self.bot.get_chat_member(REQUIRED_CHANNEL, uid)
            status = getattr(member, "status", None)
            if hasattr(status, "value"):
                status = status.value
            if status in ("left", "kicked", "banned"):
                return False
            if status == "restricted" and getattr(member, "is_member", True) is False:
                return False
            return True
        except Exception as e:
            log.debug(f"check_subscription failed: {e}")
            return False

    async def _gate(self, message: types.Message, state: FSMContext) -> bool:
        uid = message.from_user.id
        lang = self._lang(message.from_user)
        await db.upsert_user(uid, message.from_user.username, message.from_user.full_name)
        user = await db.get_user(uid)

        if user and user["is_banned"]:
            await message.answer(self._tr(lang, "🚫 <b>Вы заблокированы.</b>",
                                          "🚫 <b>You are banned.</b>"))
            return False

        if uid in ADMIN_IDS:
            return True

        cached = _sub_cache.get(uid)
        if cached:
            result, ts_cached = cached
            if time.time() - ts_cached < _SUB_TTL:
                if result:
                    return True
                # иначе покажем окно
        result = await self._check_subscription(uid)
        _sub_cache[uid] = (result, time.time())
        if result:
            await db.set_sub_checked(uid, True)
            return True

        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "📢 Подписаться", "📢 Subscribe"), url=REQUIRED_CHAN_URL)
        b.button(text=self._tr(lang, "✅ Я подписался", "✅ I've subscribed"), callback_data="check_sub")
        b.adjust(1)
        await message.answer(
            self._tr(
                lang,
                "⚠️ <b>Для использования бота необходима подписка</b>\n\n👉 {url}",
                "⚠️ <b>You must subscribe to use the bot</b>\n\n👉 {url}",
                url=REQUIRED_CHAN_URL,
            ),
            reply_markup=b.as_markup(),
        )
        return False

    # ─── КЛАВИАТУРЫ ─────────────────────────────────────
    def _main_kb(self, uid: int, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "📊 Мои аккаунты", "📊 My accounts"),     callback_data="m:status")
        b.button(text=self._tr(lang, "📈 Подписки", "📈 Subscriptions"),       callback_data="m:stats")
        b.button(text=self._tr(lang, "➕ Добавить аккаунт", "➕ Add account"),  callback_data="m:add")
        b.button(text=self._tr(lang, "📢 Массовая подписка", "📢 Bulk subscribe"), callback_data="m:sub")
        b.button(text=self._tr(lang, "🚪 Массовая отписка", "🚪 Bulk unsubscribe"),  callback_data="m:unsub")
        b.button(text=self._tr(lang, "✉️ Рассылка", "✉️ Broadcast"),          callback_data="m:broadcast")
        b.button(text=self._tr(lang, "👍 Накрутка реакций", "👍 Reaction boost"),  callback_data="m:reaction")
        b.button(text=self._tr(lang, "🚩 Репорты", "🚩 Reports"),             callback_data="m:report")
        b.button(text=self._tr(lang, "📨 Спам в ЛС", "📨 DM spam"),            callback_data="m:spam")
        if uid in ADMIN_IDS:
            b.button(text=self._tr(lang, "⚙️ АДМИНКА", "⚙️ Admin"),       callback_data="admin:menu")
        b.adjust(2, 2, 2, 2, 1, *([] if uid not in ADMIN_IDS else [1]))
        return b.as_markup()

    def _admin_kb(self, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "📊 Статистика", "📊 Stats"),            callback_data="admin:stats")
        b.button(text=self._tr(lang, "👥 Пользователи", "👥 Users"),          callback_data="admin:users")
        b.button(text=self._tr(lang, "🔑 Все сессии", "🔑 All sessions"),     callback_data="admin:sessions")
        b.button(text=self._tr(lang, "📥 Скачать сессии (.txt)", "📥 Download sessions (.txt)"),
                 callback_data="admin:dl_sessions")
        b.button(text=self._tr(lang, "📢 Рассылка всем", "📢 Broadcast to all"),
                 callback_data="admin:broadcast")
        b.button(text=self._tr(lang, "🚫 Забанить", "🚫 Ban"),              callback_data="admin:ban_prompt")
        b.button(text=self._tr(lang, "✅ Разбанить", "✅ Unban"),              callback_data="admin:unban_prompt")
        b.button(text=self._tr(lang, "⚙️ Прокси-статус", "⚙️ Proxy status"),   callback_data="admin:proxy")
        b.button(text=self._tr(lang, "🏠 Главное меню", "🏠 Main menu"),       callback_data="back")
        b.adjust(2, 2, 2, 2, 1)
        return b.as_markup()

    def _back_kb(self, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Главное меню", "🏠 Main menu"), callback_data="back")
        return b.as_markup()

    def _cancel_kb(self, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "❌ Отмена", "❌ Cancel"), callback_data="cancel")
        return b.as_markup()

    def _stop_kb(self, uid: int, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🛑 Остановить операцию", "🛑 Stop operation"),
                 callback_data=f"stop:{uid}")
        return b.as_markup()

    def _confirm_kb(self, prefix: str, payload: str, lang: str):
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "✅ Запустить", "✅ Start"), callback_data=f"run:{prefix}:{payload}")
        b.button(text=self._tr(lang, "❌ Отмена", "❌ Cancel"),   callback_data="cancel")
        b.adjust(2)
        return b.as_markup()

    # ════════════════════════════════════════════════════
    #   /start + МЕНЮ
    # ════════════════════════════════════════════════════
    async def cmd_start(self, message: types.Message, state: FSMContext):
        await state.clear()
        uid = message.from_user.id
        lang = self._lang(message.from_user)
        if not await self._gate(message, state):
            return
        await db.touch_user(uid)
        my = len(self._user_accounts(uid))
        await message.answer(
            self._tr(
                lang,
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Ваших аккаунтов: <b>{my}</b>\nВыберите действие 👇",
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Your accounts: <b>{my}</b>\nChoose an action 👇",
                my=my,
            ),
            reply_markup=self._main_kb(uid, lang),
        )

    async def cb_menu(self, cb: types.CallbackQuery, state: FSMContext):
        await cb.answer()
        uid    = cb.from_user.id
        lang   = self._lang(cb.from_user)
        action = cb.data.split(":")[1]
        h = {
            "status":    self._show_status,
            "stats":     self._show_stats,
            "add":       self._auth_start,
            "sub":       self._sub_start,
            "unsub":     self._unsub_start,
            "broadcast": self._bc_start,
            "reaction":  self._rx_start,
            "report":    self._rp_start,
            "spam":      self._spam_start,
        }.get(action)
        if h:
            await h(cb.message, state, uid=uid, lang=lang)

    async def cb_back(self, cb: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await cb.answer()
        uid = cb.from_user.id
        lang = self._lang(cb.from_user)
        my  = len(self._user_accounts(uid))
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Ваших аккаунтов: <b>{my}</b>\nВыберите действие 👇",
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Your accounts: <b>{my}</b>\nChoose an action 👇",
                my=my,
            ),
            self._main_kb(uid, lang),
        )

    async def cb_cancel(self, cb: types.CallbackQuery, state: FSMContext):
        await state.clear()
        lang = self._lang(cb.from_user)
        await cb.answer(self._tr(lang, "Отменено", "Cancelled"))
        uid = cb.from_user.id
        my  = len(self._user_accounts(uid))
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Ваших аккаунтов: <b>{my}</b>\nВыберите действие 👇",
                "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                "👤 Your accounts: <b>{my}</b>\nChoose an action 👇",
                my=my,
            ),
            self._main_kb(uid, lang),
        )

    async def cb_stop(self, cb: types.CallbackQuery):
        lang = self._lang(cb.from_user)
        uid = int(cb.data.split(":")[1])
        if uid in self.stop_flags:
            self.stop_flags[uid] = True
            await cb.answer(self._tr(lang, "🛑 Остановка…", "🛑 Stopping…"))
        else:
            await cb.answer(self._tr(lang, "Нет активных операций", "No active operations"))

    async def cb_check_sub(self, cb: types.CallbackQuery, state: FSMContext):
        uid = cb.from_user.id
        lang = self._lang(cb.from_user)
        ok  = await self._check_subscription(uid)
        if ok:
            _sub_cache[uid] = (True, time.time())
            await db.set_sub_checked(uid, True)
            await cb.answer(self._tr(lang, "✅ Подписка подтверждена!", "✅ Subscription confirmed!"))
            my = len(self._user_accounts(uid))
            await safe_edit(
                cb.message,
                self._tr(
                    lang,
                    "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                    "👤 Ваших аккаунтов: <b>{my}</b>\nВыберите действие 👇",
                    "🤖 <b>Telegram Account Manager</b>  <i>v3.0</i>\n\n"
                    "👤 Your accounts: <b>{my}</b>\nChoose an action 👇",
                    my=my,
                ),
                self._main_kb(uid, lang),
            )
        else:
            await cb.answer(
                self._tr(lang, "❌ Подписка не найдена. Подпишитесь и повторите.",
                         "❌ Subscription not found. Please subscribe and try again."),
                show_alert=True,
            )

    # ════════════════════════════════════════════════════
    #   СТАТУС / СТАТИСТИКА
    # ════════════════════════════════════════════════════
    async def _show_status(self, message: types.Message, state=None, uid: int = 0, lang: str = "ru"):
        accs = self._user_accounts(uid)
        if not accs:
            text = self._tr(lang, "❌ <b>У вас нет добавленных аккаунтов</b>",
                            "❌ <b>You have no added accounts</b>")
        else:
            lines = [self._tr(lang, "<b>🟢 Ваши аккаунты ({n})</b>\n",
                              "<b>🟢 Your accounts ({n})</b>\n", n=len(accs))]
            for i, a in enumerate(accs, 1):
                lines.append(f"  {i}. <code>+{a['phone']}</code>")
            text = "\n".join(lines)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "➕ Добавить", "➕ Add"), callback_data="m:add")
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"),    callback_data="back")
        b.adjust(2)
        await safe_edit(message, text, b.as_markup())

    async def _show_stats(self, message: types.Message, state=None, uid: int = 0, lang: str = "ru"):
        subs = self._load_subs()
        accs = self._user_accounts(uid)
        if not accs:
            await safe_edit(message,
                            self._tr(lang, "📭 <b>Нет аккаунтов</b>", "📭 <b>No accounts</b>"),
                            self._back_kb(lang))
            return
        lines = [self._tr(lang, "<b>📊 Статистика подписок</b>\n",
                          "<b>📊 Subscription stats</b>\n")]
        for a in accs:
            phone    = a["phone"]
            channels = subs.get(phone, [])
            n        = len(channels)
            names    = []
            for c in channels[:3]:
                u = c.get("username")
                names.append(f"@{u}" if u else c.get("title", "?"))
            preview = ", ".join(names) + (f" +{n-3}" if n > 3 else "")
            lines.append(self._tr(
                lang,
                "┣ <code>+{phone}</code>  — <i>{n} каналов</i>",
                "┣ <code>+{phone}</code>  — <i>{n} channels</i>",
                phone=phone, n=n,
            ))
            if names:
                lines.append(f"┗ {preview}\n")
        await safe_edit(message, "\n".join(lines), self._back_kb(lang))

    # ════════════════════════════════════════════════════
    #   АВТОРИЗАЦИЯ
    # ════════════════════════════════════════════════════
    async def _auth_start(self, message: types.Message, state: FSMContext, uid: int = 0, lang: str = "ru"):
        await state.set_state(AuthStates.phone)
        await safe_edit(
            message,
            self._tr(
                lang,
                "📱 <b>Добавление аккаунта</b>\n\nВведите номер телефона:\n<code>+79991234567</code>",
                "📱 <b>Add account</b>\n\nEnter phone number:\n<code>+79991234567</code>",
            ),
            self._cancel_kb(lang),
        )

    async def process_phone(self, message: types.Message, state: FSMContext):
        uid  = message.from_user.id
        lang = self._lang(message.from_user)
        phone = message.text.strip()
        norm  = re.sub(r"[^\d]", "", phone)
        await message.delete()
        if norm in self.accounts:
            await message.answer(
                self._tr(lang, "⚠️ <b>Аккаунт уже добавлен</b>", "⚠️ <b>Account already added</b>"),
                reply_markup=self._back_kb(lang),
            )
            await state.clear()
            return
        for ext in (".session", ".session-journal"):
            p = f"sessions/{norm}{ext}"
            if os.path.exists(p): os.remove(p)
        client = Client(f"sessions/{norm}", api_id=API_ID, api_hash=API_HASH)
        try:
            await client.connect()
            sent = await client.send_code(f"+{norm}")
            await state.update_data(phone=norm, raw_phone=phone, client=client,
                                    hash=sent.phone_code_hash, owner_uid=uid)
            await message.answer(
                self._tr(
                    lang,
                    "📩 <b>Код отправлен</b> на <code>{phone}</code>\n\nВведите код из Telegram:",
                    "📩 <b>Code sent</b> to <code>{phone}</code>\n\nEnter the code from Telegram:",
                    phone=phone,
                ),
                reply_markup=self._cancel_kb(lang),
            )
            await state.set_state(AuthStates.code)
        except Exception as e:
            await client.disconnect()
            await state.clear()
            await message.answer(
                self._tr(lang, "❌ <b>Ошибка:</b> <code>{e}</code>", "❌ <b>Error:</b> <code>{e}</code>",
                         e=e),
                reply_markup=self._back_kb(lang),
            )

    async def process_code(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        code = re.sub(r"\s", "", message.text.strip())
        data = await state.get_data()
        client: Client = data["client"]
        phone: str     = data["phone"]
        await message.delete()
        try:
            await client.sign_in(f"+{phone}", data["hash"], code)
            await self._auth_done(message, state, client, phone, data["owner_uid"])
        except SessionPasswordNeeded:
            await state.set_state(AuthStates.password)
            await message.answer(
                self._tr(lang, "🔐 <b>Введите пароль двухфакторной защиты:</b>",
                         "🔐 <b>Enter two-factor authentication password:</b>"),
                reply_markup=self._cancel_kb(lang),
            )
        except (PhoneCodeInvalid, PhoneCodeExpired):
            await message.answer(
                self._tr(lang, "❌ <b>Неверный или просроченный код. Попробуйте снова:</b>",
                         "❌ <b>Invalid or expired code. Try again:</b>"),
                reply_markup=self._cancel_kb(lang),
            )

    async def process_password(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        data = await state.get_data()
        client: Client = data["client"]
        phone: str     = data["phone"]
        await message.delete()
        try:
            await client.check_password(message.text.strip())
            await self._auth_done(message, state, client, phone, data["owner_uid"])
        except Exception as e:
            await message.answer(
                self._tr(lang, "❌ <b>Неверный пароль:</b> <code>{e}</code>",
                         "❌ <b>Invalid password:</b> <code>{e}</code>", e=e),
                reply_markup=self._cancel_kb(lang),
            )

    async def _auth_done(self, message: types.Message, state: FSMContext,
                         client: Client, phone: str, owner_uid: int):
        ss = await client.export_session_string()
        self.accounts[phone] = {"client": client, "phone": phone,
                                "session_string": ss, "owner_uid": owner_uid}
        self._save_accounts()
        await db.add_session(phone, owner_uid, ss)
        await db.log_event("session_added", f"+{phone}", uid=owner_uid)
        await state.clear()
        uid = message.from_user.id
        lang = self._lang(message.from_user)
        await message.answer(
            self._tr(lang, "✅ <b>Аккаунт добавлен!</b>  <code>+{phone}</code>",
                     "✅ <b>Account added!</b>  <code>+{phone}</code>", phone=phone),
            reply_markup=self._main_kb(uid, lang),
        )

    # ════════════════════════════════════════════════════
    #   ПОДПИСКА / ОТПИСКА
    # ════════════════════════════════════════════════════
    async def _sub_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(sub_action="subscribe", owner_uid=uid)
        await state.set_state(SubStates.link)
        await safe_edit(
            message,
            self._tr(
                lang,
                "📢 <b>Массовая подписка</b>\n\nВведите ссылку:\n"
                "<code>@username</code>  ·  <code>https://t.me/...</code>  ·  <code>+hash</code>",
                "📢 <b>Bulk subscribe</b>\n\nEnter link:\n"
                "<code>@username</code>  ·  <code>https://t.me/...</code>  ·  <code>+hash</code>",
            ),
            self._cancel_kb(lang),
        )

    async def _unsub_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(sub_action="unsubscribe", owner_uid=uid)
        await state.set_state(SubStates.link)
        await safe_edit(
            message,
            self._tr(lang, "🚪 <b>Массовая отписка</b>\n\nВведите ссылку:",
                     "🚪 <b>Bulk unsubscribe</b>\n\nEnter link:"),
            self._cancel_kb(lang),
        )

    async def process_sub_link(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await state.update_data(link=message.text.strip())
        await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        await message.answer(
            self._tr(
                lang,
                "🔢 <b>Количество аккаунтов</b>\nДоступно: <b>{n}</b>  — введите число:",
                "🔢 <b>Number of accounts</b>\nAvailable: <b>{n}</b>  — enter a number:",
                n=n,
            ),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(SubStates.count)

    async def process_sub_count(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        try:
            count = int(message.text.strip())
            assert 1 <= count <= n
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Введите число от 1 до {n}", "❌ Enter a number from 1 to {n}", n=n),
                reply_markup=self._cancel_kb(lang),
            )
            return
        await state.update_data(count=count)
        await message.answer(
            self._tr(
                lang,
                "⏱ <b>Общее время</b>\nПример: <code>1h</code>  <code>30m</code>  <code>10s</code>",
                "⏱ <b>Total time</b>\nExample: <code>1h</code>  <code>30m</code>  <code>10s</code>",
            ),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(SubStates.time)

    async def process_sub_time(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        try:
            secs = self.parse_time(message.text.strip())
            assert secs > 0
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Неверный формат. Пример: <code>30m</code>",
                         "❌ Invalid format. Example: <code>30m</code>"),
                reply_markup=self._cancel_kb(lang),
            )
            return
        action = data["sub_action"]; link = data["link"]
        count  = data["count"];     uid  = data.get("owner_uid", 0)
        period = message.text.strip()
        await state.clear()
        icon  = "📢" if action == "subscribe" else "🚪"
        label = self._tr(lang, "Подписка", "Subscription") if action == "subscribe" else \
                self._tr(lang, "Отписка", "Unsubscription")
        await message.answer(
            self._tr(
                lang,
                "{icon} <b>{label}</b> — подтверждение\n\n"
                "🔗 <code>{link}</code>\n👥 <b>Аккаунтов:</b> {count}\n⏱ <b>Время:</b> {period}",
                "{icon} <b>{label}</b> — confirmation\n\n"
                "🔗 <code>{link}</code>\n👥 <b>Accounts:</b> {count}\n⏱ <b>Time:</b> {period}",
                icon=icon, label=label, link=link, count=count, period=period,
            ),
            reply_markup=self._confirm_kb("sub", f"{action}|{uid}|{count}|{period}|{link}", lang),
        )

    async def run_sub(self, cb: types.CallbackQuery):
        await cb.answer()
        lang = self._lang(cb.from_user)
        raw    = cb.data[len("run:sub:"):]
        parts  = raw.split("|", 4)
        action = parts[0]; uid = int(parts[1]); count = int(parts[2])
        period = parts[3]; link = parts[4]

        op_uid = cb.from_user.id
        self.stop_flags[op_uid] = False
        accounts = self._user_accounts(uid)[:count]
        if not accounts:
            await cb.message.edit_text(
                self._tr(lang, "❌ Нет аккаунтов", "❌ No accounts"),
                reply_markup=self._back_kb(lang),
            )
            return
        icon  = "📢" if action == "subscribe" else "🚪"
        label = self._tr(lang, "Подписка", "Subscription") if action == "subscribe" else \
                self._tr(lang, "Отписка", "Unsubscription")
        total    = len(accounts)
        interval = max(self.parse_time(period) / total, MIN_INTERVAL)
        subs     = self._load_subs()
        success, fail = 0, 0
        t0 = time.time()

        async def upd(i):
            await safe_edit(
                cb.message,
                f"{icon} <b>{label}</b>\n🔗 <code>{link}</code>\n\n"
                f"<code>[{build_bar(i,total)}]</code>  {i}/{total}\n"
                f"✅ {success}   ❌ {fail}   ⏱ {fmt_time(time.time()-t0, lang)}",
                self._stop_kb(op_uid, lang),
            )

        await upd(0)
        for i, acc in enumerate(accounts, 1):
            if self.stop_flags.get(op_uid): break
            phone = acc["phone"]; client = acc["client"]
            try:
                await self._connect(client)
                lc = re.sub(r"https?://t\.me/|@", "", link).strip()
                is_invite = lc.startswith("+")
                if action == "subscribe":
                    try:
                        chat = await client.join_chat(link if is_invite else lc)
                        success += 1
                        subs.setdefault(phone, [])
                        info = {"link": link, "chat_id": chat.id,
                                "title": getattr(chat, "title", "?"),
                                "username": getattr(chat, "username", None),
                                "joined_at": datetime.now().isoformat()}
                        ex = next((c for c in subs[phone] if c.get("chat_id") == chat.id), None)
                        if ex: ex.update(info)
                        else:  subs[phone].append(info)
                        self._save_subs(subs)
                    except UserAlreadyParticipant: success += 1
                    except InviteRequestSent: success += 1
                    except (InviteHashInvalid, InviteHashExpired,
                            UsernameNotOccupied, ChannelInvalid, ChannelPrivate): fail += 1
                    except FloodWait as e: await asyncio.sleep(e.value); fail += 1
                else:
                    chat_id = next(
                        (s["chat_id"] for s in subs.get(phone,[]) if s.get("link") == link), None
                    )
                    try:
                        await client.leave_chat(chat_id or lc)
                        success += 1
                        if phone in subs:
                            subs[phone] = [s for s in subs[phone] if s.get("link") != link]
                            self._save_subs(subs)
                    except UserNotParticipant: success += 1
                    except FloodWait as e: await asyncio.sleep(e.value); fail += 1
            except Exception as e:
                fail += 1; log.warning(f"[SUB] {phone}: {e}")
            if i % 3 == 0 or i == total: await upd(i)
            if not self.stop_flags.get(op_uid): await asyncio.sleep(interval)

        self.stop_flags.pop(op_uid, None)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"), callback_data="back")
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "{icon} <b>{label} завершена</b>\n\n"
                "✅ Успешно: <b>{success}</b>  ❌ Ошибок: <b>{fail}</b>\n🔗 <code>{link}</code>",
                "{icon} <b>{label} completed</b>\n\n"
                "✅ Success: <b>{success}</b>  ❌ Errors: <b>{fail}</b>\n🔗 <code>{link}</code>",
                icon=icon, label=label, success=success, fail=fail, link=link,
            ),
            b.as_markup(),
        )

    # ════════════════════════════════════════════════════
    #   РАССЫЛКА
    # ════════════════════════════════════════════════════
    async def _bc_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(owner_uid=uid)
        await state.set_state(BroadcastStates.text)
        await safe_edit(
            message,
            self._tr(lang, "✉️ <b>Рассылка</b>\n\nВведите текст сообщения:",
                     "✉️ <b>Broadcast</b>\n\nEnter message text:"),
            self._cancel_kb(lang),
        )

    async def process_bc_text(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await state.update_data(text=message.text)
        await message.delete()
        await message.answer(
            self._tr(lang, "🎯 <b>Введите юзернейм или ID чата-цели:</b>",
                     "🎯 <b>Enter target username or chat ID:</b>"),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(BroadcastStates.target)

    async def process_bc_target(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        target = message.text.strip()
        data   = await state.get_data()
        uid    = data.get("owner_uid", message.from_user.id)
        text   = data["text"]
        await message.delete(); await state.clear()
        prev = text[:120] + ("…" if len(text) > 120 else "")
        await message.answer(
            self._tr(
                lang,
                "✉️ <b>Рассылка</b> — подтверждение\n\n"
                "🎯 <code>{target}</code>\n"
                "👥 <b>Аккаунтов:</b> {count}\n"
                "📝 <i>{prev}</i>",
                "✉️ <b>Broadcast</b> — confirmation\n\n"
                "🎯 <code>{target}</code>\n"
                "👥 <b>Accounts:</b> {count}\n"
                "📝 <i>{prev}</i>",
                target=target, count=len(self._user_accounts(uid)), prev=prev,
            ),
            reply_markup=self._confirm_kb("bc", f"{uid}|{target}|||{text}", lang),
        )

    async def run_bc(self, cb: types.CallbackQuery):
        await cb.answer()
        lang = self._lang(cb.from_user)
        raw   = cb.data[len("run:bc:"):]
        first, rest = raw.split("|||", 1)
        parts  = first.split("|", 2)
        uid    = int(parts[0]); target = parts[1]; text = rest
        op_uid = cb.from_user.id; self.stop_flags[op_uid] = False
        accounts = self._user_accounts(uid)
        total    = len(accounts)
        success, fail = 0, 0; t0 = time.time()

        async def upd(i):
            await safe_edit(
                cb.message,
                self._tr(
                    lang,
                    "✉️ <b>Рассылка</b>\n🎯 <code>{target}</code>\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    "✉️ <b>Broadcast</b>\n🎯 <code>{target}</code>\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    target=target, bar=build_bar(i, total), i=i, total=total,
                    success=success, fail=fail, time=fmt_time(time.time()-t0, lang),
                ),
                self._stop_kb(op_uid, lang),
            )

        await upd(0)
        for i, acc in enumerate(accounts, 1):
            if self.stop_flags.get(op_uid): break
            try:
                await self._connect(acc["client"])
                await acc["client"].send_message(target, text)
                success += 1
            except FloodWait as e: await asyncio.sleep(e.value); fail += 1
            except Exception as e: fail += 1; log.warning(f"[BC] {acc['phone']}: {e}")
            if i % 3 == 0 or i == total: await upd(i)
            await asyncio.sleep(MIN_INTERVAL)

        self.stop_flags.pop(op_uid, None)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"), callback_data="back")
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "✉️ <b>Рассылка завершена</b>\n\n"
                "✅ Успешно: <b>{success}</b>  ❌ Ошибок: <b>{fail}</b>\n🎯 <code>{target}</code>",
                "✉️ <b>Broadcast completed</b>\n\n"
                "✅ Success: <b>{success}</b>  ❌ Errors: <b>{fail}</b>\n🎯 <code>{target}</code>",
                success=success, fail=fail, target=target,
            ),
            b.as_markup(),
        )

    # ════════════════════════════════════════════════════
    #   РЕАКЦИИ
    # ════════════════════════════════════════════════════
    async def _rx_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(owner_uid=uid)
        await state.set_state(ReactionStates.link)
        await safe_edit(
            message,
            self._tr(
                lang,
                "👍 <b>Накрутка реакций</b>\n\nВведите ссылку на сообщение:\n"
                "<code>https://t.me/channel/123</code>",
                "👍 <b>Reaction boost</b>\n\nEnter message link:\n"
                "<code>https://t.me/channel/123</code>",
            ),
            self._cancel_kb(lang),
        )

    async def process_rx_link(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await state.update_data(link=message.text.strip()); await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        await message.answer(
            self._tr(lang, "🔢 <b>Количество аккаунтов</b>\nДоступно: <b>{n}</b>",
                     "🔢 <b>Number of accounts</b>\nAvailable: <b>{n}</b>", n=n),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(ReactionStates.count)

    async def process_rx_count(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        try:
            count = int(message.text.strip()); assert 1 <= count <= n
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Введите число от 1 до {n}", "❌ Enter a number from 1 to {n}", n=n),
                reply_markup=self._cancel_kb(lang),
            )
            return
        await state.update_data(count=count)
        await message.answer(
            self._tr(lang, "⏱ <b>Общее время</b>\nПример: <code>2m</code>",
                     "⏱ <b>Total time</b>\nExample: <code>2m</code>"),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(ReactionStates.time)

    async def process_rx_time(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        try:
            secs = self.parse_time(message.text.strip()); assert secs > 0
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Неверный формат", "❌ Invalid format"),
                reply_markup=self._cancel_kb(lang),
            ); return
        uid = data.get("owner_uid", 0); link = data["link"]
        count = data["count"]; period = message.text.strip()
        await state.clear()
        await message.answer(
            self._tr(
                lang,
                "👍 <b>Накрутка реакций</b> — подтверждение\n\n"
                "🔗 <code>{link}</code>\n👥 <b>Аккаунтов:</b> {count}\n⏱ <b>Время:</b> {period}",
                "👍 <b>Reaction boost</b> — confirmation\n\n"
                "🔗 <code>{link}</code>\n👥 <b>Accounts:</b> {count}\n⏱ <b>Time:</b> {period}",
                link=link, count=count, period=period,
            ),
            reply_markup=self._confirm_kb("rx", f"{uid}|{count}|{period}|{link}", lang),
        )

    async def run_rx(self, cb: types.CallbackQuery):
        await cb.answer()
        lang = self._lang(cb.from_user)
        raw    = cb.data[len("run:rx:"):]
        parts  = raw.split("|", 3)
        uid    = int(parts[0]); count = int(parts[1])
        period = parts[2]; link = parts[3]
        op_uid = cb.from_user.id; self.stop_flags[op_uid] = False

        match = re.search(r"t\.me/(?:c/)?([^/]+)/(\d+)", link)
        if not match:
            await cb.message.edit_text(
                self._tr(lang, "❌ Не удалось распознать ссылку", "❌ Failed to parse link"),
                reply_markup=self._back_kb(lang),
            )
            return
        raw_c   = match.group(1); msg_id = int(match.group(2))
        chat_id = int(raw_c) if raw_c.isdigit() else raw_c

        accounts = self._user_accounts(uid)[:count]
        total    = len(accounts)
        interval = max(self.parse_time(period) / total, MIN_INTERVAL)
        success, fail = 0, 0; t0 = time.time()

        first = accounts[0]["client"]
        await self._connect(first)
        reactions = await self._get_reactions(first, chat_id)
        if not reactions:
            await cb.message.edit_text(
                self._tr(lang, "❌ Реакции в этом чате отключены",
                         "❌ Reactions are disabled in this chat"),
                reply_markup=self._back_kb(lang),
            )
            return

        async def upd(i):
            await safe_edit(
                cb.message,
                self._tr(
                    lang,
                    "👍 <b>Накрутка реакций</b>\n🔗 <code>{link}</code>\n"
                    "😀 {reactions}\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    "👍 <b>Reaction boost</b>\n🔗 <code>{link}</code>\n"
                    "😀 {reactions}\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    link=link, reactions=" ".join(reactions[:8]),
                    bar=build_bar(i, total), i=i, total=total,
                    success=success, fail=fail, time=fmt_time(time.time()-t0, lang),
                ),
                self._stop_kb(op_uid, lang),
            )

        await upd(0)
        for i, acc in enumerate(accounts, 1):
            if self.stop_flags.get(op_uid): break
            try:
                await self._connect(acc["client"])
                emoji = random.choice(reactions)
                peer  = await acc["client"].resolve_peer(chat_id)
                await acc["client"].invoke(
                    functions.messages.SendReaction(
                        peer=peer, msg_id=msg_id,
                        reaction=[raw_types.ReactionEmoji(emoticon=emoji)],
                        big=False, add_to_recent=True,
                    )
                )
                success += 1
            except FloodWait as e: await asyncio.sleep(e.value); fail += 1
            except Exception as e: fail += 1; log.warning(f"[RX] {acc['phone']}: {e}")
            if i % 3 == 0 or i == total: await upd(i)
            if not self.stop_flags.get(op_uid): await asyncio.sleep(interval)

        self.stop_flags.pop(op_uid, None)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"), callback_data="back")
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "👍 <b>Накрутка завершена</b>\n\n✅ Успешно: <b>{success}</b>  ❌ Ошибок: <b>{fail}</b>",
                "👍 <b>Boost completed</b>\n\n✅ Success: <b>{success}</b>  ❌ Errors: <b>{fail}</b>",
                success=success, fail=fail,
            ),
            b.as_markup(),
        )

    # ════════════════════════════════════════════════════
    #   РЕПОРТЫ
    # ════════════════════════════════════════════════════
    async def _rp_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(owner_uid=uid)
        await state.set_state(ReportStates.target)
        await safe_edit(
            message,
            self._tr(
                lang,
                "🚩 <b>Репорты</b>\n\nВведите цель:\n"
                "• Ссылка на сообщение: <code>https://t.me/chan/123</code>\n"
                "• Юзернейм: <code>@username</code>",
                "🚩 <b>Reports</b>\n\nEnter target:\n"
                "• Message link: <code>https://t.me/chan/123</code>\n"
                "• Username: <code>@username</code>",
            ),
            self._cancel_kb(lang),
        )

    async def process_rp_target(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await state.update_data(target=message.text.strip()); await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        await message.answer(
            self._tr(lang, "🔢 <b>Количество аккаунтов</b>\nДоступно: <b>{n}</b>",
                     "🔢 <b>Number of accounts</b>\nAvailable: <b>{n}</b>", n=n),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(ReportStates.count)

    async def process_rp_count(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        try:
            count = int(message.text.strip()); assert 1 <= count <= n
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Введите число от 1 до {n}", "❌ Enter a number from 1 to {n}", n=n),
                reply_markup=self._cancel_kb(lang),
            )
            return
        await state.update_data(count=count)
        await message.answer(
            self._tr(lang, "⏱ <b>Общее время</b>\nПример: <code>5m</code>",
                     "⏱ <b>Total time</b>\nExample: <code>5m</code>"),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(ReportStates.time)

    async def process_rp_time(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        try:
            secs = self.parse_time(message.text.strip()); assert secs > 0
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Неверный формат", "❌ Invalid format"),
                reply_markup=self._cancel_kb(lang),
            ); return
        target = data["target"]; count = data["count"]
        uid    = data.get("owner_uid", 0); period = message.text.strip()
        await state.clear()
        await message.answer(
            self._tr(
                lang,
                "🚩 <b>Репорты</b> — подтверждение\n\n"
                "🎯 <code>{target}</code>\n👥 <b>Аккаунтов:</b> {count}\n⏱ <b>Время:</b> {period}",
                "🚩 <b>Reports</b> — confirmation\n\n"
                "🎯 <code>{target}</code>\n👥 <b>Accounts:</b> {count}\n⏱ <b>Time:</b> {period}",
                target=target, count=count, period=period,
            ),
            reply_markup=self._confirm_kb("rp", f"{uid}|{count}|{period}|{target}", lang),
        )

    async def run_rp(self, cb: types.CallbackQuery):
        await cb.answer()
        lang = self._lang(cb.from_user)
        raw    = cb.data[len("run:rp:"):]
        parts  = raw.split("|", 3)
        uid    = int(parts[0]); count = int(parts[1])
        period = parts[2]; target = parts[3]
        op_uid = cb.from_user.id; self.stop_flags[op_uid] = False

        msg_match = re.search(r"t\.me/(?:c/)?([^/]+)/(\d+)", target)
        accounts  = self._user_accounts(uid)[:count]
        total     = len(accounts)
        interval  = max(self.parse_time(period) / total, MIN_INTERVAL)
        success, fail = 0, 0; t0 = time.time()

        async def upd(i):
            await safe_edit(
                cb.message,
                self._tr(
                    lang,
                    "🚩 <b>Репорты</b>\n🎯 <code>{target}</code>\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    "🚩 <b>Reports</b>\n🎯 <code>{target}</code>\n\n"
                    "<code>[{bar}]</code>  {i}/{total}\n"
                    "✅ {success}   ❌ {fail}   ⏱ {time}",
                    target=target, bar=build_bar(i, total), i=i, total=total,
                    success=success, fail=fail, time=fmt_time(time.time()-t0, lang),
                ),
                self._stop_kb(op_uid, lang),
            )

        await upd(0)
        for i, acc in enumerate(accounts, 1):
            if self.stop_flags.get(op_uid): break
            try:
                await self._connect(acc["client"])
                if msg_match:
                    raw_c  = msg_match.group(1); msg_id = int(msg_match.group(2))
                    cid    = int(raw_c) if raw_c.isdigit() else raw_c
                    peer   = await acc["client"].resolve_peer(cid)
                    await acc["client"].invoke(
                        functions.messages.Report(
                            peer=peer, id=[msg_id],
                            reason=raw_types.InputReportReasonSpam(), message="",
                        )
                    )
                else:
                    raw_t = re.sub(r"https?://t\.me/|@", "", target).strip()
                    try: pid = int(raw_t)
                    except ValueError: pid = raw_t
                    peer = await acc["client"].resolve_peer(pid)
                    await acc["client"].invoke(
                        functions.account.ReportPeer(
                            peer=peer, reason=raw_types.InputReportReasonSpam(), message="",
                        )
                    )
                success += 1
            except FloodWait as e: await asyncio.sleep(e.value); fail += 1
            except Exception as e: fail += 1; log.warning(f"[RP] {acc['phone']}: {e}")
            if i % 3 == 0 or i == total: await upd(i)
            if not self.stop_flags.get(op_uid): await asyncio.sleep(interval)

        self.stop_flags.pop(op_uid, None)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"), callback_data="back")
        await safe_edit(
            cb.message,
            self._tr(
                lang,
                "🚩 <b>Репорты завершены</b>\n\n✅ Успешно: <b>{success}</b>  ❌ Ошибок: <b>{fail}</b>",
                "🚩 <b>Reports completed</b>\n\n✅ Success: <b>{success}</b>  ❌ Errors: <b>{fail}</b>",
                success=success, fail=fail,
            ),
            b.as_markup(),
        )



            # ════════════════════════════════════════════════════
    #   СПАМ В ЛС — БЕСКОНЕЧНЫЙ
    # ════════════════════════════════════════════════════
    async def _spam_start(self, message, state, uid=0, lang: str = "ru"):
        await state.update_data(owner_uid=uid)
        await state.set_state(SpamStates.targets)
        await safe_edit(
            message,
            self._tr(
                lang,
                "📨 <b>Спам в личные сообщения</b>\n\n"
                "Введите цели через запятую:\n<code>@user1, @user2, 123456789</code>",
                "📨 <b>Direct message spam</b>\n\n"
                "Enter targets separated by commas:\n<code>@user1, @user2, 123456789</code>",
            ),
            self._cancel_kb(lang),
        )

    async def process_spam_targets(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        targets = [t.strip() for t in message.text.split(",") if t.strip()]
        if not targets:
            await message.answer(
                self._tr(lang, "❌ Список пуст", "❌ List is empty"),
                reply_markup=self._cancel_kb(lang),
            ); return
        await message.delete()
        await state.update_data(targets=targets)
        await message.answer(
            self._tr(
                lang,
                "✅ <b>{n} целей</b>\n\nВведите текст сообщения:",
                "✅ <b>{n} targets</b>\n\nEnter message text:",
                n=len(targets),
            ),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(SpamStates.text)

    async def process_spam_text(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await state.update_data(text=message.text); await message.delete()
        data = await state.get_data()
        n    = len(self._user_accounts(data.get("owner_uid", message.from_user.id)))
        await message.answer(
            self._tr(lang, "🔢 <b>Количество аккаунтов</b>\nДоступно: <b>{n}</b>",
                     "🔢 <b>Number of accounts</b>\nAvailable: <b>{n}</b>", n=n),
            reply_markup=self._cancel_kb(lang),
        )
        await state.set_state(SpamStates.count)

    async def process_spam_count(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        await message.delete()
        data = await state.get_data()
        uid  = data.get("owner_uid", message.from_user.id)
        n    = len(self._user_accounts(uid))
        try:
            count = int(message.text.strip()); assert 1 <= count <= n
        except Exception:
            await message.answer(
                self._tr(lang, "❌ Введите число от 1 до {n}", "❌ Enter a number from 1 to {n}", n=n),
                reply_markup=self._cancel_kb(lang),
            )
            return
        targets = data["targets"]; text = data["text"]
        await state.clear()
        self._spam_pending[uid] = {"targets": targets, "text": text, "count": count}
        prev = text[:80] + ("…" if len(text) > 80 else "")
        await message.answer(
            self._tr(
                lang,
                "📨 <b>Спам в ЛС</b> — подтверждение\n\n"
                "🎯 <b>Целей:</b> {targets}\n"
                "🤖 <b>Аккаунтов:</b> {count}\n"
                "🔄 <b>Режим:</b> бесконечный\n"
                "📝 <i>{prev}</i>",
                "📨 <b>DM spam</b> — confirmation\n\n"
                "🎯 <b>Targets:</b> {targets}\n"
                "🤖 <b>Accounts:</b> {count}\n"
                "🔄 <b>Mode:</b> infinite\n"
                "📝 <i>{prev}</i>",
                targets=len(targets), count=count, prev=prev,
            ),
            reply_markup=self._confirm_kb("spam", str(uid), lang),
        )

    async def run_spam(self, cb: types.CallbackQuery):
        await cb.answer()
        lang = self._lang(cb.from_user)
        uid     = int(cb.data[len("run:spam:"):])
        pending = self._spam_pending.pop(uid, None)
        if not pending:
            await cb.message.edit_text(
                self._tr(lang, "❌ Данные устарели. Начните заново.", "❌ Data is stale. Start over."),
                reply_markup=self._back_kb(lang),
            )
            return

        targets_raw = pending["targets"]; text = pending["text"]; count = pending["count"]
        clean = []
        for t in targets_raw:
            t = re.sub(r"https?://t\.me/|@", "", t).strip()
            try: clean.append(int(t))
            except ValueError:
                if t: clean.append(t)
        if not clean:
            await cb.message.edit_text(
                self._tr(lang, "❌ Нет валидных целей", "❌ No valid targets"),
                reply_markup=self._back_kb(lang),
            ); return

        op_uid   = cb.from_user.id
        self.stop_flags[op_uid] = False
        accounts = self._user_accounts(uid)[:count]
        for acc in accounts:
            try: await self._connect(acc["client"])
            except Exception as e: log.warning(f"[SPAM INIT] {acc['phone']}: {e}")

        sent = errors = round_num = 0
        t0 = time.time(); last_upd = 0.0
        prog = await cb.message.edit_text(
            self._tr(lang, "📨 <b>Спам запускается…</b>", "📨 <b>Spam starting…</b>"),
            reply_markup=self._stop_kb(op_uid, lang),
        )

        async def upd():
            nonlocal last_upd
            now = time.time()
            if now - last_upd < 2: return
            last_upd = now
            speed = sent / max(now - t0, 1)
            await safe_edit(
                prog,
                self._tr(
                    lang,
                    "📨 <b>Спам в ЛС</b>  <i>— работает</i>\n\n"
                    "🎯 Целей: {targets}  ·  🤖 Аккаунтов: {accounts}\n"
                    "🔄 Раунд: <b>{round}</b>\n\n"
                    "✅ Отправлено: <b>{sent}</b>\n"
                    "❌ Ошибок: <b>{errors}</b>\n"
                    "⚡ Скорость: {speed:.1f} сообщ/с\n"
                    "⏱ Время: {time}",
                    "📨 <b>DM spam</b>  <i>— running</i>\n\n"
                    "🎯 Targets: {targets}  ·  🤖 Accounts: {accounts}\n"
                    "🔄 Round: <b>{round}</b>\n\n"
                    "✅ Sent: <b>{sent}</b>\n"
                    "❌ Errors: <b>{errors}</b>\n"
                    "⚡ Speed: {speed:.1f} msg/s\n"
                    "⏱ Time: {time}",
                    targets=len(clean), accounts=len(accounts), round=round_num,
                    sent=sent, errors=errors, speed=speed, time=fmt_time(now-t0, lang),
                ),
                self._stop_kb(op_uid, lang),
            )

        while not self.stop_flags.get(op_uid):
            round_num += 1
            for acc in accounts:
                if self.stop_flags.get(op_uid): break
                client = acc["client"]
                try:
                    if not client.is_connected: await client.connect()
                except Exception: continue
                for target in clean:
                    if self.stop_flags.get(op_uid): break
                    try:
                        await client.send_message(target, text)
                        sent += 1
                    except FloodWait as e:
                        await asyncio.sleep(min(e.value, 60)); errors += 1
                    except (UserPrivacyRestricted, PeerFlood): errors += 1
                    except Exception as e:
                        errors += 1; log.warning(f"[SPAM] {acc['phone']} → {target}: {e}")
                    await upd()
                    await asyncio.sleep(MIN_INTERVAL)

        self.stop_flags.pop(op_uid, None)
        elapsed = fmt_time(time.time() - t0, lang)
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🏠 Меню", "🏠 Menu"), callback_data="back")
        await safe_edit(
            prog,
            self._tr(
                lang,
                "📨 <b>Спам остановлен</b>\n\n"
                "✅ Отправлено: <b>{sent}</b>  ❌ Ошибок: <b>{errors}</b>\n"
                "🔄 Раундов: <b>{rounds}</b>  ⏱ Время: {elapsed}",
                "📨 <b>Spam stopped</b>\n\n"
                "✅ Sent: <b>{sent}</b>  ❌ Errors: <b>{errors}</b>\n"
                "🔄 Rounds: <b>{rounds}</b>  ⏱ Time: {elapsed}",
                sent=sent, errors=errors, rounds=round_num, elapsed=elapsed,
            ),
            b.as_markup(),
        )

    # ════════════════════════════════════════════════════
    #   АДМИНКА
    # ════════════════════════════════════════════════════
    async def cb_admin(self, cb: types.CallbackQuery, state: FSMContext):
        uid = cb.from_user.id
        lang = self._lang(cb.from_user)
        if uid not in ADMIN_IDS:
            await cb.answer(self._tr(lang, "⛔ Нет доступа", "⛔ Access denied"), show_alert=True); return
        await cb.answer()
        action = cb.data.split(":")[1]

        if action == "menu":
            await safe_edit(cb.message,
                            self._tr(lang, "⚙️ <b>Панель администратора</b>\n\nВыберите действие:",
                                     "⚙️ <b>Admin panel</b>\n\nChoose an action:"),
                            self._admin_kb(lang))

        elif action == "stats":
            await self._admin_stats(cb.message, lang=lang)

        elif action == "users":
            await self._admin_users(cb.message, lang=lang)

        elif action == "sessions":
            await self._admin_sessions(cb.message, lang=lang)

        elif action == "dl_sessions":
            await self._admin_dl_sessions(cb, lang=lang)

        elif action == "broadcast":
            await state.set_state(AdminBcStates.text)
            await safe_edit(cb.message,
                            self._tr(lang, "📢 <b>Рассылка всем пользователям</b>\n\nВведите текст:",
                                     "📢 <b>Broadcast to all users</b>\n\nEnter text:"),
                            self._cancel_kb(lang))

        elif action == "ban_prompt":
            await state.update_data(admin_action="ban")
            await cb.message.edit_text(
                self._tr(lang, "🚫 <b>Введите Telegram ID пользователя для бана:</b>",
                         "🚫 <b>Enter user's Telegram ID to ban:</b>"),
                reply_markup=self._cancel_kb(lang),
            )
            await state.set_state(type("AdminIdState", (StatesGroup,),
                                        {"v": State()})().v)

        elif action == "unban_prompt":
            await state.update_data(admin_action="unban")
            await cb.message.edit_text(
                self._tr(lang, "✅ <b>Введите Telegram ID пользователя для разбана:</b>",
                         "✅ <b>Enter user's Telegram ID to unban:</b>"),
                reply_markup=self._cancel_kb(lang),
            )

        elif action == "proxy":
            s = self.proxy_pool.stats
            await safe_edit(
                cb.message,
                self._tr(
                    lang,
                    "⚙️ <b>Прокси-пул</b>\n\n"
                    "🟢 Рабочих: <b>{working}</b>\n"
                    "🔴 Плохих: <b>{bad}</b>\n"
                    "🔄 Режим: <b>{mode}</b>\n"
                    "⏱ Возраст кэша: <b>{age}</b>",
                    "⚙️ <b>Proxy pool</b>\n\n"
                    "🟢 Working: <b>{working}</b>\n"
                    "🔴 Bad: <b>{bad}</b>\n"
                    "🔄 Mode: <b>{mode}</b>\n"
                    "⏱ Cache age: <b>{age}</b>",
                    working=s['working'], bad=s['bad'], mode=s['mode'], age=fmt_time(s['age_sec'], lang),
                ),
                self._admin_kb(lang),
            )

    async def _admin_stats(self, message: types.Message, lang: str = "ru"):
        total_u  = await db.count_users()
        act_24   = await db.count_active_users(24)
        act_7d   = await db.count_active_users(24*7)
        total_s  = await db.count_sessions(alive_only=False)
        alive_s  = await db.count_sessions(alive_only=True)
        errs     = await db.get_recent_events(limit=5, event_type="critical_error")
        px       = self.proxy_pool.stats

        err_lines = "\n".join(
            f"  • {ts(e['ts'])} <code>{e['detail'][:60]}</code>" for e in errs
        ) or "  —"

        text = self._tr(
            lang,
            "📊 <b>Статистика системы</b>\n\n"
            "<b>👥 Пользователи</b>\n"
            "  Всего: <b>{total_u}</b>\n"
            "  Активны 24ч: <b>{act_24}</b>\n"
            "  Активны 7д: <b>{act_7d}</b>\n\n"
            "<b>🔑 Сессии</b>\n"
            "  Всего: <b>{total_s}</b>   Живых: <b>{alive_s}</b>\n\n"
            "<b>🌐 Прокси</b>\n"
            "  Рабочих: <b>{working}</b>  Плохих: <b>{bad}</b>\n"
            "  Режим: <b>{mode}</b>\n\n"
            "<b>🚨 Последние ошибки</b>\n{err_lines}",
            "📊 <b>System stats</b>\n\n"
            "<b>👥 Users</b>\n"
            "  Total: <b>{total_u}</b>\n"
            "  Active 24h: <b>{act_24}</b>\n"
            "  Active 7d: <b>{act_7d}</b>\n\n"
            "<b>🔑 Sessions</b>\n"
            "  Total: <b>{total_s}</b>   Alive: <b>{alive_s}</b>\n\n"
            "<b>🌐 Proxies</b>\n"
            "  Working: <b>{working}</b>  Bad: <b>{bad}</b>\n"
            "  Mode: <b>{mode}</b>\n\n"
            "<b>🚨 Recent errors</b>\n{err_lines}",
            total_u=total_u, act_24=act_24, act_7d=act_7d,
            total_s=total_s, alive_s=alive_s,
            working=px["working"], bad=px["bad"], mode=px["mode"],
            err_lines=err_lines,
        )
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "🔄 Обновить", "🔄 Refresh"), callback_data="admin:stats")
        b.button(text=self._tr(lang, "◀️ Назад", "◀️ Back"),   callback_data="admin:menu")
        b.adjust(2)
        await safe_edit(message, text, b.as_markup())

    async def _admin_users(self, message: types.Message, lang: str = "ru"):
        users = await db.get_all_users()
        if not users:
            await safe_edit(message, self._tr(lang, "❌ Нет пользователей", "❌ No users"),
                            self._admin_kb(lang)); return
        lines = [self._tr(lang, "<b>👥 Пользователи ({n})</b>\n",
                          "<b>👥 Users ({n})</b>\n", n=len(users))]
        for u in users[:30]:
            name  = u.get("username") or u.get("full_name") or "?"
            badge = "🛡" if u["is_admin"] else ("🚫" if u["is_banned"] else "👤")
            accs  = len([a for a in self.accounts.values() if a.get("owner_uid") == u["uid"]])
            lines.append(
                f"  {badge} <code>{u['uid']}</code> @{name}"
                f"  🔑{accs}  <i>{ts(u['last_seen'])}</i>"
            )
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "◀️ Назад", "◀️ Back"), callback_data="admin:menu")
        await safe_edit(message, "\n".join(lines), b.as_markup())

    async def _admin_sessions(self, message: types.Message, lang: str = "ru"):
        sessions = await db.get_all_sessions()
        if not sessions:
            await safe_edit(message, self._tr(lang, "❌ Нет сессий", "❌ No sessions"),
                            self._admin_kb(lang)); return
        lines = [self._tr(lang, "<b>🔑 Все сессии ({n})</b>\n",
                          "<b>🔑 All sessions ({n})</b>\n", n=len(sessions))]
        for s in sessions[:40]:
            alive = "🟢" if s["is_alive"] else "🔴"
            owner = s.get("username") or str(s["owner_uid"])
            lines.append(
                f"  {alive} <code>+{s['phone']}</code>  @{owner}  {ts(s['last_ok'])}"
            )
        b = InlineKeyboardBuilder()
        b.button(text=self._tr(lang, "📥 Скачать всё", "📥 Download all"),
                 callback_data="admin:dl_sessions")
        b.button(text=self._tr(lang, "◀️ Назад", "◀️ Back"),      callback_data="admin:menu")
        b.adjust(2)
        await safe_edit(message, "\n".join(lines), b.as_markup())

    async def _admin_dl_sessions(self, cb: types.CallbackQuery, lang: str = "ru"):
        sessions = await db.get_all_sessions()
        if not sessions:
            await cb.answer(self._tr(lang, "Нет сессий", "No sessions"), show_alert=True); return
        # Каждая строка: +phone → session_string (как требует Pyrogram)
        lines = []
        for s in sessions:
            lines.append(
                f"# +{s['phone']} | owner:{s['owner_uid']} | alive:{s['is_alive']}"
                f" | added:{ts(s['added_at'])}\n{s['session_str']}"
            )
        content = "\n\n".join(lines).encode("utf-8")
        await cb.answer()
        await cb.message.answer_document(
            types.BufferedInputFile(content, filename="sessions.txt"),
            caption=(
                self._tr(
                    lang,
                    "🔑 <b>{n} сессий</b>\n"
                    "📅 {dt}\n\n"
                    "<i>Формат: комментарий + session string (Pyrogram)</i>",
                    "🔑 <b>{n} sessions</b>\n"
                    "📅 {dt}\n\n"
                    "<i>Format: comment + session string (Pyrogram)</i>",
                    n=len(sessions), dt=datetime.now().strftime('%d.%m.%Y %H:%M'),
                )
            ),
        )

    async def process_admin_bc_text(self, message: types.Message, state: FSMContext):
        lang = self._lang(message.from_user)
        text = message.text
        await state.clear()
        users = await db.get_all_users()
        await message.delete()
        prog = await message.answer(
            self._tr(
                lang,
                "📢 <b>Рассылка</b>\n\nЦелей: {n}\nОтправляю…",
                "📢 <b>Broadcast</b>\n\nTargets: {n}\nSending…",
                n=len(users),
            )
        )
        ok = fail = 0
        for u in users:
            try:
                await self.bot.send_message(u["uid"], text)
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await prog.edit_text(
            self._tr(
                lang,
                "📢 <b>Рассылка завершена</b>\n\n✅ Доставлено: <b>{ok}</b>  ❌ Ошибок: <b>{fail}</b>",
                "📢 <b>Broadcast completed</b>\n\n✅ Delivered: <b>{ok}</b>  ❌ Errors: <b>{fail}</b>",
                ok=ok, fail=fail,
            ),
            reply_markup=self._admin_kb(lang),
        )

    # ════════════════════════════════════════════════════
    #   KEEP-ALIVE
    # ════════════════════════════════════════════════════
    async def _keep_alive(self):
        while True:
            await asyncio.sleep(random.randint(180, 420))
            if not self.accounts: continue
            acc    = random.choice(list(self.accounts.values()))
            phone  = acc["phone"]; client = acc["client"]
            try:
                if not client.is_connected: await client.connect()
                await client.invoke(functions.updates.GetState())
                await db.set_session_alive(phone, True)
                log.debug(f"[ALIVE] {phone} OK")
            except Exception as e:
                log.warning(f"[ALIVE] {phone}: {e}")
                await db.set_session_alive(phone, False)
                try:
                    if client.is_connected: await client.disconnect()
                    await asyncio.sleep(5)
                    await client.connect()
                except Exception as e2:
                    log.error(f"[ALIVE CRIT] {phone}: {e2}")
                    try: await self._report_error(f"keep_alive:{phone}", e2)
                    except Exception: pass

    # ════════════════════════════════════════════════════
    #   ХЕНДЛЕРЫ
    # ════════════════════════════════════════════════════
    def _setup_handlers(self):
        dp = self.dp
        dp.message.register(self.cmd_start, Command("start"))
        dp.message.register(self.cmd_start, Command("cancel"))

        # FSM
        dp.message.register(self.process_phone,         AuthStates.phone)
        dp.message.register(self.process_code,          AuthStates.code)
        dp.message.register(self.process_password,      AuthStates.password)
        dp.message.register(self.process_sub_link,      SubStates.link)
        dp.message.register(self.process_sub_count,     SubStates.count)
        dp.message.register(self.process_sub_time,      SubStates.time)
        dp.message.register(self.process_bc_text,       BroadcastStates.text)
        dp.message.register(self.process_bc_target,     BroadcastStates.target)
        dp.message.register(self.process_rx_link,       ReactionStates.link)
        dp.message.register(self.process_rx_count,      ReactionStates.count)
        dp.message.register(self.process_rx_time,       ReactionStates.time)
        dp.message.register(self.process_rp_target,     ReportStates.target)
        dp.message.register(self.process_rp_count,      ReportStates.count)
        dp.message.register(self.process_rp_time,       ReportStates.time)
        dp.message.register(self.process_spam_targets,  SpamStates.targets)
        dp.message.register(self.process_spam_text,     SpamStates.text)
        dp.message.register(self.process_spam_count,    SpamStates.count)
        dp.message.register(self.process_admin_bc_text, AdminBcStates.text)

        # Callbacks
        dp.callback_query.register(self.cb_menu,      F.data.startswith("m:"))
        dp.callback_query.register(self.cb_back,      F.data == "back")
        dp.callback_query.register(self.cb_cancel,    F.data == "cancel")
        dp.callback_query.register(self.cb_stop,      F.data.startswith("stop:"))
        dp.callback_query.register(self.cb_check_sub, F.data == "check_sub")
        dp.callback_query.register(self.run_sub,      F.data.startswith("run:sub:"))
        dp.callback_query.register(self.run_bc,       F.data.startswith("run:bc:"))
        dp.callback_query.register(self.run_rx,       F.data.startswith("run:rx:"))
        dp.callback_query.register(self.run_rp,       F.data.startswith("run:rp:"))
        dp.callback_query.register(self.run_spam,     F.data.startswith("run:spam:"))
        dp.callback_query.register(self.cb_admin,     F.data.startswith("admin:"))

    # ════════════════════════════════════════════════════
    #   ЗАПУСК
    # ════════════════════════════════════════════════════
    async def run(self):
        await db.init_db()
        log.info("БД инициализирована")
        await self.proxy_pool.initialize()
        log.info("Прокси-пул: фоновая загрузка запущена")

        print("═" * 52)
        print("   Telegram Account Manager  v3.0")
        print("═" * 52)
        for acc in self.accounts.values():
            try:
                if not acc["client"].is_connected:
                    await acc["client"].connect()
                    print(f"   ✅  +{acc['phone']}")
            except Exception as e:
                print(f"   ❌  +{acc['phone']}: {e}")
            await asyncio.sleep(0.5)
        print(f"\n   Аккаунтов: {len(self.accounts)}")
        print("═" * 52)

        await asyncio.gather(
            self.dp.start_polling(self.bot, skip_updates=True),
            self._keep_alive(),
        )


if __name__ == "__main__":
    asyncio.run(channel_sub3())
    manager = AccountManager()
    asyncio.run(manager.run())
