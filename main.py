import asyncio
import json
import logging
import os
import random
import socket
import sys
import time
from datetime import datetime

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# =====================================================================
# 1. КОНФИГУРАЦИЯ (Загрузка из .env)
# =====================================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_NAME = os.getenv("DB_NAME", "quotes_bot.db")

if not BOT_TOKEN:
    sys.exit("❌ Ошибка: Переменная BOT_TOKEN не найдена! Проверь файл .env")

LANGUAGES = {
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
    "es": "🇪🇸 Español",
    "de": "🇩🇪 Deutsch",
    "kk": "🇰🇿 Қазақша",
}

INTERVAL_PRESETS = {
    30: "30 мин",
    60: "1 час",
    120: "2 часа",
    180: "3 часа",
    360: "6 часов",
    720: "12 часов",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def format_interval_label(minutes: int) -> str:
    """Форматирует минуты в читаемую строку (например: '2 ч. 30 мин.')."""
    if minutes in INTERVAL_PRESETS:
        return INTERVAL_PRESETS[minutes]
    if minutes % 60 == 0:
        return f"{minutes // 60} ч."
    if minutes > 60:
        return f"{minutes // 60} ч. {minutes % 60} мин."
    return f"{minutes} мин."


# =====================================================================
# 2. СЛОЙ БАЗЫ ДАННЫХ (SQLite с авто-миграцией под интервалы)
# =====================================================================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self):
        """Создание таблицы и автоматическое добавление колонок для интервалов."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    notify_time TEXT DEFAULT NULL,
                    is_active INTEGER DEFAULT 0,
                    language TEXT DEFAULT 'ru',
                    schedule_mode TEXT DEFAULT 'time',
                    interval_minutes INTEGER DEFAULT 180,
                    last_sent_ts INTEGER DEFAULT 0
                )
                """
            )
            async with db.execute("PRAGMA table_info(users)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]

                migrations = {
                    "language": "ALTER TABLE users ADD COLUMN language TEXT DEFAULT 'ru'",
                    "schedule_mode": "ALTER TABLE users ADD COLUMN schedule_mode TEXT DEFAULT 'time'",
                    "interval_minutes": "ALTER TABLE users ADD COLUMN interval_minutes INTEGER DEFAULT 180",
                    "last_sent_ts": "ALTER TABLE users ADD COLUMN last_sent_ts INTEGER DEFAULT 0",
                }
                for col_name, sql in migrations.items():
                    if col_name not in columns:
                        await db.execute(sql)
                        logger.info(f"БД обновлена: добавлена колонка '{col_name}'")

            await db.commit()

    async def add_user(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, language) VALUES (?, 'ru')",
                (user_id,),
            )
            await db.commit()

    async def set_exact_time_schedule(self, user_id: int, time_str: str):
        """Включает ежедневную рассылку на конкретное время ЧЧ:ММ."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, notify_time, schedule_mode, is_active)
                VALUES (?, ?, 'time', 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    notify_time = excluded.notify_time,
                    schedule_mode = 'time',
                    is_active = 1
                """,
                (user_id, time_str),
            )
            await db.commit()

    async def set_interval_schedule(self, user_id: int, minutes: int):
        """Включает циклическую рассылку каждые N минут (отсчет с текущего момента)."""
        now_ts = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, interval_minutes, schedule_mode, is_active, last_sent_ts)
                VALUES (?, ?, 'interval', 1, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    interval_minutes = excluded.interval_minutes,
                    schedule_mode = 'interval',
                    is_active = 1,
                    last_sent_ts = excluded.last_sent_ts
                """,
                (user_id, minutes, now_ts),
            )
            await db.commit()

    async def update_last_sent(self, user_id: int, ts: int):
        """Обновляет метку времени последней интервальной отправки."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET last_sent_ts = ? WHERE user_id = ?",
                (ts, user_id),
            )
            await db.commit()

    async def disable_schedule(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET is_active = 0 WHERE user_id = ?",
                (user_id,),
            )
            await db.commit()

    async def set_language(self, user_id: int, lang_code: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, language)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    language = excluded.language
                """,
                (user_id, lang_code),
            )
            await db.commit()

    async def get_user_language(self, user_id: int) -> str:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT language FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if (row and row[0]) else "ru"

    async def get_user_settings(self, user_id: int) -> dict:
        """Возвращает словарь со всеми настройками рассылки пользователя."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT notify_time, is_active, language, schedule_mode, interval_minutes
                FROM users WHERE user_id = ?
                """,
                (user_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        "notify_time": row[0],
                        "is_active": bool(row[1]),
                        "language": row[2] or "ru",
                        "schedule_mode": row[3] or "time",
                        "interval_minutes": int(row[4] or 180),
                    }
                return {
                    "notify_time": None,
                    "is_active": False,
                    "language": "ru",
                    "schedule_mode": "time",
                    "interval_minutes": 180,
                }

    async def get_due_users(self, current_time_str: str, now_ts: int) -> list[tuple[int, str, str]]:
        """
        Возвращает список пользователей (user_id, language, schedule_mode),
        которым пора отправить цитату прямо сейчас (по точному времени ИЛИ по интервалу).
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT user_id, language, schedule_mode
                FROM users
                WHERE is_active = 1
                  AND (
                      (schedule_mode = 'time' AND notify_time = ?)
                      OR
                      (schedule_mode = 'interval' AND interval_minutes > 0 AND (? - last_sent_ts) >= (interval_minutes * 60))
                  )
                """,
                (current_time_str, now_ts),
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], row[1] or "ru", row[2] or "time") for row in rows]


db = Database(DB_NAME)


# =====================================================================
# 3. СЕРВИС ЦИТАТ И АВТОМАТИЧЕСКОГО ПЕРЕВОДА
# =====================================================================
FALLBACK_QUOTES_EN = [
    ("Whatever you can do, or dream you can, begin it. Boldness has genius, power, and magic in it.", "Johann Wolfgang von Goethe"),
    ("Let him that would move the world first move himself.", "Socrates"),
    ("Be not afraid of growing slowly, be afraid only of standing still.", "Chinese Proverb"),
    ("The most difficult thing is the decision to act, the rest is merely tenacity.", "Amelia Earhart"),
    ("Success is stumbling from failure to failure with no loss of enthusiasm.", "Winston Churchill"),
    ("Your time is limited, so don't waste it living someone else's life.", "Steve Jobs"),
]


async def translate_text(text: str, target_lang: str) -> str:
    """Асинхронный перевод с 3 резервными шлюзами (не блокируется на VPS)."""
    if not text or target_lang == "en":
        return text

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }
    timeout = aiohttp.ClientTimeout(total=5)
    connector = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)

    async with aiohttp.ClientSession(
        timeout=timeout, headers=headers, connector=connector
    ) as session:
        # Шлюз 1: Стандартный Google GTX
        try:
            url_1 = "https://translate.googleapis.com/translate_a/single"
            params_1 = {
                "client": "gtx",
                "sl": "en",
                "tl": target_lang,
                "dt": "t",
                "q": text,
            }
            async with session.get(url_1, params=params_1) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    result = "".join(part[0] for part in data[0] if part[0])
                    if result and result.lower() != text.lower():
                        return result
        except Exception as e:
            logger.debug(f"Шлюз 1 (GTX) не сработал: {e}")

        # Шлюз 2: Альтернативный клиентский сервер Google (clients5)
        try:
            url_2 = "https://clients5.google.com/translate_a/t"
            params_2 = {
                "client": "dict-chrome-ex",
                "sl": "en",
                "tl": target_lang,
                "q": text,
            }
            async with session.get(url_2, params=params_2) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, list) and data:
                        first = data[0]
                        result = first[0] if isinstance(first, list) else str(first)
                        if result and result.lower() != text.lower():
                            return result
        except Exception as e:
            logger.debug(f"Шлюз 2 (clients5) не сработал: {e}")

        # Шлюз 3: Бесплатный переводчик MyMemory API
        try:
            url_3 = "https://api.mymemory.translated.net/get"
            params_3 = {
                "q": text,
                "langpair": f"en|{target_lang}",
            }
            async with session.get(url_3, params=params_3) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    translated = (data.get("responseData") or {}).get("translatedText")
                    if translated and "MYMEMORY WARNING" not in translated:
                        return translated.strip()
        except Exception as e:
            logger.warning(f"Все 3 шлюза перевода вернули ошибку ({target_lang}): {e}")

    return text


async def fetch_raw_quote() -> tuple[str, str]:
    timeout = aiohttp.ClientTimeout(total=4)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get("https://zenquotes.io/api/random") as resp:
                if resp.status == 200:
                    data = json.loads(await resp.text())
                    if isinstance(data, list) and data:
                        q = data[0].get("q", "").strip()
                        a = data[0].get("a", "").strip() or "Unknown"
                        if q:
                            return q, a
    except Exception:
        pass

    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get("https://dummyjson.com/quotes/random") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    q = data.get("quote", "").strip()
                    a = data.get("author", "").strip() or "Unknown"
                    if q:
                        return q, a
    except Exception:
        pass

    return random.choice(FALLBACK_QUOTES_EN)


async def fetch_quote(target_lang: str = "ru") -> str:
    text, author = await fetch_raw_quote()

    if target_lang != "en":
        text, author = await asyncio.gather(
            translate_text(text, target_lang),
            translate_text(author, target_lang),
        )

    return f"✨ <i>«{text}»</i>\n\n— <b>{author}</b>"


# =====================================================================
# 4. КЛАВИАТУРЫ И СОСТОЯНИЯ (FSM)
# =====================================================================
class ScheduleStates(StatesGroup):
    waiting_for_custom_time = State()
    waiting_for_custom_interval = State()


def get_main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🌟 Получить цитату")],
            [
                KeyboardButton(text="⏰ Настроить рассылку"),
                KeyboardButton(text="🌐 Язык / Language"),
            ],
        ],
        resize_keyboard=True,
    )


def get_language_kb(current_lang: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for code, title in LANGUAGES.items():
        label = f"✅ {title}" if code == current_lang else title
        row.append(InlineKeyboardButton(text=label, callback_data=f"set_lang:{code}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_schedule_hub_kb(settings: dict) -> InlineKeyboardMarkup:
    """Главное меню настроек рассылки: выбор между Точным временем и Интервалом."""
    is_active = settings["is_active"]
    mode = settings["schedule_mode"]

    time_label = "🎯 По точному времени" + (" ✅" if is_active and mode == "time" else "")
    interval_label = "🔄 По интервалу (циклично)" + (" ✅" if is_active and mode == "interval" else "")

    buttons = [
        [InlineKeyboardButton(text=time_label, callback_data="menu_mode:time")],
        [InlineKeyboardButton(text=interval_label, callback_data="menu_mode:interval")],
    ]
    if is_active:
        buttons.append([InlineKeyboardButton(text="🔕 Отключить рассылку", callback_data="disable_notify")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_exact_time_kb(settings: dict) -> InlineKeyboardMarkup:
    """Подменю выбора точного времени ЧЧ:ММ."""
    presets = ["08:00", "09:00", "12:00", "18:00", "21:00"]
    is_time_active = settings["is_active"] and settings["schedule_mode"] == "time"
    current_time = settings["notify_time"]

    buttons = []
    row = []
    for t in presets:
        label = f"✅ {t}" if (is_time_active and current_time == t) else t
        row.append(InlineKeyboardButton(text=label, callback_data=f"set_time:{t}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="⌨️ Ввести своё время (ЧЧ:ММ)", callback_data="custom_time")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к выбору режима", callback_data="back_to_schedule_hub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_interval_kb(settings: dict) -> InlineKeyboardMarkup:
    """Подменю выбора циклического интервала."""
    is_int_active = settings["is_active"] and settings["schedule_mode"] == "interval"
    current_int = settings["interval_minutes"]

    buttons = []
    row = []
    for minutes, title in INTERVAL_PRESETS.items():
        label = f"✅ {title}" if (is_int_active and current_int == minutes) else title
        row.append(InlineKeyboardButton(text=label, callback_data=f"set_interval:{minutes}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="⌨️ Ввести свой интервал (в минутах)", callback_data="custom_interval")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к выбору режима", callback_data="back_to_schedule_hub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def format_schedule_status_text(settings: dict) -> str:
    """Формирует текст с описанием текущего статуса рассылки."""
    if not settings["is_active"]:
        return "🔕 Автоматическая рассылка сейчас <b>отключена</b>."

    if settings["schedule_mode"] == "interval":
        label = format_interval_label(settings["interval_minutes"])
        return f"🔄 Активен режим: <b>По интервалу</b> (каждые <code>{label}</code>)"

    return f"🎯 Активен режим: <b>По времени</b> (ежедневно в <code>{settings['notify_time']}</code>)"


# =====================================================================
# 5. ОБРАБОТЧИКИ (HANDLERS)
# =====================================================================
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await db.add_user(message.from_user.id)
    await message.answer(
        f"Привет, <b>{message.from_user.first_name}</b>! 👋\n\n"
        "Я бот, который поможет тебе зарядиться мотивацией и правильным настроем.\n"
        "• Жми <b>«🌟 Получить цитату»</b> для мгновенного вдохновения\n"
        "• Выбирай удобный язык перевода в <b>«🌐 Язык / Language»</b>\n"
        "• Настраивай рассылку по времени или интервалу в <b>«⏰ Настроить рассылку»</b>",
        reply_markup=get_main_reply_kb(),
    )


@router.message(F.text == "🌟 Получить цитату")
async def send_instant_quote(message: Message):
    user_lang = await db.get_user_language(message.from_user.id)
    quote = await fetch_quote(target_lang=user_lang)
    await message.answer(quote)


@router.message(F.text == "🌐 Язык / Language")
async def open_language_settings(message: Message, state: FSMContext):
    await state.clear()
    current_lang = await db.get_user_language(message.from_user.id)
    lang_name = LANGUAGES.get(current_lang, "🇷🇺 Русский")

    await message.answer(
        f"🌐 Текущий язык цитат: <b>{lang_name}</b>\n\n"
        "Выбери язык, на который мне переводить цитаты:",
        reply_markup=get_language_kb(current_lang),
    )


@router.callback_query(F.data.startswith("set_lang:"))
async def process_language_selection(callback: CallbackQuery):
    lang_code = callback.data.split(":", 1)[1]
    if lang_code not in LANGUAGES:
        await callback.answer("Неизвестный язык")
        return

    await db.set_language(callback.from_user.id, lang_code)
    lang_name = LANGUAGES[lang_code]
    sample_quote = await fetch_quote(target_lang=lang_code)

    await callback.message.edit_text(
        f"✅ Язык успешно изменен на <b>{lang_name}</b>!\n\n"
        f"Вот свежая цитата на выбранном языке:\n\n{sample_quote}",
        reply_markup=get_language_kb(lang_code),
    )
    await callback.answer(f"Выбран {lang_name}")


# ---------------- НАСТРОЙКИ РАССЫЛКИ (ХАБ: ВРЕМЯ ИЛИ ИНТЕРВАЛ) ----------------
@router.message(F.text == "⏰ Настроить рассылку")
async def open_schedule_settings(message: Message, state: FSMContext):
    await state.clear()
    settings = await db.get_user_settings(message.from_user.id)
    status_text = format_schedule_status_text(settings)

    await message.answer(
        f"⚙️ <b>Настройки рассылки цитат</b>\n\n"
        f"{status_text}\n\n"
        "Выбери удобный формат получения цитат:",
        reply_markup=get_schedule_hub_kb(settings),
    )


@router.callback_query(F.data == "back_to_schedule_hub")
async def back_to_schedule_hub_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    settings = await db.get_user_settings(callback.from_user.id)
    status_text = format_schedule_status_text(settings)

    await callback.message.edit_text(
        f"⚙️ <b>Настройки рассылки цитат</b>\n\n"
        f"{status_text}\n\n"
        "Выбери удобный формат получения цитат:",
        reply_markup=get_schedule_hub_kb(settings),
    )
    await callback.answer()


@router.callback_query(F.data == "menu_mode:time")
async def open_exact_time_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    settings = await db.get_user_settings(callback.from_user.id)

    await callback.message.edit_text(
        "🎯 <b>Режим: По определенному времени</b>\n\n"
        "Цитата будет приходить один раз в день в выбранный час.\n"
        "Выбери готовое время ниже или введи своё:",
        reply_markup=get_exact_time_kb(settings),
    )
    await callback.answer()


@router.callback_query(F.data == "menu_mode:interval")
async def open_interval_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    settings = await db.get_user_settings(callback.from_user.id)

    await callback.message.edit_text(
        "🔄 <b>Режим: Циклическая интервальная рассылка</b>\n\n"
        "Бот будет присылать новую цитату регулярно через заданный промежуток времени.\n"
        "Выбери интервал на кнопках или укажи свой:",
        reply_markup=get_interval_kb(settings),
    )
    await callback.answer()


# ---------------- ОБРАБОТКА ТОЧНОГО ВРЕМЕНИ ----------------
@router.callback_query(F.data.startswith("set_time:"))
async def process_time_preset(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    selected_time = callback.data.split(":", 1)[1]
    await db.set_exact_time_schedule(callback.from_user.id, selected_time)
    settings = await db.get_user_settings(callback.from_user.id)

    await callback.message.edit_text(
        f"✅ Ежедневная рассылка по времени включена на <b>{selected_time}</b>!",
        reply_markup=get_exact_time_kb(settings),
    )
    await callback.answer("Время сохранено!")


@router.callback_query(F.data == "custom_time")
async def ask_custom_time(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ScheduleStates.waiting_for_custom_time)
    await callback.message.answer(
        "⌨️ Введи желаемое время в формате <b>ЧЧ:ММ</b> (например, <code>07:30</code> или <code>22:15</code>):"
    )
    await callback.answer()


@router.message(ScheduleStates.waiting_for_custom_time)
async def process_custom_time_input(message: Message, state: FSMContext):
    user_input = message.text.strip() if message.text else ""

    try:
        valid_time = datetime.strptime(user_input, "%H:%M").strftime("%H:%M")
    except ValueError:
        await message.answer(
            "❌ Неверный формат времени.\nПожалуйста, используй формат <b>ЧЧ:ММ</b> (например, <code>08:45</code>)."
        )
        return

    await db.set_exact_time_schedule(message.from_user.id, valid_time)
    await state.clear()
    settings = await db.get_user_settings(message.from_user.id)

    await message.answer(
        f"✅ Готово! Буду присылать тебе цитату каждый день в <b>{valid_time}</b>.",
        reply_markup=get_exact_time_kb(settings),
    )


# ---------------- ОБРАБОТКА ИНТЕРВАЛОВ ----------------
@router.callback_query(F.data.startswith("set_interval:"))
async def process_interval_preset(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    minutes = int(callback.data.split(":", 1)[1])
    await db.set_interval_schedule(callback.from_user.id, minutes)
    settings = await db.get_user_settings(callback.from_user.id)
    label = format_interval_label(minutes)

    await callback.message.edit_text(
        f"✅ Интервальная рассылка включена!\n"
        f"Новая цитата будет приходить каждые <b>{label}</b>.",
        reply_markup=get_interval_kb(settings),
    )
    await callback.answer(f"Интервал: {label}")


@router.callback_query(F.data == "custom_interval")
async def ask_custom_interval(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ScheduleStates.waiting_for_custom_interval)
    await callback.message.answer(
        "⌨️ Введи желаемый интервал рассылки <b>в минутах</b> (от <code>5</code> до <code>1440</code>).\n"
        "<i>Например: <code>45</code> (каждые 45 минут) или <code>240</code> (каждые 4 часа):</i>"
    )
    await callback.answer()


@router.message(ScheduleStates.waiting_for_custom_interval)
async def process_custom_interval_input(message: Message, state: FSMContext):
    user_input = message.text.strip() if message.text else ""

    if not user_input.isdigit():
        await message.answer("❌ Пожалуйста, введи целое число минут (например, <code>90</code>).")
        return

    minutes = int(user_input)
    if not (5 <= minutes <= 1440):
        await message.answer("❌ Интервал должен быть от <b>5</b> до <b>1440</b> минут (24 часов).")
        return

    await db.set_interval_schedule(message.from_user.id, minutes)
    await state.clear()
    settings = await db.get_user_settings(message.from_user.id)
    label = format_interval_label(minutes)

    await message.answer(
        f"✅ Готово! Циклическая рассылка настроена: каждые <b>{label}</b>.",
        reply_markup=get_interval_kb(settings),
    )


# ---------------- ОТКЛЮЧЕНИЕ РАССЫЛКИ ----------------
@router.callback_query(F.data == "disable_notify")
async def process_disable_notify(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await db.disable_schedule(callback.from_user.id)
    settings = await db.get_user_settings(callback.from_user.id)

    await callback.message.edit_text(
        "🔕 Рассылка отключена. Ты всегда можешь включить её снова!",
        reply_markup=get_schedule_hub_kb(settings),
    )
    await callback.answer("Рассылка отключена")


# =====================================================================
# 6. ПЛАНИРОВЩИК ЗАДАЧ (APScheduler: Точное время + Интервалы)
# =====================================================================
async def scheduled_quote_sender(bot: Bot):
    """Ежеминутная проверка пользователей как по расписанию ЧЧ:ММ, так и по интервалу."""
    now_str = datetime.now().strftime("%H:%M")
    now_ts = int(time.time())

    due_users = await db.get_due_users(now_str, now_ts)
    if not due_users:
        return

    logger.info(f"Начинаю рассылку ({now_str}) для {len(due_users)} пользователей.")

    raw_text, raw_author = await fetch_raw_quote()
    translated_cache: dict[str, str] = {}

    for user_id, lang_code, mode in due_users:
        if lang_code not in translated_cache:
            if lang_code == "en":
                t_text, t_author = raw_text, raw_author
            else:
                t_text, t_author = await asyncio.gather(
                    translate_text(raw_text, lang_code),
                    translate_text(raw_author, lang_code),
                )
            translated_cache[lang_code] = f"✨ <i>«{t_text}»</i>\n\n— <b>{t_author}</b>"

        quote_msg = translated_cache[lang_code]
        header = "🔄 <b>Цитата по интервалу:</b>" if mode == "interval" else "🌅 <b>Ежедневная цитата:</b>"

        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"{header}\n\n{quote_msg}",
            )
            if mode == "interval":
                await db.update_last_sent(user_id, now_ts)
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
            if "Forbidden" in str(e):
                await db.disable_schedule(user_id)


# =====================================================================
# 7. ТОЧКА ВХОДА (MAIN)
# =====================================================================
async def main():
    await db.init_db()

    session = AiohttpSession()
    session._connector_init["family"] = socket.AF_INET

    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scheduled_quote_sender,
        trigger="cron",
        minute="*",
        kwargs={"bot": bot},
    )
    scheduler.start()

    logger.info("Бот успешно запущен!")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")