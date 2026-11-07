import asyncio
import json
import logging
import os
import random
import socket
import sys
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
load_dotenv()  # Подгружает переменные из файла .env в окружение

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_NAME = os.getenv("DB_NAME")

if not BOT_TOKEN:
    sys.exit("❌ Ошибка: Переменная BOT_TOKEN не найдена! Проверь файл .env")

LANGUAGES = {
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
    "es": "🇪🇸 Español",
    "de": "🇩🇪 Deutsch",
    "kk": "🇰🇿 Қазақша",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# =====================================================================
# 2. СЛОЙ БАЗЫ ДАННЫХ (SQLite)
# =====================================================================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self):
        """Создание таблицы и проверка колонки языка."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    notify_time TEXT DEFAULT NULL,
                    is_active INTEGER DEFAULT 0,
                    language TEXT DEFAULT 'ru'
                )
                """
            )
            async with db.execute("PRAGMA table_info(users)") as cursor:
                columns = [row[1] for row in await cursor.fetchall()]
                if "language" not in columns:
                    await db.execute("ALTER TABLE users ADD COLUMN language TEXT DEFAULT 'ru'")
            await db.commit()

    async def add_user(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, language) VALUES (?, 'ru')",
                (user_id,),
            )
            await db.commit()

    async def set_schedule(self, user_id: int, time_str: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users (user_id, notify_time, is_active)
                VALUES (?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    notify_time = excluded.notify_time,
                    is_active = 1
                """,
                (user_id, time_str),
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

    async def get_user_settings(self, user_id: int) -> tuple[str | None, bool, str]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT notify_time, is_active, language FROM users WHERE user_id = ?",
                (user_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return row[0], bool(row[1]), (row[2] or "ru")
                return None, False, "ru"

    async def get_users_by_time(self, current_time: str) -> list[tuple[int, str]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT user_id, language FROM users WHERE notify_time = ? AND is_active = 1",
                (current_time,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], row[1] or "ru") for row in rows]


db = Database(DB_NAME)


# =====================================================================
# 3. СЕРВИС ЦИТАТ И АВТОМАТИЧЕСКОГО ПЕРЕВОДА
# =====================================================================
FALLBACK_QUOTES_EN = [
    ("Your time is limited, so don't waste it living someone else's life.", "Steve Jobs")
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
    # Принудительно используем IPv4 — Google часто банит IPv6-подсети VPS хостингов
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

        # Шлюз 3: Бесплатный переводчик MyMemory API (лоялен к любым VPS IP)
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
    """Получает оригинальную цитату из мировых API (работают с любым VPN)."""
    timeout = aiohttp.ClientTimeout(total=4)
    headers = {"User-Agent": "Mozilla/5.0"}

    # Источник 1: ZenQuotes API
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

    # Источник 2: DummyJSON Quotes API
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

    # Источник 3: Локальный резерв
    return random.choice(FALLBACK_QUOTES_EN)


async def fetch_quote(target_lang: str = "ru") -> str:
    """Получает цитату и переводит её на выбранный язык пользователя."""
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


def get_main_reply_kb() -> ReplyKeyboardMarkup:
    """Основная нижняя клавиатура."""
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
    """Инлайн-клавиатура выбора языка."""
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


def get_time_settings_kb(current_time: str | None, is_active: bool) -> InlineKeyboardMarkup:
    """Инлайн-клавиатура настройки времени рассылки."""
    presets = ["08:00", "09:00", "12:00", "18:00", "21:00"]
    buttons = []

    row = []
    for t in presets:
        label = f"✅ {t}" if (is_active and current_time == t) else t
        row.append(InlineKeyboardButton(text=label, callback_data=f"set_time:{t}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="⌨️ Ввести своё время", callback_data="custom_time")])

    if is_active:
        buttons.append([InlineKeyboardButton(text="🔕 Отключить рассылку", callback_data="disable_notify")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


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
        "• Настраивай ежедневную доставку в <b>«⏰ Настроить рассылку»</b>",
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


@router.message(F.text == "⏰ Настроить рассылку")
async def open_schedule_settings(message: Message, state: FSMContext):
    await state.clear()
    notify_time, is_active, _ = await db.get_user_settings(message.from_user.id)

    status_text = (
        f"🔔 Рассылка <b>включена</b> на <code>{notify_time}</code>"
        if is_active and notify_time
        else "🔕 Ежедневная рассылка сейчас <b>отключена</b>."
    )

    await message.answer(
        f"{status_text}\n\nВыбери готовое время на кнопках ниже или введи своё:",
        reply_markup=get_time_settings_kb(notify_time, is_active),
    )


@router.callback_query(F.data.startswith("set_time:"))
async def process_time_preset(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    selected_time = callback.data.split(":", 1)[1]
    await db.set_schedule(callback.from_user.id, selected_time)

    await callback.message.edit_text(
        f"✅ Отлично! Ежедневная цитата будет приходить в <b>{selected_time}</b>.",
        reply_markup=get_time_settings_kb(selected_time, True),
    )
    await callback.answer("Время сохранено!")


@router.callback_query(F.data == "disable_notify")
async def process_disable_notify(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await db.disable_schedule(callback.from_user.id)

    await callback.message.edit_text(
        "🔕 Ежедневная рассылка отключена. Ты всегда можешь включить её снова!",
        reply_markup=get_time_settings_kb(None, False),
    )
    await callback.answer("Рассылка отключена")


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

    await db.set_schedule(message.from_user.id, valid_time)
    await state.clear()

    await message.answer(
        f"✅ Готово! Буду присылать тебе вдохновляющую цитату каждый день в <b>{valid_time}</b>.",
        reply_markup=get_time_settings_kb(valid_time, True),
    )


# =====================================================================
# 6. ПЛАНИРОВЩИК ЗАДАЧ (APScheduler)
# =====================================================================
async def scheduled_quote_sender(bot: Bot):
    """Ежеминутная проверка расписания с учетом языка каждого пользователя."""
    now_str = datetime.now().strftime("%H:%M")
    users = await db.get_users_by_time(now_str)

    if not users:
        return

    logger.info(f"Начинаю рассылку на {now_str} для {len(users)} пользователей.")

    raw_text, raw_author = await fetch_raw_quote()
    translated_cache: dict[str, str] = {}

    for user_id, lang_code in users:
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

        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"🌅 <b>Ежедневная цитата:</b>\n\n{quote_msg}",
            )
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

    # Принудительно используем IPv4 для стабильной работы на Windows
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