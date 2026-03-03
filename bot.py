# bot.py
import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError
from dotenv import load_dotenv

# ----------------------------
# ENV
# ----------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE", "Posts")

DEFAULT_CHANNEL_ID = os.getenv("CHANNEL_ID")  # канал "по умолчанию"
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not BOT_TOKEN:
    raise SystemExit("Ошибка: не задана переменная BOT_TOKEN")
if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
    raise SystemExit("Ошибка: не заданы AIRTABLE_TOKEN/AIRTABLE_BASE_ID")
if not DEFAULT_CHANNEL_ID:
    raise SystemExit("Ошибка: не задана переменная CHANNEL_ID (канал по умолчанию)")
if not ADMIN_ID:
    raise SystemExit("Ошибка: не задана переменная ADMIN_ID")

# ----------------------------
# Airtable schema (поля)
# ----------------------------
# Настройте здесь, если у вас в Airtable другие имена колонок
F_TITLE = os.getenv("F_TITLE", "Title")
F_CAPTION = os.getenv("F_CAPTION", "Caption")
F_STATUS = os.getenv("F_STATUS", "Status")
F_ORDER = os.getenv("F_ORDER", "Order")
F_MEDIA = os.getenv("F_MEDIA", "Media")   # Attachment
F_WEB = os.getenv("F_WEB", "Web")         # URL

STATUS_DRAFT = os.getenv("STATUS_DRAFT", "Draft")
STATUS_PUBLISHED = os.getenv("STATUS_PUBLISHED", "Published")

AIRTABLE_API_BASE = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Bot init
# ----------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
from functools import wraps

def admin_only(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        """
        Универсальный декоратор для aiogram 2.x.
        Корректно пропускает state и любые другие аргументы.
        Работает и для callback_query, и для message.
        """
        obj = args[0]  # CallbackQuery или Message

        user_id = None
        if hasattr(obj, "from_user") and obj.from_user:
            user_id = obj.from_user.id

        if user_id != ADMIN_ID:
            if isinstance(obj, types.CallbackQuery):
                await obj.answer("⛔ Нет прав.", show_alert=False)
            elif isinstance(obj, types.Message):
                await obj.reply("⛔ Нет прав.")
            return

        return await func(*args, **kwargs)

    return wrapper

# Канал для текущей "экскурсии" (может временно меняться)
CURRENT_CHANNEL_ID = DEFAULT_CHANNEL_ID

# ----------------------------
# FSM: смена канала
# ----------------------------
class ChannelState(StatesGroup):
    waiting_for_channel = State()

# ----------------------------
# Airtable HTTP helpers
# ----------------------------
_session = requests.Session()
_headers = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

def _airtable_request(method: str, url: str, *, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
    """
    Синхронный запрос к Airtable (выполняется в executor).
    Простая обработка transient-ошибок и 429.
    """
    backoff = 1.0
    for attempt in range(6):
        r = _session.request(method, url, headers=_headers, params=params, json=json_body, timeout=30)
        # 429 / rate limit
        if r.status_code == 429:
            retry_after = float(r.headers.get("Retry-After", "1"))
            time.sleep(max(1.0, retry_after))
            continue
        # 5xx временные
        if 500 <= r.status_code <= 599:
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)
            continue

        r.raise_for_status()
        return r.json()

    # last try - raise
    r.raise_for_status()
    return {}  # never

async def airtable_get(params: dict) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: _airtable_request("GET", AIRTABLE_API_BASE, params=params))

async def airtable_patch_record(record_id: str, fields: dict) -> dict:
    loop = asyncio.get_running_loop()
    url = f"{AIRTABLE_API_BASE}/{record_id}"
    body = {"fields": fields}
    return await loop.run_in_executor(None, lambda: _airtable_request("PATCH", url, json_body=body))

async def airtable_batch_patch(records: List[Tuple[str, dict]]) -> None:
    """
    Batch PATCH: до 10 записей за запрос.
    """
    loop = asyncio.get_running_loop()
    for i in range(0, len(records), 10):
        chunk = records[i:i+10]
        body = {"records": [{"id": rid, "fields": f} for rid, f in chunk]}
        await loop.run_in_executor(None, lambda b=body: _airtable_request("PATCH", AIRTABLE_API_BASE, json_body=b))

# ----------------------------
# Data extractors
# ----------------------------
def extract_title(fields: Dict[str, Any]) -> str:
    v = fields.get(F_TITLE)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return "Без названия"

def extract_caption(fields: Dict[str, Any]) -> str:
    v = fields.get(F_CAPTION)
    if isinstance(v, str):
        return v.strip()
    return ""

def extract_order(fields: Dict[str, Any]) -> float:
    v = fields.get(F_ORDER)
    try:
        if v is None:
            return 10**12
        return float(v)
    except Exception:
        return 10**12

def extract_web_url(fields: Dict[str, Any]) -> Optional[str]:
    v = fields.get(F_WEB)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

def extract_media_url(fields: Dict[str, Any]) -> Optional[str]:
    """
    Берём первый attachment (url).
    """
    v = fields.get(F_MEDIA)
    if isinstance(v, list) and v:
        first = v[0]
        if isinstance(first, dict) and isinstance(first.get("url"), str):
            return first["url"]
    return None

# ----------------------------
# Caching to reduce Airtable API calls
# ----------------------------
DRAFT_CACHE: List[dict] = []
DRAFT_CACHE_TS: float = 0.0
DRAFT_CACHE_TTL_SEC = int(os.getenv("DRAFT_CACHE_TTL_SEC", "120"))  # 2 минуты

def _draft_cache_is_fresh() -> bool:
    return bool(DRAFT_CACHE) and (time.time() - DRAFT_CACHE_TS) < DRAFT_CACHE_TTL_SEC

async def refresh_draft_cache(*, full_fields: bool = True) -> List[dict]:
    """
    1 запрос (или несколько при пагинации) вместо запроса на каждый пост.
    full_fields=True: берём поля для публикации (caption/web/media)
    full_fields=False: только title/order/status (для списка)
    """
    fields_list = [F_TITLE, F_ORDER, F_STATUS]
    if full_fields:
        fields_list += [F_CAPTION, F_MEDIA, F_WEB]

    records: List[dict] = []
    offset = None

    while True:
        params = {
            "filterByFormula": f"{{{F_STATUS}}}='{STATUS_DRAFT}'",
            "pageSize": 100,
            "sort[0][field]": F_ORDER,
            "sort[0][direction]": "asc",
        }
        # Airtable позволяет ограничить выдачу полей
        for i, f in enumerate(fields_list):
            params[f"fields[{i}]"] = f
        if offset:
            params["offset"] = offset

        data = await airtable_get(params)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    # гарантируем сортировку на нашей стороне тоже (на случай смешанных типов)
    records.sort(key=lambda r: extract_order(r.get("fields", {})))

    global DRAFT_CACHE, DRAFT_CACHE_TS
    DRAFT_CACHE = records
    DRAFT_CACHE_TS = time.time()
    return records

async def get_next_draft_record() -> Optional[dict]:
    """
    Возвращает следующий черновик из кэша.
    Если кэша нет/протух — обновляет один раз.
    """
    if not _draft_cache_is_fresh():
        await refresh_draft_cache(full_fields=True)

    if not DRAFT_CACHE:
        return None

    return DRAFT_CACHE.pop(0)

def invalidate_cache() -> None:
    global DRAFT_CACHE, DRAFT_CACHE_TS
    DRAFT_CACHE = []
    DRAFT_CACHE_TS = 0.0

# ----------------------------
# Telegram send helpers (flood + transient)
# ----------------------------
POST_IN_PROGRESS = asyncio.Lock()

async def safe_send_to_channel(send_coro_factory, *, max_attempts: int = 5) -> None:
    """
    send_coro_factory: lambda -> coroutine (например lambda: bot.send_photo(...))
    Обрабатывает RetryAfter (flood control) и transient TelegramAPIError/Network.
    """
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await send_coro_factory()
            return
        except RetryAfter as e:
            # Telegram явно говорит сколько ждать
            await asyncio.sleep(int(getattr(e, "timeout", None) or getattr(e, "retry_after", 1) or 1))
        except TelegramAPIError as e:
            # transient (Bad Gateway и т.п.)
            logger.warning("TelegramAPIError (attempt %s/%s): %s", attempt, max_attempts, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10)
        except Exception as e:
            logger.warning("Send error (attempt %s/%s): %s", attempt, max_attempts, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10)

    raise RuntimeError("send failed after retries")

# ----------------------------
# UI
# ----------------------------
def main_menu() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)

    # 1) Опубликовать — отдельной строкой
    kb.add(types.InlineKeyboardButton("🚀 Опубликовать >>", callback_data="post"))

    # 2) Канал + Старт
    kb.add(
        types.InlineKeyboardButton("📡 Канал", callback_data="set_channel"),
        types.InlineKeyboardButton("🟢 Старт", callback_data="start_channel"),
    )

    # 3) Сброс статусов + Черновики
    kb.add(
        types.InlineKeyboardButton("🔁 Сброс статусов", callback_data="reset_status"),
        types.InlineKeyboardButton("📋 Черновики", callback_data="list"),
    )

    # 4) Завершение — отдельной строкой
    kb.add(types.InlineKeyboardButton("🟥 Завершение", callback_data="finish_channel"))

    return kb

# ----------------------------
# Commands
# ----------------------------
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.answer("Готов. Выбери действие:", reply_markup=main_menu())

# ----------------------------
# Button: list drafts
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "list")
async def callback_list(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)

    try:
        # Для списка можно использовать кэш; если кэш пуст/протух — обновим (лёгким запросом)
        if not _draft_cache_is_fresh():
            await refresh_draft_cache(full_fields=False)

        records = DRAFT_CACHE[:]  # копия
        if not records:
            await bot.send_message(callback_query.from_user.id, "Нет черновиков.")
            return

        lines = []
        for i, rec in enumerate(records, start=1):
            f = rec.get("fields", {})
            title = extract_title(f)
            lines.append(f"{i}. {title}")

        header = "Черновики:\n"
        chunk = header
        for line in lines:
            if len(chunk) + len(line) + 1 > 3800:
                await bot.send_message(callback_query.from_user.id, chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk.strip():
            await bot.send_message(callback_query.from_user.id, chunk)

    except Exception as e:
        logger.exception("Ошибка получения списка из Airtable: %s", e)
        await bot.send_message(callback_query.from_user.id, "❌ Ошибка чтения Airtable.")

# ----------------------------
# Button: post next
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "post")
@admin_only
async def callback_post(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)

    # Не даём нажимать "Опубликовать" параллельно (и ловить flood/двойные посты)
    if POST_IN_PROGRESS.locked():
        await bot.send_message(callback_query.from_user.id, "⏳ Публикация уже выполняется. Подожди пару секунд.")
        return

    async with POST_IN_PROGRESS:
        record = await get_next_draft_record()
        if not record:
            await bot.send_message(callback_query.from_user.id, "Нет черновиков для публикации.")
            return

        record_id = record.get("id")
        fields = record.get("fields", {})

        title = extract_title(fields)
        caption = extract_caption(fields) or title
        web_url = extract_web_url(fields)
        media_url = extract_media_url(fields)

        try:
            if web_url:
                text = f"{caption}\n\n{web_url}" if caption else web_url
                await safe_send_to_channel(lambda: bot.send_message(CURRENT_CHANNEL_ID, text))
            elif media_url:
                await safe_send_to_channel(lambda: bot.send_photo(CURRENT_CHANNEL_ID, media_url, caption=caption))
            else:
                await safe_send_to_channel(lambda: bot.send_message(CURRENT_CHANNEL_ID, caption or "Без текста"))
        except Exception as e:
            logger.exception("Ошибка отправки в канал: %s", e)
            # вернуть запись обратно в кэш (чтобы повторить)
            DRAFT_CACHE.insert(0, record)
            await bot.send_message(callback_query.from_user.id, "❌ Ошибка публикации (попробуй ещё раз).")
            return

        # Обновляем статус записи в Airtable (1 запрос PATCH)
        try:
            await airtable_patch_record(record_id, {F_STATUS: STATUS_PUBLISHED})
        except Exception as e:
            logger.exception("Не удалось обновить статус в Airtable: %s", e)

        await bot.send_message(callback_query.from_user.id, f"✅ Опубликован: {caption}", reply_markup=main_menu())

# ----------------------------
# Button: start message to channel
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "start_channel")
@admin_only
async def callback_start_channel(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    try:
        await safe_send_to_channel(
            lambda: bot.send_message(
                CURRENT_CHANNEL_ID,
                "👋 Начинаем!\nТут будут публиковаться материалы и фото.\nИх можно будет изучить самостоятельно даже после экскурсии."
            )
        )
        # На старте можно один раз прогреть кэш (экономит запросы дальше)
        await refresh_draft_cache(full_fields=True)
        await bot.send_message(callback_query.from_user.id, "✅ Стартовое сообщение отправлено.", reply_markup=main_menu())
    except Exception as e:
        logger.exception("Ошибка стартового сообщения: %s", e)
        await bot.send_message(callback_query.from_user.id, "❌ Не удалось отправить стартовое сообщение.")

# ----------------------------
# Reset statuses (Published -> Draft)
# ----------------------------
async def reset_published_to_draft() -> int:
    """
    Возвращает количество сброшенных записей.
    Запросов: 1..N на получение (пагинация) + batch PATCH (по 10/запрос).
    """
    # получаем опубликованные
    records: List[dict] = []
    offset = None
    while True:
        params = {
            "filterByFormula": f"{{{F_STATUS}}}='{STATUS_PUBLISHED}'",
            "pageSize": 100,
        }
        params["fields[0]"] = F_STATUS
        if offset:
            params["offset"] = offset

        data = await airtable_get(params)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    if not records:
        return 0

    updates = [(r["id"], {F_STATUS: STATUS_DRAFT}) for r in records]
    await airtable_batch_patch(updates)
    return len(updates)

@dp.callback_query_handler(lambda c: c.data == "reset_status")
@admin_only
async def callback_reset_status(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, "Начинаю сброс...")

    try:
        count = await reset_published_to_draft()
        invalidate_cache()
        await bot.send_message(callback_query.from_user.id, f"♻️ Сброшено {count} записей.", reply_markup=main_menu())
    except Exception as e:
        logger.exception("Ошибка сброса статусов: %s", e)
        await bot.send_message(callback_query.from_user.id, "❌ Ошибка сброса статусов.")

# ----------------------------
# Finish: send message + reset + channel back to default
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "finish_channel")
@admin_only
async def callback_finish(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, "Завершаю...")

    # 1) завершающее сообщение
    try:
        await safe_send_to_channel(
            lambda: bot.send_message(
                CURRENT_CHANNEL_ID,
                "🔔 На этом все, спасибо что были с нами!\nБлагодарность - приветствуется\n+79163150546 (Альфа-банк) 🅰️\n\nКанал о пешеходных прогулках и интересных местах Небанально.Москва - @NebanalnoMsk."
            )
        )
    except Exception as e:
        logger.exception("Ошибка завершающего сообщения: %s", e)

    # 2) сброс статусов
    try:
        count = await reset_published_to_draft()
        invalidate_cache()
    except Exception as e:
        logger.exception("Ошибка сброса статусов при завершении: %s", e)
        count = 0

    # 3) канал обратно на дефолт
    global CURRENT_CHANNEL_ID
    CURRENT_CHANNEL_ID = DEFAULT_CHANNEL_ID

    await bot.send_message(
        callback_query.from_user.id,
        f"✅ Завершено. Сброшено {count} записей. Канал возвращён к значению по умолчанию: {DEFAULT_CHANNEL_ID}",
        reply_markup=main_menu()
    )

# ----------------------------
# Set channel (FSM)
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "set_channel")
@admin_only
async def callback_set_channel(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != ADMIN_ID:
        await bot.answer_callback_query(callback_query.id, "⛔ Нет прав.")
        return

    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "📡 Введите канал: @name или -100...")
    await ChannelState.waiting_for_channel.set()

@dp.message_handler(state=ChannelState.waiting_for_channel)
async def process_channel_input(message: types.Message, state: FSMContext):
    global CURRENT_CHANNEL_ID
    new_channel = message.text.strip()

    try:
        chat = await bot.get_chat(new_channel)
    except Exception:
        await message.reply("❌ Бот не может получить доступ к каналу. Проверь @ и что бот — админ.")
        return

    CURRENT_CHANNEL_ID = new_channel
    await message.reply(f"✅ Канал обновлён: {chat.title} ({CURRENT_CHANNEL_ID})", reply_markup=main_menu())
    await state.finish()

# ----------------------------
# Start
# ----------------------------
if __name__ == "__main__":
    logger.info("Запуск бота...")
    executor.start_polling(dp, skip_updates=True)
