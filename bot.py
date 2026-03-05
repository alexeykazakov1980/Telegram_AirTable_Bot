import asyncio
import json
import logging
import os
import time
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import requests
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from dotenv import load_dotenv

# ----------------------------
# ENV (общие)
# ----------------------------
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Posts")  # у всех одинаковый: Posts

if not ADMIN_ID:
    raise SystemExit("Ошибка: не задана переменная ADMIN_ID")

BOTS_CONFIG_RAW = os.getenv("BOTS_CONFIG_JSON", "").strip()
if not BOTS_CONFIG_RAW:
    raise SystemExit("Ошибка: не задана переменная BOTS_CONFIG_JSON")

try:
    BOTS_CONFIG = json.loads(BOTS_CONFIG_RAW)
except json.JSONDecodeError as e:
    raise SystemExit(f"Ошибка: BOTS_CONFIG_JSON невалидный JSON: {e}")

if not isinstance(BOTS_CONFIG, list) or not BOTS_CONFIG:
    raise SystemExit("Ошибка: BOTS_CONFIG_JSON должен быть непустым JSON-массивом конфигов")

# ----------------------------
# Admin-only decorator (aiogram 2.x safe)
# ----------------------------
def admin_only(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        obj = args[0] if args else None  # Message или CallbackQuery
        user_id = getattr(getattr(obj, "from_user", None), "id", None)

        if user_id != ADMIN_ID:
            try:
                if isinstance(obj, types.CallbackQuery):
                    await obj.answer("⛔ Нет прав.", show_alert=False)
                elif isinstance(obj, types.Message):
                    await obj.reply("⛔ Нет прав.")
            except Exception:
                pass
            return

        return await func(*args, **kwargs)

    return wrapper

# ----------------------------
# Factory: create bot instance
# ----------------------------
def create_bot_instance(cfg: dict) -> Dispatcher:
    # обязательные ключи конфига
    name = cfg.get("name") or "bot"
    token = cfg.get("token")
    airtable_token = cfg.get("airtable_token") or cfg.get("airtable_api_key")
    airtable_base_id = cfg.get("airtable_base_id")
    channel_id = cfg.get("channel_id")

    if not token:
        raise SystemExit(f"[{name}] Ошибка: не задан token")
    if not airtable_token:
        raise SystemExit(f"[{name}] Ошибка: не задан airtable_token")
    if not airtable_base_id:
        raise SystemExit(f"[{name}] Ошибка: не задан airtable_base_id")
    if not channel_id:
        raise SystemExit(f"[{name}] Ошибка: не задан channel_id")

    bot = Bot(token=token)
    dp = Dispatcher(bot, storage=MemoryStorage())

    register_handlers(
        dp=dp,
        bot=bot,
        bot_name=name,
        airtable_token=airtable_token,
        airtable_base_id=airtable_base_id,
        airtable_table_name=AIRTABLE_TABLE_NAME,
        default_channel_id=str(channel_id),
    )

    return dp

# ----------------------------
# Register handlers for ONE bot (all state is per-bot)
# ----------------------------
def register_handlers(
    *,
    dp: Dispatcher,
    bot: Bot,
    bot_name: str,
    airtable_token: str,
    airtable_base_id: str,
    airtable_table_name: str,
    default_channel_id: str,
) -> None:
    # ----------------------------
    # Airtable schema (поля)
    # ----------------------------
    F_TITLE = os.getenv("F_TITLE", "Title")
    F_CAPTION = os.getenv("F_CAPTION", "Caption")
    F_STATUS = os.getenv("F_STATUS", "Status")
    F_ORDER = os.getenv("F_ORDER", "Order")
    F_MEDIA = os.getenv("F_MEDIA", "Media")   # Attachment
    F_WEB = os.getenv("F_WEB", "Web")         # URL

    STATUS_DRAFT = os.getenv("STATUS_DRAFT", "Draft")
    STATUS_PUBLISHED = os.getenv("STATUS_PUBLISHED", "Published")

    AIRTABLE_API_BASE = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}"

    # ----------------------------
    # Per-bot runtime state
    # ----------------------------
    CURRENT_CHANNEL_ID = default_channel_id

    class ChannelState(StatesGroup):
        waiting_for_channel = State()

    # ----------------------------
    # Airtable HTTP helpers (per-bot session)
    # ----------------------------
    _session = requests.Session()
    _headers = {
        "Authorization": f"Bearer {airtable_token}",
        "Content-Type": "application/json",
    }

    def _airtable_request(method: str, url: str, *, params: Optional[dict] = None, json_body: Optional[dict] = None) -> dict:
        backoff = 1.0
        last_exc: Optional[Exception] = None

        for _attempt in range(6):
            try:
                r = _session.request(method, url, headers=_headers, params=params, json=json_body, timeout=30)
                if r.status_code == 429:
                    retry_after = float(r.headers.get("Retry-After", "1"))
                    time.sleep(max(1.0, retry_after))
                    continue
                if 500 <= r.status_code <= 599:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 10)
                    continue

                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_exc = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)

        raise last_exc or RuntimeError("Airtable request failed")

    async def airtable_get(params: dict) -> dict:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: _airtable_request("GET", AIRTABLE_API_BASE, params=params))

    async def airtable_patch_record(record_id: str, fields: dict) -> dict:
        loop = asyncio.get_running_loop()
        url = f"{AIRTABLE_API_BASE}/{record_id}"
        body = {"fields": fields}
        return await loop.run_in_executor(None, lambda: _airtable_request("PATCH", url, json_body=body))

    async def airtable_batch_patch(records: List[Tuple[str, dict]]) -> None:
        loop = asyncio.get_running_loop()
        for i in range(0, len(records), 10):
            chunk = records[i:i + 10]
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
        v = fields.get(F_MEDIA)
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, dict) and isinstance(first.get("url"), str):
                return first["url"]
        return None

    # ----------------------------
    # Caching to reduce Airtable API calls (per-bot)
    # ----------------------------
    DRAFT_CACHE: List[dict] = []
    DRAFT_CACHE_TS: float = 0.0
    DRAFT_CACHE_TTL_SEC = int(os.getenv("DRAFT_CACHE_TTL_SEC", "120"))

    def _draft_cache_is_fresh() -> bool:
        return bool(DRAFT_CACHE) and (time.time() - DRAFT_CACHE_TS) < DRAFT_CACHE_TTL_SEC

    async def refresh_draft_cache(*, full_fields: bool = True) -> List[dict]:
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
            for i, f in enumerate(fields_list):
                params[f"fields[{i}]"] = f
            if offset:
                params["offset"] = offset

            data = await airtable_get(params)
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break

        records.sort(key=lambda r: extract_order(r.get("fields", {})))

        nonlocal DRAFT_CACHE, DRAFT_CACHE_TS
        DRAFT_CACHE = records
        DRAFT_CACHE_TS = time.time()
        return records

    async def get_next_draft_record() -> Optional[dict]:
        if not _draft_cache_is_fresh():
            await refresh_draft_cache(full_fields=True)

        if not DRAFT_CACHE:
            return None

        return DRAFT_CACHE.pop(0)

    def invalidate_cache() -> None:
        nonlocal DRAFT_CACHE, DRAFT_CACHE_TS
        DRAFT_CACHE = []
        DRAFT_CACHE_TS = 0.0

    # ----------------------------
    # Telegram send helpers (per-bot)
    # ----------------------------
    POST_IN_PROGRESS = asyncio.Lock()

    async def safe_send_to_channel(send_coro_factory, *, max_attempts: int = 5) -> None:
        backoff = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                await send_coro_factory()
                return
            except Exception as e:
                # Flood/retry logic simplified (aiogram exceptions are already handled upstream often)
                logger.warning("[%s] send error (attempt %s/%s): %s", bot_name, attempt, max_attempts, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
        raise RuntimeError("send failed after retries")

    # ----------------------------
    # UI (same as old)
    # ----------------------------
    def main_menu() -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(types.InlineKeyboardButton("🚀 Опубликовать >>", callback_data="post"))
        kb.add(
            types.InlineKeyboardButton("📡 Канал", callback_data="set_channel"),
            types.InlineKeyboardButton("🟢 Старт", callback_data="start_channel"),
        )
        kb.add(
            types.InlineKeyboardButton("🔁 Сброс статусов", callback_data="reset_status"),
            types.InlineKeyboardButton("📋 Черновики", callback_data="list"),
        )
        kb.add(types.InlineKeyboardButton("🟥 Завершение", callback_data="finish_channel"))
        return kb

    # ----------------------------
    # Commands
    # ----------------------------
    @dp.message_handler(commands=["start", "help"])
    async def cmd_start(message: types.Message):
        await message.answer("Готов. Выбери действие:", reply_markup=main_menu())

    # ----------------------------
    # Button: list drafts (admin_only)
    # ----------------------------
    @dp.callback_query_handler(lambda c: c.data == "list")
    @admin_only
    async def callback_list(callback_query: types.CallbackQuery):
        await callback_query.answer()

        try:
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
            logger.exception("[%s] Ошибка получения списка из Airtable: %s", bot_name, e)
            await bot.send_message(callback_query.from_user.id, "❌ Ошибка чтения Airtable.")

    # ----------------------------
    # Button: post next (admin_only)
    # ----------------------------
    @dp.callback_query_handler(lambda c: c.data == "post")
    @admin_only
    async def callback_post(callback_query: types.CallbackQuery):
        await callback_query.answer()

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
                logger.exception("[%s] Ошибка отправки в канал: %s", bot_name, e)
                DRAFT_CACHE.insert(0, record)
                await bot.send_message(callback_query.from_user.id, "❌ Ошибка публикации (попробуй ещё раз).")
                return

            try:
                await airtable_patch_record(record_id, {F_STATUS: STATUS_PUBLISHED})
            except Exception as e:
                logger.exception("[%s] Не удалось обновить статус в Airtable: %s", bot_name, e)

            await bot.send_message(callback_query.from_user.id, f"✅ Опубликован: {caption}", reply_markup=main_menu())

    # ----------------------------
    # Button: start message to channel (admin_only)
    # ----------------------------
    @dp.callback_query_handler(lambda c: c.data == "start_channel")
    @admin_only
    async def callback_start_channel(callback_query: types.CallbackQuery):
        await callback_query.answer()
        try:
            await safe_send_to_channel(
                lambda: bot.send_message(
                    CURRENT_CHANNEL_ID,
                    "👋 Начинаем!\nТут будут публиковаться материалы и фото.\nИх можно будет изучить самостоятельно даже после экскурсии."
                )
            )
            await refresh_draft_cache(full_fields=True)
            await bot.send_message(callback_query.from_user.id, "✅ Стартовое сообщение отправлено.", reply_markup=main_menu())
        except Exception as e:
            logger.exception("[%s] Ошибка стартового сообщения: %s", bot_name, e)
            await bot.send_message(callback_query.from_user.id, "❌ Не удалось отправить стартовое сообщение.")

    # ----------------------------
    # Reset statuses (Published -> Draft)
    # ----------------------------
    async def reset_published_to_draft() -> int:
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
        await callback_query.answer("Начинаю сброс...")

        try:
            count = await reset_published_to_draft()
            invalidate_cache()
            await bot.send_message(callback_query.from_user.id, f"♻️ Сброшено {count} записей.", reply_markup=main_menu())
        except Exception as e:
            logger.exception("[%s] Ошибка сброса статусов: %s", bot_name, e)
            await bot.send_message(callback_query.from_user.id, "❌ Ошибка сброса статусов.")

    # ----------------------------
    # Finish: send message + reset + channel back to default
    # ----------------------------
    @dp.callback_query_handler(lambda c: c.data == "finish_channel")
    @admin_only
    async def callback_finish(callback_query: types.CallbackQuery):
        nonlocal CURRENT_CHANNEL_ID
        await callback_query.answer("Завершаю...")

        try:
            await safe_send_to_channel(
                lambda: bot.send_message(
                    CURRENT_CHANNEL_ID,
                    "🔔 На этом все, спасибо что были с нами!\nБлагодарность - приветствуется\n+79163150546 (Альфа-банк) 🅰️\n\nКанал о пешеходных прогулках и интересных местах Небанально.Москва - @NebanalnoMsk."
                )
            )
        except Exception as e:
            logger.exception("[%s] Ошибка завершающего сообщения: %s", bot_name, e)

        try:
            count = await reset_published_to_draft()
            invalidate_cache()
        except Exception as e:
            logger.exception("[%s] Ошибка сброса статусов при завершении: %s", bot_name, e)
            count = 0

        CURRENT_CHANNEL_ID = default_channel_id

        await bot.send_message(
            callback_query.from_user.id,
            f"✅ Завершено. Сброшено {count} записей. Канал возвращён к значению по умолчанию: {default_channel_id}",
            reply_markup=main_menu()
        )

    # ----------------------------
    # Set channel (FSM) (admin_only)
    # ----------------------------
    @dp.callback_query_handler(lambda c: c.data == "set_channel")
    @admin_only
    async def callback_set_channel(callback_query: types.CallbackQuery):
        await callback_query.answer()
        await bot.send_message(callback_query.from_user.id, "📡 Введите канал: @name или -100...")
        await ChannelState.waiting_for_channel.set()

    @dp.message_handler(state=ChannelState.waiting_for_channel)
    @admin_only
    async def process_channel_input(message: types.Message, state: FSMContext):
        nonlocal CURRENT_CHANNEL_ID
        new_channel = message.text.strip()

        try:
            chat = await bot.get_chat(new_channel)
        except Exception:
            await message.reply("❌ Бот не может получить доступ к каналу. Проверь @ и что бот — админ.")
            return

        CURRENT_CHANNEL_ID = new_channel
        await message.reply(f"✅ Канал обновлён: {chat.title} ({CURRENT_CHANNEL_ID})", reply_markup=main_menu())
        await state.finish()

    logger.info("[%s] Handlers registered. Default channel: %s, Airtable base: %s/%s",
                bot_name, default_channel_id, airtable_base_id, airtable_table_name)

# ----------------------------
# Run all bots
# ----------------------------
async def run_all_bots() -> None:
    tasks = []

    for cfg in BOTS_CONFIG:
        name = cfg.get("name") or "bot"
        dp = create_bot_instance(cfg)

        # aiogram 2.x: Dispatcher.start_polling is async
        async def _runner(_dp: Dispatcher, _name: str):
            logger.info("[%s] Starting polling...", _name)
            await _dp.start_polling(skip_updates=True)

        tasks.append(asyncio.create_task(_runner(dp, name)))
        logger.info("[%s] Started", name)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    logger.info("Multi-bot runner starting. Bots: %d", len(BOTS_CONFIG))
    asyncio.run(run_all_bots())