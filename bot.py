import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.utils.exceptions import RetryAfter, TelegramAPIError
from dotenv import load_dotenv

# ---------------- ENV ----------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE", "Posts")

DEFAULT_CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not all([BOT_TOKEN, AIRTABLE_TOKEN, AIRTABLE_BASE_ID, DEFAULT_CHANNEL_ID, ADMIN_ID]):
    raise SystemExit("❌ Не заданы обязательные ENV переменные")

AIRTABLE_API_BASE = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- Bot ----------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

CURRENT_CHANNEL_ID = DEFAULT_CHANNEL_ID
POST_LOCK = asyncio.Lock()

# ---------------- FSM ----------------
class ChannelState(StatesGroup):
    waiting_for_channel = State()

# ---------------- Airtable ----------------
async def airtable_request(method: str, url: str, *, params=None, json_body=None):
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, headers=headers, params=params, json=json_body) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise Exception(f"Airtable error {resp.status}: {text}")
            return await resp.json()

# ---------------- Helpers ----------------
def admin_only(func):
    async def wrapper(*args, **kwargs):
        callback_query = args[0]

        if callback_query.from_user.id != ADMIN_ID:
            await callback_query.answer("⛔ Нет прав")
            return

        return await func(*args, **kwargs)

    return wrapper

async def safe_send(send_coro):
    for _ in range(5):
        try:
            return await send_coro()
        except RetryAfter as e:
            await asyncio.sleep(e.timeout)
        except TelegramAPIError:
            await asyncio.sleep(2)
    raise RuntimeError("Telegram send failed")

# ---------------- UI ----------------
def menu():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🚀 Опубликовать", callback_data="post"))
    kb.add(types.InlineKeyboardButton("📡 Канал", callback_data="set_channel"))
    kb.add(types.InlineKeyboardButton("🔁 Сброс", callback_data="reset"))
    kb.add(types.InlineKeyboardButton("🟥 Завершить", callback_data="finish"))
    return kb

# ---------------- Commands ----------------
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Готов к работе", reply_markup=menu())

# ---------------- Publish ----------------
@dp.callback_query_handler(lambda c: c.data == "post")
@admin_only
async def publish(callback_query: types.CallbackQuery):
    async with POST_LOCK:
        params = {
            "filterByFormula": "{Status}='Draft'",
            "maxRecords": 1,
        }
        data = await airtable_request("GET", AIRTABLE_API_BASE, params=params)
        records = data.get("records", [])
        if not records:
            await bot.send_message(callback_query.from_user.id, "Нет черновиков")
            return

        record = records[0]
        record_id = record["id"]
        fields = record["fields"]

        text = fields.get("Caption") or fields.get("Title") or "Без текста"

        await safe_send(lambda: bot.send_message(CURRENT_CHANNEL_ID, text))

        await airtable_request(
            "PATCH",
            f"{AIRTABLE_API_BASE}/{record_id}",
            json_body={"fields": {"Status": "Published"}},
        )

        await bot.send_message(callback_query.from_user.id, "✅ Опубликовано", reply_markup=menu())

# ---------------- Reset ----------------
@dp.callback_query_handler(lambda c: c.data == "reset")
@admin_only
async def reset(callback_query: types.CallbackQuery):
    params = {
        "filterByFormula": "{Status}='Published'",
    }
    data = await airtable_request("GET", AIRTABLE_API_BASE, params=params)
    records = data.get("records", [])

    for r in records:
        await airtable_request(
            "PATCH",
            f"{AIRTABLE_API_BASE}/{r['id']}",
            json_body={"fields": {"Status": "Draft"}},
        )

    await bot.send_message(callback_query.from_user.id, f"♻️ Сброшено {len(records)}", reply_markup=menu())

# ---------------- Set Channel ----------------
@dp.callback_query_handler(lambda c: c.data == "set_channel")
@admin_only
async def set_channel(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, "Введите @channel или -100...")
    await ChannelState.waiting_for_channel.set()

@dp.message_handler(state=ChannelState.waiting_for_channel)
async def process_channel(message: types.Message, state: FSMContext):
    global CURRENT_CHANNEL_ID
    CURRENT_CHANNEL_ID = message.text.strip()
    await message.answer(f"Канал обновлён: {CURRENT_CHANNEL_ID}", reply_markup=menu())
    await state.finish()

# ---------------- Finish ----------------
@dp.callback_query_handler(lambda c: c.data == "finish")
@admin_only
async def finish(callback_query: types.CallbackQuery):
    await safe_send(lambda: bot.send_message(CURRENT_CHANNEL_ID, "Спасибо за участие!"))
    await bot.send_message(callback_query.from_user.id, "Завершено", reply_markup=menu())

# ---------------- Run ----------------
if __name__ == "__main__":
    logger.info("Bot started")
    executor.start_polling(dp, skip_updates=True)