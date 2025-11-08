# bot.py
import logging
import os
from asyncio import get_running_loop

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from notion_client import Client
from dotenv import load_dotenv

# ----------------------------
# Загружаем переменные окружения
# ----------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not BOT_TOKEN or not NOTION_TOKEN or not DATABASE_ID or not CHANNEL_ID or not ADMIN_ID:
    print("Ошибка: не заданы все обязательные переменные (BOT_TOKEN, NOTION_TOKEN, DATABASE_ID, CHANNEL_ID, ADMIN_ID).")
    exit(1)

# ----------------------------
# Логирование
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Инициализация бота и Notion
# ----------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
notion = Client(auth=NOTION_TOKEN)

# ----------------------------
# FSM для смены канала
# ----------------------------
class ChannelState(StatesGroup):
    waiting_for_channel = State()

# ----------------------------
# Вспомогательные функции
# ----------------------------
def extract_title(properties):
    for key in ("Название", "Name", "Title"):
        val = properties.get(key)
        if val and val.get("title"):
            items = val["title"]
            if items:
                return "".join([t.get("plain_text","") for t in items])
    return "Без названия"

def extract_caption(properties):
    for key in ("Подпись", "Caption", "Description"):
        val = properties.get(key)
        if val and val.get("rich_text"):
            items = val["rich_text"]
            if items:
                return "".join([t.get("plain_text","") for t in items])
    return ""

def extract_image_url(properties):
    for key in ("Фото", "Image", "Files", "File"):
        val = properties.get(key)
        if not val:
            continue
        files = val.get("files") or val.get("file") or []
        if files:
            f = files[0]
            if "external" in f and f["external"].get("url"):
                return f["external"]["url"]
            if "file" in f and f["file"].get("url"):
                return f["file"]["url"]
    return None

async def fetch_first_draft():
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Черновик"}},
            sorts=[{"property":"Порядок","direction":"ascending"}],
            page_size=1
        )
        results = resp.get("results", [])
        if not results:
            return None, None
        page = results[0]
        return page["id"], page["properties"]
    except Exception as e:
        logger.exception("Ошибка запроса к Notion: %s", e)
        return None, None

async def fetch_all_pages():
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            page_size=100
        )
        return resp.get("results", [])
    except Exception as e:
        logger.exception("Ошибка получения страниц: %s", e)
        return []

# ----------------------------
# Главное меню
# ----------------------------
def main_menu():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("🚀 Опубликовать >>", callback_data="post"),
    )
    keyboard.add(
        types.InlineKeyboardButton("📡 Канал", callback_data="set_channel"),
        types.InlineKeyboardButton("📋 Черновики", callback_data="list"),
        types.InlineKeyboardButton("🔁 Сброс статуса", callback_data="reset_status"),
    )
    keyboard.add(
        types.InlineKeyboardButton("🟢 Старт", callback_data="start_channel"),
        types.InlineKeyboardButton("🟥 Завершение", callback_data="finish_channel"),
    )
    return keyboard

# ----------------------------
# Обработчики команд
# ----------------------------
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.answer("👋 Готов. Выбери действие:", reply_markup=main_menu())

# ----------------------------
# Кнопки
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "post")
async def callback_post(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    page_id, properties = await fetch_first_draft()
    if not page_id:
        await bot.send_message(callback_query.from_user.id, "Нет черновиков для публикации.")
        return

    title = extract_title(properties)
    caption = extract_caption(properties) or title
    image_url = extract_image_url(properties)

    try:
        if image_url:
            await bot.send_photo(CHANNEL_ID, image_url, caption=caption)
        else:
            await bot.send_message(CHANNEL_ID, caption)
    except Exception as e:
        logger.exception("Ошибка отправки в канал: %s", e)
        await bot.send_message(callback_query.from_user.id, "❌ Ошибка публикации.")
        return

    try:
        notion.pages.update(page_id=page_id, properties={"Статус": {"select": {"name": "Опубликовано"}}})
    except Exception as e:
        logger.exception("Не удалось обновить статус в Notion: %s", e)

    await bot.send_message(callback_query.from_user.id, f"✅ Опубликован: {caption}")

@dp.callback_query_handler(lambda c: c.data == "list")
async def callback_list(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Черновик"}},
            page_size=20
        )
        results = resp.get("results", [])
        if not results:
            await bot.send_message(callback_query.from_user.id, "Нет черновиков.")
            return
        lines = [f"{i+1}. {extract_title(p['properties'])}" for i,p in enumerate(results)]
        await bot.send_message(callback_query.from_user.id, "Черновики:\n" + "\n".join(lines))
    except Exception as e:
        logger.exception("Ошибка получения списка: %s", e)
        await bot.send_message(callback_query.from_user.id, "Ошибка при получении списка.")

@dp.callback_query_handler(lambda c: c.data == "start_channel")
async def callback_start(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    try:
        await bot.send_message(CHANNEL_ID, "👋 Начинаем!\nТут будут публиковаться материалы и фото.\nИх можно будет изучить самостоятельно после экскурсии, даже задать вопросы.")
    except Exception as e:
        logger.exception("Ошибка публикации стартового сообщения: %s", e)
        await bot.send_message(callback_query.from_user.id, "❌ Не удалось отправить стартовое сообщение.")
        return
    await bot.send_message(callback_query.from_user.id, "✅ Стартовое сообщение отправлено.", reply_markup=main_menu())

# ----------------------------
# Завершение работы
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "finish_channel")
async def callback_finish(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, "Выполняю завершение...")

    loop = get_running_loop()

    # 1️⃣ Отправляем завершающее сообщение в канал
    try:
        await bot.send_message(
            CHANNEL_ID,
            "🔔 На этом все, спасибо что были с нами!\nБлагодарность - приветствуется\n+79163150546 (Альфа-банк) 🅰️"
        )
    except Exception as e:
        logger.exception("Ошибка отправки сообщения в канал при завершении: %s", e)
        await bot.send_message(
            callback_query.from_user.id,
            "❌ Ошибка отправки сообщения в канал."
        )

    # 2️⃣ Сбрасываем все опубликованные записи
    pages = await fetch_all_pages()
    count = 0
    for page in pages:
        props = page.get("properties", {})
        status = None
        status_prop = props.get("Статус")

        if status_prop and status_prop.get("type") == "select" and status_prop.get("select"):
            status = status_prop["select"].get("name")

        if status == "Опубликовано":
            try:
                await loop.run_in_executor(
                    None,
                    lambda: notion.pages.update(
                        page_id=page["id"],
                        properties={"Статус": {"select": {"name": "Черновик"}}}
                    )
                )
                count += 1
            except Exception as e:
                logger.exception("Не удалось сбросить статус страницы %s: %s", page["id"], e)

    # 3️⃣ Сообщение пользователю о результате
    await bot.send_message(
        callback_query.from_user.id,
        f"✅ Завершено. Сброшено {count} записей в черновик.",
        reply_markup=main_menu()
    )

@dp.callback_query_handler(lambda c: c.data == "reset_status")
async def callback_reset_status(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, "Начинаю сброс...")

    loop = get_running_loop()
    pages = await fetch_all_pages()  # Получаем все страницы
    count = 0

    for page in pages:
        props = page.get("properties", {})
        status = None
        status_prop = props.get("Статус")

        if status_prop and status_prop.get("type") == "select" and status_prop.get("select"):
            status = status_prop["select"].get("name")

        if status == "Опубликовано":
            try:
                await loop.run_in_executor(
                    None,
                    lambda: notion.pages.update(
                        page_id=page["id"],
                        properties={"Статус": {"select": {"name": "Черновик"}}}
                    )
                )
                count += 1
            except Exception as e:
                logger.exception("Не удалось сбросить статус страницы %s: %s", page["id"], e)

    await bot.send_message(
        callback_query.from_user.id,
        f"♻️ Сброшено {count} записей.",
        reply_markup=main_menu()
    )

# ----------------------------
# Смена канала
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "set_channel")
async def callback_set_channel(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != ADMIN_ID:
        await bot.answer_callback_query(callback_query.id, "⛔ У вас нет прав.")
        return
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, "📡 Введите имя канала (@имя или -100...)")
    await ChannelState.waiting_for_channel.set()

@dp.message_handler(state=ChannelState.waiting_for_channel)
async def process_channel_input(message: types.Message, state: FSMContext):
    global CHANNEL_ID
    new_channel = message.text.strip()
    try:
        chat = await bot.get_chat(new_channel)
    except Exception:
        await message.reply("❌ Ошибка: бот не может получить доступ к этому каналу.")
        return
    CHANNEL_ID = new_channel
    # Сохранять в .env можно через отдельную функцию, если нужно
    await message.reply(f"✅ Канал обновлён: {chat.title} ({CHANNEL_ID})", reply_markup=main_menu())
    await state.finish()

# ----------------------------
# Старт бота
# ----------------------------
if __name__ == "__main__":
    logger.info("Запуск бота...")
    executor.start_polling(dp, skip_updates=True)
