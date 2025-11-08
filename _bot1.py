import logging
import os
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from notion_client import Client
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage

class ChannelState(StatesGroup):
    waiting_for_channel = State()

storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Укажи свой Telegram ID (узнать можно через бота @userinfobot)
ADMIN_ID = 160423286  # замените на свой ID

# Вверху файла (импорты)
import os
from pathlib import Path
from dotenv import load_dotenv

# Путь до .env рядом с bot.py
DOTENV_PATH = Path(__file__).parent / ".env"

# Загружаем .env при старте (если есть)
if DOTENV_PATH.exists():
    load_dotenv(dotenv_path=DOTENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# ----------------------------
# Настройки
# ----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN") or "ВСТАВЬ_СВОЙ_BOT_TOKEN"
NOTION_TOKEN = os.getenv("NOTION_TOKEN") or "ВСТАВЬ_СВОЙ_NOTION_TOKEN"
DATABASE_ID = os.getenv("DATABASE_ID") or "ВСТАВЬ_СВОЙ_DATABASE_ID"
CHANNEL_ID = os.getenv("CHANNEL_ID") or "@Arbat1125"

# Проверка токенов
if not BOT_TOKEN or not NOTION_TOKEN or not DATABASE_ID or not CHANNEL_ID:
    print("❌ Ошибка: не заданы переменные окружения (BOT_TOKEN, NOTION_TOKEN, DATABASE_ID, CHANNEL_ID).")
    exit(1)

# ----------------------------
# Настройка логов
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
notion = Client(auth=NOTION_TOKEN)

# ----------------------------
# Клавиатура меню
# ----------------------------
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Главное меню ---
def get_main_menu():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📋 Список", callback_data="list"),
        InlineKeyboardButton("🚀 Опубликовать", callback_data="post"),
    )
    keyboard.add(
        InlineKeyboardButton("🟢 Старт", callback_data="start"),
        InlineKeyboardButton("🔴 Завершение", callback_data="finish"),
    )
    keyboard.add(
        InlineKeyboardButton("⚙️ Сменить канал", callback_data="change_channel"),
    )
    return keyboard


@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Готов."
        "Выбери действие:",
        reply_markup=get_main_menu()
    )

# ----------------------------
# Вспомогательные функции
# ----------------------------

def update_env_variable(key: str, value: str, dotenv_path: Path = DOTENV_PATH):
    """
    Обновляет (или добавляет) ключ в .env рядом с bot.py и в os.environ.
    Гарантирует, что файл записывается в той же директории.
    """
    value = value.strip()
    lines = []
    if dotenv_path.exists():
        lines = dotenv_path.read_text(encoding="utf-8").splitlines()
    new_lines = []
    found = False
    for line in lines:
        # пропускаем пустые строки и комментарии корректно
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"{key}={value}")
    # Записываем с окончанием строки
    dotenv_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    # Обновляем окружение в текущем процессе
    os.environ[key] = value

def extract_title(properties):
    for key in ("Название", "Name", "Title"):
        val = properties.get(key)
        if val and val.get("title"):
            return "".join([t.get("plain_text", "") for t in val["title"]])
    return "Без названия"

def extract_caption(properties):
    for key in ("Подпись", "Caption", "Description"):
        val = properties.get(key)
        if val and val.get("rich_text"):
            return "".join([t.get("plain_text", "") for t in val["rich_text"]])
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
    for key in ("URL", "Ссылка"):
        val = properties.get(key)
        if val and val.get("url"):
            return val["url"]
    return None

def get_order_value(properties):
    """Получаем значение поля 'Порядок' (если есть)"""
    val = properties.get("Порядок")
    if val and val.get("number") is not None:
        return val["number"]
    return 0

async def fetch_first_draft():
    """Находим первую запись со статусом 'Черновик' по полю 'Порядок'"""
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Черновик"}},
            sorts=[{"property": "Порядок", "direction": "ascending"}],
            page_size=1
        )
    except Exception as e:
        logger.exception("Ошибка запроса к Notion: %s", e)
        return None, None

    results = resp.get("results", [])
    if not results:
        return None, None
    page = results[0]
    return page["id"], page["properties"]

# ----------------------------
# Обработчики
# ----------------------------
@dp.callback_query_handler(lambda c: c.data == "start")
async def callback_start(callback_query):
    await bot.send_message(
        callback_query.from_user.id,
        "👋 Привет! Я готов публиковать посты из Notion.\n"
        "Пожалуйста, укажи канал командой /set_channel <ID> или выбери из меню.",
        reply_markup=get_main_menu()
    )
    await bot.answer_callback_query(callback_query.id)

@dp.callback_query_handler(lambda c: c.data == "finish")
async def callback_finish(callback_query):
    try:
        # Отправляем финальное сообщение в канал
        await bot.send_message(CHANNEL_ID, "🔚 Публикация завершена. Спасибо за работу!")

        # Сбрасываем все "Опубликовано" в "Черновик"
        results = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Опубликовано"}}
        ).get("results", [])

        for page in results:
            notion.pages.update(
                page_id=page["id"],
                properties={"Статус": {"select": {"name": "Черновик"}}}
            )

        await bot.send_message(
            callback_query.from_user.id,
            f"✅ Завершено. {len(results)} пост(ов) сброшено в статус 'Черновик'."
        )
    except Exception as e:
        logger.exception("Ошибка при завершении публикации: %s", e)
        await bot.send_message(callback_query.from_user.id, "⚠️ Ошибка при завершении публикации.")

    await bot.answer_callback_query(callback_query.id)

@dp.callback_query_handler(lambda c: c.data == "list")
async def callback_list(callback_query):
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Черновик"}},
            sorts=[{"property": "Порядок", "direction": "ascending"}],
            page_size=50
        )
    except Exception as e:
        logger.exception("Ошибка получения списка из Notion: %s", e)
        await bot.answer_callback_query(callback_query.id, "Ошибка при запросе к Notion.")
        return

    results = resp.get("results", [])
    if not results:
        await bot.answer_callback_query(callback_query.id, "Нет черновиков.")
        return

    lines = []
    for i, page in enumerate(results, start=1):
        props = page["properties"]
        title = extract_title(props)
        order = get_order_value(props)
        lines.append(f"{i}. {title} (Порядок: {order})")

    await bot.send_message(callback_query.from_user.id, "📝 Черновики:\n" + "\n".join(lines))

@dp.callback_query_handler(lambda c: c.data == "post")
async def callback_post(callback_query):
    page_id, properties = await fetch_first_draft()
    if not page_id:
        await bot.send_message(callback_query.from_user.id, "⚠️ Нет черновиков для публикации.")
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
        await bot.send_message(callback_query.from_user.id, "❌ Ошибка при публикации в канал.")
        return

    try:
        notion.pages.update(
            page_id=page_id,
            properties={"Статус": {"select": {"name": "Опубликовано"}}}
        )
    except Exception as e:
        logger.exception("Не удалось обновить статус в Notion: %s", e)
        await bot.send_message(callback_query.from_user.id, f"⚠️ Пост опубликован: {caption}\nНо не удалось обновить статус в Notion.")
        return

    # 💬 Теперь отправляем сообщение с подписью из Notion
    await bot.send_message(callback_query.from_user.id, f"✅ Опубликовано: {caption}")

    # Убираем индикатор «часики» с кнопки
    await bot.answer_callback_query(callback_query.id)

@dp.callback_query_handler(lambda c: c.data == "reset")
async def callback_reset(callback_query):
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Статус", "select": {"equals": "Опубликовано"}},
            page_size=100
        )
        results = resp.get("results", [])
        if not results:
            await bot.answer_callback_query(callback_query.id, "Нет опубликованных записей для сброса.")
            return

        for page in results:
            notion.pages.update(
                page_id=page["id"],
                properties={"Статус": {"select": {"name": "Черновик"}}}
            )

        await bot.answer_callback_query(callback_query.id, f"🔄 Сброшено {len(results)} записей в 'Черновик'.")
    except Exception as e:
        logger.exception("Ошибка при сбросе статусов: %s", e)
        await bot.answer_callback_query(callback_query.id, "Ошибка при сбросе статусов.")

@dp.message_handler(state=ChannelState.waiting_for_channel)
async def process_channel_input(message: types.Message, state: FSMContext):
    new_channel = message.text.strip()

    # Проверим, что бот имеет доступ к этому каналу
    try:
        chat = await bot.get_chat(new_channel)
    except Exception:
        await message.reply(
            "❌ Ошибка: бот не может получить доступ к этому каналу.\n"
            "Проверь, что он добавлен как администратор."
        )
        return

    global CHANNEL_ID
    CHANNEL_ID = new_channel
    update_env_variable("CHANNEL_ID", CHANNEL_ID)

    await message.reply(f"✅ Канал обновлён: {chat.title} ({CHANNEL_ID})")
    await state.finish()


# ------------------------------------------------
# Обработчик команды /setchannel
@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    # только админ может менять канал
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ У вас нет прав для изменения канала.")
        return

    # Ожидаем: /setchannel @channel или /setchannel -1001234567890
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Использование: /setchannel @имя_канала  (или /setchannel -1001234567890)")
        return

    new_channel = parts[1].strip()
    # Удалим возможные окруж. символы
    new_channel = new_channel.strip('"').strip("'")

    # Проверим, что бот действительно может "видеть" этот чат (позволяет поймать ошибки заранее)
    try:
        # get_chat валидирует канал/чат. Если канал приватный и бот не в нём -> ошибка
        await bot.get_chat(new_channel)
    except Exception as e:
        logger.exception("get_chat failed for %s: %s", new_channel, e)
        await message.reply(
            "Не удалось найти указанный канал/чат или у бота нет доступа.\n"
            "Убедитесь, что:\n"
            "• Вы правильно указали @username или числовой ID (начинается с -100...)\n"
            "• Бот добавлен в канал и имеет права отправки сообщений."
        )
        return

    # обновляем переменную в памяти и в .env
    global CHANNEL_ID
    CHANNEL_ID = new_channel
    update_env_variable("CHANNEL_ID", CHANNEL_ID)

    await message.reply(f"✅ Канал для публикации обновлён: {CHANNEL_ID}")

# ----------------------------
# Запуск бота
# ----------------------------
if __name__ == "__main__":
    logger.info("🚀 Бот запущен!")
    executor.start_polling(dp, skip_updates=True)
