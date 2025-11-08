import logging
import os
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from notion_client import Client
from dotenv import load_dotenv

# ----------------------------
# Настройка окружения
# ----------------------------
load_dotenv()  # Загружает переменные из .env файла

BOT_TOKEN = os.getenv("BOT_TOKEN")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# ----------------------------
# Проверка конфигурации
# ----------------------------
if not BOT_TOKEN or not NOTION_TOKEN or not DATABASE_ID or not CHANNEL_ID:
    print("❌ Ошибка: не заданы все обязательные переменные (BOT_TOKEN, NOTION_TOKEN, DATABASE_ID, CHANNEL_ID).")
    print("Создай файл .env рядом с bot.py и добавь туда:")
    print("""
BOT_TOKEN=твой_бот_токен
NOTION_TOKEN=твой_ноушен_токен
DATABASE_ID=ID_твоей_базы
CHANNEL_ID=@имя_твоего_канала
    """)
    exit(1)

# ----------------------------
# Логирование
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Инициализация клиентов
# ----------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
notion = Client(auth=NOTION_TOKEN)

# ----------------------------
# Вспомогательные функции
# ----------------------------
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

async def fetch_first_draft():
    """
    Возвращает (page_id, properties) самого первого черновика в базе Notion,
    отсортированного по пользовательскому полю "Порядок".
    """
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,
            filter={
                "property": "Статус",
                "select": {"equals": "Черновик"}
            },
            sorts=[{
                "property": "Порядок",      # Сортировка по пользовательскому полю "Порядок"
                "direction": "ascending"    # Самая маленькая цифра "Порядок" будет первой
            }],
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
# Обработчики команд
# ----------------------------
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.reply("👋 Привет! Я публикую черновики из Notion в канал.\nИспользуй команды:\n\n/list — показать черновики\n/post — опубликовать следующий черновик")

@dp.message_handler(commands=["list"])
async def cmd_list(message: types.Message):
    try:
        resp = notion.databases.query(
            database_id=DATABASE_ID,   
            filter={
                "property": "Статус",
                "select": {"equals": "Черновик"}
            },
            page_size=10
        )
    except Exception as e:
        logger.exception("Ошибка получения списка из Notion: %s", e)
        await message.reply("⚠️ Ошибка при запросе к Notion.")
        return

    results = resp.get("results", [])
    if not results:
        await message.reply("Нет черновиков в Notion.")
        return

    lines = []
    for i, page in enumerate(results, start=1):
        props = page["properties"]
        title = extract_title(props)
        lines.append(f"{i}. {title}")

    await message.reply("📝 Черновики:\n" + "\n".join(lines))

@dp.message_handler(commands=["post"])
async def cmd_post(message: types.Message):
    page_id, properties = await fetch_first_draft()
    if not page_id:
        await message.reply("Нет черновиков для публикации.")
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
        logger.exception("Ошибка при отправке в канал: %s", e)
        await message.reply("⚠️ Ошибка при публикации в канал.")
        return

    try:
        notion.pages.update(
            page_id=page_id,
            properties={"Статус": {"select": {"name": "Опубликовано"}}}
        )
    except Exception as e:
        logger.exception("Не удалось обновить статус в Notion: %s", e)
        await message.reply("✅ Пост опубликован, но не удалось обновить статус в Notion.")
        return

    await message.reply("✅ Пост опубликован и помечен как 'Опубликовано' в Notion.")

# ----------------------------
# Точка входа
# ----------------------------
if __name__ == "__main__":
    logger.info("🚀 Бот запущен...")
    executor.start_polling(dp, skip_updates=True)
