import asyncio
import base64
import logging
import os
from datetime import datetime
from typing import Optional

import aiosqlite
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    CallbackQuery,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from dotenv import load_dotenv
from yookassa import Configuration, Payment

load_dotenv()

# ==================== НАСТРОЙКИ ====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
XAI_API_KEY = os.getenv("XAI_API_KEY")
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
BOT_USERNAME = os.getenv("BOT_USERNAME", "your_bot")

# Режим работы: polling (локально) или webhook (продакшен)
USE_WEBHOOK = os.getenv("USE_WEBHOOK", "false").lower() == "true"
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "")          # https://yourdomain.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8080"))

if not TELEGRAM_TOKEN or not XAI_API_KEY:
    raise ValueError("Укажите TELEGRAM_TOKEN и XAI_API_KEY в файле .env")

if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_SECRET_KEY
    PAYMENTS_ENABLED = True
else:
    PAYMENTS_ENABLED = False

XAI_MODEL = "grok-imagine-image-2.0"
XAI_ENDPOINT = "https://api.x.ai/v1/images/edits"
DB_PATH = "bot_data.db"

# Максимум одновременных генераций
MAX_CONCURRENT_GENERATIONS = 3

# Пакеты
PACKAGES = {
    "pack_1": {"name": "1 генерация", "credits": 1, "price": 49},
    "pack_5": {"name": "5 генераций", "credits": 5, "price": 199},
    "pack_15": {"name": "15 генераций", "credits": 15, "price": 499},
    "pack_50": {"name": "50 генераций", "credits": 50, "price": 1299},
}

# Стили
STYLES = {
    "modern": {
        "name": "🏠 Современный",
        "prompt": (
            "Redesign this room into a modern contemporary interior design. "
            "Keep the exact room layout, walls, windows, doors and architecture unchanged. "
            "Replace all furniture and decor with modern clean-lined pieces, neutral color palette "
            "(beige, white, soft gray, black accents), high-quality materials, excellent natural "
            "and artificial lighting, minimalist style, cozy and elegant atmosphere. "
            "Photorealistic high-quality result."
        ),
    },
    "scandi": {
        "name": "🌲 Скандинавский",
        "prompt": (
            "Redesign this room in Scandinavian style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Use light wood, white and soft pastel colors, natural materials, "
            "simple functional furniture, cozy textiles, lots of natural light, "
            "minimalist and hygge atmosphere. Photorealistic result."
        ),
    },
    "loft": {
        "name": "🧱 Лофт",
        "prompt": (
            "Redesign this room in industrial loft style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Add exposed brick or concrete elements if suitable, metal and wood furniture, "
            "darker industrial color palette, vintage industrial lighting, open space feeling. "
            "Photorealistic result."
        ),
    },
    "minimal": {
        "name": "⬜ Минимализм",
        "prompt": (
            "Redesign this room in pure minimalist style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Very clean lines, almost no clutter, monochrome or very limited color palette, "
            "high-quality simple furniture, lots of empty space, soft lighting. "
            "Photorealistic result."
        ),
    },
    "japandi": {
        "name": "🍃 Japandi",
        "prompt": (
            "Redesign this room in Japandi style (Japanese + Scandinavian). "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Natural materials, light wood, soft neutral colors, clean lines, "
            "zen atmosphere, functional beauty, low furniture, calm and balanced space. "
            "Photorealistic result."
        ),
    },
    "classic": {
        "name": "🏛️ Классика",
        "prompt": (
            "Redesign this room in elegant modern classic style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Sophisticated furniture with soft curves, warm neutral tones, "
            "quality textiles, subtle decorative elements, balanced and refined look. "
            "Photorealistic result."
        ),
    },
    "japanese": {
        "name": "🏯 Японский",
        "prompt": (
            "Redesign this room in traditional Japanese interior style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Tatami, shoji screens feeling, natural wood, low furniture, "
            "minimal decoration, zen calm atmosphere, soft natural light. "
            "Photorealistic result."
        ),
    },
    "provence": {
        "name": "🌸 Прованс",
        "prompt": (
            "Redesign this room in French Provence style. "
            "Keep the exact room layout, walls, windows, doors and architecture. "
            "Light pastel colors, floral motifs, vintage wooden furniture, "
            "cozy textiles, romantic and warm atmosphere. Photorealistic result."
        ),
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== ОЧЕРЕДЬ ГЕНЕРАЦИЙ ====================
generation_queue: asyncio.Queue = asyncio.Queue()
active_generations = 0
active_lock = asyncio.Lock()


# ==================== БАЗА ДАННЫХ ====================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                credits INTEGER DEFAULT 1,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id TEXT PRIMARY KEY,
                user_id INTEGER,
                package_id TEXT,
                amount REAL,
                credits INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                style_id TEXT,
                style_name TEXT,
                created_at TEXT,
                success INTEGER DEFAULT 1
            )
        """)
        await db.commit()


async def get_or_create_user(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            if row:
                return row[0]
        await db.execute(
            "INSERT INTO users (user_id, credits, created_at) VALUES (?, 1, ?)",
            (user_id, datetime.utcnow().isoformat()),
        )
        await db.commit()
        return 1


async def add_credits(user_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()


async def spend_credit(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            if not row or row[0] < 1:
                return False
        await db.execute("UPDATE users SET credits = credits - 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        return True


async def save_payment(payment_id: str, user_id: int, package_id: str, amount: float, credits: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO payments (payment_id, user_id, package_id, amount, credits, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
            (payment_id, user_id, package_id, amount, credits, datetime.utcnow().isoformat()),
        )
        await db.commit()


async def complete_payment(payment_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, credits, status FROM payments WHERE payment_id = ?", (payment_id,)
        ) as cur:
            row = await cur.fetchone()
            if not row or row[2] == "succeeded":
                return None
            user_id, credits = row[0], row[1]
        await db.execute("UPDATE payments SET status = 'succeeded' WHERE payment_id = ?", (payment_id,))
        await db.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (credits, user_id))
        await db.commit()
        return user_id, credits


async def add_history(user_id: int, style_id: str, style_name: str, success: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO history (user_id, style_id, style_name, created_at, success) VALUES (?, ?, ?, ?, ?)",
            (user_id, style_id, style_name, datetime.utcnow().isoformat(), 1 if success else 0),
        )
        await db.commit()


async def get_history(user_id: int, limit: int = 10) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT style_name, created_at, success FROM history
               WHERE user_id = ? ORDER BY id DESC LIMIT ?""",
            (user_id, limit),
        ) as cur:
            rows = await cur.fetchall()
            return rows


# ==================== ЮKASSA ====================
def create_yookassa_payment(user_id: int, package_id: str):
    if not PAYMENTS_ENABLED:
        return None
    package = PACKAGES.get(package_id)
    if not package:
        return None
    try:
        payment = Payment.create({
            "amount": {"value": f"{package['price']:.2f}", "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": f"https://t.me/{BOT_USERNAME}"},
            "capture": True,
            "description": f"Генерации интерьера: {package['name']}",
            "metadata": {"user_id": str(user_id), "package_id": package_id},
        })
        return payment.id, payment.confirmation.confirmation_url
    except Exception as e:
        logger.error(f"YooKassa error: {e}")
        return None


async def check_payment_status(payment_id: str) -> str:
    try:
        payment = Payment.find_one(payment_id)
        return payment.status
    except Exception as e:
        logger.error(f"Check payment error: {e}")
        return "error"


# ==================== XAI ====================
async def redesign_image(image_bytes: bytes, prompt: str) -> bytes:
    base64_image = base64.b64encode(image_bytes).decode("utf-8")
    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": XAI_MODEL,
        "prompt": prompt,
        "image": {"url": f"data:image/jpeg;base64,{base64_image}", "type": "image_url"},
    }
    timeout = aiohttp.ClientTimeout(total=180)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(XAI_ENDPOINT, headers=headers, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"xAI {resp.status}: {text[:300]}")
            result = await resp.json()
        image_url = result["data"][0]["url"]
        async with session.get(image_url) as img_resp:
            if img_resp.status != 200:
                raise RuntimeError("Не удалось скачать результат")
            return await img_resp.read()


# ==================== WORKER ОЧЕРЕДИ ====================
async def generation_worker():
    """Фоновый воркер: берёт задачи из очереди и выполняет генерации."""
    global active_generations
    while True:
        task = await generation_queue.get()
        try:
            async with active_lock:
                active_generations += 1

            chat_id = task["chat_id"]
            message_id = task["message_id"]
            user_id = task["user_id"]
            image_bytes = task["image_bytes"]
            style_id = task["style_id"]
            style = STYLES[style_id]

            try:
                result_bytes = await redesign_image(image_bytes, style["prompt"])
                await add_history(user_id, style_id, style["name"], success=True)
                new_balance = await get_or_create_user(user_id)

                await bot.send_photo(
                    chat_id=chat_id,
                    photo=BufferedInputFile(result_bytes, filename="interior.jpg"),
                    caption=(
                        f"✅ Готово! Стиль: <b>{style['name']}</b>\n"
                        f"Осталось генераций: <b>{new_balance}</b>\n\n"
                        "Можешь выбрать другой стиль или прислать новое фото."
                    ),
                    parse_mode="HTML",
                    reply_markup=get_styles_keyboard(),
                )
                try:
                    await bot.delete_message(chat_id, message_id)
                except Exception:
                    pass
            except Exception as e:
                await add_credits(user_id, 1)
                await add_history(user_id, style_id, style["name"], success=False)
                logger.exception("Generation failed")
                await bot.send_message(
                    chat_id,
                    f"❌ Ошибка генерации. Кредит возвращён.\n<code>{str(e)[:250]}</code>",
                    parse_mode="HTML",
                )
        finally:
            async with active_lock:
                active_generations -= 1
            generation_queue.task_done()


# ==================== FSM ====================
class RedesignStates(StatesGroup):
    choosing_style = State()


# ==================== КЛАВИАТУРЫ ====================
def get_styles_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for style_id, style in STYLES.items():
        builder.add(InlineKeyboardButton(text=style["name"], callback_data=f"style:{style_id}"))
    builder.adjust(2)
    return builder.as_markup()


def get_packages_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for pack_id, pack in PACKAGES.items():
        builder.add(InlineKeyboardButton(
            text=f"{pack['name']} — {pack['price']} ₽",
            callback_data=f"buy:{pack_id}",
        ))
    builder.adjust(1)
    builder.add(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
    return builder.as_markup()


def get_payment_keyboard(payment_url: str, payment_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="💳 Оплатить (СБП / Карта)", url=payment_url))
    builder.add(InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check:{payment_id}"))
    builder.adjust(1)
    return builder.as_markup()


# ==================== БОТ ====================
bot = Bot(token=TELEGRAM_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    credits = await get_or_create_user(message.from_user.id)
    await message.answer(
    "Хотите увидеть, как может выглядеть ваша квартира после ремонта?\n\n"
    "<b>«Новый Ремонт»</b> поможет:\n"
    "✨ Изменить интерьер по фотографии\n"
    "✨ Найти идеи для кухни, спальни и гостиной\n\n"
    "<b>Всего три шага:</b>\n"
    "1. Загрузите фото комнаты\n"
    "2. Выберите стиль\n"
    "3. Получите визуализацию!\n\n"
    f"У вас сейчас: <b>{credits}</b> генераций.\n\n"
    "Начните с первой комнаты и найдите интерьер своей мечты ❤️",
    parse_mode="HTML",
)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "📌 <b>Как пользоваться</b>\n\n"
        "1. Отправь фото комнаты\n"
        "2. Выбери стиль кнопкой\n"
        "3. Получи готовый интерьер\n\n"
        "Новым пользователям — <b>1 бесплатная генерация</b>.\n"
        "Дальше покупай пакеты через /buy (СБП и карты).",
        parse_mode="HTML",
    )


@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    credits = await get_or_create_user(message.from_user.id)
    queue_size = generation_queue.qsize()
    text = f"💰 Баланс: <b>{credits}</b> генераций"
    if queue_size > 0:
        text += f"\n⏳ В очереди сейчас: {queue_size} задач"
    text += "\n\nКупить: /buy"
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("history"))
async def cmd_history(message: Message):
    rows = await get_history(message.from_user.id, limit=15)
    if not rows:
        await message.answer("История пуста. Сделай первую генерацию!")
        return
    lines = ["📜 <b>Последние генерации:</b>\n"]
    for style_name, created_at, success in rows:
        status = "✅" if success else "❌"
        # created_at примерно: 2026-10-30T09:15:00.123456
        try:
            dt = datetime.fromisoformat(created_at)
            time_str = dt.strftime("%d.%m %H:%M")
        except Exception:
            time_str = created_at[:16]
        lines.append(f"{status} {style_name} — {time_str}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("buy"))
async def cmd_buy(message: Message):
    if not PAYMENTS_ENABLED:
        await message.answer("Оплата пока не настроена.")
        return
    await message.answer(
        "Выбери пакет:\n\nОплата через <b>ЮKassa</b> (СБП и карты).",
        parse_mode="HTML",
        reply_markup=get_packages_keyboard(),
    )


@dp.callback_query(F.data.startswith("buy:"))
async def process_buy(callback: CallbackQuery):
    if not PAYMENTS_ENABLED:
        await callback.answer("Оплата отключена", show_alert=True)
        return
    package_id = callback.data.split(":")[1]
    package = PACKAGES.get(package_id)
    if not package:
        await callback.answer("Пакет не найден", show_alert=True)
        return
    result = create_yookassa_payment(callback.from_user.id, package_id)
    if not result:
        await callback.message.edit_text("Не удалось создать платёж.")
        await callback.answer()
        return
    payment_id, payment_url = result
    await save_payment(payment_id, callback.from_user.id, package_id, package["price"], package["credits"])
    await callback.message.edit_text(
        f"🧾 <b>{package['name']}</b>\n"
        f"Сумма: <b>{package['price']} ₽</b>\n"
        f"Генераций: <b>{package['credits']}</b>\n\n"
        "Оплати через СБП или карту, затем нажми «Я оплатил».",
        parse_mode="HTML",
        reply_markup=get_payment_keyboard(payment_url, payment_id),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("check:"))
async def process_check_payment(callback: CallbackQuery):
    payment_id = callback.data.split(":")[1]
    status = await check_payment_status(payment_id)
    if status == "succeeded":
        result = await complete_payment(payment_id)
        if result:
            user_id, credits = result
            new_balance = await get_or_create_user(user_id)
            await callback.message.edit_text(
                f"✅ Оплата прошла!\nНачислено: <b>+{credits}</b>\nБаланс: <b>{new_balance}</b>",
                parse_mode="HTML",
            )
        else:
            await callback.answer("Уже обработано", show_alert=True)
    elif status in ("pending", "waiting_for_capture"):
        await callback.answer("Платёж ещё не поступил. Подождите и нажмите снова.", show_alert=True)
    else:
        await callback.answer(f"Статус: {status}", show_alert=True)


@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    await callback.message.edit_text("Пришли фото комнаты или используй /buy и /balance.")
    await callback.answer()


@dp.message(F.photo)
async def handle_photo(message: Message, state: FSMContext):
    user_id = message.from_user.id
    credits = await get_or_create_user(user_id)
    if credits < 1:
        kb = get_packages_keyboard() if PAYMENTS_ENABLED else None
        await message.answer("Генерации закончились. Купи пакет: /buy", reply_markup=kb)
        return

    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    photo_io = await bot.download_file(file.file_path)
    image_bytes = photo_io.read()

    await state.update_data(image_bytes=image_bytes)
    await state.set_state(RedesignStates.choosing_style)

    queue_info = ""
    if generation_queue.qsize() > 0 or active_generations > 0:
        queue_info = f"\n⏳ Сейчас в работе/очереди: {active_generations + generation_queue.qsize()}"

    await message.answer(
        f"Фото получено! Баланс: <b>{credits}</b>{queue_info}\n\nВыбери стиль:",
        parse_mode="HTML",
        reply_markup=get_styles_keyboard(),
    )


@dp.callback_query(F.data.startswith("style:"))
async def process_style(callback: CallbackQuery, state: FSMContext):
    style_id = callback.data.split(":")[1]
    style = STYLES.get(style_id)
    if not style:
        await callback.answer("Неизвестный стиль", show_alert=True)
        return

    data = await state.get_data()
    image_bytes = data.get("image_bytes")
    if not image_bytes:
        await callback.message.answer("Фото потерялось. Пришли заново.")
        await state.clear()
        await callback.answer()
        return

    user_id = callback.from_user.id
    if not await spend_credit(user_id):
        await callback.message.edit_text("Генерации закончились. /buy")
        await callback.answer()
        return

    # Ставим в очередь
    position = generation_queue.qsize() + 1
    status_msg = await callback.message.edit_text(
        f"🎨 Стиль: <b>{style['name']}</b>\n"
        f"Позиция в очереди: <b>#{position}</b>\n\n"
        "Генерация займёт 20–90 секунд. Я пришлю результат сюда.",
        parse_mode="HTML",
    )

    await generation_queue.put({
        "chat_id": callback.message.chat.id,
        "message_id": status_msg.message_id,
        "user_id": user_id,
        "image_bytes": image_bytes,
        "style_id": style_id,
    })
    await callback.answer()


@dp.message(F.text)
async def handle_text(message: Message):
    await message.answer(
        "Пришли <b>фото</b> комнаты 🏠\n\n"
        "/balance — баланс\n/buy — купить\n/history — история",
        parse_mode="HTML",
    )


# ==================== ЗАПУСК ====================
async def on_startup():
    await init_db()
    # Запускаем воркеры очереди
    for _ in range(MAX_CONCURRENT_GENERATIONS):
        asyncio.create_task(generation_worker())
    logger.info(f"Started {MAX_CONCURRENT_GENERATIONS} generation workers")

    if USE_WEBHOOK:
        webhook_url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
        await bot.set_webhook(webhook_url, secret_token=WEBHOOK_SECRET)
        logger.info(f"Webhook set: {webhook_url}")
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted, using polling")


async def on_shutdown():
    if USE_WEBHOOK:
        await bot.delete_webhook()
    await bot.session.close()


async def main_polling():
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()


def main_webhook():
    # aiohttp приложение для вебхука
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
    )
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    async def _on_startup(app):
        await on_startup()

    async def _on_shutdown(app):
        await on_shutdown()

    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)


if __name__ == "__main__":
    if USE_WEBHOOK:
        if not WEBHOOK_HOST:
            raise ValueError("Для webhook укажите WEBHOOK_HOST в .env")
        main_webhook()
    else:
        asyncio.run(main_polling())
