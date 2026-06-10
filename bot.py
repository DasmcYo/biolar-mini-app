import os
import asyncio
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, BotCommand,
    InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo,
)
from aiogram.filters import CommandStart, Command

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

APP_URL = WEBHOOK_URL.rstrip("/") if WEBHOOK_URL else ""

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def main_keyboard() -> InlineKeyboardMarkup:
    if APP_URL:
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="🌿 Открыть Biolar App",
                web_app=WebAppInfo(url=APP_URL),
            )
        ]])
    # Локально — кнопки команд вместо WebApp
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="referral")],
        [InlineKeyboardButton(text="🎁 Розыгрыш месяца",   callback_data="giveaway")],
    ])


def ref_keyboard(ref_code: str, bot_username: str) -> InlineKeyboardMarkup:
    ref_link = f"https://t.me/{bot_username}?start=ref_{ref_code}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться ссылкой", url=f"https://t.me/share/url?url={ref_link}&text=Подбери%20добавки%20под%20свою%20цель%20%F0%9F%8C%BF")]
    ])


@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else ""
    ref = args.replace("ref_", "") if args.startswith("ref_") else None

    name = message.from_user.first_name or "друг"
    text = (
        f"Привет, {name}! 👋\n\n"
        "Я помогу подобрать добавки Biolar Organics под твои цели — "
        "пройди короткий квиз прямо здесь.\n\n"
        "Открой приложение 👇"
    )
    if ref:
        text += f"\n\n_Тебя пригласил друг — ты уже в списке участников розыгрыша_ 🎁"

    await message.answer(text, reply_markup=main_keyboard(), parse_mode="Markdown")


@dp.message(Command("app"))
async def cmd_app(message: Message):
    await message.answer("Открывай 👇", reply_markup=main_keyboard())


@dp.message(Command("referral"))
async def cmd_referral(message: Message):
    import db
    user = await db.get_user(message.from_user.id)
    if not user:
        user = await db.get_or_create_user(
            message.from_user.id,
            message.from_user.username or "",
            message.from_user.first_name or "",
        )
    count = await db.get_referral_count(user["ref_code"])
    bot_info = await bot.get_me()
    text = (
        f"🔗 *Твоя реферальная ссылка*\n\n"
        f"Ты пригласил: *{count}* чел.\n\n"
        f"*Уровни наград:*\n"
        f"• 3 чел. → промокод −15%\n"
        f"• 7 чел. → промокод −25%\n"
        f"• 15 чел. → бесплатный продукт 🎁\n\n"
        f"Нажми кнопку чтобы поделиться ссылкой:"
    )
    await message.answer(
        text,
        reply_markup=ref_keyboard(user["ref_code"], bot_info.username),
        parse_mode="Markdown",
    )


@dp.message(Command("giveaway"))
async def cmd_giveaway(message: Message):
    import db
    user = await db.get_or_create_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or "",
    )
    already = await db.is_in_giveaway(message.from_user.id)
    count = await db.get_giveaway_count()

    if already:
        await message.answer(
            f"✅ Ты уже участвуешь в розыгрыше!\n"
            f"Всего участников: *{count}*\n\n"
            f"Итоги — в конце месяца 🎁",
            parse_mode="Markdown",
        )
    else:
        await db.join_giveaway(message.from_user.id)
        count = await db.get_giveaway_count()
        await message.answer(
            f"🎉 Ты в игре! Участников уже: *{count}*\n\n"
            f"Победитель объявляется в конце месяца.\n"
            f"Пригласи друзей — каждый даёт +1 шанс 👇",
            reply_markup=ref_keyboard(user["ref_code"], (await bot.get_me()).username),
            parse_mode="Markdown",
        )


async def set_commands():
    await bot.set_my_commands([
        BotCommand(command="start",    description="Открыть приложение"),
        BotCommand(command="referral", description="Реферальная ссылка"),
        BotCommand(command="giveaway", description="Участвовать в розыгрыше"),
    ])


async def main():
    """Запуск в режиме polling (для локальной разработки)."""
    await db.init_db()
    await set_commands()
    print("Bot started (polling mode)...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    import db
    asyncio.run(main())
