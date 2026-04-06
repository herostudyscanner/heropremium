import asyncio
import os
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command
import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
PORT = int(os.getenv("PORT", 10000))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- BOT QISMI ---

@dp.message(Command("start"))
async def start_cmd(m: Message):
    try:
        # Bazadan ma'lumot olish
        u, p, ends_at = await db.get_or_create_user(m.from_user.id)
        
        kb = [[InlineKeyboardButton(text="📱 Panelga Kirish", web_app=WebAppInfo(url=WEBAPP_URL))]]
        text = (
            "👋 Assalomu alaykum! Hero Scanner PRO botiga xush kelibsiz.\n\n"
            f"👤 *Login:* `{u}`\n🔑 *Parol:* `{p}`\n"
            "💎 *Status:* Oddiy (Premium olish uchun /premium bosing)"
        )
        await m.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logger.error(f"Start xatosi: {e}")
        await m.answer("⚠️ Baza bilan ulanishda xatolik yuz berdi. Birozdan so'ng urinib ko'ring.")

# --- WEB SERVER QISMI ---

async def handle_index(request):
    try:
        # GitHub'da scanner.html faylingiz asosiy papkada bo'lishi kerak
        with open('scanner.html', 'r', encoding='utf-8') as f:
            return web.Response(text=f.read(), content_type='text/html')
    except Exception:
        return web.Response(text="<h1>Scanner fayli topilmadi!</h1>", content_type='text/html')

app = web.Application()
app.router.add_get('/', handle_index)

# --- ASOSIY ISHGA TUSHIRISH ---

async def main():
    # 1. BAZANI ISHGA TUSHIRISH (Pollingdan oldin bo'lishi shart!)
    logger.info("Bazaga ulanishga harakat qilinmoqda...")
    await db.init_db()
    
    # 2. WEB SERVERNI ISHGA TUSHIRISH
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"✅ Web server ishga tushdi: {PORT}")
    
    # 3. BOTNI ISHGA TUSHIRISH
    logger.info("✅ Bot pollingni boshlamoqda...")
    # Eski update'larni o'chirib yuborish (Conflict bermasligi uchun)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot to'xtatildi")
