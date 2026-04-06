import asyncio
import os
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, Message, LabeledPrice, PreCheckoutQuery, SuccessfulPayment
from aiogram.filters import Command
import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN") # BotFather'dan olinadigan Click/Payme tokeni
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- TELEGRAM BOT QISMI ---

@dp.message(Command("start"))
async def start_cmd(m: Message):
    u, p, ends_at = await db.get_or_create_user(m.from_user.id)
    kb = [[InlineKeyboardButton(text="📱 Panelga Kirish", web_app=WebAppInfo(url=WEBAPP_URL))]]
    text = (
        "👋 Assalomu alaykum! Hero Scanner PRO botiga xush kelibsiz.\n\n"
        f"👤 *Login:* `{u}`\n🔑 *Parol:* `{p}`\n"
        "💎 *Status:* Oddiy (Premium olish uchun /premium bosing)"
    )
    await m.answer(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# TO'LOV TIZIMI (Click / Payme)
@dp.message(Command("premium"))
async def buy_premium(m: Message):
    if not PROVIDER_TOKEN:
        return await m.answer("To'lov tizimi hozircha ulanmagan (Admin Click token kiritishi kerak).")
    
    await bot.send_invoice(
        chat_id=m.chat.id,
        title="💎 Hero Premium",
        description="1 oylik Arvoh Davomat (Ghost Scan) va cheksiz imkoniyatlar.",
        payload="premium_1_month",
        provider_token=PROVIDER_TOKEN,
        currency="UZS",
        prices=[LabeledPrice(label="Premium Obuna", amount=5000000)] # 50,000 so'm (tiyinlarda)
    )

@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def process_successful_payment(m: Message):
    await db.set_premium(m.from_user.id)
    await m.answer("🎉 Tabriklaymiz! To'lov muvaffaqiyatli o'tdi. Siz endi **Premium** foydalanuvchisiz! Mini App orqali Arvoh Davomatdan foydalanishingiz mumkin.")

# --- WEB SERVER (MINI APP API) QISMI ---

async def handle_ghost_scan(request):
    """Arvoh davomat so'rovi (Premiumlar uchun)"""
    data = await request.json()
    login = data.get('login')
    
    is_prem = await db.check_premium(login)
    if not is_prem:
        return web.json_response({"status": "error", "message": "Bu funksiya faqat Premium foydalanuvchilar uchun!"})
    
    # KELAJAKDAGI MANTIQ: Bu yerda bazadan o'sha xonadagi talabalarni topib, bot orqali xabar yuboriladi
    # Hozircha imitatsiya qilamiz:
    return web.json_response({"status": "success", "message": "👻 So'rov yuborildi! 3 ta aktiv foydalanuvchi sizning xonangizda. Ular tasdiqlashi kutilmoqda..."})

async def handle_index(request):
    try:
        with open('scanner.html', 'r', encoding='utf-8') as f:
            return web.Response(text=f.read(), content_type='text/html')
    except Exception:
        return web.Response(text="<h1>Scanner fayli topilmadi!</h1>", content_type='text/html')

app = web.Application()
app.router.add_get('/', handle_index) # <--- BU JUDA MUHIM: Sayt ochilishi uchun
app.router.add_post('/api/ghost_scan', handle_ghost_scan)

async def main():
    await db.init_db()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Web server ishga tushdi: {PORT}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
