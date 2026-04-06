import asyncio
import os
import time
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command
import jwt
import database as db
import hero_api as api

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN   = os.getenv("BOT_TOKEN")
ADMIN_ID    = int(os.getenv("ADMIN_ID", 0))
raw_url     = os.getenv("WEBAPP_URL", "").rstrip("/")
WEBAPP_URL  = f"https://{raw_url.replace('https://','').replace('http://','')}" if raw_url else ""
PORT        = int(os.getenv("PORT", 8080))
JWT_SECRET  = os.getenv("JWT_SECRET", "change-me-in-production-use-random-string")
JWT_EXPIRE_HOURS = 24

bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp  = Dispatcher()

# ─── JWT HELPERS ──────────────────────────────────────────────────────────────

def create_token(user_id: int, telegram_id: int, role: str) -> str:
    payload = {
        "user_id": user_id,
        "telegram_id": telegram_id,
        "role": role,
        "exp": datetime.now(tz=timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

_rate_store: dict[str, list[float]] = defaultdict(list)

def is_rate_limited(key: str, max_req: int = 5, window_sec: int = 60) -> bool:
    now = time.time()
    _rate_store[key] = [t for t in _rate_store[key] if now - t < window_sec]
    if len(_rate_store[key]) >= max_req:
        return True
    _rate_store[key].append(now)
    return False

# ─── MIDDLEWARE ───────────────────────────────────────────────────────────────

PUBLIC_PATHS = {"/", "/health", "/api/auth/login"}

@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in PUBLIC_PATHS or request.method == "OPTIONS":
        return await handler(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return web.json_response({"status": "error", "message": "Unauthorized"}, status=401)

    payload = decode_token(auth_header[7:])
    if not payload:
        return web.json_response({"status": "error", "message": "Token yaroqsiz yoki muddati tugagan"}, status=401)

    request["user"] = payload
    return await handler(request)

@web.middleware
async def security_headers_middleware(request: web.Request, handler):
    resp = await handler(request)
    resp.headers["X-Content-Type-Options"]  = "nosniff"
    resp.headers["X-Frame-Options"]          = "DENY"
    resp.headers["X-XSS-Protection"]         = "1; mode=block"
    resp.headers["Referrer-Policy"]           = "strict-origin-when-cross-origin"
    resp.headers["Cache-Control"]             = "no-store"
    return resp

# ─── VALIDATION HELPERS ───────────────────────────────────────────────────────

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

def validate_email(email: str) -> bool:
    return bool(email and EMAIL_RE.match(email) and len(email) <= 200)

def validate_url(url: str) -> bool:
    return bool(url and url.startswith("http") and len(url) <= 2000)

def get_client_ip(request: web.Request) -> str:
    return request.headers.get("X-Forwarded-For", request.remote or "unknown").split(",")[0].strip()

# ─── ROUTE HANDLERS ───────────────────────────────────────────────────────────

async def auth_login(request: web.Request):
    ip = get_client_ip(request)
    if is_rate_limited(f"login:{ip}", max_req=10, window_sec=60):
        return web.json_response({"status": "error", "message": "Juda ko'p urinish. 1 daqiqa kuting."}, status=429)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    login    = str(data.get("login", "")).strip()[:50]
    password = str(data.get("password", "")).strip()[:100]

    if not login or not password:
        return web.json_response({"status": "error", "message": "Maydonlar bo'sh"}, status=400)

    user = await db.verify_login(login, password)
    if not user:
        # Same rate key to prevent timing attacks
        if is_rate_limited(f"login_fail:{ip}", max_req=5, window_sec=120):
            return web.json_response({"status": "error", "message": "Akkaunt vaqtincha bloklandi"}, status=429)
        return web.json_response({"status": "error", "message": "Login yoki parol xato"}, status=401)

    role  = "super_admin" if user["telegram_id"] == ADMIN_ID else "user"
    token = create_token(user["id"], user["telegram_id"], role)
    return web.json_response({"status": "success", "token": token, "user_id": user["id"], "role": role})


async def get_users_list(request: web.Request):
    user = request["user"]
    rows = await db.get_hero_accounts(user["user_id"])
    return web.json_response({"status": "success", "users": [dict(r) for r in rows]})


async def add_user(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    email    = str(data.get("email", "")).strip()
    password = str(data.get("password", "")).strip()[:200]

    if not validate_email(email):
        return web.json_response({"status": "error", "message": "Email manzil noto'g'ri"}, status=400)
    if not password:
        return web.json_response({"status": "error", "message": "Parol kiritilmagan"}, status=400)

    await db.add_hero_account(user["user_id"], email, password, "NO_TOKEN")
    return web.json_response({"status": "success"})


async def delete_user(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
        account_id = int(data.get("account_id", 0))
    except Exception:
        return web.json_response({"status": "error", "message": "Noto'g'ri so'rov"}, status=400)

    if account_id <= 0:
        return web.json_response({"status": "error", "message": "ID xato"}, status=400)

    success = await db.delete_hero_account(user["user_id"], account_id)
    if success:
        return web.json_response({"status": "success"})
    return web.json_response({"status": "error", "message": "Akkaunt topilmadi"}, status=404)


async def get_stats(request: web.Request):
    user = request["user"]
    stats = await db.get_user_stats(user["user_id"])
    return web.json_response({"status": "success", "data": stats})


async def get_history(request: web.Request):
    user = request["user"]
    logs = await db.get_user_scan_history(user["user_id"], limit=20)
    # Convert datetime to ISO string for JSON serialization
    for log in logs:
        if "scanned_at" in log and log["scanned_at"]:
            log["scanned_at"] = log["scanned_at"].isoformat()
    return web.json_response({"status": "success", "logs": logs})


async def do_scan(request: web.Request):
    user     = request["user"]
    user_id  = user["user_id"]
    ip       = get_client_ip(request)

    if is_rate_limited(f"scan:{user_id}", max_req=10, window_sec=60):
        return web.json_response({"status": "error", "message": "Juda tez skanlamoqdasiz"}, status=429)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    qr_url = str(data.get("url", "")).strip()
    if not validate_url(qr_url):
        return web.json_response({"status": "error", "message": "URL noto'g'ri"}, status=400)

    accounts = await db.get_active_tokens(user_id)
    if not accounts:
        return web.json_response({"status": "error", "message": "Hali akkaunt qo'shilmagan"}, status=400)

    async def update_token(email: str, token: str):
        await db.add_hero_account(user_id, email, "", token)

    success, total, duration, report = await api.mark_all_accounts_smart(
        list(accounts), qr_url, update_token
    )
    await db.save_detailed_scan(user_id, success, total, duration, report)

    return web.json_response({
        "status":   "success",
        "success":  success,
        "total":    total,
        "duration": duration,
        "report":   report,
    })


async def get_admin_all_data(request: web.Request):
    user = request["user"]
    if user.get("role") != "super_admin":
        return web.json_response({"status": "error", "message": "Ruxsat yo'q"}, status=403)

    data = await db.get_super_admin_data()
    # Passwords are already excluded from DB query — safe to return
    for log in data.get("logs", []):
        if "scanned_at" in log and log["scanned_at"]:
            log["scanned_at"] = log["scanned_at"].isoformat()
    return web.json_response({"status": "success", "data": data})


async def index(request: web.Request):
    if os.path.exists("scanner.html"):
        return web.FileResponse("scanner.html")
    return web.Response(text="404 Not Found", status=404)


async def health_check(request: web.Request):
    return web.Response(text="OK", status=200)

# ─── TELEGRAM BOT ─────────────────────────────────────────────────────────────

if bot:
    @dp.message(Command("start"))
    async def cmd_start(m: Message):
        kb = [[InlineKeyboardButton(text="📱 Panelga Kirish", web_app=WebAppInfo(url=WEBAPP_URL))]]
        await m.answer(
            "🛡 *Hero Scanner PRO*\n\nPanelga kirish uchun quyidagi tugmani bosing.\n"
            "Login va parol olish: /login",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
            parse_mode="Markdown"
        )

    @dp.message(Command("login"))
    async def cmd_login(m: Message):
        login, plain_password = await db.get_or_create_user(m.from_user.id)
        await m.answer(
            f"🔐 *Sizning kirish ma'lumotlaringiz:*\n\n"
            f"👤 Login: `{login}`\n"
            f"🔑 Parol: `{plain_password}`\n\n"
            f"⚠️ Bu xabarni o'chirib tashlang — parol faqat bir marta ko'rsatiladi.",
            parse_mode="Markdown"
        )

# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    app = web.Application(middlewares=[security_headers_middleware, auth_middleware])

    app.router.add_get("/",                    index)
    app.router.add_get("/health",              health_check)
    app.router.add_post("/api/auth/login",     auth_login)
    app.router.add_get("/api/users",           get_users_list)
    app.router.add_post("/api/users/add",      add_user)
    app.router.add_post("/api/users/delete",   delete_user)
    app.router.add_get("/api/stats",           get_stats)
    app.router.add_get("/api/history",         get_history)
    app.router.add_post("/api/scan",           do_scan)
    app.router.add_get("/api/admin/all_data",  get_admin_all_data)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info(f"✅ Server started on port {PORT}")

    await db.init_db()

    if bot:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    else:
        logger.warning("⚠️ BOT_TOKEN not set — running without Telegram bot")
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
