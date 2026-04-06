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
PREMIUM_ACTIVATION_CODE = os.getenv("PREMIUM_ACTIVATION_CODE", "")

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


def is_user_premium(profile: dict | None) -> bool:
    if not profile:
        return False
    if not profile.get("is_premium"):
        return False
    until = profile.get("premium_until")
    if not until:
        return True
    if isinstance(until, str):
        try:
            until = datetime.fromisoformat(until)
        except ValueError:
            return False
    return until > datetime.now()


def parse_time_to_minutes(value: str) -> tuple[int, int] | None:
    if not value or "-" not in value:
        return None
    try:
        start, end = value.split("-", 1)
        sh, sm = [int(x) for x in start.strip().split(":", 1)]
        eh, em = [int(x) for x in end.strip().split(":", 1)]
        return sh * 60 + sm, eh * 60 + em
    except Exception:
        return None


def schedule_matches_pref(slot: dict, pref: dict) -> bool:
    course = (pref.get("preferred_course") or "").strip().lower()
    room = (pref.get("preferred_room") or "").strip().lower()
    days = {d.strip().lower() for d in (pref.get("preferred_days") or "").split(",") if d.strip()}
    tr = parse_time_to_minutes(pref.get("preferred_time_range") or "")

    slot_course = str(slot.get("course", "")).lower()
    slot_room = str(slot.get("room", "")).lower()
    slot_day = str(slot.get("day", "")).lower()
    slot_time = str(slot.get("time", ""))
    slot_minutes = parse_time_to_minutes(slot_time)

    if course and course not in slot_course:
        return False
    if room and room not in slot_room:
        return False
    if days and slot_day not in days:
        return False
    if tr and slot_minutes:
        start, end = tr
        s2, e2 = slot_minutes
        if s2 < start or e2 > end:
            return False
    return True

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
    profile = await db.get_user_profile(user["id"])
    return web.json_response({
        "status": "success",
        "token": token,
        "user_id": user["id"],
        "role": role,
        "premium": is_user_premium(profile),
    })


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


async def get_premium_status(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    premium = is_user_premium(profile)
    return web.json_response({
        "status": "success",
        "data": {
            "is_premium": premium,
            "premium_until": profile.get("premium_until").isoformat() if profile and profile.get("premium_until") else None,
            "google_email": profile.get("google_email") if profile else None,
            "timezone": profile.get("timezone") if profile else "Asia/Tashkent",
        }
    })


async def activate_premium(request: web.Request):
    user = request["user"]
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    code = str(data.get("code", "")).strip()
    days = int(data.get("days", 30) or 30)
    if days <= 0 or days > 365:
        days = 30

    # Super admin can activate directly without code.
    if user.get("role") != "super_admin":
        if not PREMIUM_ACTIVATION_CODE:
            return web.json_response({"status": "error", "message": "Premium kod serverda sozlanmagan"}, status=500)
        if code != PREMIUM_ACTIVATION_CODE:
            return web.json_response({"status": "error", "message": "Premium kod noto'g'ri"}, status=403)

    await db.activate_premium(user["user_id"], days=days)
    return web.json_response({"status": "success", "message": f"Premium {days} kunga yoqildi"})


async def get_schedule_preferences(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    pref = await db.get_schedule_preferences(user["user_id"])
    data = pref or {}
    data["google_email"] = profile.get("google_email") if profile else None
    data["timezone"] = profile.get("timezone") if profile else "Asia/Tashkent"
    return web.json_response({"status": "success", "data": data})


async def save_schedule_preferences(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    google_email = str(data.get("google_email", "")).strip()
    tz_name = str(data.get("timezone", "Asia/Tashkent")).strip()[:64] or "Asia/Tashkent"
    preferred_course = str(data.get("preferred_course", "")).strip()[:120]
    preferred_room = str(data.get("preferred_room", "")).strip()[:120]
    preferred_days = str(data.get("preferred_days", "")).strip()[:80]
    preferred_time_range = str(data.get("preferred_time_range", "")).strip()[:40]
    notes = str(data.get("notes", "")).strip()[:1200]

    if google_email and not validate_email(google_email):
        return web.json_response({"status": "error", "message": "Google email noto'g'ri"}, status=400)

    await db.save_google_profile(user["user_id"], google_email, tz_name)
    await db.upsert_schedule_preferences(
        user["user_id"], preferred_course, preferred_room, preferred_days, preferred_time_range, notes
    )
    return web.json_response({"status": "success", "message": "Premium jadval sozlamalari saqlandi"})


async def build_schedule_plan(request: web.Request):
    user = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu bo'lim faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    pref = await db.get_schedule_preferences(user["user_id"]) or {}
    schedule = data.get("schedule", [])
    if not isinstance(schedule, list):
        return web.json_response({"status": "error", "message": "schedule massiv bo'lishi kerak"}, status=400)

    # Basic planner: find free slots that match user preferences.
    candidates = []
    for slot in schedule:
        if not isinstance(slot, dict):
            continue
        status = str(slot.get("status", "")).lower()
        if status and status not in {"free", "available", "open"}:
            continue
        if schedule_matches_pref(slot, pref):
            candidates.append({
                "course": slot.get("course"),
                "room": slot.get("room"),
                "day": slot.get("day"),
                "time": slot.get("time"),
                "reason": "matched_preferences"
            })
        if len(candidates) >= 5:
            break

    result = {
        "total_input": len(schedule),
        "suggested": candidates,
        "message": "Mos bo'sh slotlar topildi" if candidates else "Mos slot topilmadi, preferensiyani kengaytiring",
        "next_step": "Topilgan slotni tanlab booking API ga yuborish mumkin"
    }
    await db.save_booking_plan(user["user_id"], {"schedule_count": len(schedule), "pref": pref}, result, status="planned")
    return web.json_response({"status": "success", "data": result})


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
    app.router.add_get("/api/premium/status",  get_premium_status)
    app.router.add_post("/api/premium/activate", activate_premium)
    app.router.add_get("/api/schedule/preferences", get_schedule_preferences)
    app.router.add_post("/api/schedule/preferences", save_schedule_preferences)
    app.router.add_post("/api/schedule/plan", build_schedule_plan)
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
