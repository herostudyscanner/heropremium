import asyncio
import csv
import os
import re
import time
import logging
from io import StringIO
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import aiohttp
import jwt
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo
)

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
JWT_SECRET  = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_EXPIRE_HOURS = 24
PREMIUM_CODE = os.getenv("PREMIUM_ACTIVATION_CODE", "")
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.getenv("GOOGLE_REDIRECT_URI", "")
GOOGLE_SCOPES = "openid email profile https://www.googleapis.com/auth/calendar.readonly"

HERO_LOGIN_URL = "https://api.newuzbekistan.hero.study/v1/users/login?lang=en"

bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp  = Dispatcher()

# ─── JWT ──────────────────────────────────────────────────────────────────────

def create_token(user_id: int, telegram_id: int, role: str) -> str:
    payload = {
        "user_id": user_id, "telegram_id": telegram_id, "role": role,
        "exp": datetime.now(tz=timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def decode_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None

# ─── RATE LIMITER ─────────────────────────────────────────────────────────────

_rate_store: dict[str, list] = defaultdict(list)

def is_rate_limited(key: str, max_req: int = 5, window_sec: int = 60) -> bool:
    now = time.time()
    _rate_store[key] = [t for t in _rate_store[key] if now - t < window_sec]
    if len(_rate_store[key]) >= max_req:
        return True
    _rate_store[key].append(now)
    return False

# ─── HELPERS ──────────────────────────────────────────────────────────────────

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

def validate_email(email: str) -> bool:
    return bool(email and EMAIL_RE.match(email) and len(email) <= 200)

def validate_url(url: str) -> bool:
    return bool(url and url.startswith("http") and len(url) <= 2000)

def get_client_ip(request: web.Request) -> str:
    return request.headers.get("X-Forwarded-For", request.remote or "unknown").split(",")[0].strip()

def is_user_premium(profile) -> bool:
    if not profile or not profile.get("is_premium"):
        return False
    until = profile.get("premium_until")
    if not until:
        return True
    if isinstance(until, str):
        try:
            until = datetime.fromisoformat(until)
        except ValueError:
            return False
    if until.tzinfo:
        return until > datetime.now(tz=timezone.utc)
    return until > datetime.now()

def parse_time_to_minutes(value: str):
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
    room   = (pref.get("preferred_room") or "").strip().lower()
    days   = {d.strip().lower() for d in (pref.get("preferred_days") or "").split(",") if d.strip()}
    tr     = parse_time_to_minutes(pref.get("preferred_time_range") or "")
    if course and course not in str(slot.get("course", "")).lower():
        return False
    if room and room not in str(slot.get("room", "")).lower():
        return False
    if days and str(slot.get("day", "")).lower() not in days:
        return False
    if tr:
        sm = parse_time_to_minutes(str(slot.get("time", "")))
        if sm and (sm[0] < tr[0] or sm[1] > tr[1]):
            return False
    return True

def normalize_schedule_item(raw: dict):
    if not isinstance(raw, dict):
        return None
    time_text = str(raw.get("time", "")).strip()
    minutes   = parse_time_to_minutes(time_text)
    if not minutes:
        return None
    s, e = minutes
    day = str(raw.get("day", "")).strip().lower()
    if not day:
        return None
    return {
        "course": str(raw.get("course", "")).strip()[:160],
        "room":   str(raw.get("room", "")).strip()[:120],
        "day": day[:32], "start_min": s, "end_min": e,
        "teacher":    str(raw.get("teacher", "")).strip()[:160],
        "group_name": str(raw.get("group", raw.get("group_name", ""))).strip()[:120],
        "raw": raw,
    }

def build_google_oauth_state(user_id: int, telegram_id: int) -> str:
    payload = {
        "uid": int(user_id), "tg": int(telegram_id), "kind": "google_oauth_state",
        "exp": datetime.now(tz=timezone.utc) + timedelta(minutes=15),
        "iat": datetime.now(tz=timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def parse_google_oauth_state(state: str):
    try:
        data = jwt.decode(state, JWT_SECRET, algorithms=["HS256"])
        return data if data.get("kind") == "google_oauth_state" else None
    except Exception:
        return None

def iso_to_minute_fields(dt_iso: str):
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00")).astimezone()
        return dt.strftime("%A").lower(), dt.hour * 60 + dt.minute, dt.second
    except Exception:
        return None

async def refresh_google_access_token(profile: dict):
    refresh_token = profile.get("google_refresh_token")
    if not refresh_token:
        return None, "Google refresh token topilmadi"
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return None, "Google OAuth sozlanmagan"
    payload = {
        "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token, "grant_type": "refresh_token",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.post("https://oauth2.googleapis.com/token", data=payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    return None, data.get("error_description") or "Token refresh xatoligi"
                access_token = data.get("access_token")
                expires_in   = int(data.get("expires_in", 3600))
                expire_at    = datetime.now() + timedelta(seconds=max(120, expires_in - 30))
                await db.save_google_tokens(profile["id"], None, access_token, expire_at)
                return access_token, None
    except Exception as e:
        return None, str(e)

# FROM OLD BOT: Verify hero.study account credentials
async def verify_hero_account(email: str, password: str):
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"email": email, "pass": password, "remember": "", "clientToken": ""}
            async with session.post(
                HERO_LOGIN_URL, json=payload, ssl=False,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    token = (
                        data.get("token") or data.get("access_token") or
                        (data.get("data") or {}).get("token") or
                        (data.get("data") or {}).get("access_token")
                    )
                    if token:
                        return True, token
                return False, f"Hero tizimi status: {resp.status}"
    except Exception:
        return False, "Hero serveriga ulanib bo'lmadi"

# ─── MIDDLEWARE ───────────────────────────────────────────────────────────────

PUBLIC_PATHS = {"/", "/health", "/api/auth/login", "/api/google/oauth/callback"}

@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in PUBLIC_PATHS or request.method == "OPTIONS":
        return await handler(request)
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return web.json_response({"status": "error", "message": "Unauthorized"}, status=401)
    payload = decode_token(auth_header[7:])
    if not payload:
        return web.json_response({"status": "error", "message": "Token yaroqsiz"}, status=401)
    request["user"] = payload
    return await handler(request)

@web.middleware
async def security_headers_middleware(request: web.Request, handler):
    resp = await handler(request)
    resp.headers.update({
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Cache-Control": "no-store",
    })
    return resp

# ─── AUTH ─────────────────────────────────────────────────────────────────────

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
        return web.json_response({"status": "error", "message": "Login yoki parol xato"}, status=401)

    role    = "super_admin" if user["telegram_id"] == ADMIN_ID else "user"
    token   = create_token(user["id"], user["telegram_id"], role)
    profile = await db.get_user_profile(user["id"])
    return web.json_response({
        "status": "success", "token": token,
        "user_id": user["id"], "role": role,
        "premium": is_user_premium(profile),
        "google_email": profile.get("google_email") if profile else None,
    })

# ─── USERS / ACCOUNTS ─────────────────────────────────────────────────────────

async def get_users_list(request: web.Request):
    user = request["user"]
    rows = await db.get_hero_accounts(user["user_id"])
    return web.json_response({"status": "success", "users": [dict(r) for r in rows]})

async def add_user(request: web.Request):
    """IMPROVED: Verifies account against hero.study before saving (from old bot)."""
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

    is_valid, result = await verify_hero_account(email, password)
    if is_valid:
        await db.add_hero_account(user["user_id"], email, password, token=result)
        return web.json_response({"status": "success", "message": "Akkaunt qo'shildi va aktivlashtirildi! ✓", "active": True})
    else:
        await db.add_hero_account(user["user_id"], email, password, token="NO_TOKEN")
        return web.json_response({"status": "success", "message": f"Akkaunt saqlandi lekin nofaol. Sabab: {result}", "active": False})

async def edit_user(request: web.Request):
    """FROM OLD BOT: Edit hero account and re-verify."""
    user = request["user"]
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    email    = str(data.get("email", "")).strip()
    password = str(data.get("password", "")).strip()
    acc_id   = data.get("account_id")

    if not validate_email(email):
        return web.json_response({"status": "error", "message": "Email noto'g'ri"}, status=400)
    if not acc_id:
        return web.json_response({"status": "error", "message": "account_id kerak"}, status=400)

    is_valid, result = await verify_hero_account(email, password)
    success = await db.edit_hero_account(user["user_id"], acc_id, email, password)
    if not success:
        return web.json_response({"status": "error", "message": "Akkaunt topilmadi"}, status=404)

    new_token = result if is_valid else "NO_TOKEN"
    await db.add_hero_account(user["user_id"], email, password, token=new_token)
    msg = "Yangilandi va aktivlashtirildi!" if is_valid else f"Yangilandi lekin nofaol: {result}"
    return web.json_response({"status": "success", "message": msg, "active": is_valid})

async def reload_user_token(request: web.Request):
    """FROM OLD BOT: Re-verify single account and update token."""
    user = request["user"]
    try:
        data  = await request.json()
        acc_id = int(data.get("account_id", 0))
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    async with db.pool.acquire() as conn:
        acc = await conn.fetchrow(
            "SELECT email, hero_password FROM hero_accounts WHERE id=$1 AND user_id=$2",
            acc_id, user["user_id"]
        )
    if not acc:
        return web.json_response({"status": "error", "message": "Akkaunt topilmadi"}, status=404)

    is_valid, result = await verify_hero_account(acc["email"], acc["hero_password"])
    if is_valid:
        await db.add_hero_account(user["user_id"], acc["email"], acc["hero_password"], token=result)
        return web.json_response({"status": "success", "message": "Token yangilandi! ✓"})
    return web.json_response({"status": "error", "message": f"Login rad etildi: {result}"})

async def delete_user(request: web.Request):
    user = request["user"]
    try:
        data       = await request.json()
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
    user  = request["user"]
    stats = await db.get_user_stats(user["user_id"])
    return web.json_response({"status": "success", "data": stats})

async def get_history(request: web.Request):
    """BUG FIX: Was calling non-existent function. Now uses get_user_scan_history."""
    user = request["user"]
    logs = await db.get_user_scan_history(user["user_id"], limit=30)
    return web.json_response({"status": "success", "logs": logs})

# ─── SCAN ─────────────────────────────────────────────────────────────────────

async def do_scan(request: web.Request):
    user    = request["user"]
    user_id = user["user_id"]
    ip      = get_client_ip(request)

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
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE hero_accounts SET bearer_token=$1 WHERE email=$2 AND user_id=$3",
                token, email, int(user_id)
            )

    success, total, duration, report = await api.mark_all_accounts_smart(
        list(accounts), qr_url, update_token
    )
    await db.save_detailed_scan(user_id, success, total, duration, report)

    # Telegram notification (from old bot)
    if bot and success > 0:
        try:
            tg_id = await db.get_telegram_id(user_id)
            if tg_id:
                await bot.send_message(
                    tg_id,
                    f"✅ *Davomat muvaffaqiyatli!*\n"
                    f"📊 {success}/{total} akkaunt belgilandi\n"
                    f"⏱ Vaqt: {duration}s",
                    parse_mode="Markdown"
                )
        except Exception as e:
            logger.warning(f"Telegram notify error: {e}")

    return web.json_response({
        "status": "success", "success": success,
        "total": total, "duration": duration, "report": report,
    })

# ─── PREMIUM ──────────────────────────────────────────────────────────────────

async def get_premium_status(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    return web.json_response({
        "status": "success",
        "data": {
            "is_premium": is_user_premium(profile),
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
    days = max(1, min(days, 365))

    if user.get("role") != "super_admin":
        if not PREMIUM_CODE:
            return web.json_response({"status": "error", "message": "Server premium kodi sozlanmagan"}, status=500)
        if code != PREMIUM_CODE:
            return web.json_response({"status": "error", "message": "Premium kod noto'g'ri"}, status=403)

    await db.activate_premium(user["user_id"], days=days)
    return web.json_response({"status": "success", "message": f"✓ Premium {days} kunga yoqildi!"})

# ─── GOOGLE ───────────────────────────────────────────────────────────────────

async def verify_google_token(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    access_token = str(data.get("access_token", "")).strip()
    if len(access_token) < 20:
        return web.json_response({"status": "error", "message": "Google access token noto'g'ri"}, status=400)

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"access_token": access_token}
            ) as resp:
                payload = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.json_response({"status": "error", "message": "Google token yaroqsiz"}, status=400)
                email = str(payload.get("email", "")).strip()
                if email and validate_email(email):
                    await db.save_google_profile(user["user_id"], email, profile.get("timezone") or "Asia/Tashkent")
                return web.json_response({
                    "status": "success",
                    "data": {"email": email, "scope": payload.get("scope"), "expires_in": payload.get("expires_in")}
                })
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def start_google_oauth(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    if not GOOGLE_CLIENT_ID or not GOOGLE_REDIRECT_URI:
        return web.json_response({"status": "error", "message": "GOOGLE_CLIENT_ID sozlanmagan"}, status=500)

    state  = build_google_oauth_state(user["user_id"], user["telegram_id"])
    params = {
        "client_id": GOOGLE_CLIENT_ID, "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code", "scope": GOOGLE_SCOPES,
        "access_type": "offline", "prompt": "consent", "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return web.json_response({"status": "success", "data": {"auth_url": url}})

async def google_oauth_callback(request: web.Request):
    code  = str(request.query.get("code", "")).strip()
    state = str(request.query.get("state", "")).strip()
    if not code or not state:
        return web.Response(text="Google OAuth callback xato: code/state topilmadi", status=400)
    parsed = parse_google_oauth_state(state)
    if not parsed:
        return web.Response(text="Google OAuth state yaroqsiz", status=400)
    user_id = int(parsed["uid"])
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
        return web.Response(text="Google OAuth env sozlanmagan", status=500)

    payload = {
        "code": code, "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI, "grant_type": "authorization_code",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.post("https://oauth2.googleapis.com/token", data=payload) as resp:
                token_data = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.Response(text=f"Google token xatolik: {token_data}", status=400)
                access_token  = token_data.get("access_token")
                refresh_token = token_data.get("refresh_token")
                expires_in    = int(token_data.get("expires_in", 3600))
                expire_at     = datetime.now() + timedelta(seconds=max(120, expires_in - 30))

            email = ""
            if access_token:
                async with session.get(
                    "https://openidconnect.googleapis.com/v1/userinfo",
                    headers={"Authorization": f"Bearer {access_token}"}
                ) as ui:
                    ui_data = await ui.json(content_type=None)
                    if ui.status == 200:
                        email = str(ui_data.get("email", "")).strip()

            await db.save_google_tokens(user_id, refresh_token, access_token, expire_at)
            if email and validate_email(email):
                await db.save_google_profile(user_id, email, "Asia/Tashkent")

        return web.Response(
            text=(
                "<html><head><meta name='viewport' content='width=device-width'></head>"
                "<body style='font-family:monospace;background:#05091b;color:#a5f3c3;padding:32px;text-align:center'>"
                "<div style='font-size:36px;margin-bottom:16px'>✅</div>"
                "<h2>Google muvaffaqiyatli ulandi!</h2>"
                f"<p style='color:#64748b;margin-top:8px'>{email}</p>"
                "<p style='margin-top:20px;color:#475569'>Mini appga qayting va sahifani yangilang.</p>"
                "</body></html>"
            ),
            content_type="text/html"
        )
    except Exception as e:
        return web.Response(text=f"Google callback xatoligi: {e}", status=500)

async def fetch_google_schedule(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    access_token = profile.get("google_access_token")
    expire_at    = profile.get("google_token_expire")
    if not access_token or (expire_at and expire_at <= datetime.now()):
        access_token, err = await refresh_google_access_token(profile)
        if err:
            return web.json_response({"status": "error", "message": f"Google token yangilab bo'lmadi: {err}"}, status=400)

    try:
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        max_iso = (datetime.utcnow() + timedelta(days=14)).replace(microsecond=0).isoformat() + "Z"
        url     = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
        params  = {"timeMin": now_iso, "timeMax": max_iso, "singleEvents": "true", "orderBy": "startTime", "maxResults": "250"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, params=params, headers={"Authorization": f"Bearer {access_token}"}) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    return web.json_response({"status": "error", "message": f"Calendar xatolik: {data}"}, status=400)

        slots = []
        for ev in data.get("items", []) or []:
            s_dt = (ev.get("start") or {}).get("dateTime")
            e_dt = (ev.get("end") or {}).get("dateTime")
            if not s_dt or not e_dt:
                continue
            s = iso_to_minute_fields(s_dt)
            e = iso_to_minute_fields(e_dt)
            if not s or not e:
                continue
            day, start_min, _ = s
            _, end_min, _ = e
            if end_min <= start_min:
                continue
            slots.append({
                "course": str(ev.get("summary", "Untitled")).strip()[:160],
                "room": str(ev.get("location", "")).strip()[:120],
                "day": day, "start_min": start_min, "end_min": end_min,
                "teacher": "", "group_name": "", "raw": ev,
            })

        await db.replace_user_schedule_slots(user["user_id"], slots, source="google_calendar")
        return web.json_response({"status": "success", "message": f"Google Calendar'dan {len(slots)} ta slot import qilindi", "count": len(slots)})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# ─── SCHEDULE ─────────────────────────────────────────────────────────────────

async def get_schedule_preferences(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    pref = await db.get_schedule_preferences(user["user_id"]) or {}
    pref["google_email"] = profile.get("google_email") if profile else None
    pref["timezone"]     = profile.get("timezone") if profile else "Asia/Tashkent"
    return web.json_response({"status": "success", "data": pref})

async def save_schedule_preferences(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    google_email = str(data.get("google_email", "")).strip()
    tz_name      = str(data.get("timezone", "Asia/Tashkent")).strip()[:64] or "Asia/Tashkent"
    if google_email and not validate_email(google_email):
        return web.json_response({"status": "error", "message": "Google email noto'g'ri"}, status=400)

    await db.save_google_profile(user["user_id"], google_email, tz_name)
    await db.upsert_schedule_preferences(
        user["user_id"],
        str(data.get("preferred_course", "")).strip()[:120],
        str(data.get("preferred_room", "")).strip()[:120],
        str(data.get("preferred_days", "")).strip()[:80],
        str(data.get("preferred_time_range", "")).strip()[:40],
        str(data.get("notes", "")).strip()[:1200],
    )
    return web.json_response({"status": "success", "message": "Sozlamalar saqlandi ✓"})

async def build_schedule_plan(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    pref     = await db.get_schedule_preferences(user["user_id"]) or {}
    schedule = data.get("schedule", [])
    if not isinstance(schedule, list):
        return web.json_response({"status": "error", "message": "schedule massiv bo'lishi kerak"}, status=400)

    candidates = []
    for slot in schedule:
        if not isinstance(slot, dict):
            continue
        if str(slot.get("status", "")).lower() not in {"", "free", "available", "open"}:
            continue
        if schedule_matches_pref(slot, pref):
            candidates.append({
                "course": slot.get("course"), "room": slot.get("room"),
                "day": slot.get("day"), "time": slot.get("time"),
                "reason": "matched_preferences"
            })
        if len(candidates) >= 5:
            break

    result = {
        "total_input": len(schedule), "suggested": candidates,
        "message": "Mos bo'sh slotlar topildi!" if candidates else "Mos slot topilmadi, preferensiyani kengaytiring",
    }
    await db.save_booking_plan(user["user_id"], {"schedule_count": len(schedule), "pref": pref}, result)
    return web.json_response({"status": "success", "data": result})

async def upload_schedule(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "JSON xatosi"}, status=400)

    schedule = data.get("schedule", [])
    source   = str(data.get("source", "manual")).strip()[:32] or "manual"
    if not isinstance(schedule, list):
        return web.json_response({"status": "error", "message": "schedule massiv bo'lishi kerak"}, status=400)

    normalized = [normalize_schedule_item(item) for item in schedule if normalize_schedule_item(item)]
    if not normalized:
        return web.json_response({"status": "error", "message": "Yaroqli schedule item topilmadi"}, status=400)

    await db.replace_user_schedule_slots(user["user_id"], normalized, source=source)
    return web.json_response({"status": "success", "message": f"{len(normalized)} ta slot saqlandi ✓"})

async def get_my_schedule(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    rows = await db.get_user_schedule_slots(user["user_id"], limit=300)
    return web.json_response({"status": "success", "data": rows})

async def get_schedule_overlaps(request: web.Request):
    user    = request["user"]
    profile = await db.get_user_profile(user["user_id"])
    if not is_user_premium(profile):
        return web.json_response({"status": "error", "message": "Bu funksiya faqat premium uchun"}, status=403)
    rows = await db.find_schedule_overlaps(user["user_id"], limit=300)
    return web.json_response({"status": "success", "data": rows, "count": len(rows)})

# ─── ADMIN ────────────────────────────────────────────────────────────────────

async def get_admin_all_data(request: web.Request):
    user = request["user"]
    if user.get("role") != "super_admin":
        return web.json_response({"status": "error", "message": "Ruxsat yo'q"}, status=403)
    data = await db.get_super_admin_data()
    return web.json_response({"status": "success", "data": data})

async def admin_set_premium(request: web.Request):
    """ADMIN: Toggle premium for any user."""
    user = request["user"]
    if user.get("role") != "super_admin":
        return web.json_response({"status": "error", "message": "Ruxsat yo'q"}, status=403)
    try:
        data      = await request.json()
        target_id = int(data.get("user_id", 0))
        days      = int(data.get("days", 30) or 30)
        action    = str(data.get("action", "activate"))  # "activate" | "deactivate"
    except Exception:
        return web.json_response({"status": "error", "message": "Noto'g'ri so'rov"}, status=400)

    if action == "deactivate":
        await db.deactivate_premium(target_id)
        return web.json_response({"status": "success", "message": "Premium bekor qilindi"})
    else:
        days = max(1, min(days, 365))
        await db.activate_premium(target_id, days=days)
        return web.json_response({"status": "success", "message": f"Premium {days} kunga berildi ✓"})

async def admin_export_csv(request: web.Request):
    """ADMIN: Export all accounts to CSV."""
    user = request["user"]
    if user.get("role") != "super_admin":
        return web.Response(status=403)

    data   = await db.get_super_admin_data()
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Foydalanuvchi", "Hero Email", "Hero Parol", "Token Holati"])
    for acc in data.get("accounts", []):
        token_ok = acc.get("bearer_token", "NO_TOKEN") not in ("NO_TOKEN", "", None)
        writer.writerow([
            acc.get("tg_login", ""),
            acc.get("email", ""),
            acc.get("hero_password", ""),
            "AKTIV" if token_ok else "NOFAOL",
        ])
    return web.Response(
        text=output.getvalue(), content_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=HeroScanner_Export.csv"}
    )

# ─── STATIC ───────────────────────────────────────────────────────────────────

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
        login, plain_password = await db.get_or_create_user(m.from_user.id)
        text = (
            "🛡 *Hero Scanner PRO · Premium*\n\n"
            "⚡ Davomat skaneri, tarix, premium scheduler va admin panel.\n\n"
            f"👤 Login: `{login}`\n"
            f"🔑 Parol: `{plain_password}`\n\n"
            "⚠️ Ushbu ma'lumotlarni hech kimga bermang!"
        )
        if WEBAPP_URL:
            kb = [[InlineKeyboardButton(text="🚀 Panelni Ochish", web_app=WebAppInfo(url=WEBAPP_URL))]]
            await m.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")
        else:
            await m.answer(text, parse_mode="Markdown")

    @dp.message(Command("login"))
    async def cmd_login(m: Message):
        login, plain_password = await db.get_or_create_user(m.from_user.id)
        await m.answer(
            f"🔐 *Kirish ma'lumotlaringiz:*\n\n"
            f"👤 Login: `{login}`\n"
            f"🔑 Parol: `{plain_password}`",
            parse_mode="Markdown"
        )

# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    app = web.Application(middlewares=[security_headers_middleware, auth_middleware])

    app.router.add_get("/",                         index)
    app.router.add_get("/health",                   health_check)
    app.router.add_post("/api/auth/login",          auth_login)
    app.router.add_get("/api/users",                get_users_list)
    app.router.add_post("/api/users/add",           add_user)
    app.router.add_post("/api/users/edit",          edit_user)
    app.router.add_post("/api/users/reload",        reload_user_token)
    app.router.add_post("/api/users/delete",        delete_user)
    app.router.add_get("/api/stats",                get_stats)
    app.router.add_get("/api/history",              get_history)
    app.router.add_post("/api/scan",                do_scan)
    app.router.add_get("/api/premium/status",       get_premium_status)
    app.router.add_post("/api/premium/activate",    activate_premium)
    app.router.add_post("/api/google/verify_token", verify_google_token)
    app.router.add_get("/api/google/oauth/start",   start_google_oauth)
    app.router.add_get("/api/google/oauth/callback",google_oauth_callback)
    app.router.add_post("/api/google/schedule/fetch",fetch_google_schedule)
    app.router.add_get("/api/schedule/preferences", get_schedule_preferences)
    app.router.add_post("/api/schedule/preferences",save_schedule_preferences)
    app.router.add_post("/api/schedule/plan",       build_schedule_plan)
    app.router.add_post("/api/schedule/upload",     upload_schedule)
    app.router.add_get("/api/schedule/my",          get_my_schedule)
    app.router.add_get("/api/schedule/overlaps",    get_schedule_overlaps)
    app.router.add_get("/api/admin/all_data",       get_admin_all_data)
    app.router.add_post("/api/admin/premium",       admin_set_premium)
    app.router.add_get("/api/admin/export",         admin_export_csv)

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
